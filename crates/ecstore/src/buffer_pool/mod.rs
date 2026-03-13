// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Thread-local shard buffer pool for erasure coding I/O.
//!
//! Under sustained large-object GET/PUT load, the erasure coding paths allocate and
//! immediately free large buffers (64 KiB – 2 MiB) on every erasure block. The
//! allocator (jemalloc) calls `madvise(MADV_FREE)` when freeing these large
//! allocations, returning physical pages to the OS. The next allocation then
//! triggers page faults to bring them back.
//!
//! A per-thread slab pool keyed by size class.  Buffers that are returned to the
//! pool stay mapped (pages stay warm), eliminating the `madvise` / page-fault
//! round-trip for the common case.
//!
//! All pooled buffers are allocated with 4096-byte alignment via [`aligned_vec::AVec`],
//! satisfying the `O_DIRECT` (for future use) without any `unsafe` code.  Unpooled
//! fallback buffers (below threshold or above max class) are also 4096-byte aligned
//! for consistency.
//!
//! Pool usage is controlled by two environment variables read once at first call:
//! - `RUSTFS_SHARD_POOL_GET` — enables pool on the read (GET) path
//! - `RUSTFS_SHARD_POOL_PUT` — enables pool on the write (PUT) path
//!
//! Both default to `false` (disabled) so the feature is opt-in.

pub mod pool_config;
pub mod size_class;

#[cfg(test)]
mod tests;

use aligned_vec::{AVec, ConstAlign};
use size_class::{NUM_CLASSES, POOL_THRESHOLD, size_class_index};
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};

/// Alignment used for all pooled (and unpooled-fallback) buffers.
/// 4096 bytes satisfies the Linux `O_DIRECT` requirement for all common
/// filesystem block sizes, for future `O_DIRECT` use.
const ALIGN: usize = 4096;

/// Maximum number of buffers retained per size class per thread.
/// 32 × 2 MiB = 64 MiB worst-case per thread per class; in practice the
/// working set is much smaller because the erasure block loop is sequential.
/// TODO: Needs tuning.
const MAX_PER_CLASS: usize = 32;

/// ShardBuf is a pooled, 4096-byte-aligned byte buffer with RAII return-to-pool semantics.
//
/// Create new instance with `aquire` top level function.
///
/// When dropped, the underlying allocation is returned to the
/// thread-local pool (if it was pooled) rather than freed.
///
/// `Deref` / `DerefMut` yield `&[u8]` / `&mut [u8]` of length `self.len()`,
/// i.e. the *logical* fill length, not the full capacity.
///
/// Use [`ShardBuf::full_slice_mut`] to get a `&mut [u8]` of the full
/// capacity for passing to a read call.
pub struct ShardBuf {
    /// The underlying aligned allocation.  Always `Some` while the `ShardBuf`
    /// is live; taken (replaced with `None`) in `Drop` before returning to pool.
    inner: Option<AVec<u8, ConstAlign<ALIGN>>>,
    /// Logical fill length — the number of bytes that have been written.
    len: usize,
    /// Whether this buffer came from the pool (and should be returned to it).
    pooled: bool,
}

impl ShardBuf {
    /// Allocate a fresh 4096-aligned buffer of `capacity` bytes, not pooled.
    fn alloc(capacity: usize) -> Self {
        let mut v: AVec<u8, ConstAlign<ALIGN>> = AVec::with_capacity(ALIGN, capacity);
        // Extend to full capacity so the slice is addressable.
        v.resize(capacity, 0u8);
        ShardBuf {
            inner: Some(v),
            len: capacity,
            pooled: false,
        }
    }

    /// Wrap a buffer returned from the pool.
    fn from_pooled(mut v: AVec<u8, ConstAlign<ALIGN>>) -> Self {
        let cap = v.capacity();
        // Ensure the vec's length covers the full capacity so slices work.
        if v.len() < cap {
            v.resize(cap, 0u8);
        }
        ShardBuf {
            inner: Some(v),
            len: cap,
            pooled: true,
        }
    }

    /// The logical fill length of this buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the logical length is zero.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The total allocated capacity (>= `len`, rounded up to size class).
    pub fn capacity(&self) -> usize {
        self.inner.as_ref().map_or(0, |v| v.capacity())
    }

    /// Set the logical fill length after a read has populated `n` bytes.
    ///
    /// # Panics
    /// Panics if `n > capacity()`.
    pub fn set_len(&mut self, n: usize) {
        assert!(n <= self.capacity(), "set_len({n}) > capacity({})", self.capacity());
        self.len = n;
    }

    /// Returns a mutable slice over the full capacity of the buffer.
    ///
    /// Use this when passing the buffer to a read call that will fill it.
    /// After the read, call [`set_len`](ShardBuf::set_len) with the number of
    /// bytes actually written.
    pub fn full_slice_mut(&mut self) -> &mut [u8] {
        self.inner.as_mut().expect("ShardBuf already consumed").as_mut_slice()
    }

    /// Convert into a `Vec<u8>` of length `self.len()`.
    ///
    /// The returned `Vec` is a copy when the logical length differs from
    /// capacity (last partial block), otherwise it reuses the allocation.
    /// The pool does not receive the buffer back, ownership transfers to
    /// caller. Use this when the downstream API requires `Vec<u8>`.
    pub fn into_vec(mut self) -> Vec<u8> {
        let len = self.len;
        // Take the AVec so Drop does not try to return it.
        let mut v = self.inner.take().expect("ShardBuf already consumed");
        self.pooled = false; // prevent double-return in Drop
        v.truncate(len);
        // AVec does not implement Into<Vec>, so we copy into a Vec.
        v.as_slice().to_vec()
    }

    /// Returns a raw pointer to the start of the buffer data.
    ///
    /// Useful for verifying alignment in tests.
    pub fn as_ptr(&self) -> *const u8 {
        self.inner.as_ref().expect("ShardBuf already consumed").as_ptr()
    }
}

impl Deref for ShardBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.inner.as_ref().expect("ShardBuf already consumed").as_slice()[..self.len]
    }
}

impl DerefMut for ShardBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        let len = self.len;
        &mut self.inner.as_mut().expect("ShardBuf already consumed").as_mut_slice()[..len]
    }
}

impl Drop for ShardBuf {
    fn drop(&mut self) {
        if self.pooled
            && let Some(v) = self.inner.take()
        {
            SHARD_POOL.with(|pool| {
                pool.borrow_mut().return_buf(v);
            });
        }
        // If not pooled, inner drops normally here.
    }
}

/// Per-thread slab pool. One stack of free buffers per size class.
struct SlabPool {
    slabs: [Vec<AVec<u8, ConstAlign<ALIGN>>>; NUM_CLASSES],
    max_per_class: usize,
}

impl SlabPool {
    fn new(max_per_class: usize) -> Self {
        SlabPool {
            // Array init, each element is an independent Vec.
            slabs: std::array::from_fn(|_| Vec::new()),
            max_per_class,
        }
    }

    /// Acquire a buffer of at least `size` bytes from the pool.
    ///
    /// Returns a pooled `ShardBuf` if a matching size class exists and a
    /// free buffer is available, otherwise allocates a fresh one.
    fn acquire(&mut self, size: usize) -> ShardBuf {
        if let Some(idx) = size_class_index(size) {
            if let Some(v) = self.slabs[idx].pop() {
                // Free buffer in pool, return it.
                return ShardBuf::from_pooled(v);
            }
            // No free buffer in pool, allocate a new one at class size.
            let class_size = size_class::SIZE_CLASSES[idx];
            let mut v: AVec<u8, ConstAlign<ALIGN>> = AVec::with_capacity(ALIGN, class_size);
            v.resize(class_size, 0u8);
            let mut buf = ShardBuf {
                inner: Some(v),
                len: class_size,
                pooled: true,
            };
            buf.len = size; // logical length = requested size
            buf
        } else {
            // Above max class, allocate directly, no pooling.
            ShardBuf::alloc(size)
        }
    }

    /// Return a buffer to the pool.  Drops it if the slab is full.
    fn return_buf(&mut self, v: AVec<u8, ConstAlign<ALIGN>>) {
        if let Some(idx) = size_class_index(v.capacity())
            && self.slabs[idx].len() < self.max_per_class
        {
            self.slabs[idx].push(v);
            return;
        }
        // Slab full or no matching class, let it drop.
        drop(v);
    }

    /// Number of free buffers currently held in the pool for a given size class index.
    #[cfg(test)]
    fn free_count(&self, class_idx: usize) -> usize {
        self.slabs[class_idx].len()
    }
}

// Thread local pool of buffers
thread_local! {
    static SHARD_POOL: RefCell<SlabPool> = RefCell::new(SlabPool::new(MAX_PER_CLASS));
}

/// Acquire a 4096-byte-aligned buffer of at least `size` bytes.
///
/// - If `size < POOL_THRESHOLD` (64 KiB), returns a freshly allocated,
///   non-pooled buffer. jemalloc's tcache handles small allocations without
///   `madvise` overhead, so pooling adds no benefit.
/// - If `size` falls within a known size class (64 KiB – 2 MiB), returns a
///   buffer from the thread-local pool (or allocates a new one at class size).
/// - If `size > 2 MiB`, returns a freshly allocated, non-pooled buffer.
///
/// The buffer is automatically returned to the pool when dropped (unless it
/// was below the threshold or above the max class).
pub fn acquire(size: usize) -> ShardBuf {
    if size < POOL_THRESHOLD {
        return ShardBuf::alloc(size);
    }
    SHARD_POOL.with(|pool| pool.borrow_mut().acquire(size))
}

/// Number of free buffers in the thread-local pool for a given size class index.
///
/// Intended for use in tests only.
#[cfg(test)]
pub fn pool_free_count(class_idx: usize) -> usize {
    SHARD_POOL.with(|pool| pool.borrow().free_count(class_idx))
}
