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

use super::*;
use size_class::{POOL_THRESHOLD, SIZE_CLASSES, size_class_index};

// ---------------------------------------------------------------------------
// Threshold / size-class routing
// ---------------------------------------------------------------------------

#[test]
fn test_acquire_below_threshold() {
    let size = POOL_THRESHOLD - 1;
    let buf = acquire(size);
    // Below threshold → not pooled; dropping it does not touch the pool.
    assert_eq!(buf.len(), size);
    assert!(!buf.pooled);
    // Pool should be untouched for the 64 KiB class (index 0).
    assert_eq!(pool_free_count(0), 0);
}

#[test]
fn test_acquire_exact_class() {
    // 128 KiB is exactly SIZE_CLASSES[1].
    let size = 128 * 1024;
    let idx = size_class_index(size).expect("128 KiB must have a class");
    assert_eq!(SIZE_CLASSES[idx], size);

    let buf = acquire(size);
    assert!(buf.pooled);
    assert_eq!(buf.len(), size);
    assert!(buf.capacity() >= size);
}

#[test]
fn test_acquire_rounds_up() {
    // 100 KiB is between 64 KiB and 128 KiB → should get the 128 KiB class.
    let size = 100 * 1024;
    let buf = acquire(size);
    assert!(buf.pooled);
    assert_eq!(buf.len(), size);
    assert_eq!(buf.capacity(), 128 * 1024);
}

#[test]
fn test_acquire_above_max_class() {
    // 3 MiB exceeds the largest class (2 MiB) → unpooled.
    let size = 3 * 1024 * 1024;
    let buf = acquire(size);
    assert!(!buf.pooled);
    assert_eq!(buf.len(), size);
    // Dropping should not affect any pool slab.
    drop(buf);
    for i in 0..NUM_CLASSES {
        assert_eq!(pool_free_count(i), 0, "class {i} should be empty after above-max drop");
    }
}

// ---------------------------------------------------------------------------
// Drop / return-to-pool
// ---------------------------------------------------------------------------

#[test]
fn test_drop_returns_to_pool() {
    let size = 128 * 1024;
    let idx = size_class_index(size).unwrap();

    // Drain any pre-existing pool entries for this class on this thread.
    while pool_free_count(idx) > 0 {
        let _ = acquire(size);
    }
    assert_eq!(pool_free_count(idx), 0);

    {
        let buf = acquire(size);
        assert!(buf.pooled);
        assert_eq!(pool_free_count(idx), 0); // still held
    } // buf drops here → returned to pool

    assert_eq!(pool_free_count(idx), 1);

    // Acquire again — should reuse the pooled buffer.
    let buf2 = acquire(size);
    assert!(buf2.pooled);
    assert_eq!(pool_free_count(idx), 0);
}

#[test]
fn test_pool_max_capacity() {
    let size = 256 * 1024;
    let idx = size_class_index(size).unwrap();

    // Drain existing pool entries.
    while pool_free_count(idx) > 0 {
        let _ = acquire(size);
    }

    // Acquire MAX_PER_CLASS + 1 buffers, then drop them all.
    let mut bufs: Vec<ShardBuf> = (0..=MAX_PER_CLASS).map(|_| acquire(size)).collect();
    // All are held; pool is empty.
    assert_eq!(pool_free_count(idx), 0);

    // Drop them one by one.
    for _ in 0..MAX_PER_CLASS {
        bufs.pop();
    }
    // Pool should now hold exactly MAX_PER_CLASS buffers.
    assert_eq!(pool_free_count(idx), MAX_PER_CLASS);

    // Drop the (MAX_PER_CLASS + 1)-th buffer — pool is full, so it is freed.
    bufs.pop();
    assert_eq!(pool_free_count(idx), MAX_PER_CLASS, "pool should not exceed MAX_PER_CLASS");
}

// ---------------------------------------------------------------------------
// Alignment
// ---------------------------------------------------------------------------

#[test]
fn test_alignment_4096_all_classes() {
    for &class_size in &SIZE_CLASSES {
        let buf = acquire(class_size);
        let ptr = buf.as_ptr() as usize;
        assert_eq!(ptr % 4096, 0, "class {class_size}: pointer {ptr:#x} is not 4096-byte aligned");
    }
}

#[test]
fn test_alignment_4096_unpooled_below_threshold() {
    let buf = acquire(1024); // below threshold
    assert_eq!(buf.as_ptr() as usize % 4096, 0);
}

#[test]
fn test_alignment_4096_unpooled_above_max() {
    let buf = acquire(3 * 1024 * 1024); // above max class
    assert_eq!(buf.as_ptr() as usize % 4096, 0);
}

// ---------------------------------------------------------------------------
// set_len / Deref / DerefMut
// ---------------------------------------------------------------------------

#[test]
fn test_set_len_and_deref() {
    let mut buf = acquire(128 * 1024);
    // Write a pattern into the first 100 bytes via full_slice_mut.
    buf.full_slice_mut()[..100].fill(0xAB);
    buf.set_len(100);

    // Deref should yield exactly 100 bytes.
    assert_eq!(buf.len(), 100);
    assert_eq!(buf.deref().len(), 100);
    assert!(buf.iter().all(|&b| b == 0xAB));
}

#[test]
fn test_deref_mut_writes_visible() {
    let mut buf = acquire(128 * 1024);
    buf.set_len(64);
    for b in buf.deref_mut() {
        *b = 0xFF;
    }
    assert!(buf.iter().all(|&b| b == 0xFF));
}

// ---------------------------------------------------------------------------
// into_vec
// ---------------------------------------------------------------------------

#[test]
fn test_into_vec_correct_length() {
    let mut buf = acquire(128 * 1024);
    buf.full_slice_mut()[..50].fill(0x42);
    buf.set_len(50);

    let v = buf.into_vec();
    assert_eq!(v.len(), 50);
    assert!(v.iter().all(|&b| b == 0x42));
}

#[test]
fn test_into_vec_does_not_return_to_pool() {
    let size = 128 * 1024;
    let idx = size_class_index(size).unwrap();

    while pool_free_count(idx) > 0 {
        let _ = acquire(size);
    }

    let buf = acquire(size);
    let _v = buf.into_vec(); // transfers ownership, should NOT return to pool

    assert_eq!(pool_free_count(idx), 0, "into_vec must not return buffer to pool");
}

// ---------------------------------------------------------------------------
// Thread-local isolation
// ---------------------------------------------------------------------------

#[test]
fn test_thread_local_isolation() {
    use std::sync::{Arc, Barrier};

    let size = 128 * 1024;
    let barrier = Arc::new(Barrier::new(2));

    let b2 = Arc::clone(&barrier);
    let handle = std::thread::spawn(move || {
        // Acquire a buffer on the spawned thread.
        let mut buf = acquire(size);
        buf.full_slice_mut().fill(0x11);
        b2.wait(); // synchronise: both threads hold a buffer now
        // The buffer on this thread should still contain 0x11.
        assert!(buf.iter().all(|&b| b == 0x11), "spawned thread data corrupted");
    });

    let mut buf = acquire(size);
    buf.full_slice_mut().fill(0x22);
    barrier.wait(); // synchronise
    assert!(buf.iter().all(|&b| b == 0x22), "main thread data corrupted");

    handle.join().expect("spawned thread panicked");
}
