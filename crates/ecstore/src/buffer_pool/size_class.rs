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

/// Minimum buffer size eligible for pooling (64 KiB).
///
/// Below this threshold, jemalloc's thread-local cache (tcache) handles
/// allocations without `madvise` overhead, so pooling adds no benefit.
pub const POOL_THRESHOLD: usize = 64 * 1024;

/// The set of pooled size classes, in ascending order.
///
/// Each class is a power-of-two multiple of 64 KiB, covering the range of
/// shard sizes produced by common erasure configurations with block_size=1MiB:
///
/// | Erasure config       | Shard size  | Class    |
/// |----------------------|-------------|----------|
/// | 8+4,  block=1MiB     | 131_072 B   | 131_072  |
/// | 12+4, block=1MiB     |  87_382 B   | 131_072  |
/// | 4+2,  block=1MiB     | 262_144 B   | 262_144  |
/// | encode buf 8+4 1MiB  | 1_572_864 B | 2_097_152|
pub const SIZE_CLASSES: [usize; 6] = [
    64 * 1024,       // 64 KiB
    128 * 1024,      // 128 KiB
    256 * 1024,      // 256 KiB
    512 * 1024,      // 512 KiB
    1024 * 1024,     // 1 MiB
    2 * 1024 * 1024, // 2 MiB
];

pub const NUM_CLASSES: usize = SIZE_CLASSES.len();

/// Returns the index into `SIZE_CLASSES` for the smallest class >= `n`.
///
/// Returns `None` when `n` exceeds the largest class (2 MiB), in which case
/// the caller should allocate directly without pooling.
pub fn size_class_index(n: usize) -> Option<usize> {
    SIZE_CLASSES.iter().position(|&c| c >= n)
}

/// Returns the capacity (in bytes) of the smallest pooled size class >= `n`,
/// or `None` if `n` exceeds the largest class.
pub fn size_class(n: usize) -> Option<usize> {
    size_class_index(n).map(|i| SIZE_CLASSES[i])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_class_exact_boundaries() {
        assert_eq!(size_class(64 * 1024), Some(64 * 1024));
        assert_eq!(size_class(128 * 1024), Some(128 * 1024));
        assert_eq!(size_class(2 * 1024 * 1024), Some(2 * 1024 * 1024));
    }

    #[test]
    fn test_size_class_rounds_up() {
        // 100 KiB rounds up to 128 KiB class
        assert_eq!(size_class(100 * 1024), Some(128 * 1024));
        // 1 byte above 64 KiB rounds up to 128 KiB
        assert_eq!(size_class(64 * 1024 + 1), Some(128 * 1024));
    }

    #[test]
    fn test_size_class_above_max_returns_none() {
        assert_eq!(size_class(2 * 1024 * 1024 + 1), None);
        assert_eq!(size_class(usize::MAX), None);
    }

    #[test]
    fn test_size_class_zero() {
        // Zero is below threshold but size_class itself just finds the first class >= 0
        assert_eq!(size_class(0), Some(64 * 1024));
    }
}
