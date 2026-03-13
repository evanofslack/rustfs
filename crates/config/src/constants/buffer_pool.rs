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

/// Environment variable to enable the thread-local shard buffer pool for GET (read) paths.
///
/// - Purpose: When enabled, large shard buffers (>= 64 KB) on the erasure-decode read path
///   are drawn from a per-thread pool instead of being freshly allocated each time.
///   This eliminates the `madvise(MADV_FREE)` / page-fault overhead that shows up under
///   sustained large-object GET load.
/// - Default: disabled (opt-in).
pub const ENV_SHARD_POOL_GET_ENABLED: &str = "RUSTFS_SHARD_POOL_GET_ENABLED";

/// Environment variable to enable the thread-local shard buffer pool for PUT (write) paths.
///
/// - Purpose: When enabled, the large encode buffer allocated per erasure block on the write
///   path is drawn from the pool, keeping pages warm across successive PUT blocks.
/// - Default: disabled (opt-in).
pub const ENV_SHARD_POOL_PUT_ENABLED: &str = "RUSTFS_SHARD_POOL_PUT_ENABLED";

/// Default state for the GET shard pool: disabled.
pub const DEFAULT_SHARD_POOL_GET_ENABLED: bool = false;

/// Default state for the PUT shard pool: disabled.
pub const DEFAULT_SHARD_POOL_PUT_ENABLED: bool = false;
