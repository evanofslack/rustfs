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

use rustfs_config::{
    DEFAULT_SHARD_POOL_GET_ENABLED, DEFAULT_SHARD_POOL_PUT_ENABLED, ENV_SHARD_POOL_GET_ENABLED, ENV_SHARD_POOL_PUT_ENABLED,
};
use std::sync::OnceLock;

static POOL_GET_ENABLED: OnceLock<bool> = OnceLock::new();
static POOL_PUT_ENABLED: OnceLock<bool> = OnceLock::new();

fn parse_bool_env(key: &str) -> bool {
    std::env::var(key)
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
}

/// Returns `true` when the shard pool is enabled for the GET (read) path.
///
/// Controlled by `RUSTFS_SHARD_POOL_GET`.  Defaults to `false`.
/// The value is read from the environment once and cached.
pub fn pool_get_enabled() -> bool {
    *POOL_GET_ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_SHARD_POOL_GET_ENABLED, DEFAULT_SHARD_POOL_GET_ENABLED))
}

/// Returns `true` when the shard pool is enabled for the PUT (write) path.
///
/// Controlled by `RUSTFS_SHARD_POOL_PUT`.  Defaults to `false`.
/// The value is read from the environment once and cached.
pub fn pool_put_enabled() -> bool {
    *POOL_PUT_ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_SHARD_POOL_PUT_ENABLED, DEFAULT_SHARD_POOL_PUT_ENABLED))
}
