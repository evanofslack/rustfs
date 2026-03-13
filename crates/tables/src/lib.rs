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

//! # rustfs-tables
//!
//! S3 Tables — Iceberg REST Catalog implementation for RustFS.
//!
//! This crate provides the business logic layer:
//! - `CatalogStore` trait and the object-storage-backed implementation
//! - OCC commit logic (`commit` module)
//! - Request/response models matching the MinIO AIStor / Iceberg REST spec
//! - S3 object-key path helpers
//!
//! HTTP handlers and route registration live in `rustfs/src/admin/handlers/tables.rs`,
//! following the same pattern as `rustfs-batch`.

pub mod catalog;
pub mod commit;
pub mod error;
pub mod models;
pub mod path;

use catalog::s3_store::S3CatalogStore;
use catalog::SharedCatalogStore;
use std::sync::Arc;
use std::sync::OnceLock;

static GLOBAL_CATALOG: OnceLock<SharedCatalogStore> = OnceLock::new();

/// Initialize the global catalog store.
/// Must be called once during server startup when the `tables` feature is enabled.
pub fn init_tables_catalog() {
    GLOBAL_CATALOG.get_or_init(|| Arc::new(S3CatalogStore::new()));
}

/// Returns the global catalog store instance.
///
/// # Panics
/// Panics if `init_tables_catalog` has not been called.
pub fn get_global_catalog() -> SharedCatalogStore {
    GLOBAL_CATALOG
        .get()
        .expect("tables catalog not initialized; call init_tables_catalog() at startup")
        .clone()
}
