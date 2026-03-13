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

//! `CatalogStore` trait — the single abstraction between HTTP handlers and
//! the underlying metadata persistence mechanism.

pub mod s3_store;

use crate::error::Result;
use crate::models::{
    CommitTableRequest, CommitTableResponse, CommitTransactionChange, CreateTableRequest, GetNamespaceResponse,
    LoadTableResult, UpdateNamespacePropertiesResponse, WarehouseMeta,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait CatalogStore: Send + Sync + 'static {
    // ── Warehouse ─────────────────────────────────────────────────────────

    /// Create a new warehouse. If `upgrade_existing` is true, adopt an
    /// existing bucket (already has objects) without error.
    async fn create_warehouse(&self, name: &str, upgrade_existing: bool) -> Result<()>;

    /// List all known warehouses.
    async fn list_warehouses(&self) -> Result<Vec<WarehouseMeta>>;

    /// Get metadata for a specific warehouse.
    async fn get_warehouse(&self, name: &str) -> Result<WarehouseMeta>;

    /// Delete a warehouse. If `preserve_bucket` is false the underlying
    /// bucket is also deleted (if empty).
    async fn delete_warehouse(&self, name: &str, preserve_bucket: bool) -> Result<()>;

    // ── Namespaces ────────────────────────────────────────────────────────

    async fn create_namespace(&self, warehouse: &str, ns: Vec<String>, props: HashMap<String, String>) -> Result<()>;

    async fn list_namespaces(&self, warehouse: &str, parent: Option<&str>) -> Result<Vec<Vec<String>>>;

    async fn get_namespace(&self, warehouse: &str, ns: &[String]) -> Result<GetNamespaceResponse>;

    async fn namespace_exists(&self, warehouse: &str, ns: &[String]) -> Result<bool>;

    async fn update_namespace_properties(
        &self,
        warehouse: &str,
        ns: &[String],
        updates: HashMap<String, String>,
        removals: Vec<String>,
    ) -> Result<UpdateNamespacePropertiesResponse>;

    async fn drop_namespace(&self, warehouse: &str, ns: &[String]) -> Result<()>;

    // ── Tables ────────────────────────────────────────────────────────────

    async fn create_table(&self, warehouse: &str, ns: &[String], req: CreateTableRequest) -> Result<LoadTableResult>;

    async fn list_tables(&self, warehouse: &str, ns: &[String]) -> Result<Vec<String>>;

    async fn load_table(&self, warehouse: &str, ns: &[String], table: &str) -> Result<LoadTableResult>;

    async fn table_exists(&self, warehouse: &str, ns: &[String], table: &str) -> Result<bool>;

    async fn commit_table(
        &self,
        warehouse: &str,
        ns: &[String],
        table: &str,
        req: CommitTableRequest,
    ) -> Result<CommitTableResponse>;

    async fn drop_table(&self, warehouse: &str, ns: &[String], table: &str, purge: bool) -> Result<()>;

    async fn rename_table(
        &self,
        warehouse: &str,
        src_ns: &[String],
        src_table: &str,
        dst_ns: &[String],
        dst_table: &str,
    ) -> Result<()>;

    /// Atomically commit changes to multiple tables. All-or-nothing: if any
    /// table commit fails the entire transaction is rolled back (best-effort).
    async fn commit_transaction(&self, warehouse: &str, changes: Vec<CommitTransactionChange>) -> Result<Vec<CommitTableResponse>>;
}

/// Type alias for a shared catalog store instance.
pub type SharedCatalogStore = Arc<dyn CatalogStore>;
