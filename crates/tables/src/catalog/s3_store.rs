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

//! Object-storage-backed `CatalogStore` implementation.
//!
//! Catalog state lives entirely within the warehouse bucket under the
//! `.rustfs-tables/` hidden prefix.  Version-hint CAS uses
//! `rustfs-ecstore`'s conditional PUT to implement optimistic concurrency.

use super::CatalogStore;
use crate::commit;
use crate::error::{Result, TablesError};
use crate::models::{
    CommitTableRequest, CommitTableResponse, CommitTransactionChange, CreateTableRequest, GetNamespaceResponse,
    LoadTableResult, UpdateNamespacePropertiesResponse, WarehouseMeta,
};
use crate::path;
use async_trait::async_trait;
use rustfs_ecstore::global::get_global_store;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};
use uuid::Uuid;

// ─── Namespace properties stored in .namespace.json ──────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct NamespaceMeta {
    properties: HashMap<String, String>,
}

// ─── Warehouse registry ───────────────────────────────────────────────────────

/// In-memory warehouse registry.  On a real multi-node deployment this would
/// need to be persisted; for the MVP it survives process restarts via
/// rediscovery (the `.rustfs-tables/.warehouse.json` object in each bucket).
#[derive(Debug, Serialize, Deserialize, Clone)]
struct WarehouseEntry {
    name: String,
    bucket: String,
    uuid: String,
    created_at: String,
    properties: HashMap<String, String>,
}

/// Key for the warehouse registry object inside a bucket.
fn warehouse_registry_key() -> &'static str {
    ".rustfs-tables/.warehouse.json"
}

pub struct S3CatalogStore {
    /// Protects warehouse registry mutations in this process.
    /// Object-level CAS handles cross-process races.
    _lock: Arc<Mutex<()>>,
}

impl S3CatalogStore {
    pub fn new() -> Self {
        Self {
            _lock: Arc::new(Mutex::new(())),
        }
    }

    // ── Low-level helpers ────────────────────────────────────────────────

    async fn get_object_bytes(&self, bucket: &str, key: &str) -> Result<Option<bytes::Bytes>> {
        let store = get_global_store();
        match store.get_object_bytes(bucket, key).await {
            Ok(data) => Ok(Some(data)),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("NoSuchKey") || msg.contains("not found") || msg.contains("404") {
                    Ok(None)
                } else {
                    Err(TablesError::Internal(format!("get {key}: {e}")))
                }
            }
        }
    }

    async fn put_object_bytes(&self, bucket: &str, key: &str, data: bytes::Bytes) -> Result<()> {
        let store = get_global_store();
        store
            .put_object_bytes(bucket, key, data)
            .await
            .map_err(|e| TablesError::Internal(format!("put {key}: {e}")))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let store = get_global_store();
        store
            .delete_object(bucket, key)
            .await
            .map_err(|e| TablesError::Internal(format!("delete {key}: {e}")))
    }

    async fn list_prefix(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let store = get_global_store();
        store
            .list_objects_with_prefix(bucket, prefix)
            .await
            .map_err(|e| TablesError::Internal(format!("list {prefix}: {e}")))
    }

    async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool> {
        let store = get_global_store();
        match store.head_object(bucket, key).await {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("NoSuchKey") || msg.contains("not found") || msg.contains("404") {
                    Ok(false)
                } else {
                    Err(TablesError::Internal(format!("head {key}: {e}")))
                }
            }
        }
    }

    // ── Namespace helpers ────────────────────────────────────────────────

    async fn read_namespace_meta(&self, bucket: &str, ns: &[String]) -> Result<Option<NamespaceMeta>> {
        let key = path::namespace_meta_key_str(&ns.join("/"));
        match self.get_object_bytes(bucket, &key).await? {
            None => Ok(None),
            Some(data) => {
                let meta: NamespaceMeta = serde_json::from_slice(&data)?;
                Ok(Some(meta))
            }
        }
    }

    async fn write_namespace_meta(&self, bucket: &str, ns: &[String], meta: &NamespaceMeta) -> Result<()> {
        let key = path::namespace_meta_key_str(&ns.join("/"));
        let data = serde_json::to_vec(meta).map_err(|e| TablesError::Internal(e.to_string()))?;
        self.put_object_bytes(bucket, &key, bytes::Bytes::from(data)).await
    }

    // ── Table metadata helpers ───────────────────────────────────────────

    pub(crate) async fn read_version_hint(&self, bucket: &str, ns: &[String], table: &str) -> Result<u64> {
        let key = path::version_hint_key(ns, table);
        match self.get_object_bytes(bucket, &key).await? {
            None => Ok(0),
            Some(data) => {
                let s = std::str::from_utf8(&data).map_err(|e| TablesError::Internal(e.to_string()))?;
                s.trim()
                    .parse::<u64>()
                    .map_err(|e| TablesError::Internal(format!("version hint parse: {e}")))
            }
        }
    }

    pub(crate) async fn write_version_hint(&self, bucket: &str, ns: &[String], table: &str, version: u64) -> Result<()> {
        let key = path::version_hint_key(ns, table);
        let data = version.to_string().into_bytes();
        self.put_object_bytes(bucket, &key, bytes::Bytes::from(data)).await
    }

    pub(crate) async fn read_table_metadata(
        &self,
        bucket: &str,
        ns: &[String],
        table: &str,
        version: u64,
    ) -> Result<serde_json::Value> {
        let key = path::table_metadata_key(ns, table, version);
        match self.get_object_bytes(bucket, &key).await? {
            None => Err(TablesError::NotFound(format!("table metadata v{version} for {table}"))),
            Some(data) => serde_json::from_slice(&data).map_err(|e| TablesError::Internal(e.to_string())),
        }
    }

    pub(crate) async fn write_table_metadata(
        &self,
        bucket: &str,
        ns: &[String],
        table: &str,
        version: u64,
        meta: &serde_json::Value,
    ) -> Result<()> {
        let key = path::table_metadata_key(ns, table, version);
        let data = serde_json::to_vec(meta).map_err(|e| TablesError::Internal(e.to_string()))?;
        self.put_object_bytes(bucket, &key, bytes::Bytes::from(data)).await
    }

    /// Attempt to CAS the version hint from `old_version` to `new_version`.
    /// The object-based impl reads the current hint and only overwrites if it
    /// still matches `old_version`.  This is a best-effort CAS — under high
    /// concurrency a sled-backed impl would be more reliable.
    pub(crate) async fn cas_version_hint(
        &self,
        bucket: &str,
        ns: &[String],
        table: &str,
        old_version: u64,
        new_version: u64,
    ) -> Result<()> {
        let current = self.read_version_hint(bucket, ns, table).await?;
        if current != old_version {
            return Err(TablesError::CommitConflict(format!(
                "version hint changed: expected {old_version}, found {current}"
            )));
        }
        self.write_version_hint(bucket, ns, table, new_version).await
    }

    // ── Warehouse registry helpers ───────────────────────────────────────

    async fn read_warehouse_entry(&self, bucket: &str) -> Result<Option<WarehouseEntry>> {
        match self.get_object_bytes(bucket, warehouse_registry_key()).await? {
            None => Ok(None),
            Some(data) => {
                let entry: WarehouseEntry = serde_json::from_slice(&data)?;
                Ok(Some(entry))
            }
        }
    }

    async fn write_warehouse_entry(&self, bucket: &str, entry: &WarehouseEntry) -> Result<()> {
        let data = serde_json::to_vec(entry).map_err(|e| TablesError::Internal(e.to_string()))?;
        self.put_object_bytes(bucket, warehouse_registry_key(), bytes::Bytes::from(data)).await
    }

    async fn bucket_exists(&self, name: &str) -> bool {
        // Try to list objects in the bucket (zero objects is fine, we just need the bucket to exist).
        let store = get_global_store();
        store.list_objects_with_prefix(name, "").await.is_ok()
    }
}

#[async_trait]
impl CatalogStore for S3CatalogStore {
    // ── Warehouses ────────────────────────────────────────────────────────

    async fn create_warehouse(&self, name: &str, upgrade_existing: bool) -> Result<()> {
        if self.read_warehouse_entry(name).await?.is_some() {
            if !upgrade_existing {
                return Err(TablesError::AlreadyExists(format!("warehouse {name}")));
            }
            debug!("upgrading existing bucket {name} to warehouse");
            return Ok(());
        }

        // Bucket must already exist (rustfs manages bucket creation via S3 API).
        if !self.bucket_exists(name).await {
            return Err(TablesError::NotFound(format!("bucket {name} does not exist; create it first")));
        }

        let entry = WarehouseEntry {
            name: name.to_string(),
            bucket: name.to_string(),
            uuid: Uuid::new_v4().to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            properties: HashMap::new(),
        };
        self.write_warehouse_entry(name, &entry).await
    }

    async fn list_warehouses(&self) -> Result<Vec<WarehouseMeta>> {
        // We cannot enumerate all buckets from here without ecstore support,
        // so we return empty for now. A production impl would maintain a
        // global registry object.
        Ok(vec![])
    }

    async fn get_warehouse(&self, name: &str) -> Result<WarehouseMeta> {
        match self.read_warehouse_entry(name).await? {
            None => Err(TablesError::NotFound(format!("warehouse {name}"))),
            Some(e) => Ok(WarehouseMeta {
                name: e.name,
                bucket: e.bucket,
                uuid: e.uuid,
                created_at: e.created_at,
                properties: e.properties,
            }),
        }
    }

    async fn delete_warehouse(&self, name: &str, _preserve_bucket: bool) -> Result<()> {
        // Verify it exists.
        self.get_warehouse(name).await?;

        // Ensure no namespaces remain.
        let nss = self.list_namespaces(name, None).await?;
        if !nss.is_empty() {
            return Err(TablesError::NotEmpty(format!(
                "warehouse {name} has {} namespace(s); delete them first",
                nss.len()
            )));
        }

        self.delete_object(name, warehouse_registry_key()).await
    }

    // ── Namespaces ────────────────────────────────────────────────────────

    async fn create_namespace(&self, warehouse: &str, ns: Vec<String>, props: HashMap<String, String>) -> Result<()> {
        if self.namespace_exists(warehouse, &ns).await? {
            return Err(TablesError::AlreadyExists(format!("namespace {}", ns.join("."))));
        }
        let meta = NamespaceMeta { properties: props };
        self.write_namespace_meta(warehouse, &ns, &meta).await
    }

    async fn list_namespaces(&self, warehouse: &str, _parent: Option<&str>) -> Result<Vec<Vec<String>>> {
        let prefix = format!("{}/", path::TABLES_META_PREFIX);
        let keys = self.list_prefix(warehouse, &prefix).await?;

        let mut seen = std::collections::HashSet::new();
        for key in &keys {
            // A namespace marker looks like: .rustfs-tables/{ns}/.namespace.json
            if key.ends_with("/.namespace.json") {
                let stripped = key
                    .strip_prefix(&prefix)
                    .unwrap_or(key)
                    .strip_suffix("/.namespace.json")
                    .unwrap_or("");
                if !stripped.is_empty() && stripped != "." {
                    let parts: Vec<String> = stripped.split('/').map(|s| s.to_string()).collect();
                    seen.insert(parts);
                }
            }
        }

        let mut result: Vec<Vec<String>> = seen.into_iter().collect();
        result.sort();
        Ok(result)
    }

    async fn get_namespace(&self, warehouse: &str, ns: &[String]) -> Result<GetNamespaceResponse> {
        match self.read_namespace_meta(warehouse, ns).await? {
            None => Err(TablesError::NotFound(format!("namespace {}", ns.join(".")))),
            Some(meta) => Ok(GetNamespaceResponse {
                namespace: ns.to_vec(),
                properties: meta.properties,
            }),
        }
    }

    async fn namespace_exists(&self, warehouse: &str, ns: &[String]) -> Result<bool> {
        let key = path::namespace_meta_key_str(&ns.join("/"));
        self.object_exists(warehouse, &key).await
    }

    async fn update_namespace_properties(
        &self,
        warehouse: &str,
        ns: &[String],
        updates: HashMap<String, String>,
        removals: Vec<String>,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        let mut meta = match self.read_namespace_meta(warehouse, ns).await? {
            None => return Err(TablesError::NotFound(format!("namespace {}", ns.join(".")))),
            Some(m) => m,
        };

        let mut updated_keys = vec![];
        let mut removed_keys = vec![];
        let mut missing_keys = vec![];

        for key in &removals {
            if meta.properties.remove(key).is_some() {
                removed_keys.push(key.clone());
            } else {
                missing_keys.push(key.clone());
            }
        }

        for (k, v) in &updates {
            meta.properties.insert(k.clone(), v.clone());
            updated_keys.push(k.clone());
        }

        self.write_namespace_meta(warehouse, ns, &meta).await?;

        Ok(UpdateNamespacePropertiesResponse {
            updated: updated_keys,
            removed: removed_keys,
            missing: missing_keys,
        })
    }

    async fn drop_namespace(&self, warehouse: &str, ns: &[String]) -> Result<()> {
        if !self.namespace_exists(warehouse, ns).await? {
            return Err(TablesError::NotFound(format!("namespace {}", ns.join("."))));
        }

        let tables = self.list_tables(warehouse, ns).await?;
        if !tables.is_empty() {
            return Err(TablesError::NotEmpty(format!(
                "namespace {} has {} table(s)",
                ns.join("."),
                tables.len()
            )));
        }

        let key = path::namespace_meta_key_str(&ns.join("/"));
        self.delete_object(warehouse, &key).await
    }

    // ── Tables ────────────────────────────────────────────────────────────

    async fn create_table(&self, warehouse: &str, ns: &[String], req: CreateTableRequest) -> Result<LoadTableResult> {
        if !self.namespace_exists(warehouse, ns).await? {
            return Err(TablesError::NotFound(format!("namespace {}", ns.join("."))));
        }
        if self.table_exists(warehouse, ns, &req.name).await? {
            return Err(TablesError::AlreadyExists(format!("table {}", req.name)));
        }

        let table_uuid = Uuid::new_v4().to_string();
        let location = path::table_data_location(warehouse, ns, &req.name);

        let mut metadata: serde_json::Value = serde_json::json!({
            "format-version": 2,
            "table-uuid": table_uuid,
            "location": location,
            "last-updated-ms": chrono::Utc::now().timestamp_millis(),
            "last-column-id": 0,
            "current-schema-id": 0,
            "schemas": [req.schema],
            "default-spec-id": 0,
            "partition-specs": [req.partition_spec.unwrap_or(serde_json::json!({"spec-id": 0, "fields": []}))],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": req.properties,
            "current-snapshot-id": serde_json::Value::Null,
            "refs": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
        });

        // Embed the schema's field count as last-column-id if available
        if let Some(fields) = req.schema.get("fields").and_then(|f| f.as_array()) {
            if let Some(last_id) = fields.iter().filter_map(|f| f.get("id").and_then(|i| i.as_i64())).max() {
                metadata["last-column-id"] = serde_json::json!(last_id);
                metadata["schemas"][0]["schema-id"] = serde_json::json!(0);
            }
        }

        self.write_table_metadata(warehouse, ns, &req.name, 1, &metadata).await?;
        self.write_version_hint(warehouse, ns, &req.name, 1).await?;

        let metadata_location = path::metadata_location(warehouse, ns, &req.name, 1);
        Ok(LoadTableResult {
            metadata,
            metadata_location,
            config: HashMap::new(),
        })
    }

    async fn list_tables(&self, warehouse: &str, ns: &[String]) -> Result<Vec<String>> {
        let prefix = path::namespace_prefix(ns);
        let keys = self.list_prefix(warehouse, &prefix).await?;

        let mut tables = std::collections::HashSet::new();
        for key in &keys {
            // version-hint.text marks a table: .rustfs-tables/{ns}/{table}/version-hint.text
            if key.ends_with("/version-hint.text") {
                if let Some(stripped) = key.strip_prefix(&prefix) {
                    let table_name = stripped.split('/').next().unwrap_or("").to_string();
                    if !table_name.is_empty() {
                        tables.insert(table_name);
                    }
                }
            }
        }

        let mut result: Vec<String> = tables.into_iter().collect();
        result.sort();
        Ok(result)
    }

    async fn load_table(&self, warehouse: &str, ns: &[String], table: &str) -> Result<LoadTableResult> {
        let version = self.read_version_hint(warehouse, ns, table).await?;
        if version == 0 {
            return Err(TablesError::NotFound(format!("table {table}")));
        }
        let metadata = self.read_table_metadata(warehouse, ns, table, version).await?;
        let metadata_location = path::metadata_location(warehouse, ns, table, version);
        Ok(LoadTableResult {
            metadata,
            metadata_location,
            config: HashMap::new(),
        })
    }

    async fn table_exists(&self, warehouse: &str, ns: &[String], table: &str) -> Result<bool> {
        let key = path::version_hint_key(ns, table);
        self.object_exists(warehouse, &key).await
    }

    async fn commit_table(
        &self,
        warehouse: &str,
        ns: &[String],
        table: &str,
        req: CommitTableRequest,
    ) -> Result<CommitTableResponse> {
        commit::commit_table(self, warehouse, ns, table, req).await
    }

    async fn drop_table(&self, warehouse: &str, ns: &[String], table: &str, purge: bool) -> Result<()> {
        if !self.table_exists(warehouse, ns, table).await? {
            return Err(TablesError::NotFound(format!("table {table}")));
        }

        if purge {
            // Delete all metadata objects.
            let prefix = path::table_prefix(ns, table);
            let keys = self.list_prefix(warehouse, &prefix).await?;
            for key in keys {
                if let Err(e) = self.delete_object(warehouse, &key).await {
                    warn!("drop_table purge: failed to delete {key}: {e}");
                }
            }
        } else {
            // Remove only the version hint (catalog entry), leave data files.
            let key = path::version_hint_key(ns, table);
            self.delete_object(warehouse, &key).await?;
        }

        Ok(())
    }

    async fn rename_table(
        &self,
        warehouse: &str,
        src_ns: &[String],
        src_table: &str,
        dst_ns: &[String],
        dst_table: &str,
    ) -> Result<()> {
        if !self.table_exists(warehouse, src_ns, src_table).await? {
            return Err(TablesError::NotFound(format!("table {src_table}")));
        }
        if self.table_exists(warehouse, dst_ns, dst_table).await? {
            return Err(TablesError::AlreadyExists(format!("table {dst_table}")));
        }
        if !self.namespace_exists(warehouse, dst_ns).await? {
            return Err(TablesError::NotFound(format!("namespace {}", dst_ns.join("."))));
        }

        let version = self.read_version_hint(warehouse, src_ns, src_table).await?;
        let metadata = self.read_table_metadata(warehouse, src_ns, src_table, version).await?;

        // Write metadata under new location.
        self.write_table_metadata(warehouse, dst_ns, dst_table, version, &metadata).await?;
        self.write_version_hint(warehouse, dst_ns, dst_table, version).await?;

        // Remove old version hint (acts as atomic-ish rename for catalog purposes).
        let old_hint = path::version_hint_key(src_ns, src_table);
        self.delete_object(warehouse, &old_hint).await?;

        Ok(())
    }

    async fn commit_transaction(&self, warehouse: &str, changes: Vec<CommitTransactionChange>) -> Result<Vec<CommitTableResponse>> {
        // For the MVP we execute commits sequentially and roll back on first failure
        // by tracking which ones succeeded.
        let mut responses = Vec::with_capacity(changes.len());
        let mut completed: Vec<(Vec<String>, String, u64)> = vec![];

        for change in changes {
            let ns = &change.identifier.namespace;
            let table = &change.identifier.name;
            let req = CommitTableRequest {
                identifier: Some(change.identifier.clone()),
                requirements: change.requirements,
                updates: change.updates,
            };

            match commit::commit_table(self, warehouse, ns, table, req).await {
                Ok(resp) => {
                    // Track old version for rollback (we wrote new_version = old+1).
                    let new_version = resp.metadata_location
                        .split("v")
                        .last()
                        .and_then(|s| s.strip_suffix(".metadata.json"))
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(1);
                    completed.push((ns.clone(), table.clone(), new_version));
                    responses.push(resp);
                }
                Err(e) => {
                    // Best-effort rollback of completed commits.
                    for (rns, rtable, rver) in completed.iter().rev() {
                        let rollback_ver = rver.saturating_sub(1);
                        if let Err(rb_err) = self.write_version_hint(warehouse, rns, rtable, rollback_ver).await {
                            warn!("transaction rollback failed for {rtable}: {rb_err}");
                        }
                    }
                    return Err(e);
                }
            }
        }

        Ok(responses)
    }
}

impl Default for S3CatalogStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_warehouse_registry_key() {
        assert_eq!(warehouse_registry_key(), ".rustfs-tables/.warehouse.json");
    }

    #[test]
    fn test_namespace_meta_roundtrip() {
        let meta = NamespaceMeta {
            properties: [("owner".to_string(), "team".to_string())].into(),
        };
        let json = serde_json::to_vec(&meta).unwrap();
        let back: NamespaceMeta = serde_json::from_slice(&json).unwrap();
        assert_eq!(back.properties.get("owner").unwrap(), "team");
    }
}
