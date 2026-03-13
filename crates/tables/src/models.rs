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

//! Request / response types for the Iceberg REST Catalog API.
//!
//! JSON shapes match the MinIO AIStor Tables API Reference exactly,
//! which in turn follows the Apache Iceberg REST Catalog OpenAPI spec.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Error response ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergErrorBody {
    pub code: u16,
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

/// `{"error": {"code": N, "type": "...", "message": "..."}}`
#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergErrorResponse {
    pub error: IcebergErrorBody,
}

impl IcebergErrorResponse {
    pub fn new(code: u16, error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: IcebergErrorBody {
                code,
                error_type: error_type.into(),
                message: message.into(),
            },
        }
    }
}

// ─── Warehouse ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateWarehouseRequest {
    pub name: String,
    #[serde(rename = "upgrade-existing", default)]
    pub upgrade_existing: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateWarehouseResponse {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WarehouseMeta {
    pub name: String,
    pub bucket: String,
    pub uuid: String,
    #[serde(rename = "created-at")]
    pub created_at: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListWarehousesResponse {
    pub warehouses: Vec<String>,
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogConfig {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}

// ─── Namespaces ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNamespaceRequest {
    pub namespace: Vec<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateNamespacePropertiesRequest {
    #[serde(default)]
    pub updates: HashMap<String, String>,
    #[serde(default)]
    pub removals: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateNamespacePropertiesResponse {
    pub updated: Vec<String>,
    pub removed: Vec<String>,
    pub missing: Vec<String>,
}

// ─── Tables ──────────────────────────────────────────────────────────────────

/// Minimal Iceberg schema representation for the REST API.
/// We pass this through as raw JSON so that the `iceberg` crate can deserialize
/// it into proper `iceberg::spec::Schema` when needed.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RawSchema(pub serde_json::Value);

/// Minimal partition spec for the REST API (pass-through).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RawPartitionSpec(pub serde_json::Value);

/// Minimal sort order for the REST API (pass-through).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RawSortOrder(pub serde_json::Value);

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub schema: serde_json::Value,
    #[serde(rename = "partition-spec", skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<serde_json::Value>,
    #[serde(rename = "write-order", skip_serializing_if = "Option::is_none")]
    pub write_order: Option<serde_json::Value>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    #[serde(rename = "stage-create", default)]
    pub stage_create: bool,
}

/// Returned from CreateTable, LoadTable, and CommitTable.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoadTableResult {
    /// Full Iceberg TableMetadata JSON.
    pub metadata: serde_json::Value,
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    #[serde(default)]
    pub config: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifier>,
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

// ─── Commit (OCC) ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitTableRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdentifier>,
    #[serde(default)]
    pub requirements: Vec<TableRequirement>,
    #[serde(default)]
    pub updates: Vec<TableUpdate>,
}

/// Commit precondition — validated against current metadata before applying updates.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    AssertTableDoesNotExist,
    AssertTableUuid {
        uuid: String,
    },
    AssertRefSnapshotId {
        #[serde(rename = "ref")]
        r#ref: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    AssertLastAssignedFieldId {
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32,
    },
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32,
    },
    AssertLastAssignedPartitionId {
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i32,
    },
}

/// State mutation applied atomically after requirements pass.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    AssignUuid {
        uuid: String,
    },
    UpgradeFormatVersion {
        #[serde(rename = "format-version")]
        format_version: i32,
    },
    AddSchema {
        schema: serde_json::Value,
        #[serde(rename = "last-column-id", skip_serializing_if = "Option::is_none")]
        last_column_id: Option<i32>,
    },
    SetCurrentSchema {
        #[serde(rename = "schema-id")]
        schema_id: i32,
    },
    AddPartitionSpec {
        spec: serde_json::Value,
    },
    SetDefaultSpec {
        #[serde(rename = "spec-id")]
        spec_id: i32,
    },
    AddSortOrder {
        #[serde(rename = "sort-order")]
        sort_order: serde_json::Value,
    },
    SetDefaultSortOrder {
        #[serde(rename = "sort-order-id")]
        sort_order_id: i32,
    },
    AddSnapshot {
        snapshot: serde_json::Value,
    },
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "type")]
        ref_type: String,
        #[serde(flatten)]
        extra: HashMap<String, serde_json::Value>,
    },
    RemoveSnapshots {
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },
    RemoveSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
    },
    SetProperties {
        updates: HashMap<String, String>,
    },
    RemoveProperties {
        removals: Vec<String>,
    },
    SetLocation {
        location: String,
    },
}

/// Response from a successful single-table commit.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommitTableResponse {
    pub metadata: serde_json::Value,
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
}

// ─── Rename ──────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct RenameTableRequest {
    pub source: TableIdentifier,
    pub destination: TableIdentifier,
}

// ─── Multi-table transaction ─────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitTransactionRequest {
    #[serde(rename = "table-changes")]
    pub table_changes: Vec<CommitTransactionChange>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitTransactionChange {
    pub identifier: TableIdentifier,
    #[serde(default)]
    pub requirements: Vec<TableRequirement>,
    #[serde(default)]
    pub updates: Vec<TableUpdate>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_response_roundtrip() {
        let r = IcebergErrorResponse::new(409, "CommitFailedException", "conflict");
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"code\":409"));
        assert!(json.contains("\"type\":\"CommitFailedException\""));
        let back: IcebergErrorResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.error.code, 409);
        assert_eq!(back.error.error_type, "CommitFailedException");
    }

    #[test]
    fn test_create_warehouse_request_roundtrip() {
        let json = r#"{"name":"analytics","upgrade-existing":false}"#;
        let req: CreateWarehouseRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "analytics");
        assert!(!req.upgrade_existing);
    }

    #[test]
    fn test_create_namespace_request_roundtrip() {
        let json = r#"{"namespace":["data_science"],"properties":{"owner":"team"}}"#;
        let req: CreateNamespaceRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.namespace, vec!["data_science"]);
        assert_eq!(req.properties.get("owner").unwrap(), "team");
    }

    #[test]
    fn test_table_requirement_serde() {
        let req = TableRequirement::AssertTableUuid {
            uuid: "abc-123".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("assert-table-uuid"));
        let back: TableRequirement = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, TableRequirement::AssertTableUuid { .. }));
    }

    #[test]
    fn test_table_update_add_snapshot_serde() {
        let update = TableUpdate::AddSnapshot {
            snapshot: serde_json::json!({"snapshot-id": 1234, "manifest-list": "s3://bucket/snap.avro"}),
        };
        let json = serde_json::to_string(&update).unwrap();
        assert!(json.contains("\"action\":\"add-snapshot\""));
    }

    #[test]
    fn test_set_properties_serde() {
        let update = TableUpdate::SetProperties {
            updates: [("key".to_string(), "val".to_string())].into(),
        };
        let json = serde_json::to_string(&update).unwrap();
        assert!(json.contains("set-properties"));
        let back: TableUpdate = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, TableUpdate::SetProperties { .. }));
    }

    #[test]
    fn test_rename_table_request_roundtrip() {
        let json = r#"{"source":{"namespace":["ns1"],"name":"t1"},"destination":{"namespace":["ns2"],"name":"t2"}}"#;
        let req: RenameTableRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.source.name, "t1");
        assert_eq!(req.destination.namespace, vec!["ns2"]);
    }

    #[test]
    fn test_load_table_result_roundtrip() {
        let result = LoadTableResult {
            metadata: serde_json::json!({"format-version": 2, "table-uuid": "abc"}),
            metadata_location: "s3://bucket/.rustfs-tables/ns/t/metadata/v1.metadata.json".into(),
            config: HashMap::new(),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("metadata-location"));
        let back: LoadTableResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.metadata_location, result.metadata_location);
    }
}
