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

//! Optimistic concurrency control (OCC) for table commits.
//!
//! The algorithm:
//! 1. Load current version hint and metadata.
//! 2. Validate all `requirements` against current state.  Fail fast with 409.
//! 3. Apply all `updates` to produce new metadata JSON.
//! 4. Write new metadata JSON at `v{N+1}`.
//! 5. CAS the version hint from N to N+1.
//! 6. If CAS fails, retry from step 1 up to `MAX_RETRIES` times.

use crate::catalog::s3_store::S3CatalogStore;
use crate::error::{Result, TablesError};
use crate::models::{CommitTableRequest, CommitTableResponse, TableRequirement, TableUpdate};
use crate::path;
use tracing::debug;

const MAX_RETRIES: usize = 5;

pub async fn commit_table(
    store: &S3CatalogStore,
    warehouse: &str,
    ns: &[String],
    table: &str,
    req: CommitTableRequest,
) -> Result<CommitTableResponse> {
    for attempt in 0..MAX_RETRIES {
        debug!("commit attempt {}/{} for {table}", attempt + 1, MAX_RETRIES);

        let version = store.read_version_hint(warehouse, ns, table).await?;
        if version == 0 {
            // Check if this is a create-new scenario (AssertTableDoesNotExist).
            let has_assert_not_exist = req
                .requirements
                .iter()
                .any(|r| matches!(r, TableRequirement::AssertTableDoesNotExist));
            if !has_assert_not_exist {
                return Err(TablesError::NotFound(format!("table {table}")));
            }
        }

        let current_metadata = if version > 0 {
            store.read_table_metadata(warehouse, ns, table, version).await?
        } else {
            serde_json::Value::Null
        };

        validate_requirements(&current_metadata, &req.requirements)?;

        let new_version = version + 1;
        let new_metadata = apply_updates(current_metadata, &req.updates)?;

        store
            .write_table_metadata(warehouse, ns, table, new_version, &new_metadata)
            .await?;

        match store
            .cas_version_hint(warehouse, ns, table, version, new_version)
            .await
        {
            Ok(()) => {
                let metadata_location = path::metadata_location(warehouse, ns, table, new_version);
                return Ok(CommitTableResponse {
                    metadata: new_metadata,
                    metadata_location,
                });
            }
            Err(TablesError::CommitConflict(_)) if attempt + 1 < MAX_RETRIES => {
                debug!("CAS conflict on attempt {}, retrying", attempt + 1);
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Err(TablesError::CommitConflict(format!(
        "table {table}: exceeded {MAX_RETRIES} commit retries"
    )))
}

/// Validate all requirements against the current table metadata.
/// Returns `TablesError::CommitConflict` if any requirement is not satisfied.
pub fn validate_requirements(
    metadata: &serde_json::Value,
    requirements: &[TableRequirement],
) -> Result<()> {
    for req in requirements {
        match req {
            TableRequirement::AssertTableDoesNotExist => {
                if !metadata.is_null() {
                    return Err(TablesError::CommitConflict(
                        "assert-table-does-not-exist: table already exists".into(),
                    ));
                }
            }

            TableRequirement::AssertTableUuid { uuid } => {
                let actual = metadata
                    .get("table-uuid")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if actual != uuid.as_str() {
                    return Err(TablesError::CommitConflict(format!(
                        "assert-table-uuid: expected {uuid}, got {actual}"
                    )));
                }
            }

            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                let actual_id = metadata
                    .get("refs")
                    .and_then(|refs| refs.get(r#ref))
                    .and_then(|r| r.get("snapshot-id"))
                    .and_then(|v| v.as_i64());

                match (snapshot_id, actual_id) {
                    (None, None) => {}
                    (Some(expected), Some(actual)) if *expected == actual => {}
                    (Some(expected), actual) => {
                        return Err(TablesError::CommitConflict(format!(
                            "assert-ref-snapshot-id: ref={ref} expected {:?}, got {:?}",
                            expected, actual
                        )));
                    }
                    (None, Some(_)) => {
                        return Err(TablesError::CommitConflict(format!(
                            "assert-ref-snapshot-id: ref={ref} expected null, got a snapshot"
                        )));
                    }
                }
            }

            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id,
            } => {
                let actual = metadata
                    .get("last-column-id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32;
                if actual != *last_assigned_field_id {
                    return Err(TablesError::CommitConflict(format!(
                        "assert-last-assigned-field-id: expected {last_assigned_field_id}, got {actual}"
                    )));
                }
            }

            TableRequirement::AssertCurrentSchemaId { current_schema_id } => {
                let actual = metadata
                    .get("current-schema-id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32;
                if actual != *current_schema_id {
                    return Err(TablesError::CommitConflict(format!(
                        "assert-current-schema-id: expected {current_schema_id}, got {actual}"
                    )));
                }
            }

            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id,
            } => {
                let actual = metadata
                    .get("last-partition-id")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32;
                if actual != *last_assigned_partition_id {
                    return Err(TablesError::CommitConflict(format!(
                        "assert-last-assigned-partition-id: expected {last_assigned_partition_id}, got {actual}"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Apply a sequence of table updates to the current metadata JSON.
pub fn apply_updates(
    mut metadata: serde_json::Value,
    updates: &[TableUpdate],
) -> Result<serde_json::Value> {
    let now_ms = chrono::Utc::now().timestamp_millis();

    if metadata.is_null() {
        metadata = serde_json::json!({
            "format-version": 2,
            "last-updated-ms": now_ms,
            "schemas": [],
            "partition-specs": [],
            "sort-orders": [],
            "snapshots": [],
            "refs": {},
            "properties": {},
        });
    }

    for update in updates {
        match update {
            TableUpdate::AssignUuid { uuid } => {
                metadata["table-uuid"] = serde_json::json!(uuid);
            }

            TableUpdate::UpgradeFormatVersion { format_version } => {
                metadata["format-version"] = serde_json::json!(format_version);
            }

            TableUpdate::AddSchema { schema, last_column_id } => {
                let schemas = metadata["schemas"].as_array_mut().ok_or_else(|| {
                    TablesError::Internal("metadata.schemas is not an array".into())
                })?;
                let next_id = schemas.len() as i64;
                let mut s = schema.clone();
                if s.get("schema-id").is_none() {
                    s["schema-id"] = serde_json::json!(next_id);
                }
                schemas.push(s);
                if let Some(lcid) = last_column_id {
                    metadata["last-column-id"] = serde_json::json!(lcid);
                }
            }

            TableUpdate::SetCurrentSchema { schema_id } => {
                metadata["current-schema-id"] = serde_json::json!(schema_id);
            }

            TableUpdate::AddPartitionSpec { spec } => {
                let specs = metadata["partition-specs"].as_array_mut().ok_or_else(|| {
                    TablesError::Internal("metadata.partition-specs is not an array".into())
                })?;
                specs.push(spec.clone());
            }

            TableUpdate::SetDefaultSpec { spec_id } => {
                metadata["default-spec-id"] = serde_json::json!(spec_id);
            }

            TableUpdate::AddSortOrder { sort_order } => {
                let orders = metadata["sort-orders"].as_array_mut().ok_or_else(|| {
                    TablesError::Internal("metadata.sort-orders is not an array".into())
                })?;
                orders.push(sort_order.clone());
            }

            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                metadata["default-sort-order-id"] = serde_json::json!(sort_order_id);
            }

            TableUpdate::AddSnapshot { snapshot } => {
                let snapshots = metadata["snapshots"].as_array_mut().ok_or_else(|| {
                    TablesError::Internal("metadata.snapshots is not an array".into())
                })?;
                snapshots.push(snapshot.clone());

                // Append to snapshot-log.
                if let Some(snap_id) = snapshot.get("snapshot-id") {
                    let log = metadata["snapshot-log"].as_array_mut();
                    if let Some(log) = log {
                        log.push(serde_json::json!({
                            "snapshot-id": snap_id,
                            "timestamp-ms": snapshot.get("timestamp-ms").cloned().unwrap_or(serde_json::json!(now_ms))
                        }));
                    }
                }
            }

            TableUpdate::SetSnapshotRef {
                ref_name,
                snapshot_id,
                ref_type,
                extra,
            } => {
                let refs = metadata["refs"].as_object_mut().ok_or_else(|| {
                    TablesError::Internal("metadata.refs is not an object".into())
                })?;
                let mut ref_obj = serde_json::json!({
                    "snapshot-id": snapshot_id,
                    "type": ref_type,
                });
                for (k, v) in extra {
                    ref_obj[k] = v.clone();
                }
                refs.insert(ref_name.clone(), ref_obj);

                // If setting main/HEAD, update current-snapshot-id.
                if ref_name == "main" {
                    metadata["current-snapshot-id"] = serde_json::json!(snapshot_id);
                }
            }

            TableUpdate::RemoveSnapshots { snapshot_ids } => {
                let snapshots = metadata["snapshots"].as_array_mut().ok_or_else(|| {
                    TablesError::Internal("metadata.snapshots is not an array".into())
                })?;
                snapshots.retain(|s| {
                    s.get("snapshot-id")
                        .and_then(|v| v.as_i64())
                        .map(|id| !snapshot_ids.contains(&id))
                        .unwrap_or(true)
                });
            }

            TableUpdate::RemoveSnapshotRef { ref_name } => {
                if let Some(refs) = metadata["refs"].as_object_mut() {
                    refs.remove(ref_name);
                }
            }

            TableUpdate::SetProperties { updates } => {
                let props = metadata["properties"].as_object_mut().ok_or_else(|| {
                    TablesError::Internal("metadata.properties is not an object".into())
                })?;
                for (k, v) in updates {
                    props.insert(k.clone(), serde_json::json!(v));
                }
            }

            TableUpdate::RemoveProperties { removals } => {
                if let Some(props) = metadata["properties"].as_object_mut() {
                    for key in removals {
                        props.remove(key);
                    }
                }
            }

            TableUpdate::SetLocation { location } => {
                metadata["location"] = serde_json::json!(location);
            }
        }
    }

    metadata["last-updated-ms"] = serde_json::json!(now_ms);
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TableRequirement;

    fn make_metadata(uuid: &str, schema_id: i32, last_field_id: i32, last_partition_id: i32) -> serde_json::Value {
        serde_json::json!({
            "format-version": 2,
            "table-uuid": uuid,
            "current-schema-id": schema_id,
            "last-column-id": last_field_id,
            "last-partition-id": last_partition_id,
            "refs": {},
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "properties": {},
            "partition-specs": [],
            "sort-orders": [],
        })
    }

    #[test]
    fn test_assert_table_does_not_exist_passes_on_null() {
        let meta = serde_json::Value::Null;
        let reqs = vec![TableRequirement::AssertTableDoesNotExist];
        assert!(validate_requirements(&meta, &reqs).is_ok());
    }

    #[test]
    fn test_assert_table_does_not_exist_fails_when_exists() {
        let meta = make_metadata("abc", 0, 0, 0);
        let reqs = vec![TableRequirement::AssertTableDoesNotExist];
        assert!(matches!(
            validate_requirements(&meta, &reqs),
            Err(TablesError::CommitConflict(_))
        ));
    }

    #[test]
    fn test_assert_table_uuid_passes() {
        let meta = make_metadata("my-uuid", 0, 0, 0);
        let reqs = vec![TableRequirement::AssertTableUuid {
            uuid: "my-uuid".into(),
        }];
        assert!(validate_requirements(&meta, &reqs).is_ok());
    }

    #[test]
    fn test_assert_table_uuid_fails() {
        let meta = make_metadata("wrong-uuid", 0, 0, 0);
        let reqs = vec![TableRequirement::AssertTableUuid {
            uuid: "my-uuid".into(),
        }];
        assert!(matches!(
            validate_requirements(&meta, &reqs),
            Err(TablesError::CommitConflict(_))
        ));
    }

    #[test]
    fn test_assert_current_schema_id_passes() {
        let meta = make_metadata("u", 3, 0, 0);
        let reqs = vec![TableRequirement::AssertCurrentSchemaId { current_schema_id: 3 }];
        assert!(validate_requirements(&meta, &reqs).is_ok());
    }

    #[test]
    fn test_assert_current_schema_id_fails() {
        let meta = make_metadata("u", 3, 0, 0);
        let reqs = vec![TableRequirement::AssertCurrentSchemaId { current_schema_id: 2 }];
        assert!(matches!(
            validate_requirements(&meta, &reqs),
            Err(TablesError::CommitConflict(_))
        ));
    }

    #[test]
    fn test_assert_last_assigned_field_id_passes() {
        let meta = make_metadata("u", 0, 5, 0);
        let reqs = vec![TableRequirement::AssertLastAssignedFieldId {
            last_assigned_field_id: 5,
        }];
        assert!(validate_requirements(&meta, &reqs).is_ok());
    }

    #[test]
    fn test_assert_last_partition_id_passes() {
        let meta = make_metadata("u", 0, 0, 999);
        let reqs = vec![TableRequirement::AssertLastAssignedPartitionId {
            last_assigned_partition_id: 999,
        }];
        assert!(validate_requirements(&meta, &reqs).is_ok());
    }

    #[test]
    fn test_apply_set_properties() {
        let meta = serde_json::json!({
            "properties": {},
            "schemas": [],
            "partition-specs": [],
            "sort-orders": [],
            "snapshots": [],
            "snapshot-log": [],
            "refs": {},
        });
        let updates = vec![TableUpdate::SetProperties {
            updates: [("key".to_string(), "value".to_string())].into(),
        }];
        let result = apply_updates(meta, &updates).unwrap();
        assert_eq!(result["properties"]["key"], "value");
    }

    #[test]
    fn test_apply_add_snapshot_and_set_ref() {
        let meta = serde_json::json!({
            "properties": {},
            "schemas": [],
            "partition-specs": [],
            "sort-orders": [],
            "snapshots": [],
            "snapshot-log": [],
            "refs": {},
            "current-snapshot-id": serde_json::Value::Null,
        });
        let snapshot_id: i64 = 12345;
        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: serde_json::json!({
                    "snapshot-id": snapshot_id,
                    "timestamp-ms": 1000,
                    "manifest-list": "s3://bucket/snap.avro",
                }),
            },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".into(),
                snapshot_id,
                ref_type: "branch".into(),
                extra: Default::default(),
            },
        ];
        let result = apply_updates(meta, &updates).unwrap();
        assert_eq!(result["snapshots"].as_array().unwrap().len(), 1);
        assert_eq!(result["current-snapshot-id"], snapshot_id);
        assert_eq!(result["refs"]["main"]["snapshot-id"], snapshot_id);
    }

    #[test]
    fn test_apply_remove_properties() {
        let meta = serde_json::json!({
            "properties": {"a": "1", "b": "2"},
            "schemas": [], "partition-specs": [], "sort-orders": [],
            "snapshots": [], "snapshot-log": [], "refs": {},
        });
        let updates = vec![TableUpdate::RemoveProperties {
            removals: vec!["a".to_string()],
        }];
        let result = apply_updates(meta, &updates).unwrap();
        assert!(result["properties"].get("a").is_none());
        assert_eq!(result["properties"]["b"], "2");
    }

    #[test]
    fn test_apply_updates_on_null_creates_skeleton() {
        let updates = vec![TableUpdate::AssignUuid {
            uuid: "new-uuid".into(),
        }];
        let result = apply_updates(serde_json::Value::Null, &updates).unwrap();
        assert_eq!(result["table-uuid"], "new-uuid");
        assert!(result.get("format-version").is_some());
    }
}
