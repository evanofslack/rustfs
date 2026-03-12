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

//! File-based persistence for batch jobs.
//!
//! Layout under `.rustfs.sys/config/batch-jobs/{job-id}/`:
//!   - `job.json`         — job metadata and progress counters
//!   - `definition.yaml`  — original YAML submitted by the user
//!   - `failures.jsonl`   — append-only failure log (one JSON object per line)

use crate::error::{BatchError, Result};
use crate::job::BatchJob;
use rustfs_ecstore::config::com::{read_config, save_config};
use rustfs_ecstore::store_api::StorageAPI;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

const BATCH_JOBS_PREFIX: &str = "config/batch-jobs";

fn job_json_path(job_id: &str) -> String {
    format!("{BATCH_JOBS_PREFIX}/{job_id}/job.json")
}

fn definition_yaml_path(job_id: &str) -> String {
    format!("{BATCH_JOBS_PREFIX}/{job_id}/definition.yaml")
}

fn failures_jsonl_path(job_id: &str) -> String {
    format!("{BATCH_JOBS_PREFIX}/{job_id}/failures.jsonl")
}

/// A single failure record appended to `failures.jsonl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureRecord {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    pub error: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub size: i64,
}

pub struct BatchStore<S: StorageAPI> {
    store: Arc<S>,
}

impl<S: StorageAPI> BatchStore<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    /// Persist job metadata to `job.json`.
    pub async fn save_job(&self, job: &BatchJob) -> Result<()> {
        let data = serde_json::to_vec(job).map_err(BatchError::Json)?;
        save_config(self.store.clone(), &job_json_path(&job.id), data)
            .await
            .map_err(|e| BatchError::Storage(e.to_string()))
    }

    /// Read job metadata from `job.json`.
    pub async fn load_job(&self, job_id: &str) -> Result<BatchJob> {
        let data = read_config(self.store.clone(), &job_json_path(job_id))
            .await
            .map_err(|e| BatchError::Storage(e.to_string()))?;
        serde_json::from_slice(&data).map_err(BatchError::Json)
    }

    /// Save the original YAML definition.
    pub async fn save_definition(&self, job_id: &str, yaml: &str) -> Result<()> {
        save_config(self.store.clone(), &definition_yaml_path(job_id), yaml.as_bytes().to_vec())
            .await
            .map_err(|e| BatchError::Storage(e.to_string()))
    }

    /// Read back the original YAML definition.
    pub async fn load_definition(&self, job_id: &str) -> Result<String> {
        let data = read_config(self.store.clone(), &definition_yaml_path(job_id))
            .await
            .map_err(|e| BatchError::Storage(e.to_string()))?;
        String::from_utf8(data).map_err(|e| BatchError::Storage(e.to_string()))
    }

    /// Append a failure record to `failures.jsonl`.
    ///
    /// Reads existing content, appends the new line, and writes back.
    /// This is safe for low-frequency appends (failures are rare; <1% of objects typically).
    pub async fn append_failure(&self, job_id: &str, record: &FailureRecord) -> Result<()> {
        let path = failures_jsonl_path(job_id);

        let existing = read_config(self.store.clone(), &path).await.unwrap_or_default();

        let mut new_line = serde_json::to_vec(record).map_err(BatchError::Json)?;
        new_line.push(b'\n');

        let mut combined = existing;
        combined.extend_from_slice(&new_line);

        save_config(self.store.clone(), &path, combined)
            .await
            .map_err(|e| BatchError::Storage(e.to_string()))
    }

    /// Read all failure records from `failures.jsonl`.
    pub async fn load_failures(&self, job_id: &str) -> Result<Vec<FailureRecord>> {
        let path = failures_jsonl_path(job_id);
        let data = match read_config(self.store.clone(), &path).await {
            Ok(d) => d,
            Err(_) => return Ok(vec![]),
        };

        let mut records = Vec::new();
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<FailureRecord>(line) {
                Ok(r) => records.push(r),
                Err(e) => warn!("skipping malformed batch job failure record: {e}"),
            }
        }
        Ok(records)
    }

    /// Clear the failures file (called after a successful retry pass).
    pub async fn clear_failures(&self, job_id: &str) -> Result<()> {
        let path = failures_jsonl_path(job_id);
        save_config(self.store.clone(), &path, Vec::new())
            .await
            .map_err(|e| BatchError::Storage(e.to_string()))
    }

    /// List all job IDs found on disk by scanning `config/batch-jobs/` prefix.
    ///
    /// Uses `list_objects_v2` with a `/` delimiter to enumerate subdirectory names.
    pub async fn list_job_ids(&self) -> Vec<String> {
        use rustfs_ecstore::disk::RUSTFS_META_BUCKET;
        // use rustfs_ecstore::store_api::ListOperations;

        let prefix = format!("{BATCH_JOBS_PREFIX}/");
        match self
            .store
            .clone()
            .list_objects_v2(RUSTFS_META_BUCKET, &prefix, None, Some("/".to_string()), 1000, false, None, false)
            .await
        {
            Ok(result) => result
                .prefixes
                .into_iter()
                .filter_map(|p| {
                    let stripped = p.strip_prefix(&prefix).unwrap_or("").trim_end_matches('/').to_owned();
                    if stripped.is_empty() { None } else { Some(stripped) }
                })
                .collect(),
            Err(e) => {
                // Not found / not initialized yet is normal on first startup.
                warn!("failed to list batch job IDs (this is normal on first startup): {e}");
                vec![]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_helpers() {
        assert_eq!(job_json_path("abc123"), "config/batch-jobs/abc123/job.json");
        assert_eq!(definition_yaml_path("abc123"), "config/batch-jobs/abc123/definition.yaml");
        assert_eq!(failures_jsonl_path("abc123"), "config/batch-jobs/abc123/failures.jsonl");
    }

    #[test]
    fn test_failure_record_serialization() {
        let rec = FailureRecord {
            key: "path/to/object.bin".into(),
            version_id: None,
            error: "timeout".into(),
            timestamp: chrono::Utc::now(),
            size: 1_048_576,
        };
        let json = serde_json::to_string(&rec).expect("serialize");
        let back: FailureRecord = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.key, rec.key);
        assert_eq!(back.size, rec.size);
    }
}
