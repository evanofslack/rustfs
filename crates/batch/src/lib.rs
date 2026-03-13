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

//! # rustfs-batch
//!
//! MinIO-compatible batch job framework for RustFS.
//!
//! ## Supported job types
//! - `replicate` — copy objects from a source bucket to a target bucket (local↔local,
//!   local→remote, or remote→local).
//!
//! ## Usage
//!
//! Call [`init_batch_service`] once during startup, passing an `Arc<impl StorageAPI>`.
//! The service recovers any interrupted in-progress jobs from disk and registers HTTP
//! routes via the admin handler in `rustfs/src/admin/handlers/batch.rs`.

pub mod client;
pub mod error;
pub mod job;
pub mod registry;
pub mod store;
pub mod worker;
pub mod yaml;

use error::Result;
use job::{BatchJob, BatchJobType};
use registry::JobRegistry;
use rustfs_common::get_global_local_node_name;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::StorageAPI;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use store::BatchStore;
use tracing::{info, warn};
use uuid::Uuid;
use worker::worker_count;
use yaml::BatchJobYaml;

const ENV_JOB_RETENTION_DAYS: &str = "RUSTFS_BATCH_JOB_RETENTION_DAYS";
const DEFAULT_JOB_RETENTION_DAYS: u64 = 3;

fn job_retention() -> Duration {
    let days = rustfs_utils::get_env_u64(ENV_JOB_RETENTION_DAYS, DEFAULT_JOB_RETENTION_DAYS);
    Duration::from_hours(days * 24)
}

/// Build a job ID embedding job type and the owning node address so that any
/// cluster node can route cancel/status requests without a distributed lookup.
/// Format: `<jobtype>-<uuid>|<node_addr>` (e.g. `replicate-a3f2...|node1:9000`)
/// When running in single-node mode (node_addr is empty) the `|<node_addr>` suffix
/// is omitted and all operations remain local.
async fn make_job_id(job_type: &BatchJobType) -> String {
    let node_addr = get_global_local_node_name().await;
    if node_addr.is_empty() {
        format!("{}-{}", job_type, Uuid::new_v4())
    } else {
        format!("{}-{}|{}", job_type, Uuid::new_v4(), node_addr)
    }
}

/// Extract the owner node address (`host:port`) from a job ID, or `None` for single-node IDs.
/// Format: `<jobtype>-<uuid>|<host:port>` → returns the `<host:port>` part.
pub fn parse_owner_node(job_id: &str) -> Option<&str> {
    job_id.split_once('|').map(|(_, node)| node)
}

/// Global batch service instance, initialized once during startup.
static GLOBAL_BATCH_SERVICE: OnceLock<Arc<BatchService<ECStore>>> = OnceLock::new();

/// Retrieve the global batch service, if initialized.
pub fn get_global_batch_service() -> Option<Arc<BatchService<ECStore>>> {
    GLOBAL_BATCH_SERVICE.get().cloned()
}

/// Set the global batch service (called from `init_batch_service`).
pub fn set_global_batch_service(svc: Arc<BatchService<ECStore>>) {
    let _ = GLOBAL_BATCH_SERVICE.set(svc);
}

pub struct BatchService<S: StorageAPI> {
    pub registry: Arc<JobRegistry>,
    pub store: Arc<BatchStore<S>>,
    pub ecstore: Arc<S>,
}

impl<S: StorageAPI + 'static> BatchService<S> {
    pub fn new(ecstore: Arc<S>) -> Self {
        Self {
            registry: Arc::new(JobRegistry::new()),
            store: Arc::new(BatchStore::new(ecstore.clone())),
            ecstore,
        }
    }

    /// Start a new batch job from YAML definition bytes.
    pub async fn start_job(&self, yaml_bytes: &[u8], user: String) -> Result<job::BatchJobResult> {
        let yaml_str = std::str::from_utf8(yaml_bytes).map_err(|e| error::BatchError::InvalidJobDefinition(e.to_string()))?;

        let job_def: BatchJobYaml = BatchJobYaml::from_yaml_str(yaml_str)?;

        let replicate = job_def
            .replicate
            .as_ref()
            .ok_or_else(|| error::BatchError::UnsupportedJobType("only job type 'replicate' is currently supported".into()))?;

        let yaml_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            yaml_str.hash(&mut h);
            format!("{:x}", h.finish())
        };

        let job_id = make_job_id(&BatchJobType::Replicate).await;
        let job = BatchJob::new(job_id.clone(), BatchJobType::Replicate, user, yaml_hash, replicate);

        let workers = worker_count();
        let (control, counters) = self
            .registry
            .register(
                job.clone(),
                replicate.source.endpoint.as_deref(),
                &replicate.source.bucket,
                replicate.target.endpoint.as_deref(),
                &replicate.target.bucket,
                workers,
            )
            .await?;

        self.store.save_definition(&job_id, yaml_str).await?;
        self.store.save_job(&job).await?;

        let result = job::BatchJobResult {
            id: job_id.clone(),
            job_type: job.job_type.to_string(),
            user: job.user.clone(),
            started: job.created_at,
        };

        let config = replicate.clone();
        let store = self.store.clone();
        let ecstore = self.ecstore.clone();
        let registry = self.registry.clone();
        tokio::spawn(async move {
            worker::run_replicate_job_arc(job, config, store, ecstore, registry, control, counters).await;
        });

        info!(job_id = %job_id, "batch job submitted");
        Ok(result)
    }

    /// Cancel a running job. Forwarding to the owner node is handled at the HTTP handler layer.
    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        self.registry.cancel(job_id).await
    }

    /// Get status of a specific job by ID. Forwarding to the owner node is handled at the HTTP handler layer.
    pub async fn job_status(&self, job_id: &str) -> Result<job::BatchJobStatus> {
        let snapshot = self
            .registry
            .get_active_job_snapshot(job_id)
            .await
            .ok_or_else(|| error::BatchError::JobNotFound(job_id.to_owned()))?;
        Ok(job::BatchJobStatus {
            last_metric: snapshot.to_job_metric(),
        })
    }

    /// Get the status of the most recently created in-progress job on this node.
    pub async fn job_status_last_active(&self) -> Option<job::BatchJobStatus> {
        self.registry
            .get_last_active_job_snapshot()
            .await
            .map(|s| job::BatchJobStatus {
                last_metric: s.to_job_metric(),
            })
    }

    /// Get status of all jobs within the retention window on this node.
    pub async fn job_status_all(&self) -> job::BatchJobStatusList {
        let statuses = self
            .registry
            .get_all_job_snapshots()
            .await
            .into_iter()
            .map(|s| job::BatchJobStatus {
                last_metric: s.to_job_metric(),
            })
            .collect();
        job::BatchJobStatusList { statuses }
    }

    /// Get the original YAML definition for a job on this node. Forwarding is handled at the HTTP handler layer.
    pub async fn describe_job(&self, job_id: &str) -> Result<String> {
        self.store.load_definition(job_id).await
    }

    /// List jobs on this node filtered by type, status, and/or bucket.
    /// Cross-node fan-out is handled at the HTTP handler layer.
    pub async fn list_jobs(
        &self,
        job_type: Option<&str>,
        status: Option<&str>,
        bucket: Option<&str>,
    ) -> job::ListBatchJobsResult {
        self.registry.list_jobs(job_type, status, bucket).await
    }
}

/// Initialize the batch service with an `ECStore` and register the global singleton.
///
/// Called from `rustfs/src/main.rs` during startup.
pub async fn init_batch_service(ecstore: Arc<ECStore>) -> Arc<BatchService<ECStore>> {
    let service = init_batch_service_generic(ecstore).await;
    set_global_batch_service(service.clone());
    service
}

/// Initialize the batch service and recover any interrupted jobs from disk.
async fn init_batch_service_generic<S: StorageAPI + 'static>(ecstore: Arc<S>) -> Arc<BatchService<S>> {
    let service = Arc::new(BatchService::new(ecstore));

    let jobs_to_resume = service.registry.load_from_store(&*service.store).await;

    for job in jobs_to_resume {
        info!(job_id = %job.id, "resuming interrupted batch job");

        let yaml_str = match service.store.load_definition(&job.id).await {
            Ok(s) => s,
            Err(e) => {
                warn!(job_id = %job.id, "cannot load batch job definition for resume: {e}");
                continue;
            }
        };
        let job_def = match BatchJobYaml::from_yaml_str(&yaml_str) {
            Ok(d) => d,
            Err(e) => {
                warn!(job_id = %job.id, "cannot parse batch job definition for resume: {e}");
                continue;
            }
        };
        let Some(replicate) = job_def.replicate else {
            continue;
        };

        let workers = worker_count();
        let (control, counters) = match service
            .registry
            .register(
                job.clone(),
                replicate.source.endpoint.as_deref(),
                &replicate.source.bucket,
                replicate.target.endpoint.as_deref(),
                &replicate.target.bucket,
                workers,
            )
            .await
        {
            Ok(pair) => pair,
            Err(e) => {
                warn!(job_id = %job.id, "cannot register batch job for resume: {e}");
                continue;
            }
        };

        let store = service.store.clone();
        let ecstore = service.ecstore.clone();
        let registry = service.registry.clone();
        tokio::spawn(async move {
            worker::run_replicate_job_arc(job, replicate, store, ecstore, registry, control, counters).await;
        });
    }

    let retention = job_retention();
    let registry_weak = Arc::downgrade(&service.registry);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            match registry_weak.upgrade() {
                Some(reg) => {
                    reg.evict_expired(retention).await;
                }
                None => break,
            }
        }
    });

    service
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_owner_node_with_node() {
        let job_id = "replicate-abc123|node1:9000";
        assert_eq!(parse_owner_node(job_id), Some("node1:9000"));
    }

    #[test]
    fn test_parse_owner_node_single_node() {
        let job_id = "replicate-abc123";
        assert_eq!(parse_owner_node(job_id), None);
    }

    #[tokio::test]
    async fn test_make_job_id_format_single_node() {
        // GLOBAL_LOCAL_NODE_NAME is empty in tests, so no "|" suffix.
        let id = make_job_id(&BatchJobType::Replicate).await;
        assert!(id.starts_with("replicate-"), "id={id}");
        assert!(!id.contains('|'), "id={id}");
    }
}
