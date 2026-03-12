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

use chrono::Utc;
use error::Result;
use job::{BatchJob, BatchJobStatusType, BatchJobType};
use registry::JobRegistry;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::StorageAPI;
use std::sync::{Arc, OnceLock};
use store::BatchStore;
use tracing::{info, warn};
use uuid::Uuid;
use worker::worker_count;
use yaml::BatchJobYaml;

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
    ///
    /// Returns a [`BatchJob`] with the assigned ID, or an error if the job definition is
    /// invalid, or a duplicate job is already active.
    pub async fn start_job(&self, yaml_bytes: &[u8], user: String) -> Result<job::BatchJobResult> {
        let yaml_str = std::str::from_utf8(yaml_bytes).map_err(|e| error::BatchError::InvalidJobDefinition(e.to_string()))?;

        let job_def: BatchJobYaml = BatchJobYaml::from_yaml_str(yaml_str)?;

        let replicate = job_def
            .replicate
            .as_ref()
            .ok_or_else(|| error::BatchError::UnsupportedJobType("only 'replicate' is currently supported".into()))?;

        // Compute dedup hash.
        let yaml_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            yaml_str.hash(&mut h);
            format!("{:x}", h.finish())
        };

        let job_id = Uuid::new_v4().to_string();
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

        // Persist definition and initial metadata.
        self.store.save_definition(&job_id, yaml_str).await?;
        self.store.save_job(&job).await?;

        let result = job::BatchJobResult {
            id: job_id.clone(),
            job_type: job.job_type.to_string(),
            user: job.user.clone(),
            started: job.created_at,
        };

        // Spawn background task.
        let config = replicate.clone();
        let store = self.store.clone();
        let ecstore = self.ecstore.clone();
        let registry = self.registry.clone();
        tokio::spawn(async move {
            worker::run_replicate_job_arc(job, config, store, ecstore, registry, control, counters).await;
        });

        info!(job_id = %job_id, "batch: job submitted");
        Ok(result)
    }

    /// Cancel a running job.
    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        self.registry.cancel(job_id).await
    }

    /// Get status of a job.
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

    /// Get the original YAML definition for a job.
    pub async fn describe_job(&self, job_id: &str) -> Result<String> {
        self.store.load_definition(job_id).await
    }

    /// List jobs, optionally filtered by type and/or bucket.
    pub async fn list_jobs(&self, job_type: Option<&str>) -> job::ListBatchJobsResult {
        self.registry.list_jobs(job_type).await
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
        info!(job_id = %job.id, "batch: resuming interrupted job");

        // Load the YAML definition to reconstruct the config.
        let yaml_str = match service.store.load_definition(&job.id).await {
            Ok(s) => s,
            Err(e) => {
                warn!(job_id = %job.id, "batch: cannot load definition for resume: {e}");
                continue;
            }
        };
        let job_def = match BatchJobYaml::from_yaml_str(&yaml_str) {
            Ok(d) => d,
            Err(e) => {
                warn!(job_id = %job.id, "batch: cannot parse definition for resume: {e}");
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
                warn!(job_id = %job.id, "batch: cannot register for resume: {e}");
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

    // Clean up old completed jobs (older than 3 days, matching MinIO behaviour).
    clean_old_jobs_generic(&service).await;

    service
}

async fn clean_old_jobs_generic<S: StorageAPI + 'static>(service: &BatchService<S>) {
    let cutoff = Utc::now() - chrono::Duration::days(3);
    let ids = service.store.list_job_ids().await;
    for id in ids {
        if let Ok(job) = service.store.load_job(&id).await {
            let is_terminal = matches!(
                job.status,
                BatchJobStatusType::Completed | BatchJobStatusType::Failed | BatchJobStatusType::Cancelled
            );
            let is_old = job.finished_at.map_or(false, |t| t < cutoff);
            if is_terminal && is_old {
                info!(job_id = %id, "batch: cleaning up old job");
                // We don't have delete_config exposed; for now just log.
                // A future cleanup pass can use delete_config from ecstore::config::com.
            }
        }
    }
}
