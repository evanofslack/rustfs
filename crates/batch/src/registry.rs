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

use crate::error::{BatchError, Result};
use crate::job::{BatchJob, BatchJobInfo, BatchJobStatusType, BatchJobType, JobControl, JobCounters, ListBatchJobsResult};
use crate::store::BatchStore;
use chrono::Utc;
use rustfs_ecstore::store_api::StorageAPI;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Key used for deduplication: source_bucket + "|" + target_bucket (with optional endpoints).
fn dedup_key(source_endpoint: Option<&str>, source_bucket: &str, target_endpoint: Option<&str>, target_bucket: &str) -> String {
    format!(
        "{}:{}|{}:{}",
        source_endpoint.unwrap_or("local"),
        source_bucket,
        target_endpoint.unwrap_or("local"),
        target_bucket
    )
}

struct RegistryEntry {
    job: BatchJob,
    control: Arc<JobControl>,
    counters: Arc<JobCounters>,
}

pub struct JobRegistry {
    /// Active job entries indexed by job ID.
    entries: RwLock<HashMap<String, RegistryEntry>>,
    /// Dedup set: maps dedup_key → job_id for in-progress jobs only.
    dedup: RwLock<HashMap<String, String>>,
}

impl Default for JobRegistry {
    fn default() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            dedup: RwLock::new(HashMap::new()),
        }
    }
}

impl JobRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new job. Returns `Err(DuplicateJob)` if an active job already
    /// uses the same source+target combination.
    pub async fn register(
        &self,
        job: BatchJob,
        source_endpoint: Option<&str>,
        source_bucket: &str,
        target_endpoint: Option<&str>,
        target_bucket: &str,
        workers: usize,
    ) -> Result<(Arc<JobControl>, Arc<JobCounters>)> {
        let key = dedup_key(source_endpoint, source_bucket, target_endpoint, target_bucket);

        let mut dedup = self.dedup.write().await;
        if dedup.contains_key(&key) {
            return Err(BatchError::DuplicateJob);
        }

        let control = Arc::new(JobControl::new(workers));
        let counters = Arc::new(JobCounters::new(job.job_type.to_string(), job.id.clone(), source_bucket.to_owned()));

        dedup.insert(key.clone(), job.id.clone());
        drop(dedup);

        let mut entries = self.entries.write().await;
        entries.insert(
            job.id.clone(),
            RegistryEntry {
                job,
                control: control.clone(),
                counters: counters.clone(),
            },
        );

        Ok((control, counters))
    }

    /// Release the dedup lock for a job when it reaches a terminal state, so a new job
    /// with the same source+target combination can be started. The entry remains in
    /// `entries` until evicted by `evict_expired`.
    pub async fn unregister(
        &self,
        job_id: &str,
        source_endpoint: Option<&str>,
        source_bucket: &str,
        target_endpoint: Option<&str>,
        target_bucket: &str,
    ) {
        let key = dedup_key(source_endpoint, source_bucket, target_endpoint, target_bucket);
        self.dedup.write().await.remove(&key);
        // Entry stays in `entries` for the retention window, do not remove it here.
        let _ = job_id;
    }

    /// Remove registry entries whose effective end time is older than `retention`.
    /// Called periodically by the background eviction task.
    pub async fn evict_expired(&self, retention: Duration) {
        let cutoff = Utc::now() - chrono::Duration::from_std(retention).unwrap_or(chrono::Duration::days(3));
        let mut entries = self.entries.write().await;
        entries.retain(|_, entry| {
            let is_terminal = matches!(
                entry.job.status,
                BatchJobStatusType::Completed | BatchJobStatusType::Failed | BatchJobStatusType::Cancelled
            );
            if !is_terminal {
                return true;
            }
            let end_time = entry.job.finished_at.unwrap_or(entry.job.created_at);
            end_time >= cutoff
        });
    }

    /// Cancel a job, returning an error if not found.
    pub async fn cancel(&self, job_id: &str) -> Result<()> {
        let entries = self.entries.read().await;
        let entry = entries
            .get(job_id)
            .ok_or_else(|| BatchError::JobNotFound(job_id.to_owned()))?;
        entry.control.cancel.cancel();
        Ok(())
    }

    /// Look up an active job's metadata snapshot.
    pub async fn get_job(&self, job_id: &str) -> Option<BatchJob> {
        self.entries.read().await.get(job_id).map(|e| e.job.clone())
    }

    /// Return a live progress snapshot for a job (merges counters into the stored job snapshot).
    /// Works for both active and retained terminal jobs.
    pub async fn get_active_job_snapshot(&self, job_id: &str) -> Option<ActiveJobSnapshot> {
        let entries = self.entries.read().await;
        let entry = entries.get(job_id)?;
        let (objects, objects_failed, bytes_transferred, bytes_failed) = entry.counters.snapshot();
        Some(ActiveJobSnapshot {
            job: entry.job.clone(),
            objects,
            objects_failed,
            bytes_transferred,
            bytes_failed,
        })
    }

    /// Return a snapshot for the most recently created in-progress job, or `None` if no
    /// active jobs exist.
    pub async fn get_last_active_job_snapshot(&self) -> Option<ActiveJobSnapshot> {
        let entries = self.entries.read().await;
        let entry = entries
            .values()
            .filter(|e| e.job.status == BatchJobStatusType::InProgress)
            .max_by_key(|e| e.job.created_at)?;
        let (objects, objects_failed, bytes_transferred, bytes_failed) = entry.counters.snapshot();
        Some(ActiveJobSnapshot {
            job: entry.job.clone(),
            objects,
            objects_failed,
            bytes_transferred,
            bytes_failed,
        })
    }

    /// Return snapshots for all jobs currently in the registry (within the retention window).
    pub async fn get_all_job_snapshots(&self) -> Vec<ActiveJobSnapshot> {
        let entries = self.entries.read().await;
        let mut snapshots: Vec<ActiveJobSnapshot> = entries
            .values()
            .map(|entry| {
                let (objects, objects_failed, bytes_transferred, bytes_failed) = entry.counters.snapshot();
                ActiveJobSnapshot {
                    job: entry.job.clone(),
                    objects,
                    objects_failed,
                    bytes_transferred,
                    bytes_failed,
                }
            })
            .collect();
        snapshots.sort_by_key(|s| s.job.created_at);
        snapshots
    }

    /// List registered jobs with optional filters for type, status, and bucket.
    ///
    /// `bucket_filter` matches jobs where either the source or target bucket equals the value.
    pub async fn list_jobs(
        &self,
        job_type_filter: Option<&str>,
        status_filter: Option<&str>,
        bucket_filter: Option<&str>,
    ) -> ListBatchJobsResult {
        let entries = self.entries.read().await;
        let jobs = entries
            .values()
            .filter(|e| {
                if let Some(jt) = job_type_filter
                    && e.job.job_type.to_string() != jt
                {
                    return false;
                }
                if let Some(st) = status_filter
                    && e.job.status.to_string() != st
                {
                    return false;
                }
                if let Some(bucket) = bucket_filter
                    && e.job.source_bucket != bucket
                    && e.job.target_bucket != bucket
                {
                    return false;
                }
                true
            })
            .map(|e| BatchJobInfo {
                id: e.job.id.clone(),
                job_type: e.job.job_type.to_string(),
                user: e.job.user.clone(),
                started: e.job.started_at.unwrap_or(e.job.created_at),
                elapsed: e.job.elapsed_nanos(),
                status: e.job.status.to_string(),
            })
            .collect();
        ListBatchJobsResult { jobs }
    }

    /// Load completed/failed/cancelled jobs from disk into registry for listing.
    /// Returns in-progress jobs, so they can be resumed.
    pub async fn load_from_store<S: StorageAPI>(&self, store: &BatchStore<S>) -> Vec<BatchJob> {
        // TODO: only 1 node should handle loading old jobs from disk, keep single owner.
        let job_ids = store.list_job_ids().await;
        let mut to_resume = Vec::new();

        for id in job_ids {
            match store.load_job(&id).await {
                Ok(job) => {
                    info!(job_id = id, status = ?job.status, "recovered batch job from store");
                    if job.status == BatchJobStatusType::InProgress {
                        to_resume.push(job.clone());
                    }
                    // Register non-active jobs in the entries map for listing purposes
                    // (they have no control/counters since they are not running).
                    let mut entries = self.entries.write().await;
                    entries.insert(
                        job.id.clone(),
                        RegistryEntry {
                            counters: Arc::new(JobCounters::new(
                                job.job_type.to_string(),
                                job.id.clone(),
                                job.source_bucket.clone(),
                            )),
                            job,
                            control: Arc::new(JobControl::new(1)),
                        },
                    );
                }
                Err(e) => warn!(job_id = id, "fail load batch job: {e}"),
            }
        }

        to_resume
    }

    /// Update the stored job snapshot (called after progress persist).
    pub async fn update_job_snapshot(&self, job_id: &str, updated: BatchJob) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(job_id) {
            entry.job = updated;
        }
    }

    /// Transition a job's status in the registry.
    pub async fn set_status(&self, job_id: &str, status: BatchJobStatusType) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(job_id) {
            entry.job.status = status;
            if matches!(
                entry.job.status,
                BatchJobStatusType::Completed | BatchJobStatusType::Failed | BatchJobStatusType::Cancelled
            ) {
                entry.job.finished_at = Some(Utc::now());
            }
        }
    }
}

/// A snapshot of a running job with live counter values merged in.
#[derive(Debug)]
pub struct ActiveJobSnapshot {
    pub job: BatchJob,
    pub objects: i64,
    pub objects_failed: i64,
    pub bytes_transferred: i64,
    pub bytes_failed: i64,
}

impl ActiveJobSnapshot {
    /// Convert to the madmin `JobMetric` type for API responses.
    pub fn to_job_metric(&self) -> rustfs_madmin::metrics::JobMetric {
        use rustfs_madmin::metrics::{JobMetric, ReplicateInfo};

        let replicate = if self.job.job_type == BatchJobType::Replicate {
            Some(ReplicateInfo {
                bucket: self.job.last_bucket.clone(),
                object: self.job.last_object.clone(),
                objects: self.objects,
                objects_failed: self.objects_failed,
                bytes_transferred: self.bytes_transferred,
                bytes_failed: self.bytes_failed,
            })
        } else {
            None
        };

        JobMetric {
            job_id: self.job.id.clone(),
            job_type: self.job.job_type.to_string(),
            start_time: self.job.started_at.unwrap_or(self.job.created_at),
            last_update: Utc::now(),
            retry_attempts: self.job.retry_attempts as i32,
            complete: self.job.status == BatchJobStatusType::Completed,
            failed: self.job.status == BatchJobStatusType::Failed,
            replicate,
            key_rotate: None,
            expired: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{BatchJob, BatchJobType};
    use crate::yaml::{EndpointYaml, ReplicateJobYaml};

    fn make_test_job(id: &str) -> BatchJob {
        let config = ReplicateJobYaml {
            api_version: "v1".into(),
            source: EndpointYaml {
                endpoint_type: "rustfs".into(),
                bucket: "src".into(),
                prefix: None,
                endpoint: None,
                credentials: None,
            },
            target: EndpointYaml {
                endpoint_type: "s3".into(),
                bucket: "dst".into(),
                prefix: None,
                endpoint: Some("https://remote:9000".into()),
                credentials: Some(crate::yaml::CredentialsYaml {
                    access_key: "A".into(),
                    secret_key: "S".into(),
                }),
            },
            flags: None,
        };
        BatchJob::new(id.into(), BatchJobType::Replicate, "admin".into(), "hash1".into(), &config)
    }

    #[tokio::test]
    async fn test_register_and_lookup() {
        let registry = JobRegistry::new();
        let job = make_test_job("job-1");
        let (control, _counters) = registry
            .register(job, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("register");
        assert!(!control.cancel.is_cancelled());
        assert!(registry.get_job("job-1").await.is_some());
    }

    #[tokio::test]
    async fn test_duplicate_job_rejected() {
        let registry = JobRegistry::new();
        let job1 = make_test_job("job-1");
        let job2 = make_test_job("job-2");

        registry
            .register(job1, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("first register");

        let err = registry
            .register(job2, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect_err("should reject duplicate");

        assert!(matches!(err, BatchError::DuplicateJob));
    }

    #[tokio::test]
    async fn test_cancel_job() {
        let registry = JobRegistry::new();
        let job = make_test_job("job-cancel");
        let (control, _) = registry
            .register(job, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("register");

        registry.cancel("job-cancel").await.expect("cancel");
        assert!(control.cancel.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancel_nonexistent_returns_error() {
        let registry = JobRegistry::new();
        let err = registry.cancel("nonexistent").await.expect_err("should fail");
        assert!(matches!(err, BatchError::JobNotFound(_)));
    }

    #[tokio::test]
    async fn test_list_jobs() {
        let registry = JobRegistry::new();
        let job = make_test_job("job-list");
        registry
            .register(job, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("register");

        let result = registry.list_jobs(None, None, None).await;
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.jobs[0].id, "job-list");
    }

    #[tokio::test]
    async fn test_list_jobs_filter_by_type() {
        let registry = JobRegistry::new();
        let job = make_test_job("job-filter");
        registry
            .register(job, None, "bucket-a", Some("https://remote:9000"), "bucket-b", 4)
            .await
            .expect("register");

        let result = registry.list_jobs(Some("replicate"), None, None).await;
        assert_eq!(result.jobs.len(), 1);

        let result_empty = registry.list_jobs(Some("keyrotate"), None, None).await;
        assert_eq!(result_empty.jobs.len(), 0);
    }

    #[tokio::test]
    async fn test_list_jobs_filter_by_bucket() {
        let registry = JobRegistry::new();
        let job = make_test_job("job-bucket");
        registry
            .register(job, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("register");

        let result = registry.list_jobs(None, None, Some("src")).await;
        assert_eq!(result.jobs.len(), 1);

        let result = registry.list_jobs(None, None, Some("dst")).await;
        assert_eq!(result.jobs.len(), 1);

        let result = registry.list_jobs(None, None, Some("other")).await;
        assert_eq!(result.jobs.len(), 0);
    }

    #[tokio::test]
    async fn test_list_jobs_filter_by_status() {
        let registry = JobRegistry::new();
        let job = make_test_job("job-status-filter");
        registry
            .register(job, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("register");

        let result = registry.list_jobs(None, Some("in-progress"), None).await;
        assert_eq!(result.jobs.len(), 1);

        let result = registry.list_jobs(None, Some("completed"), None).await;
        assert_eq!(result.jobs.len(), 0);
    }

    #[tokio::test]
    async fn test_get_last_active_job_snapshot() {
        let registry = JobRegistry::new();
        let job = make_test_job("job-last-active");
        registry
            .register(job, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("register");

        let snapshot = registry.get_last_active_job_snapshot().await;
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().job.id, "job-last-active");
    }

    #[tokio::test]
    async fn test_get_all_job_snapshots() {
        let registry = JobRegistry::new();
        let job = make_test_job("job-all");
        registry
            .register(job, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("register");

        let snapshots = registry.get_all_job_snapshots().await;
        assert_eq!(snapshots.len(), 1);
    }

    #[tokio::test]
    async fn test_unregister_allows_new_job() {
        let registry = JobRegistry::new();
        let job1 = make_test_job("job-1");
        registry
            .register(job1, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("register");

        registry
            .unregister("job-1", None, "src", Some("https://remote:9000"), "dst")
            .await;

        let job2 = make_test_job("job-2");
        registry
            .register(job2, None, "src", Some("https://remote:9000"), "dst", 4)
            .await
            .expect("re-register after unregister");
    }
}
