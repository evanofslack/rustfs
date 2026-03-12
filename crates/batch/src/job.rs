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

//! Runtime job types, status, and lifecycle definitions.

use crate::yaml::ReplicateJobYaml;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use tokio_util::sync::CancellationToken;

/// The type of a batch job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchJobType {
    Replicate,
}

impl std::fmt::Display for BatchJobType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchJobType::Replicate => write!(f, "replicate"),
        }
    }
}

/// Lifecycle status of a batch job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BatchJobStatusType {
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for BatchJobStatusType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchJobStatusType::InProgress => write!(f, "in-progress"),
            BatchJobStatusType::Completed => write!(f, "completed"),
            BatchJobStatusType::Failed => write!(f, "failed"),
            BatchJobStatusType::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Persisted job metadata (stored as `job.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchJob {
    pub id: String,
    pub job_type: BatchJobType,
    pub status: BatchJobStatusType,
    /// Admin user who submitted the job.
    pub user: String,
    /// SHA256 hash of submitted YAML for dedup.
    pub yaml_hash: String,
    pub retry_attempts: u32,
    pub max_retries: u32,
    /// Milliseconds from YAML retry.delay string.
    pub retry_delay_ms: u64,
    // Progress fields (mirror ReplicateInfo in madmin).
    pub last_bucket: String,
    pub last_object: String,
    pub objects: i64,
    pub objects_failed: i64,
    pub bytes_transferred: i64,
    pub bytes_failed: i64,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_persisted_at: Option<DateTime<Utc>>,
    /// S3 continuation token for resuming interrupted enumeration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_continuation_token: Option<String>,
    /// Source bucket name (for list filtering without YAML parse).
    #[serde(default)]
    pub source_bucket: String,
    /// Target bucket name (for list filtering without YAML parse).
    #[serde(default)]
    pub target_bucket: String,
}

impl BatchJob {
    pub fn new(id: String, job_type: BatchJobType, user: String, yaml_hash: String, config: &ReplicateJobYaml) -> Self {
        let (max_retries, retry_delay_ms) = config
            .flags
            .as_ref()
            .and_then(|f| f.retry.as_ref())
            .map(|r| (r.attempts, parse_delay_ms(&r.delay)))
            .unwrap_or((3, 500));

        Self {
            id,
            job_type,
            status: BatchJobStatusType::InProgress,
            user,
            yaml_hash,
            retry_attempts: 0,
            max_retries,
            retry_delay_ms,
            last_bucket: String::new(),
            last_object: String::new(),
            objects: 0,
            objects_failed: 0,
            bytes_transferred: 0,
            bytes_failed: 0,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            last_persisted_at: None,
            last_continuation_token: None,
            source_bucket: config.source.bucket.clone(),
            target_bucket: config.target.bucket.clone(),
        }
    }

    pub fn elapsed_nanos(&self) -> i64 {
        let start = self.started_at.unwrap_or(self.created_at);
        let end = self.finished_at.unwrap_or_else(Utc::now);
        (end - start).num_nanoseconds().unwrap_or(0)
    }
}

/// In-memory atomic counters for a running job, shared between the enumerator and workers.
#[derive(Debug, Default)]
pub struct JobCounters {
    pub objects: AtomicI64,
    pub objects_failed: AtomicI64,
    pub bytes_transferred: AtomicI64,
    pub bytes_failed: AtomicI64,
}

impl JobCounters {
    pub fn inc_success(&self, bytes: i64) {
        self.objects.fetch_add(1, Ordering::Relaxed);
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn inc_failure(&self, bytes: i64) {
        self.objects_failed.fetch_add(1, Ordering::Relaxed);
        self.bytes_failed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (i64, i64, i64, i64) {
        (
            self.objects.load(Ordering::Relaxed),
            self.objects_failed.load(Ordering::Relaxed),
            self.bytes_transferred.load(Ordering::Relaxed),
            self.bytes_failed.load(Ordering::Relaxed),
        )
    }
}

/// Control handle for a running job.
#[derive(Clone, Debug)]
pub struct JobControl {
    pub cancel: CancellationToken,
    pub workers: usize,
}

impl JobControl {
    pub fn new(workers: usize) -> Self {
        Self {
            cancel: CancellationToken::new(),
            workers,
        }
    }
}

/// Response returned to the caller when a job is started (matches BatchJobResult in madmin-go).
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchJobResult {
    pub id: String,
    #[serde(rename = "type")]
    pub job_type: String,
    pub user: String,
    pub started: DateTime<Utc>,
}

/// Status response (matches BatchJobStatus in madmin-go).
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchJobStatus {
    #[serde(rename = "LastMetric")]
    pub last_metric: rustfs_madmin::metrics::JobMetric,
}

/// Response for `?all=true` on status-job — returns all jobs in the retention window.
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchJobStatusList {
    pub statuses: Vec<BatchJobStatus>,
}

/// Entry in a list-jobs response (matches BatchJobInfo in madmin-go).
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchJobInfo {
    pub id: String,
    #[serde(rename = "type")]
    pub job_type: String,
    pub user: String,
    pub started: DateTime<Utc>,
    /// Elapsed nanoseconds.
    pub elapsed: i64,
    pub status: String,
}

/// Response for list-jobs (matches ListBatchJobsResult in madmin-go).
#[derive(Debug, Serialize, Deserialize)]
pub struct ListBatchJobsResult {
    pub jobs: Vec<BatchJobInfo>,
}

/// Parse a duration string like "500ms", "1s", "2m" into milliseconds.
pub fn parse_delay_ms(s: &str) -> u64 {
    let s = s.trim();
    if s.ends_with("ms") {
        s[..s.len() - 2].parse().unwrap_or(500)
    } else if s.ends_with('s') {
        s[..s.len() - 1].parse::<u64>().unwrap_or(1) * 1000
    } else if s.ends_with('m') {
        s[..s.len() - 1].parse::<u64>().unwrap_or(1) * 60_000
    } else {
        500
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_delay_ms() {
        assert_eq!(parse_delay_ms("500ms"), 500);
        assert_eq!(parse_delay_ms("2s"), 2000);
        assert_eq!(parse_delay_ms("1m"), 60_000);
        assert_eq!(parse_delay_ms("0ms"), 0);
    }

    #[test]
    fn test_job_status_display() {
        assert_eq!(BatchJobStatusType::InProgress.to_string(), "in-progress");
        assert_eq!(BatchJobStatusType::Completed.to_string(), "completed");
        assert_eq!(BatchJobStatusType::Failed.to_string(), "failed");
        assert_eq!(BatchJobStatusType::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn test_job_type_display() {
        assert_eq!(BatchJobType::Replicate.to_string(), "replicate");
    }

    #[test]
    fn test_job_counters() {
        let c = JobCounters::default();
        c.inc_success(1024);
        c.inc_success(2048);
        c.inc_failure(512);
        let (objects, failed, bytes, bytes_failed) = c.snapshot();
        assert_eq!(objects, 2);
        assert_eq!(failed, 1);
        assert_eq!(bytes, 3072);
        assert_eq!(bytes_failed, 512);
    }

    #[test]
    fn test_batch_job_serialization() {
        let config = crate::yaml::ReplicateJobYaml {
            api_version: "v1".into(),
            source: crate::yaml::EndpointYaml {
                endpoint_type: "rustfs".into(),
                bucket: "src".into(),
                prefix: None,
                endpoint: None,
                credentials: None,
            },
            target: crate::yaml::EndpointYaml {
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
        let job = BatchJob::new("test-id".into(), BatchJobType::Replicate, "admin".into(), "hash".into(), &config);
        let json = serde_json::to_string(&job).expect("serialize");
        let back: BatchJob = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.id, "test-id");
        assert_eq!(back.max_retries, 3);
        assert_eq!(back.retry_delay_ms, 500);
    }
}
