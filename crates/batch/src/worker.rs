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

//! Worker pool for batch replication jobs.
//!
//! Flow:
//!  1. An enumerator lists source objects (remote via AWS SDK or local via ECStore).
//!  2. Objects are fanned out to N concurrent transfers via a semaphore.
//!  3. Each slot HEADs the target; if the object is already there with a matching
//!     ETag, it is skipped. Otherwise it is GET→PUT transferred.
//!  4. Failures are appended to `failures.jsonl`.
//!  5. After enumeration, if failures remain and retries are configured, the
//!     failures file is replayed, then cleared on success.

use crate::client::{BatchS3Client, ListedObject};
use crate::error::{BatchError, Result};
use crate::job::{BatchJob, BatchJobStatusType, JobControl, JobCounters};
use crate::registry::JobRegistry;
use crate::store::{BatchStore, FailureRecord};
use crate::yaml::{FilterYaml, ReplicateJobYaml};
use bytes::Bytes;
use chrono::Utc;
use http::HeaderMap;
use rustfs_ecstore::store_api::{ObjectOptions, PutObjReader, StorageAPI};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

const DEFAULT_WORKER_COUNT: usize = 4;
const ENV_WORKER_COUNT: &str = "RUSTFS_BATCH_REPLICATION_WORKERS";

pub fn worker_count() -> usize {
    std::env::var(ENV_WORKER_COUNT)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_WORKER_COUNT)
}

/// A unit of work: one object to transfer.
#[derive(Debug, Clone)]
struct WorkItem {
    key: String,
    etag: Option<String>,
    size: i64,
}

/// Run a full batch replication job. Spawned as a background task.
pub async fn run_replicate_job_arc<S: StorageAPI + 'static>(
    job: BatchJob,
    config: ReplicateJobYaml,
    store: Arc<BatchStore<S>>,
    ecstore: Arc<S>,
    registry: Arc<JobRegistry>,
    control: Arc<JobControl>,
    counters: Arc<JobCounters>,
) {
    let job_id = job.id.clone();
    let source_endpoint = config.source.endpoint.clone();
    let source_bucket = config.source.bucket.clone();
    let target_endpoint = config.target.endpoint.clone();
    let target_bucket = config.target.bucket.clone();

    info!(job_id = %job_id, "starting batch replication job");

    // Mark job as started.
    {
        let mut updated = job.clone();
        updated.started_at = Some(Utc::now());
        registry.update_job_snapshot(&job_id, updated.clone()).await;
        if let Err(e) = store.save_job(&updated).await {
            error!(job_id = %job_id, "fail save batch job start state: {e}");
        }
    }

    let result = run_replication_passes(
        &job,
        &config,
        store.clone(),
        ecstore.clone(),
        registry.clone(),
        control.clone(),
        counters.clone(),
    )
    .await;

    let final_status = match result {
        Ok(()) => {
            info!(job_id = %job_id, "batch job completed successfully");
            BatchJobStatusType::Completed
        }
        Err(BatchError::JobCancelled) => {
            info!(job_id = %job_id, "batch job cancelled");
            BatchJobStatusType::Cancelled
        }
        Err(e) => {
            error!(job_id = %job_id, "batch job failed: {e}");
            BatchJobStatusType::Failed
        }
    };

    registry.set_status(&job_id, final_status.clone()).await;

    if let Some(mut snapshot) = registry.get_job(&job_id).await {
        snapshot.status = final_status;
        snapshot.finished_at = Some(Utc::now());
        if let Err(e) = store.save_job(&snapshot).await {
            error!(job_id = %job_id, "fail persist final batch job state: {e}");
        }
    }

    registry
        .unregister(
            &job_id,
            source_endpoint.as_deref(),
            &source_bucket,
            target_endpoint.as_deref(),
            &target_bucket,
        )
        .await;
}

/// Core replication loop with retry support.
async fn run_replication_passes<S: StorageAPI + 'static>(
    job: &BatchJob,
    config: &ReplicateJobYaml,
    store: Arc<BatchStore<S>>,
    ecstore: Arc<S>,
    registry: Arc<JobRegistry>,
    control: Arc<JobControl>,
    counters: Arc<JobCounters>,
) -> Result<()> {
    let filter = config.flags.as_ref().and_then(|f| f.filter.as_ref());
    let max_retries = job.max_retries;
    let retry_delay = Duration::from_millis(job.retry_delay_ms);

    let source_client = BatchS3Client::from_endpoint(&config.source).await?;
    let target_client = BatchS3Client::from_endpoint(&config.target).await?;

    enumerate_and_transfer(
        job,
        config,
        filter,
        source_client.as_ref(),
        target_client.as_ref(),
        ecstore.clone(),
        store.clone(),
        registry.clone(),
        control.clone(),
        counters.clone(),
        job.last_continuation_token.clone(),
    )
    .await?;

    for attempt in 0..max_retries {
        let failures = store.load_failures(&job.id).await?;
        if failures.is_empty() {
            break;
        }

        info!(
            job_id = %job.id,
            attempt = attempt + 1,
            failure_count = failures.len(),
            "retrying failed batch objects"
        );

        tokio::time::sleep(retry_delay).await;

        retry_failures(
            job,
            config,
            &failures,
            source_client.as_ref(),
            target_client.as_ref(),
            ecstore.clone(),
            store.clone(),
            counters.clone(),
            control.clone(),
        )
        .await?;

        store.clear_failures(&job.id).await?;
    }

    let remaining = store.load_failures(&job.id).await?;
    if !remaining.is_empty() {
        return Err(BatchError::Transfer(format!(
            "batch job finished with {} failed objects after {} retries",
            remaining.len(),
            max_retries
        )));
    }

    Ok(())
}

/// Enumerate source objects and transfer them concurrently (bounded by `control.workers`).
#[allow(clippy::too_many_arguments)]
async fn enumerate_and_transfer<S: StorageAPI + 'static>(
    job: &BatchJob,
    config: &ReplicateJobYaml,
    filter: Option<&FilterYaml>,
    source_client: Option<&BatchS3Client>,
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    store: Arc<BatchStore<S>>,
    registry: Arc<JobRegistry>,
    control: Arc<JobControl>,
    counters: Arc<JobCounters>,
    resume_token: Option<String>,
) -> Result<()> {
    let sem = Arc::new(tokio::sync::Semaphore::new(control.workers));
    let job_id = job.id.clone();
    let source_bucket = config.source.bucket.clone();
    let target_bucket = config.target.bucket.clone();
    let source_prefix = config.source.prefix.clone().unwrap_or_default();
    let target_prefix = config.target.prefix.clone().unwrap_or_default();

    debug!(
        job_id = job_id,
        bucket_source = source_bucket,
        bucket_target = target_bucket,
        "start batch replicate objects"
    );

    let mut continuation_token = resume_token;

    loop {
        if control.cancel.is_cancelled() {
            return Err(BatchError::JobCancelled);
        }

        // Fetch one page of source objects.
        let (items, more) = fetch_page(
            source_client,
            ecstore.clone(),
            &source_bucket,
            &source_prefix,
            continuation_token.as_deref(),
            filter,
        )
        .await?;

        continuation_token = more;

        debug!(
            job_id = job_id,
            object_count = items.len(),
            bucket_source = source_bucket,
            bucket_target = target_bucket,
            "satch replicate object page"
        );
        for item in items {
            if control.cancel.is_cancelled() {
                return Err(BatchError::JobCancelled);
            }

            let target_key = build_target_key(&item.key, &target_prefix);
            let already_exists =
                check_target_exists(target_client, ecstore.clone(), &target_bucket, &target_key, item.etag.as_deref()).await;

            if already_exists {
                debug!(
                    job_id = job_id,
                    key = item.key,
                    bucket_source = source_bucket,
                    bucket_target = &target_bucket,
                    "object already exists in target, skipping"
                );
                counters.inc_success(0);
                continue;
            }

            let _permit = sem.clone().acquire_owned().await.expect("semaphore open");

            match transfer_object(
                source_client,
                target_client,
                ecstore.clone(),
                &source_bucket,
                &item.key,
                &target_bucket,
                &target_key,
            )
            .await
            {
                Ok(bytes) => {
                    debug!(
                        job_id = job_id,
                        key = item.key,
                        bucket_source = source_bucket,
                        bucket_target = &target_bucket,
                        "success replicate object"
                    );
                    counters.inc_success(bytes);
                }
                Err(e) => {
                    warn!(
                        job_id = job_id,
                        key = %item.key, 
                        bucket_source = source_bucket,
                        bucket_target = &target_bucket,
                         "failure replicate object: {e}");
                    counters.inc_failure(item.size);
                    let rec = FailureRecord {
                        key: item.key.clone(),
                        version_id: None,
                        error: e.to_string(),
                        timestamp: Utc::now(),
                        size: item.size,
                    };
                    if let Err(se) = store.append_failure(&job_id, &rec).await {
                        error!(job_id = job_id, "fail write failure record: {se}");
                    }
                }
            }
        }

        // Persist progress periodically (after each page).
        if let Some(mut snapshot) = registry.get_job(&job_id).await {
            let (objects, objects_failed, bytes_transferred, bytes_failed) = counters.snapshot();
            snapshot.objects = objects;
            snapshot.objects_failed = objects_failed;
            snapshot.bytes_transferred = bytes_transferred;
            snapshot.bytes_failed = bytes_failed;
            snapshot.last_continuation_token = continuation_token.clone();
            snapshot.last_persisted_at = Some(Utc::now());
            registry.update_job_snapshot(&job_id, snapshot.clone()).await;
            if let Err(e) = store.save_job(&snapshot).await {
                warn!(job_id = job_id, "fail persist batch job progress: {e}");
            } else {
                debug!(
                    job_id = job_id,
                    bucket_source = source_bucket,
                    bucket_target = &target_bucket,
                    objects,
                    objects_failed,
                    bytes_transferred,
                    bytes_failed,
                    "success persist batch job progress"
                );
            }
        }

        if continuation_token.is_none() {
            break;
        }
    }

    Ok(())
}

/// Fetch one page from source. Returns `(items, next_continuation_token)`.
async fn fetch_page<S: StorageAPI + 'static>(
    source_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    source_bucket: &str,
    source_prefix: &str,
    token: Option<&str>,
    filter: Option<&FilterYaml>,
) -> Result<(Vec<WorkItem>, Option<String>)> {
    if let Some(sc) = source_client {
        let page = sc.list_objects_page(token).await?;
        let items = page
            .objects
            .into_iter()
            .filter(|obj| passes_filter(obj, filter))
            .map(|obj| WorkItem {
                key: obj.key,
                etag: obj.etag,
                size: obj.size,
            })
            .collect();
        Ok((items, page.next_token))
    } else {
        // Local source: use ECStore list_objects_v2 with delimiter-free listing.
        let result = ecstore
            .clone()
            .list_objects_v2(
                source_bucket,
                source_prefix,
                token.map(|s| s.to_owned()),
                None, // no delimiter — enumerate all objects
                1000,
                false,
                None,
                false,
            )
            .await
            .map_err(|e| BatchError::Transfer(e.to_string()))?;

        let next_token = if result.is_truncated {
            result.next_continuation_token
        } else {
            None
        };

        let items = result
            .objects
            .into_iter()
            .filter(|obj| {
                let last_modified = obj
                    .mod_time
                    .and_then(|t| chrono::DateTime::from_timestamp(t.unix_timestamp(), 0));
                let lo = ListedObject {
                    key: obj.name.clone(),
                    size: obj.size,
                    etag: obj.etag.clone(),
                    last_modified,
                };
                passes_filter(&lo, filter)
            })
            .map(|obj| WorkItem {
                key: obj.name,
                etag: obj.etag,
                size: obj.size,
            })
            .collect();

        Ok((items, next_token))
    }
}

/// Re-process known-failed objects.
#[allow(clippy::too_many_arguments)]
async fn retry_failures<S: StorageAPI + 'static>(
    job: &BatchJob,
    config: &ReplicateJobYaml,
    failures: &[FailureRecord],
    source_client: Option<&BatchS3Client>,
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    store: Arc<BatchStore<S>>,
    counters: Arc<JobCounters>,
    control: Arc<JobControl>,
) -> Result<()> {
    let source_bucket = &config.source.bucket;
    let target_bucket = &config.target.bucket;
    let target_prefix = config.target.prefix.clone().unwrap_or_default();

    for rec in failures {
        if control.cancel.is_cancelled() {
            return Err(BatchError::JobCancelled);
        }

        let target_key = build_target_key(&rec.key, &target_prefix);

        match transfer_object(
            source_client,
            target_client,
            ecstore.clone(),
            source_bucket,
            &rec.key,
            target_bucket,
            &target_key,
        )
        .await
        {
            Ok(bytes) => {
                debug!(
                    job_id = job.id,
                    key = rec.key,
                    bucket_source = source_bucket,
                    bucket_target = &target_bucket,
                    "success retry replicate object"
                );
                counters.inc_success(bytes);
            }
            Err(e) => {
                counters.inc_failure(rec.size);
                let new_rec = FailureRecord {
                    key: rec.key.clone(),
                    version_id: rec.version_id.clone(),
                    error: e.to_string(),
                    timestamp: Utc::now(),
                    size: rec.size,
                };
                if let Err(se) = store.append_failure(&job.id, &new_rec).await {
                    error!(job_id = job.id, "fail write retry failure record: {se}");
                }
            }
        }
    }

    Ok(())
}

fn build_target_key(source_key: &str, target_prefix: &str) -> String {
    if target_prefix.is_empty() {
        source_key.to_owned()
    } else {
        format!("{}/{}", target_prefix.trim_end_matches('/'), source_key)
    }
}

/// Check whether the object already exists at the target with a matching ETag.
async fn check_target_exists<S: StorageAPI + 'static>(
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    target_bucket: &str,
    target_key: &str,
    source_etag: Option<&str>,
) -> bool {
    if let Some(tc) = target_client {
        match tc.head_object(target_key).await {
            Ok(Some(head)) => source_etag.map_or(true, |se| head.etag.as_deref() == Some(se)),
            Ok(None) => false,
            Err(e) => {
                warn!("Head target failed for {target_key}: {e}");
                false
            }
        }
    } else {
        match ecstore
            .get_object_info(target_bucket, target_key, &ObjectOptions::default())
            .await
        {
            Ok(info) => source_etag.map_or(true, |se| info.etag.as_deref() == Some(se)),
            Err(_) => false,
        }
    }
}

/// GET from source and PUT to target. Returns bytes transferred.
async fn transfer_object<S: StorageAPI + 'static>(
    source_client: Option<&BatchS3Client>,
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    source_bucket: &str,
    source_key: &str,
    target_bucket: &str,
    target_key: &str,
) -> Result<i64> {
    let (body, size) = if let Some(sc) = source_client {
        sc.get_object(source_key).await?
    } else {
        get_local_object(ecstore.as_ref(), source_bucket, source_key).await?
    };

    if let Some(tc) = target_client {
        tc.put_object(target_key, body, HashMap::new()).await?;
    } else {
        put_local_object(ecstore.as_ref(), target_bucket, target_key, body).await?;
    }

    Ok(size)
}

async fn get_local_object<S: StorageAPI>(store: &S, bucket: &str, key: &str) -> Result<(Bytes, i64)> {
    let mut reader = store
        .get_object_reader(bucket, key, None, HeaderMap::new(), &ObjectOptions::default())
        .await
        .map_err(|e| BatchError::Transfer(e.to_string()))?;

    let size = reader.object_info.size;
    let data = reader.read_all().await.map_err(|e| BatchError::Transfer(e.to_string()))?;
    Ok((Bytes::from(data), size))
}

async fn put_local_object<S: StorageAPI>(store: &S, bucket: &str, key: &str, body: Bytes) -> Result<()> {
    let mut reader = PutObjReader::from_vec(body.into());
    store
        .put_object(bucket, key, &mut reader, &ObjectOptions::default())
        .await
        .map_err(|e| BatchError::Transfer(e.to_string()))?;
    Ok(())
}

/// Apply filter rules to decide whether to include an object in the transfer.
fn passes_filter(obj: &ListedObject, filter: Option<&FilterYaml>) -> bool {
    let Some(f) = filter else {
        return true;
    };

    let now = Utc::now();

    if let (Some(lm), Some(newer_than)) = (obj.last_modified, f.newer_than.as_ref()) {
        if now - lm > parse_duration_to_chrono(newer_than) {
            return false;
        }
    }

    if let (Some(lm), Some(older_than)) = (obj.last_modified, f.older_than.as_ref()) {
        if now - lm < parse_duration_to_chrono(older_than) {
            return false;
        }
    }

    if let (Some(lm), Some(created_after)) = (obj.last_modified, f.created_after.as_ref()) {
        if let Ok(dt) = created_after.parse::<chrono::DateTime<Utc>>() {
            if lm < dt {
                return false;
            }
        }
    }

    if let (Some(lm), Some(created_before)) = (obj.last_modified, f.created_before.as_ref()) {
        if let Ok(dt) = created_before.parse::<chrono::DateTime<Utc>>() {
            if lm > dt {
                return false;
            }
        }
    }

    true
}

fn parse_duration_to_chrono(s: &str) -> chrono::Duration {
    let s = s.trim();
    if s.ends_with('d') {
        chrono::Duration::days(s[..s.len() - 1].parse().unwrap_or(0))
    } else if s.ends_with('h') {
        chrono::Duration::hours(s[..s.len() - 1].parse().unwrap_or(0))
    } else if s.ends_with('m') {
        chrono::Duration::minutes(s[..s.len() - 1].parse().unwrap_or(0))
    } else if s.ends_with('s') {
        chrono::Duration::seconds(s[..s.len() - 1].parse().unwrap_or(0))
    } else {
        chrono::Duration::zero()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yaml::FilterYaml;

    #[test]
    fn test_passes_filter_no_filter() {
        let obj = ListedObject {
            key: "test.bin".into(),
            size: 1024,
            etag: None,
            last_modified: None,
        };
        assert!(passes_filter(&obj, None));
    }

    #[test]
    fn test_passes_filter_newer_than_excludes_old_object() {
        let obj = ListedObject {
            key: "old.bin".into(),
            size: 512,
            etag: None,
            last_modified: Some(Utc::now() - chrono::Duration::days(30)),
        };
        let filter = FilterYaml {
            newer_than: Some("7d".into()),
            ..Default::default()
        };
        assert!(!passes_filter(&obj, Some(&filter)));
    }

    #[test]
    fn test_passes_filter_newer_than_includes_recent_object() {
        let obj = ListedObject {
            key: "recent.bin".into(),
            size: 512,
            etag: None,
            last_modified: Some(Utc::now() - chrono::Duration::hours(1)),
        };
        let filter = FilterYaml {
            newer_than: Some("7d".into()),
            ..Default::default()
        };
        assert!(passes_filter(&obj, Some(&filter)));
    }

    #[test]
    fn test_passes_filter_with_no_last_modified() {
        let obj = ListedObject {
            key: "no-time.bin".into(),
            size: 512,
            etag: None,
            last_modified: None,
        };
        let filter = FilterYaml {
            newer_than: Some("7d".into()),
            ..Default::default()
        };
        // If last_modified is None, filter conditions that depend on it are skipped.
        assert!(passes_filter(&obj, Some(&filter)));
    }

    #[test]
    fn test_build_target_key_no_prefix() {
        assert_eq!(build_target_key("path/to/object.bin", ""), "path/to/object.bin");
    }

    #[test]
    fn test_build_target_key_with_prefix() {
        assert_eq!(build_target_key("object.bin", "backup/"), "backup/object.bin");
        assert_eq!(build_target_key("object.bin", "backup"), "backup/object.bin");
    }

    #[test]
    fn test_parse_duration_to_chrono() {
        assert_eq!(parse_duration_to_chrono("7d"), chrono::Duration::days(7));
        assert_eq!(parse_duration_to_chrono("24h"), chrono::Duration::hours(24));
        assert_eq!(parse_duration_to_chrono("30m"), chrono::Duration::minutes(30));
        assert_eq!(parse_duration_to_chrono("60s"), chrono::Duration::seconds(60));
        assert_eq!(parse_duration_to_chrono("unknown"), chrono::Duration::zero());
    }
}
