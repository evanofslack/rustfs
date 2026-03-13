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
//!     ETag, it is skipped. Otherwise it is transferred.
//!  4. Single-part objects are streamed directly (GET → PUT, no full-body buffer).
//!  5. Multipart objects (ETag contains "-N" suffix) are transferred part-by-part,
//!     preserving the original part boundaries.
//!  6. Failures are appended to `failures.jsonl`.
//!  7. After enumeration, if failures remain and retries are configured, the
//!     failures file is replayed, then cleared on success.

use crate::client::{BatchS3Client, ListedObject};
use crate::error::{BatchError, Result};
use crate::job::{BatchJob, BatchJobStatusType, JobControl, JobCounters};
use crate::registry::JobRegistry;
use crate::store::{BatchStore, FailureRecord};
use crate::yaml::{FilterYaml, ReplicateJobYaml};
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::body::SdkBody;
use chrono::Utc;
use futures_util::StreamExt as _;
use http::HeaderMap;
use http_body::Frame;
use http_body_util::StreamBody;
use rustfs_ecstore::store_api::{CompletePart, ObjectOptions, PutObjReader, StorageAPI};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::io::ReaderStream;
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
    /// Number of parts if this is a multipart object (from ETag "-N" suffix), else `None`.
    part_count: Option<u32>,
}

/// Parse the part count from a multipart ETag like `"abc123-5"` → `Some(5)`.
/// Returns `None` for single-part ETags.
fn parse_part_count(etag: &str) -> Option<u32> {
    let etag = etag.trim_matches('"');
    let suffix = etag.rsplit_once('-')?.1;
    suffix.parse::<u32>().ok().filter(|&n| n > 0)
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

        // Reset failure counters before the retry pass so status metrics reflect
        // only the current attempt's outcome, not the cumulative total across passes.
        counters.reset_failures();

        // Update retry_attempts in the registry snapshot so status-job responses
        // report the current attempt number.
        if let Some(mut snapshot) = registry.get_job(&job.id).await {
            snapshot.retry_attempts = attempt + 1;
            registry.update_job_snapshot(&job.id, snapshot).await;
        }

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
            "batch replicate object page"
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

            let transfer_result = dispatch_transfer(
                source_client,
                target_client,
                ecstore.clone(),
                &source_bucket,
                &item.key,
                &target_bucket,
                &target_key,
                item.size,
                item.part_count,
            )
            .await;

            match transfer_result {
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
                        "failure replicate object: {e}"
                    );
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
            .map(|obj| {
                let part_count = obj.etag.as_deref().and_then(parse_part_count);
                WorkItem {
                    key: obj.key,
                    etag: obj.etag,
                    size: obj.size,
                    part_count,
                }
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
            .map(|obj| {
                let part_count = obj.etag.as_deref().and_then(parse_part_count);
                WorkItem {
                    key: obj.name,
                    etag: obj.etag,
                    size: obj.size,
                    part_count,
                }
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
        let part_count = rec.key.rsplit_once('-').and_then(|(_, s)| s.parse::<u32>().ok());

        match dispatch_transfer(
            source_client,
            target_client,
            ecstore.clone(),
            source_bucket,
            &rec.key,
            target_bucket,
            &target_key,
            rec.size,
            part_count,
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

/// Route a single object to the appropriate transfer function based on
/// whether it is single-part or multipart. Returns bytes transferred.
#[allow(clippy::too_many_arguments)]
async fn dispatch_transfer<S: StorageAPI + 'static>(
    source_client: Option<&BatchS3Client>,
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    source_bucket: &str,
    source_key: &str,
    target_bucket: &str,
    target_key: &str,
    size: i64,
    part_count: Option<u32>,
) -> Result<i64> {
    if let Some(n) = part_count {
        multipart_transfer_object(
            source_client,
            target_client,
            ecstore,
            source_bucket,
            source_key,
            target_bucket,
            target_key,
            n,
        )
        .await
    } else {
        stream_transfer_object(
            source_client,
            target_client,
            ecstore,
            source_bucket,
            source_key,
            target_bucket,
            target_key,
            size,
        )
        .await
    }
}

/// Transfer a single-part (or unknown-layout) object by streaming GET → PUT
/// without buffering the full body in memory.
///
/// The four direction cases:
/// - Local → Remote: `get_object_reader` → `ByteStream` → `put_object_stream`
/// - Remote → Local: `get_object_stream` → `AsyncRead` → `put_object`
/// - Local → Local:  `copy_object` (server-side, no byte movement through this process)
/// - Remote → Remote: `get_object_stream` → `put_object_stream` (piped)
#[allow(clippy::too_many_arguments)]
async fn stream_transfer_object<S: StorageAPI + 'static>(
    source_client: Option<&BatchS3Client>,
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    source_bucket: &str,
    source_key: &str,
    target_bucket: &str,
    target_key: &str,
    size: i64,
) -> Result<i64> {
    match (source_client, target_client) {
        // Local → Local: read from ECStore, write back to ECStore
        (None, None) => {
            let reader = ecstore
                .get_object_reader(source_bucket, source_key, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .map_err(|e| BatchError::Transfer(e.to_string()))?;

            let bytes = reader.object_info.size;
            let mut put_reader = PutObjReader::from_async_read(reader.stream, bytes);

            ecstore
                .put_object(target_bucket, target_key, &mut put_reader, &ObjectOptions::default())
                .await
                .map_err(|e| BatchError::Transfer(e.to_string()))?;

            Ok(bytes)
        }

        // Local → Remote: read from ECStore, stream to S3
        (None, Some(tc)) => {
            let reader = ecstore
                .get_object_reader(source_bucket, source_key, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .map_err(|e| BatchError::Transfer(e.to_string()))?;

            let actual_size = reader.object_info.size;
            let stream = async_read_to_bytestream(reader.stream);
            tc.put_object_stream(target_key, stream, actual_size, HashMap::new()).await?;
            Ok(actual_size)
        }

        // Remote → Local: stream from S3, write to ECStore
        (Some(sc), None) => {
            let (byte_stream, remote_size) = sc.get_object_stream(source_key).await?;
            let actual_size = if remote_size > 0 { remote_size } else { size };

            // ByteStream implements AsyncRead via the SDK's built-in adapter
            let async_read = byte_stream.into_async_read();
            let mut put_reader = PutObjReader::from_async_read(async_read, actual_size);

            ecstore
                .put_object(target_bucket, target_key, &mut put_reader, &ObjectOptions::default())
                .await
                .map_err(|e| BatchError::Transfer(e.to_string()))?;

            Ok(actual_size)
        }

        // Remote → Remote: stream from S3 source directly to S3 target
        (Some(sc), Some(tc)) => {
            let (byte_stream, remote_size) = sc.get_object_stream(source_key).await?;
            let actual_size = if remote_size > 0 { remote_size } else { size };
            tc.put_object_stream(target_key, byte_stream, actual_size, HashMap::new())
                .await?;
            Ok(actual_size)
        }
    }
}

/// Transfer a multipart object part-by-part, preserving original part boundaries.
///
/// Opens a multipart upload on the target, fetches each source part by number
/// (preserving original part sizes), uploads it, then completes the upload.
/// On any error the target upload is aborted before returning.
#[allow(clippy::too_many_arguments)]
async fn multipart_transfer_object<S: StorageAPI + 'static>(
    source_client: Option<&BatchS3Client>,
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    source_bucket: &str,
    source_key: &str,
    target_bucket: &str,
    target_key: &str,
    part_count: u32,
) -> Result<i64> {
    let upload_id = open_target_multipart(target_client, ecstore.clone(), target_bucket, target_key).await?;

    let result = do_multipart_parts(
        source_client,
        target_client,
        ecstore.clone(),
        source_bucket,
        source_key,
        target_bucket,
        target_key,
        &upload_id,
        part_count,
    )
    .await;

    match result {
        Ok((completed_parts, total_bytes)) => {
            complete_target_multipart(target_client, ecstore, target_bucket, target_key, &upload_id, completed_parts).await?;
            Ok(total_bytes)
        }
        Err(e) => {
            if let Err(ae) = abort_target_multipart(target_client, ecstore, target_bucket, target_key, &upload_id).await {
                warn!("abort multipart upload failed after transfer error ({}): {ae}", source_key);
            }
            Err(e)
        }
    }
}

/// Open a multipart upload on the target (remote or local).
async fn open_target_multipart<S: StorageAPI>(
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    target_bucket: &str,
    target_key: &str,
) -> Result<String> {
    if let Some(tc) = target_client {
        tc.create_multipart_upload(target_key).await
    } else {
        let result = ecstore
            .new_multipart_upload(target_bucket, target_key, &ObjectOptions::default())
            .await
            .map_err(|e| BatchError::Transfer(e.to_string()))?;
        Ok(result.upload_id)
    }
}

/// Complete a multipart upload on the target.
async fn complete_target_multipart<S: StorageAPI>(
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    target_bucket: &str,
    target_key: &str,
    upload_id: &str,
    parts: Vec<(i32, String)>,
) -> Result<()> {
    if let Some(tc) = target_client {
        tc.complete_multipart_upload(target_key, upload_id, parts).await
    } else {
        let complete_parts: Vec<CompletePart> = parts
            .into_iter()
            .map(|(num, etag)| CompletePart {
                part_num: num as usize,
                etag: Some(etag),
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_sha1: None,
                checksum_sha256: None,
                checksum_crc64nvme: None,
            })
            .collect();

        ecstore
            .clone()
            .complete_multipart_upload(target_bucket, target_key, upload_id, complete_parts, &ObjectOptions::default())
            .await
            .map_err(|e| BatchError::Transfer(e.to_string()))?;
        Ok(())
    }
}

/// Abort a multipart upload on the target.
async fn abort_target_multipart<S: StorageAPI>(
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    target_bucket: &str,
    target_key: &str,
    upload_id: &str,
) -> Result<()> {
    if let Some(tc) = target_client {
        tc.abort_multipart_upload(target_key, upload_id).await
    } else {
        ecstore
            .abort_multipart_upload(target_bucket, target_key, upload_id, &ObjectOptions::default())
            .await
            .map_err(|e| BatchError::Transfer(e.to_string()))
    }
}

/// Transfer all parts for a multipart object.
/// Returns a list of `(part_number, etag)` pairs and the total bytes transferred.
#[allow(clippy::too_many_arguments)]
async fn do_multipart_parts<S: StorageAPI + 'static>(
    source_client: Option<&BatchS3Client>,
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    source_bucket: &str,
    source_key: &str,
    target_bucket: &str,
    target_key: &str,
    upload_id: &str,
    part_count: u32,
) -> Result<(Vec<(i32, String)>, i64)> {
    let mut completed_parts: Vec<(i32, String)> = Vec::with_capacity(part_count as usize);
    let mut total_bytes: i64 = 0;

    for part_num in 1..=part_count {
        let (part_stream, part_size) =
            get_source_part_stream(source_client, ecstore.clone(), source_bucket, source_key, part_num).await?;

        let part_etag = put_target_part(
            target_client,
            ecstore.clone(),
            target_bucket,
            target_key,
            upload_id,
            part_num,
            part_stream,
            part_size,
        )
        .await?;

        completed_parts.push((part_num as i32, part_etag));
        total_bytes += part_size;
    }

    Ok((completed_parts, total_bytes))
}

/// Fetch a single part from the source as a `ByteStream` + size pair.
///
/// For remote sources, uses the S3 `PartNumber` query parameter.
/// For local sources, uses `get_object_reader` with `ObjectOptions { part_number }`.
async fn get_source_part_stream<S: StorageAPI>(
    source_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    source_bucket: &str,
    source_key: &str,
    part_num: u32,
) -> Result<(PartStream, i64)> {
    if let Some(sc) = source_client {
        let (byte_stream, size) = sc.get_object_part_stream(source_key, part_num).await?;
        Ok((PartStream::Remote(byte_stream), size))
    } else {
        let opts = ObjectOptions {
            part_number: Some(part_num as usize),
            ..Default::default()
        };
        let reader = ecstore
            .get_object_reader(source_bucket, source_key, None, HeaderMap::new(), &opts)
            .await
            .map_err(|e| BatchError::Transfer(e.to_string()))?;
        let size = reader.object_info.size;
        Ok((PartStream::Local(reader), size))
    }
}

/// Upload a single part to the target.
async fn put_target_part<S: StorageAPI + 'static>(
    target_client: Option<&BatchS3Client>,
    ecstore: Arc<S>,
    target_bucket: &str,
    target_key: &str,
    upload_id: &str,
    part_num: u32,
    part_stream: PartStream,
    part_size: i64,
) -> Result<String> {
    if let Some(tc) = target_client {
        let byte_stream = part_stream.into_byte_stream(part_size);
        tc.upload_part(target_key, upload_id, part_num, byte_stream, part_size).await
    } else {
        let mut put_reader = part_stream.into_put_obj_reader(part_size);
        let info = ecstore
            .put_object_part(
                target_bucket,
                target_key,
                upload_id,
                part_num as usize,
                &mut put_reader,
                &ObjectOptions::default(),
            )
            .await
            .map_err(|e| BatchError::Transfer(e.to_string()))?;
        Ok(info.etag.unwrap_or_default())
    }
}

/// Unifies local (`GetObjectReader`) and remote (`ByteStream`) part sources so that
/// `put_target_part` can consume either without an intermediate buffer.
enum PartStream {
    Remote(ByteStream),
    Local(rustfs_ecstore::store_api::GetObjectReader),
}

impl PartStream {
    fn into_byte_stream(self, _size: i64) -> ByteStream {
        match self {
            PartStream::Remote(bs) => bs,
            PartStream::Local(reader) => async_read_to_bytestream(reader.stream),
        }
    }

    fn into_put_obj_reader(self, size: i64) -> PutObjReader {
        match self {
            PartStream::Remote(bs) => {
                let async_read = bs.into_async_read();
                PutObjReader::from_async_read(async_read, size)
            }
            PartStream::Local(reader) => PutObjReader::from_async_read(reader.stream, size),
        }
    }
}

/// Wrap an `AsyncRead` as a non-retryable `ByteStream` suitable for AWS SDK PUT calls.
fn async_read_to_bytestream(reader: impl tokio::io::AsyncRead + Send + Sync + Unpin + 'static) -> ByteStream {
    let stream = ReaderStream::new(reader);
    let body = StreamBody::new(stream.map(|r: std::io::Result<_>| r.map(Frame::data)));
    ByteStream::new(SdkBody::from_body_1_x(body))
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yaml::FilterYaml;

    #[test]
    fn test_parse_part_count_multipart() {
        assert_eq!(parse_part_count("abc123-5"), Some(5));
        assert_eq!(parse_part_count("\"abc123-5\""), Some(5)); // quoted ETag
        assert_eq!(parse_part_count("deadbeef-1"), Some(1));
        assert_eq!(parse_part_count("deadbeef-100"), Some(100));
    }

    #[test]
    fn test_parse_part_count_single_part() {
        // A regular MD5 ETag has no "-N" suffix with a pure numeric component
        assert_eq!(parse_part_count("d41d8cd98f00b204e9800998ecf8427e"), None);
        // Zero is not a valid part count
        assert_eq!(parse_part_count("abc-0"), None);
    }

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
