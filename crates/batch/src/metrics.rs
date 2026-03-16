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

//! Prometheus metrics for batch job operations.
//!
//! Call [`register_batch_metrics`] once at service startup to register help
//! text with the metrics recorder. All other functions are safe to call from
//! multiple threads at any point after that.

use metrics::{counter, describe_counter, describe_gauge, gauge};

const METRIC_JOBS_ACTIVE: &str = "rustfs_batch_jobs_active";
const METRIC_JOBS_TOTAL: &str = "rustfs_batch_jobs_total";
const METRIC_JOB_DURATION_SECONDS: &str = "rustfs_batch_job_duration_seconds";
const METRIC_OBJECTS_PROCESSED: &str = "rustfs_batch_objects_processed_total";
const METRIC_OBJECTS_FAILED: &str = "rustfs_batch_objects_failed_total";
const METRIC_BYTES_TRANSFERRED: &str = "rustfs_batch_bytes_transferred_total";

const LABEL_JOB_TYPE: &str = "job_type";
const LABEL_JOB_ID: &str = "job_id";
const LABEL_SOURCE_BUCKET: &str = "source_bucket";
const LABEL_TARGET_BUCKET: &str = "target_bucket";
const LABEL_STATUS: &str = "status";

/// Register Prometheus help text for all batch metrics.
///
/// Must be called once during [`crate::init_batch_service`]. Duplicate
/// registrations are handled gracefully by the `metrics` crate.
pub fn register_batch_metrics() {
    describe_gauge!(METRIC_JOBS_ACTIVE, "Number of batch jobs currently in progress");
    describe_counter!(
        METRIC_JOBS_TOTAL,
        "Total number of batch jobs that have reached a terminal state (completed, failed, or cancelled)"
    );
    describe_gauge!(
        METRIC_JOB_DURATION_SECONDS,
        "Duration in seconds of the most recently completed batch job, by outcome"
    );
    describe_counter!(METRIC_OBJECTS_PROCESSED, "Total number of objects successfully transferred by batch jobs");
    describe_counter!(METRIC_OBJECTS_FAILED, "Total number of objects that failed to transfer in batch jobs");
    describe_counter!(METRIC_BYTES_TRANSFERRED, "Total bytes successfully transferred by batch jobs");
}

/// Increment the active-jobs gauge when a job transitions to in-progress.
pub fn record_job_started(job_type: &str, source_bucket: &str, target_bucket: &str) {
    gauge!(
        METRIC_JOBS_ACTIVE,
        LABEL_JOB_TYPE => job_type.to_owned(),
        LABEL_SOURCE_BUCKET => source_bucket.to_owned(),
        LABEL_TARGET_BUCKET => target_bucket.to_owned(),
    )
    .increment(1.0);
}

/// Decrement the active-jobs gauge, increment the terminal counter, and record
/// the job duration when a job reaches a terminal state.
///
/// `status` should be one of `"completed"`, `"failed"`, or `"cancelled"`.
pub fn record_job_terminal(job_type: &str, source_bucket: &str, target_bucket: &str, status: &str, elapsed_secs: f64) {
    gauge!(
        METRIC_JOBS_ACTIVE,
        LABEL_JOB_TYPE => job_type.to_owned(),
        LABEL_SOURCE_BUCKET => source_bucket.to_owned(),
        LABEL_TARGET_BUCKET => target_bucket.to_owned(),
    )
    .decrement(1.0);

    counter!(
        METRIC_JOBS_TOTAL,
        LABEL_JOB_TYPE => job_type.to_owned(),
        LABEL_SOURCE_BUCKET => source_bucket.to_owned(),
        LABEL_TARGET_BUCKET => target_bucket.to_owned(),
        LABEL_STATUS => status.to_owned(),
    )
    .increment(1);

    gauge!(
        METRIC_JOB_DURATION_SECONDS,
        LABEL_JOB_TYPE => job_type.to_owned(),
        LABEL_SOURCE_BUCKET => source_bucket.to_owned(),
        LABEL_TARGET_BUCKET => target_bucket.to_owned(),
        LABEL_STATUS => status.to_owned(),
    )
    .set(elapsed_secs);
}

/// Increment object-processed and bytes-transferred counters for a successful transfer.
pub fn record_object_processed(job_type: &str, job_id: &str, source_bucket: &str, bytes: i64) {
    counter!(
        METRIC_OBJECTS_PROCESSED,
        LABEL_JOB_TYPE => job_type.to_owned(),
        LABEL_JOB_ID => job_id.to_owned(),
        LABEL_SOURCE_BUCKET => source_bucket.to_owned(),
    )
    .increment(1);

    counter!(
        METRIC_BYTES_TRANSFERRED,
        LABEL_JOB_TYPE => job_type.to_owned(),
        LABEL_JOB_ID => job_id.to_owned(),
        LABEL_SOURCE_BUCKET => source_bucket.to_owned(),
    )
    .increment(bytes.max(0) as u64);
}

/// Increment the object-failed counter for a transfer that did not succeed.
pub fn record_object_failed(job_type: &str, job_id: &str, source_bucket: &str) {
    counter!(
        METRIC_OBJECTS_FAILED,
        LABEL_JOB_TYPE => job_type.to_owned(),
        LABEL_JOB_ID => job_id.to_owned(),
        LABEL_SOURCE_BUCKET => source_bucket.to_owned(),
    )
    .increment(1);
}
