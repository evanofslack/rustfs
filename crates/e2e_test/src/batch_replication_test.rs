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

//! E2E integration tests for local→local batch replication jobs.
//!
//! These tests require the `batch-operations` feature flag and a running RustFS
//! binary built with the same feature. Set `RUSTFS_BUILD_FEATURES=batch-operations`
//! before running so `common::build_rustfs_binary` picks it up.
//!
//! Run with:
//! ```
//! RUSTFS_BUILD_FEATURES=batch-operations \
//!   cargo test --package e2e_test --features batch-operations \
//!     batch_replication -- --ignored
//! ```

use crate::common::{RustFSTestClusterEnvironment, RustFSTestEnvironment, awscurl_get, awscurl_post, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use serial_test::serial;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a local→local replicate YAML.
///
/// Omitting `endpoint` and `credentials` tells the server to treat
/// both endpoints as local (same RustFS instance).
fn local_replicate_yaml(src_bucket: &str, dst_bucket: &str) -> String {
    format!(
        r#"replicate:
  apiVersion: v1
  source:
    type: rustfs
    bucket: {src_bucket}
  target:
    type: rustfs
    bucket: {dst_bucket}
"#
    )
}

/// Poll `status-job?jobId=<id>` until `LastMetric.complete` or `LastMetric.failed`
/// is true, or until `timeout_secs` elapses.
///
/// Returns the parsed `LastMetric` JSON object on success so callers can assert
/// on `replicate.objects`, `replicate.bytesTransferred`, etc.
async fn poll_until_complete(
    base_url: &str,
    job_id: &str,
    access_key: &str,
    secret_key: &str,
    timeout_secs: u64,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{base_url}/rustfs/admin/v3/status-job?jobId={job_id}");
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        let resp = awscurl_get(&url, access_key, secret_key).await?;
        let v: serde_json::Value = serde_json::from_str(&resp)?;

        let metric = &v["LastMetric"];
        if metric["complete"].as_bool().unwrap_or(false) {
            return Ok(metric.clone());
        }
        if metric["failed"].as_bool().unwrap_or(false) {
            return Err(format!("job {job_id} ended in failed state: {metric}").into());
        }

        if std::time::Instant::now() >= deadline {
            return Err(format!("timed out after {timeout_secs}s waiting for job {job_id} to complete").into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Start a job and return the job ID string.
async fn start_job(
    base_url: &str,
    yaml: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{base_url}/rustfs/admin/v3/start-job");
    let resp = awscurl_post(&url, yaml, access_key, secret_key).await?;
    let v: serde_json::Value = serde_json::from_str(&resp)?;
    let id = v["id"].as_str().ok_or("start-job response missing 'id'")?.to_owned();
    info!(job_id = %id, "batch job started");
    Ok(id)
}

// ---------------------------------------------------------------------------
// Single-node tests
// ---------------------------------------------------------------------------

/// Replicate 3 small single-part objects, verify content and metrics.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; requires batch-operations feature. Set RUSTFS_BUILD_FEATURES=batch-operations"]
async fn batch_replicate_single_part_local_to_local() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();

    let src = "batch-sp-src";
    let dst = "batch-sp-dst";
    client.create_bucket().bucket(src).send().await?;
    client.create_bucket().bucket(dst).send().await?;

    // Upload 3 small objects with known content.
    let objects: Vec<(&str, &[u8])> = vec![
        ("obj-a", b"hello from object a"),
        ("obj-b", b"hello from object b"),
        ("obj-c", b"hello from object c"),
    ];
    let total_bytes: i64 = objects.iter().map(|(_, b)| b.len() as i64).sum();

    for (key, body) in &objects {
        client
            .put_object()
            .bucket(src)
            .key(*key)
            .body(ByteStream::from_static(body))
            .send()
            .await?;
    }

    let yaml = local_replicate_yaml(src, dst);
    let job_id = start_job(&env.url, &yaml, &env.access_key, &env.secret_key).await?;

    let metric = poll_until_complete(&env.url, &job_id, &env.access_key, &env.secret_key, 60).await?;

    // Check replication counters.
    let rep = &metric["replicate"];
    assert_eq!(rep["objects"].as_i64().unwrap_or(0), 3, "expected 3 objects replicated");
    assert_eq!(rep["objectsFailed"].as_i64().unwrap_or(-1), 0, "expected 0 failures");
    assert_eq!(
        rep["bytesTransferred"].as_i64().unwrap_or(0),
        total_bytes,
        "bytesTransferred must equal sum of object sizes"
    );

    // Verify every object is present in dst with matching content.
    for (key, expected) in &objects {
        let get = client.get_object().bucket(dst).key(*key).send().await?;
        let body = get.body.collect().await?.into_bytes();
        assert_eq!(body.as_ref(), *expected, "content mismatch for {key} in dst bucket");
    }

    info!("PASSED: batch_replicate_single_part_local_to_local");
    Ok(())
}

/// Replicate one multipart object (3 × 5 MiB parts), verify size and metrics.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; requires batch-operations feature. Set RUSTFS_BUILD_FEATURES=batch-operations"]
async fn batch_replicate_multipart_local_to_local() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();

    let src = "batch-mp-src";
    let dst = "batch-mp-dst";
    client.create_bucket().bucket(src).send().await?;
    client.create_bucket().bucket(dst).send().await?;

    // Upload a 3-part multipart object (each part = 5 MiB, minimum allowed).
    const PART_SIZE: usize = 5 * 1024 * 1024;
    const NUM_PARTS: usize = 3;
    let part_data: Vec<u8> = (0u8..=255).cycle().take(PART_SIZE).collect();
    let total_bytes = (PART_SIZE * NUM_PARTS) as i64;
    let key = "multipart-obj";

    let mpu = client.create_multipart_upload().bucket(src).key(key).send().await?;
    let upload_id = mpu.upload_id().ok_or("no upload_id")?;

    let mut completed_parts = Vec::new();
    for part_num in 1..=(NUM_PARTS as i32) {
        let resp = client
            .upload_part()
            .bucket(src)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_num)
            .body(ByteStream::from(part_data.clone()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_num)
                .e_tag(resp.e_tag().unwrap_or(""))
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket(src)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;

    let yaml = local_replicate_yaml(src, dst);
    let job_id = start_job(&env.url, &yaml, &env.access_key, &env.secret_key).await?;

    let metric = poll_until_complete(&env.url, &job_id, &env.access_key, &env.secret_key, 120).await?;

    let rep = &metric["replicate"];
    assert_eq!(rep["objects"].as_i64().unwrap_or(0), 1, "expected 1 object replicated");
    assert_eq!(rep["objectsFailed"].as_i64().unwrap_or(-1), 0, "expected 0 failures");
    assert_eq!(
        rep["bytesTransferred"].as_i64().unwrap_or(0),
        total_bytes,
        "bytesTransferred must equal {total_bytes}"
    );

    // Verify object exists in dst with correct size.
    let head = client.head_object().bucket(dst).key(key).send().await?;
    assert_eq!(
        head.content_length().unwrap_or(0),
        total_bytes,
        "dst object content-length must be {total_bytes}"
    );

    info!("PASSED: batch_replicate_multipart_local_to_local");
    Ok(())
}

/// Verify list-jobs reflects the job during and after completion.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; requires batch-operations feature. Set RUSTFS_BUILD_FEATURES=batch-operations"]
async fn batch_list_and_status_correct() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();

    let src = "batch-list-src";
    let dst = "batch-list-dst";
    client.create_bucket().bucket(src).send().await?;
    client.create_bucket().bucket(dst).send().await?;

    client
        .put_object()
        .bucket(src)
        .key("item")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await?;

    let yaml = local_replicate_yaml(src, dst);
    let job_id = start_job(&env.url, &yaml, &env.access_key, &env.secret_key).await?;

    // While the job may still be in-progress, list-jobs must already include it.
    let list_url = format!("{}/rustfs/admin/v3/list-jobs?jobType=replicate", env.url);
    let list_resp = awscurl_get(&list_url, &env.access_key, &env.secret_key).await?;
    let list: serde_json::Value = serde_json::from_str(&list_resp)?;
    let jobs = list["jobs"].as_array().ok_or("list-jobs response missing 'jobs'")?;
    let found = jobs.iter().any(|j| j["id"].as_str() == Some(&job_id));
    assert!(found, "job {job_id} must appear in list-jobs immediately after start");

    // Wait for completion.
    poll_until_complete(&env.url, &job_id, &env.access_key, &env.secret_key, 60).await?;

    // Job must still appear in list after completion (retention window).
    let list_resp2 = awscurl_get(&list_url, &env.access_key, &env.secret_key).await?;
    let list2: serde_json::Value = serde_json::from_str(&list_resp2)?;
    let jobs2 = list2["jobs"].as_array().ok_or("list-jobs response missing 'jobs'")?;
    let found2 = jobs2.iter().any(|j| j["id"].as_str() == Some(&job_id));
    assert!(found2, "completed job {job_id} must still appear in list-jobs");

    info!("PASSED: batch_list_and_status_correct");
    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-node (cluster) tests
// ---------------------------------------------------------------------------

/// Replicate 3 small objects in a 4-node cluster; poll status from a peer node.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a 4-node rustfs cluster; requires batch-operations feature. Set RUSTFS_BUILD_FEATURES=batch-operations"]
async fn batch_replicate_single_part_cluster() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.start().await?;

    let client0 = cluster.create_s3_client(0)?;
    let src = "cluster-sp-src";
    let dst = "cluster-sp-dst";
    client0.create_bucket().bucket(src).send().await?;
    client0.create_bucket().bucket(dst).send().await?;

    let objects: Vec<(&str, &[u8])> = vec![
        ("k1", b"cluster object one"),
        ("k2", b"cluster object two"),
        ("k3", b"cluster object three"),
    ];
    let total_bytes: i64 = objects.iter().map(|(_, b)| b.len() as i64).sum();

    for (key, body) in &objects {
        client0
            .put_object()
            .bucket(src)
            .key(*key)
            .body(ByteStream::from_static(body))
            .send()
            .await?;
    }

    // Submit job via node 0.
    let node0_url = &cluster.nodes[0].url;
    let yaml = local_replicate_yaml(src, dst);
    let job_id = start_job(node0_url, &yaml, &cluster.access_key, &cluster.secret_key).await?;

    // Poll status from node 1 to exercise cross-node routing.
    let node1_url = &cluster.nodes[1].url;
    let metric = poll_until_complete(node1_url, &job_id, &cluster.access_key, &cluster.secret_key, 120).await?;

    let rep = &metric["replicate"];
    assert_eq!(rep["objects"].as_i64().unwrap_or(0), 3);
    assert_eq!(rep["objectsFailed"].as_i64().unwrap_or(-1), 0);
    assert_eq!(rep["bytesTransferred"].as_i64().unwrap_or(0), total_bytes);

    // Verify content in dst via node 0.
    for (key, expected) in &objects {
        let get = client0.get_object().bucket(dst).key(*key).send().await?;
        let body = get.body.collect().await?.into_bytes();
        assert_eq!(body.as_ref(), *expected, "content mismatch for {key} in dst");
    }

    info!("PASSED: batch_replicate_single_part_cluster");
    Ok(())
}

/// Replicate a 3-part multipart object in a 4-node cluster; poll from peer node.
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a 4-node rustfs cluster; requires batch-operations feature. Set RUSTFS_BUILD_FEATURES=batch-operations"]
async fn batch_replicate_multipart_cluster() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.start().await?;

    let client0 = cluster.create_s3_client(0)?;
    let src = "cluster-mp-src";
    let dst = "cluster-mp-dst";
    client0.create_bucket().bucket(src).send().await?;
    client0.create_bucket().bucket(dst).send().await?;

    const PART_SIZE: usize = 5 * 1024 * 1024;
    const NUM_PARTS: usize = 3;
    let part_data: Vec<u8> = (0u8..=255).cycle().take(PART_SIZE).collect();
    let total_bytes = (PART_SIZE * NUM_PARTS) as i64;
    let key = "cluster-mp-obj";

    let mpu = client0.create_multipart_upload().bucket(src).key(key).send().await?;
    let upload_id = mpu.upload_id().ok_or("no upload_id")?;

    let mut completed_parts = Vec::new();
    for part_num in 1..=(NUM_PARTS as i32) {
        let resp = client0
            .upload_part()
            .bucket(src)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_num)
            .body(ByteStream::from(part_data.clone()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_num)
                .e_tag(resp.e_tag().unwrap_or(""))
                .build(),
        );
    }

    client0
        .complete_multipart_upload()
        .bucket(src)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;

    let node0_url = &cluster.nodes[0].url;
    let yaml = local_replicate_yaml(src, dst);
    let job_id = start_job(node0_url, &yaml, &cluster.access_key, &cluster.secret_key).await?;

    // Poll from node 1.
    let node1_url = &cluster.nodes[1].url;
    let metric = poll_until_complete(node1_url, &job_id, &cluster.access_key, &cluster.secret_key, 180).await?;

    let rep = &metric["replicate"];
    assert_eq!(rep["objects"].as_i64().unwrap_or(0), 1);
    assert_eq!(rep["objectsFailed"].as_i64().unwrap_or(-1), 0);
    assert_eq!(rep["bytesTransferred"].as_i64().unwrap_or(0), total_bytes);

    let head = client0.head_object().bucket(dst).key(key).send().await?;
    assert_eq!(head.content_length().unwrap_or(0), total_bytes);

    info!("PASSED: batch_replicate_multipart_cluster");
    Ok(())
}

/// Start one job via node 0 and one via node 1; list-jobs from either node must
/// show both (fan-out to peers).
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a 4-node rustfs cluster; requires batch-operations feature. Set RUSTFS_BUILD_FEATURES=batch-operations"]
async fn batch_list_jobs_cluster() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.start().await?;

    // Create two independent src/dst bucket pairs so the dedup guard does not
    // reject the second job (same-src+same-dst is rejected as duplicate).
    let client0 = cluster.create_s3_client(0)?;
    for bucket in ["list-src-a", "list-dst-a", "list-src-b", "list-dst-b"] {
        client0.create_bucket().bucket(bucket).send().await?;
    }

    // Put at least one object so the job has something to enumerate.
    client0
        .put_object()
        .bucket("list-src-a")
        .key("x")
        .body(ByteStream::from_static(b"a"))
        .send()
        .await?;
    client0
        .put_object()
        .bucket("list-src-b")
        .key("y")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await?;

    let node0_url = cluster.nodes[0].url.clone();
    let node1_url = cluster.nodes[1].url.clone();

    // Job A started from node 0, job B started from node 1.
    let yaml_a = local_replicate_yaml("list-src-a", "list-dst-a");
    let yaml_b = local_replicate_yaml("list-src-b", "list-dst-b");
    let job_a = start_job(&node0_url, &yaml_a, &cluster.access_key, &cluster.secret_key).await?;
    let job_b = start_job(&node1_url, &yaml_b, &cluster.access_key, &cluster.secret_key).await?;

    info!(job_a = %job_a, job_b = %job_b, "both jobs started");

    // Wait for both to finish before asserting list (avoids races with unregistered jobs).
    poll_until_complete(&node0_url, &job_a, &cluster.access_key, &cluster.secret_key, 120).await?;
    poll_until_complete(&node1_url, &job_b, &cluster.access_key, &cluster.secret_key, 120).await?;

    // list-jobs from node 0 must fan-out and include job_b owned by node 1.
    let list_url = format!("{node0_url}/rustfs/admin/v3/list-jobs?jobType=replicate");
    let resp = awscurl_get(&list_url, &cluster.access_key, &cluster.secret_key).await?;
    let list: serde_json::Value = serde_json::from_str(&resp)?;
    let jobs = list["jobs"].as_array().ok_or("missing 'jobs'")?;

    let ids: Vec<&str> = jobs.iter().filter_map(|j| j["id"].as_str()).collect();
    assert!(ids.contains(&job_a.as_str()), "list from node 0 must include job_a");
    assert!(ids.contains(&job_b.as_str()), "list from node 0 must include job_b (cross-node fan-out)");

    // list-jobs from node 1 must include job_a owned by node 0.
    let list_url1 = format!("{node1_url}/rustfs/admin/v3/list-jobs?jobType=replicate");
    let resp1 = awscurl_get(&list_url1, &cluster.access_key, &cluster.secret_key).await?;
    let list1: serde_json::Value = serde_json::from_str(&resp1)?;
    let jobs1 = list1["jobs"].as_array().ok_or("missing 'jobs'")?;
    let ids1: Vec<&str> = jobs1.iter().filter_map(|j| j["id"].as_str()).collect();
    assert!(ids1.contains(&job_a.as_str()), "list from node 1 must include job_a (cross-node fan-out)");
    assert!(ids1.contains(&job_b.as_str()), "list from node 1 must include job_b");

    info!("PASSED: batch_list_jobs_cluster");
    Ok(())
}
