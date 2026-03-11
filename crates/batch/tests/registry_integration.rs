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

//! Integration tests for the job registry.

use rustfs_batch::error::BatchError;
use rustfs_batch::job::{BatchJob, BatchJobStatusType, BatchJobType};
use rustfs_batch::registry::JobRegistry;
use rustfs_batch::yaml::{CredentialsYaml, EndpointYaml, ReplicateJobYaml};

fn make_job(id: &str, source_bucket: &str, target_bucket: &str) -> BatchJob {
    let config = ReplicateJobYaml {
        api_version: "v1".into(),
        source: EndpointYaml {
            endpoint_type: "minio".into(),
            bucket: source_bucket.into(),
            prefix: None,
            endpoint: None,
            credentials: None,
        },
        target: EndpointYaml {
            endpoint_type: "s3".into(),
            bucket: target_bucket.into(),
            prefix: None,
            endpoint: Some("https://remote.example.com:9000".into()),
            credentials: Some(CredentialsYaml {
                access_key: "A".into(),
                secret_key: "S".into(),
            }),
        },
        flags: None,
    };
    BatchJob::new(id.into(), BatchJobType::Replicate, "admin".into(), "hash".into(), &config)
}

#[tokio::test]
async fn test_registry_lifecycle() {
    let registry = JobRegistry::new();

    // Register a job.
    let job = make_job("j1", "bucket-src", "bucket-dst");
    let (control, counters) = registry
        .register(job.clone(), None, "bucket-src", Some("https://remote.example.com:9000"), "bucket-dst", 4)
        .await
        .expect("register");

    // Lookup.
    let found = registry.get_job("j1").await;
    assert!(found.is_some());

    // Increment counters.
    counters.inc_success(1024);
    counters.inc_failure(512);

    // Snapshot.
    let snapshot = registry.get_active_job_snapshot("j1").await.expect("snapshot");
    assert_eq!(snapshot.objects, 1);
    assert_eq!(snapshot.objects_failed, 1);
    assert_eq!(snapshot.bytes_transferred, 1024);
    assert_eq!(snapshot.bytes_failed, 512);

    // Convert to madmin metric.
    let metric = snapshot.to_job_metric();
    assert_eq!(metric.job_id, "j1");
    assert!(!metric.complete);
    assert!(!metric.failed);
    let rep = metric.replicate.expect("replicate info");
    assert_eq!(rep.objects, 1);
    assert_eq!(rep.bytes_transferred, 1024);

    // Cancel.
    registry.cancel("j1").await.expect("cancel");
    assert!(control.cancel.is_cancelled());

    // Set status and unregister.
    registry.set_status("j1", BatchJobStatusType::Cancelled).await;
    let updated = registry.get_job("j1").await.expect("still in registry");
    assert_eq!(updated.status, BatchJobStatusType::Cancelled);
    assert!(updated.finished_at.is_some());

    registry.unregister("j1", None, "bucket-src", Some("https://remote.example.com:9000"), "bucket-dst").await;
    assert!(registry.get_job("j1").await.is_none());
}

#[tokio::test]
async fn test_registry_dedup_same_endpoint() {
    let registry = JobRegistry::new();
    let j1 = make_job("dedup-1", "src", "dst");
    let j2 = make_job("dedup-2", "src", "dst");

    registry
        .register(j1, None, "src", Some("https://remote:9000"), "dst", 4)
        .await
        .expect("first");

    let err = registry
        .register(j2, None, "src", Some("https://remote:9000"), "dst", 4)
        .await
        .expect_err("duplicate must fail");

    assert!(matches!(err, BatchError::DuplicateJob));
}

#[tokio::test]
async fn test_registry_allows_same_bucket_different_target() {
    let registry = JobRegistry::new();
    let j1 = make_job("t1", "src", "dst-a");
    let j2 = make_job("t2", "src", "dst-b");

    registry
        .register(j1, None, "src", Some("https://remote1:9000"), "dst-a", 4)
        .await
        .expect("first");

    registry
        .register(j2, None, "src", Some("https://remote2:9000"), "dst-b", 4)
        .await
        .expect("second with different target must succeed");
}

#[tokio::test]
async fn test_list_jobs_returns_all() {
    let registry = JobRegistry::new();

    for i in 0..3 {
        let job = make_job(&format!("list-{i}"), &format!("src-{i}"), &format!("dst-{i}"));
        registry
            .register(job, None, &format!("src-{i}"), Some("https://remote:9000"), &format!("dst-{i}"), 4)
            .await
            .expect("register");
    }

    let result = registry.list_jobs(None).await;
    assert_eq!(result.jobs.len(), 3);

    let result_filtered = registry.list_jobs(Some("replicate")).await;
    assert_eq!(result_filtered.jobs.len(), 3);

    let result_empty = registry.list_jobs(Some("keyrotate")).await;
    assert_eq!(result_empty.jobs.len(), 0);
}
