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

//! Integration tests for the batch YAML parsing and job type definitions.

use rustfs_batch::yaml::{BatchJobYaml, REPLICATE_JOB_TEMPLATE};

#[test]
fn test_template_parses_and_round_trips() {
    let job: BatchJobYaml = serde_yaml::from_str(REPLICATE_JOB_TEMPLATE).expect("template must parse");
    let yaml_out = job.to_yaml_string().expect("serialize back to YAML");
    let job2: BatchJobYaml = serde_yaml::from_str(&yaml_out).expect("re-parse from serialized YAML");
    assert!(job2.replicate.is_some());
}

#[test]
fn test_full_job_yaml_with_all_flags() {
    let yaml = r#"
replicate:
  apiVersion: v1
  source:
    type: minio
    bucket: source-bucket
    prefix: logs/
  target:
    type: s3
    bucket: target-bucket
    prefix: backup/
    endpoint: "https://s3.us-east-1.amazonaws.com"
    credentials:
      accessKey: AKID1234
      secretKey: SECRET5678
  flags:
    filter:
      newerThan: "7d"
      olderThan: "365d"
      createdAfter: "2024-01-01T00:00:00Z"
      createdBefore: "2025-01-01T00:00:00Z"
      tags:
        - key: env
          value: production
      metadata:
        - key: x-custom-meta
          value: enabled
    notify:
      endpoint: "https://webhook.example.com/batch-notify"
      token: "Bearer abc123"
    retry:
      attempts: 5
      delay: "2s"
"#;

    let job: BatchJobYaml = serde_yaml::from_str(yaml).expect("parse full YAML");
    let rep = job.replicate.as_ref().expect("replicate");

    assert_eq!(rep.api_version, "v1");
    assert_eq!(rep.source.bucket, "source-bucket");
    assert_eq!(rep.source.prefix.as_deref(), Some("logs/"));
    assert!(rep.source.endpoint.is_none(), "local source has no endpoint");
    assert!(rep.source.credentials.is_none());

    assert_eq!(rep.target.bucket, "target-bucket");
    assert_eq!(rep.target.prefix.as_deref(), Some("backup/"));
    assert!(rep.target.endpoint.is_some());
    let creds = rep.target.credentials.as_ref().expect("target credentials");
    assert_eq!(creds.access_key, "AKID1234");
    assert_eq!(creds.secret_key, "SECRET5678");

    let flags = rep.flags.as_ref().expect("flags");
    let filter = flags.filter.as_ref().expect("filter");
    assert_eq!(filter.newer_than.as_deref(), Some("7d"));
    assert_eq!(filter.older_than.as_deref(), Some("365d"));
    assert_eq!(filter.created_after.as_deref(), Some("2024-01-01T00:00:00Z"));
    assert_eq!(filter.created_before.as_deref(), Some("2025-01-01T00:00:00Z"));
    let tags = filter.tags.as_ref().expect("tags");
    assert_eq!(tags[0].key, "env");
    assert_eq!(tags[0].value, "production");

    let notify = flags.notify.as_ref().expect("notify");
    assert_eq!(notify.endpoint, "https://webhook.example.com/batch-notify");

    let retry = flags.retry.as_ref().expect("retry");
    assert_eq!(retry.attempts, 5);
    assert_eq!(retry.delay, "2s");
}

#[test]
fn test_missing_replicate_key_gives_none() {
    let yaml = "{}";
    let job: BatchJobYaml = serde_yaml::from_str(yaml).expect("empty yaml");
    assert!(job.replicate.is_none());
}

#[test]
fn test_from_yaml_str_helper() {
    let job = BatchJobYaml::from_yaml_str(REPLICATE_JOB_TEMPLATE).expect("helper parse");
    assert!(job.replicate.is_some());
}
