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

//! YAML job definition types compatible with MinIO's `mc batch generate` output.

use serde::{Deserialize, Serialize};

/// Top-level batch job definition, matching MinIO's YAML format.
/// The `replicate` key maps to a replicate job; future types (keyrotate, expire) would add
/// additional optional fields here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchJobYaml {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicate: Option<ReplicateJobYaml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateJobYaml {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub source: EndpointYaml,
    pub target: EndpointYaml,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flags: Option<FlagsYaml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointYaml {
    /// "s3" or "rustfs"
    #[serde(rename = "type")]
    pub endpoint_type: String,
    pub bucket: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// None means local (this RustFS instance)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<CredentialsYaml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialsYaml {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FlagsYaml {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterYaml>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<NotifyYaml>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryYaml>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FilterYaml {
    /// e.g. "7d" — only objects newer than this duration
    #[serde(rename = "newerThan", skip_serializing_if = "Option::is_none")]
    pub newer_than: Option<String>,
    /// e.g. "7d"
    #[serde(rename = "olderThan", skip_serializing_if = "Option::is_none")]
    pub older_than: Option<String>,
    /// RFC3339 date string
    #[serde(rename = "createdAfter", skip_serializing_if = "Option::is_none")]
    pub created_after: Option<String>,
    /// RFC3339 date string
    #[serde(rename = "createdBefore", skip_serializing_if = "Option::is_none")]
    pub created_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<TagFilterYaml>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<MetadataFilterYaml>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagFilterYaml {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataFilterYaml {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyYaml {
    pub endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryYaml {
    /// Number of retry attempts (MinIO default: 3)
    pub attempts: u32,
    /// Duration string e.g. "500ms", "1s"
    pub delay: String,
}

/// Template constant for `mc batch generate replicate` compatibility.
pub const REPLICATE_JOB_TEMPLATE: &str = r#"replicate:
  apiVersion: v1
  source:
    type: "minio"
    bucket: SOURCE-BUCKET
    prefix: ""
    # endpoint and credentials omitted for local source
  target:
    type: "s3"
    bucket: TARGET-BUCKET
    prefix: ""
    endpoint: "https://TARGET-ENDPOINT:9000"
    credentials:
      accessKey: ACCESS-KEY
      secretKey: SECRET-KEY
  flags:
    filter:
      newerThan: "7d"
      olderThan: "7d"
    retry:
      attempts: 3
      delay: "500ms"
"#;

impl BatchJobYaml {
    pub fn from_yaml_str(s: &str) -> crate::error::Result<Self> {
        Ok(serde_yaml::from_str(s)?)
    }

    pub fn to_yaml_string(&self) -> crate::error::Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_yaml() {
        let yaml = r#"
replicate:
  apiVersion: v1
  source:
    type: minio
    bucket: src-bucket
    prefix: myprefix/
  target:
    type: s3
    bucket: dst-bucket
    endpoint: "https://remote.example.com:9000"
    credentials:
      accessKey: AKID
      secretKey: SECRET
  flags:
    retry:
      attempts: 3
      delay: "500ms"
"#;
        let job: BatchJobYaml = serde_yaml::from_str(yaml).expect("parse");
        let rep = job.replicate.as_ref().expect("replicate key");
        assert_eq!(rep.source.bucket, "src-bucket");
        assert_eq!(rep.target.bucket, "dst-bucket");
        assert!(rep.source.endpoint.is_none(), "local source has no endpoint");
        assert!(rep.target.credentials.is_some());
        let retry = rep.flags.as_ref().and_then(|f| f.retry.as_ref()).expect("retry");
        assert_eq!(retry.attempts, 3);
        assert_eq!(retry.delay, "500ms");
    }

    #[test]
    fn test_template_is_valid_yaml() {
        let _job: BatchJobYaml = serde_yaml::from_str(REPLICATE_JOB_TEMPLATE).expect("template must parse");
    }

    #[test]
    fn test_local_source_no_credentials() {
        let yaml = r#"
replicate:
  apiVersion: v1
  source:
    type: minio
    bucket: local-bucket
  target:
    type: s3
    bucket: remote-bucket
    endpoint: "https://remote.example.com"
    credentials:
      accessKey: A
      secretKey: S
"#;
        let job: BatchJobYaml = serde_yaml::from_str(yaml).expect("parse");
        let rep = job.replicate.as_ref().expect("replicate");
        assert!(rep.source.credentials.is_none());
        assert!(rep.source.endpoint.is_none());
    }
}
