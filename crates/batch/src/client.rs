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

//! S3 client wrapper for remote batch job endpoints.
//!
//! Reuses the credential and config-building patterns from
//! `crates/ecstore/src/bucket/bucket_target_sys.rs` and
//! `crates/ecstore/src/tier/warm_backend_s3sdk.rs`.

use crate::error::{BatchError, Result};
use crate::yaml::EndpointYaml;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials as SdkCredentials, Region as SdkRegion};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use std::collections::HashMap;

/// A page of listed objects returned from a remote bucket.
pub struct ListPage {
    pub objects: Vec<ListedObject>,
    /// S3 continuation token for the next page, `None` if this is the last page.
    pub next_token: Option<String>,
}

/// Metadata for a single listed object.
#[derive(Debug, Clone)]
pub struct ListedObject {
    pub key: String,
    pub size: i64,
    pub etag: Option<String>,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// An S3 client scoped to a remote bucket with optional prefix.
pub struct BatchS3Client {
    client: S3Client,
    pub bucket: String,
    pub prefix: Option<String>,
}

impl BatchS3Client {
    /// Build a client from the YAML endpoint definition.
    /// Returns `None` if the endpoint has no remote URL (i.e., it is a local endpoint).
    pub async fn from_endpoint(endpoint: &EndpointYaml) -> Result<Option<Self>> {
        let Some(url) = &endpoint.endpoint else {
            return Ok(None);
        };
        let Some(creds) = &endpoint.credentials else {
            return Err(BatchError::InvalidJobDefinition("remote endpoint requires credentials".into()));
        };

        let sdk_creds = SdkCredentials::new(creds.access_key.clone(), creds.secret_key.clone(), None, None, "rustfs-batch");

        let config = aws_sdk_s3::Config::builder()
            .endpoint_url(url.clone())
            .credentials_provider(sdk_creds)
            .region(SdkRegion::new("us-east-1"))
            .force_path_style(true)
            .behavior_version(BehaviorVersion::latest())
            .build();

        Ok(Some(Self {
            client: S3Client::from_conf(config),
            bucket: endpoint.bucket.clone(),
            prefix: endpoint.prefix.clone(),
        }))
    }

    /// List one page of objects. Returns up to 1000 keys per call.
    pub async fn list_objects_page(&self, continuation_token: Option<&str>) -> Result<ListPage> {
        let prefix = self.prefix.clone().unwrap_or_default();

        let mut req = self.client.list_objects_v2().bucket(&self.bucket).max_keys(1000);

        if !prefix.is_empty() {
            req = req.prefix(prefix);
        }
        if let Some(token) = continuation_token {
            req = req.continuation_token(token);
        }

        let output = req.send().await.map_err(|e| BatchError::S3Client(e.to_string()))?;

        let objects = output
            .contents()
            .iter()
            .filter_map(|obj| {
                let key = obj.key()?.to_owned();
                let size = obj.size().unwrap_or(0);
                let etag = obj.e_tag().map(|s| s.trim_matches('"').to_owned());
                let last_modified = obj.last_modified().and_then(|t| {
                    let secs = t.secs();
                    chrono::DateTime::from_timestamp(secs, 0)
                });
                Some(ListedObject {
                    key,
                    size,
                    etag,
                    last_modified,
                })
            })
            .collect();

        let next_token = if output.is_truncated().unwrap_or(false) {
            output.next_continuation_token().map(|s| s.to_owned())
        } else {
            None
        };

        Ok(ListPage { objects, next_token })
    }

    /// HEAD an object to check existence and ETag.
    pub async fn head_object(&self, key: &str) -> Result<Option<HeadResult>> {
        let result = self.client.head_object().bucket(&self.bucket).key(key).send().await;

        match result {
            Ok(output) => {
                let etag = output.e_tag().map(|s| s.trim_matches('"').to_owned());
                let size = output.content_length().unwrap_or(0);
                Ok(Some(HeadResult { etag, size }))
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_not_found() {
                    Ok(None)
                } else {
                    Err(BatchError::S3Client(svc_err.to_string()))
                }
            }
        }
    }

    /// GET an object, returning a streaming `ByteStream` and the declared content-length.
    /// The body is not buffered; callers must consume the stream without holding it across
    /// `.await` points that might cancel it.
    pub async fn get_object_stream(&self, key: &str) -> Result<(ByteStream, i64)> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| BatchError::S3Client(e.to_string()))?;

        let size = output.content_length().unwrap_or(0);
        Ok((output.body, size))
    }

    /// GET a single part of a multipart object by its 1-based part number.
    /// Returns the part's `ByteStream` and the declared content-length for that part.
    /// Only valid for objects whose ETag contains a `-N` suffix.
    pub async fn get_object_part_stream(&self, key: &str, part_number: u32) -> Result<(ByteStream, i64)> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .part_number(part_number as i32)
            .send()
            .await
            .map_err(|e| BatchError::S3Client(e.to_string()))?;

        let size = output.content_length().unwrap_or(0);
        Ok((output.body, size))
    }

    /// PUT an object from a `ByteStream` of known `size`.
    pub async fn put_object_stream(
        &self,
        key: &str,
        stream: ByteStream,
        size: i64,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_length(size)
            .body(stream);

        for (k, v) in &metadata {
            req = req.metadata(k, v);
        }

        req.send().await.map_err(|e| BatchError::S3Client(e.to_string()))?;
        Ok(())
    }

    /// Initiate a multipart upload. Returns the upload ID.
    pub async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| BatchError::S3Client(e.to_string()))?;

        output
            .upload_id()
            .map(|s| s.to_owned())
            .ok_or_else(|| BatchError::S3Client("create_multipart_upload returned no upload_id".into()))
    }

    /// Upload one part. Returns the ETag for that part (needed for `complete_multipart_upload`).
    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        stream: ByteStream,
        size: i64,
    ) -> Result<String> {
        let output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .content_length(size)
            .body(stream)
            .send()
            .await
            .map_err(|e| BatchError::S3Client(e.to_string()))?;

        output
            .e_tag()
            .map(|s| s.to_owned())
            .ok_or_else(|| BatchError::S3Client("upload_part returned no ETag".into()))
    }

    /// Complete a multipart upload. `parts` is a list of `(1-based part number, etag)` pairs
    /// in ascending part-number order.
    pub async fn complete_multipart_upload(&self, key: &str, upload_id: &str, parts: Vec<(i32, String)>) -> Result<()> {
        let completed_parts: Vec<CompletedPart> = parts
            .into_iter()
            .map(|(num, etag)| CompletedPart::builder().part_number(num).e_tag(etag).build())
            .collect();

        let completed = CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| BatchError::S3Client(e.to_string()))?;

        Ok(())
    }

    /// Abort an in-progress multipart upload. Should be called on transfer error to avoid
    /// orphaned partial uploads consuming storage.
    pub async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| BatchError::S3Client(e.to_string()))?;
        Ok(())
    }
}

/// Result of a HEAD object request.
#[derive(Debug)]
pub struct HeadResult {
    pub etag: Option<String>,
    pub size: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yaml::{CredentialsYaml, EndpointYaml};

    #[tokio::test]
    async fn test_local_endpoint_returns_none() {
        let endpoint = EndpointYaml {
            endpoint_type: "rustfs".into(),
            bucket: "local-bucket".into(),
            prefix: None,
            endpoint: None,
            credentials: None,
        };
        let result = BatchS3Client::from_endpoint(&endpoint).await.expect("no error");
        assert!(result.is_none(), "local endpoint must return None");
    }

    #[tokio::test]
    async fn test_remote_without_credentials_returns_error() {
        let endpoint = EndpointYaml {
            endpoint_type: "s3".into(),
            bucket: "remote-bucket".into(),
            prefix: None,
            endpoint: Some("https://remote.example.com".into()),
            credentials: None,
        };
        let result = BatchS3Client::from_endpoint(&endpoint).await;
        assert!(matches!(result, Err(BatchError::InvalidJobDefinition(_))));
    }

    #[tokio::test]
    async fn test_client_builds_for_remote_endpoint() {
        let endpoint = EndpointYaml {
            endpoint_type: "s3".into(),
            bucket: "remote-bucket".into(),
            prefix: Some("data/".into()),
            endpoint: Some("https://remote.example.com:9000".into()),
            credentials: Some(CredentialsYaml {
                access_key: "AKID".into(),
                secret_key: "SECRET".into(),
            }),
        };
        let client = BatchS3Client::from_endpoint(&endpoint).await.expect("build");
        let client = client.expect("should be Some for remote");
        assert_eq!(client.bucket, "remote-bucket");
        assert_eq!(client.prefix.as_deref(), Some("data/"));
    }
}
