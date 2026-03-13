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

//! Inter-node forwarding for batch admin operations.
//!
//! Batch jobs run exclusively on the node that accepted the start-job request and
//! hold all in-flight state (cancellation token, live counters) in that node's
//! in-memory JobRegistry. When a cancel/status/describe request arrives at a
//! different node it must be forwarded to the owner; otherwise the registry lookup
//! returns JobNotFound. PeerBatchClient handles that forwarding over the same
//! admin HTTP API that external clients use, signed with the shared RPC secret so
//! the receiving node's auth middleware accepts it.
//!
//! A per-node client map is kept inside BatchService (via PeerClientPool) so we
//! pay the reqwest::Client construction cost only once per peer.

use crate::error::{BatchError, Result};
use crate::job::{BatchJobStatus, BatchJobStatusList, ListBatchJobsResult};
use http::Method;
use reqwest::Client;
use rustfs_ecstore::rpc::gen_signature_headers;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const ADMIN_BATCH_PREFIX: &str = "/rustfs/admin/v3";

pub struct PeerBatchClient {
    base_url: String,
    client: Client,
}

impl PeerBatchClient {
    fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: Client::new(),
        }
    }

    fn signed_headers(&self, url: &str, method: &Method) -> reqwest::header::HeaderMap {
        let hdrs = gen_signature_headers(url, method);
        let mut out = reqwest::header::HeaderMap::new();
        for (k, v) in &hdrs {
            if let (Ok(name), Ok(val)) = (
                reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()),
                reqwest::header::HeaderValue::from_bytes(v.as_bytes()),
            ) {
                out.insert(name, val);
            }
        }
        out
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        let url = format!("{}{}/cancel-job?id={}", self.base_url, ADMIN_BATCH_PREFIX, job_id);
        let headers = self.signed_headers(&url, &Method::DELETE);
        let resp = self
            .client
            .delete(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BatchError::Transfer(format!("peer cancel_job: {e}")))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(BatchError::Transfer(format!("peer cancel_job HTTP {status}: {body}")))
        }
    }

    pub async fn job_status(&self, job_id: &str) -> Result<BatchJobStatus> {
        let url = format!("{}{}/status-job?jobId={}", self.base_url, ADMIN_BATCH_PREFIX, job_id);
        let headers = self.signed_headers(&url, &Method::GET);
        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BatchError::Transfer(format!("peer job_status: {e}")))?;

        if resp.status().is_success() {
            resp.json::<BatchJobStatus>()
                .await
                .map_err(|e| BatchError::Transfer(format!("peer job_status decode: {e}")))
        } else {
            let status = resp.status();
            if status.as_u16() == 404 {
                return Err(BatchError::JobNotFound(job_id.to_owned()));
            }
            let body = resp.text().await.unwrap_or_default();
            Err(BatchError::Transfer(format!("peer job_status HTTP {status}: {body}")))
        }
    }

    pub async fn job_status_all(&self) -> Result<BatchJobStatusList> {
        let url = format!("{}{}/status-job?all=true", self.base_url, ADMIN_BATCH_PREFIX);
        let headers = self.signed_headers(&url, &Method::GET);
        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BatchError::Transfer(format!("peer job_status_all: {e}")))?;

        if resp.status().is_success() {
            resp.json::<BatchJobStatusList>()
                .await
                .map_err(|e| BatchError::Transfer(format!("peer job_status_all decode: {e}")))
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(BatchError::Transfer(format!("peer job_status_all HTTP {status}: {body}")))
        }
    }

    pub async fn list_jobs(
        &self,
        job_type: Option<&str>,
        status: Option<&str>,
        bucket: Option<&str>,
    ) -> Result<ListBatchJobsResult> {
        let mut qs = String::new();
        if let Some(jt) = job_type {
            qs.push_str(&format!("jobType={jt}&"));
        }
        if let Some(s) = status {
            qs.push_str(&format!("status={s}&"));
        }
        if let Some(b) = bucket {
            qs.push_str(&format!("bucket={b}&"));
        }
        let qs = qs.trim_end_matches('&');
        let url = if qs.is_empty() {
            format!("{}{}/list-jobs", self.base_url, ADMIN_BATCH_PREFIX)
        } else {
            format!("{}{}/list-jobs?{}", self.base_url, ADMIN_BATCH_PREFIX, qs)
        };

        let headers = self.signed_headers(&url, &Method::GET);
        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BatchError::Transfer(format!("peer list_jobs: {e}")))?;

        if resp.status().is_success() {
            resp.json::<ListBatchJobsResult>()
                .await
                .map_err(|e| BatchError::Transfer(format!("peer list_jobs decode: {e}")))
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(BatchError::Transfer(format!("peer list_jobs HTTP {status}: {body}")))
        }
    }

    pub async fn describe_job(&self, job_id: &str) -> Result<String> {
        let url = format!("{}{}/describe-job?jobId={}", self.base_url, ADMIN_BATCH_PREFIX, job_id);
        let headers = self.signed_headers(&url, &Method::GET);
        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BatchError::Transfer(format!("peer describe_job: {e}")))?;

        if resp.status().is_success() {
            resp.text()
                .await
                .map_err(|e| BatchError::Transfer(format!("peer describe_job decode: {e}")))
        } else {
            let status = resp.status();
            if status.as_u16() == 404 {
                return Err(BatchError::JobNotFound(job_id.to_owned()));
            }
            let body = resp.text().await.unwrap_or_default();
            Err(BatchError::Transfer(format!("peer describe_job HTTP {status}: {body}")))
        }
    }
}

/// Cached pool of one `PeerBatchClient` per peer node base URL.
/// Avoids re-creating `reqwest::Client` (and its connection pool) on every request.
pub struct PeerClientPool {
    clients: RwLock<HashMap<String, Arc<PeerBatchClient>>>,
}

impl PeerClientPool {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, base_url: &str) -> Arc<PeerBatchClient> {
        {
            let map = self.clients.read().await;
            if let Some(c) = map.get(base_url) {
                return c.clone();
            }
        }
        let mut map = self.clients.write().await;
        map.entry(base_url.to_owned())
            .or_insert_with(|| Arc::new(PeerBatchClient::new(base_url.to_owned())))
            .clone()
    }
}
