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

//! MinIO-compatible batch job HTTP handlers.
//!
//! Endpoints (all under `/rustfs/admin/v3/`):
//! - `POST   start-job`       — submit YAML, start a job
//! - `GET    list-jobs`       — list jobs (optional ?jobType=replicate&bucket=)
//! - `GET    status-job`      — get progress metrics
//! - `GET    describe-job`    — get original YAML definition
//! - `DELETE cancel-job`      — cancel a running job
//! - `GET    generate-job`    — generate a YAML template
//!
//! ## Cross-node routing
//!
//! In a cluster every job ID embeds the owning node's `host:port` after a `|`
//! separator (e.g. `replicate-abc123|127.0.0.1:9001`).  When a request arrives
//! at a node that does not own the job, the handler proxies the **original**
//! HTTP request verbatim (preserving the client's SigV4 `Authorization` header)
//! to the owning node.  The receiving node re-validates the same SigV4
//! credential — this works because all cluster nodes share the same root
//! credentials.  No re-signing is required.

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::Uri;
use hyper::StatusCode;
use matchit::Params;
use reqwest::Client;
use rustfs_batch::{get_global_batch_service, parse_owner_node, yaml::REPLICATE_JOB_TEMPLATE};
use rustfs_utils::http::headers::RUSTFS_BATCH_PROXY_REQUEST;
use rustfs_common::get_global_local_node_name;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::global::get_global_endpoints;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use tracing::{error, warn};

fn extract_query_params(uri: &Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }
    params
}

async fn validate_batch_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<String> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "missing credentials"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await?;

    Ok(cred.access_key.clone())
}

fn json_response(value: impl serde::Serialize) -> S3Result<S3Response<(StatusCode, Body)>> {
    let body = serde_json::to_string(&value).map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
    Ok(S3Response::new((StatusCode::OK, Body::from(body))))
}

/// Returns the base URLs (`http://host:port`) of all non-local cluster peers.
async fn non_local_peer_base_urls() -> Vec<String> {
    let local = get_global_local_node_name().await;
    get_global_endpoints()
        .get_nodes()
        .into_iter()
        .filter(|n| !n.is_local)
        .map(|n| {
            let host = n.url.host_str().unwrap_or("");
            let port = n.url.port().map(|p| format!(":{p}")).unwrap_or_default();
            let scheme = n.url.scheme();
            format!("{scheme}://{host}{port}")
        })
        .filter(|url| local.is_empty() || !url.contains(&local))
        .collect()
}

/// If the job ID encodes a non-local owner, return `Some("http://host:port")`.
/// Returns `None` when the job is local (no owner suffix, or owner == this node).
async fn owner_base_url_if_remote(job_id: &str) -> Option<String> {
    let owner = parse_owner_node(job_id)?;
    let local = get_global_local_node_name().await;
    if local.is_empty() || owner == local {
        return None;
    }
    Some(format!("http://{owner}"))
}

/// Forward `req` verbatim to `target_base_url`, preserving all headers
/// (including the original SigV4 `Authorization`).  The path-and-query from
/// `req.uri` is appended to `target_base_url`.
async fn proxy_to_node(target_base_url: &str, req: S3Request<Body>) -> S3Result<S3Response<(StatusCode, Body)>> {
    let path_and_query = req.uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let url = format!("{target_base_url}{path_and_query}");

    let method = reqwest::Method::from_bytes(req.method.as_str().as_bytes())
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("proxy: bad method: {e}")))?;

    let body_bytes = {
        use s3s::stream::ByteStream;
        let mut input = req.input;
        input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("proxy: body read: {e}")))?
    };

    let client = Client::new();
    let mut builder = client.request(method, &url);

    for (name, value) in &req.headers {
        if let Ok(v) = value.to_str() {
            builder = builder.header(name.as_str(), v);
        }
    }

    if !body_bytes.is_empty() {
        builder = builder.body(body_bytes.to_vec());
    }

    let resp = builder
        .send()
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("proxy request failed: {e}")))?;

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let resp_body = resp
        .bytes()
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("proxy response read: {e}")))?;

    Ok(S3Response::new((status, Body::from(resp_body.to_vec()))))
}

pub fn register_batch_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        hyper::Method::POST,
        &format!("{ADMIN_PREFIX}/v3/start-job"),
        AdminOperation(&StartBatchJobHandler {}),
    )?;
    r.insert(
        hyper::Method::GET,
        &format!("{ADMIN_PREFIX}/v3/list-jobs"),
        AdminOperation(&ListBatchJobsHandler {}),
    )?;
    r.insert(
        hyper::Method::GET,
        &format!("{ADMIN_PREFIX}/v3/status-job"),
        AdminOperation(&BatchJobStatusHandler {}),
    )?;
    r.insert(
        hyper::Method::GET,
        &format!("{ADMIN_PREFIX}/v3/describe-job"),
        AdminOperation(&DescribeBatchJobHandler {}),
    )?;
    r.insert(
        hyper::Method::DELETE,
        &format!("{ADMIN_PREFIX}/v3/cancel-job"),
        AdminOperation(&CancelBatchJobHandler {}),
    )?;
    r.insert(
        hyper::Method::GET,
        &format!("{ADMIN_PREFIX}/v3/generate-job"),
        AdminOperation(&GenerateBatchJobHandler {}),
    )?;
    Ok(())
}

pub struct StartBatchJobHandler {}

#[async_trait::async_trait]
impl Operation for StartBatchJobHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let user = validate_batch_admin_request(&req, AdminAction::StartBatchJobAction).await?;

        let Some(svc) = get_global_batch_service() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "batch service not initialized".to_string(),
            ));
        };

        let mut input = req.input;
        let body = input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await.map_err(|e| {
            warn!("batch start-job: body read failed: {e}");
            s3_error!(InvalidRequest, "failed to read request body")
        })?;

        if body.is_empty() {
            return Err(s3_error!(InvalidRequest, "request body (YAML job definition) is required"));
        }

        match svc.start_job(&body, user).await {
            Ok(result) => json_response(result),
            Err(rustfs_batch::error::BatchError::DuplicateJob) => Err(S3Error::with_message(
                S3ErrorCode::BucketAlreadyExists,
                "an active job already exists for this source+target combination".to_string(),
            )),
            Err(rustfs_batch::error::BatchError::InvalidJobDefinition(msg))
            | Err(rustfs_batch::error::BatchError::UnsupportedJobType(msg)) => {
                Err(S3Error::with_message(S3ErrorCode::InvalidArgument, msg))
            }
            Err(e) => {
                error!("batch start-job: {e}");
                Err(S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))
            }
        }
    }
}

pub struct ListBatchJobsHandler {}

#[async_trait::async_trait]
impl Operation for ListBatchJobsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_admin_request(&req, AdminAction::ListBatchJobsAction).await?;

        let Some(svc) = get_global_batch_service() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "batch service not initialized".to_string(),
            ));
        };

        let params = extract_query_params(&req.uri);
        let job_type = params.get("jobType").map(|s| s.as_str());
        let status_filter = params.get("status").map(|s| s.as_str());
        let bucket = params.get("bucket").map(|s| s.as_str());

        // Local results.
        let mut local_result = svc.list_jobs(job_type, status_filter, bucket).await;

        // Fan-out the original request to all non-local peers concurrently.
        // Skip fan-out when this request was itself forwarded by a peer to avoid
        // infinite proxy loops.
        let is_proxied = req.headers.get(RUSTFS_BATCH_PROXY_REQUEST).is_some();
        let peer_urls = if is_proxied { vec![] } else { non_local_peer_base_urls().await };
        if !peer_urls.is_empty() {
            let path_and_query = req.uri.path_and_query().map(|pq| pq.as_str().to_owned()).unwrap_or_default();
            let orig_headers = req.headers.clone();
            let client = Client::new();

            let handles: Vec<_> = peer_urls
                .into_iter()
                .map(|base| {
                    let pq = path_and_query.clone();
                    let hdrs = orig_headers.clone();
                    let client = client.clone();
                    tokio::spawn(async move {
                        let url = format!("{base}{pq}");
                        let mut builder = client.get(&url);
                        for (name, value) in &hdrs {
                            if let Ok(v) = value.to_str() {
                                builder = builder.header(name.as_str(), v);
                            }
                        }
                        builder = builder.header(RUSTFS_BATCH_PROXY_REQUEST, "1");
                        match builder.send().await {
                            Ok(resp) if resp.status().is_success() => {
                                resp.json::<rustfs_batch::job::ListBatchJobsResult>().await.ok()
                            }
                            Ok(resp) => {
                                warn!(peer = %base, status = %resp.status(), "fan-out list-jobs non-success");
                                None
                            }
                            Err(e) => {
                                warn!(peer = %base, "fan-out list-jobs failed: {e}");
                                None
                            }
                        }
                    })
                })
                .collect();

            for h in handles {
                if let Ok(Some(peer_result)) = h.await {
                    local_result.jobs.extend(peer_result.jobs);
                }
            }
        }

        json_response(local_result)
    }
}

pub struct BatchJobStatusHandler {}

#[async_trait::async_trait]
impl Operation for BatchJobStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_admin_request(&req, AdminAction::DescribeBatchJobAction).await?;

        let Some(svc) = get_global_batch_service() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "batch service not initialized".to_string(),
            ));
        };

        let params = extract_query_params(&req.uri);

        // ?all=true — return all jobs in the retention window; fan-out handled below.
        if params.get("all").map(|v| v == "true").unwrap_or(false) {
            let mut result = svc.job_status_all().await;

            // Fan-out to peers; skip if this request was itself forwarded.
            let is_proxied = req.headers.get(RUSTFS_BATCH_PROXY_REQUEST).is_some();
            let peer_urls = if is_proxied { vec![] } else { non_local_peer_base_urls().await };
            if !peer_urls.is_empty() {
                let pq = req.uri.path_and_query().map(|pq| pq.as_str().to_owned()).unwrap_or_default();
                let hdrs = req.headers.clone();
                let client = Client::new();
                let handles: Vec<_> = peer_urls
                    .into_iter()
                    .map(|base| {
                        let pq = pq.clone();
                        let hdrs = hdrs.clone();
                        let client = client.clone();
                        tokio::spawn(async move {
                            let url = format!("{base}{pq}");
                            let mut builder = client.get(&url);
                            for (name, value) in &hdrs {
                                if let Ok(v) = value.to_str() {
                                    builder = builder.header(name.as_str(), v);
                                }
                            }
                            builder = builder.header(RUSTFS_BATCH_PROXY_REQUEST, "1");
                            match builder.send().await {
                                Ok(resp) if resp.status().is_success() => {
                                    resp.json::<rustfs_batch::job::BatchJobStatusList>().await.ok()
                                }
                                Ok(resp) => {
                                    warn!(peer = %base, status = %resp.status(), "fan-out status-all non-success");
                                    None
                                }
                                Err(e) => {
                                    warn!(peer = %base, "fan-out status-all failed: {e}");
                                    None
                                }
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    if let Ok(Some(peer_list)) = h.await {
                        result.statuses.extend(peer_list.statuses);
                    }
                }
            }

            return json_response(result);
        }

        // No jobId — return the most recently created active job (fan-out to peers too).
        let Some(job_id) = params.get("jobId") else {
            let local = svc.job_status_last_active().await;

            // Skip fan-out when this request was itself forwarded by a peer.
            let is_proxied = req.headers.get(RUSTFS_BATCH_PROXY_REQUEST).is_some();
            let peer_urls = if is_proxied { vec![] } else { non_local_peer_base_urls().await };
            let mut candidates: Vec<rustfs_batch::job::BatchJobStatus> = local.into_iter().collect();

            if !peer_urls.is_empty() {
                let pq = req.uri.path_and_query().map(|pq| pq.as_str().to_owned()).unwrap_or_default();
                let hdrs = req.headers.clone();
                let client = Client::new();
                let handles: Vec<_> = peer_urls
                    .into_iter()
                    .map(|base| {
                        let pq = pq.clone();
                        let hdrs = hdrs.clone();
                        let client = client.clone();
                        tokio::spawn(async move {
                            let url = format!("{base}{pq}");
                            let mut builder = client.get(&url);
                            for (name, value) in &hdrs {
                                if let Ok(v) = value.to_str() {
                                    builder = builder.header(name.as_str(), v);
                                }
                            }
                            builder = builder.header(RUSTFS_BATCH_PROXY_REQUEST, "1");
                            match builder.send().await {
                                Ok(resp) if resp.status().is_success() => {
                                    resp.json::<rustfs_batch::job::BatchJobStatus>().await.ok()
                                }
                                _ => None,
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    if let Ok(Some(s)) = h.await {
                        candidates.push(s);
                    }
                }
            }

            return match candidates.into_iter().max_by_key(|s| s.last_metric.start_time) {
                Some(status) => json_response(status),
                None => Err(S3Error::with_message(S3ErrorCode::NoSuchKey, "no active batch job found".to_string())),
            };
        };

        // Specific jobId — proxy to owner node if needed.
        if let Some(owner_url) = owner_base_url_if_remote(job_id).await {
            return proxy_to_node(&owner_url, req).await;
        }

        match svc.job_status(job_id).await {
            Ok(status) => json_response(status),
            Err(rustfs_batch::error::BatchError::JobNotFound(_)) => {
                Err(S3Error::with_message(S3ErrorCode::NoSuchKey, format!("job {job_id} not found")))
            }
            Err(e) => Err(S3Error::with_message(S3ErrorCode::InternalError, e.to_string())),
        }
    }
}

pub struct DescribeBatchJobHandler {}

#[async_trait::async_trait]
impl Operation for DescribeBatchJobHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_admin_request(&req, AdminAction::DescribeBatchJobAction).await?;

        let Some(svc) = get_global_batch_service() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "batch service not initialized".to_string(),
            ));
        };

        let params = extract_query_params(&req.uri);
        let Some(job_id) = params.get("jobId") else {
            return Err(s3_error!(InvalidRequest, "jobId query parameter is required"));
        };

        if let Some(owner_url) = owner_base_url_if_remote(job_id).await {
            return proxy_to_node(&owner_url, req).await;
        }

        match svc.describe_job(job_id).await {
            Ok(yaml) => Ok(S3Response::new((StatusCode::OK, Body::from(yaml)))),
            Err(rustfs_batch::error::BatchError::JobNotFound(_)) | Err(rustfs_batch::error::BatchError::Storage(_)) => {
                Err(S3Error::with_message(S3ErrorCode::NoSuchKey, format!("job {job_id} not found")))
            }
            Err(e) => Err(S3Error::with_message(S3ErrorCode::InternalError, e.to_string())),
        }
    }
}

pub struct CancelBatchJobHandler {}

#[async_trait::async_trait]
impl Operation for CancelBatchJobHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_admin_request(&req, AdminAction::CancelBatchJobAction).await?;

        let Some(svc) = get_global_batch_service() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "batch service not initialized".to_string(),
            ));
        };

        let params = extract_query_params(&req.uri);
        // MinIO uses `id` param for cancel-job.
        let job_id = params
            .get("id")
            .or_else(|| params.get("jobId"))
            .ok_or_else(|| s3_error!(InvalidRequest, "id query parameter is required"))?;

        if let Some(owner_url) = owner_base_url_if_remote(job_id).await {
            return proxy_to_node(&owner_url, req).await;
        }

        match svc.cancel_job(job_id).await {
            Ok(()) => Ok(S3Response::new((StatusCode::OK, Body::empty()))),
            Err(rustfs_batch::error::BatchError::JobNotFound(_)) => {
                Err(S3Error::with_message(S3ErrorCode::NoSuchKey, format!("job {job_id} not found")))
            }
            Err(e) => Err(S3Error::with_message(S3ErrorCode::InternalError, e.to_string())),
        }
    }
}

pub struct GenerateBatchJobHandler {}

#[async_trait::async_trait]
impl Operation for GenerateBatchJobHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_admin_request(&req, AdminAction::StartBatchJobAction).await?;

        let params = extract_query_params(&req.uri);
        let job_type = params.get("jobType").map(|s| s.as_str()).unwrap_or("replicate");

        match job_type {
            "replicate" => Ok(S3Response::new((StatusCode::OK, Body::from(REPLICATE_JOB_TEMPLATE.to_string())))),
            other => Err(S3Error::with_message(
                S3ErrorCode::InvalidArgument,
                format!("unsupported jobType: {other}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_structs_can_be_created() {
        let _h1 = StartBatchJobHandler {};
        let _h2 = ListBatchJobsHandler {};
        let _h3 = BatchJobStatusHandler {};
        let _h4 = DescribeBatchJobHandler {};
        let _h5 = CancelBatchJobHandler {};
        let _h6 = GenerateBatchJobHandler {};
    }

    #[test]
    fn test_route_registration() {
        let mut router: S3Router<AdminOperation> = S3Router::new(false);
        register_batch_route(&mut router).expect("register batch routes");

        assert!(router.contains_route(hyper::Method::POST, &format!("{ADMIN_PREFIX}/v3/start-job")));
        assert!(router.contains_route(hyper::Method::GET, &format!("{ADMIN_PREFIX}/v3/list-jobs")));
        assert!(router.contains_route(hyper::Method::GET, &format!("{ADMIN_PREFIX}/v3/status-job")));
        assert!(router.contains_route(hyper::Method::GET, &format!("{ADMIN_PREFIX}/v3/describe-job")));
        assert!(router.contains_route(hyper::Method::DELETE, &format!("{ADMIN_PREFIX}/v3/cancel-job")));
        assert!(router.contains_route(hyper::Method::GET, &format!("{ADMIN_PREFIX}/v3/generate-job")));
    }
}
