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

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::Uri;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_batch::{get_global_batch_service, yaml::REPLICATE_JOB_TEMPLATE};
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
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

pub fn register_batch_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/start-job"),
        AdminOperation(&StartBatchJobHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/list-jobs"),
        AdminOperation(&ListBatchJobsHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/status-job"),
        AdminOperation(&BatchJobStatusHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/describe-job"),
        AdminOperation(&DescribeBatchJobHandler {}),
    )?;
    r.insert(
        Method::DELETE,
        &format!("{ADMIN_PREFIX}/v3/cancel-job"),
        AdminOperation(&CancelBatchJobHandler {}),
    )?;
    r.insert(
        Method::GET,
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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "batch service not initialized".into()));
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
                "an active job already exists for this source+target combination".into(),
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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "batch service not initialized".into()));
        };

        let params = extract_query_params(&req.uri);
        let job_type = params.get("jobType").map(|s| s.as_str());

        let result = svc.list_jobs(job_type).await;
        json_response(result)
    }
}

pub struct BatchJobStatusHandler {}

#[async_trait::async_trait]
impl Operation for BatchJobStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_admin_request(&req, AdminAction::DescribeBatchJobAction).await?;

        let Some(svc) = get_global_batch_service() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "batch service not initialized".into()));
        };

        let params = extract_query_params(&req.uri);
        let Some(job_id) = params.get("jobId") else {
            return Err(s3_error!(InvalidRequest, "jobId query parameter is required"));
        };

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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "batch service not initialized".into()));
        };

        let params = extract_query_params(&req.uri);
        let Some(job_id) = params.get("jobId") else {
            return Err(s3_error!(InvalidRequest, "jobId query parameter is required"));
        };

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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "batch service not initialized".into()));
        };

        let params = extract_query_params(&req.uri);
        // MinIO uses `id` param for cancel-job.
        let job_id = params
            .get("id")
            .or_else(|| params.get("jobId"))
            .ok_or_else(|| s3_error!(InvalidRequest, "id query parameter is required"))?;

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
            "replicate" => Ok(S3Response::new((StatusCode::OK, Body::from(REPLICATE_JOB_TEMPLATE)))),
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

        assert!(router.contains_route(Method::POST, &format!("{ADMIN_PREFIX}/v3/start-job")));
        assert!(router.contains_route(Method::GET, &format!("{ADMIN_PREFIX}/v3/list-jobs")));
        assert!(router.contains_route(Method::GET, &format!("{ADMIN_PREFIX}/v3/status-job")));
        assert!(router.contains_route(Method::GET, &format!("{ADMIN_PREFIX}/v3/describe-job")));
        assert!(router.contains_route(Method::DELETE, &format!("{ADMIN_PREFIX}/v3/cancel-job")));
        assert!(router.contains_route(Method::GET, &format!("{ADMIN_PREFIX}/v3/generate-job")));
    }
}
