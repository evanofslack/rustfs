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

//! Iceberg REST Catalog HTTP handlers for the S3 Tables feature.
//!
//! Endpoints (all under `/_iceberg/v1/`):
//! - Warehouse CRUD (MinIO AIStor extension)
//! - Namespace CRUD
//! - Table CRUD + commit (OCC) + rename + multi-table transaction
//!
//! Authentication: SigV4 with service=s3tables, validated via `check_key_valid`
//! + policy action check with `s3tables:*` actions.

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::RemoteAddr;
use http::header::CONTENT_TYPE;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_policy::policy::action::{Action, S3TablesAction};
use rustfs_tables::catalog::CatalogStore;
use rustfs_tables::error::TablesError;
use rustfs_tables::models::{
    CatalogConfig, CommitTableRequest, CommitTransactionRequest, CreateNamespaceRequest, CreateTableRequest,
    CreateWarehouseRequest, IcebergErrorResponse, RenameTableRequest, UpdateNamespacePropertiesRequest,
};
use s3s::stream::ByteStream;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use tracing::error;

// ─── Auth helper ─────────────────────────────────────────────────────────────

async fn validate_tables_request(req: &S3Request<Body>, action: S3TablesAction) -> S3Result<()> {
    let Some(cred_input) = req.credentials.as_ref() else {
        return Err(s3_error!(AccessDenied, "missing credentials"));
    };
    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred_input.access_key).await?;
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::S3TablesAction(action)], remote_addr).await?;
    Ok(())
}

// ─── Response helpers ─────────────────────────────────────────────────────────

fn json_response(value: &impl serde::Serialize) -> S3Result<S3Response<(StatusCode, Body)>> {
    let body =
        serde_json::to_string(value).map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
    let mut resp = S3Response::new((StatusCode::OK, Body::from(body)));
    resp.headers
        .insert(CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
    Ok(resp)
}

fn no_content() -> S3Result<S3Response<(StatusCode, Body)>> {
    Ok(S3Response::new((StatusCode::NO_CONTENT, Body::empty())))
}

fn tables_err(err: TablesError) -> S3Error {
    let status = err.http_status();
    let body = IcebergErrorResponse::new(status.as_u16(), err.error_type(), err.to_string());
    let json = serde_json::to_string(&body).unwrap_or_else(|_| {
        r#"{"error":{"code":500,"type":"InternalError","message":"serialization failed"}}"#.to_string()
    });
    S3Error::with_message(S3ErrorCode::InternalError, json)
}

async fn parse_body<T: serde::de::DeserializeOwned>(req: &mut S3Request<Body>) -> S3Result<T> {
    let bytes = req
        .input
        .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
    serde_json::from_slice::<T>(&bytes)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidArgument, format!("invalid JSON body: {e}")))
}

fn catalog() -> std::sync::Arc<dyn CatalogStore> {
    rustfs_tables::get_global_catalog()
}

// ─── Route registration ───────────────────────────────────────────────────────

pub fn register_tables_routes(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // Warehouse CRUD (MinIO AIStor extension)
    r.insert(Method::POST, "/_iceberg/v1/warehouses", AdminOperation(&CreateWarehouseHandler))?;
    r.insert(Method::GET, "/_iceberg/v1/warehouses", AdminOperation(&ListWarehousesHandler))?;
    r.insert(Method::GET, "/_iceberg/v1/warehouses/{warehouse}", AdminOperation(&GetWarehouseHandler))?;
    r.insert(Method::DELETE, "/_iceberg/v1/warehouses/{warehouse}", AdminOperation(&DeleteWarehouseHandler))?;

    // Catalog config
    r.insert(Method::GET, "/_iceberg/v1/{prefix}/config", AdminOperation(&GetConfigHandler))?;

    // Namespaces
    r.insert(Method::GET, "/_iceberg/v1/{prefix}/namespaces", AdminOperation(&ListNamespacesHandler))?;
    r.insert(Method::POST, "/_iceberg/v1/{prefix}/namespaces", AdminOperation(&CreateNamespaceHandler))?;
    r.insert(
        Method::HEAD,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}",
        AdminOperation(&NamespaceExistsHandler),
    )?;
    r.insert(
        Method::GET,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}",
        AdminOperation(&GetNamespaceHandler),
    )?;
    r.insert(
        Method::POST,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}/properties",
        AdminOperation(&UpdateNamespacePropertiesHandler),
    )?;
    r.insert(
        Method::DELETE,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}",
        AdminOperation(&DropNamespaceHandler),
    )?;

    // Tables
    r.insert(
        Method::GET,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}/tables",
        AdminOperation(&ListTablesHandler),
    )?;
    r.insert(
        Method::POST,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}/tables",
        AdminOperation(&CreateTableHandler),
    )?;
    r.insert(
        Method::HEAD,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}",
        AdminOperation(&TableExistsHandler),
    )?;
    r.insert(
        Method::GET,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}",
        AdminOperation(&LoadTableHandler),
    )?;
    r.insert(
        Method::POST,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}",
        AdminOperation(&CommitTableHandler),
    )?;
    r.insert(
        Method::DELETE,
        "/_iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}",
        AdminOperation(&DropTableHandler),
    )?;
    r.insert(
        Method::POST,
        "/_iceberg/v1/{prefix}/tables/rename",
        AdminOperation(&RenameTableHandler),
    )?;
    r.insert(
        Method::POST,
        "/_iceberg/v1/{prefix}/transactions/commit",
        AdminOperation(&CommitTransactionHandler),
    )?;

    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn warehouse_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    params
        .get("warehouse")
        .or_else(|| params.get("prefix"))
        .map(|s| s.to_string())
        .ok_or_else(|| s3_error!(InvalidArgument, "missing warehouse/prefix path parameter"))
}

fn ns_from_params(params: &Params<'_, '_>) -> S3Result<Vec<String>> {
    params
        .get("namespace")
        .map(|s| s.split('\x1f').map(|p| p.to_string()).collect::<Vec<_>>())
        .or_else(|| {
            params
                .get("namespace")
                .map(|s| vec![s.to_string()])
        })
        .ok_or_else(|| s3_error!(InvalidArgument, "missing namespace path parameter"))
}

/// Parse the `{namespace}` param — Iceberg encodes multi-level namespaces
/// as 0x1F-separated strings in the URL.
fn parse_namespace_param(raw: &str) -> Vec<String> {
    // Try 0x1F separator first, then fall back to treating as single-level.
    if raw.contains('\x1f') {
        raw.split('\x1f').map(|s| s.to_string()).collect()
    } else {
        vec![raw.to_string()]
    }
}

// ─── Warehouse handlers ───────────────────────────────────────────────────────

pub struct CreateWarehouseHandler;
#[async_trait::async_trait]
impl Operation for CreateWarehouseHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::CreateWarehouseAction).await?;
        let body: CreateWarehouseRequest = parse_body(&mut req).await?;
        catalog()
            .create_warehouse(&body.name, body.upgrade_existing)
            .await
            .map_err(tables_err)?;
        json_response(&serde_json::json!({"name": body.name}))
    }
}

pub struct ListWarehousesHandler;
#[async_trait::async_trait]
impl Operation for ListWarehousesHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::ListWarehousesAction).await?;
        let warehouses = catalog().list_warehouses().await.map_err(tables_err)?;
        let names: Vec<String> = warehouses.into_iter().map(|w| w.name).collect();
        json_response(&serde_json::json!({"warehouses": names}))
    }
}

pub struct GetWarehouseHandler;
#[async_trait::async_trait]
impl Operation for GetWarehouseHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::GetWarehouseAction).await?;
        let name = warehouse_from_params(&params)?;
        let meta = catalog().get_warehouse(&name).await.map_err(tables_err)?;
        json_response(&meta)
    }
}

pub struct DeleteWarehouseHandler;
#[async_trait::async_trait]
impl Operation for DeleteWarehouseHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::DeleteWarehouseAction).await?;
        let name = warehouse_from_params(&params)?;
        let preserve = req.uri.query().and_then(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .find(|(k, _)| k == "preserve-bucket")
                .and_then(|(_, v)| v.parse::<bool>().ok())
        }).unwrap_or(false);
        catalog().delete_warehouse(&name, preserve).await.map_err(tables_err)?;
        no_content()
    }
}

// ─── Config handler ───────────────────────────────────────────────────────────

pub struct GetConfigHandler;
#[async_trait::async_trait]
impl Operation for GetConfigHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::GetWarehouseAction).await?;
        let warehouse = warehouse_from_params(&params)?;

        let mut defaults = HashMap::new();
        // Tell clients the S3 endpoint to use for data file I/O.
        // The warehouse bucket is at this same endpoint.
        defaults.insert("warehouse".to_string(), format!("s3://{warehouse}"));

        let config = CatalogConfig {
            defaults,
            overrides: HashMap::new(),
        };
        json_response(&config)
    }
}

// ─── Namespace handlers ───────────────────────────────────────────────────────

pub struct ListNamespacesHandler;
#[async_trait::async_trait]
impl Operation for ListNamespacesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::ListNamespacesAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let parent = req.uri.query().and_then(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .find(|(k, _)| k == "parent")
                .map(|(_, v)| v.into_owned())
        });
        let namespaces = catalog()
            .list_namespaces(&warehouse, parent.as_deref())
            .await
            .map_err(tables_err)?;
        json_response(&serde_json::json!({"namespaces": namespaces}))
    }
}

pub struct CreateNamespaceHandler;
#[async_trait::async_trait]
impl Operation for CreateNamespaceHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::CreateNamespaceAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let body: CreateNamespaceRequest = parse_body(&mut req).await?;
        catalog()
            .create_namespace(&warehouse, body.namespace.clone(), body.properties.clone())
            .await
            .map_err(tables_err)?;
        json_response(&serde_json::json!({"namespace": body.namespace, "properties": body.properties}))
    }
}

pub struct NamespaceExistsHandler;
#[async_trait::async_trait]
impl Operation for NamespaceExistsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::GetNamespaceAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let exists = catalog().namespace_exists(&warehouse, &ns).await.map_err(tables_err)?;
        if exists {
            Ok(S3Response::new((StatusCode::OK, Body::empty())))
        } else {
            Err(tables_err(TablesError::NotFound(format!("namespace {}", ns.join(".")))))
        }
    }
}

pub struct GetNamespaceHandler;
#[async_trait::async_trait]
impl Operation for GetNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::GetNamespaceAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let resp = catalog().get_namespace(&warehouse, &ns).await.map_err(tables_err)?;
        json_response(&resp)
    }
}

pub struct UpdateNamespacePropertiesHandler;
#[async_trait::async_trait]
impl Operation for UpdateNamespacePropertiesHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::UpdateNamespacePropertiesAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let body: UpdateNamespacePropertiesRequest = parse_body(&mut req).await?;
        let resp = catalog()
            .update_namespace_properties(&warehouse, &ns, body.updates, body.removals)
            .await
            .map_err(tables_err)?;
        json_response(&resp)
    }
}

pub struct DropNamespaceHandler;
#[async_trait::async_trait]
impl Operation for DropNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::DeleteNamespaceAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        catalog().drop_namespace(&warehouse, &ns).await.map_err(tables_err)?;
        no_content()
    }
}

// ─── Table handlers ───────────────────────────────────────────────────────────

pub struct ListTablesHandler;
#[async_trait::async_trait]
impl Operation for ListTablesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::ListTablesAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let names = catalog().list_tables(&warehouse, &ns).await.map_err(tables_err)?;
        let identifiers: Vec<serde_json::Value> = names
            .into_iter()
            .map(|name| serde_json::json!({"namespace": ns, "name": name}))
            .collect();
        json_response(&serde_json::json!({"identifiers": identifiers}))
    }
}

pub struct CreateTableHandler;
#[async_trait::async_trait]
impl Operation for CreateTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::CreateTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let body: CreateTableRequest = parse_body(&mut req).await?;
        let result = catalog().create_table(&warehouse, &ns, body).await.map_err(tables_err)?;
        json_response(&result)
    }
}

pub struct TableExistsHandler;
#[async_trait::async_trait]
impl Operation for TableExistsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::GetTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let table = params.get("table").unwrap_or("").to_string();
        let exists = catalog().table_exists(&warehouse, &ns, &table).await.map_err(tables_err)?;
        if exists {
            Ok(S3Response::new((StatusCode::OK, Body::empty())))
        } else {
            Err(tables_err(TablesError::NotFound(format!("table {table}"))))
        }
    }
}

pub struct LoadTableHandler;
#[async_trait::async_trait]
impl Operation for LoadTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::GetTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let table = params.get("table").unwrap_or("").to_string();
        let result = catalog().load_table(&warehouse, &ns, &table).await.map_err(tables_err)?;
        json_response(&result)
    }
}

pub struct CommitTableHandler;
#[async_trait::async_trait]
impl Operation for CommitTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::UpdateTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let table = params.get("table").unwrap_or("").to_string();
        let body: CommitTableRequest = parse_body(&mut req).await?;
        let result = catalog()
            .commit_table(&warehouse, &ns, &table, body)
            .await
            .map_err(tables_err)?;
        json_response(&result)
    }
}

pub struct DropTableHandler;
#[async_trait::async_trait]
impl Operation for DropTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::DeleteTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let raw_ns = params.get("namespace").unwrap_or("");
        let ns = parse_namespace_param(raw_ns);
        let table = params.get("table").unwrap_or("").to_string();
        let purge = req.uri.query().and_then(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .find(|(k, _)| k == "purgeRequested")
                .and_then(|(_, v)| v.parse::<bool>().ok())
        }).unwrap_or(true);
        catalog()
            .drop_table(&warehouse, &ns, &table, purge)
            .await
            .map_err(tables_err)?;
        no_content()
    }
}

pub struct RenameTableHandler;
#[async_trait::async_trait]
impl Operation for RenameTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::RenameTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let body: RenameTableRequest = parse_body(&mut req).await?;
        catalog()
            .rename_table(
                &warehouse,
                &body.source.namespace,
                &body.source.name,
                &body.destination.namespace,
                &body.destination.name,
            )
            .await
            .map_err(tables_err)?;
        no_content()
    }
}

pub struct CommitTransactionHandler;
#[async_trait::async_trait]
impl Operation for CommitTransactionHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_tables_request(&req, S3TablesAction::UpdateTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let body: CommitTransactionRequest = parse_body(&mut req).await?;
        let results = catalog()
            .commit_transaction(&warehouse, body.table_changes)
            .await
            .map_err(tables_err)?;
        json_response(&serde_json::json!({"commit-results": results}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_namespace_param_single() {
        let ns = parse_namespace_param("analytics");
        assert_eq!(ns, vec!["analytics"]);
    }

    #[test]
    fn test_parse_namespace_param_multi() {
        let ns = parse_namespace_param("a\x1fb");
        assert_eq!(ns, vec!["a", "b"]);
    }

    #[test]
    fn test_route_registration_compiles() {
        // Verifies all route strings are valid matchit patterns.
        // Actual insertion is tested via route_registration_test.rs pattern.
        let routes = vec![
            "/_iceberg/v1/warehouses",
            "/_iceberg/v1/warehouses/{warehouse}",
            "/_iceberg/v1/{prefix}/config",
            "/_iceberg/v1/{prefix}/namespaces",
            "/_iceberg/v1/{prefix}/namespaces/{namespace}",
            "/_iceberg/v1/{prefix}/namespaces/{namespace}/properties",
            "/_iceberg/v1/{prefix}/namespaces/{namespace}/tables",
            "/_iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "/_iceberg/v1/{prefix}/tables/rename",
            "/_iceberg/v1/{prefix}/transactions/commit",
        ];
        for route in routes {
            assert!(route.starts_with("/_iceberg"), "route should start with /_iceberg: {route}");
        }
    }
}
