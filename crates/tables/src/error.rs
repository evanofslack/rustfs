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

use hyper::StatusCode;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, TablesError>;

#[derive(Debug, Error)]
pub enum TablesError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("commit conflict: {0}")]
    CommitConflict(String),

    #[error("not empty: {0}")]
    NotEmpty(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("purge not supported: {0}")]
    PurgeNotSupported(String),

    #[error("table recovery in progress: {0}")]
    RecoveryInProgress(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl TablesError {
    pub fn http_status(&self) -> StatusCode {
        match self {
            TablesError::NotFound(_) => StatusCode::NOT_FOUND,
            TablesError::AlreadyExists(_) | TablesError::CommitConflict(_) | TablesError::NotEmpty(_) => {
                StatusCode::CONFLICT
            }
            TablesError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            TablesError::PurgeNotSupported(_) => StatusCode::NOT_IMPLEMENTED,
            TablesError::RecoveryInProgress(_) => StatusCode::SERVICE_UNAVAILABLE,
            TablesError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn error_type(&self) -> &'static str {
        match self {
            TablesError::NotFound(_) => "IcebergTableNotFound",
            TablesError::AlreadyExists(_) => "IcebergTableAlreadyExists",
            TablesError::CommitConflict(_) => "CommitFailedException",
            TablesError::NotEmpty(_) => "IcebergNamespaceNotEmptyError",
            TablesError::InvalidRequest(_) => "BadRequest",
            TablesError::PurgeNotSupported(_) => "IcebergPurgeNotSupported",
            TablesError::RecoveryInProgress(_) => "TableRecoveryInProgress",
            TablesError::Internal(_) => "InternalError",
        }
    }
}

impl From<serde_json::Error> for TablesError {
    fn from(e: serde_json::Error) -> Self {
        TablesError::Internal(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_status_mapping() {
        assert_eq!(TablesError::NotFound("x".into()).http_status(), StatusCode::NOT_FOUND);
        assert_eq!(TablesError::AlreadyExists("x".into()).http_status(), StatusCode::CONFLICT);
        assert_eq!(TablesError::CommitConflict("x".into()).http_status(), StatusCode::CONFLICT);
        assert_eq!(TablesError::NotEmpty("x".into()).http_status(), StatusCode::CONFLICT);
        assert_eq!(TablesError::InvalidRequest("x".into()).http_status(), StatusCode::BAD_REQUEST);
        assert_eq!(TablesError::PurgeNotSupported("x".into()).http_status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            TablesError::RecoveryInProgress("x".into()).http_status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(TablesError::Internal("x".into()).http_status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_error_type_strings() {
        assert_eq!(TablesError::NotFound("x".into()).error_type(), "IcebergTableNotFound");
        assert_eq!(TablesError::AlreadyExists("x".into()).error_type(), "IcebergTableAlreadyExists");
        assert_eq!(TablesError::CommitConflict("x".into()).error_type(), "CommitFailedException");
        assert_eq!(TablesError::NotEmpty("x".into()).error_type(), "IcebergNamespaceNotEmptyError");
        assert_eq!(TablesError::InvalidRequest("x".into()).error_type(), "BadRequest");
        assert_eq!(TablesError::PurgeNotSupported("x".into()).error_type(), "IcebergPurgeNotSupported");
        assert_eq!(TablesError::RecoveryInProgress("x".into()).error_type(), "TableRecoveryInProgress");
        assert_eq!(TablesError::Internal("x".into()).error_type(), "InternalError");
    }
}
