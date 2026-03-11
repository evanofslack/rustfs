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

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BatchError {
    #[error("job not found: {0}")]
    JobNotFound(String),

    #[error("duplicate job: an active job already exists for this source+target combination")]
    DuplicateJob,

    #[error("job already completed")]
    JobAlreadyCompleted,

    #[error("job was cancelled")]
    JobCancelled,

    #[error("unsupported job type: {0}")]
    UnsupportedJobType(String),

    #[error("invalid job definition: {0}")]
    InvalidJobDefinition(String),

    #[error("YAML parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("S3 client error: {0}")]
    S3Client(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("transfer error: {0}")]
    Transfer(String),
}

pub type Result<T> = std::result::Result<T, BatchError>;
