#!/usr/bin/env bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Shared configuration for ListObjects benchmark scripts.
# Source this file from other scripts: source "$(dirname "$0")/config.sh"

set -euo pipefail

DefaultAccessKey=rustfsadmin
DefaultSecretKey=rustfsadmin
DefaultRegion=us-east-1
# DefaultEndpoint=http://localhost:9000
DefaultEndpoint=http://10.33.1.166:9000

DefaultObjectsTiers="100 1000 5000 10000"
DefaultNestedPrefixes=100

DefaultHyperfineRuns=10
DefaultHyperfineWarmup=2
DefaultSeedParallelism=32

DefaultResultsPath='./results'

# --- Endpoint & credentials ------------------------------------------------
# Override via environment variables before running any script.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$DefaultAccessKey}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$DefaultSecretKey}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$DefaultRegion}"
ENDPOINT="${S3_ENDPOINT:-$DefaultEndpoint}"

# --- Benchmark parameters ---------------------------------------------------
# Object count tiers to seed and benchmark. Override with BENCH_TIERS env var.
IFS=' ' read -r -a TIERS <<<"${BENCH_TIERS:-$DefaultObjectsTiers}"

# Number of prefixes for nested layout buckets.
NESTED_PREFIX_COUNT="${BENCH_NESTED_PREFIXES:-$DefaultNestedPrefixes}"

# Bucket name prefix (avoids collision with real data).
BUCKET_PREFIX="rustfs-bench-list"

# Hyperfine parameters.
HYPERFINE_RUNS="${HYPERFINE_RUNS:-$DefaultHyperfineRuns}"
HYPERFINE_WARMUP="${HYPERFINE_WARMUP:-$DefaultHyperfineWarmup}"

# Parallel upload concurrency for seeding.
SEED_PARALLELISM="${SEED_PARALLELISM:-$DefaultSeedParallelism}"

# Results output directory.
RESULTS_DIR="${RESULTS_DIR:-$DefaultResultsPath}"
mkdir -p "$RESULTS_DIR"

# --- Helpers ----------------------------------------------------------------

# Wrapper around aws s3api that injects the endpoint URL and suppresses pager.
s3api() {
    aws s3api --endpoint-url "$ENDPOINT" --no-cli-pager "$@"
}

# Wrapper around aws s3 that injects the endpoint URL.
s3() {
    aws s3 --endpoint-url "$ENDPOINT" "$@"
}

# Print a timestamped log message.
log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

# Check that required CLI tools are installed.
require_tools() {
    local missing=()
    for tool in "$@"; do
        if ! command -v "$tool" &>/dev/null; then
            missing+=("$tool")
        fi
    done
    if [ ${#missing[@]} -gt 0 ]; then
        echo "ERROR: Missing required tools: ${missing[*]}"
        echo ""
        echo "Install them:"
        echo "  brew install ${missing[*]}    # macOS"
        echo "  apt install ${missing[*]}     # Debian/Ubuntu"
        echo "  cargo install ${missing[*]}   # Rust-based tools (hyperfine)"
        exit 1
    fi
}

# Verify connectivity to the S3 endpoint.
check_endpoint() {
    if ! s3api list-buckets --query 'Buckets[0].Name' --output text &>/dev/null; then
        echo "ERROR: Cannot connect to S3 endpoint at $ENDPOINT"
        echo "Make sure RustFS is running. These scripts do not manage the server."
        exit 1
    fi
    log "Connected to $ENDPOINT"
}

# Get bucket name for a given tier and layout.
bucket_name() {
    local layout=$1 # "flat" or "nested"
    local count=$2
    echo "${BUCKET_PREFIX}-${layout}-${count}"
}

# Count objects in a bucket (handles pagination).
bucket_object_count() {
    local bucket=$1
    s3 ls "s3://$bucket/" --recursive --summarize 2>/dev/null |
        awk '/Total Objects:/{print $3}'
}
