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

# Cleanup: remove all benchmark buckets and results.
#
# Usage:
#   ./cleanup.sh            # Remove buckets + results
#   ./cleanup.sh --keep-results  # Remove buckets only

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.sh"

require_tools aws

check_endpoint

KEEP_RESULTS=false
if [ "${1:-}" = "--keep-results" ]; then
    KEEP_RESULTS=true
fi

# List all bench buckets.
BUCKETS=$(s3api list-buckets --query "Buckets[?starts_with(Name, \`${BUCKET_PREFIX}\`)].Name" --output text 2>/dev/null | tr '\t' ' ')

if [ -z "$BUCKETS" ] || [ "$BUCKETS" = "None" ]; then
    log "No benchmark buckets found."
else
    for bucket in $BUCKETS; do
        log "Removing bucket: $bucket (deleting all objects first)..."
        # Use aws s3 rb --force which handles deletion of all objects.
        s3 rb "s3://$bucket" --force 2>/dev/null || true
        log "Removed: $bucket"
    done
fi

# Clean results directory.
if [ "$KEEP_RESULTS" = false ] && [ -d "$RESULTS_DIR" ]; then
    log "Removing results directory: $RESULTS_DIR"
    rm -rf "$RESULTS_DIR"
    log "Results removed."
else
    log "Results kept at: $RESULTS_DIR"
fi

log "Cleanup complete."
