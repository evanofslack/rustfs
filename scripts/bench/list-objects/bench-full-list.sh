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

# Benchmark: Full list (list all objects in one call, up to MaxKeys).
#
# Measures the latency of a single ListObjectsV2 call that returns all objects
# in the bucket. This is the simplest and most common list pattern.
#
# Usage:
#   ./bench-full-list.sh              # Run all tiers
#   ./bench-full-list.sh 1000 5000    # Run specific tiers

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.sh"

require_tools aws hyperfine jq

check_endpoint

if [ $# -gt 0 ]; then
    TIERS=("$@")
fi

LABEL="full-list"
OUTPUT_JSON="$RESULTS_DIR/${LABEL}.json"

log "=== Benchmark: Full List ==="
log "Runs: $HYPERFINE_RUNS, Warmup: $HYPERFINE_WARMUP"

COMMANDS=()
NAMES=()

for tier in "${TIERS[@]}"; do
    for layout in flat nested; do
        bucket=$(bucket_name "$layout" "$tier")

        # Verify the bucket exists and is populated.
        if ! s3api head-bucket --bucket "$bucket" 2>/dev/null; then
            log "WARN: Bucket $bucket does not exist. Run seed.sh first. Skipping."
            continue
        fi

        COMMANDS+=("aws s3api --endpoint-url $ENDPOINT --no-cli-pager list-objects-v2 --bucket $bucket --max-items $tier --output json > /dev/null")
        NAMES+=("${layout}-${tier}")
    done
done

if [ ${#COMMANDS[@]} -eq 0 ]; then
    log "No buckets found. Run seed.sh first."
    exit 1
fi

# Build hyperfine arguments.
HYPERFINE_ARGS=(
    --runs "$HYPERFINE_RUNS"
    --warmup "$HYPERFINE_WARMUP"
    --export-json "$OUTPUT_JSON"
    --export-markdown "$RESULTS_DIR/${LABEL}.md"
    --style full
)

for i in "${!COMMANDS[@]}"; do
    HYPERFINE_ARGS+=(--command-name "${NAMES[$i]}" "${COMMANDS[$i]}")
done

log "Running hyperfine..."
hyperfine "${HYPERFINE_ARGS[@]}"

log "Results saved to:"
log "  JSON: $OUTPUT_JSON"
log "  Markdown: $RESULTS_DIR/${LABEL}.md"
