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

# Benchmark: Paginated list (iterate through all pages using continuation tokens).
#
# Measures the total time to walk through all pages of a ListObjectsV2 response
# using a page size of 1000 (the default S3 page size). This exercises the
# continuation token and forward-seek path.
#
# Usage:
#   ./bench-paginated.sh              # Run all tiers
#   ./bench-paginated.sh 5000 10000   # Run specific tiers

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.sh"

require_tools aws hyperfine jq

check_endpoint

if [ $# -gt 0 ]; then
    TIERS=("$@")
fi

# Only benchmark tiers with > 1000 objects (otherwise pagination is meaningless).
PAGINATED_TIERS=()
for tier in "${TIERS[@]}"; do
    if [ "$tier" -gt 1000 ]; then
        PAGINATED_TIERS+=("$tier")
    fi
done

if [ ${#PAGINATED_TIERS[@]} -eq 0 ]; then
    log "No tiers > 1000 objects. Pagination benchmark requires > 1000 objects."
    exit 0
fi

LABEL="paginated"
OUTPUT_JSON="$RESULTS_DIR/${LABEL}.json"
PAGE_SIZE="${PAGE_SIZE:-1000}"

# Create a helper script for paginated listing.
# This avoids complex shell escaping inside hyperfine.
PAGINATE_SCRIPT=$(mktemp)
trap 'rm -f "$PAGINATE_SCRIPT"' EXIT

cat > "$PAGINATE_SCRIPT" << 'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
ENDPOINT=$1
BUCKET=$2
PAGE_SIZE=$3

TOKEN=""
TOTAL=0
while true; do
    if [ -z "$TOKEN" ]; then
        RESP=$(aws s3api --endpoint-url "$ENDPOINT" --no-cli-pager \
            list-objects-v2 --bucket "$BUCKET" --max-keys "$PAGE_SIZE" \
            --output json 2>/dev/null)
    else
        RESP=$(aws s3api --endpoint-url "$ENDPOINT" --no-cli-pager \
            list-objects-v2 --bucket "$BUCKET" --max-keys "$PAGE_SIZE" \
            --starting-token "$TOKEN" \
            --output json 2>/dev/null)
    fi

    COUNT=$(echo "$RESP" | jq -r '.KeyCount // 0')
    TOTAL=$((TOTAL + COUNT))
    IS_TRUNCATED=$(echo "$RESP" | jq -r '.IsTruncated // false')
    TOKEN=$(echo "$RESP" | jq -r '.NextContinuationToken // empty')

    if [ "$IS_TRUNCATED" != "true" ] || [ -z "$TOKEN" ]; then
        break
    fi
done
SCRIPT
chmod +x "$PAGINATE_SCRIPT"

log "=== Benchmark: Paginated List (page size: $PAGE_SIZE) ==="
log "Runs: $HYPERFINE_RUNS, Warmup: $HYPERFINE_WARMUP"

COMMANDS=()
NAMES=()

for tier in "${PAGINATED_TIERS[@]}"; do
    for layout in flat nested; do
        bucket=$(bucket_name "$layout" "$tier")

        if ! s3api head-bucket --bucket "$bucket" 2>/dev/null; then
            log "WARN: Bucket $bucket does not exist. Run seed.sh first. Skipping."
            continue
        fi

        pages=$(( (tier + PAGE_SIZE - 1) / PAGE_SIZE ))
        COMMANDS+=("bash $PAGINATE_SCRIPT $ENDPOINT $bucket $PAGE_SIZE")
        NAMES+=("${layout}-${tier} (${pages}p)")
    done
done

if [ ${#COMMANDS[@]} -eq 0 ]; then
    log "No eligible buckets. Run seed.sh first."
    exit 1
fi

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
