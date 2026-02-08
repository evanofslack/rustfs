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

# Seed benchmarking buckets with objects.
#
# Creates buckets in two layouts:
#   - flat:   all objects at the root (key = "obj-NNNNNN")
#   - nested: objects distributed under NESTED_PREFIX_COUNT prefixes
#             (key = "prefix-NNN/obj-NNNNNN")
#
# Usage:
#   ./seed.sh              # Seed all tiers
#   ./seed.sh 1000 5000    # Seed only specific tiers

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.sh"

require_tools aws jq

check_endpoint

# Allow overriding tiers via positional args.
if [ $# -gt 0 ]; then
    TIERS=("$@")
fi

# Generate a small payload (1 KB) for all objects.
PAYLOAD_FILE=$(mktemp)
dd if=/dev/urandom bs=1024 count=1 of="$PAYLOAD_FILE" 2>/dev/null
trap 'rm -f "$PAYLOAD_FILE"' EXIT

# Seed a single flat bucket.
seed_flat_bucket() {
    local count=$1
    local bucket
    bucket=$(bucket_name "flat" "$count")

    # Create bucket if it doesn't exist.
    if ! s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        log "Creating bucket: $bucket"
        s3api create-bucket --bucket "$bucket" >/dev/null
    fi

    # Check existing object count to allow resume.
    local existing
    existing=$(bucket_object_count "$bucket")
    if [ "$existing" -ge "$count" ]; then
        log "Bucket $bucket already has $existing objects (>= $count). Skipping."
        return
    fi

    log "Seeding $bucket with $count objects (flat layout, $SEED_PARALLELISM parallel)..."

    # Use xargs for parallel upload.
    seq 0 $((count - 1)) | xargs -P "$SEED_PARALLELISM" -I{} \
        aws s3api --endpoint-url "$ENDPOINT" --no-cli-pager \
        put-object --bucket "$bucket" \
        --key "$(printf 'obj-%06d' {})" \
        --body "$PAYLOAD_FILE" \
        --output text --query '"."' 2>/dev/null

    local final_count
    final_count=$(bucket_object_count "$bucket")
    log "Bucket $bucket: $final_count objects"
}

# Seed a single nested bucket.
seed_nested_bucket() {
    local count=$1
    local bucket
    bucket=$(bucket_name "nested" "$count")

    # Create bucket if it doesn't exist.
    if ! s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        log "Creating bucket: $bucket"
        s3api create-bucket --bucket "$bucket" >/dev/null
    fi

    # Check existing object count.
    local existing
    existing=$(bucket_object_count "$bucket")
    if [ "$existing" -ge "$count" ]; then
        log "Bucket $bucket already has $existing objects (>= $count). Skipping."
        return
    fi

    log "Seeding $bucket with $count objects (nested under $NESTED_PREFIX_COUNT prefixes, $SEED_PARALLELISM parallel)..."

    local objects_per_prefix=$(( count / NESTED_PREFIX_COUNT ))
    local remainder=$(( count % NESTED_PREFIX_COUNT ))

    # Generate all keys and upload in parallel.
    {
        for p in $(seq 0 $((NESTED_PREFIX_COUNT - 1))); do
            local prefix_name
            prefix_name=$(printf 'prefix-%03d' "$p")
            local this_count=$objects_per_prefix
            if [ "$p" -lt "$remainder" ]; then
                this_count=$((objects_per_prefix + 1))
            fi
            for o in $(seq 0 $((this_count - 1))); do
                printf '%s/obj-%06d\n' "$prefix_name" "$o"
            done
        done
    } | xargs -P "$SEED_PARALLELISM" -I{} \
        aws s3api --endpoint-url "$ENDPOINT" --no-cli-pager \
        put-object --bucket "$bucket" \
        --key "{}" \
        --body "$PAYLOAD_FILE" \
        --output text --query '"."' 2>/dev/null

    local final_count
    final_count=$(bucket_object_count "$bucket")
    log "Bucket $bucket: $final_count objects"
}

# Main: seed all tiers.
log "Seeding benchmarks buckets (tiers: ${TIERS[*]})"
for tier in "${TIERS[@]}"; do
    seed_flat_bucket "$tier"
    seed_nested_bucket "$tier"
done

log "Seeding complete."
log "Buckets created:"
s3api list-buckets --query 'Buckets[?starts_with(Name, `bench-list`)].Name' --output text | tr '\t' '\n' | sort
