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
# Create all files locally, then `aws s3 sync` in one shot
#
# Usage:
#   ./seed.sh              # Seed all tiers
#   ./seed.sh 1000 5000    # Seed only specific tiers

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.sh"

require_tools aws

check_endpoint

# Allow overriding tiers via positional args.
if [ $# -gt 0 ]; then
    TIERS=("$@")
fi

# Create a staging area for local files. Cleaned up on exit.
STAGE_DIR=$(mktemp -d)
trap 'rm -rf "$STAGE_DIR"' EXIT

# Create a single 1 KB payload file that we hardlink everywhere.
PAYLOAD_FILE="$STAGE_DIR/.payload"
dd if=/dev/urandom bs=1024 count=1 of="$PAYLOAD_FILE" 2>/dev/null

# Create local files for a flat layout.
create_flat_files() {
    local count=$1
    local dir="$STAGE_DIR/flat-${count}"
    mkdir -p "$dir"

    log "Creating $count local files (flat)..."
    for i in $(seq 0 $((count - 1))); do
        ln "$PAYLOAD_FILE" "$dir/$(printf 'obj-%06d' "$i")"
    done
}

# Create local files for a nested layout.
create_nested_files() {
    local count=$1
    local dir="$STAGE_DIR/nested-${count}"
    mkdir -p "$dir"

    local objects_per_prefix=$((count / NESTED_PREFIX_COUNT))
    local remainder=$((count % NESTED_PREFIX_COUNT))

    log "Creating $count local files (nested, $NESTED_PREFIX_COUNT prefixes)..."
    for p in $(seq 0 $((NESTED_PREFIX_COUNT - 1))); do
        local prefix_name
        prefix_name=$(printf 'prefix-%03d' "$p")
        mkdir -p "$dir/$prefix_name"

        local this_count=$objects_per_prefix
        if [ "$p" -lt "$remainder" ]; then
            this_count=$((objects_per_prefix + 1))
        fi
        for o in $(seq 0 $((this_count - 1))); do
            ln "$PAYLOAD_FILE" "$dir/$prefix_name/$(printf 'obj-%06d' "$o")"
        done
    done
}

# Sync a local directory to an S3 bucket.
sync_to_bucket() {
    local layout=$1
    local count=$2
    local dir="$STAGE_DIR/${layout}-${count}"
    local bucket
    bucket=$(bucket_name "$layout" "$count")

    # Create bucket if it doesn't exist.
    if ! s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        log "Creating bucket: $bucket"
        s3api create-bucket --bucket "$bucket" >/dev/null
    fi

    log "Syncing $count objects to s3://$bucket/ ..."
    s3 sync "$dir/" "s3://$bucket/" --quiet
    log "Done: $bucket"
}

# Main: seed all tiers.
log "Seeding benchmark buckets (tiers: ${TIERS[*]})"

for tier in "${TIERS[@]}"; do
    # Create local files for both layouts.
    create_flat_files "$tier"
    create_nested_files "$tier"

    # Sync both to S3.
    sync_to_bucket "flat" "$tier"
    sync_to_bucket "nested" "$tier"

    # Free disk space: remove staged files for this tier.
    rm -rf "$STAGE_DIR/flat-${tier}" "$STAGE_DIR/nested-${tier}"
done

log "Seeding complete."
log "Buckets:"
s3api list-buckets --query 'Buckets[?starts_with(Name, `bench-list`)].Name' --output text | tr '\t' '\n' | sort
