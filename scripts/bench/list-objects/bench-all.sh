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

# Orchestrator: run all ListObjects benchmarks and generate a combined report.
#
# Usage:
#   ./bench-all.sh              # Seed + run all benchmarks
#   ./bench-all.sh --no-seed    # Skip seeding (buckets already populated)
#   ./bench-all.sh --tiers "1000 5000"  # Override tiers

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.sh"

require_tools aws hyperfine jq

# Parse arguments.
SKIP_SEED=false
while [ $# -gt 0 ]; do
    case "$1" in
    --no-seed)
        SKIP_SEED=true
        shift
        ;;
    --tiers)
        export BENCH_TIERS="$2"
        IFS=' ' read -r -a TIERS <<<"$BENCH_TIERS"
        shift 2
        ;;
    *)
        echo "Unknown argument: $1"
        echo "Usage: $0 [--no-seed] [--tiers \"1000 5000\"]"
        exit 1
        ;;
    esac
done

check_endpoint

START_TIME=$(date +%s)
GIT_REV=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(git -C "$SCRIPT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")

log "======================================================="
log " ListObjects Benchmark Suite"
log " Branch: $GIT_BRANCH  Commit: $GIT_REV"
log " Endpoint: $ENDPOINT"
log " Tiers: ${TIERS[*]}"
log " Runs: $HYPERFINE_RUNS  Warmup: $HYPERFINE_WARMUP"
log "======================================================="

# Step 1: Seed buckets.
if [ "$SKIP_SEED" = false ]; then
    log ""
    log "--- Step 1/4: Seeding buckets ---"
    bash "$SCRIPT_DIR/seed.sh"
else
    log ""
    log "--- Step 1/4: Seeding skipped (--no-seed) ---"
fi

# Step 2: Full list benchmark.
log ""
log "--- Step 2/4: Full list benchmark ---"
bash "$SCRIPT_DIR/bench-full-list.sh"

# Step 3: Paginated list benchmark.
log ""
log "--- Step 3/4: Paginated list benchmark ---"
bash "$SCRIPT_DIR/bench-paginated.sh"

# Step 4: Prefix/delimiter benchmark.
log ""
log "--- Step 4/4: Prefix & delimiter benchmark ---"
bash "$SCRIPT_DIR/bench-prefix.sh"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

# Generate combined report.
REPORT="$RESULTS_DIR/report.md"

cat >"$REPORT" <<EOF
# ListObjects Benchmark Report

- **Date:** $(date '+%Y-%m-%d %H:%M:%S')
- **Branch:** $GIT_BRANCH
- **Commit:** $GIT_REV
- **Endpoint:** $ENDPOINT
- **Tiers:** ${TIERS[*]}
- **Runs per benchmark:** $HYPERFINE_RUNS (warmup: $HYPERFINE_WARMUP)
- **Total time:** ${ELAPSED}s

---

## Full List

$(cat "$RESULTS_DIR/full-list.md" 2>/dev/null || echo "_No results._")

---

## Paginated List

$(cat "$RESULTS_DIR/paginated.md" 2>/dev/null || echo "_No results._")

---

## Prefix & Delimiter

$(cat "$RESULTS_DIR/prefix.md" 2>/dev/null || echo "_No results._")

---

## Raw Data

JSON results are in \`$RESULTS_DIR/\`:
- \`full-list.json\`
- \`paginated.json\`
- \`prefix.json\`
EOF

log ""
log "======================================================="
log " Benchmark suite complete! Total time: ${ELAPSED}s"
log " Report: $REPORT"
log "======================================================="
