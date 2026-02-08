# ListObjects Integration Benchmarks

End-to-end benchmarks for `ListObjectsV2` API performance, using `aws` CLI
and [`hyperfine`](https://github.com/sharkdp/hyperfine) for statistical timing.

## Prerequisites

Install the required tools:

```bash
# macOS
brew install awscli hyperfine jq

# Linux (Debian/Ubuntu)
apt install awscli jq
cargo install hyperfine
```

## Setup

These scripts benchmark against a **running RustFS instance**. Start one
separately before running benchmarks:

```bash
# Option 1: Docker compose (from repo root)
docker compose -f docker-compose-simple.yml up -d

# Option 2: Local binary
cargo build --release
./target/release/rustfs server /tmp/rustfs-data --address :9000
```

Default endpoint is `http://localhost:9000`. Override via environment variable:

```bash
export S3_ENDPOINT=http://localhost:9000
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
```

## Quick Start

Run the full benchmark suite (seed + all benchmarks):

```bash
cd scripts/bench/list-objects
./bench-all.sh
```

## Scripts

| Script               | Description                                                                 |
| -------------------- | --------------------------------------------------------------------------- |
| `config.sh`          | Shared configuration (endpoint, tiers, helpers). Sourced by other scripts.  |
| `seed.sh`            | Creates buckets and populates them with objects in flat and nested layouts. |
| `bench-full-list.sh` | Benchmarks a single `list-objects-v2` call returning all objects.           |
| `bench-paginated.sh` | Benchmarks paginated listing using continuation tokens (tiers > 1000).      |
| `bench-prefix.sh`    | Benchmarks prefix-filtered listing and delimiter-based directory listing.   |
| `bench-all.sh`       | Orchestrates: seed → full-list → paginated → prefix. Generates a report.    |
| `cleanup.sh`         | Removes all benchmark buckets and result files.                             |

## Configuration

All settings can be overridden via environment variables:

| Variable                | Default                     | Description                          |
| ----------------------- | --------------------------- | ------------------------------------ |
| `S3_ENDPOINT`           | `http://localhost:9000`     | RustFS endpoint URL                  |
| `AWS_ACCESS_KEY_ID`     | `rustfsadmin`               | S3 access key                        |
| `AWS_SECRET_ACCESS_KEY` | `rustfsadmin`               | S3 secret key                        |
| `BENCH_TIERS`           | `100 1000 5000 10000 50000` | Space-separated object count tiers   |
| `BENCH_NESTED_PREFIXES` | `100`                       | Number of prefixes for nested layout |
| `HYPERFINE_RUNS`        | `10`                        | Number of benchmark iterations       |
| `HYPERFINE_WARMUP`      | `2`                         | Number of warmup iterations          |
| `SEED_PARALLELISM`      | `32`                        | Parallel uploads during seeding      |
| `RESULTS_DIR`           | `target/bench-results`      | Output directory for results         |
| `PAGE_SIZE`             | `1000`                      | Page size for paginated benchmark    |

## Running Individual Benchmarks

```bash
# Seed only specific tiers
./seed.sh 1000 5000

# Run a single benchmark
./bench-full-list.sh 1000 5000

# Run with custom settings
HYPERFINE_RUNS=20 HYPERFINE_WARMUP=5 ./bench-paginated.sh 10000

# Skip seeding (buckets already exist)
./bench-all.sh --no-seed

# Override tiers
./bench-all.sh --tiers "1000 10000"
```

```

## Output

Results are written to `target/bench-results/` (or `$RESULTS_DIR`):

```

target/bench-results/
├── full-list.json # Raw hyperfine JSON output
├── full-list.md # Markdown table
├── paginated.json
├── paginated.md
├── prefix.json
├── prefix.md
└── report.md # Combined report with metadata

````

## Cleanup

```bash
# Remove all benchmark buckets and results
./cleanup.sh

# Remove buckets only, keep results
./cleanup.sh --keep-results
````

## Bucket Layouts

The benchmarks test two directory layouts to cover different I/O patterns:

- **Flat**: All objects at root level (`obj-000000`, `obj-000001`, ...).
  Tests sequential directory scanning performance.

- **Nested**: Objects distributed across prefixes
  (`prefix-000/obj-000000`, `prefix-001/obj-000000`, ...).
  Tests recursive traversal and prefix filtering.
