# Batch Crate Instructions

Applies to `crates/batch/`.

## Purpose

Implements the MinIO-compatible batch job framework, starting with the `replicate` job type.
Wire-level compatible with `mc batch` CLI commands.

## Key Constraints

- Never bypass job deduplication: reject duplicate source+target combinations with HTTP 409.
- Always cancel workers via `CancellationToken` — never force-kill tasks.
- Persist progress to disk every 10 seconds; do not lose progress on restart.
- Job state on disk is the source of truth; the in-memory registry is a cache.

## Adding New Job Types

1. Add a new variant to `BatchJobType` in `job.rs`.
2. Add a corresponding YAML struct in `yaml.rs`.
3. Add a worker implementation in `worker.rs` following the `replicate` pattern.
4. Keep existing behavior for all other job types.

## Testing

- Unit tests live alongside source files.
- Integration tests under `tests/`.
- Do not use `unwrap()` or `expect()` outside tests.
