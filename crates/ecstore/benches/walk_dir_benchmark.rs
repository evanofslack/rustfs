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

//! Benchmarks for `walk_dir` and `list_path_raw` performance.
//!
//! These benchmarks measure the filesystem traversal and multi-disk merge
//! pipeline used by ListObjects, scanner, decommission, and rebalance.
//!
//! ## What These Benchmarks Cover
//!
//! - **walk_dir**: Single-disk filesystem traversal through the duplex pipe.
//!   Parameterized by object count. Captures per-object I/O cost including
//!   readdir, read_metadata (open/stat/read/close of xl.meta), and msgpack
//!   serialization. Optimizations like parallel metadata reads, raw getdents64,
//!   and O_NOATIME will show up here.
//!
//! - **walk_dir_mixed**: Same as walk_dir but with a realistic directory layout
//!   containing both objects (dirs with xl.meta) and empty subdirectories.
//!   Exercises the `is_empty_dir` check path (openat + getdents64 per empty
//!   dir). Optimizations like nlink-based empty dir detection will show here.
//!
//! - **list_path_raw**: Full multi-disk pipeline including concurrent walk_dir
//!   tasks feeding the merge/reconciliation loop. Parameterized by disk count
//!   and object count. Shows merge overhead and scaling behavior.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rustfs_ecstore::cache_value::metacache_set::{ListPathRawOptions, list_path_raw};
use rustfs_ecstore::disk::disk_store::LocalDiskWrapper;
use rustfs_ecstore::disk::endpoint::Endpoint;
use rustfs_ecstore::disk::local::LocalDisk;
use rustfs_ecstore::disk::{Disk, DiskAPI, DiskStore, WalkDirOptions};
use rustfs_filemeta::{MetaCacheEntry, MetacacheReader};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

const BUCKET: &str = "bench-bucket";

/// Minimal xl.meta content sufficient for walk_dir to pick up as an object.
fn fake_xl_meta() -> Bytes {
    Bytes::from_static(&[0u8, 10])
}

/// Create a LocalDisk backed by a temporary directory.
///
/// Returns the TempDir guard (drop = cleanup) and a DiskStore.
async fn create_disk(tmp: &TempDir) -> DiskStore {
    let mut ep = Endpoint::try_from(tmp.path().to_str().expect("non-utf8 path")).expect("endpoint");
    ep.is_local = true;

    let disk = LocalDisk::new(&ep, false).await.expect("LocalDisk::new");
    disk.make_volume(BUCKET).await.expect("make_volume");

    Arc::new(Disk::Local(Box::new(LocalDiskWrapper::new(Arc::new(disk), false))))
}

/// Create a temporary directory with a LocalDisk and N fake objects.
///
/// On-disk layout:
///   <tmpdir>/<BUCKET>/obj_NNNNNN/xl.meta
fn setup_bench_disk(rt: &Runtime, object_count: usize) -> (TempDir, DiskStore) {
    rt.block_on(async {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store = create_disk(&tmp).await;

        let meta = fake_xl_meta();
        for i in 0..object_count {
            let key = format!("obj_{i:06}/xl.meta");
            store.write_all(BUCKET, &key, meta.clone()).await.expect("write_all");
        }

        (tmp, store)
    })
}

/// Create a temporary directory with a mixed layout:
/// - `object_count` real objects (dirs with xl.meta)
/// - `empty_dir_count` empty subdirectories (no xl.meta)
///
/// On-disk layout:
///   <tmpdir>/<BUCKET>/obj_NNNNNN/xl.meta    (real objects)
///   <tmpdir>/<BUCKET>/dir_NNNNNN/           (empty dirs)
///
/// This exercises the `is_empty_dir` code path in scan_dir, where each empty
/// directory triggers an openat + getdents64 + close to check emptiness.
fn setup_bench_disk_mixed(rt: &Runtime, object_count: usize, empty_dir_count: usize) -> (TempDir, DiskStore) {
    rt.block_on(async {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store = create_disk(&tmp).await;

        let meta = fake_xl_meta();

        // Create real objects (dir with xl.meta).
        for i in 0..object_count {
            let key = format!("obj_{i:06}/xl.meta");
            store.write_all(BUCKET, &key, meta.clone()).await.expect("write_all");
        }

        // Create empty subdirectories.
        // These have no xl.meta, so scan_dir will call is_empty_dir on each.
        let bucket_path = tmp.path().join(BUCKET);
        for i in 0..empty_dir_count {
            let dir = bucket_path.join(format!("dir_{i:06}"));
            tokio::fs::create_dir_all(&dir).await.expect("create empty dir");
        }

        (tmp, store)
    })
}

/// Create multiple temp disks, each containing the same N objects.
fn setup_bench_disks(rt: &Runtime, disk_count: usize, object_count: usize) -> (Vec<TempDir>, Vec<DiskStore>) {
    let mut dirs = Vec::with_capacity(disk_count);
    let mut stores = Vec::with_capacity(disk_count);
    for _ in 0..disk_count {
        let (dir, store) = setup_bench_disk(rt, object_count);
        dirs.push(dir);
        stores.push(store);
    }
    (dirs, stores)
}

/// Helper: run walk_dir on a store and drain all results.
async fn run_walk_dir(store: DiskStore) -> u64 {
    let (rd, mut wr) = tokio::io::duplex(64);

    let walk_opts = WalkDirOptions {
        bucket: BUCKET.to_string(),
        base_dir: "".to_string(),
        recursive: true,
        ..Default::default()
    };

    let writer_handle = tokio::spawn(async move {
        let _ = store.walk_dir(walk_opts, &mut wr).await;
    });

    let mut reader = MetacacheReader::new(rd);
    let mut count = 0u64;
    while let Ok(Some(_)) = reader.peek().await {
        let _ = reader.skip(1).await;
        count += 1;
    }

    writer_handle.await.expect("walk_dir task panicked");
    count
}

// Bench 1: walk_dir — single disk, objects only
fn bench_walk_dir(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    let object_counts = [100, 500, 1000, 5000];

    let mut group = c.benchmark_group("walk_dir");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    for &obj_count in &object_counts {
        let (_tmp, store) = setup_bench_disk(&rt, obj_count);

        group.bench_with_input(BenchmarkId::new("objects_only", format!("{obj_count}_objs")), &store, |b, store| {
            b.to_async(&rt).iter(|| {
                let store = store.clone();
                async move {
                    std::hint::black_box(run_walk_dir(store).await);
                }
            });
        });
    }

    group.finish();
}

// Bench 2: walk_dir — mixed layout (objects + empty dirs)
fn bench_walk_dir_mixed(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    // Each config is (objects, empty_dirs).
    // The empty dirs exercise is_empty_dir (readdir-based today, nlink-based after fix).
    let configs: Vec<(usize, usize, &str)> = vec![
        (100, 0, "100_objs/0_empty"),         // baseline: no empty dirs
        (100, 100, "100_objs/100_empty"),     // 50% empty dirs
        (100, 500, "100_objs/500_empty"),     // mostly empty dirs
        (500, 500, "500_objs/500_empty"),     // equal mix
        (1000, 1000, "1000_objs/1000_empty"), // large equal mix
    ];

    let mut group = c.benchmark_group("walk_dir_mixed");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for (obj_count, empty_count, label) in &configs {
        let (_tmp, store) = setup_bench_disk_mixed(&rt, *obj_count, *empty_count);

        group.bench_with_input(BenchmarkId::new("mixed", label), &store, |b, store| {
            b.to_async(&rt).iter(|| {
                let store = store.clone();
                async move {
                    std::hint::black_box(run_walk_dir(store).await);
                }
            });
        });
    }

    group.finish();
}

// Bench 3: list_path_raw — multi-disk merge
fn bench_list_path_raw(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    let configs: Vec<(usize, usize)> = vec![
        // (disk_count, object_count)
        (1, 100),
        (1, 500),
        (1, 1000),
        (2, 100),
        (2, 500),
        (2, 1000),
        (4, 100),
        (4, 500),
        (4, 1000),
    ];

    let mut group = c.benchmark_group("list_path_raw");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &(disk_count, obj_count) in &configs {
        let (_tmps, stores) = setup_bench_disks(&rt, disk_count, obj_count);
        let param = format!("{disk_count}_disks/{obj_count}_objs");

        group.bench_with_input(BenchmarkId::new("merge", &param), &stores, |b, stores| {
            b.to_async(&rt).iter(|| {
                let stores = stores.clone();
                async move {
                    let cancel = CancellationToken::new();
                    let agreed_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
                    let partial_count = Arc::new(std::sync::atomic::AtomicU64::new(0));

                    let ac = agreed_count.clone();
                    let pc = partial_count.clone();

                    let opts = ListPathRawOptions {
                        disks: stores.iter().map(|s| Some(s.clone())).collect(),
                        fallback_disks: vec![],
                        bucket: BUCKET.to_string(),
                        path: "".to_string(),
                        recursive: true,
                        min_disks: disk_count,
                        report_not_found: false,
                        per_disk_limit: -1,
                        agreed: Some(Box::new(move |_entry: MetaCacheEntry| {
                            let ac = ac.clone();
                            Box::pin(async move {
                                ac.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            })
                        })),
                        partial: Some(Box::new(move |_entries, _errs| {
                            let pc = pc.clone();
                            Box::pin(async move {
                                pc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            })
                        })),
                        finished: None,
                        filter_prefix: None,
                        forward_to: None,
                    };

                    let _ = list_path_raw(cancel, opts).await;

                    std::hint::black_box((
                        agreed_count.load(std::sync::atomic::Ordering::Relaxed),
                        partial_count.load(std::sync::atomic::Ordering::Relaxed),
                    ));
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_walk_dir, bench_walk_dir_mixed, bench_list_path_raw,);

criterion_main!(benches);
