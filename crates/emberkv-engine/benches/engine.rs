//! Micro-benchmarks for the storage engine.
//!
//! Run with `cargo bench -p emberkv-engine`. Writes use `SyncPolicy::Never`
//! so the numbers measure the engine rather than the disk's fsync latency.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use emberkv_engine::{Engine, Options, SyncPolicy};

const KEYS: u64 = 100_000;

fn key(i: u64) -> Vec<u8> {
    format!("key:{i:010}").into_bytes()
}

/// Cheap deterministic key picker (SplitMix64) so runs are reproducible.
fn next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, Options::default().sync(SyncPolicy::Never)).expect("open engine")
}

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("put");
    for size in [128usize, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        let value = vec![0xAB; size];
        let mut rng = 1;
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| engine.put(&key(next(&mut rng) % KEYS), black_box(&value)).unwrap());
        });
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let value = vec![0xCD; 128];
    for i in 0..KEYS {
        engine.put(&key(i), &value).unwrap();
    }
    let mut rng = 7;
    c.bench_function("get/128 (hit)", |b| {
        b.iter(|| black_box(engine.get(&key(next(&mut rng) % KEYS)).unwrap()));
    });
    c.bench_function("get (miss)", |b| {
        b.iter(|| black_box(engine.get(b"does-not-exist").unwrap()));
    });
    c.bench_function("scan_prefix/100", |b| {
        b.iter(|| black_box(engine.scan_prefix(b"key:00000", 100).unwrap()));
    });
}

fn bench_recovery(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        let value = vec![0xEF; 128];
        for i in 0..KEYS {
            engine.put(&key(i), &value).unwrap();
        }
    }
    let mut group = c.benchmark_group("recovery");
    group.sample_size(10);
    group.throughput(Throughput::Elements(KEYS));
    group.bench_function("open/100k keys", |b| b.iter(|| drop(open(dir.path()))));
    group.finish();
}

criterion_group!(benches, bench_put, bench_get, bench_recovery);
criterion_main!(benches);
