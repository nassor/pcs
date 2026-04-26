// Pipeline benchmarks
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native" cargo bench --bench pipeline
//
// Three focused comparisons:
//   1. IPC round-trip (Pipeline write+read vs postcard encode+decode) — the main
//      zero-copy-IPC win for Arrow-native checkpoints.
//   2. Append 1M rows — "don't regress" baseline.
//   3. Column scan (sum f64 over 1M rows) — cache-efficient projection check.

use std::sync::Arc;

use arrow_array::Float64Array;
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use pcs_core::{component::Component, pipeline::Dataset};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Benchmark component — 10 f64 fields, a wide-schema shape
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct WideRow {
    f0: f64,
    f1: f64,
    f2: f64,
    f3: f64,
    f4: f64,
    f5: f64,
    f6: f64,
    f7: f64,
    f8: f64,
    f9: f64,
}

impl Component for WideRow {
    fn name() -> &'static str {
        "WideRow"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(
            (0..10)
                .map(|i| Field::new(format!("f{i}"), DataType::Float64, false))
                .collect::<Vec<_>>(),
        ))
    }
}

fn make_wide_rows(n: usize) -> Vec<WideRow> {
    (0..n)
        .map(|i| {
            let v = i as f64;
            WideRow {
                f0: v,
                f1: v + 1.0,
                f2: v + 2.0,
                f3: v + 3.0,
                f4: v + 4.0,
                f5: v + 5.0,
                f6: v + 6.0,
                f7: v + 7.0,
                f8: v + 8.0,
                f9: v + 9.0,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmark 1 — IPC round-trip vs postcard round-trip for 1M rows
// ---------------------------------------------------------------------------

fn bench_ipc_round_trip(c: &mut Criterion) {
    let rows = make_wide_rows(1_000_000);

    // Build the Pipeline once and keep it for the encode benchmark.
    let mut pipeline = Dataset::new();
    pipeline.register_component::<WideRow>().unwrap();
    pipeline.append::<WideRow>(&rows).unwrap();

    // --- Pipeline IPC encode ---
    let mut group = c.benchmark_group("ipc_roundtrip_1m_rows");
    group.sample_size(10);

    group.bench_function("ipc_encode", |b| {
        b.iter(|| {
            let mut buf: Vec<u8> = Vec::with_capacity(128 * 1024 * 1024);
            pipeline.write_ipc(&mut buf).unwrap();
            buf
        });
    });

    // Pre-build the IPC bytes for the decode benchmark.
    let mut ipc_bytes: Vec<u8> = Vec::new();
    pipeline.write_ipc(&mut ipc_bytes).unwrap();

    group.bench_function("ipc_decode", |b| {
        b.iter(|| {
            let mut cursor = std::io::Cursor::new(&ipc_bytes);
            Dataset::read_ipc(&mut cursor).unwrap()
        });
    });

    // --- postcard baseline: encode Vec<WideRow> ---
    group.bench_function("postcard_encode", |b| {
        b.iter(|| postcard::to_allocvec(&rows).unwrap());
    });

    let postcard_bytes = postcard::to_allocvec(&rows).unwrap();

    group.bench_function("postcard_decode", |b| {
        b.iter(|| {
            let _decoded: Vec<WideRow> = postcard::from_bytes(&postcard_bytes).unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 2 — Append 1M rows (don't-regress baseline)
// ---------------------------------------------------------------------------

fn bench_append_1m_rows(c: &mut Criterion) {
    let rows = make_wide_rows(1_000_000);

    c.bench_function("world_append_1m", |b| {
        b.iter(|| {
            let mut pipeline = Dataset::new();
            pipeline.register_component::<WideRow>().unwrap();
            pipeline.append::<WideRow>(&rows).unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// Benchmark 3 — Column scan: sum f64 over 1M rows
// ---------------------------------------------------------------------------

fn bench_column_scan_1m(c: &mut Criterion) {
    let rows = make_wide_rows(1_000_000);

    let mut pipeline = Dataset::new();
    pipeline.register_component::<WideRow>().unwrap();
    pipeline.append::<WideRow>(&rows).unwrap();

    // Pipeline: borrow the f0 column and sum it.
    c.bench_function("world_column_scan_sum_f64_1m", |b| {
        b.iter(|| {
            let col = pipeline.column::<WideRow>("f0").unwrap();
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let sum: f64 = arr.values().iter().sum();
            sum
        });
    });

    // Baseline: plain Vec<WideRow> scalar loop.
    c.bench_function("vec_scalar_sum_f64_1m", |b| {
        b.iter(|| {
            let sum: f64 = rows.iter().map(|r| r.f0).sum();
            sum
        });
    });
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_ipc_round_trip,
    bench_append_1m_rows,
    bench_column_scan_1m
);
criterion_main!(benches);
