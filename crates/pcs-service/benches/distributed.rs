// Distributed benchmarks
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native" \
//     cargo bench --bench distributed --features distributed
//
// Two focused benchmarks:
//   1. Checkpoint serialization — Arrow IPC bytes/sec for a wide-schema pipeline.
//   2. Snapshot build + install round-trip — full state machine dump + restore
//      (requires --features distributed-raft).

#![cfg(feature = "distributed")]

use std::sync::Arc;

use arrow_array::Float64Array;
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use pcs_core::component::Component;
use pcs_core::pipeline::Dataset;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Wide benchmark component — 10 f64 columns
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

fn make_arrow_pipeline(n_rows: usize) -> Dataset {
    use arrow_array::RecordBatch;

    let mut pipeline = Dataset::new();
    pipeline.register_component::<WideRow>().unwrap();

    let col: Vec<f64> = (0..n_rows).map(|i| i as f64).collect();
    let arrays: Vec<Arc<dyn arrow_array::Array>> = (0..10)
        .map(|_| Arc::new(Float64Array::from(col.clone())) as Arc<dyn arrow_array::Array>)
        .collect();

    let schema = WideRow::schema();
    let batch = RecordBatch::try_new(schema, arrays).expect("build batch");
    pipeline
        .append_record_batch(WideRow::name(), batch)
        .expect("append");
    pipeline
}

// ---------------------------------------------------------------------------
// Benchmark 1 — checkpoint serialization: pipeline → Arrow IPC bytes
// ---------------------------------------------------------------------------

fn bench_checkpoint_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_serialization");

    for &n_rows in &[1_000usize, 100_000, 500_000] {
        let label = format!("{}k_rows_10_cols", n_rows / 1_000);
        let pipeline = make_arrow_pipeline(n_rows);

        group.throughput(criterion::Throughput::Elements(n_rows as u64));
        group.bench_function(&label, |b| {
            b.iter(|| {
                let mut buf = Vec::new();
                pipeline
                    .write_ipc(&mut buf)
                    .expect("write_ipc failed in benchmark");
                std::hint::black_box(buf.len())
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 2 — snapshot build + install round-trip (Raft feature only)
// ---------------------------------------------------------------------------

#[cfg(feature = "distributed-raft")]
fn bench_snapshot_round_trip(c: &mut Criterion) {
    use criterion::BatchSize;
    use pcs_service::distributed::consensus::snapshot::{
        build_snapshot_bytes, install_snapshot_bytes,
    };
    use pcs_service::distributed::consensus::state_machine::apply as sm_apply;
    use pcs_service::distributed::consensus::types::ConsensusCommand;
    use redb::Database;
    use uuid::Uuid;

    let mut group = c.benchmark_group("snapshot_round_trip");

    group.bench_function("3_batches_with_claims", |b| {
        b.iter_batched(
            || {
                // Setup: create temp db and seed 3 master batches + 1 claim each.
                let file = tempfile::NamedTempFile::new().expect("tempfile");
                let path = file.into_temp_path();
                let db = Database::create(&path).expect("redb create");
                for i in 0u64..3 {
                    sm_apply(
                        &db,
                        ConsensusCommand::RegisterMasterBatch {
                            batch_id: i,
                            component: format!("comp_{i}"),
                            schema_id: 1,
                            ipc_bytes: vec![0xAB; 512],
                            total_rows: 100,
                            now_at_propose: 0,
                        },
                    )
                    .expect("apply");

                    sm_apply(
                        &db,
                        ConsensusCommand::ClaimRowRange {
                            batch_id: i,
                            row_range_start: 0,
                            row_range_end: 100,
                            claim_id: Uuid::new_v4(),
                            instance_id: Uuid::new_v4(),
                            lease_ttl_millis: 90_000,
                            now_at_propose: 0,
                        },
                    )
                    .expect("apply claim");
                }
                (db, path)
            },
            |(src_db, _path)| {
                // Timed: build snapshot then install into fresh db.
                let snap = build_snapshot_bytes(&src_db).expect("build_snapshot_bytes");
                let file2 = tempfile::NamedTempFile::new().expect("tempfile2");
                let path2 = file2.into_temp_path();
                let dst_db = Database::create(&path2).expect("redb create 2");
                install_snapshot_bytes(&dst_db, &snap, None).expect("install_snapshot_bytes");
                std::hint::black_box(snap.len())
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion groups
// ---------------------------------------------------------------------------

#[cfg(not(feature = "distributed-raft"))]
criterion_group!(benches, bench_checkpoint_serialize);

#[cfg(feature = "distributed-raft")]
criterion_group!(
    benches,
    bench_checkpoint_serialize,
    bench_snapshot_round_trip
);

criterion_main!(benches);
