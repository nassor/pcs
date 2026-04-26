// DataFusion vs PCS: TPC-H Q6 comparison
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
//     cargo bench --bench vs_datafusion_q6 --features datafusion -- --sample-size 10
//
// This benchmark compares PCS's Scheduler against DataFusion for TPC-H Q6.
//
// WHY pcs LOSES AND WHY THAT IS FINE:
//   DataFusion has a mature vectorized query executor, JIT-like expression
//   compilation, and a cost-based optimizer tuned for OLAP workloads.
//   PCS is not an OLAP query engine — it is a distributed batch processing
//   engine for imperative pipeline workloads. Single-query SQL is not PCS's
//   target use case; DataFusion is the right tool for SQL-first workloads.
//   PCS's advantages are in:
//     - Schema-flexible imperative systems (ML feature pipelines, ETL logic)
//     - Distributed at-least-once batch processing with checkpoint/recovery
//     - Composable stage DAGs not expressible as a single SQL query
//   Losing to DataFusion on SQL Q6 is expected (2-10×). The point of this
//   benchmark is to document the gap honestly so the README does not overclaim.
//
// Data: 1M rows, same synthetic lineitem generator as tpch_q6.rs (seed=42).
// Q6 SQL:
//   SELECT SUM(l_extendedprice * l_discount) AS revenue
//   FROM lineitem
//   WHERE l_shipdate >= 8766   -- 1994-01-01 as days since epoch
//     AND l_shipdate < 9131    -- 1995-01-01
//     AND l_discount BETWEEN 0.05 AND 0.07
//     AND l_quantity < 24;
//
// Note: we use integer dates (days since epoch) throughout since our synthetic
// data represents dates that way. DataFusion operates on the same Int32 column.

use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, UInt8Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use pcs_core::PcsError;
use pcs_core::component::Component;
use pcs_core::pipeline::{Dataset, Pipeline};
use pcs_core::system::{ParallelSystem, ResourceUpdate, System, SystemMeta, WriteSet};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Date constants
// ---------------------------------------------------------------------------
const SHIPDATE_GE: i32 = 8766;
const SHIPDATE_LT: i32 = 9131;
const DISCOUNT_LO: f64 = 0.05;
const DISCOUNT_HI: f64 = 0.07;
const QUANTITY_LT: f64 = 24.0;

// ---------------------------------------------------------------------------
// Lineitem component (12-column schema)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct Lineitem {
    l_orderkey: i64,
    l_partkey: i64,
    l_suppkey: i64,
    l_linenumber: i32,
    l_quantity: f64,
    l_extendedprice: f64,
    l_discount: f64,
    l_tax: f64,
    l_returnflag: u8,
    l_linestatus: u8,
    l_shipdate: i32,
    l_commitdate: i32,
}

impl Component for Lineitem {
    fn name() -> &'static str {
        "Lineitem"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::UInt8, false),
            Field::new("l_linestatus", DataType::UInt8, false),
            Field::new("l_shipdate", DataType::Int32, false),
            Field::new("l_commitdate", DataType::Int32, false),
        ]))
    }
}

// Revenue placeholder component
#[derive(Serialize, Deserialize, Clone)]
struct Revenue {
    piece: f64,
}

impl Component for Revenue {
    fn name() -> &'static str {
        "Revenue"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "piece",
            DataType::Float64,
            false,
        )]))
    }
}

// Resources
use arrow_array::BooleanArray;
struct FilterMask(Arc<BooleanArray>);
struct Q6Revenue(f64);

// ---------------------------------------------------------------------------
// Synthetic data generator (same LCG as tpch_q6.rs)
// ---------------------------------------------------------------------------

fn generate_lineitem_batch(n: usize, seed: u64) -> RecordBatch {
    use std::num::Wrapping;
    let mut state = Wrapping(seed);
    let lcg = |s: &mut Wrapping<u64>| -> u64 {
        *s = *s * Wrapping(6364136223846793005) + Wrapping(1442695040888963407);
        s.0
    };

    let mut l_orderkey = Vec::with_capacity(n);
    let mut l_partkey = Vec::with_capacity(n);
    let mut l_suppkey = Vec::with_capacity(n);
    let mut l_linenumber = Vec::with_capacity(n);
    let mut l_quantity = Vec::with_capacity(n);
    let mut l_extendedprice = Vec::with_capacity(n);
    let mut l_discount = Vec::with_capacity(n);
    let mut l_tax = Vec::with_capacity(n);
    let mut l_returnflag = Vec::with_capacity(n);
    let mut l_linestatus = Vec::with_capacity(n);
    let mut l_shipdate = Vec::with_capacity(n);
    let mut l_commitdate = Vec::with_capacity(n);

    for i in 0..n {
        let r0 = lcg(&mut state);
        let r1 = lcg(&mut state);
        let r2 = lcg(&mut state);
        let r3 = lcg(&mut state);
        let r4 = lcg(&mut state);
        let r5 = lcg(&mut state);
        let r6 = lcg(&mut state);
        let r7 = lcg(&mut state);

        l_orderkey.push(i as i64 / 6 + 1);
        l_partkey.push((r0 % 200_000) as i64 + 1);
        l_suppkey.push((r1 % 10_000) as i64 + 1);
        l_linenumber.push((i % 7 + 1) as i32);
        let qty = 1.0 + (r2 % 50) as f64;
        l_quantity.push(qty);
        let unit_price = 0.90 + (r3 % 10499001) as f64 / 100.0;
        l_extendedprice.push(qty * unit_price);
        l_discount.push((r4 % 11) as f64 / 100.0);
        l_tax.push((r5 % 9) as f64 / 100.0);
        l_returnflag.push((r6 % 3) as u8);
        l_linestatus.push((r6 % 2) as u8);
        let sd_base = 8036i32;
        let sd_range = 2525u64;
        l_shipdate.push(sd_base + (r7 % sd_range) as i32);
        l_commitdate.push(sd_base + (lcg(&mut state) % sd_range) as i32 + 30);
    }

    RecordBatch::try_new(
        Lineitem::schema(),
        vec![
            Arc::new(Int64Array::from(l_orderkey)),
            Arc::new(Int64Array::from(l_partkey)),
            Arc::new(Int64Array::from(l_suppkey)),
            Arc::new(Int32Array::from(l_linenumber)),
            Arc::new(Float64Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Float64Array::from(l_discount)),
            Arc::new(Float64Array::from(l_tax)),
            Arc::new(UInt8Array::from(l_returnflag)),
            Arc::new(UInt8Array::from(l_linestatus)),
            Arc::new(Int32Array::from(l_shipdate)),
            Arc::new(Int32Array::from(l_commitdate)),
        ],
    )
    .expect("generate_lineitem_batch failed")
}

// ---------------------------------------------------------------------------
// PCS Scheduler stages
// ---------------------------------------------------------------------------

struct PcsFilterStage;

#[async_trait]
impl ParallelSystem for PcsFilterStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("pcs_q6_filter")
            .read("Lineitem", "l_shipdate")
            .read("Lineitem", "l_discount")
            .read("Lineitem", "l_quantity")
            .write_resource::<FilterMask>()
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| PcsError::generic("Lineitem not found"))?;
        let sd_arr = batch
            .column_by_name("l_shipdate")
            .ok_or_else(|| PcsError::generic("l_shipdate not found"))?
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let disc_arr = batch
            .column_by_name("l_discount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let qty_arr = batch
            .column_by_name("l_quantity")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let n = sd_arr.len();
        let mask: Vec<bool> = (0..n)
            .map(|i| {
                let sd = sd_arr.value(i);
                let disc = disc_arr.value(i);
                (SHIPDATE_GE..SHIPDATE_LT).contains(&sd)
                    && (DISCOUNT_LO..=DISCOUNT_HI).contains(&disc)
                    && qty_arr.value(i) < QUANTITY_LT
            })
            .collect();
        let update = ResourceUpdate::new(FilterMask(Arc::new(BooleanArray::from(mask))));
        Ok(WriteSet::new().with_resource(update))
    }
}

struct PcsComputeStage;

#[async_trait]
impl ParallelSystem for PcsComputeStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("pcs_q6_compute")
            .read("Lineitem", "l_extendedprice")
            .read("Lineitem", "l_discount")
            .read_resource::<FilterMask>()
            .write("Revenue", "piece")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| PcsError::generic("Lineitem not found"))?;
        let price_arr = batch
            .column_by_name("l_extendedprice")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let disc_arr = batch
            .column_by_name("l_discount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let n = price_arr.len();
        let pieces: Vec<f64> = (0..n)
            .map(|i| price_arr.value(i) * disc_arr.value(i))
            .collect();

        Ok(WriteSet::new().put("Revenue", "piece", Arc::new(Float64Array::from(pieces))))
    }
}

struct PcsAggregateStage;

#[async_trait]
impl System for PcsAggregateStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("pcs_q6_aggregate")
            .read("Revenue", "piece")
            .read_resource::<FilterMask>()
            .write_resource::<Q6Revenue>()
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), PcsError> {
        let mask_arr = pipeline
            .get_resource::<FilterMask>()
            .ok_or_else(|| PcsError::generic("FilterMask not found"))?
            .0
            .clone();

        let rev_batch = pipeline
            .batch_for("Revenue")
            .ok_or_else(|| PcsError::generic("Revenue not found"))?;
        let piece_arr = rev_batch
            .column_by_name("piece")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let revenue: f64 = (0..piece_arr.len())
            .filter(|&i| mask_arr.value(i))
            .map(|i| piece_arr.value(i))
            .sum();

        pipeline.insert_resource(Q6Revenue(revenue));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PCS pipeline builder
// ---------------------------------------------------------------------------

fn build_pcs_pipeline(batch: &RecordBatch) -> Dataset {
    let mut pipeline = Dataset::new();
    pipeline.register_component::<Lineitem>().unwrap();
    pipeline.register_component::<Revenue>().unwrap();
    pipeline
        .append_record_batch("Lineitem", batch.clone())
        .unwrap();
    let n = batch.num_rows();
    let rev = RecordBatch::try_new(
        Revenue::schema(),
        vec![Arc::new(Float64Array::from(vec![0.0f64; n]))],
    )
    .unwrap();
    pipeline.append_record_batch("Revenue", rev).unwrap();
    pipeline
}

// ---------------------------------------------------------------------------
// DataFusion Q6 runner
// ---------------------------------------------------------------------------

async fn datafusion_q6(batch: RecordBatch) -> f64 {
    let ctx = SessionContext::new();

    // Register the RecordBatch as a MemTable.
    let schema = batch.schema();
    let provider = MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable creation failed");
    ctx.register_table("lineitem", Arc::new(provider))
        .expect("register_table failed");

    // Q6 SQL — using integer dates matching our synthetic data representation.
    let sql = format!(
        "SELECT SUM(l_extendedprice * l_discount) AS revenue \
         FROM lineitem \
         WHERE l_shipdate >= {SHIPDATE_GE} \
           AND l_shipdate < {SHIPDATE_LT} \
           AND l_discount >= {DISCOUNT_LO} \
           AND l_discount <= {DISCOUNT_HI} \
           AND l_quantity < {QUANTITY_LT}"
    );

    let df = ctx.sql(&sql).await.expect("sql parse failed");
    let results = df.collect().await.expect("datafusion execute failed");

    // Extract the single SUM result
    if results.is_empty() || results[0].num_rows() == 0 {
        return 0.0;
    }
    let col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("DataFusion revenue column is not Float64");
    col.value(0)
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn bench_vs_datafusion_q6(c: &mut Criterion) {
    const N: usize = 1_000_000;
    const SEED: u64 = 42;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let batch = generate_lineitem_batch(N, SEED);

    println!("\n[vs_datafusion_q6] {} rows, {} CPUs", N, num_cpus::get());

    // Correctness: PCS and DataFusion must agree on the revenue total.
    {
        let pcs_revenue = {
            let mut wl = Pipeline::new("q6_check");
            wl.data = build_pcs_pipeline(&batch);
            wl.add_parallel_system(PcsFilterStage);
            wl.add_parallel_system(PcsComputeStage);
            wl.add_system(PcsAggregateStage);
            rt.block_on(wl.run()).unwrap();
            wl.data
                .get_resource::<Q6Revenue>()
                .map(|r| r.0)
                .unwrap_or(0.0)
        };

        let df_revenue = rt.block_on(datafusion_q6(batch.clone()));

        let eps = pcs_revenue.abs() * 1e-9 + 1.0;
        assert!(
            (pcs_revenue - df_revenue).abs() < eps,
            "PCS vs DataFusion Q6 revenue mismatch: pcs={pcs_revenue:.4} df={df_revenue:.4}"
        );
        println!("[vs_datafusion_q6] correctness check passed — revenue={pcs_revenue:.4}");
    }

    let mut group = c.benchmark_group("vs_datafusion_q6");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));

    // PCS Pipeline
    group.bench_function("pcs_pipeline", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("q6");
            wl.data = build_pcs_pipeline(std::hint::black_box(&batch));
            wl.add_parallel_system(PcsFilterStage);
            wl.add_parallel_system(PcsComputeStage);
            wl.add_system(PcsAggregateStage);
            rt.block_on(wl.run()).unwrap();
            let r = wl
                .data
                .get_resource::<Q6Revenue>()
                .map(|r| r.0)
                .unwrap_or(0.0);
            std::hint::black_box(r)
        })
    });

    // DataFusion SQL
    group.bench_function("datafusion_sql", |b| {
        b.iter(|| {
            let r = rt.block_on(datafusion_q6(std::hint::black_box(batch.clone())));
            std::hint::black_box(r)
        })
    });

    group.finish();

    println!("[vs_datafusion_q6] NOTE: PCS is expected to LOSE to DataFusion on SQL Q6 (2-10x).");
    println!("  DataFusion has a mature vectorized OLAP executor.");
    println!("  PCS's strength is imperative pipeline workloads + distributed processing.");
    println!("  This benchmark exists for honest README documentation, not to claim a win.");
}

criterion_group!(benches, bench_vs_datafusion_q6);
criterion_main!(benches);
