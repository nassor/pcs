// TPC-H Query 6 benchmark
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
//     cargo bench --bench tpch_q6 -- --sample-size 10
//
// TPC-H Q6:
//   SELECT SUM(l_extendedprice * l_discount) AS revenue
//   FROM lineitem
//   WHERE l_shipdate >= '1994-01-01'
//     AND l_shipdate < '1995-01-01'
//     AND l_discount BETWEEN 0.05 AND 0.07
//     AND l_quantity < 24;
//
// Synthetic data: ~1 000 000 rows, seed=42.
// Date constants (days since Unix epoch 1970-01-01):
//   1994-01-01 = 8766
//   1995-01-01 = 9131
//
// Four benchmarks:
//   1. narrow_scalar — scalar loop over 12-column RecordBatch
//   2. narrow_pcs   — Scheduler on 12-column lineitem schema
//   3. wide_scalar   — scalar loop touching all 30 columns per row
//   4. wide_pcs     — Scheduler on 30-column schema (touches only 4)
//
// Expected: wide_pcs >= 2x faster than wide_scalar (column projection wins).
//
// Architecture of the Scheduler version:
//   FilterStage (parallel): composite mask for all three predicates.
//   ComputeStage (parallel): revenue_piece = extendedprice * discount per row.
//   AggregateStage (sequential): sum filtered revenue_piece into a resource.

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use pcs_core::PcsError;
use pcs_core::component::Component;
use pcs_core::pipeline::{Dataset, Pipeline};
use pcs_core::system::{
    ParallelSystem, ResourceUpdate, SliceWriteSet, System, SystemMeta, WriteSet,
};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Date constants (days since 1970-01-01)
// ---------------------------------------------------------------------------
const SHIPDATE_GE: i32 = 8766; // 1994-01-01
const SHIPDATE_LT: i32 = 9131; // 1995-01-01
const DISCOUNT_LO: f64 = 0.05;
const DISCOUNT_HI: f64 = 0.07;
const QUANTITY_LT: f64 = 24.0;

// ---------------------------------------------------------------------------
// Narrow lineitem component (12 columns)
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

// ---------------------------------------------------------------------------
// Wide lineitem schema (30 columns: 12 real + 18 junk)
// ---------------------------------------------------------------------------

fn wide_lineitem_schema() -> Arc<Schema> {
    let mut fields = vec![
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
    ];
    // 18 junk columns: 6x (i64 + f64 + bool)
    for i in 0..6 {
        fields.push(Field::new(format!("junk_i64_{i}"), DataType::Int64, false));
        fields.push(Field::new(
            format!("junk_f64_{i}"),
            DataType::Float64,
            false,
        ));
        fields.push(Field::new(
            format!("junk_bool_{i}"),
            DataType::Boolean,
            false,
        ));
    }
    Arc::new(Schema::new(fields))
}

// Revenue output component
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

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

struct FilterMask(Arc<BooleanArray>);
struct Q6Revenue(f64);

// ---------------------------------------------------------------------------
// Data generator
// ---------------------------------------------------------------------------

fn generate_narrow_batch(n: usize, seed: u64) -> RecordBatch {
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

        // shipdate: 1992-01-02 (8036) .. 1998-12-01 (~10561); 2525 day range
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
    .expect("generate_narrow_batch failed")
}

/// Build the wide batch by taking the narrow 12 columns + 18 junk columns.
fn generate_wide_batch(narrow: &RecordBatch) -> RecordBatch {
    let n = narrow.num_rows();
    let schema = wide_lineitem_schema();

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = narrow.columns().to_vec();
    for i in 0u64..6 {
        columns.push(Arc::new(Int64Array::from(vec![i as i64; n])));
        columns.push(Arc::new(Float64Array::from(vec![i as f64 * 0.1; n])));
        let bools: Vec<bool> = (0..n).map(|j| (j + i as usize).is_multiple_of(2)).collect();
        columns.push(Arc::new(BooleanArray::from(bools)));
    }

    RecordBatch::try_new(schema, columns).expect("generate_wide_batch failed")
}

// ---------------------------------------------------------------------------
// Scalar baselines
// ---------------------------------------------------------------------------

fn scalar_q6_narrow(batch: &RecordBatch) -> f64 {
    let qty = batch
        .column_by_name("l_quantity")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let price = batch
        .column_by_name("l_extendedprice")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let disc = batch
        .column_by_name("l_discount")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let sd = batch
        .column_by_name("l_shipdate")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let mut revenue = 0.0f64;
    for i in 0..batch.num_rows() {
        let shipdate = sd.value(i);
        let discount = disc.value(i);
        let quantity = qty.value(i);
        if (SHIPDATE_GE..SHIPDATE_LT).contains(&shipdate)
            && (DISCOUNT_LO..=DISCOUNT_HI).contains(&discount)
            && quantity < QUANTITY_LT
        {
            revenue += price.value(i) * discount;
        }
    }
    revenue
}

/// Wide scalar: forces touching all 30 columns on each row iteration to simulate
/// a row-oriented data layout (30-field struct). The 18 junk column reads are
/// made visible to the optimizer via black_box so they cannot be elided.
fn scalar_q6_wide(wide_batch: &RecordBatch) -> f64 {
    let qty = wide_batch
        .column_by_name("l_quantity")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let price = wide_batch
        .column_by_name("l_extendedprice")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let disc = wide_batch
        .column_by_name("l_discount")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let sd = wide_batch
        .column_by_name("l_shipdate")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Collect junk column references — these simulate the memory pressure of
    // reading a row-oriented struct with 30 fields.
    let junk_i64s: Vec<&Int64Array> = (0..6)
        .map(|i| {
            wide_batch
                .column_by_name(&format!("junk_i64_{i}"))
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        })
        .collect();
    let junk_f64s: Vec<&Float64Array> = (0..6)
        .map(|i| {
            wide_batch
                .column_by_name(&format!("junk_f64_{i}"))
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
        })
        .collect();
    let junk_bools: Vec<&BooleanArray> = (0..6)
        .map(|i| {
            wide_batch
                .column_by_name(&format!("junk_bool_{i}"))
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
        })
        .collect();

    let mut revenue = 0.0f64;
    let mut junk_acc = 0i64;
    for i in 0..wide_batch.num_rows() {
        // Touch junk columns to simulate row-scan memory pressure
        for j in &junk_i64s {
            junk_acc = junk_acc.wrapping_add(j.value(i));
        }
        for j in &junk_f64s {
            let _ = std::hint::black_box(j.value(i));
        }
        for j in &junk_bools {
            let _ = std::hint::black_box(j.value(i));
        }

        let shipdate = sd.value(i);
        let discount = disc.value(i);
        let quantity = qty.value(i);
        if (SHIPDATE_GE..SHIPDATE_LT).contains(&shipdate)
            && (DISCOUNT_LO..=DISCOUNT_HI).contains(&discount)
            && quantity < QUANTITY_LT
        {
            revenue += price.value(i) * discount;
        }
    }
    let _ = std::hint::black_box(junk_acc);
    revenue
}

// ---------------------------------------------------------------------------
// Scheduler stages (parameterised on component name)
// ---------------------------------------------------------------------------

struct FilterStage {
    component: &'static str,
}

#[async_trait]
impl ParallelSystem for FilterStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q6_filter")
            .read(self.component, "l_shipdate")
            .read(self.component, "l_discount")
            .read(self.component, "l_quantity")
            .write_resource::<FilterMask>()
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let batch = pipeline
            .batch_for(self.component)
            .ok_or_else(|| PcsError::generic("lineitem component not found"))?;
        let mask = compute_filter_mask(batch)?;
        let update = ResourceUpdate::new(FilterMask(Arc::new(mask)));
        Ok(WriteSet::new().with_resource(update))
    }
}

fn compute_filter_mask(batch: &RecordBatch) -> Result<BooleanArray, PcsError> {
    let sd_arr = batch
        .column_by_name("l_shipdate")
        .ok_or_else(|| PcsError::generic("l_shipdate not found"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| PcsError::generic("l_shipdate wrong type"))?;
    let disc_arr = batch
        .column_by_name("l_discount")
        .ok_or_else(|| PcsError::generic("l_discount not found"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| PcsError::generic("l_discount wrong type"))?;
    let qty_arr = batch
        .column_by_name("l_quantity")
        .ok_or_else(|| PcsError::generic("l_quantity not found"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| PcsError::generic("l_quantity wrong type"))?;

    let n = sd_arr.len();
    let mut mask = Vec::with_capacity(n);
    for i in 0..n {
        let sd = sd_arr.value(i);
        let disc = disc_arr.value(i);
        mask.push(
            (SHIPDATE_GE..SHIPDATE_LT).contains(&sd)
                && (DISCOUNT_LO..=DISCOUNT_HI).contains(&disc)
                && qty_arr.value(i) < QUANTITY_LT,
        );
    }
    Ok(BooleanArray::from(mask))
}

struct ComputeStage {
    component: &'static str,
}

#[async_trait]
impl ParallelSystem for ComputeStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q6_compute")
            .read(self.component, "l_extendedprice")
            .read(self.component, "l_discount")
            .read_resource::<FilterMask>()
            .write("Revenue", "piece")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let batch = pipeline
            .batch_for(self.component)
            .ok_or_else(|| PcsError::generic("lineitem component not found"))?;
        let price_arr = batch
            .column_by_name("l_extendedprice")
            .ok_or_else(|| PcsError::generic("l_extendedprice not found"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PcsError::generic("l_extendedprice wrong type"))?;
        let disc_arr = batch
            .column_by_name("l_discount")
            .ok_or_else(|| PcsError::generic("l_discount not found"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PcsError::generic("l_discount wrong type"))?;

        let n = price_arr.len();
        let pieces: Vec<f64> = (0..n)
            .map(|i| price_arr.value(i) * disc_arr.value(i))
            .collect();

        Ok(WriteSet::new().put("Revenue", "piece", Arc::new(Float64Array::from(pieces))))
    }

    fn run_slice(
        &self,
        pipeline: &Dataset,
        rows: std::ops::Range<u32>,
    ) -> Option<Result<SliceWriteSet, PcsError>> {
        let batch = pipeline.batch_for(self.component)?;
        let price_arr = batch
            .column_by_name("l_extendedprice")?
            .as_any()
            .downcast_ref::<Float64Array>()?;
        let disc_arr = batch
            .column_by_name("l_discount")?
            .as_any()
            .downcast_ref::<Float64Array>()?;
        let start = rows.start as usize;
        let len = (rows.end - rows.start) as usize;
        let price_slice = price_arr.slice(start, len);
        let disc_slice = disc_arr.slice(start, len);
        let price_s = price_slice.as_any().downcast_ref::<Float64Array>().unwrap();
        let disc_s = disc_slice.as_any().downcast_ref::<Float64Array>().unwrap();
        let pieces: Vec<f64> = (0..len)
            .map(|i| price_s.value(i) * disc_s.value(i))
            .collect();
        let arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(pieces));
        Some(Ok(SliceWriteSet::new(rows).put("Revenue", "piece", arr)))
    }
}

struct AggregateStage;

#[async_trait]
impl System for AggregateStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q6_aggregate")
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
            .ok_or_else(|| PcsError::generic("piece not found"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PcsError::generic("piece wrong type"))?;

        let revenue: f64 = (0..piece_arr.len())
            .filter(|&i| mask_arr.value(i))
            .map(|i| piece_arr.value(i))
            .sum();

        pipeline.insert_resource(Q6Revenue(revenue));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Pipeline builders
// ---------------------------------------------------------------------------

fn build_narrow_pipeline(batch: &RecordBatch) -> Dataset {
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

fn build_wide_pipeline(wide_batch: &RecordBatch) -> Dataset {
    let mut pipeline = Dataset::new();
    pipeline.register_raw_component("WideLineitem", wide_lineitem_schema());
    pipeline.register_component::<Revenue>().unwrap();
    pipeline
        .append_record_batch("WideLineitem", wide_batch.clone())
        .unwrap();
    let n = wide_batch.num_rows();
    let rev = RecordBatch::try_new(
        Revenue::schema(),
        vec![Arc::new(Float64Array::from(vec![0.0f64; n]))],
    )
    .unwrap();
    pipeline.append_record_batch("Revenue", rev).unwrap();
    pipeline
}

// ---------------------------------------------------------------------------
// Correctness check
// ---------------------------------------------------------------------------

fn assert_revenue_close(label: &str, scalar: f64, arrow: f64) {
    let eps = scalar.abs() * 1e-9 + 1.0;
    assert!(
        (scalar - arrow).abs() < eps,
        "{label}: revenue mismatch — scalar={scalar:.4} arrow={arrow:.4}"
    );
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_q6(c: &mut Criterion) {
    const N: usize = 1_000_000;
    const SEED: u64 = 42;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let narrow_batch = generate_narrow_batch(N, SEED);
    let wide_batch = generate_wide_batch(&narrow_batch);

    println!(
        "\n[tpch_q6] {} rows, {} CPUs, narrow_cols={}, wide_cols={}",
        N,
        num_cpus::get(),
        narrow_batch.num_columns(),
        wide_batch.num_columns()
    );

    // Correctness checks
    {
        let scalar_narrow = scalar_q6_narrow(&narrow_batch);
        let scalar_wide = scalar_q6_wide(&wide_batch);

        let mut wl_n = Pipeline::new("q6_narrow_check");
        wl_n.data = build_narrow_pipeline(&narrow_batch);
        wl_n.add_parallel_system(FilterStage {
            component: "Lineitem",
        });
        wl_n.add_parallel_system(ComputeStage {
            component: "Lineitem",
        });
        wl_n.add_system(AggregateStage);
        rt.block_on(wl_n.run()).unwrap();
        let arrow_narrow = wl_n
            .data
            .get_resource::<Q6Revenue>()
            .map(|r| r.0)
            .unwrap_or(0.0);
        assert_revenue_close("narrow", scalar_narrow, arrow_narrow);

        let mut wl_w = Pipeline::new("q6_wide_check");
        wl_w.data = build_wide_pipeline(&wide_batch);
        wl_w.add_parallel_system(FilterStage {
            component: "WideLineitem",
        });
        wl_w.add_parallel_system(ComputeStage {
            component: "WideLineitem",
        });
        wl_w.add_system(AggregateStage);
        rt.block_on(wl_w.run()).unwrap();
        let arrow_wide = wl_w
            .data
            .get_resource::<Q6Revenue>()
            .map(|r| r.0)
            .unwrap_or(0.0);
        assert_revenue_close("wide", scalar_wide, arrow_wide);

        // Both scalar variants must agree (same data, same filter logic)
        assert_revenue_close("narrow==wide scalar", scalar_narrow, scalar_wide);

        println!("[tpch_q6] correctness check passed — revenue={scalar_narrow:.4}");
    }

    let mut group = c.benchmark_group("tpch_q6");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(15));

    group.bench_function("narrow_scalar", |b| {
        b.iter(|| {
            let r = scalar_q6_narrow(std::hint::black_box(&narrow_batch));
            std::hint::black_box(r)
        })
    });

    group.bench_function("narrow_pcs", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("q6_narrow");
            wl.data = build_narrow_pipeline(std::hint::black_box(&narrow_batch));
            wl.add_parallel_system(FilterStage {
                component: "Lineitem",
            });
            wl.add_parallel_system(ComputeStage {
                component: "Lineitem",
            });
            wl.add_system(AggregateStage);
            rt.block_on(wl.run()).unwrap();
            let r = wl
                .data
                .get_resource::<Q6Revenue>()
                .map(|r| r.0)
                .unwrap_or(0.0);
            std::hint::black_box(r)
        })
    });

    group.bench_function("wide_scalar", |b| {
        b.iter(|| {
            let r = scalar_q6_wide(std::hint::black_box(&wide_batch));
            std::hint::black_box(r)
        })
    });

    group.bench_function("wide_pcs", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("q6_wide");
            wl.data = build_wide_pipeline(std::hint::black_box(&wide_batch));
            wl.add_parallel_system(FilterStage {
                component: "WideLineitem",
            });
            wl.add_parallel_system(ComputeStage {
                component: "WideLineitem",
            });
            wl.add_system(AggregateStage);
            rt.block_on(wl.run()).unwrap();
            let r = wl
                .data
                .get_resource::<Q6Revenue>()
                .map(|r| r.0)
                .unwrap_or(0.0);
            std::hint::black_box(r)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_q6);
criterion_main!(benches);
