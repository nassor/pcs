// TPC-H Query 1 benchmark
//
// Run with native CPU tuning for representative numbers:
//
//   RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" \
//     cargo bench --bench tpch_q1 -- --sample-size 10
//
// TPC-H Q1:
//   SELECT l_returnflag, l_linestatus,
//          SUM(l_quantity), SUM(l_extendedprice),
//          SUM(l_extendedprice * (1 - l_discount)),
//          SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
//          AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
//          COUNT(*)
//   FROM lineitem
//   WHERE l_shipdate <= '1998-12-01' - INTERVAL '90' DAY
//   GROUP BY l_returnflag, l_linestatus
//   ORDER BY l_returnflag, l_linestatus;
//
// Synthetic data: ~1 000 000 rows, seed=42.
// Threshold date: 1998-09-02 (days since epoch 1970-01-01 = 10471).
//
// Architecture:
//   Stage 1 (ParallelSystem): FilterStage — computes a boolean mask for
//     l_shipdate <= threshold and stores it as a BooleanArray resource.
//   Stage 2 (ParallelSystem): ComputeStage — reads the mask plus price/
//     discount/tax columns, writes disc_price and charge columns.
//   Stage 3 (System): AggregateStage — groups on (returnflag, linestatus)
//     and stores a Vec<Q1GroupResult> resource.
//
// Scalar baseline: single-pass Vec<LineItem> loop with the same filter +
//   aggregation logic — used as a lower bound for the Arrow pipeline.

use std::collections::HashMap;
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
// Lineitem schema (TPC-H subset used by Q1)
// ---------------------------------------------------------------------------
// Days since Unix epoch for 1998-09-02 (= 1998-12-01 minus 90 days)
const SHIPDATE_THRESHOLD: i32 = 10471;

// Distinct returnflag values (A=0, N=1, R=2)
const RETURNFLAG_VALUES: &[u8] = &[0, 1, 2];
// Distinct linestatus values (F=0, O=1)
const LINESTATUS_VALUES: &[u8] = &[0, 1];

// Lineitem component — only the Q1-relevant fields
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

// Derived columns component — disc_price and charge
#[derive(Serialize, Deserialize, Clone)]
struct LineitemDerived {
    disc_price: f64,
    charge: f64,
}

impl Component for LineitemDerived {
    fn name() -> &'static str {
        "LineitemDerived"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("disc_price", DataType::Float64, false),
            Field::new("charge", DataType::Float64, false),
        ]))
    }
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/// Boolean mask from the filter stage: row i passes iff mask[i] = true.
struct FilterMask(Arc<BooleanArray>);

/// Aggregation result record.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Q1GroupResult {
    returnflag: u8,
    linestatus: u8,
    sum_qty: f64,
    sum_base_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    avg_qty: f64,
    avg_price: f64,
    avg_disc: f64,
    count_order: u64,
}

/// The final aggregation resource: sorted by (returnflag, linestatus).
struct Q1Result(Vec<Q1GroupResult>);

// ---------------------------------------------------------------------------
// Data generator
// ---------------------------------------------------------------------------

/// Generate a RecordBatch of `n` rows with seed-based deterministic data.
fn generate_lineitem_batch(n: usize, seed: u64) -> RecordBatch {
    use std::num::Wrapping;

    let mut state = Wrapping(seed);
    let lcg_next = |s: &mut Wrapping<u64>| -> u64 {
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
        let r0 = lcg_next(&mut state);
        let r1 = lcg_next(&mut state);
        let r2 = lcg_next(&mut state);
        let r3 = lcg_next(&mut state);

        l_orderkey.push(i as i64 / 6 + 1);
        l_partkey.push((r0 % 200_000) as i64 + 1);
        l_suppkey.push((r1 % 10_000) as i64 + 1);
        l_linenumber.push((i % 7 + 1) as i32);
        l_quantity.push(1.0 + (r2 % 50) as f64);
        // extendedprice: quantity * unit price; unit price in [0.90, 104999.00]
        let unit_price = 0.90 + (r3 % 10499001) as f64 / 100.0;
        let qty = l_quantity[i];
        l_extendedprice.push(qty * unit_price);
        // discount: 0.00..0.10 in steps of 0.01
        let disc_r = lcg_next(&mut state);
        l_discount.push((disc_r % 11) as f64 / 100.0);
        // tax: 0.00..0.08 in steps of 0.01
        let tax_r = lcg_next(&mut state);
        l_tax.push((tax_r % 9) as f64 / 100.0);
        // returnflag: A/N/R  (roughly: F rows get A or R, O rows get N)
        let rf_r = lcg_next(&mut state);
        l_returnflag.push(RETURNFLAG_VALUES[(rf_r % 3) as usize]);
        // linestatus: F or O
        let ls_r = lcg_next(&mut state);
        l_linestatus.push(LINESTATUS_VALUES[(ls_r % 2) as usize]);
        // shipdate: 1992-01-02 (8036) .. 1998-12-01 (10471+90=10561)
        // ~80% of rows should pass the filter (shipdate <= 10471)
        let sd_r = lcg_next(&mut state);
        let shipdate_base = 8036i32;
        let shipdate_range = 2560i32; // covers ~7 years; 80% is <= threshold
        l_shipdate.push(shipdate_base + (sd_r % shipdate_range as u64) as i32);
        let cd_r = lcg_next(&mut state);
        l_commitdate.push(shipdate_base + (cd_r % 2560) as i32 + 30);
    }

    let schema = Lineitem::schema();
    RecordBatch::try_new(
        schema,
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
    .expect("generate_lineitem_batch: RecordBatch construction failed")
}

// ---------------------------------------------------------------------------
// Scalar baseline structs
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LineItemRow {
    l_quantity: f64,
    l_extendedprice: f64,
    l_discount: f64,
    l_tax: f64,
    l_returnflag: u8,
    l_linestatus: u8,
    l_shipdate: i32,
}

fn extract_scalar_rows(batch: &RecordBatch) -> Vec<LineItemRow> {
    let qty = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let price = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let disc = batch
        .column(6)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let tax = batch
        .column(7)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let rf = batch
        .column(8)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let ls = batch
        .column(9)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let sd = batch
        .column(10)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    (0..batch.num_rows())
        .map(|i| LineItemRow {
            l_quantity: qty.value(i),
            l_extendedprice: price.value(i),
            l_discount: disc.value(i),
            l_tax: tax.value(i),
            l_returnflag: rf.value(i),
            l_linestatus: ls.value(i),
            l_shipdate: sd.value(i),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Scalar Q1 baseline
// ---------------------------------------------------------------------------

fn scalar_q1(rows: &[LineItemRow]) -> Vec<Q1GroupResult> {
    #[derive(Default, Clone)]
    struct Acc {
        sum_qty: f64,
        sum_price: f64,
        sum_disc_price: f64,
        sum_charge: f64,
        count: u64,
        sum_disc: f64, // for avg_disc
    }

    let mut groups: HashMap<(u8, u8), Acc> = HashMap::new();

    for row in rows {
        if row.l_shipdate > SHIPDATE_THRESHOLD {
            continue;
        }
        let key = (row.l_returnflag, row.l_linestatus);
        let acc = groups.entry(key).or_default();
        let disc_price = row.l_extendedprice * (1.0 - row.l_discount);
        let charge = disc_price * (1.0 + row.l_tax);
        acc.sum_qty += row.l_quantity;
        acc.sum_price += row.l_extendedprice;
        acc.sum_disc_price += disc_price;
        acc.sum_charge += charge;
        acc.sum_disc += row.l_discount;
        acc.count += 1;
    }

    let mut result: Vec<Q1GroupResult> = groups
        .into_iter()
        .map(|((rf, ls), acc)| {
            let count = acc.count as f64;
            Q1GroupResult {
                returnflag: rf,
                linestatus: ls,
                sum_qty: acc.sum_qty,
                sum_base_price: acc.sum_price,
                sum_disc_price: acc.sum_disc_price,
                sum_charge: acc.sum_charge,
                avg_qty: acc.sum_qty / count,
                avg_price: acc.sum_price / count,
                avg_disc: acc.sum_disc / count,
                count_order: acc.count,
            }
        })
        .collect();

    result.sort_by_key(|r| (r.returnflag, r.linestatus));
    result
}

// ---------------------------------------------------------------------------
// Scheduler stages
// ---------------------------------------------------------------------------

// Stage 1: FilterStage — parallel, computes boolean mask resource.
struct FilterStage;

#[async_trait]
impl ParallelSystem for FilterStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q1_filter")
            .read("Lineitem", "l_shipdate")
            .write_resource::<FilterMask>()
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| PcsError::generic("Lineitem not found"))?;
        let sd_col = batch
            .column_by_name("l_shipdate")
            .ok_or_else(|| PcsError::generic("l_shipdate not found"))?;
        let sd_arr = sd_col
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| PcsError::generic("l_shipdate wrong type"))?;

        let mask: Vec<bool> = sd_arr
            .values()
            .iter()
            .map(|&v| v <= SHIPDATE_THRESHOLD)
            .collect();
        let bool_array = Arc::new(BooleanArray::from(mask));
        let update = ResourceUpdate::new(FilterMask(bool_array));

        Ok(WriteSet::new().with_resource(update))
    }

    fn run_slice(
        &self,
        pipeline: &Dataset,
        rows: std::ops::Range<u32>,
    ) -> Option<Result<SliceWriteSet, PcsError>> {
        let batch = pipeline.batch_for("Lineitem")?;
        let sd_col = batch.column_by_name("l_shipdate")?;
        let sd_arr = sd_col.as_any().downcast_ref::<Int32Array>()?;
        let start = rows.start as usize;
        let len = (rows.end - rows.start) as usize;
        let slice = sd_arr.slice(start, len);
        let slice_arr = slice.as_any().downcast_ref::<Int32Array>().unwrap();
        let mask: Vec<bool> = slice_arr
            .values()
            .iter()
            .map(|&v| v <= SHIPDATE_THRESHOLD)
            .collect();
        let bool_array: Arc<dyn arrow_array::Array> = Arc::new(BooleanArray::from(mask));
        Some(Ok(SliceWriteSet::new(rows).put(
            "_filter_mask",
            "mask",
            bool_array,
        )))
    }

    fn merge_slices(&self, slices: Vec<SliceWriteSet>) -> Result<WriteSet, PcsError> {
        use arrow_select::concat::concat;
        // Concatenate mask slices into one BooleanArray resource.
        let arrays: Vec<&dyn arrow_array::Array> = slices
            .iter()
            .filter_map(|s| s.fields.get(&("_filter_mask", "mask")))
            .map(|a| a.as_ref())
            .collect();
        if arrays.is_empty() {
            let empty: Arc<BooleanArray> = Arc::new(BooleanArray::from(vec![false; 0]));
            return Ok(WriteSet::new().with_resource(ResourceUpdate::new(FilterMask(empty))));
        }
        let merged = concat(&arrays)
            .map_err(|e| PcsError::generic(format!("FilterStage merge error: {e}")))?;
        let bool_arr = merged
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| PcsError::generic("FilterStage: merged array is not BooleanArray"))?;
        let update = ResourceUpdate::new(FilterMask(Arc::new(bool_arr.clone())));
        Ok(WriteSet::new().with_resource(update))
    }
}

// Stage 2: ComputeStage — parallel, writes disc_price and charge columns.
struct ComputeStage;

#[async_trait]
impl ParallelSystem for ComputeStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q1_compute")
            .read("Lineitem", "l_extendedprice")
            .read("Lineitem", "l_discount")
            .read("Lineitem", "l_tax")
            .read_resource::<FilterMask>()
            .write("LineitemDerived", "disc_price")
            .write("LineitemDerived", "charge")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| PcsError::generic("Lineitem not found"))?;

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
        let tax_arr = batch
            .column_by_name("l_tax")
            .ok_or_else(|| PcsError::generic("l_tax not found"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PcsError::generic("l_tax wrong type"))?;

        let n = price_arr.len();
        let mut disc_price = Vec::with_capacity(n);
        let mut charge = Vec::with_capacity(n);

        for i in 0..n {
            let dp = price_arr.value(i) * (1.0 - disc_arr.value(i));
            let ch = dp * (1.0 + tax_arr.value(i));
            disc_price.push(dp);
            charge.push(ch);
        }

        Ok(WriteSet::new()
            .put(
                "LineitemDerived",
                "disc_price",
                Arc::new(Float64Array::from(disc_price)),
            )
            .put(
                "LineitemDerived",
                "charge",
                Arc::new(Float64Array::from(charge)),
            ))
    }
}

// Stage 3: AggregateStage — sequential, groups on (returnflag, linestatus).
struct AggregateStage;

#[async_trait]
impl System for AggregateStage {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("q1_aggregate")
            .read("Lineitem", "l_quantity")
            .read("Lineitem", "l_extendedprice")
            .read("Lineitem", "l_discount")
            .read("Lineitem", "l_returnflag")
            .read("Lineitem", "l_linestatus")
            .read("LineitemDerived", "disc_price")
            .read("LineitemDerived", "charge")
            .read_resource::<FilterMask>()
            .write_resource::<Q1Result>()
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), PcsError> {
        let mask = pipeline
            .get_resource::<FilterMask>()
            .ok_or_else(|| PcsError::generic("FilterMask resource not found"))?;
        let mask_arr = mask.0.clone();

        let li_batch = pipeline
            .batch_for("Lineitem")
            .ok_or_else(|| PcsError::generic("Lineitem not found"))?;
        let ld_batch = pipeline
            .batch_for("LineitemDerived")
            .ok_or_else(|| PcsError::generic("LineitemDerived not found"))?;

        let qty_arr = li_batch
            .column_by_name("l_quantity")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let price_arr = li_batch
            .column_by_name("l_extendedprice")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let disc_arr = li_batch
            .column_by_name("l_discount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let rf_arr = li_batch
            .column_by_name("l_returnflag")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let ls_arr = li_batch
            .column_by_name("l_linestatus")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let dp_arr = ld_batch
            .column_by_name("disc_price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ch_arr = ld_batch
            .column_by_name("charge")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        #[derive(Default, Clone)]
        struct Acc {
            sum_qty: f64,
            sum_price: f64,
            sum_disc_price: f64,
            sum_charge: f64,
            sum_disc: f64,
            count: u64,
        }

        let mut groups: HashMap<(u8, u8), Acc> = HashMap::new();
        let n = qty_arr.len();
        for i in 0..n {
            if !mask_arr.value(i) {
                continue;
            }
            let key = (rf_arr.value(i), ls_arr.value(i));
            let acc = groups.entry(key).or_default();
            acc.sum_qty += qty_arr.value(i);
            acc.sum_price += price_arr.value(i);
            acc.sum_disc_price += dp_arr.value(i);
            acc.sum_charge += ch_arr.value(i);
            acc.sum_disc += disc_arr.value(i);
            acc.count += 1;
        }

        let mut result: Vec<Q1GroupResult> = groups
            .into_iter()
            .map(|((rf, ls), acc)| {
                let count = acc.count as f64;
                Q1GroupResult {
                    returnflag: rf,
                    linestatus: ls,
                    sum_qty: acc.sum_qty,
                    sum_base_price: acc.sum_price,
                    sum_disc_price: acc.sum_disc_price,
                    sum_charge: acc.sum_charge,
                    avg_qty: acc.sum_qty / count,
                    avg_price: acc.sum_price / count,
                    avg_disc: acc.sum_disc / count,
                    count_order: acc.count,
                }
            })
            .collect();

        result.sort_by_key(|r| (r.returnflag, r.linestatus));
        pipeline.insert_resource(Q1Result(result));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Pipeline builder
// ---------------------------------------------------------------------------

fn build_pipeline(batch: &RecordBatch) -> Dataset {
    let mut pipeline = Dataset::new();
    pipeline.register_component::<Lineitem>().unwrap();
    pipeline.register_component::<LineitemDerived>().unwrap();

    // Append Lineitem raw batch
    pipeline
        .append_record_batch("Lineitem", batch.clone())
        .unwrap();

    // Append placeholder LineitemDerived (zeros, same row count)
    let n = batch.num_rows();
    let derived_batch = RecordBatch::try_new(
        LineitemDerived::schema(),
        vec![
            Arc::new(Float64Array::from(vec![0.0f64; n])),
            Arc::new(Float64Array::from(vec![0.0f64; n])),
        ],
    )
    .unwrap();
    pipeline
        .append_record_batch("LineitemDerived", derived_batch)
        .unwrap();

    pipeline
}

// ---------------------------------------------------------------------------
// Correctness check
// ---------------------------------------------------------------------------

fn assert_results_match(scalar: &[Q1GroupResult], arrow: &[Q1GroupResult]) {
    assert_eq!(
        scalar.len(),
        arrow.len(),
        "Q1 result group count mismatch: scalar={} arrow={}",
        scalar.len(),
        arrow.len()
    );
    for (s, a) in scalar.iter().zip(arrow.iter()) {
        assert_eq!(
            s.returnflag, a.returnflag,
            "returnflag mismatch: {} vs {}",
            s.returnflag, a.returnflag
        );
        assert_eq!(
            s.linestatus, a.linestatus,
            "linestatus mismatch: {} vs {}",
            s.linestatus, a.linestatus
        );
        assert_eq!(
            s.count_order, a.count_order,
            "count_order mismatch for group ({},{}): scalar={} arrow={}",
            s.returnflag, s.linestatus, s.count_order, a.count_order
        );
        let eps = s.sum_charge.abs() * 1e-9 + 1e-6;
        assert!(
            (s.sum_charge - a.sum_charge).abs() < eps,
            "sum_charge mismatch for group ({},{}): scalar={:.4} arrow={:.4}",
            s.returnflag,
            s.linestatus,
            s.sum_charge,
            a.sum_charge
        );
    }
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_q1(c: &mut Criterion) {
    const N: usize = 1_000_000;
    const SEED: u64 = 42;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let batch = generate_lineitem_batch(N, SEED);
    let scalar_rows = extract_scalar_rows(&batch);

    println!("\n[tpch_q1] {} rows, {} CPUs", N, num_cpus::get());

    // Correctness check before benchmarking
    {
        let scalar_result = scalar_q1(&scalar_rows);
        let mut wl = Pipeline::new("q1_check");
        wl.data = build_pipeline(&batch);
        wl.add_parallel_system(FilterStage);
        wl.add_parallel_system(ComputeStage);
        wl.add_system(AggregateStage);
        rt.block_on(wl.run()).unwrap();
        let arrow_result = wl.data.get_resource::<Q1Result>().unwrap();
        assert_results_match(&scalar_result, &arrow_result.0);
        println!(
            "[tpch_q1] correctness check passed ({} groups)",
            scalar_result.len()
        );
    }

    let mut group = c.benchmark_group("tpch_q1");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(15));

    group.bench_function("scalar_baseline", |b| {
        b.iter(|| {
            let result = scalar_q1(std::hint::black_box(&scalar_rows));
            std::hint::black_box(result)
        })
    });

    group.bench_function("pcs_pipeline", |b| {
        b.iter(|| {
            let mut wl = Pipeline::new("q1");
            wl.data = build_pipeline(std::hint::black_box(&batch));
            wl.add_parallel_system(FilterStage);
            wl.add_parallel_system(ComputeStage);
            wl.add_system(AggregateStage);
            rt.block_on(wl.run()).unwrap();
            let result = wl
                .data
                .get_resource::<Q1Result>()
                .map(|r| r.0.len())
                .unwrap_or(0);
            std::hint::black_box(result)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_q1);
criterion_main!(benches);
