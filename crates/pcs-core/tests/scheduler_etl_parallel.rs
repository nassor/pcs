// Integration test ported from examples/scheduler_etl_parallel.rs
// Verifies ParallelSystem path with disjoint field writes in stage 1.

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use pcs_core::PcsError;
use pcs_core::component::Component;
use pcs_core::pipeline::{Dataset, Pipeline};
use pcs_core::system::{FieldRef, ParallelSystem, System, SystemMeta, WriteSet, system_fn};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Transaction {
    id: u64,
    amount: f64,
    currency: String,
    valid: bool,
    usd_amount: f64,
}

impl Component for Transaction {
    fn name() -> &'static str {
        "Transaction"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("valid", DataType::Boolean, false),
            Field::new("usd_amount", DataType::Float64, false),
        ]))
    }
}

impl Transaction {
    const AMOUNT: FieldRef<Transaction> = FieldRef::new("amount");
    const CURRENCY: FieldRef<Transaction> = FieldRef::new("currency");
    const VALID: FieldRef<Transaction> = FieldRef::new("valid");
    const USD_AMOUNT: FieldRef<Transaction> = FieldRef::new("usd_amount");
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

struct FxRates {
    eur: f64,
    gbp: f64,
    jpy: f64,
    cad: f64,
}

impl FxRates {
    fn rate_for(&self, currency: &str) -> f64 {
        match currency {
            "USD" => 1.0,
            "EUR" => self.eur,
            "GBP" => self.gbp,
            "JPY" => self.jpy,
            "CAD" => self.cad,
            _ => 1.0,
        }
    }
}

struct Report {
    total_rows: usize,
    valid_count: usize,
    rejected_count: usize,
    total_usd: f64,
}

// ---------------------------------------------------------------------------
// Systems
// ---------------------------------------------------------------------------

struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").write_component("Transaction")
    }
    async fn run(&self, pipeline: &mut Dataset) -> Result<(), PcsError> {
        let transactions = vec![
            Transaction {
                id: 1001,
                amount: 1500.00,
                currency: "USD".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1002,
                amount: 2300.50,
                currency: "EUR".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1003,
                amount: 750.00,
                currency: "GBP".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1004,
                amount: -50.00,
                currency: "USD".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1005,
                amount: 5000.00,
                currency: "JPY".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1006,
                amount: 320.75,
                currency: "EUR".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1007,
                amount: 1200.00,
                currency: "GBP".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1008,
                amount: 0.00,
                currency: "USD".into(),
                valid: false,
                usd_amount: 0.0,
            },
            Transaction {
                id: 1009,
                amount: 680.00,
                currency: "CAD".into(),
                valid: false,
                usd_amount: 0.0,
            },
        ];
        pipeline.append::<Transaction>(&transactions)?;
        Ok(())
    }
}

struct ValidateSystem;

#[async_trait]
impl ParallelSystem for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT)
            .writes(Transaction::VALID)
    }
    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let txns = pipeline.view::<Transaction>()?;
        let amount_col = txns.f64(Transaction::AMOUNT)?;
        let n = txns.len();
        let valid_flags: Vec<bool> = (0..n).map(|i| amount_col.value(i) > 0.0).collect();
        let new_valid: Arc<dyn arrow_array::Array> = Arc::new(BooleanArray::from(valid_flags));
        Ok(WriteSet::new().put("Transaction", "valid", new_valid))
    }
}

struct EnrichSystem;

#[async_trait]
impl ParallelSystem for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich")
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .writes(Transaction::USD_AMOUNT)
            .read_resource::<FxRates>()
    }
    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let rates = pipeline
            .get_resource::<FxRates>()
            .ok_or_else(|| PcsError::generic("FxRates resource not found"))?;
        let txns = pipeline.view::<Transaction>()?;
        let amount_col = txns.f64(Transaction::AMOUNT)?;
        let currency_col = txns.str(Transaction::CURRENCY)?;
        let n = txns.len();
        let usd_amounts: Vec<f64> = (0..n)
            .map(|i| {
                let rate = rates.rate_for(currency_col.value(i));
                amount_col.value(i) * rate
            })
            .collect();
        let new_usd: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(usd_amounts));
        Ok(WriteSet::new().put("Transaction", "usd_amount", new_usd))
    }
}

fn make_report_system() -> impl System {
    system_fn(
        SystemMeta::new("report")
            .read_component("Transaction")
            .write_resource::<Report>(),
        |data| {
            let (n, valid_count, total_usd) = {
                let txns = data.view::<Transaction>()?;
                let n = txns.len();
                let valid_col = txns.bool(Transaction::VALID)?;
                let usd_col = txns.f64(Transaction::USD_AMOUNT)?;
                let mut valid_count = 0usize;
                let mut total_usd = 0.0f64;
                for i in 0..n {
                    if valid_col.value(i) {
                        valid_count += 1;
                        total_usd += usd_col.value(i);
                    }
                }
                (n, valid_count, total_usd)
            };
            let rejected_count = n - valid_count;
            data.insert_resource(Report {
                total_rows: n,
                valid_count,
                rejected_count,
                total_usd,
            });
            Ok(())
        },
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scheduler_etl_parallel_pipeline_runs_and_produces_correct_report() {
    let mut pipeline = Pipeline::builder("etl-parallel")
        .with::<Transaction>()
        .with_resource(FxRates {
            eur: 1.08,
            gbp: 1.27,
            jpy: 0.0067,
            cad: 0.74,
        })
        .with_system(IngestSystem)
        .with_parallel_system(ValidateSystem)
        .with_parallel_system(EnrichSystem)
        .with_system(make_report_system())
        .build();

    pipeline.run().await.unwrap();

    let report = pipeline
        .data()
        .get_resource::<Report>()
        .expect("Report resource missing after pipeline run");

    assert_eq!(report.total_rows, 9);
    assert_eq!(report.valid_count, 7);
    assert_eq!(report.rejected_count, 2);
    assert!(report.total_usd > 0.0);
}

#[tokio::test]
async fn test_scheduler_etl_parallel_stage_layout_has_concurrent_stage() {
    let mut pipeline = Pipeline::builder("etl-parallel-stages")
        .with::<Transaction>()
        .with_resource(FxRates {
            eur: 1.08,
            gbp: 1.27,
            jpy: 0.0067,
            cad: 0.74,
        })
        .with_system(IngestSystem)
        .with_parallel_system(ValidateSystem)
        .with_parallel_system(EnrichSystem)
        .with_system(make_report_system())
        .build();

    pipeline.run().await.unwrap();

    let stages = pipeline.stages().unwrap_or_default();
    // Stage 0: ingest; Stage 1: validate+enrich (disjoint writes); Stage 2: report
    assert_eq!(stages.len(), 3);
    assert_eq!(stages[1].len(), 2, "validate and enrich share stage 1");
}
