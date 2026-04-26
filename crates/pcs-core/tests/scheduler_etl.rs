// Integration test ported from examples/scheduler_etl.rs
// Runs the full 4-system ETL pipeline and asserts on data correctness.

use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_array::Float64Array;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use pcs_core::PcsError;
use pcs_core::component::Component;
use pcs_core::pipeline::{Dataset, Pipeline};
use pcs_core::system::{FieldRef, System, SystemMeta, system_fn};

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
impl System for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT)
            .writes(Transaction::VALID)
    }
    async fn run(&self, pipeline: &mut Dataset) -> Result<(), PcsError> {
        let batch = pipeline
            .columns::<Transaction>()
            .ok_or_else(|| PcsError::generic("Transaction batch not found"))?
            .clone();
        let n;
        let valid_flags: Vec<bool>;
        {
            let txns = pipeline.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            n = txns.len();
            valid_flags = (0..n).map(|i| amount_col.value(i) > 0.0).collect();
        }
        let new_valid = Arc::new(BooleanArray::from(valid_flags));
        let schema = batch.schema();
        let valid_idx = schema.index_of("valid").unwrap();
        let new_columns: Vec<Arc<dyn arrow_array::Array>> = (0..schema.fields().len())
            .map(|i| {
                if i == valid_idx {
                    new_valid.clone() as Arc<dyn arrow_array::Array>
                } else {
                    batch.column(i).clone()
                }
            })
            .collect();
        let new_batch = arrow_array::RecordBatch::try_new(schema, new_columns)
            .map_err(|e| PcsError::generic(format!("RecordBatch rebuild error: {e}")))?;
        pipeline.replace_batch::<Transaction>(new_batch)?;
        Ok(())
    }
}

struct EnrichSystem;

#[async_trait]
impl System for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich")
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .writes(Transaction::USD_AMOUNT)
            .read_resource::<FxRates>()
    }
    async fn run(&self, pipeline: &mut Dataset) -> Result<(), PcsError> {
        let rates = pipeline
            .get_resource::<FxRates>()
            .ok_or_else(|| PcsError::generic("FxRates resource not found"))?;
        let batch = pipeline
            .columns::<Transaction>()
            .ok_or_else(|| PcsError::generic("Transaction batch not found"))?
            .clone();
        let n;
        let usd_amounts: Vec<f64>;
        {
            let txns = pipeline.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            let currency_col = txns.str(Transaction::CURRENCY)?;
            n = txns.len();
            usd_amounts = (0..n)
                .map(|i| {
                    let rate = rates.rate_for(currency_col.value(i));
                    amount_col.value(i) * rate
                })
                .collect();
        }
        let new_usd = Arc::new(Float64Array::from(usd_amounts));
        let schema = batch.schema();
        let usd_idx = schema.index_of("usd_amount").unwrap();
        let new_columns: Vec<Arc<dyn arrow_array::Array>> = (0..schema.fields().len())
            .map(|i| {
                if i == usd_idx {
                    new_usd.clone() as Arc<dyn arrow_array::Array>
                } else {
                    batch.column(i).clone()
                }
            })
            .collect();
        let new_batch = arrow_array::RecordBatch::try_new(schema, new_columns)
            .map_err(|e| PcsError::generic(format!("RecordBatch rebuild error: {e}")))?;
        pipeline.replace_batch::<Transaction>(new_batch)?;
        Ok(())
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
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scheduler_etl_pipeline_runs_and_produces_correct_report() {
    let mut pipeline = Pipeline::builder("etl")
        .with::<Transaction>()
        .with_resource(FxRates {
            eur: 1.08,
            gbp: 1.27,
            jpy: 0.0067,
            cad: 0.74,
        })
        .with_system(IngestSystem)
        .with_system(ValidateSystem)
        .with_system(EnrichSystem)
        .with_system(make_report_system())
        .build();

    pipeline.run().await.unwrap();

    let report = pipeline
        .data()
        .get_resource::<Report>()
        .expect("Report resource missing after pipeline run");

    // 9 rows total; id 1004 (amount=-50) and id 1008 (amount=0) are rejected.
    assert_eq!(report.total_rows, 9);
    assert_eq!(report.valid_count, 7);
    assert_eq!(report.rejected_count, 2);
    assert!(report.total_usd > 0.0, "total USD should be positive");

    // Stage layout: ValidateSystem and EnrichSystem share stage 1 (disjoint fields).
    let stages = pipeline.stages().unwrap_or_default();
    assert_eq!(
        stages.len(),
        3,
        "expected 3 stages: ingest / validate+enrich / report"
    );
    assert_eq!(
        stages[1].len(),
        2,
        "stage 1 should hold both validate and enrich"
    );
}
