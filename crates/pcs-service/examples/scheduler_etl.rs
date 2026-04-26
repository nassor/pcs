//! Arrow-backed ETL pipeline: financial transaction processing with field-granular scheduling.
//!
//! Demonstrates PCS's Arrow pipeline running a 4-system ETL workflow over columnar
//! transaction data. The key difference from the ECS-based `pipeline_etl.rs` is that
//! systems declare access at **field granularity**, enabling the scheduler to place
//! non-conflicting systems in the same stage even when they touch the same component.
//!
//! ## Stage layout (automatically computed from field access declarations)
//!
//! ```text
//! Stage 0:  [IngestSystem]
//!           — writes all fields of Transaction (no prior data)
//!
//! Stage 1:  [ValidateSystem, EnrichSystem]
//!           — ValidateSystem writes "valid" field only
//!           — EnrichSystem writes "usd_amount" field only
//!           — These two fields are DISJOINT → same stage (parallel-safe)
//!           — Both read "amount"/"currency" which IngestSystem wrote → must follow stage 0
//!
//! Stage 2:  [ReportSystem]
//!           — reads "valid" (written by ValidateSystem in stage 1)
//!           — reads "usd_amount" (written by EnrichSystem in stage 1)
//!           — must follow stage 1
//! ```
//!
//! The field-level scheduler detects that ValidateSystem and EnrichSystem write
//! completely different fields and places them in the same stage. With the old
//! type-level model they would have conflicted (same component → sequential).
//!
//! ## Running
//!
//! ```bash
//! cargo run --example scheduler_etl
//! ```

use std::sync::Arc;

use arrow_array::{BooleanArray, Float64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

use pcs_service::PcsError;
use pcs_service::component::Component;
use pcs_service::pipeline::{Dataset, Pipeline};
use pcs_service::system::{FieldRef, System, SystemMeta, system_fn};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Component type
// ---------------------------------------------------------------------------

/// A financial transaction in columnar form.
///
/// Fields:
/// - `id`: unique transaction ID
/// - `amount`: original amount in native currency
/// - `currency`: ISO currency code ("USD", "EUR", etc.)
/// - `valid`: set to `true` by ValidateSystem if amount > 0
/// - `usd_amount`: filled by EnrichSystem after currency conversion
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
    const ID: FieldRef<Transaction> = FieldRef::new("id");
    const AMOUNT: FieldRef<Transaction> = FieldRef::new("amount");
    const CURRENCY: FieldRef<Transaction> = FieldRef::new("currency");
    const VALID: FieldRef<Transaction> = FieldRef::new("valid");
    const USD_AMOUNT: FieldRef<Transaction> = FieldRef::new("usd_amount");
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/// FX rates used by EnrichSystem (USD as base).
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

/// Summary written by ReportSystem.
struct Report {
    total_rows: usize,
    valid_count: usize,
    rejected_count: usize,
    total_usd: f64,
}

// ---------------------------------------------------------------------------
// System 1 — IngestSystem
// ---------------------------------------------------------------------------

/// Loads seed transaction data into the pipeline.
///
/// Writes all fields of Transaction (the initial data population).
/// Because it writes the entire component, it is placed in stage 0.
struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        // Write all fields — any subsequent reader must come after us.
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
            }, // negative → rejected
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
            }, // zero → rejected
            Transaction {
                id: 1009,
                amount: 680.00,
                currency: "CAD".into(),
                valid: false,
                usd_amount: 0.0,
            },
        ];

        pipeline.append::<Transaction>(&transactions)?;
        println!("[ingest]    loaded {} transactions", transactions.len());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// System 2a — ValidateSystem   (Stage 1 — same stage as EnrichSystem)
// ---------------------------------------------------------------------------

/// Marks each row valid or invalid based on whether amount > 0.
///
/// Only writes the `valid` field. Because this field is disjoint from
/// `usd_amount` (written by EnrichSystem), both systems land in stage 1.
struct ValidateSystem;

#[async_trait]
impl System for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT) // reads amount to decide validity
            .writes(Transaction::VALID) // writes only the valid flag
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), PcsError> {
        // Clone the batch for schema access and batch rebuild.
        let batch = pipeline
            .columns::<Transaction>()
            .ok_or_else(|| PcsError::generic("Transaction batch not found"))?
            .clone();

        // Use ComponentView for ergonomic column extraction.
        let n;
        let valid_flags: Vec<bool>;
        {
            let txns = pipeline.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            n = txns.len();
            valid_flags = (0..n).map(|i| amount_col.value(i) > 0.0).collect();
        }
        let valid_count = valid_flags.iter().filter(|&&v| v).count();

        // Replace the batch with updated valid column.
        // We reconstruct all columns, swapping in the new `valid` array.
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

        println!(
            "[validate]  {} valid, {} rejected",
            valid_count,
            n - valid_count
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// System 2b — EnrichSystem   (Stage 1 — same stage as ValidateSystem)
// ---------------------------------------------------------------------------

/// Converts amounts to USD using FX rates.
///
/// Only writes `usd_amount`. Because this is disjoint from `valid`
/// (written by ValidateSystem), they share stage 1.
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

        // Clone the batch for schema access and batch rebuild.
        let batch = pipeline
            .columns::<Transaction>()
            .ok_or_else(|| PcsError::generic("Transaction batch not found"))?
            .clone();

        // Use ComponentView for ergonomic column extraction.
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

        println!("[enrich]    converted {} rows to USD", n);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// System 3 — ReportSystem   (Stage 2 — after ValidateSystem + EnrichSystem)
// ---------------------------------------------------------------------------

/// Reads all enriched/validated data and prints a summary.
///
/// Reads `valid` (from ValidateSystem) and `usd_amount` (from EnrichSystem),
/// so it must come after stage 1. Created with `system_fn` for brevity.
fn make_report_system() -> impl System {
    system_fn(
        SystemMeta::new("report")
            .reads(Transaction::ID)
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .reads(Transaction::VALID)
            .reads(Transaction::USD_AMOUNT)
            .write_resource::<Report>(),
        |data| {
            let n;
            let mut valid_count = 0usize;
            let mut total_usd = 0.0f64;

            println!();
            println!("╔═══════════════════════════════════════════════════════╗");
            println!("║       ARROW ETL PIPELINE — TRANSACTION REPORT        ║");
            println!("╠═══════════════════════════════════════════════════════╣");

            {
                let txns = data.view::<Transaction>()?;
                n = txns.len();

                let id_col = txns.u64(Transaction::ID)?;
                let amount_col = txns.f64(Transaction::AMOUNT)?;
                let currency_col = txns.str(Transaction::CURRENCY)?;
                let valid_col = txns.bool(Transaction::VALID)?;
                let usd_col = txns.f64(Transaction::USD_AMOUNT)?;

                for i in 0..n {
                    let id = id_col.value(i);
                    let amount = amount_col.value(i);
                    let currency = currency_col.value(i);
                    let is_valid = valid_col.value(i);
                    let usd = usd_col.value(i);

                    if is_valid {
                        valid_count += 1;
                        total_usd += usd;
                        if currency == "USD" {
                            println!(
                                "║  #{:<5}  {:>10.2} USD                                ║",
                                id, usd
                            );
                        } else {
                            println!(
                                "║  #{:<5}  {:>10.2} {} → {:>10.2} USD           ║",
                                id, amount, currency, usd
                            );
                        }
                    } else {
                        println!(
                            "║  #{:<5}  {:>10.2} {}  REJECTED                     ║",
                            id, amount, currency
                        );
                    }
                }
            } // txns borrow released here

            let rejected = n - valid_count;
            println!("╠═══════════════════════════════════════════════════════╣");
            println!(
                "║  Total rows:   {:>4}                                  ║",
                n
            );
            println!(
                "║  Valid:        {:>4}                                  ║",
                valid_count
            );
            println!(
                "║  Rejected:     {:>4}                                  ║",
                rejected
            );
            println!(
                "║  Total USD:    {:>12.2}                         ║",
                total_usd
            );
            if valid_count > 0 {
                println!(
                    "║  Average USD:  {:>12.2}                         ║",
                    total_usd / valid_count as f64
                );
            }
            println!("╚═══════════════════════════════════════════════════════╝");

            data.insert_resource(Report {
                total_rows: n,
                valid_count,
                rejected_count: rejected,
                total_usd,
            });

            Ok(())
        },
    )
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), PcsError> {
    // Build the pipeline: data + systems bundled together.
    //
    // Stage layout computed by the DAG scheduler:
    //   Stage 0: [IngestSystem]         — writes_component("Transaction")
    //   Stage 1: [ValidateSystem,       — reads "amount", writes "valid"
    //             EnrichSystem]         — reads "amount"/"currency", writes "usd_amount"
    //             (disjoint field writes → same stage)
    //   Stage 2: [ReportSystem]         — reads "valid" + "usd_amount"
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

    println!("Starting ETL pipeline...");

    pipeline.run().await?;

    // Print stage layout to demonstrate field-level scheduling.
    let stages = pipeline.stages().unwrap_or_default();
    println!();
    println!("Stage layout (field-level DAG):");
    for (i, stage) in stages.iter().enumerate() {
        println!("  Stage {i}: {stage:?}");
    }
    println!("  (ValidateSystem and EnrichSystem share stage 1");
    println!("   because they write disjoint fields of Transaction)");

    // Verify the report resource was written.
    let report = pipeline
        .data()
        .get_resource::<Report>()
        .ok_or_else(|| PcsError::generic("Report resource missing after pipeline run"))?;
    println!();
    println!(
        "Pipeline complete: {}/{} valid, {} rejected, ${:.2} total USD",
        report.valid_count, report.total_rows, report.rejected_count, report.total_usd
    );

    Ok(())
}
