//! Order processing pipeline as a WebAssembly Component Model guest.
//!
//! This is the WASM port of `crates/pcs-service/examples/scheduler_etl.rs`. It
//! demonstrates a 2-stage field-granular DAG (Validate + Enrich in parallel,
//! then Report) running entirely inside a WASM guest via `pcs-guest`.
//!
//! # How it differs from the native example
//!
//! The native example owns its own data — `IngestSystem` appends a hardcoded
//! transaction list at the start of every `run`. In the WASM model the data
//! flows in via the host's `run-batch` Arrow IPC payload, so the guest must
//! NOT have an ingest system. The host loads `Transaction` rows (from a
//! Parquet/CSV/JSON source configured in `pcs-service`'s TOML) and hands them
//! to the guest as one batch per partition.
//!
//! The `FxRates` resource from the native example is folded into the
//! `EnrichSystem` struct rather than stored on the `Dataset`. The reason is
//! that `Dataset::write_ipc` only serializes registered components and the
//! alive bitmap — it does **not** serialize the resource map. A fresh dataset
//! reconstructed from IPC has zero resources, so any `get_resource::<FxRates>`
//! call inside `run_on` would fail. Carrying configuration on the system
//! struct itself sidesteps that limitation entirely.
//!
//! Same reason applies to the `Report` resource the native ReportSystem
//! writes. The WASM port prints to stdout (which `wasmtime-wasi` routes back
//! to the host's tracing layer via the wasi:cli/stdout adapter) and skips the
//! resource write.
//!
//! # Build
//!
//! ```bash
//! cargo component build --release -p order-processing-wasm --target wasm32-wasip2
//! ```
//!
//! The output component is at:
//!
//! ```text
//! target/wasm32-wasip1/release/order_processing_wasm.wasm
//! ```
//!
//! (cargo-component renames the target dir to `wasm32-wasip1` in its final
//! component-wrap step. This is expected.)
//!
//! # Run via pcs-service
//!
//! Once `pcs-service`'s `pipeline.wasm` TOML loader lands, the
//! example can be exercised end-to-end via:
//!
//! ```bash
//! pcs-service serve --config examples/configs/standalone_wasm.toml
//! ```
//!
//! See `README.md` in this crate for the full instructions.

#![deny(missing_docs)]

// cargo-component generates `src/bindings.rs` only when building for
// wasm32-wasip2. On the host target the file does not exist, so the module
// declaration and the macro invocation are gated behind `#[cfg(target_arch
// = "wasm32")]`. This lets `cargo check --workspace` compile the crate as an
// empty cdylib on the host while `cargo component build` produces the real
// component on wasm32.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings;

use std::sync::Arc;

use pcs_guest::arrow_array::{BooleanArray, Float64Array, RecordBatch};
use pcs_guest::arrow_schema::{DataType, Field, Schema};
use pcs_guest::prelude::*;

// ---------------------------------------------------------------------------
// Component type — Transaction (mirrors the native scheduler_etl example).
// ---------------------------------------------------------------------------

/// A financial transaction in columnar form.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Transaction {
    /// Unique transaction id.
    pub id: u64,
    /// Original amount in `currency`.
    pub amount: f64,
    /// ISO currency code ("USD", "EUR", "GBP", "JPY", "CAD").
    pub currency: String,
    /// Set to `true` by `ValidateSystem` if `amount > 0`.
    pub valid: bool,
    /// Filled by `EnrichSystem` after FX conversion.
    pub usd_amount: f64,
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
// FX rates — carried as a struct field on EnrichSystem rather than a Dataset
// resource because resources don't round-trip through Arrow IPC.
// ---------------------------------------------------------------------------

/// USD-base exchange rates used by `EnrichSystem`.
#[derive(Clone, Copy, Debug)]
struct FxRates {
    eur: f64,
    gbp: f64,
    jpy: f64,
    cad: f64,
}

impl FxRates {
    const DEFAULT: FxRates = FxRates {
        eur: 1.08,
        gbp: 1.27,
        jpy: 0.0067,
        cad: 0.74,
    };

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

// ---------------------------------------------------------------------------
// System 1 — ValidateSystem    (Stage 0 — same stage as EnrichSystem)
// ---------------------------------------------------------------------------

/// Marks each row valid or invalid based on whether `amount > 0`.
///
/// Reads `amount`, writes `valid`. The `valid` field is disjoint from
/// `usd_amount` (written by `EnrichSystem`), so the field-granular DAG places
/// the two systems in the same stage.
struct ValidateSystem;

#[pcs_guest::prelude::async_trait]
impl System for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT)
            .writes(Transaction::VALID)
    }

    async fn run(&self, dataset: &mut Dataset) -> PcsResult<()> {
        let batch = dataset
            .columns::<Transaction>()
            .ok_or_else(|| PcsError::generic("Transaction batch not found"))?
            .clone();

        let n;
        let valid_flags: Vec<bool>;
        {
            let txns = dataset.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            n = txns.len();
            valid_flags = (0..n).map(|i| amount_col.value(i) > 0.0).collect();
        }
        let valid_count = valid_flags.iter().filter(|&&v| v).count();

        let new_valid = Arc::new(BooleanArray::from(valid_flags));
        let schema = batch.schema();
        let valid_idx = schema
            .index_of("valid")
            .map_err(|e| PcsError::generic(format!("Transaction.valid missing: {e}")))?;

        let new_columns: Vec<Arc<dyn pcs_guest::arrow_array::Array>> = (0..schema.fields().len())
            .map(|i| {
                if i == valid_idx {
                    new_valid.clone() as Arc<dyn pcs_guest::arrow_array::Array>
                } else {
                    batch.column(i).clone()
                }
            })
            .collect();

        let new_batch = RecordBatch::try_new(schema, new_columns)
            .map_err(|e| PcsError::generic(format!("RecordBatch rebuild error: {e}")))?;

        dataset.replace_batch::<Transaction>(new_batch)?;

        println!(
            "[validate]  {valid_count} valid, {} rejected",
            n - valid_count
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// System 2 — EnrichSystem      (Stage 0 — same stage as ValidateSystem)
// ---------------------------------------------------------------------------

/// Converts amounts to USD using `FxRates`.
///
/// Reads `amount` and `currency`, writes `usd_amount`. The rates are a struct
/// field rather than a `Dataset` resource because resources don't survive
/// the host↔guest Arrow IPC round-trip — see crate-level docs.
struct EnrichSystem {
    rates: FxRates,
}

#[pcs_guest::prelude::async_trait]
impl System for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich")
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .writes(Transaction::USD_AMOUNT)
    }

    async fn run(&self, dataset: &mut Dataset) -> PcsResult<()> {
        let batch = dataset
            .columns::<Transaction>()
            .ok_or_else(|| PcsError::generic("Transaction batch not found"))?
            .clone();

        let n;
        let usd_amounts: Vec<f64>;
        {
            let txns = dataset.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            let currency_col = txns.str(Transaction::CURRENCY)?;
            n = txns.len();
            usd_amounts = (0..n)
                .map(|i| {
                    let rate = self.rates.rate_for(currency_col.value(i));
                    amount_col.value(i) * rate
                })
                .collect();
        }

        let new_usd = Arc::new(Float64Array::from(usd_amounts));
        let schema = batch.schema();
        let usd_idx = schema
            .index_of("usd_amount")
            .map_err(|e| PcsError::generic(format!("Transaction.usd_amount missing: {e}")))?;

        let new_columns: Vec<Arc<dyn pcs_guest::arrow_array::Array>> = (0..schema.fields().len())
            .map(|i| {
                if i == usd_idx {
                    new_usd.clone() as Arc<dyn pcs_guest::arrow_array::Array>
                } else {
                    batch.column(i).clone()
                }
            })
            .collect();

        let new_batch = RecordBatch::try_new(schema, new_columns)
            .map_err(|e| PcsError::generic(format!("RecordBatch rebuild error: {e}")))?;

        dataset.replace_batch::<Transaction>(new_batch)?;

        println!("[enrich]    converted {n} rows to USD");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// System 3 — ReportSystem     (Stage 1 — after Validate + Enrich)
// ---------------------------------------------------------------------------

/// Prints a per-row summary plus aggregate totals to stdout.
///
/// Created via `system_fn` (closure helper) for brevity. Reads the four
/// fields produced by the prior stage. Writes nothing — this is the terminal
/// stage of the per-batch DAG, so its only side effect is the printed report.
fn make_report_system() -> impl System {
    system_fn(
        SystemMeta::new("report")
            .reads(Transaction::ID)
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .reads(Transaction::VALID)
            .reads(Transaction::USD_AMOUNT),
        |data| {
            let txns = data.view::<Transaction>()?;
            let n = txns.len();

            let id_col = txns.u64(Transaction::ID)?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            let currency_col = txns.str(Transaction::CURRENCY)?;
            let valid_col = txns.bool(Transaction::VALID)?;
            let usd_col = txns.f64(Transaction::USD_AMOUNT)?;

            let mut valid_count = 0usize;
            let mut total_usd = 0.0f64;

            println!();
            println!("[report]    ── transaction batch ──");
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
                        println!("[report]      #{id:<6} {usd:>12.2} USD");
                    } else {
                        println!(
                            "[report]      #{id:<6} {amount:>12.2} {currency} → {usd:>12.2} USD"
                        );
                    }
                } else {
                    println!("[report]      #{id:<6} {amount:>12.2} {currency}  REJECTED");
                }
            }

            let rejected = n - valid_count;
            println!(
                "[report]    {valid_count}/{n} valid, {rejected} rejected, {total_usd:>12.2} USD total"
            );
            Ok(())
        },
    )
}

// ---------------------------------------------------------------------------
// Pipeline construction — called by the export_pipeline! macro on first use.
// ---------------------------------------------------------------------------

/// Build the order_processing pipeline.
///
/// Called lazily by the `export_pipeline!` macro on the first call to any WIT
/// export (typically `describe`). The macro wraps the returned [`Pipeline`] in
/// `OnceLock<Mutex<Pipeline>>` so it's constructed exactly once per component
/// instance.
///
/// FxRates are baked into `EnrichSystem` at construction time. A future
/// version can read them from the `init(config)` JSON blob via
/// `pcs_guest::config_get_typed("fx_rates")` once that helper graduates from
/// stub.
pub fn build() -> Pipeline {
    Pipeline::builder("order_processing")
        .with::<Transaction>()
        .with_system(ValidateSystem)
        .with_system(EnrichSystem {
            rates: FxRates::DEFAULT,
        })
        .with_system(make_report_system())
        .build()
}

// ---------------------------------------------------------------------------
// WIT export wiring — only on wasm32 (cargo-component generates `bindings`).
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
pcs_guest::export_pipeline!(build);
