//! Stage 4 of the polyglot example — the **Rust** guest.
//!
//! Reads the columns the Go, Python and JavaScript stages produced and writes
//! `settlement`, the one variable-length (`Utf8`) output of the chain. It also
//! carries the chain's only cross-batch state, a `Ledger` of settled volume.
//!
//! # Why this stage is the Rust one
//!
//! The other three stages mutate Arrow IPC bytes in place — they overwrite
//! fixed-width value slots and pass every other byte, including both
//! flatbuffers, through untouched. Writing a `Utf8` column means rewriting the
//! offsets buffer and the values buffer *and* the RecordBatch flatbuffer that
//! describes their lengths, so it needs a real Arrow writer. This stage has
//! one, via `pcs-guest` → arrow-rs.
//!
//! # DAG shape
//!
//! Two systems in two stages, derived from field access rather than declared:
//!
//! ```text
//! stage 0:  settle   reads  valid, flagged        writes settlement
//! stage 1:  ledger   reads  settlement, usd_amount  writes GuestState<Ledger>
//! ```
//!
//! `ledger` reads what `settle` writes, so the field-granular scheduler is
//! forced to sequence them. Nothing here declares a stage number.
//!
//! # Build
//!
//! ```bash
//! cargo component build --release -p polyglot-settle-wasm --target wasm32-wasip2
//! ```
//!
//! The component lands in `target/wasm32-wasip1/release/polyglot_settle_wasm.wasm`
//! — the `wasip1` directory name is expected for a `wasm32-wasip2`
//! cargo-component build (see `crates/pcs-guest/PINS.md`).

#![deny(missing_docs)]

// cargo-component generates `src/bindings.rs` only when building for
// wasm32-wasip2. On the host target the file does not exist, so the module
// declaration and the macro invocation are gated behind
// `#[cfg(target_arch = "wasm32")]`. This lets `cargo check --workspace`
// compile the crate as an empty cdylib while `cargo component build` produces
// the real component on wasm32.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings;

use std::sync::Arc;

use pcs_guest::GuestState;
use pcs_guest::arrow_array::{ArrayRef, RecordBatch, StringArray};
use pcs_guest::arrow_schema::{DataType, Field, Schema};
use pcs_guest::prelude::*;
use pcs_polyglot_order::Order;

/// Settlement outcome for a rejected row.
const REJECTED: &str = "REJECTED";
/// Settlement outcome for a row the JavaScript stage flagged as risky.
const HOLD: &str = "HOLD";
/// Settlement outcome for a clean row — the only one the ledger counts.
const SETTLED: &str = "SETTLED";

const ORDER_VALID: FieldRef<Order> = FieldRef::new("valid");
const ORDER_FLAGGED: FieldRef<Order> = FieldRef::new("flagged");
const ORDER_USD_AMOUNT: FieldRef<Order> = FieldRef::new("usd_amount");
const ORDER_SETTLEMENT: FieldRef<Order> = FieldRef::new("settlement");

// ---------------------------------------------------------------------------
// host-io bridge — real imports on wasm32, no-ops on the host target.
// ---------------------------------------------------------------------------

/// Report a metric through `pcs:pipeline/host-io`.
#[cfg(target_arch = "wasm32")]
fn host_metric(name: &str, value: f64) {
    crate::bindings::pcs::pipeline::host_io::metric(name, value);
}

/// Host-target stand-in: there is no host-io import to call.
#[cfg(not(target_arch = "wasm32"))]
fn host_metric(_name: &str, _value: f64) {}

/// Emit an info-level log line through `pcs:pipeline/host-io`.
#[cfg(target_arch = "wasm32")]
fn host_log(target: &str, message: &str) {
    use crate::bindings::pcs::pipeline::host_io::{LogLevel, log};
    log(LogLevel::Info, target, message);
}

/// Host-target stand-in: there is no host-io import to call.
#[cfg(not(target_arch = "wasm32"))]
fn host_log(_target: &str, _message: &str) {}

// ---------------------------------------------------------------------------
// Ledger — the guest's cross-batch state.
// ---------------------------------------------------------------------------

/// Running total of settled orders, carried across `run-batch` calls.
///
/// This is the *state* component, not a batch component: it lives in a
/// `GuestState<Ledger>` resource, is never registered on the batch dataset, and
/// therefore never appears in the output IPC. `export_pipeline!(build, state =
/// Ledger)` serialises it into `run-result.checkpoint` after every batch and
/// restores it from `prior` before the next one.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Ledger {
    /// Rows that reached `SETTLED`, summed over every batch so far.
    pub settled_count: i64,
    /// USD volume of those rows, summed over every batch so far.
    pub settled_usd: f64,
}

impl Component for Ledger {
    fn name() -> &'static str {
        "Ledger"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("settled_count", DataType::Int64, false),
            Field::new("settled_usd", DataType::Float64, false),
        ]))
    }
}

// ---------------------------------------------------------------------------
// System 1 — SettleSystem (stage 0)
// ---------------------------------------------------------------------------

/// Assigns `settlement` from the flags the upstream stages wrote.
///
/// `!valid → REJECTED`, `flagged → HOLD`, otherwise `SETTLED`. Rejection wins
/// over the risk hold: a row the Go stage rejected never had its amount
/// converted, so its `risk_score` is meaningless.
struct SettleSystem;

#[pcs_guest::prelude::async_trait]
impl System for SettleSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("settle")
            .reads(ORDER_VALID)
            .reads(ORDER_FLAGGED)
            .writes(ORDER_SETTLEMENT)
    }

    async fn run(&self, dataset: &mut Dataset) -> PcsResult<()> {
        let batch = dataset
            .columns::<Order>()
            .ok_or_else(|| PcsError::generic("polyglot-settle: Order batch not found"))?
            .clone();

        let settlements: Vec<&'static str> = {
            let orders = dataset.view::<Order>()?;
            let valid = orders.bool(ORDER_VALID)?;
            let flagged = orders.bool(ORDER_FLAGGED)?;
            (0..orders.len())
                .map(|i| {
                    if !valid.value(i) {
                        REJECTED
                    } else if flagged.value(i) {
                        HOLD
                    } else {
                        SETTLED
                    }
                })
                .collect()
        };

        let held = settlements.iter().filter(|s| **s == HOLD).count();
        let rejected = settlements.iter().filter(|s| **s == REJECTED).count();

        let schema = batch.schema();
        let settlement_idx = schema
            .index_of(ORDER_SETTLEMENT.field)
            .map_err(|e| PcsError::generic(format!("polyglot-settle: settlement missing: {e}")))?;
        let new_settlement: ArrayRef = Arc::new(StringArray::from(settlements));

        let columns: Vec<ArrayRef> = (0..schema.fields().len())
            .map(|i| {
                if i == settlement_idx {
                    new_settlement.clone()
                } else {
                    batch.column(i).clone()
                }
            })
            .collect();

        let new_batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| PcsError::generic(format!("polyglot-settle: batch rebuild: {e}")))?;
        dataset.replace_batch::<Order>(new_batch)?;

        host_metric("settle.held_rows", held as f64);
        host_metric("settle.rejected_rows", rejected as f64);
        host_log(
            "settle",
            &format!(
                "{held} held, {rejected} rejected out of {}",
                batch.num_rows()
            ),
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// System 2 — LedgerSystem (stage 1)
// ---------------------------------------------------------------------------

/// Accumulates settled volume into the cross-batch [`Ledger`].
///
/// Writes no column. Reading `settlement` — which [`SettleSystem`] writes — is
/// what places this system in a second stage; the sequencing is derived from
/// field access, not declared.
struct LedgerSystem;

#[pcs_guest::prelude::async_trait]
impl System for LedgerSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ledger")
            .reads(ORDER_SETTLEMENT)
            .reads(ORDER_USD_AMOUNT)
            .write_resource::<GuestState<Ledger>>()
    }

    async fn run(&self, dataset: &mut Dataset) -> PcsResult<()> {
        let (batch_count, batch_usd) = {
            let orders = dataset.view::<Order>()?;
            let settlement = orders.str(ORDER_SETTLEMENT)?;
            let usd = orders.f64(ORDER_USD_AMOUNT)?;
            (0..orders.len())
                .filter(|i| settlement.value(*i) == SETTLED)
                .fold((0i64, 0.0f64), |(n, sum), i| (n + 1, sum + usd.value(i)))
        };

        let state = dataset
            .get_resource_mut::<GuestState<Ledger>>()
            .ok_or_else(|| PcsError::generic("polyglot-settle: GuestState<Ledger> missing"))?;

        match state.rows.first_mut() {
            Some(ledger) => {
                ledger.settled_count += batch_count;
                ledger.settled_usd += batch_usd;
            }
            None => state.rows.push(Ledger {
                settled_count: batch_count,
                settled_usd: batch_usd,
            }),
        }

        let (total_count, total_usd) = state
            .rows
            .first()
            .map_or((0, 0.0), |l| (l.settled_count, l.settled_usd));

        host_metric("settle.settled_usd_total", total_usd);
        host_metric("settle.settled_count_total", total_count as f64);
        host_log(
            "ledger",
            &format!(
                "batch settled {batch_count} rows / {batch_usd:.2} USD; \
                 lifetime {total_count} rows / {total_usd:.2} USD"
            ),
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Pipeline construction.
// ---------------------------------------------------------------------------

/// Build the settle pipeline.
///
/// Registers only `Order`: `Ledger` is state, and a state component must NOT be
/// registered on the batch dataset — `Dataset`'s IPC format requires every
/// registered component to hold exactly the dataset's row count, while ledger
/// rows are independent of batch rows.
pub fn build() -> Pipeline {
    Pipeline::builder("polyglot-settle-rs")
        .with::<Order>()
        .with_system(SettleSystem)
        .with_system(LedgerSystem)
        .build()
}

// ---------------------------------------------------------------------------
// WIT export wiring — only on wasm32 (cargo-component generates `bindings`).
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
pcs_guest::export_pipeline!(build, state = Ledger);
