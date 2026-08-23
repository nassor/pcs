//! Stage 6 of the polyglot example: the Rust guest.
//!
//! Reads the columns the Go, Python, TypeScript, Kotlin and C# stages produced
//! and writes `settlement`, the one variable-length (`Utf8`) output of the
//! chain. It also carries the chain's only cross-batch state, a `Ledger` of
//! settled volume net of fees.
//!
//! # Why this stage is the Rust one
//!
//! The other five stages mutate Arrow IPC bytes in place, overwriting
//! fixed-width value slots and passing every other byte through untouched.
//! Writing a `Utf8` column means rewriting the offsets buffer, the values
//! buffer, and the RecordBatch flatbuffer that describes their lengths, so it
//! needs a real Arrow writer. `settlement` is the schema's only
//! variable-length output, so it is the only column that needs one. This stage
//! has one, via `pcs-guest` and arrow-rs.
//!
//! Running last also makes the ledger a cross-language check. Its total nets
//! the Kotlin stage's `fee` out of the Python stage's `usd_amount`, over the
//! rows the Go stage's `valid` and the C# stage's `review_tier` let through, so
//! two numbers depend on four `pcs-arrow-ipc` guests agreeing with arrow-rs.
//!
//! # DAG shape
//!
//! Two systems in two stages, derived from field access rather than declared:
//!
//! ```text
//! stage 0:  settle  reads  valid, review_tier           writes settlement
//! stage 1:  ledger  reads  settlement, usd_amount, fee  writes GuestState<Ledger>
//! ```
//!
//! `ledger` reads what `settle` writes, so the field-granular scheduler is
//! forced to sequence them.
//!
//! # Build
//!
//! ```bash
//! cargo component build --release -p polyglot-settle-wasm --target wasm32-wasip2
//! ```
//!
//! The component lands in `target/wasm32-wasip1/release/polyglot_settle_wasm.wasm`;
//! the `wasip1` directory name is expected for a `wasm32-wasip2`
//! cargo-component build (see `crates/pcs-guest/PINS.md`).

#![deny(missing_docs)]

// cargo-component generates `src/bindings.rs` only when building for
// wasm32-wasip2, so the module declaration and the macro invocation are gated
// on `target_arch = "wasm32"`.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings;

use std::sync::Arc;

use pcs_guest::GuestState;
use pcs_guest::arrow_array::{ArrayRef, RecordBatch, StringArray};
use pcs_guest::arrow_schema::{DataType, Field, Schema};
use pcs_guest::prelude::*;
use pcs_polyglot_order::Order;

/// Settlement outcome for a row the Go stage rejected.
const REJECTED: &str = "REJECTED";
/// Settlement outcome for review tier 2, the escalation the C# stage assigns
/// to a flagged row.
const HOLD: &str = "HOLD";
/// Settlement outcome for review tier 1, the manual look the C# stage assigns
/// to a row above its review score.
const REVIEW: &str = "REVIEW";
/// Settlement outcome for a clean row; the only one the ledger counts.
const SETTLED: &str = "SETTLED";

/// Review tier the C# stage assigns to a flagged row.
const TIER_HOLD: i64 = 2;
/// Review tier the C# stage assigns to a row above its review score.
const TIER_REVIEW: i64 = 1;
/// Review tier the C# stage leaves on a row that needs no look.
const TIER_CLEAR: i64 = 0;

const ORDER_VALID: FieldRef<Order> = FieldRef::new("valid");
const ORDER_USD_AMOUNT: FieldRef<Order> = FieldRef::new("usd_amount");
const ORDER_FEE: FieldRef<Order> = FieldRef::new("fee");
const ORDER_REVIEW_TIER: FieldRef<Order> = FieldRef::new("review_tier");
const ORDER_SETTLEMENT: FieldRef<Order> = FieldRef::new("settlement");

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
    /// Net USD volume of those rows, `usd_amount - fee`, summed over every
    /// batch so far.
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

/// Assigns `settlement` from the validity flag and the review tier upstream
/// wrote.
///
/// `!valid → REJECTED`, tier 2 → `HOLD`, tier 1 → `REVIEW`, tier 0 →
/// `SETTLED`. Rejection wins over the tier: a row the Go stage rejected never
/// had its amount converted, so its `risk_score` and therefore its tier are
/// meaningless. Any other tier value is a codec fault upstream, not a row to
/// settle by default, so it fails the batch.
struct SettleSystem;

#[pcs_guest::prelude::async_trait]
impl System for SettleSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("settle")
            .reads(ORDER_VALID)
            .reads(ORDER_REVIEW_TIER)
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
            let tier = orders.i64(ORDER_REVIEW_TIER)?;
            (0..orders.len())
                .map(|i| {
                    if !valid.value(i) {
                        return Ok(REJECTED);
                    }
                    match tier.value(i) {
                        TIER_HOLD => Ok(HOLD),
                        TIER_REVIEW => Ok(REVIEW),
                        TIER_CLEAR => Ok(SETTLED),
                        other => Err(PcsError::system_execution(format!(
                            "polyglot-settle: row {i} carries unknown review_tier {other}"
                        ))),
                    }
                })
                .collect::<PcsResult<Vec<&'static str>>>()?
        };

        let held = settlements.iter().filter(|s| **s == HOLD).count();
        let reviewed = settlements.iter().filter(|s| **s == REVIEW).count();
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
        host_metric("settle.review_rows", reviewed as f64);
        host_metric("settle.rejected_rows", rejected as f64);
        host_log(
            "settle",
            &format!(
                "{held} held, {reviewed} in review, {rejected} rejected out of {}",
                batch.num_rows()
            ),
        );
        Ok(())
    }
}

/// Accumulates settled volume, net of fees, into the cross-batch [`Ledger`].
///
/// Writes no column. It reads `settlement`, which [`SettleSystem`] writes, and
/// that places it in a second stage: the sequencing comes from field access,
/// not from a declaration.
struct LedgerSystem;

#[pcs_guest::prelude::async_trait]
impl System for LedgerSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ledger")
            .reads(ORDER_SETTLEMENT)
            .reads(ORDER_USD_AMOUNT)
            .reads(ORDER_FEE)
            .write_resource::<GuestState<Ledger>>()
    }

    async fn run(&self, dataset: &mut Dataset) -> PcsResult<()> {
        let (batch_count, batch_usd) = {
            let orders = dataset.view::<Order>()?;
            let settlement = orders.str(ORDER_SETTLEMENT)?;
            let usd = orders.f64(ORDER_USD_AMOUNT)?;
            let fee = orders.f64(ORDER_FEE)?;
            (0..orders.len())
                .filter(|i| settlement.value(*i) == SETTLED)
                .fold((0i64, 0.0f64), |(n, sum), i| {
                    (n + 1, sum + usd.value(i) - fee.value(i))
                })
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
                "batch settled {batch_count} rows / {batch_usd:.2} USD net; \
                 lifetime {total_count} rows / {total_usd:.2} USD net"
            ),
        );
        Ok(())
    }
}

/// Build the settle pipeline.
///
/// Registers only `Order`. `Ledger` is state and must not be registered on the
/// batch dataset: `Dataset`'s IPC format requires every registered component to
/// hold exactly the dataset's row count, while ledger rows are independent of
/// batch rows.
pub fn build() -> Pipeline {
    Pipeline::builder("polyglot-settle-rs")
        .with::<Order>()
        .with_system(SettleSystem)
        .with_system(LedgerSystem)
        .build()
}

#[cfg(target_arch = "wasm32")]
pcs_guest::export_pipeline!(build, state = Ledger);
