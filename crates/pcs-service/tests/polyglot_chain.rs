//! End-to-end regression for the four-language guest chain in `examples/polyglot/`.
//!
//! Four WebAssembly components — Go, Python, JavaScript, Rust — each export the
//! same `pcs:pipeline@0.2.0` world and each write a different column of the same
//! `Order` component. This test drives them through the real host
//! (`WasmPipelineRuntime`) in order and asserts the exact values every stage
//! produced, so a regression in any single language's Arrow IPC codec fails here
//! with a named column rather than as a mysterious byte diff.
//!
//! Three of the four guests hand-roll their Arrow IPC codec against nothing but
//! their language's standard library, mutating fixed-width value bytes in place.
//! That is the part most likely to rot, and the assertions below are the only
//! automated check that all three still agree with arrow-rs.
//!
//! # Soft skip
//!
//! Building the components needs Go, Python and Node toolchains that the default
//! `test` CI job does not install, so a missing `examples/polyglot/build/`
//! prints `SKIP:` and passes. The dedicated `Polyglot Guests` CI job installs
//! all four toolchains, runs `scripts/build-polyglot.sh`, and then this test
//! actually asserts.
//!
//! ```bash
//! bash scripts/build-polyglot.sh
//! cargo test -p pcs-service --features wasm --test polyglot_chain -- --nocapture
//! ```

#![cfg(feature = "wasm")]

use std::collections::HashMap;
use std::path::PathBuf;

use pcs_core::runtime::PipelineRuntime;
use pcs_polyglot_order::{Order, fixture_rows};
use pcs_service::dataset::Dataset;
use pcs_service::wasm::{WasmEngine, WasmPipelineRuntime};

/// Absolute tolerance for float comparisons: `100.0 * 1.10` is not exactly
/// `110.0` in f64, and the value crosses three language runtimes before it is
/// checked.
const EPS: f64 = 1e-6;

/// 100 epoch ticks × 100 ms — the same 10 s per-call budget the service loader
/// grants. The Python guest re-initialises CPython on every `run-batch` because
/// the host builds a fresh `Store` per call, so this is not as generous as it
/// looks.
const EPOCH_TICKS: u64 = 100;

/// `(file, runtime name, config)` for each stage, in execution order.
type StageSpec = (
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
);

const STAGES: [StageSpec; 4] = [
    (
        "validate-go.wasm",
        "polyglot-validate-go",
        &[("min_amount", "0")],
    ),
    (
        "enrich-py.wasm",
        "polyglot-enrich-py",
        &[("fx_eur", "1.10"), ("fx_gbp", "1.30"), ("fx_jpy", "0.0068")],
    ),
    (
        "score-js.wasm",
        "polyglot-score-js",
        &[("risk_threshold", "50000")],
    ),
    ("settle-rs.wasm", "polyglot-settle-rs", &[]),
];

/// The one stateful stage. Only it may return a `run-result.checkpoint`.
const STATEFUL_STAGE: &str = "polyglot-settle-rs";

/// Resolve the build directory exactly the way the driver example does, so a
/// contributor pointing `PCS_POLYGLOT_BUILD_DIR` at a scratch tree gets both.
fn build_dir() -> PathBuf {
    match std::env::var_os("PCS_POLYGLOT_BUILD_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root above crates/pcs-service")
            .join("examples/polyglot/build"),
    }
}

fn order_dataset() -> Dataset {
    let mut dataset = Dataset::new();
    dataset
        .register_component::<Order>()
        .expect("register Order");
    dataset
        .append::<Order>(&fixture_rows())
        .expect("append fixture rows");
    dataset
}

/// The fingerprint the host will see in every stage's descriptor, derived live
/// from `pcs_polyglot_order::Order` rather than hardcoded — that is the whole
/// point of the check.
fn expected_fingerprint() -> String {
    let mut dataset = Dataset::new();
    dataset
        .register_component::<Order>()
        .expect("register Order");
    format!("{:08x}", dataset.schemas().fingerprint())
}

fn load_stages() -> Option<Vec<(&'static str, WasmPipelineRuntime)>> {
    let dir = build_dir();
    let missing: Vec<&str> = STAGES
        .iter()
        .map(|(file, _, _)| *file)
        .filter(|file| !dir.join(file).exists())
        .collect();
    if !missing.is_empty() {
        println!(
            "SKIP: polyglot components not built (run scripts/build-polyglot.sh) — \
             missing {missing:?} under {}",
            dir.display()
        );
        return None;
    }

    let engine = WasmEngine::new().expect("WasmEngine init");
    Some(
        STAGES
            .iter()
            .map(|(file, name, config)| {
                let bytes =
                    std::fs::read(dir.join(file)).unwrap_or_else(|e| panic!("read {file}: {e}"));
                let config: HashMap<String, String> = config
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect();
                let runtime = WasmPipelineRuntime::from_bytes(
                    engine.clone(),
                    *name,
                    &bytes,
                    config,
                    EPOCH_TICKS,
                )
                .unwrap_or_else(|e| panic!("compile {file}: {e}"));
                (*name, runtime)
            })
            .collect(),
    )
}

/// Read the ledger the Rust stage accumulated out of a checkpoint blob.
fn ledger_totals(blob: &[u8]) -> (i64, f64) {
    let state = Dataset::read_ipc(&mut &blob[..]).expect("decode checkpoint blob");
    let batch = state
        .batch_for("Ledger")
        .expect("checkpoint carries a Ledger component");
    let count = batch
        .column_by_name("settled_count")
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::Int64Array>())
        .expect("Ledger.settled_count is Int64")
        .value(0);
    let usd = batch
        .column_by_name("settled_usd")
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::Float64Array>())
        .expect("Ledger.settled_usd is Float64")
        .value(0);
    (count, usd)
}

fn assert_floats(label: &str, actual: &[f64], expected: &[f64]) {
    assert_eq!(actual.len(), expected.len(), "{label}: row count");
    for (i, (a, e)) in actual.iter().zip(expected).enumerate() {
        assert!(
            (a - e).abs() < EPS,
            "{label}[{i}]: expected {e}, got {a} (tolerance {EPS})"
        );
    }
}

/// Assert every column of the chain's output. The only column Rust wrote is
/// `settlement`; `valid` came from Go, `usd_amount` from Python, and
/// `risk_score` / `flagged` from JavaScript.
fn assert_chain_output(dataset: &Dataset) {
    let orders = dataset.view::<Order>().expect("Order view");
    assert_eq!(orders.len(), 5, "row count survived the chain");

    let ids: Vec<i64> = (0..5).map(|i| orders.i64("id").unwrap().value(i)).collect();
    assert_eq!(ids, [1, 2, 3, 4, 5], "id must pass through untouched");

    let regions: Vec<&str> = (0..5)
        .map(|i| orders.str("region").unwrap().value(i))
        .collect();
    assert_eq!(
        regions,
        ["emea", "emea", "apac", "amer", "emea"],
        "region must pass through untouched"
    );

    let currencies: Vec<&str> = (0..5)
        .map(|i| orders.str("currency").unwrap().value(i))
        .collect();
    assert_eq!(currencies, ["EUR", "GBP", "JPY", "USD", "EUR"]);

    let amounts: Vec<f64> = (0..5)
        .map(|i| orders.f64("amount").unwrap().value(i))
        .collect();
    assert_floats(
        "amount",
        &amounts,
        &[100.0, -5.0, 1_000_000.0, 60_000.0, 0.0],
    );

    // Go: valid = amount > min_amount (0). Row 5 has amount == 0, which is not
    // greater than the threshold.
    let valid: Vec<bool> = (0..5)
        .map(|i| orders.bool("valid").unwrap().value(i))
        .collect();
    assert_eq!(
        valid,
        [true, false, true, true, false],
        "`valid` is written by the Go guest"
    );

    // Python: usd_amount = valid ? amount * fx(currency) : 0.
    let usd: Vec<f64> = (0..5)
        .map(|i| orders.f64("usd_amount").unwrap().value(i))
        .collect();
    assert_floats(
        "usd_amount (Python guest)",
        &usd,
        &[110.0, 0.0, 6_800.0, 60_000.0, 0.0],
    );

    // JavaScript: risk_score = usd_amount / risk_threshold, flagged = risk >= 1.
    let risk: Vec<f64> = (0..5)
        .map(|i| orders.f64("risk_score").unwrap().value(i))
        .collect();
    assert_floats(
        "risk_score (JavaScript guest)",
        &risk,
        &[0.0022, 0.0, 0.136, 1.2, 0.0],
    );

    let flagged: Vec<bool> = (0..5)
        .map(|i| orders.bool("flagged").unwrap().value(i))
        .collect();
    assert_eq!(
        flagged,
        [false, false, false, true, false],
        "`flagged` is written by the JavaScript guest"
    );

    // Rust: !valid -> REJECTED, flagged -> HOLD, else SETTLED.
    let settlement: Vec<&str> = (0..5)
        .map(|i| orders.str("settlement").unwrap().value(i))
        .collect();
    assert_eq!(
        settlement,
        ["SETTLED", "REJECTED", "SETTLED", "HOLD", "REJECTED"],
        "`settlement` is written by the Rust guest"
    );
}

/// The whole chain, in one test: four `describe()` contracts, the nine output
/// columns, and the cross-batch ledger. Splitting it would recompile and
/// re-instantiate four components per assertion group for no extra coverage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_language_chain_produces_exact_values() {
    let Some(stages) = load_stages() else {
        return;
    };

    // ---- describe() ------------------------------------------------------
    let fingerprint = expected_fingerprint();
    for (name, runtime) in &stages {
        let descriptor = runtime.describe().expect("describe()");
        assert_eq!(
            descriptor.schema_fingerprint, fingerprint,
            "{name} reports a schema fingerprint that disagrees with \
             pcs_polyglot_order::Order — the generated constants have drifted, \
             re-run `cargo run -p pcs-service --features wasm \
             --example polyglot_orders -- emit` and rebuild"
        );
        assert_eq!(
            runtime.declared_components(),
            ["Order"],
            "{name} must declare exactly the Order component"
        );
        assert_eq!(
            descriptor.stateful,
            *name == STATEFUL_STAGE,
            "{name}: only {STATEFUL_STAGE} keeps state across batches"
        );
    }

    // ---- batch 1 ---------------------------------------------------------
    let mut dataset = order_dataset();
    let mut checkpoint: Option<Vec<u8>> = None;
    for (name, runtime) in &stages {
        let produced = runtime
            .run_on_with_state(&mut dataset, None)
            .await
            .unwrap_or_else(|e| panic!("{name} run-batch: {e}"));
        if *name == STATEFUL_STAGE {
            assert!(
                produced.is_some(),
                "{name} is stateful and must return a checkpoint"
            );
            checkpoint = produced;
        } else {
            assert!(
                produced.is_none(),
                "{name} is stateless and must return no checkpoint"
            );
        }
    }
    assert_chain_output(&dataset);

    let checkpoint = checkpoint.expect("the stateful stage returned a checkpoint");
    let (count1, usd1) = ledger_totals(&checkpoint);
    assert_eq!(count1, 2, "two rows settled in batch 1");
    assert!(
        (usd1 - 6_910.0).abs() < EPS,
        "batch 1 settled USD: expected 6910.0, got {usd1}"
    );

    // ---- batch 2: the checkpoint is the only channel for guest state -----
    // The host builds a fresh wasmtime Store per call, so if the totals double
    // it can only be because the blob round-tripped.
    let mut dataset2 = order_dataset();
    let mut checkpoint2: Option<Vec<u8>> = None;
    for (name, runtime) in &stages {
        let prior = if *name == STATEFUL_STAGE {
            Some(checkpoint.as_slice())
        } else {
            None
        };
        let produced = runtime
            .run_on_with_state(&mut dataset2, prior)
            .await
            .unwrap_or_else(|e| panic!("{name} run-batch (2): {e}"));
        if *name == STATEFUL_STAGE {
            checkpoint2 = produced;
        }
    }
    assert_chain_output(&dataset2);

    let (count2, usd2) = ledger_totals(&checkpoint2.expect("batch 2 checkpoint"));
    assert_eq!(
        count2, 4,
        "the ledger accumulated across the batch boundary"
    );
    assert!(
        (usd2 - 13_820.0).abs() < EPS,
        "batch 2 settled USD: expected 13820.0, got {usd2}"
    );
}
