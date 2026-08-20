//! Host↔guest contract tests against the pcs-guest-smoketest WebAssembly
//! component.
//!
//! This is the host side of the CI guest round-trip check against the host
//! arrow-ipc pin. The shell wrapper at `scripts/ci/guest_ipc_roundtrip.sh`
//! orchestrates the full flow:
//!
//! 1. `cargo component build --release -p pcs-guest-smoketest --target wasm32-wasip2`
//! 2. `cargo test --test wasm_roundtrip -p pcs-service --features wasm`
//!
//! The artifact lands under `target/wasm32-wasip1/release/` even though the
//! build targets `wasm32-wasip2`: cargo-component 0.21.1 compiles the core
//! module for wasip1 and adapts it into a wasip2 component, keeping the
//! pre-adapter directory name.
//!
//! Three properties are covered:
//!
//! - **Arrow IPC byte-exactness.** No system touches `Ping` and the guest keeps
//!   its state in a resource, so the whole dataset's round-tripped IPC bytes
//!   must be identical. The workspace pins `arrow-ipc = "=59.2.0"` exactly and
//!   both pcs-core (host) and pcs-guest (guest) take it via
//!   `workspace = true`; if a transitive dep ever resolved a different patch
//!   version on either side the bytes would differ and this assertion would
//!   fail long before the drift reached production.
//! - **Config delivery.** `build()` in the guest reads the `greeting` key
//!   through the `host-io` `get-config` import and appends it to the pipeline
//!   name, so `describe()` reports whether the host's
//!   `[pipeline.wasm.config]` table reached the guest.
//! - **State across batches.** The guest is exported with
//!   `export_pipeline!(build, state = Counter)`, so threading each
//!   `run-batch` checkpoint back in as the next `prior` must make the counter
//!   read 1, 2, 3 — the host creates a fresh wasmtime Store per call, so this
//!   can only work through the blob.

#![cfg(feature = "wasm")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pcs_core::runtime::PipelineRuntime;
use pcs_service::component::Component;
use pcs_service::dataset::Dataset;
use pcs_service::wasm::{WasmEngine, WasmPipelineRuntime};
use serde::{Deserialize, Serialize};

use arrow_array::UInt64Array;
use arrow_schema::{DataType, Field, Schema};

/// Host-side mirror of the `Ping` component declared in
/// `crates/pcs-guest-smoketest/src/lib.rs`.
///
/// The two definitions MUST share the same name and field shape — that's the
/// invariant this test enforces. If the smoketest schema ever drifts the
/// schema-fingerprint check below will fail.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Ping {
    seq: u64,
}

impl Component for Ping {
    fn name() -> &'static str {
        "Ping"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "seq",
            DataType::UInt64,
            false,
        )]))
    }
}

/// Host-side mirror of the smoketest's state component.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Counter {
    count: u64,
}

impl Component for Counter {
    fn name() -> &'static str {
        "Counter"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }
}

fn smoketest_wasm_path() -> PathBuf {
    // Locate the smoketest output relative to the workspace root. cargo's
    // CARGO_MANIFEST_DIR points at crates/pcs-service for this test, so go up
    // two levels to reach the workspace root.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root above crates/pcs-service");
    workspace_root
        .join("target")
        .join("wasm32-wasip1")
        .join("release")
        .join("pcs_guest_smoketest.wasm")
}

fn load_runtime(config: HashMap<String, String>) -> WasmPipelineRuntime {
    let wasm_path = smoketest_wasm_path();
    assert!(
        wasm_path.exists(),
        "smoketest .wasm not found at {}; \
         run `cargo component build --release -p pcs-guest-smoketest --target wasm32-wasip2` first",
        wasm_path.display()
    );

    let wasm_bytes =
        std::fs::read(&wasm_path).unwrap_or_else(|e| panic!("read smoketest wasm: {e}"));

    let engine = WasmEngine::new().expect("WasmEngine init");
    WasmPipelineRuntime::from_bytes(
        engine,
        "smoketest",
        &wasm_bytes,
        config,
        // 60 epoch ticks * 100 ms = 6 s — plenty for an identity round-trip.
        60,
    )
    .expect("WasmPipelineRuntime::from_bytes")
}

/// Seed a dataset from the runtime's own template so every component the guest
/// declares is registered, then fill the data plane with `rows` `Ping` values.
fn seeded_dataset(runtime: &WasmPipelineRuntime, rows: usize) -> Dataset {
    let mut dataset = runtime.template_dataset();
    let pings: Vec<Ping> = (0..rows).map(|i| Ping { seq: i as u64 }).collect();
    dataset.append::<Ping>(&pings).expect("append Ping rows");
    dataset
}

/// Decode a `run-result.checkpoint` blob and read the counter out of it.
fn counter_in_blob(blob: &[u8]) -> Option<u64> {
    let state = Dataset::read_ipc(&mut &blob[..]).expect("decode state blob");
    counter_value(&state)
}

fn counter_value(dataset: &Dataset) -> Option<u64> {
    let batch = dataset.batch_for(Counter::name())?;
    if batch.num_rows() == 0 {
        return None;
    }
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("Counter.count is a u64 column");
    Some(col.value(0))
}

#[tokio::test(flavor = "current_thread")]
async fn smoketest_arrow_ipc_round_trip_is_byte_exact() {
    let runtime = load_runtime(HashMap::new());

    // Surface the descriptor and capture the schema fingerprint the guest
    // computed for itself. We'll cross-check against the host's local
    // SchemaRegistry fingerprint after seeding the dataset.
    let descriptor = runtime.describe().expect("guest describe");
    assert_eq!(descriptor.name, "smoketest");
    let declared: Vec<&str> = descriptor
        .components
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(
        declared,
        vec!["Ping"],
        "the state component lives in a resource, so describe() declares only Ping"
    );
    assert!(
        descriptor.stateful,
        "a `state = Counter` guest must declare itself stateful"
    );
    assert!(
        !descriptor.components[0].arrow_schema_ipc.is_empty(),
        "guest must emit non-empty Arrow IPC schema bytes for Ping"
    );

    let mut dataset = seeded_dataset(&runtime, 16);

    // Cross-check: host fingerprint must equal guest fingerprint as a hex
    // string. If this fails, something has drifted in `SchemaRegistry::fingerprint`
    // OR the Schema definitions don't match between sides.
    let host_fingerprint_hex = format!("{:08x}", dataset.schemas().fingerprint());
    assert_eq!(
        descriptor.schema_fingerprint, host_fingerprint_hex,
        "guest schema fingerprint ({}) must match host fingerprint ({})",
        descriptor.schema_fingerprint, host_fingerprint_hex
    );

    // BEFORE snapshot of the dataset's Arrow IPC.
    let mut before: Vec<u8> = Vec::new();
    dataset.write_ipc(&mut before).expect("write_ipc before");
    let before_rows = dataset.rows();

    // Drive the round-trip. No system touches Ping, and the counter lives in a
    // resource → identity over the whole dataset.
    runtime
        .run_on(&mut dataset)
        .await
        .expect("guest run_on success");

    // AFTER snapshot.
    let mut after: Vec<u8> = Vec::new();
    dataset.write_ipc(&mut after).expect("write_ipc after");
    let after_rows = dataset.rows();

    assert_eq!(
        before_rows, after_rows,
        "row count must survive the round-trip exactly"
    );

    // The byte-exact equality is the load-bearing assertion: it catches
    // arrow-ipc patch drift between host and guest, and any layout change in
    // Dataset::write_ipc that would silently corrupt checkpoints in production.
    assert_eq!(
        before, after,
        "Arrow IPC bytes must be byte-exact across the host↔guest round-trip; \
         if this assertion fails, arrow-ipc has drifted between pcs-core and pcs-guest"
    );
}

/// `[pipeline.wasm.config]` values reach the guest through the `host-io`
/// `get-config` import, including during `describe()`.
#[tokio::test(flavor = "current_thread")]
async fn guest_reads_host_injected_config() {
    let without = load_runtime(HashMap::new());
    assert_eq!(
        without.describe().expect("describe").name,
        "smoketest",
        "an empty config table means the guest sees no `greeting` key"
    );

    let with = load_runtime(HashMap::from([(
        "greeting".to_string(),
        "hello".to_string(),
    )]));
    assert_eq!(
        with.describe().expect("describe").name,
        "smoketest-hello",
        "the guest must observe the injected `greeting` value"
    );
}

/// Threading `run-result.checkpoint` back in as the next `prior` is the only
/// way guest state survives, because the host builds a fresh wasmtime Store —
/// and therefore fresh guest linear memory — for every call.
#[tokio::test(flavor = "current_thread")]
async fn guest_state_survives_across_batches() {
    let runtime = load_runtime(HashMap::new());

    let mut prior: Option<Vec<u8>> = None;
    for expected in 1..=3u64 {
        let mut dataset = seeded_dataset(&runtime, 4);

        let next = runtime
            .run_on_with_state(&mut dataset, prior.as_deref())
            .await
            .expect("guest run_on_with_state success")
            .expect("a stateful guest must return a checkpoint blob");

        assert_eq!(
            counter_value(&dataset),
            None,
            "batch {expected}: state must not leak into the output dataset"
        );

        // The blob is the channel the host persists; it carries the state.
        assert_eq!(
            counter_in_blob(&next),
            Some(expected),
            "batch {expected}: the checkpoint blob must carry the updated counter"
        );

        prior = Some(next);
    }
}

/// Without the blob the guest starts from scratch every time — the negative
/// control for the test above.
#[tokio::test(flavor = "current_thread")]
async fn guest_state_resets_when_prior_is_dropped() {
    let runtime = load_runtime(HashMap::new());

    for _ in 0..3 {
        let mut dataset = seeded_dataset(&runtime, 4);
        let blob = runtime
            .run_on_with_state(&mut dataset, None)
            .await
            .expect("guest run_on_with_state success")
            .expect("a stateful guest must return a checkpoint blob");
        assert_eq!(
            counter_in_blob(&blob),
            Some(1),
            "with no prior, every batch is the guest's first"
        );
    }
}

/// Regression: `run_on` must survive being awaited on a **multi-threaded**
/// tokio runtime, which is what `#[tokio::main]` gives `pcs-service`.
///
/// The guest is linked against the synchronous WASI implementation
/// (`add_to_linker_sync`), so every WASI import the guest touches funnels into
/// `wasmtime_wasi::runtime::in_tokio` → `Handle::block_on`. Called from a
/// thread that is driving a tokio runtime, that panics with "Cannot start a
/// runtime from within a runtime" and takes the whole service down on its first
/// batch. `WasmPipelineRuntime::run_on_with_state` therefore has to hand the
/// call to `spawn_blocking`.
///
/// The other tests in this file pin `current_thread` and so never exercised the
/// flavour the binary actually runs under.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_on_works_on_a_multi_thread_runtime() {
    let runtime = load_runtime(HashMap::new());
    let mut dataset = seeded_dataset(&runtime, 3);
    let before = dataset.rows();

    runtime
        .run_on(&mut dataset)
        .await
        .expect("run_on must not panic or fail on a multi-thread runtime");

    assert_eq!(
        dataset.rows(),
        before,
        "the smoketest is an identity pipeline, so the row count must survive"
    );
}
