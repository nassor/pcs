//! Arrow IPC round-trip test against the pcs-guest-smoketest WebAssembly
//! component.
//!
//! This is the host side of the CI guest round-trip RecordBatch check against
//! host arrow-ipc pin). The shell wrapper at `scripts/ci/guest_ipc_roundtrip.sh`
//! orchestrates the full flow:
//!
//! 1. `cargo component build --release -p pcs-guest-smoketest --target wasm32-wasip2`
//! 2. `cargo test --test wasm_roundtrip -p pcs-service --features wasm`
//!
//! The test:
//! - Loads the smoketest `.wasm` from `target/wasm32-wasip1/release/pcs_guest_smoketest.wasm`.
//! - Compiles it via `WasmPipelineRuntime::from_bytes`.
//! - Calls `describe()` to surface the component's declared schema fingerprint.
//! - Constructs a host-side `Dataset` containing a `Ping` component (matching
//!   the smoketest's schema exactly) with several rows.
//! - Writes the dataset to Arrow IPC bytes (the BEFORE snapshot).
//! - Calls `runtime.run_on(&mut dataset)` — the smoketest is an identity
//!   pipeline (zero systems), so the dataset must come back unchanged.
//! - Writes the post-run dataset to Arrow IPC bytes (the AFTER snapshot).
//! - Asserts BEFORE == AFTER byte-exact: any drift in arrow-ipc between host
//!   and guest would corrupt the round-trip and fail this assertion.
//!
//! Why this catches arrow-ipc version drift: the workspace pins
//! `arrow-ipc = "=58.1.0"` exactly. Both pcs-core (host) and pcs-guest (guest)
//! depend on it via `workspace = true`. If a transitive dep ever resolves a
//! different patch version on either side, the IPC bytes won't be byte-equal
//! and this test will fail loudly long before the drift reaches production.

#![cfg(feature = "wasm")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pcs_core::runtime::PipelineRuntime;
use pcs_service::component::Component;
use pcs_service::pipeline::Dataset;
use pcs_service::wasm::{WasmEngine, WasmPipelineRuntime};
use serde::{Deserialize, Serialize};

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

#[tokio::test(flavor = "current_thread")]
async fn smoketest_arrow_ipc_round_trip_is_byte_exact() {
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
    let runtime = WasmPipelineRuntime::from_bytes(
        engine,
        "smoketest",
        &wasm_bytes,
        HashMap::new(),
        // 60 epoch ticks * 100 ms = 6 s — plenty for an identity round-trip.
        60,
    )
    .expect("WasmPipelineRuntime::from_bytes");

    // Surface the descriptor and capture the schema fingerprint the guest
    // computed for itself. We'll cross-check against the host's local
    // SchemaRegistry fingerprint after registering Ping.
    let descriptor = runtime.describe().expect("guest describe");
    assert_eq!(descriptor.name, "smoketest");
    assert_eq!(
        descriptor.components.len(),
        1,
        "smoketest should declare exactly one component (Ping)"
    );
    assert_eq!(descriptor.components[0].name, "Ping");
    assert!(
        !descriptor.components[0].arrow_schema_ipc.is_empty(),
        "guest must emit non-empty Arrow IPC schema bytes for Ping"
    );

    // Build a host-side dataset with the matching Ping schema and a few rows.
    let mut dataset = Dataset::new();
    dataset
        .register_component::<Ping>()
        .expect("register Ping host-side");
    let rows: Vec<Ping> = (0..16).map(|i| Ping { seq: i as u64 }).collect();
    dataset.append::<Ping>(&rows).expect("append Ping rows");

    // Cross-check: host fingerprint must equal guest fingerprint as a hex
    // string. If this fails, something has drifted in `SchemaRegistry::fingerprint`
    // OR the Schema definitions don't match between sides.
    let host_fingerprint_hex = format!("{:08x}", dataset.schemas().fingerprint());
    assert_eq!(
        descriptor.schema_fingerprint, host_fingerprint_hex,
        "guest schema fingerprint ({}) must match host fingerprint ({})",
        descriptor.schema_fingerprint, host_fingerprint_hex
    );

    // BEFORE snapshot of the dataset Arrow IPC.
    let mut before: Vec<u8> = Vec::new();
    dataset.write_ipc(&mut before).expect("write_ipc before");
    let before_rows = dataset.rows();

    // Drive the round-trip. Smoketest has zero systems → identity pipeline.
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
