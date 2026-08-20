//! Minimal guest pipeline used as a build fixture and Arrow IPC round-trip fixture.
//!
//! This crate is intentionally trivial. It declares one data component and one
//! system, and exists to exercise `pcs_guest::export_pipeline!` against
//! cargo-component-generated bindings and produce a valid WebAssembly
//! component:
//!
//! - `Ping` is the data plane. No system touches it, so `run-batch` is an
//!   identity function and any byte difference in the round-tripped Arrow IPC
//!   indicates `arrow-ipc` drift between host and guest.
//! - `Counter` is the guest's cross-batch state, declared via
//!   `export_pipeline!(build, state = Counter)`. It lives in a
//!   `GuestState<Counter>` resource — not a registered component — so it never
//!   appears in the output IPC. `count_batches` increments it once per
//!   `run-batch`, so a host threading `run-result.checkpoint` back in as the
//!   next `prior` observes 1, 2, 3, ….
//! - `build()` reads the `greeting` config key through the host-io `get-config`
//!   import and appends it to the pipeline name, so `describe()` proves config
//!   reached the guest.
//!
//! On the host target the `export_pipeline!` invocation is gated out so the
//! crate compiles as an empty cdylib and `cargo check --workspace` stays
//! green. The WebAssembly build happens via `cargo component build -p
//! pcs-guest-smoketest --target wasm32-wasip2`.

#![deny(missing_docs)]

// cargo-component generates `src/bindings.rs` when building for wasm32-wasip2
// via `cargo component build`. The file does NOT exist on the host target, so
// the module declaration is gated. `#[allow(warnings)]` silences bindgen
// output noise that we have no control over.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings;

use pcs_guest::GuestState;
use pcs_guest::arrow_schema::{DataType, Field, Schema};
use pcs_guest::prelude::*;
use std::sync::Arc;

/// A single no-op component so the pipeline has at least one schema registered
/// for `describe()` to emit. Arrow schema is a single `u64` field; serde
/// round-trip is handled by `serde_arrow` via the default `Component` impl.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Ping {
    /// Monotonic sequence number — exists so the schema has a field.
    pub seq: u64,
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

/// The guest's state: one row holding the number of batches this logical
/// partition has processed.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Counter {
    /// Batches processed, including the current one.
    pub count: u64,
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

/// Increment the batch counter, seeding it on the first batch.
///
/// The state resource is installed by the macro before any system runs, so its
/// absence is a bug in the SDK rather than a recoverable condition.
fn count_batches(data: &mut Dataset) -> Result<(), PcsError> {
    let state = data
        .get_resource_mut::<GuestState<Counter>>()
        .ok_or_else(|| PcsError::generic("smoketest: GuestState<Counter> resource missing"))?;

    match state.rows.first_mut() {
        Some(counter) => counter.count += 1,
        None => state.rows.push(Counter { count: 1 }),
    }
    Ok(())
}

/// Construct the smoketest pipeline.
///
/// Registers `Ping` (the identity data plane) and adds the single
/// `count_batches` system. The pipeline name carries the `greeting` config
/// value when the host injected one, which is how the round-trip test observes
/// that `[pipeline.wasm.config]` reached the guest.
pub fn build() -> Pipeline {
    // `pcs_config_get` is emitted by `export_pipeline!` into this crate and
    // only exists on wasm32, where `crate::bindings` exists. On the host target
    // the fixture compiles without any config source.
    #[cfg(target_arch = "wasm32")]
    let greeting = pcs_config_get("greeting");
    #[cfg(not(target_arch = "wasm32"))]
    let greeting: Option<String> = None;

    let name = match greeting {
        Some(g) => format!("smoketest-{g}"),
        None => "smoketest".to_string(),
    };

    let mut pipeline = Pipeline::new(name);
    pipeline
        .data
        .register_component::<Ping>()
        .expect("register Ping");
    // `Counter` is deliberately NOT registered: state lives in a resource, so
    // it stays out of `describe()` and out of the output IPC.
    pipeline.add_system(system_fn(SystemMeta::new("count_batches"), count_batches));
    pipeline
}

// The macro invocation references `crate::bindings`, which is only generated
// by cargo-component when building for wasm32. On the host target the bindings
// module doesn't exist, so we gate the invocation out entirely. This keeps
// `cargo check --workspace` green without excluding the crate.
#[cfg(target_arch = "wasm32")]
pcs_guest::export_pipeline!(build, state = Counter);
