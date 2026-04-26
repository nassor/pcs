//! Minimal guest pipeline used as a build fixture and Arrow IPC round-trip fixture.
//!
//! This crate is intentionally trivial: one component, no systems, an empty
//! pipeline. Its only job is to exercise `pcs_guest::export_pipeline!` against
//! cargo-component-generated bindings and produce a valid WebAssembly
//! component.
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

/// Construct the smoketest pipeline. Registers `Ping` and adds no systems —
/// `run-batch` becomes an identity function that echoes input IPC back out.
pub fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("smoketest");
    pipeline
        .data
        .register_component::<Ping>()
        .expect("register Ping");
    pipeline
}

// The macro invocation references `crate::bindings`, which is only generated
// by cargo-component when building for wasm32. On the host target the bindings
// module doesn't exist, so we gate the invocation out entirely. This keeps
// `cargo check --workspace` green without excluding the crate.
#[cfg(target_arch = "wasm32")]
pcs_guest::export_pipeline!(build);
