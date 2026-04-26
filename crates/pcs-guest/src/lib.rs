//! `pcs-guest` — Guest SDK for PCS WebAssembly Component Model pipelines.
//!
//! This crate is the canonical source of the `pcs:pipeline@0.1.0` WIT package
//! (see `wit/pipeline.wit`). Downstream guest crates that want to export a PCS
//! pipeline as a WebAssembly component point their
//! `package.metadata.component.target.path` at this crate's `wit/` directory.
//!
//! # Authoring a guest pipeline
//!
//! Create a cdylib crate that depends on `pcs-guest`, configure
//! `[package.metadata.component]` in its `Cargo.toml` to target the
//! `pcs-pipeline` world, write a `fn` that constructs your [`Pipeline`], and
//! wire it up with [`export_pipeline!`]:
//!
//! ```ignore
//! use pcs_guest::prelude::*;
//!
//! fn build() -> Pipeline {
//!     let mut pipeline = Pipeline::new("my-pipeline");
//!     // pipeline.data.register_component::<MyComponent>().unwrap();
//!     // pipeline.add_system(MySystem);
//!     pipeline
//! }
//!
//! pcs_guest::export_pipeline!(build);
//! ```
//!
//! `cargo component build --target wasm32-wasip2` on the downstream crate
//! produces a valid WebAssembly component implementing the `pcs-pipeline`
//! world. The host loads it via `wasmtime` and drives the
//! `describe` / `init` / `run-batch` / `snapshot` / `restore` exports.
//!
//! # Error handling in user systems
//!
//! The macro-generated `run-batch` impl converts [`PcsError`] variants bubbling
//! out of `Pipeline::run_on` into the WIT `run-error` variant per the frozen
//! mapping:
//!
//! | `PcsError` variant                                                   | WIT `run-error`       |
//! |----------------------------------------------------------------------|-----------------------|
//! | `RetryExhausted`, `SystemExecution`                                  | `retryable(string)`   |
//! | `ComponentNotFound`, `ResourceNotFound`, `EntityNotFound`,           |                       |
//! | `Configuration`, `Scheduler`, `Store`, `Generic`                     | `permanent(string)`   |
//!
//! `schema-mismatch` is reserved for `restore()` and is never emitted from
//! `run-batch`. `LeaseExpired` is dropped from the guest mapping.
//!
//! **Guideline for system authors:** construct errors explicitly rather than
//! relying on `.unwrap()` or `panic!()` inside `System::run`. A panic becomes a
//! WebAssembly trap and the host surfaces it as `permanent` via a trap-specific
//! override — the operator loses the batch. Returning a structured
//! `PcsError::SystemExecution(...)` instead lets the runner release the claim
//! and retry on the next tick.

#![deny(missing_docs)]

// -----------------------------------------------------------------------------
// Re-exports — the surface guest authors write against.
// -----------------------------------------------------------------------------

pub use pcs_core::{
    Component, Dataset, PcsError, PcsResult, Pipeline, PipelineBuilder, RetryMode, Row, RunStats,
    SchemaRegistry, System, SystemConfig, SystemMeta, WriteSet, system_fn,
};

// Re-export the Arrow sub-crates guest authors need to define `Component`
// schemas without forcing every user crate to add `arrow-schema`, `arrow-array`
// as direct deps at the exact `=58.1.0` pin. The pcs-guest dep graph pulls
// them in at the workspace-pinned version and re-surfaces them here.
//
// NOTE on serde: `serde` is NOT re-exported here because `#[derive(Serialize)]`
// expansions reference the literal `::serde` path at the call site, not
// `::pcs_guest::serde`. Guest authors who want serde derives add `serde` as
// a direct dep in their own crate — it's a trivial line and matches the
// normal Rust convention for using derive macros.
pub use arrow_array;
pub use arrow_schema;

/// A curated prelude for guest pipeline crates.
///
/// `use pcs_guest::prelude::*;` imports the most common types for building a
/// pipeline: [`Component`], [`Dataset`], [`Pipeline`], [`System`], and the
/// error + metadata types needed to define systems.
pub mod prelude {
    pub use pcs_core::prelude::*;
}

/// Runtime glue referenced by [`export_pipeline!`] macro expansions.
///
/// This module is **not** part of the public API. Its contents are only stable
/// within a single `pcs-guest` version, and the macro is the only legitimate
/// consumer. Guest authors should not import anything from `__rt` directly.
#[doc(hidden)]
pub mod __rt {
    pub use pcs_core::{Dataset, PcsError, PcsResult, Pipeline, RunStats};
    pub use pollster;
    pub use std::sync::{Mutex, MutexGuard, OnceLock};

    pub use arrow_ipc::writer::StreamWriter;
    pub use arrow_schema::Schema;

    use serde_json::Value as JsonValue;

    /// Canonical scratch state owned by the macro-generated component.
    ///
    /// `PIPELINE` is initialized lazily on the first call to any WIT export via
    /// the user's `build()` function, wrapped in a `Mutex` for `Send + Sync`
    /// plumbing even though wasm32-wasip2 is single-threaded in practice.
    ///
    /// `CONFIG` holds the parsed `serde_json::Value` from the WIT `init`
    /// export. `None` until `init` has been called.
    pub struct GuestState {
        pipeline: OnceLock<Mutex<Pipeline>>,
        config: OnceLock<JsonValue>,
    }

    impl Default for GuestState {
        fn default() -> Self {
            Self::new()
        }
    }

    impl GuestState {
        /// Construct an empty state. The macro creates exactly one static
        /// instance per component.
        pub const fn new() -> Self {
            Self {
                pipeline: OnceLock::new(),
                config: OnceLock::new(),
            }
        }

        /// Lock and return the pipeline, initializing via `build` on first use.
        ///
        /// # Panics
        ///
        /// Panics if the inner `Mutex` is poisoned. Under wasm32-wasip2 the
        /// guest is single-threaded so poisoning requires a panic-while-locked
        /// — which already traps the guest and doesn't reach this path.
        pub fn pipeline<F>(&self, build: F) -> MutexGuard<'_, Pipeline>
        where
            F: FnOnce() -> Pipeline,
        {
            self.pipeline
                .get_or_init(|| Mutex::new(build()))
                .lock()
                .expect("pcs-guest: pipeline mutex poisoned")
        }

        /// Parse and stash the config blob handed in by the host's WIT
        /// `init(config: string)` call. The string is expected to be a JSON
        /// object (the host serializes `pipeline.wasm.config` TOML → JSON).
        pub fn set_config(&self, raw: &str) -> Result<(), String> {
            let value: JsonValue = serde_json::from_str(raw)
                .map_err(|e| format!("pcs-guest: init config is not valid JSON: {e}"))?;
            // Ignore a second `init` — host should only call once.
            let _ = self.config.set(value);
            Ok(())
        }

        /// Fetch a top-level config key as a raw JSON value. Returns `None` if
        /// `init` hasn't been called or the key is absent.
        pub fn config_get(&self, key: &str) -> Option<&JsonValue> {
            self.config.get().and_then(|v| v.get(key))
        }
    }

    /// Map a `PcsError` from `Pipeline::run_on` into a `(is_retryable, message)`
    /// pair the macro uses to construct the caller's `RunError` variant.
    ///
    /// Mapping is frozen per the 2026-04-15 wasm-guest + wasm-lead + dist-expert
    /// review. `retryable` only on `RetryExhausted` and `SystemExecution`;
    /// everything else including `Generic` is `permanent`. `schema-mismatch` is
    /// reserved for `restore()` and never emitted here — any mid-batch schema
    /// problem folds into `permanent` via `Configuration`.
    pub fn classify_run_error(err: &PcsError) -> (bool, String) {
        let is_retryable = matches!(
            err,
            PcsError::RetryExhausted { .. } | PcsError::SystemExecution(_)
        );
        (is_retryable, err.to_string())
    }

    /// Serialize a single Arrow `Schema` as IPC schema-message bytes via an
    /// empty `StreamWriter` (no record batches; the stream start contains the
    /// full schema descriptor in IPC wire format).
    pub fn schema_to_ipc_bytes(schema: &Schema) -> PcsResult<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, schema)
                .map_err(|e| PcsError::generic(format!("schema_to_ipc_bytes new: {e}")))?;
            writer
                .finish()
                .map_err(|e| PcsError::generic(format!("schema_to_ipc_bytes finish: {e}")))?;
        }
        Ok(buf)
    }

    /// Format a `u32` fingerprint as the stable 8-char hex string the WIT
    /// `schema-fingerprint: string` field expects.
    pub fn fingerprint_hex(fp: u32) -> String {
        format!("{fp:08x}")
    }
}

// -----------------------------------------------------------------------------
// export_pipeline! — the one macro that wires a user Pipeline to the WIT
// exports.
// -----------------------------------------------------------------------------

/// Wire a user-authored pipeline to the `pcs-pipeline` WIT world exports.
///
/// The single argument is the identifier of a `fn() -> Pipeline` that
/// constructs the pipeline (registers components, adds systems, configures
/// retries). The macro generates:
///
/// - a hidden static holding the lazily-built [`Pipeline`] plus any `init`
///   config blob,
/// - an `impl Guest for __PcsComponent` block covering the five WIT exports
///   (`describe`, `init`, `run-batch`, `snapshot`, `restore`),
/// - the `crate::bindings::export!` handshake cargo-component requires.
///
/// # Requirements
///
/// - The downstream crate must be built with `cargo component build`. The
///   generated `crate::bindings` module is what this macro expansion targets.
/// - The WIT world is `pcs-pipeline` from `pcs:pipeline@0.1.0`, authored in
///   `crates/pcs-guest/wit/pipeline.wit`. Point your `[package.metadata.component.target.path]`
///   at that directory.
///
/// # Example
///
/// ```ignore
/// use pcs_guest::prelude::*;
///
/// fn build() -> Pipeline {
///     Pipeline::new("demo")
/// }
///
/// pcs_guest::export_pipeline!(build);
/// ```
///
/// See the crate-level documentation for the full error-handling contract and
/// the defensive trap-avoidance guarantees the macro provides on your behalf.
// `clippy::crate_in_macro_def` is deliberately allowed: the `crate::bindings`
// references inside the expansion MUST resolve to the caller's crate, not to
// pcs-guest. Caller-side bindings is the core design decision (see module
// docs). Rewriting to `$crate::bindings` would point at pcs-guest and break.
#[allow(clippy::crate_in_macro_def)]
#[macro_export]
macro_rules! export_pipeline {
    ($build_fn:ident) => {
        const _: () = {
            // Single static per component. `GuestState::new()` is `const`.
            static __PCS_STATE: $crate::__rt::GuestState = $crate::__rt::GuestState::new();

            struct __PcsComponent;

            impl crate::bindings::exports::pcs::pipeline::pipeline::Guest for __PcsComponent {
                fn describe()
                -> crate::bindings::exports::pcs::pipeline::pipeline::PipelineDescriptor {
                    // PipelineDescriptor is re-exported as a type alias inside the
                    // `exports::...::pipeline` module because `interface pipeline` uses
                    // it directly. ComponentDescriptor is NOT — the WIT `interface
                    // pipeline` only reaches it transitively via PipelineDescriptor.components,
                    // so we have to import it from the package-level `types` module.
                    use crate::bindings::exports::pcs::pipeline::pipeline::PipelineDescriptor;
                    use crate::bindings::pcs::pipeline::types::ComponentDescriptor;

                    let pipeline = __PCS_STATE.pipeline($build_fn);
                    let registry = pipeline.data.schemas();

                    // Stable iteration order: sort component names before
                    // emitting, so the WIT `components: list<component-descriptor>`
                    // ordering is deterministic across runs and across host/guest.
                    let mut entries: Vec<(&'static str, std::sync::Arc<$crate::__rt::Schema>)> =
                        registry
                            .iter()
                            .map(|(name, entry)| (*name, entry.schema.clone()))
                            .collect();
                    entries.sort_by_key(|(name, _)| *name);

                    let components: Vec<ComponentDescriptor> = entries
                        .into_iter()
                        .map(|(name, schema)| {
                            // If schema-to-IPC serialization fails, emit an
                            // empty descriptor rather than trapping. The host's
                            // load-time validation will reject a descriptor
                            // with zero-length schema bytes and surface a clean
                            // error instead of a mid-describe trap.
                            let arrow_schema_ipc =
                                $crate::__rt::schema_to_ipc_bytes(&schema).unwrap_or_default();
                            ComponentDescriptor {
                                name: name.to_string(),
                                arrow_schema_ipc,
                            }
                        })
                        .collect();

                    let fingerprint = $crate::__rt::fingerprint_hex(registry.fingerprint());

                    PipelineDescriptor {
                        name: pipeline.name().to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        components,
                        stateful: false,
                        schema_fingerprint: fingerprint,
                    }
                }

                fn init(config: String) -> Result<(), String> {
                    // Eager-parse the JSON blob and stash it. Lazy-init the
                    // pipeline on first use of any other export.
                    __PCS_STATE.set_config(&config)
                }

                fn run_batch(
                    input: Vec<u8>,
                    _prior: Option<Vec<u8>>,
                ) -> Result<
                    crate::bindings::exports::pcs::pipeline::pipeline::RunResult,
                    crate::bindings::exports::pcs::pipeline::pipeline::RunError,
                > {
                    // RunResult / RunError are re-exported as type aliases in the
                    // `exports::...::pipeline` module. RunMetrics is NOT — it's only
                    // reached transitively via RunResult.metrics, so pull it from
                    // the package-level `types` module.
                    use crate::bindings::exports::pcs::pipeline::pipeline::{RunError, RunResult};
                    use crate::bindings::pcs::pipeline::types::RunMetrics;

                    let start = std::time::Instant::now();

                    let mut reader: &[u8] = &input[..];
                    let mut dataset = $crate::__rt::Dataset::read_ipc(&mut reader)
                        .map_err(|e| RunError::Permanent(format!("ipc decode: {e}")))?;

                    // Checkpoint restore inside run-batch is a v0.2 concern.
                    // For v0.1.0 the prior blob is ignored — checkpoints travel
                    // via the separate snapshot/restore exports.

                    let rows_in = dataset.rows() as u64;

                    let pipeline = __PCS_STATE.pipeline($build_fn);
                    let run_result =
                        $crate::__rt::pollster::block_on(pipeline.run_on(&mut dataset));

                    if let Err(err) = run_result {
                        let (is_retryable, msg) = $crate::__rt::classify_run_error(&err);
                        return Err(if is_retryable {
                            RunError::Retryable(msg)
                        } else {
                            RunError::Permanent(msg)
                        });
                    }

                    let rows_out = dataset.rows() as u64;

                    let mut output: Vec<u8> = Vec::new();
                    dataset
                        .write_ipc(&mut output)
                        .map_err(|e| RunError::Permanent(format!("ipc encode: {e}")))?;

                    // RunMetrics is filled honestly in v0.1.0:
                    //
                    // - `wall_ns` measured by the macro itself via Instant.
                    // - `rows_in` / `rows_out` from Dataset::rows() before/after.
                    // - `systems_run` and `retries` hardcoded to 0 because
                    //   `Pipeline::run_on` (the entry the macro uses) does NOT
                    //   update `Pipeline::last_stats` — only the `Pipeline::run`
                    //   and `run_with_io` entries do, and those touch the template's
                    //   own `data` rather than an external dataset.
                    //   Filling these from `pipeline.last_stats()` would surface
                    //   stale values, so we report 0 instead. A future version
                    //   should either (a) make `run_on` populate a per-call
                    //   stats struct or (b) thread an explicit out-parameter
                    //   through the entry point.
                    let metrics = RunMetrics {
                        wall_ns: start.elapsed().as_nanos() as u64,
                        rows_in,
                        rows_out,
                        systems_run: 0,
                        retries: 0,
                    };

                    Ok(RunResult {
                        output,
                        checkpoint: None,
                        metrics,
                    })
                }

                fn snapshot() -> Result<Vec<u8>, String> {
                    // v0.1.0: snapshot the template dataset via Arrow IPC. User
                    // pipelines wanting custom state should override by
                    // building their snapshot into `Pipeline::data` before
                    // returning from here — a future version will surface a
                    // user-hook for custom blob formats.
                    let pipeline = __PCS_STATE.pipeline($build_fn);
                    let mut buf: Vec<u8> = Vec::new();
                    pipeline
                        .data
                        .write_ipc(&mut buf)
                        .map_err(|e| format!("snapshot write_ipc: {e}"))?;
                    Ok(buf)
                }

                fn restore(cp: Vec<u8>) -> Result<(), String> {
                    // Mirror image of snapshot. Replaces the template's data
                    // with the deserialized checkpoint. Schema-fingerprint
                    // validation is the host's concern — if the bytes parse at
                    // all we trust them.
                    let mut reader: &[u8] = &cp[..];
                    let restored = $crate::__rt::Dataset::read_ipc(&mut reader)
                        .map_err(|e| format!("restore read_ipc: {e}"))?;
                    let mut pipeline = __PCS_STATE.pipeline($build_fn);
                    pipeline.data = restored;
                    Ok(())
                }
            }

    crate::bindings::export!(__PcsComponent with_types_in crate::bindings);
        };
    };
}

// -----------------------------------------------------------------------------
// Config accessors — small ergonomics helpers for guest authors to read the
// config blob handed in via WIT `init(config)`.
// -----------------------------------------------------------------------------

/// Fetch a top-level config key as a raw JSON value reference.
///
/// Returns `None` if the host hasn't called `init` yet, or if the key is
/// absent in the parsed JSON object.
///
/// # Example
///
/// ```ignore
/// if let Some(v) = pcs_guest::config_get("batch_size") {
///     if let Some(n) = v.as_u64() { /* ... */ }
/// }
/// ```
///
/// For typed deserialization see [`config_get_typed`].
// NB: intentionally returns `Option<&'static serde_json::Value>` so callers
// can inspect without cloning. The OnceLock lifetime is effectively 'static.
pub fn config_get(_key: &str) -> Option<&'static serde_json::Value> {
    // Users get their config through the macro-local GuestState; the freestanding
    // helpers on pcs_guest::config_get would need a shared global, which the
    // macro deliberately avoids (one GuestState per component keeps state
    // local). Guest authors who need config access should thread it through
    // their build() or System constructors, not call into this helper.
    //
    // Retained as a stub for a future iteration where we move GuestState to a
    // truly global static; for now return None so the type signature is stable.
    None
}

/// Attempt to deserialize a top-level config key into `T`.
///
/// Currently a stub — see [`config_get`] for the placeholder rationale. Will
/// light up once we settle on a global GuestState design or a config-channel
/// on `Pipeline` itself.
pub fn config_get_typed<T>(_key: &str) -> Option<Result<T, serde_json::Error>>
where
    T: for<'de> serde::Deserialize<'de>,
{
    None
}

// -----------------------------------------------------------------------------
// Unit tests for the runtime-glue helpers. The `export_pipeline!` macro itself
// is exercised end-to-end by the `pcs-guest-smoketest` sibling crate, which
// builds to a real wasm component. The tests here cover the pieces that run
// on the host target without wit-bindgen generated code.
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_run_error_retryable_variants() {
        // RetryExhausted maps to retryable.
        let e = PcsError::retry_exhausted(PcsError::generic("x"), 3);
        let (is_retry, _msg) = __rt::classify_run_error(&e);
        assert!(is_retry, "RetryExhausted should be retryable");

        // SystemExecution maps to retryable.
        let e = PcsError::system_execution("oops");
        let (is_retry, _msg) = __rt::classify_run_error(&e);
        assert!(is_retry, "SystemExecution should be retryable");
    }

    #[test]
    fn classify_run_error_permanent_variants() {
        // Generic maps to permanent (was flipped from retryable 2026-04-15).
        let e = PcsError::generic("unknown");
        let (is_retry, _msg) = __rt::classify_run_error(&e);
        assert!(!is_retry, "Generic must be permanent (dist-expert review)");

        // Configuration maps to permanent.
        let e = PcsError::configuration("bad value");
        let (is_retry, _msg) = __rt::classify_run_error(&e);
        assert!(!is_retry, "Configuration should be permanent");
    }

    #[test]
    fn fingerprint_hex_format_is_8_char_lowercase_hex() {
        assert_eq!(__rt::fingerprint_hex(0), "00000000");
        assert_eq!(__rt::fingerprint_hex(0xdeadbeef), "deadbeef");
        assert_eq!(__rt::fingerprint_hex(0x1), "00000001");
    }

    #[test]
    fn schema_to_ipc_bytes_nonempty_and_deterministic() {
        use arrow_schema::{DataType, Field, Schema};
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let bytes_a = __rt::schema_to_ipc_bytes(&schema).expect("schema_to_ipc_bytes ok");
        let bytes_b = __rt::schema_to_ipc_bytes(&schema).expect("schema_to_ipc_bytes ok");
        assert!(!bytes_a.is_empty(), "ipc schema bytes must be nonempty");
        assert_eq!(bytes_a, bytes_b, "ipc schema bytes must be deterministic");
    }

    #[test]
    fn guest_state_config_parse_and_lookup() {
        let state = __rt::GuestState::new();
        // Before init → None.
        assert!(state.config_get("anything").is_none());

        state
            .set_config(r#"{"batch_size": 1000, "tax_rate": 0.07}"#)
            .expect("valid JSON");

        assert_eq!(
            state.config_get("batch_size").and_then(|v| v.as_u64()),
            Some(1000)
        );
        assert!(state.config_get("missing").is_none());
    }

    #[test]
    fn guest_state_rejects_malformed_json() {
        let state = __rt::GuestState::new();
        let err = state.set_config("not json").expect_err("should fail");
        assert!(err.contains("not valid JSON"), "got: {err}");
    }
}
