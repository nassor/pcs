//! `pcs-processor`: processor SDK for PCS WebAssembly Component Model pipelines.
//!
//! This crate owns the `pcs:pipeline@0.3.0` WIT package (`wit/pipeline.wit`).
//! Processor crates that export a PCS pipeline as a WebAssembly component point
//! their `wit_bindgen::generate!` at this crate's `wit/` directory.
//!
//! # Authoring a processor pipeline
//!
//! Write a cdylib crate that depends on `pcs-processor`. A processor is one
//! declared row struct plus one function per transform; the macros generate the
//! WIT surface, the schema, the fingerprint, the IPC decode/encode, the config
//! reads, the metrics and log bridges, the error mapping and the checkpoint
//! state.
//!
//! ```ignore
//! use pcs_processor::prelude::*;
//!
//! /// One order.
//! #[derive(Component, serde::Serialize, serde::Deserialize)]
//! pub struct Order {
//!     /// Row identity.
//!     pub id: i64,
//!     /// Settlement outcome this processor writes.
//!     pub settlement: String,
//! }
//!
//! /// Settle one row.
//! #[transform(component = Order)]
//! pub fn settle(row: &mut Order) -> pcs_processor::Result<()> {
//!     row.settlement = "SETTLED".to_string();
//!     Ok(())
//! }
//!
//! /// Build the pipeline.
//! #[processor(name = "my-pipeline")]
//! pub fn build() -> Pipeline {
//!     Pipeline::builder("my-pipeline")
//!         .with::<Order>()
//!         .with_system(settle_system())
//!         .build()
//! }
//! ```
//!
//! `cargo build --target wasm32-wasip2` on that crate produces a
//! WebAssembly component implementing the `pcs-pipeline` world. The host loads
//! it via `wasmtime` and drives the two processor exports, `describe` and
//! `run-batch`. Nothing in the source above is target-gated: every
//! wasm32-only item lives inside the [`processor`] expansion.
//!
//! [`export_pipeline!`] is the same wiring as a `macro_rules!` macro, for a
//! crate that hand-writes its own `bindings` module and `System` impls.
//!
//! # Config
//!
//! Values from the `config` node inside the service config's `wasm` node are read through
//! the `pcs:pipeline/host-io` `get-config` import, which the host answers on
//! every call including `describe`. [`processor`] and [`export_pipeline!`] emit
//! `pcs_config_get` and `pcs_config_parse` into the processor crate, because WIT
//! bindings are caller-side. A [`transform`] taking a second `&`[`Config`]
//! parameter reads them typed, with a default for an absent key.
//!
//! # State across batches
//!
//! The host creates a fresh wasmtime `Store` per call, so processor linear memory
//! never survives a batch boundary. A processor that needs state declares exactly
//! one state component:
//!
//! ```ignore
//! #[processor(name = "my-pipeline", state = Counter)]
//! pub fn build() -> Pipeline { /* ... */ }
//! ```
//!
//! The macro decodes the previous batch's rows from `run-batch`'s `prior`
//! argument into a [`ProcessorState`]`<Counter>` resource on the batch dataset,
//! runs the pipeline, then serialises whatever the systems left there into
//! `run-result.checkpoint`. The host persists that blob verbatim and hands it
//! back as the next `prior`. A [`fold`] reaches that state through
//! [`ProcessorState::get_or_insert_default`].
//!
//! State is a resource, not a registered component, because `Dataset`'s Arrow
//! IPC format requires every component to hold exactly the dataset's row count
//! while state rows are independent of batch rows. State therefore never
//! appears in the processor's output IPC and cannot be double-applied. A processor
//! needing more fields widens that one component's schema.
//!
//! # Error handling in user systems
//!
//! The macro-generated `run-batch` converts [`PcsError`] variants bubbling out
//! of `Pipeline::run_on_with_stats` into the WIT `run-error` variant:
//!
//! | `PcsError` variant                                                   | WIT `run-error`       |
//! |----------------------------------------------------------------------|-----------------------|
//! | `RetryExhausted`, `SystemExecution`                                  | `retryable(string)`   |
//! | `ComponentNotFound`, `ResourceNotFound`, `EntityNotFound`,           |                       |
//! | `Configuration`, `Scheduler`, `Store`, `Generic`                     | `permanent(string)`   |
//!
//! `schema-mismatch` is never emitted from `run-batch`.
//!
//! An [`Error`] returned by a [`transform`] or [`fold`] function becomes a
//! `PcsError::SystemExecution`, so a failing row or batch releases the claim
//! and retries on the next tick.
//!
//! System authors should construct errors explicitly instead of `.unwrap()` or
//! `panic!()` inside `System::run`. A panic becomes a WebAssembly trap, the
//! host surfaces it as `permanent`, and the operator loses the batch.

#![deny(missing_docs)]

pub use pcs_core::{
    Component, Dataset, PcsError, PcsResult, Pipeline, PipelineBuilder, RetryMode, Row, RunStats,
    SchemaRegistry, System, SystemConfig, SystemMeta, WriteSet, system_fn,
};

mod config;
mod error;

pub use config::Config;
pub use error::{Error, Result};

/// The authoring macros. See each one's own documentation.
///
/// Re-exported from `pcs-macros`, which a processor crate never names
/// directly. [`Component`](macro@Component) shares its name with the
/// [`Component`](trait@Component) trait: derives live in the macro namespace,
/// traits in the type namespace, so both are in scope at once.
pub use pcs_macros::{Component, fold, processor, transform};

// Arrow sub-crates processor authors need to define `Component` schemas,
// re-surfaced at the workspace-pinned `=59.2.0` version so user crates do not
// depend on them directly. `serde` is not re-exported: `#[derive(Serialize)]`
// expansions reference the literal `::serde` path at the call site, so processor
// authors add `serde` as a direct dep of their own crate.
pub use arrow_array;
pub use arrow_schema;

/// The windowing primitives (`WindowSpec`, `WatermarkState`,
/// `WindowedSystemBuilder`, ...), behind the `windows` feature.
///
/// A processor that implements Beam-style windowing over the merged rows it
/// receives keeps its open windows in its checkpoint state and uses these
/// primitives for the window maths; the host tracks the node's watermark
/// separately and reports it as `pcs_window_watermark_seconds`.
#[cfg(feature = "windows")]
pub use pcs_core::windows;

/// A curated prelude for processor pipeline crates.
///
/// `use pcs_processor::prelude::*;` imports the most common types for building a
/// pipeline: [`Component`](trait@Component), [`Dataset`], [`Pipeline`],
/// [`System`], the error + metadata types needed to define systems, and the
/// four authoring macros.
pub mod prelude {
    pub use pcs_core::prelude::*;

    pub use crate::{Config, Error, Result};
    pub use pcs_macros::{Component, fold, processor, transform};
}

/// Runtime glue referenced by [`export_pipeline!`] and by the `pcs-macros`
/// expansions.
///
/// Not public API. Its contents are stable only within a single `pcs-processor`
/// version, and the macros are the only legitimate consumers.
#[doc(hidden)]
pub mod __rt {
    pub use pcs_core::{
        Component, Dataset, PcsError, PcsResult, Pipeline, RunStats, System, SystemMeta,
    };
    pub use pollster;
    pub use std::sync::{Mutex, MutexGuard, OnceLock};

    pub use arrow_ipc::writer::StreamWriter;
    pub use arrow_schema::Schema;

    // `#[async_trait]`, so a generated `System` impl does not force the
    // processor crate to depend on async-trait itself.
    pub use pcs_core::prelude::async_trait;

    // `#[derive(Component)]`'s generated `schema()` traces the Arrow fields
    // from the type. The derive expands in the processor crate, which has no
    // serde_arrow dependency of its own.
    pub use serde_arrow;

    // The boundary-agnostic half of the glue lives in `pcs_core::sdk`, so the
    // native plugin SDK reuses the same state, error and schema handling
    // without depending on this crate and its wasm-oriented pins.
    pub use pcs_core::sdk::RouteDecision;
    pub use pcs_core::sdk::{
        NoState, PipelineSlot, ProcessorState, ProcessorStateSpec, Stateful, classify_run_error,
        fingerprint_hex, schema_to_ipc_bytes,
    };
}

pub use pcs_core::sdk::ProcessorState;
pub use pcs_core::sdk::RouteDecision;

/// Wire a user-authored pipeline to the `pcs-pipeline` WIT world exports.
///
/// The first argument names a `fn() -> Pipeline` that constructs the pipeline.
/// The optional `state = T` argument names the one [`Component`](trait@Component) whose rows
/// survive across `run-batch` calls. The macro generates:
///
/// - a hidden static holding the lazily built [`Pipeline`],
/// - an `impl Processor for __PcsComponent` block covering the two WIT exports
///   (`describe`, `run-batch`),
/// - `pcs_config_get` / `pcs_config_parse` free functions in the calling crate,
/// - the `crate::bindings::export!` handshake `wit-bindgen` requires.
///
/// # Requirements
///
/// - Build the downstream crate with `cargo build --target wasm32-wasip2`; the
///   expansion targets its `crate::bindings` module.
/// - That module must be a `wit_bindgen::generate!` against
///   `crates/pcs-processor/wit/`, which defines world `pcs-pipeline` of
///   `pcs:pipeline@0.3.0`:
///
/// ```ignore
/// #[cfg(target_arch = "wasm32")]
/// #[allow(warnings)]
/// mod bindings {
///     wit_bindgen::generate!({
///         path: "../path/to/crates/pcs-processor/wit",
///         world: "pcs-pipeline",
///         generate_all,
///     });
/// }
/// ```
///
/// - A `state = T` processor must register `T` in `build()`, so `describe()`
///   declares it and the host's template dataset carries it.
///
/// # Example
///
/// ```ignore
/// use pcs_processor::prelude::*;
///
/// fn build() -> Pipeline {
///     Pipeline::new("demo")
/// }
///
/// pcs_processor::export_pipeline!(build);
/// ```
///
/// See the crate documentation for the error-handling contract.
// `clippy::crate_in_macro_def` is allowed because the `crate::bindings`
// references inside the expansion must resolve to the caller's crate, not to
// pcs-processor. `$crate::bindings` would point at pcs-processor and break. The same
// reason forces `pcs_config_get` to be emitted here rather than in pcs-processor.
#[allow(clippy::crate_in_macro_def)]
#[macro_export]
macro_rules! export_pipeline {
    ($build_fn:ident) => {
        $crate::export_pipeline!(@impl $build_fn, $crate::__rt::NoState);
    };
    ($build_fn:ident, state = $state:ty) => {
        $crate::export_pipeline!(@impl $build_fn, $crate::__rt::Stateful<$state>);
    };
    (@impl $build_fn:ident, $spec:ty) => {
        /// Read a `[pipeline.wasm.config]` value injected by the host.
        ///
        /// Backed by the `pcs:pipeline/host-io` `get-config` import, so the
        /// value is available in every export including `describe`. Values are
        /// strings; parse numerics yourself or use `pcs_config_parse`.
        #[cfg(target_arch = "wasm32")]
        pub fn pcs_config_get(key: &str) -> Option<String> {
            crate::bindings::pcs::pipeline::host_io::get_config(key)
        }

        /// Read and parse a `[pipeline.wasm.config]` value.
        ///
        /// `None` means the key was absent; `Some(Err(_))` means it was present
        /// but did not parse as `T`.
        #[cfg(target_arch = "wasm32")]
        pub fn pcs_config_parse<T: core::str::FromStr>(key: &str) -> Option<Result<T, T::Err>> {
            pcs_config_get(key).map(|v| v.parse::<T>())
        }

        const _: () = {
            use $crate::__rt::ProcessorStateSpec as _;

            static __PCS_PIPELINE: $crate::__rt::PipelineSlot = $crate::__rt::PipelineSlot::new();

            struct __PcsComponent;

            impl crate::bindings::exports::pcs::pipeline::pipeline::Guest for __PcsComponent {
                fn describe()
                -> crate::bindings::exports::pcs::pipeline::pipeline::PipelineDescriptor {
                    // `PipelineDescriptor` is re-exported inside the exports
                    // module because `interface pipeline` uses it directly.
                    // `ComponentDescriptor` is only reached transitively, so it
                    // comes from the package-level `types` module.
                    use crate::bindings::exports::pcs::pipeline::pipeline::PipelineDescriptor;
                    use crate::bindings::pcs::pipeline::types::ComponentDescriptor;

                    let pipeline = __PCS_PIPELINE.pipeline($build_fn);
                    let registry = pipeline.data.schemas();

                    // Sorted by component name, so the WIT
                    // `components: list<component-descriptor>` order is
                    // deterministic across runs and across host and processor.
                    let mut entries: Vec<(&'static str, std::sync::Arc<$crate::__rt::Schema>)> =
                        registry
                            .iter()
                            .map(|(name, entry)| (*name, entry.schema.clone()))
                            .collect();
                    entries.sort_by_key(|(name, _)| *name);

                    let components: Vec<ComponentDescriptor> = entries
                        .into_iter()
                        .map(|(name, schema)| {
                            // On a schema-to-IPC failure, emit an empty
                            // descriptor rather than trapping: the host's
                            // load-time validation rejects zero-length schema
                            // bytes with a clean error.
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
                        stateful: <$spec>::STATEFUL,
                        schema_fingerprint: fingerprint,
                    }
                }

                fn run_batch(
                    input: Vec<u8>,
                    prior: Option<Vec<u8>>,
                ) -> Result<
                    crate::bindings::exports::pcs::pipeline::pipeline::RunResult,
                    crate::bindings::exports::pcs::pipeline::pipeline::RunError,
                > {
                    // `RunResult` / `RunError` are re-exported in the exports
                    // module; `RunMetrics` is only reached transitively, so it
                    // comes from the package-level `types` module.
                    use crate::bindings::exports::pcs::pipeline::pipeline::{RunError, RunResult};
                    use crate::bindings::pcs::pipeline::types::RunMetrics;

                    let start = std::time::Instant::now();

                    let mut reader: &[u8] = &input[..];
                    let mut dataset = $crate::__rt::Dataset::read_ipc(&mut reader)
                        .map_err(|e| RunError::Permanent(format!("ipc decode: {e}")))?;

                    // Measured before the state blob is merged, so `rows_in`
                    // reports the data plane only.
                    let rows_in = dataset.rows() as u64;

                    <$spec>::restore(&mut dataset, prior.as_deref())
                        .map_err(|e| RunError::Permanent(format!("state restore: {e}")))?;

                    let pipeline = __PCS_PIPELINE.pipeline($build_fn);
                    let run_result = $crate::__rt::pollster::block_on(
                        pipeline.run_on_with_stats(&mut dataset),
                    );

                    let stats = match run_result {
                        Ok(stats) => stats,
                        Err(err) => {
                            let (is_retryable, msg) = $crate::__rt::classify_run_error(&err);
                            return Err(if is_retryable {
                                RunError::Retryable(msg)
                            } else {
                                RunError::Permanent(msg)
                            });
                        }
                    };

                    let rows_out = dataset.rows() as u64;

                    let checkpoint = <$spec>::capture(&dataset)
                        .map_err(|e| RunError::Permanent(format!("state capture: {e}")))?;
                    let routes: Option<Vec<String>> = dataset
                        .get_resource::<$crate::__rt::RouteDecision>()
                        .map(|decision| decision.0.clone());

                    let mut output: Vec<u8> = Vec::new();
                    dataset
                        .write_ipc(&mut output)
                        .map_err(|e| RunError::Permanent(format!("ipc encode: {e}")))?;

                    let metrics = RunMetrics {
                        wall_ns: start.elapsed().as_nanos() as u64,
                        rows_in,
                        rows_out,
                        systems_run: stats.systems_run as u32,
                        retries: stats.retries_this_batch,
                    };

                    Ok(RunResult {
                        output,
                        checkpoint,
                        metrics,
                        routes,
                    })
                }
            }

    crate::bindings::export!(__PcsComponent with_types_in crate::bindings);
        };
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use __rt::ProcessorStateSpec;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    /// State component used by the `Stateful` round-trip tests.
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
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

    /// A batch dataset whose row count deliberately differs from the state row
    /// count.
    fn batch_dataset() -> Dataset {
        let mut data = Dataset::new();
        data.register_component::<Counter>()
            .expect("register unrelated component");
        data
    }

    #[test]
    fn classify_run_error_retryable_variants() {
        let e = PcsError::retry_exhausted(PcsError::generic("x"), 3);
        let (is_retry, _msg) = __rt::classify_run_error(&e);
        assert!(is_retry, "RetryExhausted should be retryable");

        let e = PcsError::system_execution("oops");
        let (is_retry, _msg) = __rt::classify_run_error(&e);
        assert!(is_retry, "SystemExecution should be retryable");
    }

    #[test]
    fn classify_run_error_permanent_variants() {
        let e = PcsError::generic("unknown");
        let (is_retry, _msg) = __rt::classify_run_error(&e);
        assert!(!is_retry, "Generic must be permanent (dist-expert review)");

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
    fn no_state_is_declared_stateless_and_captures_nothing() {
        const { assert!(!__rt::NoState::STATEFUL) };
        let mut data = batch_dataset();
        __rt::NoState::restore(&mut data, Some(b"ignored")).expect("no-op restore");
        assert_eq!(
            __rt::NoState::capture(&data).expect("no-op capture"),
            None,
            "a stateless processor must return no checkpoint"
        );
        assert!(
            data.get_resource::<ProcessorState<Counter>>().is_none(),
            "a stateless processor must not install a state resource"
        );
    }

    #[test]
    fn stateful_round_trips_the_state_rows() {
        const { assert!(<__rt::Stateful<Counter>>::STATEFUL) };

        let mut source = batch_dataset();
        <__rt::Stateful<Counter>>::restore(&mut source, None).expect("cold-start restore");
        source
            .get_resource_mut::<ProcessorState<Counter>>()
            .expect("state resource installed")
            .rows
            .push(Counter { count: 41 });

        let blob = <__rt::Stateful<Counter>>::capture(&source)
            .expect("capture")
            .expect("stateful processor must produce a blob");

        let mut target = batch_dataset();
        <__rt::Stateful<Counter>>::restore(&mut target, Some(&blob)).expect("restore");
        let state = target
            .get_resource::<ProcessorState<Counter>>()
            .expect("state resource installed");
        assert_eq!(state.rows, vec![Counter { count: 41 }]);
    }

    #[test]
    fn state_is_absent_from_the_batch_output_ipc() {
        let mut data = batch_dataset();
        let mut without_state: Vec<u8> = Vec::new();
        data.write_ipc(&mut without_state).expect("write_ipc");

        <__rt::Stateful<Counter>>::restore(&mut data, None).expect("restore");
        data.get_resource_mut::<ProcessorState<Counter>>()
            .expect("state resource")
            .rows
            .push(Counter { count: 9 });

        let mut with_state: Vec<u8> = Vec::new();
        data.write_ipc(&mut with_state).expect("write_ipc");

        assert_eq!(
            without_state, with_state,
            "state lives in a resource, so it must not change the output IPC"
        );
    }

    #[test]
    fn stateful_restore_starts_empty_without_prior() {
        for prior in [None, Some(&[][..])] {
            let mut target = batch_dataset();
            <__rt::Stateful<Counter>>::restore(&mut target, prior).expect("cold-start restore");
            assert!(
                target
                    .get_resource::<ProcessorState<Counter>>()
                    .expect("state resource installed")
                    .is_empty(),
                "a missing or empty prior means the processor starts from scratch"
            );
        }
    }

    #[test]
    fn stateful_capture_without_restore_is_an_error() {
        let data = batch_dataset();
        let err = <__rt::Stateful<Counter>>::capture(&data)
            .expect_err("a missing state resource must be an error, not a silent None");
        let msg = err.to_string();
        assert!(msg.contains("Counter"), "got: {msg}");
    }

    #[test]
    fn stateful_capture_of_empty_state_round_trips_as_empty() {
        let mut data = batch_dataset();
        <__rt::Stateful<Counter>>::restore(&mut data, None).expect("restore");
        let blob = <__rt::Stateful<Counter>>::capture(&data)
            .expect("capture")
            .expect("blob");

        let mut next = batch_dataset();
        <__rt::Stateful<Counter>>::restore(&mut next, Some(&blob)).expect("restore");
        assert!(
            next.get_resource::<ProcessorState<Counter>>()
                .expect("state resource")
                .is_empty()
        );
    }
}
