//! `pcs-guest`: guest SDK for PCS WebAssembly Component Model pipelines.
//!
//! This crate owns the `pcs:pipeline@0.2.0` WIT package (`wit/pipeline.wit`).
//! Guest crates that export a PCS pipeline as a WebAssembly component point
//! their `package.metadata.component.target.path` at this crate's `wit/`
//! directory.
//!
//! # Authoring a guest pipeline
//!
//! Write a cdylib crate that depends on `pcs-guest`, target the `pcs-pipeline`
//! world from `[package.metadata.component]` in its `Cargo.toml`, add a `fn`
//! that constructs a [`Pipeline`], and wire it up with [`export_pipeline!`]:
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
//! `cargo component build --target wasm32-wasip2` on that crate produces a
//! WebAssembly component implementing the `pcs-pipeline` world. The host loads
//! it via `wasmtime` and drives the two guest exports, `describe` and
//! `run-batch`.
//!
//! # Config
//!
//! Values from the service TOML `[pipeline.wasm.config]` table are read through
//! the `pcs:pipeline/host-io` `get-config` import, which the host answers on
//! every call including `describe`. [`export_pipeline!`] emits `pcs_config_get`
//! and `pcs_config_parse` into the guest crate, because WIT bindings are
//! caller-side.
//!
//! # State across batches
//!
//! The host creates a fresh wasmtime `Store` per call, so guest linear memory
//! never survives a batch boundary. A guest that needs state declares exactly
//! one state component:
//!
//! ```ignore
//! pcs_guest::export_pipeline!(build, state = Counter);
//! ```
//!
//! The macro decodes the previous batch's rows from `run-batch`'s `prior`
//! argument into a [`GuestState`]`<Counter>` resource on the batch dataset,
//! runs the pipeline, then serialises whatever the systems left there into
//! `run-result.checkpoint`. The host persists that blob verbatim and hands it
//! back as the next `prior`.
//!
//! State is a resource, not a registered component, because `Dataset`'s Arrow
//! IPC format requires every component to hold exactly the dataset's row count
//! while state rows are independent of batch rows. State therefore never
//! appears in the guest's output IPC and cannot be double-applied. A guest
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
//! System authors should construct errors explicitly instead of `.unwrap()` or
//! `panic!()` inside `System::run`. A panic becomes a WebAssembly trap, the
//! host surfaces it as `permanent`, and the operator loses the batch.
//! Returning a structured `PcsError::SystemExecution(...)` lets the runner
//! release the claim and retry on the next tick.

#![deny(missing_docs)]

pub use pcs_core::{
    Component, Dataset, PcsError, PcsResult, Pipeline, PipelineBuilder, RetryMode, Row, RunStats,
    SchemaRegistry, System, SystemConfig, SystemMeta, WriteSet, system_fn,
};

// Arrow sub-crates guest authors need to define `Component` schemas,
// re-surfaced at the workspace-pinned `=59.2.0` version so user crates do not
// depend on them directly. `serde` is not re-exported: `#[derive(Serialize)]`
// expansions reference the literal `::serde` path at the call site, so guest
// authors add `serde` as a direct dep of their own crate.
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
/// Not public API. Its contents are stable only within a single `pcs-guest`
/// version, and the macro is the only legitimate consumer.
#[doc(hidden)]
pub mod __rt {
    pub use pcs_core::{Component, Dataset, PcsError, PcsResult, Pipeline, RunStats};
    pub use pollster;
    pub use std::sync::{Mutex, MutexGuard, OnceLock};

    pub use arrow_ipc::writer::StreamWriter;
    pub use arrow_schema::Schema;

    // The boundary-agnostic half of the glue lives in `pcs_core::sdk`, so the
    // native plugin SDK reuses the same state, error and schema handling
    // without depending on this crate and its wasm-oriented pins.
    pub use pcs_core::sdk::{
        GuestStateSpec, NoState, PipelineSlot, Stateful, classify_run_error, fingerprint_hex,
        schema_to_ipc_bytes,
    };
}

pub use pcs_core::sdk::GuestState;

/// Wire a user-authored pipeline to the `pcs-pipeline` WIT world exports.
///
/// The first argument names a `fn() -> Pipeline` that constructs the pipeline.
/// The optional `state = T` argument names the one [`Component`] whose rows
/// survive across `run-batch` calls. The macro generates:
///
/// - a hidden static holding the lazily built [`Pipeline`],
/// - an `impl Guest for __PcsComponent` block covering the two WIT exports
///   (`describe`, `run-batch`),
/// - `pcs_config_get` / `pcs_config_parse` free functions in the calling crate,
/// - the `crate::bindings::export!` handshake cargo-component requires.
///
/// # Requirements
///
/// - Build the downstream crate with `cargo component build`; the expansion
///   targets its generated `crate::bindings` module.
/// - Point `[package.metadata.component.target.path]` at
///   `crates/pcs-guest/wit/`, which defines world `pcs-pipeline` of
///   `pcs:pipeline@0.2.0`.
/// - A `state = T` guest must register `T` in `build()`, so `describe()`
///   declares it and the host's template dataset carries it.
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
/// See the crate documentation for the error-handling contract.
// `clippy::crate_in_macro_def` is allowed because the `crate::bindings`
// references inside the expansion must resolve to the caller's crate, not to
// pcs-guest. `$crate::bindings` would point at pcs-guest and break. The same
// reason forces `pcs_config_get` to be emitted here rather than in pcs-guest.
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
            use $crate::__rt::GuestStateSpec as _;

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
                    // deterministic across runs and across host and guest.
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
    use __rt::GuestStateSpec;
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
            "a stateless guest must return no checkpoint"
        );
        assert!(
            data.get_resource::<GuestState<Counter>>().is_none(),
            "a stateless guest must not install a state resource"
        );
    }

    #[test]
    fn stateful_round_trips_the_state_rows() {
        const { assert!(<__rt::Stateful<Counter>>::STATEFUL) };

        let mut source = batch_dataset();
        <__rt::Stateful<Counter>>::restore(&mut source, None).expect("cold-start restore");
        source
            .get_resource_mut::<GuestState<Counter>>()
            .expect("state resource installed")
            .rows
            .push(Counter { count: 41 });

        let blob = <__rt::Stateful<Counter>>::capture(&source)
            .expect("capture")
            .expect("stateful guest must produce a blob");

        let mut target = batch_dataset();
        <__rt::Stateful<Counter>>::restore(&mut target, Some(&blob)).expect("restore");
        let state = target
            .get_resource::<GuestState<Counter>>()
            .expect("state resource installed");
        assert_eq!(state.rows, vec![Counter { count: 41 }]);
    }

    #[test]
    fn state_is_absent_from_the_batch_output_ipc() {
        let mut data = batch_dataset();
        let mut without_state: Vec<u8> = Vec::new();
        data.write_ipc(&mut without_state).expect("write_ipc");

        <__rt::Stateful<Counter>>::restore(&mut data, None).expect("restore");
        data.get_resource_mut::<GuestState<Counter>>()
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
                    .get_resource::<GuestState<Counter>>()
                    .expect("state resource installed")
                    .is_empty(),
                "a missing or empty prior means the guest starts from scratch"
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
            next.get_resource::<GuestState<Counter>>()
                .expect("state resource")
                .is_empty()
        );
    }
}
