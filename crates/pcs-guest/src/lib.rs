//! `pcs-guest` — Guest SDK for PCS WebAssembly Component Model pipelines.
//!
//! This crate is the canonical source of the `pcs:pipeline@0.2.0` WIT package
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
//! world. The host loads it via `wasmtime` and drives the two guest exports,
//! `describe` and `run-batch`.
//!
//! # Config
//!
//! Values from the service TOML `[pipeline.wasm.config]` table are read through
//! the `pcs:pipeline/host-io` `get-config` import, which the host answers on
//! every call including `describe`. [`export_pipeline!`] emits
//! `pcs_config_get` / `pcs_config_parse` into your crate for that purpose —
//! they must live in your crate because the WIT bindings are caller-side.
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
//! while state rows are independent of batch rows. As a consequence the state
//! never appears in the guest's output IPC, so it cannot be double-applied.
//! One component, not a list: a guest needing more fields widens that
//! component's schema.
//!
//! # Error handling in user systems
//!
//! The macro-generated `run-batch` impl converts [`PcsError`] variants bubbling
//! out of `Pipeline::run_on_with_stats` into the WIT `run-error` variant per
//! the frozen mapping:
//!
//! | `PcsError` variant                                                   | WIT `run-error`       |
//! |----------------------------------------------------------------------|-----------------------|
//! | `RetryExhausted`, `SystemExecution`                                  | `retryable(string)`   |
//! | `ComponentNotFound`, `ResourceNotFound`, `EntityNotFound`,           |                       |
//! | `Configuration`, `Scheduler`, `Store`, `Generic`                     | `permanent(string)`   |
//!
//! `schema-mismatch` is never emitted from `run-batch`. `LeaseExpired` is
//! dropped from the guest mapping.
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
// as direct deps at the exact `=59.2.0` pin. The pcs-guest dep graph pulls
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
    pub use pcs_core::{Component, Dataset, PcsError, PcsResult, Pipeline, RunStats};
    pub use pollster;
    pub use std::sync::{Mutex, MutexGuard, OnceLock};

    pub use arrow_ipc::writer::StreamWriter;
    pub use arrow_schema::Schema;

    use std::marker::PhantomData;

    /// Holds the lazily-built [`Pipeline`] owned by the macro-generated
    /// component.
    ///
    /// Initialized on the first call to any WIT export via the user's `build()`
    /// function, wrapped in a `Mutex` for `Send + Sync` plumbing even though
    /// wasm32-wasip2 is single-threaded in practice.
    ///
    /// There is deliberately no config slot: config is pulled on demand through
    /// the `host-io` `get-config` import, which the host answers on every call.
    pub struct PipelineSlot {
        pipeline: OnceLock<Mutex<Pipeline>>,
    }

    impl Default for PipelineSlot {
        fn default() -> Self {
            Self::new()
        }
    }

    impl PipelineSlot {
        /// Construct an empty state. The macro creates exactly one static
        /// instance per component.
        pub const fn new() -> Self {
            Self {
                pipeline: OnceLock::new(),
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
    }

    /// How a macro expansion moves guest state across the `run-batch` boundary.
    ///
    /// Two impls exist and the macro picks one from its invocation form:
    /// [`NoState`] for `export_pipeline!(build)` and [`Stateful<C>`] for
    /// `export_pipeline!(build, state = C)`. Keeping the branch in the type
    /// system rather than in `macro_rules!` means the big expansion body exists
    /// exactly once.
    pub trait GuestStateSpec {
        /// Value the macro writes into `pipeline-descriptor.stateful`.
        const STATEFUL: bool;

        /// Make the previous batch's state available to the pipeline's systems.
        ///
        /// Called before the pipeline runs, with `prior` exactly as the host
        /// passed it. [`Stateful`] decodes it into a [`crate::GuestState`]`<C>`
        /// resource
        /// on `data`; [`NoState`] does nothing.
        fn restore(data: &mut Dataset, prior: Option<&[u8]>) -> PcsResult<()>;

        /// Serialise the post-run state back out of `data`.
        ///
        /// The returned blob becomes `run-result.checkpoint`, which the host
        /// persists verbatim and returns as the next `prior`.
        fn capture(data: &Dataset) -> PcsResult<Option<Vec<u8>>>;
    }

    /// [`GuestStateSpec`] for a stateless guest: both hooks are no-ops.
    pub struct NoState;

    impl GuestStateSpec for NoState {
        const STATEFUL: bool = false;

        fn restore(_data: &mut Dataset, _prior: Option<&[u8]>) -> PcsResult<()> {
            Ok(())
        }

        fn capture(_data: &Dataset) -> PcsResult<Option<Vec<u8>>> {
            Ok(None)
        }
    }

    /// [`GuestStateSpec`] backed by one Arrow component `C`.
    ///
    /// The state lives in the batch dataset as a [`crate::GuestState`]`<C>`
    /// *resource*,
    /// not as a registered component: `Dataset`'s IPC format requires every
    /// component to have exactly the dataset's row count, and state rows are
    /// independent of batch rows. Resources are invisible to
    /// `Dataset::write_ipc`, so the state never leaks into the guest's output
    /// and cannot be double-applied on the next batch.
    ///
    /// The blob is a standalone single-component Arrow IPC stream, so the host
    /// can persist it with the existing checkpoint machinery without knowing
    /// the schema.
    pub struct Stateful<C>(PhantomData<C>);

    impl<C> GuestStateSpec for Stateful<C>
    where
        C: Component + serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        const STATEFUL: bool = true;

        fn restore(data: &mut Dataset, prior: Option<&[u8]>) -> PcsResult<()> {
            let rows = match prior {
                // No prior, or an empty payload from a runtime that wrote one:
                // either way this batch is the guest's first.
                None | Some([]) => Vec::new(),
                Some(bytes) => {
                    let name = C::name();
                    let prior_data = Dataset::read_ipc(&mut &bytes[..])?;
                    match prior_data.batch_for(name) {
                        None => Vec::new(),
                        Some(batch) => {
                            let on_disk = prior_data.schemas().get_version(name).unwrap_or(1);
                            let migrated = C::migrate(on_disk, batch.clone())?;
                            C::from_record_batch(&migrated)?
                        }
                    }
                }
            };
            data.insert_resource(crate::GuestState::<C>::new(rows));
            Ok(())
        }

        fn capture(data: &Dataset) -> PcsResult<Option<Vec<u8>>> {
            let state = data.get_resource::<crate::GuestState<C>>().ok_or_else(|| {
                PcsError::generic(format!(
                    "pcs-guest: GuestState<{}> resource is missing after the run; \
                         a system must have replaced the dataset wholesale",
                    C::name()
                ))
            })?;

            // Serialise through a scratch dataset holding only `C`, so the
            // stream satisfies Dataset's "every component has row_count rows"
            // invariant regardless of the batch's own row count.
            let mut scratch = Dataset::new();
            scratch.register_component::<C>()?;
            if !state.rows.is_empty() {
                scratch.append::<C>(&state.rows)?;
            }
            let mut buf: Vec<u8> = Vec::new();
            scratch.write_ipc(&mut buf)?;
            Ok(Some(buf))
        }
    }

    /// Map a `PcsError` from `Pipeline::run_on_with_stats` into a
    /// `(is_retryable, message)` pair the macro uses to construct the caller's
    /// `RunError` variant.
    ///
    /// Mapping is frozen per the 2026-04-15 wasm-guest + wasm-lead + dist-expert
    /// review. `retryable` only on `RetryExhausted` and `SystemExecution`;
    /// everything else including `Generic` is `permanent`. `schema-mismatch` is
    /// never emitted here — any mid-batch schema problem folds into
    /// `permanent` via `Configuration`.
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
// GuestState — the handle systems use to read and update cross-batch state.
// -----------------------------------------------------------------------------

/// Cross-batch guest state, held as a [`Dataset`] resource.
///
/// A guest exported with `export_pipeline!(build, state = C)` gets one of these
/// inserted into the batch dataset before its systems run, holding the rows the
/// previous batch left behind (empty on a cold start). Whatever the systems
/// leave in it is serialised into `run-result.checkpoint` afterwards.
///
/// It is a resource rather than a registered component because `Dataset`'s
/// Arrow IPC format requires every component to have exactly the dataset's row
/// count, while state rows are independent of batch rows. Resources are not
/// serialised by [`Dataset::write_ipc`], so state never leaks into the output.
///
/// ```ignore
/// fn count_batches(data: &mut Dataset) -> Result<(), PcsError> {
///     let state = data
///         .get_resource_mut::<GuestState<Counter>>()
///         .ok_or_else(|| PcsError::generic("guest state missing"))?;
///     match state.rows.first_mut() {
///         Some(counter) => counter.count += 1,
///         None => state.rows.push(Counter { count: 1 }),
///     }
///     Ok(())
/// }
/// ```
pub struct GuestState<C> {
    /// The state rows. Systems mutate this in place.
    pub rows: Vec<C>,
}

impl<C> GuestState<C> {
    /// Wrap the rows restored from the previous batch.
    pub fn new(rows: Vec<C>) -> Self {
        Self { rows }
    }

    /// `true` on a cold start, before any system has written state.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl<C> Default for GuestState<C> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

// -----------------------------------------------------------------------------
// export_pipeline! — the one macro that wires a user Pipeline to the WIT
// exports.
// -----------------------------------------------------------------------------

/// Wire a user-authored pipeline to the `pcs-pipeline` WIT world exports.
///
/// The first argument is the identifier of a `fn() -> Pipeline` that constructs
/// the pipeline (registers components, adds systems, configures retries). An
/// optional `state = T` argument names the one [`Component`] whose rows survive
/// across `run-batch` calls. The macro generates:
///
/// - a hidden static holding the lazily-built [`Pipeline`],
/// - an `impl Guest for __PcsComponent` block covering the two WIT exports
///   (`describe`, `run-batch`),
/// - `pcs_config_get` / `pcs_config_parse` free functions in *your* crate,
/// - the `crate::bindings::export!` handshake cargo-component requires.
///
/// # Requirements
///
/// - The downstream crate must be built with `cargo component build`. The
///   generated `crate::bindings` module is what this macro expansion targets.
/// - The WIT world is `pcs-pipeline` from `pcs:pipeline@0.2.0`, authored in
///   `crates/pcs-guest/wit/pipeline.wit`. Point your `[package.metadata.component.target.path]`
///   at that directory.
/// - A `state = T` guest must register `T` in `build()`, so that `describe()`
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
/// See the crate-level documentation for the full error-handling contract and
/// the defensive trap-avoidance guarantees the macro provides on your behalf.
// `clippy::crate_in_macro_def` is deliberately allowed: the `crate::bindings`
// references inside the expansion MUST resolve to the caller's crate, not to
// pcs-guest. Caller-side bindings is the core design decision (see module
// docs). Rewriting to `$crate::bindings` would point at pcs-guest and break.
// The same reason forces `pcs_config_get` to be emitted here rather than
// defined in pcs-guest.
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

            // Single static per component. `PipelineSlot::new()` is `const`.
            static __PCS_PIPELINE: $crate::__rt::PipelineSlot = $crate::__rt::PipelineSlot::new();

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

                    let pipeline = __PCS_PIPELINE.pipeline($build_fn);
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

                    // Rows the host handed in, measured before the state blob is
                    // merged so `rows_in` reports the data plane only.
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

                    // Every field is measured, none invented: `wall_ns` from
                    // Instant, row counts from Dataset::rows(), and
                    // `systems_run` / `retries` from the per-call RunStats that
                    // `run_on_with_stats` returns.
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

// -----------------------------------------------------------------------------
// Unit tests for the runtime-glue helpers. The `export_pipeline!` macro itself
// is exercised end-to-end by the `pcs-guest-smoketest` sibling crate, which
// builds to a real wasm component. The tests here cover the pieces that run
// on the host target without wit-bindgen generated code.
// -----------------------------------------------------------------------------

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
    /// count, which is the case the resource design has to survive.
    fn batch_dataset() -> Dataset {
        let mut data = Dataset::new();
        data.register_component::<Counter>()
            .expect("register unrelated component");
        data
    }

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

        // A system leaves count = 41 behind.
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

        // The next batch restores it.
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
