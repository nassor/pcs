//! Glue for SDKs that drive a [`Pipeline`] across a foreign function boundary.
//!
//! Two SDKs sit on top of this module. `pcs-guest` wires a pipeline to the
//! `pcs:pipeline@0.2.0` WIT world for a WebAssembly component, and `pcs-plugin`
//! wires the same pipeline to the C ABI of a native shared library. Both face
//! the same four problems: hold the pipeline behind a sync entry point, move
//! state across a batch boundary that resets memory, classify a [`PcsError`]
//! for the caller's error type, and report schemas the way the host expects.
//!
//! Nothing here is specific to either boundary, so both SDKs re-export it
//! rather than keeping a copy.

use std::marker::PhantomData;
use std::sync::{Mutex, MutexGuard, OnceLock};

use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;

use crate::{Component, Dataset, PcsError, PcsResult, Pipeline};

/// Holds the lazily built [`Pipeline`] owned by an SDK's exported entry points.
///
/// An SDK creates exactly one static instance per exported component. The
/// pipeline is built on the first call to any entry point, then reused.
///
/// `OnceLock` rather than `LazyLock`: the initializer is the caller's `build`
/// function, handed to [`PipelineSlot::pipeline`] per call, so there is nothing
/// for a `LazyLock` to store at declaration time.
pub struct PipelineSlot {
    pipeline: OnceLock<Mutex<Pipeline>>,
}

impl Default for PipelineSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineSlot {
    /// Construct an empty slot.
    pub const fn new() -> Self {
        Self {
            pipeline: OnceLock::new(),
        }
    }

    /// Lock and return the pipeline, initialising via `build` on first use.
    ///
    /// # Panics
    ///
    /// Panics if the inner `Mutex` is poisoned, which needs a panic while the
    /// lock is held. A WebAssembly guest traps before reaching this path; a
    /// native plugin catches the unwind at its boundary and reports a permanent
    /// error, so the poisoned lock surfaces on the next call instead.
    pub fn pipeline<F>(&self, build: F) -> MutexGuard<'_, Pipeline>
    where
        F: FnOnce() -> Pipeline,
    {
        self.pipeline
            .get_or_init(|| Mutex::new(build()))
            .lock()
            .expect("pcs-core sdk: pipeline mutex poisoned")
    }
}

/// How an SDK moves guest state across a batch boundary.
///
/// An SDK picks an impl from its macro's invocation form: [`NoState`] for a
/// stateless pipeline, [`Stateful<C>`] for one that carries rows. Keeping the
/// branch in the type system rather than in `macro_rules!` keeps the large
/// expansion body to one copy.
pub trait GuestStateSpec {
    /// Value the SDK reports as the descriptor's `stateful` flag.
    const STATEFUL: bool;

    /// Make the previous batch's state available to the pipeline's systems.
    ///
    /// Called before the pipeline runs, with `prior` exactly as the host passed
    /// it. [`Stateful`] decodes it into a [`GuestState<C>`] resource on `data`;
    /// [`NoState`] does nothing.
    fn restore(data: &mut Dataset, prior: Option<&[u8]>) -> PcsResult<()>;

    /// Serialise the post-run state back out of `data`.
    ///
    /// The returned blob becomes the checkpoint the host persists verbatim and
    /// returns as the next `prior`.
    fn capture(data: &Dataset) -> PcsResult<Option<Vec<u8>>>;
}

/// [`GuestStateSpec`] for a stateless pipeline: both hooks are no-ops.
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
/// The state lives in the batch dataset as a [`GuestState<C>`] resource, not as
/// a registered component: [`Dataset`]'s IPC format requires every component to
/// hold exactly the dataset's row count, while state rows are independent of
/// batch rows. Resources are invisible to [`Dataset::write_ipc`], so state never
/// leaks into the output.
///
/// The blob is a standalone single-component Arrow IPC stream, so the host
/// persists it with the existing checkpoint machinery without knowing the
/// schema.
pub struct Stateful<C>(PhantomData<C>);

impl<C> GuestStateSpec for Stateful<C>
where
    C: Component + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    const STATEFUL: bool = true;

    fn restore(data: &mut Dataset, prior: Option<&[u8]>) -> PcsResult<()> {
        let rows = match prior {
            // No prior, or an empty payload: either way this batch is the
            // pipeline's first.
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
        data.insert_resource(GuestState::<C>::new(rows));
        Ok(())
    }

    fn capture(data: &Dataset) -> PcsResult<Option<Vec<u8>>> {
        let state = data.get_resource::<GuestState<C>>().ok_or_else(|| {
            PcsError::generic(format!(
                "pcs sdk: GuestState<{}> resource is missing after the run; \
                     a system must have replaced the dataset wholesale",
                C::name()
            ))
        })?;

        // Serialise through a scratch dataset holding only `C`, so the stream
        // satisfies Dataset's "every component has row_count rows" invariant
        // whatever the batch's own row count is.
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

/// Map a [`PcsError`] from [`Pipeline::run_on_with_stats`] into a
/// `(is_retryable, message)` pair an SDK turns into its own error type.
///
/// `retryable` only for [`PcsError::RetryExhausted`] and
/// [`PcsError::SystemExecution`]; everything else, [`PcsError::Generic`]
/// included, is permanent. A schema mismatch is never reported from a batch.
pub fn classify_run_error(err: &PcsError) -> (bool, String) {
    let is_retryable = matches!(
        err,
        PcsError::RetryExhausted { .. } | PcsError::SystemExecution(_)
    );
    (is_retryable, err.to_string())
}

/// Serialize a single Arrow [`Schema`] as IPC schema-message bytes.
///
/// Writes an empty `StreamWriter`, whose stream start carries the schema
/// descriptor and nothing else. This is the shape a host parses out of a
/// component descriptor.
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

/// Format a [`crate::SchemaRegistry::fingerprint`] value as the stable 8-char
/// lowercase hex string a descriptor's `schema_fingerprint` field carries.
pub fn fingerprint_hex(fp: u32) -> String {
    format!("{fp:08x}")
}

/// Cross-batch state, held as a [`Dataset`] resource.
///
/// An SDK's `state = C` form inserts one of these into the batch dataset before
/// the systems run, holding the rows the previous batch left behind (empty on a
/// cold start). Whatever the systems leave in it is serialised into the
/// checkpoint afterwards.
///
/// It is a resource rather than a registered component because [`Dataset`]'s
/// Arrow IPC format requires every component to hold exactly the dataset's row
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_hex_is_lowercase_and_zero_padded_to_eight() {
        assert_eq!(fingerprint_hex(0), "00000000");
        assert_eq!(fingerprint_hex(0xd52f95a6), "d52f95a6");
        assert_eq!(fingerprint_hex(0xff), "000000ff");
        assert_eq!(fingerprint_hex(u32::MAX), "ffffffff");
    }

    #[test]
    fn classify_run_error_marks_only_execution_and_retry_exhausted_retryable() {
        let (retryable, message) = classify_run_error(&PcsError::system_execution("boom"));
        assert!(retryable);
        assert!(message.contains("boom"));

        assert!(
            classify_run_error(&PcsError::retry_exhausted(PcsError::generic("why"), 3)).0,
            "an exhausted retry budget is worth another tick"
        );

        assert!(!classify_run_error(&PcsError::configuration("bad")).0);
        assert!(!classify_run_error(&PcsError::generic("other")).0);
    }

    #[test]
    fn no_state_captures_nothing() {
        let mut data = Dataset::new();
        assert!(NoState::restore(&mut data, Some(&[1, 2, 3])).is_ok());
        assert_eq!(NoState::capture(&data).unwrap(), None);
        const { assert!(!NoState::STATEFUL) };
    }

    #[test]
    fn a_pipeline_slot_builds_once() {
        static SLOT: PipelineSlot = PipelineSlot::new();
        {
            let pipeline = SLOT.pipeline(|| Pipeline::new("first"));
            assert_eq!(pipeline.name(), "first");
        }
        // The second call must not rebuild, so the name from the first build
        // survives even though this closure would produce a different one.
        let pipeline = SLOT.pipeline(|| Pipeline::new("second"));
        assert_eq!(pipeline.name(), "first");
    }
}
