//! [`Sink`] trait — push-based batch egress from a [`Dataset`].
//!
//! A `Sink` receives [`RecordBatch`]es one at a time via
//! [`write_batch`](Sink::write_batch), then is finalised with
//! [`finish`](Sink::finish).  Sinks must be finalised before dropping to
//! ensure all buffered data is flushed.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::error::PcsError;
use crate::pipeline::Dataset;

/// Push-based batch sink for Arrow data.
///
/// Implementations need only be `Send`.  Concurrent writes to the same sink
/// are not supported — exclusive mutable access via `&mut self` provides all
/// necessary synchronisation.
#[async_trait]
pub trait Sink: Send {
    /// Write one batch.  May be called multiple times.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError>;

    /// Flush and finalise the sink.  Must be called exactly once after all
    /// batches have been written.
    async fn finish(&mut self) -> Result<(), PcsError>;

    /// The schema this sink expects.  Batches must conform to this schema.
    fn schema(&self) -> Arc<Schema>;

    /// Approximate number of rows currently buffered in the sink but not yet
    /// consumed downstream.
    ///
    /// Returns `None` if the sink does not support backpressure probing.
    /// The scheduler uses this to pause upstream pipelines when a downstream
    /// sink is full.
    fn pending_rows(&self) -> Option<usize> {
        None
    }
}

/// Write all rows of `component_name` from `dataset` to `sink`.
///
/// Retrieves the raw [`RecordBatch`] for the component and calls
/// [`write_batch`](Sink::write_batch) once (the whole component is one
/// contiguous `RecordBatch`).  Does **not** call [`finish`](Sink::finish) —
/// the caller is responsible for finalisation.
///
/// Returns the number of rows written.
///
/// # Errors
///
/// Returns `PcsError::Generic` if the component is not registered in `dataset`,
/// or the first error from `sink.write_batch()`.
pub async fn drain_dataset<K: Sink + ?Sized>(
    dataset: &Dataset,
    component_name: &'static str,
    sink: &mut K,
) -> Result<usize, PcsError> {
    let batch = dataset.batch_for(component_name).ok_or_else(|| {
        PcsError::generic(format!(
            "drain_dataset: component '{component_name}' is not registered in the dataset"
        ))
    })?;
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let n = batch.num_rows();
    sink.write_batch(batch).await?;
    Ok(n)
}
