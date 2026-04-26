//! [`Source`] trait — pull-based batch ingestion into a [`Dataset`].
//!
//! A `Source` produces [`RecordBatch`]es on demand.  The pipeline calls
//! [`next_batch`](Source::next_batch) in a loop until `None` is returned
//! (EOF), then appends each batch into the dataset.
//!
//! ## Example
//!
//! ```rust,ignore
//! // See pcs-service io examples for ChannelSource usage.
//! ```

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::error::PcsError;
use crate::pipeline::Dataset;

/// Pull-based batch source for Arrow data.
///
/// Each call to [`next_batch`](Self::next_batch) yields the next
/// [`RecordBatch`], or `None` at EOF. Implementations should produce batches
/// of reasonable size (~64 k rows) rather than one giant batch.
#[async_trait]
pub trait Source: Send + Sync {
    /// Arrow [`Schema`] that every batch from this source conforms to.
    ///
    /// Fixed per source instance — the schema must not change between calls.
    fn schema(&self) -> Arc<Schema>;

    /// Pull the next batch, or `None` at EOF.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError>;

    /// Estimated total row count, or `None` if unknown.
    ///
    /// Used only for progress reporting; callers must not rely on accuracy.
    fn estimated_rows(&self) -> Option<usize> {
        None
    }
}

/// Drain all batches from `source` into `dataset` under `component_name`.
///
/// The component must already be registered (via
/// [`register_component`](Dataset::register_component) or
/// [`register_raw_component`](Dataset::register_raw_component)) before
/// calling this function.
///
/// Returns the total number of rows appended.
///
/// # Errors
///
/// Returns the first error from `source.next_batch()` or from
/// [`Dataset::append_record_batch`].
pub async fn drain_into_dataset<S: Source + ?Sized>(
    source: &mut S,
    dataset: &mut Dataset,
    component_name: &'static str,
) -> Result<usize, PcsError> {
    let mut total = 0usize;
    while let Some(batch) = source.next_batch().await? {
        let n = batch.num_rows();
        dataset.append_record_batch(component_name, batch)?;
        total += n;
    }
    Ok(total)
}
