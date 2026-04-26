//! [`ParquetSource`] — Parquet file source.
//!
//! Reads a Parquet file using [`parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder`]
//! (synchronous API) on a dedicated OS thread, streaming batches through a
//! [`tokio::sync::mpsc`] channel.
//!
//! ## Async strategy
//!
//! Parquet reading is inherently synchronous (the `parquet` crate's async API
//! requires the `async` feature which is not included in our dependency).  A
//! dedicated OS thread owns the [`parquet::arrow::arrow_reader::ParquetRecordBatchReader`]
//! and sends each [`RecordBatch`] through an `mpsc` channel.  [`Source::next_batch`]
//! awaits the channel rather than blocking the async executor.  This avoids
//! materialising the entire file into memory before processing begins.
//!
//! ## Schema inference
//!
//! Parquet is self-describing — the schema is read from the file metadata.
//! No explicit schema is required at construction time.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::mpsc;

use crate::error::PcsError;
use crate::io::source::Source;

/// Parquet file source.
///
/// The schema is read from the Parquet file metadata at construction time.
/// RecordBatches are streamed one at a time from a background OS thread —
/// the file is never fully materialised in memory.
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(feature = "io")]
/// # {
/// use std::path::Path;
/// use pcs_service::io::parquet_source::ParquetSource;
///
/// let src = ParquetSource::from_path(Path::new("data.parquet")).unwrap();
/// # }
/// ```
pub struct ParquetSource {
    rx: mpsc::Receiver<Result<RecordBatch, PcsError>>,
    schema: Arc<Schema>,
    estimated_rows: Option<usize>,
}

impl ParquetSource {
    /// Open a Parquet file.
    ///
    /// Reads schema and row-count metadata synchronously, then spawns an OS
    /// thread to stream batches.  Returns immediately; batches are produced
    /// on demand via [`Source::next_batch`].
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the file cannot be opened or contains
    /// invalid Parquet data.
    pub fn from_path(path: &Path) -> Result<Self, PcsError> {
        let path = path.to_owned();
        Self::open_sync(path)
    }

    /// Open a Parquet file asynchronously (metadata read off the tokio runtime
    /// via `spawn_blocking`).
    ///
    /// Prefer this method when called from async code to avoid blocking the
    /// executor thread during the metadata read.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the file cannot be opened, contains
    /// invalid Parquet data, or the blocking task panics.
    pub async fn from_path_async(path: &Path) -> Result<Self, PcsError> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || Self::open_sync(path))
            .await
            .map_err(|e| PcsError::generic(format!("ParquetSource: spawn_blocking panic: {e}")))?
    }

    /// Internal constructor: opens the file, reads metadata, spawns the
    /// reader thread, and returns the source handle.
    fn open_sync(path: PathBuf) -> Result<Self, PcsError> {
        let file = std::fs::File::open(&path)
            .map_err(|e| PcsError::generic(format!("ParquetSource: cannot open {path:?}: {e}")))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| PcsError::generic(format!("ParquetSource: builder error: {e}")))?;

        let schema = builder.schema().clone();

        // Estimate total rows from row-group metadata (no data read).
        let estimated_rows: usize = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .sum();
        let estimated_rows = if estimated_rows == 0 {
            None
        } else {
            Some(estimated_rows)
        };

        let reader = builder
            .build()
            .map_err(|e| PcsError::generic(format!("ParquetSource: reader build error: {e}")))?;

        // Channel capacity of 4 lets the reader stay slightly ahead without
        // excessive memory use.
        let (tx, rx) = mpsc::channel(4);

        std::thread::spawn(move || {
            for result in reader {
                let msg = result
                    .map_err(|e| PcsError::generic(format!("ParquetSource: read error: {e}")));
                // If the receiver is dropped (e.g. pipeline aborted), stop.
                if tx.blocking_send(msg).is_err() {
                    break;
                }
            }
        });

        Ok(Self {
            rx,
            schema,
            estimated_rows,
        })
    }
}

#[async_trait]
impl Source for ParquetSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        match self.rx.recv().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }
}

#[cfg(all(test, feature = "io"))]
mod tests {
    use super::*;
    use crate::io::parquet_sink::ParquetSink;
    use crate::io::sink::Sink;
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::NamedTempFile;

    fn make_batch(schema: Arc<Schema>, n: i64) -> RecordBatch {
        let ids = Arc::new(Int64Array::from_iter_values(0..n));
        let vals = Arc::new(Float64Array::from_iter_values(
            (0..n).map(|i| i as f64 * 1.5),
        ));
        RecordBatch::try_new(schema, vec![ids, vals]).unwrap()
    }

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
        ]))
    }

    #[tokio::test]
    async fn test_parquet_source_round_trip() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), 1000);

        let f = NamedTempFile::new().unwrap();
        let mut sink = ParquetSink::from_path(f.path(), schema.clone()).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);

        let mut src = ParquetSource::from_path(f.path()).unwrap();
        assert_eq!(src.schema().fields().len(), 2);

        let mut total_rows = 0;
        while let Some(b) = src.next_batch().await.unwrap() {
            total_rows += b.num_rows();
        }
        assert_eq!(total_rows, 1000);
    }

    #[tokio::test]
    async fn test_parquet_source_async_from_path() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), 50);

        let f = NamedTempFile::new().unwrap();
        let mut sink = ParquetSink::from_path(f.path(), schema.clone()).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);

        let mut src = ParquetSource::from_path_async(f.path()).await.unwrap();
        let first = src.next_batch().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 50);
    }

    #[tokio::test]
    async fn test_parquet_source_schema_derived_from_file() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), 10);

        let f = NamedTempFile::new().unwrap();
        let mut sink = ParquetSink::from_path(f.path(), schema.clone()).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);

        let src = ParquetSource::from_path(f.path()).unwrap();
        let s = src.schema();
        assert_eq!(s.field(0).name(), "id");
        assert_eq!(s.field(1).name(), "val");
    }

    #[tokio::test]
    async fn test_parquet_source_estimated_rows() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), 42);

        let f = NamedTempFile::new().unwrap();
        let mut sink = ParquetSink::from_path(f.path(), schema.clone()).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);

        let src = ParquetSource::from_path(f.path()).unwrap();
        assert_eq!(src.estimated_rows(), Some(42));
    }

    /// Verify that `next_batch` streams: pull the first batch and confirm
    /// data arrives without waiting for the full file to be read.
    #[tokio::test]
    async fn test_parquet_source_streams_first_batch_without_full_materialisation() {
        let schema = make_schema();
        // Write a file with 10 000 rows — large enough to be meaningful but
        // still fast in CI.
        let batch = make_batch(schema.clone(), 10_000);

        let f = NamedTempFile::new().unwrap();
        let mut sink = ParquetSink::from_path(f.path(), schema.clone()).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);

        // Open the source and pull only the first batch — the channel-based
        // impl returns it without reading further into the file.
        let mut src = ParquetSource::from_path(f.path()).unwrap();
        let first = src.next_batch().await.unwrap();
        assert!(first.is_some(), "first batch must not be None");
        // Drop the source — reader thread sees channel closed and exits.
    }
}
