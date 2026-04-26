//! [`CsvSource`] — CSV file source.
//!
//! Reads a CSV file using [`arrow_csv::ReaderBuilder`] with an explicit schema
//! and an optional header row.  Reading is performed on a dedicated OS thread;
//! [`Source::next_batch`] awaits an `mpsc` channel to avoid blocking the async
//! executor and to avoid materialising the entire file up front.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::PcsError;
use crate::io::source::Source;

/// CSV file source.
///
/// Batches are produced on demand by a background OS thread; the file is
/// never fully materialised in memory.
///
/// # Example
///
/// ```rust,ignore
/// # #[cfg(feature = "io")]
/// # {
/// use std::sync::Arc;
/// use std::path::Path;
/// use arrow_schema::{DataType, Field, Schema};
/// use pcs_service::io::csv_source::CsvSource;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id",    DataType::Int64,   false),
///     Field::new("score", DataType::Float64, false),
/// ]));
/// let src = CsvSource::from_path(Path::new("data.csv"), schema, true).unwrap();
/// # }
/// ```
pub struct CsvSource {
    rx: mpsc::Receiver<Result<RecordBatch, PcsError>>,
    schema: Arc<Schema>,
}

impl CsvSource {
    /// Open a CSV file with an explicit schema.
    ///
    /// `has_header` — `true` if the first row contains column names (they will
    /// be skipped during reading; the provided `schema` governs column types).
    ///
    /// Spawns an OS thread to stream batches; returns immediately.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the file cannot be opened or the reader
    /// cannot be initialised.
    pub fn from_path(path: &Path, schema: Arc<Schema>, has_header: bool) -> Result<Self, PcsError> {
        let file = std::fs::File::open(path)
            .map_err(|e| PcsError::generic(format!("CsvSource: cannot open {path:?}: {e}")))?;
        let reader = ReaderBuilder::new(schema.clone())
            .with_header(has_header)
            .build(BufReader::new(file))
            .map_err(|e| PcsError::generic(format!("CsvSource: reader build failed: {e}")))?;

        let (tx, rx) = mpsc::channel(4);

        std::thread::spawn(move || {
            for result in reader {
                let msg =
                    result.map_err(|e| PcsError::generic(format!("CsvSource: read error: {e}")));
                if tx.blocking_send(msg).is_err() {
                    break;
                }
            }
        });

        Ok(Self { rx, schema })
    }
}

#[async_trait]
impl Source for CsvSource {
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
}

#[cfg(all(test, feature = "io"))]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_csv(header: bool, rows: &[&str]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        if header {
            writeln!(f, "id,score").unwrap();
        }
        for row in rows {
            writeln!(f, "{row}").unwrap();
        }
        f
    }

    #[tokio::test]
    async fn test_csv_source_with_header() {
        let f = write_csv(true, &["1,1.5", "2,2.5", "3,3.5"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let mut src = CsvSource::from_path(f.path(), schema, true).unwrap();
        let mut total = 0;
        while let Some(b) = src.next_batch().await.unwrap() {
            total += b.num_rows();
        }
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn test_csv_source_without_header() {
        let f = write_csv(false, &["10,0.1", "20,0.2"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let mut src = CsvSource::from_path(f.path(), schema, false).unwrap();
        let batch = src.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let scores = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((scores.value(0) - 0.1).abs() < 1e-9);
    }

    /// Streaming: pull only the first batch; the reader thread handles the rest
    /// lazily.  We verify the source works without consuming all rows.
    #[tokio::test]
    async fn test_csv_source_streams_first_batch_without_full_materialisation() {
        // Write a file with many rows across multiple batches.
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "id,score").unwrap();
        for i in 0i64..5_000 {
            writeln!(f, "{i},{}", i as f64 * 0.1).unwrap();
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let mut src = CsvSource::from_path(f.path(), schema, true).unwrap();
        let first = src.next_batch().await.unwrap();
        assert!(first.is_some(), "first batch must not be None");
        // Drop source without consuming all batches — reader thread exits cleanly.
    }
}
