//! [`ParquetSink`] — Parquet file sink.
//!
//! Writes [`RecordBatch`]es to a Parquet file using
//! [`parquet::arrow::ArrowWriter`] with Snappy compression.

use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::error::PcsError;
use crate::io::sink::Sink;

/// Parquet file sink using Snappy compression.
///
/// The [`ArrowWriter`] is held directly (no `Mutex`) because [`Sink::write_batch`]
/// and [`Sink::finish`] both take `&mut self`, so exclusive mutable access
/// provides all necessary synchronisation.  `ArrowWriter<BufWriter<File>>`
/// auto-derives `Sync` (no interior mutability), so `ParquetSink` satisfies
/// the `Sink: Send + Sync` bound without any locking overhead.
pub struct ParquetSink {
    writer: Option<ArrowWriter<BufWriter<std::fs::File>>>,
    schema: Arc<Schema>,
}

impl ParquetSink {
    /// Open (or create/truncate) a Parquet file for writing with Snappy
    /// compression.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the file cannot be created or the
    /// Parquet writer cannot be initialised.
    pub fn from_path(path: &Path, schema: Arc<Schema>) -> Result<Self, PcsError> {
        let file = std::fs::File::create(path)
            .map_err(|e| PcsError::generic(format!("ParquetSink: cannot create {path:?}: {e}")))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let writer = ArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(props))
            .map_err(|e| PcsError::generic(format!("ParquetSink: ArrowWriter init error: {e}")))?;
        Ok(Self {
            writer: Some(writer),
            schema,
        })
    }
}

#[async_trait]
impl Sink for ParquetSink {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.writer
            .as_mut()
            .ok_or_else(|| PcsError::generic("ParquetSink: write called after finish"))?
            .write(batch)
            .map_err(|e| PcsError::generic(format!("ParquetSink: write error: {e}")))
    }

    async fn finish(&mut self) -> Result<(), PcsError> {
        if let Some(writer) = self.writer.take() {
            writer
                .close()
                .map_err(|e| PcsError::generic(format!("ParquetSink: finish error: {e}")))?;
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "io"))]
mod tests {
    use super::*;
    use crate::io::parquet_source::ParquetSource;
    use crate::io::source::Source;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_parquet_sink_creates_valid_file() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from_iter_values(0i32..100));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();

        let f = NamedTempFile::new().unwrap();
        let mut sink = ParquetSink::from_path(f.path(), schema.clone()).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);

        // Read back and verify.
        let mut src = ParquetSource::from_path(f.path()).unwrap();
        let result = src.next_batch().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 100);
    }
}
