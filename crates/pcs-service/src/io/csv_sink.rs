//! [`CsvSink`] — CSV file sink.
//!
//! Writes each [`RecordBatch`] to a CSV file using [`arrow_csv::WriterBuilder`].

use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::WriterBuilder;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::error::PcsError;
use crate::io::sink::Sink;

/// CSV file sink.
pub struct CsvSink {
    writer: arrow_csv::Writer<BufWriter<std::fs::File>>,
    schema: Arc<Schema>,
    header_written: bool,
}

impl CsvSink {
    /// Open (or create/truncate) a CSV file for writing.
    ///
    /// `has_header` — if `true`, the column names are written as the first row
    /// on the first call to `write_batch`.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the file cannot be opened.
    pub fn from_path(path: &Path, schema: Arc<Schema>, has_header: bool) -> Result<Self, PcsError> {
        let file = std::fs::File::create(path)
            .map_err(|e| PcsError::generic(format!("CsvSink: cannot open {path:?}: {e}")))?;
        let writer = WriterBuilder::new()
            .with_header(has_header)
            .build(BufWriter::new(file));
        Ok(Self {
            writer,
            schema,
            header_written: false,
        })
    }
}

#[async_trait]
impl Sink for CsvSink {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        let _ = self.header_written; // suppress unused warning
        self.writer
            .write(batch)
            .map_err(|e| PcsError::generic(format!("CsvSink: write error: {e}")))?;
        self.header_written = true;
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), PcsError> {
        // arrow_csv::Writer is buffered via BufWriter; flush is implicit on drop.
        // There is no explicit finish() method on arrow_csv::Writer.
        Ok(())
    }
}

#[cfg(all(test, feature = "io"))]
mod tests {
    use super::*;
    use crate::io::csv_source::CsvSource;
    use crate::io::source::Source;
    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_csv_sink_round_trip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
        ]));
        let id_arr: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::Int64Array::from_iter_values(0i64..4));
        let val_arr: Arc<dyn arrow_array::Array> =
            Arc::new(Float64Array::from(vec![1.1f64, 2.2, 3.3, 4.4]));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_arr, val_arr]).unwrap();

        let f = NamedTempFile::new().unwrap();
        let mut sink = CsvSink::from_path(f.path(), schema.clone(), true).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);

        let mut src = CsvSource::from_path(f.path(), schema, true).unwrap();
        let result = src.next_batch().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 4);
    }
}
