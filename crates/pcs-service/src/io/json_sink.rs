//! [`JsonSink`] — newline-delimited JSON file sink.
//!
//! Writes each [`RecordBatch`] as a block of NDJSON lines using
//! [`arrow_json::writer::LineDelimitedWriter`].

use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::writer::LineDelimitedWriter;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::error::PcsError;
use crate::io::sink::Sink;

/// Newline-delimited JSON sink backed by a file.
pub struct JsonSink {
    writer: LineDelimitedWriter<BufWriter<std::fs::File>>,
    schema: Arc<Schema>,
}

impl JsonSink {
    /// Open (or create/truncate) a JSON Lines file for writing.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the file cannot be opened.
    pub fn from_path(path: &Path, schema: Arc<Schema>) -> Result<Self, PcsError> {
        let file = std::fs::File::create(path)
            .map_err(|e| PcsError::generic(format!("JsonSink: cannot open {path:?}: {e}")))?;
        let writer = LineDelimitedWriter::new(BufWriter::new(file));
        Ok(Self { writer, schema })
    }
}

#[async_trait]
impl Sink for JsonSink {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.writer
            .write(batch)
            .map_err(|e| PcsError::generic(format!("JsonSink: write error: {e}")))
    }

    async fn finish(&mut self) -> Result<(), PcsError> {
        self.writer
            .finish()
            .map_err(|e| PcsError::generic(format!("JsonSink: finish error: {e}")))
    }
}

#[cfg(all(test, feature = "io"))]
mod tests {
    use super::*;
    use crate::io::json_source::JsonSource;
    use crate::io::source::Source;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_json_sink_round_trip() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from_iter_values(0i32..5));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();

        let f = NamedTempFile::new().unwrap();
        let mut sink = JsonSink::from_path(f.path(), schema.clone()).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();
        drop(sink);

        let mut src = JsonSource::from_path(f.path(), schema.clone()).unwrap();
        let result = src.next_batch().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 5);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0i32..5 {
            assert_eq!(col.value(i as usize), i);
        }
    }
}
