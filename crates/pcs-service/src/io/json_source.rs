//! [`JsonSource`]: newline-delimited JSON file source.
//!
//! Reads NDJSON with [`arrow_json::ReaderBuilder`]. Parsing runs on a dedicated
//! OS thread and [`Source::next_batch`] awaits an `mpsc` channel, so the async
//! executor never blocks. The file bytes are read once at construction because
//! the JSON reader needs a seekable `Read` and schema inference rewinds; the
//! parsed [`RecordBatch`]es are then produced lazily.

use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::PcsError;
use crate::io::source::Source;

/// Newline-delimited JSON source backed by a file.
///
/// File bytes are read at construction time; [`RecordBatch`]es are produced
/// lazily by a background OS thread.
pub struct JsonSource {
    rx: mpsc::Receiver<Result<RecordBatch, PcsError>>,
    schema: Arc<Schema>,
}

impl JsonSource {
    /// Open a JSON Lines file with an explicit schema.
    ///
    /// Each line must be a complete JSON object matching `schema`. Arrow's JSON
    /// reader handles column projection and type coercion.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the file cannot be read or parsing
    /// fails.
    pub fn from_path(path: &Path, schema: Arc<Schema>) -> Result<Self, PcsError> {
        let data = std::fs::read(path)
            .map_err(|e| PcsError::generic(format!("JsonSource: cannot read {path:?}: {e}")))?;
        Self::from_bytes(&data, schema)
    }

    /// Open a JSON Lines file and infer the schema from the first `infer_max`
    /// rows (default 1024 when passing `None`).
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if the file cannot be read or schema
    /// inference fails.
    pub fn with_infer(path: &Path, infer_max: Option<usize>) -> Result<Self, PcsError> {
        let data = std::fs::read(path)
            .map_err(|e| PcsError::generic(format!("JsonSource: cannot read {path:?}: {e}")))?;
        let max = infer_max.unwrap_or(1024);
        let cursor = Cursor::new(&data);
        let (schema, _) = arrow_json::reader::infer_json_schema(cursor, Some(max))
            .map_err(|e| PcsError::generic(format!("JsonSource: schema inference failed: {e}")))?;
        Self::from_bytes(&data, Arc::new(schema))
    }

    /// Construct from raw bytes (NDJSON) with an explicit schema.
    ///
    /// Spawns an OS thread to iterate the reader; returns immediately.
    pub fn from_bytes(data: &[u8], schema: Arc<Schema>) -> Result<Self, PcsError> {
        let cursor = Cursor::new(data.to_owned());
        let reader = ReaderBuilder::new(schema.clone())
            .build(cursor)
            .map_err(|e| PcsError::generic(format!("JsonSource: reader build failed: {e}")))?;

        let (tx, rx) = mpsc::channel(4);

        std::thread::spawn(move || {
            for result in reader {
                let msg =
                    result.map_err(|e| PcsError::generic(format!("JsonSource: read error: {e}")));
                if tx.blocking_send(msg).is_err() {
                    break;
                }
            }
        });

        Ok(Self { rx, schema })
    }
}

#[async_trait]
impl Source for JsonSource {
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
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_ndjson(records: &[&str]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        for line in records {
            writeln!(f, "{line}").unwrap();
        }
        f
    }

    #[tokio::test]
    async fn test_json_source_from_path_explicit_schema() {
        let f = write_ndjson(&[
            r#"{"id":1,"val":1.5}"#,
            r#"{"id":2,"val":2.5}"#,
            r#"{"id":3,"val":3.5}"#,
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float64, false),
        ]));

        let mut src = JsonSource::from_path(f.path(), schema.clone()).unwrap();
        assert_eq!(src.schema().fields().len(), 2);

        let mut total_rows = 0;
        while let Some(batch) = src.next_batch().await.unwrap() {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_json_source_with_infer() {
        let f = write_ndjson(&[r#"{"x":10}"#, r#"{"x":20}"#]);
        let mut src = JsonSource::with_infer(f.path(), None).unwrap();

        let batch = src.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let col = batch.column_by_name("x").expect("column x should exist");
        assert_eq!(col.len(), 2);
    }

    #[tokio::test]
    async fn test_json_source_from_bytes() {
        let data = b"{\"a\":42}\n{\"a\":99}\n";
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let mut src = JsonSource::from_bytes(data, schema).unwrap();
        let batch = src.next_batch().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
        assert_eq!(col.value(1), 99);
    }

    /// Pull only the first batch without consuming the full file.
    #[tokio::test]
    async fn test_json_source_streams_first_batch_without_full_materialisation() {
        let mut f = NamedTempFile::new().unwrap();
        for i in 0..5_000i32 {
            writeln!(f, r#"{{"v":{i}}}"#).unwrap();
        }
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let mut src = JsonSource::from_path(f.path(), schema).unwrap();
        let first = src.next_batch().await.unwrap();
        assert!(first.is_some(), "first batch must not be None");
        // Drop the source without consuming all batches; the reader thread exits.
    }
}
