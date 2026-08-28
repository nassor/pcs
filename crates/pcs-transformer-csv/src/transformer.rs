//! [`CsvTransformer`]: the `csv` format, read and written through `arrow-csv`.

use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::{ReaderBuilder, WriterBuilder};
use arrow_schema::Schema;

use pcs_core::error::PcsError;
use pcs_transformer::{BatchReader, BatchWriter, ConfigValue, Transformer, TransformerFactory};

/// Whether a header row is read and written by default.
const DEFAULT_HAS_HEADERS: bool = true;

/// The `csv` byte format.
///
/// CSV carries no schema, so reading needs a declared one and the declared
/// types govern the columns whatever the text looks like. `has_headers` skips a
/// first row of column names while reading and emits one while writing.
pub struct CsvTransformer {
    has_headers: bool,
}

impl CsvTransformer {
    /// Build the format with an explicit header setting.
    pub fn new(has_headers: bool) -> Self {
        Self { has_headers }
    }
}

impl Default for CsvTransformer {
    fn default() -> Self {
        Self::new(DEFAULT_HAS_HEADERS)
    }
}

impl Transformer for CsvTransformer {
    fn format(&self) -> &'static str {
        "csv"
    }

    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, PcsError> {
        let schema = declared.ok_or_else(|| {
            PcsError::configuration("csv: reading needs a declared schema; add schema_fields")
        })?;
        // `build` wraps the handle in its own `BufReader`.
        let reader = ReaderBuilder::new(Arc::clone(&schema))
            .with_header(self.has_headers)
            .build(input)
            .map_err(|e| PcsError::generic(format!("csv: reader build failed: {e}")))?;
        Ok(Box::new(CsvBatchReader { reader, schema }))
    }

    fn open_writer(
        &self,
        output: Box<dyn Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, PcsError> {
        let _ = schema;
        let writer = WriterBuilder::new()
            .with_header(self.has_headers)
            .build(BufWriter::new(output));
        Ok(Box::new(CsvBatchWriter { writer }))
    }
}

/// Factory for [`CsvTransformer`].
///
/// `options`:
/// - `has_headers` (bool, optional, default `true`), written
///   `options has_headers=#false`.
pub struct CsvTransformerFactory;

impl TransformerFactory for CsvTransformerFactory {
    fn format_name(&self) -> &'static str {
        "csv"
    }

    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Transformer>, PcsError> {
        let has_headers = match options.get("has_headers") {
            None => DEFAULT_HAS_HEADERS,
            Some(value) => value.as_bool().ok_or_else(|| {
                PcsError::configuration("csv: option 'has_headers' must be a boolean")
            })?,
        };
        Ok(Arc::new(CsvTransformer::new(has_headers)))
    }
}

struct CsvBatchReader {
    reader: arrow_csv::Reader<std::fs::File>,
    schema: Arc<Schema>,
}

impl BatchReader for CsvBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        match self.reader.next() {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(PcsError::generic(format!("csv: read error: {e}"))),
        }
    }
}

struct CsvBatchWriter {
    writer: arrow_csv::Writer<BufWriter<Box<dyn Write + Send>>>,
}

impl BatchWriter for CsvBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.writer
            .write(batch)
            .map_err(|e| PcsError::generic(format!("csv: write error: {e}")))
    }

    fn finish(self: Box<Self>) -> Result<(), PcsError> {
        // CSV has no trailer, but the `BufWriter` this writer owns does hold
        // bytes: recover it and flush explicitly rather than leaving the last
        // block to `Drop`, which swallows the error.
        let mut buffered = self.writer.into_inner();
        buffered
            .flush()
            .map_err(|e| PcsError::generic(format!("csv: finish error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Seek;
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::NamedTempFile;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]))
    }

    fn write_csv(header: bool, rows: &[&str]) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("temp file");
        if header {
            writeln!(f, "id,score").expect("header");
        }
        for row in rows {
            writeln!(f, "{row}").expect("row");
        }
        f.flush().expect("flush");
        f
    }

    fn read_all(transformer: &CsvTransformer, file: std::fs::File) -> Vec<RecordBatch> {
        let mut reader = transformer
            .open_reader(file, Some(schema()))
            .expect("reader opens");
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch().expect("read") {
            batches.push(batch);
        }
        batches
    }

    #[test]
    fn a_header_row_is_skipped_when_has_headers_is_set() {
        let f = write_csv(true, &["1,1.5", "2,2.5", "3,3.5"]);
        let batches = read_all(
            &CsvTransformer::new(true),
            f.reopen().expect("reopen for read"),
        );
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    #[test]
    fn every_row_is_data_when_has_headers_is_clear() {
        let f = write_csv(false, &["10,0.1", "20,0.2"]);
        let batches = read_all(
            &CsvTransformer::new(false),
            f.reopen().expect("reopen for read"),
        );
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        let scores = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float column");
        assert!((scores.value(0) - 0.1).abs() < 1e-9);
    }

    #[test]
    fn reading_without_a_declared_schema_is_a_configuration_error() {
        let f = write_csv(true, &["1,1.5"]);
        let Err(err) = CsvTransformer::default().open_reader(f.reopen().expect("reopen"), None)
        else {
            panic!("csv cannot infer a schema");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn a_write_then_read_round_trip_preserves_every_row() {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0i64..4)),
                Arc::new(Float64Array::from(vec![1.1f64, 2.2, 3.3, 4.4])),
            ],
        )
        .expect("batch");

        let transformer = CsvTransformer::new(true);
        let mut file = NamedTempFile::new().expect("temp file");
        let mut writer = transformer
            .open_writer(Box::new(file.reopen().expect("reopen for write")), schema())
            .expect("writer opens");
        writer.write_batch(&batch).expect("write");
        writer.finish().expect("finish flushes");

        file.rewind().expect("rewind");
        let batches = read_all(&transformer, file.reopen().expect("reopen for read"));
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    }

    #[test]
    fn the_factory_reads_has_headers_off_its_options_table() {
        let options = pcs_transformer::from_kdl_str("has_headers #false").expect("parse kdl");
        let transformer = CsvTransformerFactory.build(&options).expect("build");
        assert_eq!(transformer.format(), "csv");
        assert_eq!(CsvTransformerFactory.format_name(), "csv");

        // A no-header write emits no name row, so the round trip through a
        // has_headers=#true read would lose the first record.
        let f = write_csv(false, &["7,0.7"]);
        let mut reader = transformer
            .open_reader(f.reopen().expect("reopen"), Some(schema()))
            .expect("reader opens");
        let batch = reader.next_batch().expect("read").expect("one batch");
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn a_non_boolean_has_headers_is_a_configuration_error() {
        let options = pcs_transformer::from_kdl_str("has_headers \"yes\"").expect("parse kdl");
        let Err(err) = CsvTransformerFactory.build(&options) else {
            panic!("has_headers must be a boolean");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("has_headers"), "got: {err}");
    }

    #[test]
    fn csv_has_no_message_surface() {
        let transformer = CsvTransformer::default();
        assert_eq!(transformer.message_shape(), None);
        let Err(err) = transformer.open_message_decoder(schema()) else {
            panic!("csv decodes no discrete messages");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'csv'"), "got: {err}");
    }
}
