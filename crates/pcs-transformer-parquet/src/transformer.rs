//! [`ParquetTransformer`]: the `parquet` format, read and written through the
//! synchronous `parquet` Arrow reader and writer.

use std::io::Write;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use pcs_core::error::PcsError;
use pcs_transformer::{BatchReader, BatchWriter, ConfigValue, Transformer, TransformerFactory};

/// The `parquet` byte format.
///
/// Snappy compression is fixed: it is the only setting a PCS pipeline has ever
/// written, and a checkpoint or dataset file that changes codec between runs is
/// a compatibility problem, not a tuning knob.
#[derive(Default)]
pub struct ParquetTransformer;

impl ParquetTransformer {
    /// Build the format.
    pub fn new() -> Self {
        Self
    }
}

impl Transformer for ParquetTransformer {
    fn format(&self) -> &'static str {
        "parquet"
    }

    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, PcsError> {
        if declared.is_some() {
            return Err(PcsError::configuration(
                "parquet: the file carries its own schema; remove schema_fields",
            ));
        }

        let builder = ParquetRecordBatchReaderBuilder::try_new(input)
            .map_err(|e| PcsError::generic(format!("parquet: builder error: {e}")))?;
        let schema = builder.schema().clone();

        // Row-group metadata is already in memory: no data pages are read.
        let rows: usize = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|group| group.num_rows() as usize)
            .sum();
        let estimated_rows = (rows > 0).then_some(rows);

        let reader = builder
            .build()
            .map_err(|e| PcsError::generic(format!("parquet: reader build error: {e}")))?;
        Ok(Box::new(ParquetBatchReader {
            reader,
            schema,
            estimated_rows,
        }))
    }

    fn open_writer(
        &self,
        output: Box<dyn Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, PcsError> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        // `ArrowWriter` buffers a row group of its own, so the handle arrives
        // unwrapped and `close` flushes through it.
        let writer = ArrowWriter::try_new(output, schema, Some(props))
            .map_err(|e| PcsError::generic(format!("parquet: writer init error: {e}")))?;
        Ok(Box::new(ParquetBatchWriter { writer }))
    }
}

/// Factory for [`ParquetTransformer`]. Reads no options.
pub struct ParquetTransformerFactory;

impl TransformerFactory for ParquetTransformerFactory {
    fn format_name(&self) -> &'static str {
        "parquet"
    }

    fn build(&self, _options: &ConfigValue) -> Result<Arc<dyn Transformer>, PcsError> {
        Ok(Arc::new(ParquetTransformer::new()))
    }
}

struct ParquetBatchReader {
    reader: ParquetRecordBatchReader,
    schema: Arc<Schema>,
    estimated_rows: Option<usize>,
}

impl BatchReader for ParquetBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        match self.reader.next() {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(PcsError::generic(format!("parquet: read error: {e}"))),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }
}

struct ParquetBatchWriter {
    writer: ArrowWriter<Box<dyn Write + Send>>,
}

impl BatchWriter for ParquetBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.writer
            .write(batch)
            .map_err(|e| PcsError::generic(format!("parquet: write error: {e}")))
    }

    fn finish(self: Box<Self>) -> Result<(), PcsError> {
        // Writes the footer and flushes the writer's own buffer down to the
        // handle. Without it the file has no readable metadata at all.
        self.writer
            .close()
            .map(|_metadata| ())
            .map_err(|e| PcsError::generic(format!("parquet: finish error: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::NamedTempFile;

    use pcs_transformer::ConfigMap;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
        ]))
    }

    fn batch(rows: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(Float64Array::from_iter_values(
                    (0..rows).map(|i| i as f64 * 1.5),
                )),
            ],
        )
        .expect("batch")
    }

    /// Write `rows` rows through the transformer and hand back the file.
    fn written(rows: i64) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp file");
        let mut writer = ParquetTransformer::new()
            .open_writer(Box::new(file.reopen().expect("reopen for write")), schema())
            .expect("writer opens");
        writer.write_batch(&batch(rows)).expect("write");
        writer.finish().expect("close writes the footer");
        file
    }

    #[test]
    fn the_schema_comes_from_the_file() {
        let file = written(10);
        let reader = ParquetTransformer::new()
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        let schema = reader.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "val");
    }

    #[test]
    fn a_declared_schema_is_a_configuration_error() {
        let file = written(1);
        let Err(err) =
            ParquetTransformer::new().open_reader(file.reopen().expect("reopen"), Some(schema()))
        else {
            panic!("parquet must reject a declared schema");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn estimated_rows_comes_from_row_group_metadata() {
        let file = written(42);
        let reader = ParquetTransformer::new()
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        assert_eq!(reader.estimated_rows(), Some(42));
    }

    #[test]
    fn a_write_then_read_round_trip_preserves_every_row() {
        let file = written(1_000);
        let mut reader = ParquetTransformer::new()
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        let mut rows = 0;
        while let Some(batch) = reader.next_batch().expect("read") {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 1_000);
    }

    #[test]
    fn the_factory_builds_the_format_and_reads_no_options() {
        let transformer = ParquetTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("build");
        assert_eq!(transformer.format(), "parquet");
        assert_eq!(ParquetTransformerFactory.format_name(), "parquet");
    }

    #[test]
    fn parquet_has_no_message_surface() {
        let transformer = ParquetTransformer::new();
        assert_eq!(transformer.message_shape(), None);
        let Err(err) = transformer.encode_messages(&batch(1)) else {
            panic!("parquet encodes no discrete messages");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'parquet'"), "got: {err}");
    }
}
