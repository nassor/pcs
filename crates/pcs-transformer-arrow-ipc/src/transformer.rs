//! [`ArrowIpcTransformer`]: the `arrow-ipc` format, both surfaces.

use std::io::{BufReader, BufWriter, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;

use pcs_core::error::PcsError;
use pcs_transformer::{
    BatchReader, BatchWriter, ConfigValue, MessageDecoder, MessageShape, Transformer,
    TransformerFactory,
};

/// The `arrow-ipc` byte format: one Arrow IPC stream, whether that stream is a
/// whole file or one message payload.
///
/// Both surfaces carry the same encapsulation, the Arrow **stream** format
/// (`StreamWriter`/`StreamReader`): a schema message, then one message per
/// `RecordBatch`, then an end-of-stream marker. The random-access file format
/// would buy nothing here. [`BatchReader`] pulls forward only, and every
/// consumer of the read surface is a local file or an object already being
/// spooled, so a footer index is never consulted. It would also leave the two
/// surfaces mutually unreadable.
#[derive(Default)]
pub struct ArrowIpcTransformer;

impl ArrowIpcTransformer {
    /// Build the format.
    pub fn new() -> Self {
        Self
    }
}

impl Transformer for ArrowIpcTransformer {
    fn format(&self) -> &'static str {
        "arrow-ipc"
    }

    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, PcsError> {
        if declared.is_some() {
            return Err(PcsError::configuration(
                "arrow-ipc: the stream carries its own schema; remove schema_fields",
            ));
        }

        // The schema message is read here, so a handle whose bytes are not an
        // IPC stream is refused at open rather than part way through, and the
        // same `stream header` text the message surface reports names it.
        let reader = StreamReader::try_new(BufReader::new(input), None)
            .map_err(|e| PcsError::generic(format!("arrow-ipc: stream header: {e}")))?;
        let schema = reader.schema();
        Ok(Box::new(ArrowIpcBatchReader { reader, schema }))
    }

    fn open_writer(
        &self,
        output: Box<dyn Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, PcsError> {
        // The schema message goes out here, so a run that writes no batch
        // still leaves a valid, readable, zero-row stream.
        let writer = StreamWriter::try_new(BufWriter::new(output), schema.as_ref())
            .map_err(|e| PcsError::generic(format!("arrow-ipc: writer init error: {e}")))?;
        Ok(Box::new(ArrowIpcBatchWriter { writer }))
    }

    fn open_message_decoder(
        &self,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn MessageDecoder>, PcsError> {
        Ok(Box::new(ArrowIpcMessageDecoder {
            schema,
            batches: Vec::new(),
        }))
    }

    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, PcsError> {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())
                .map_err(|e| PcsError::generic(format!("arrow-ipc: encode: {e}")))?;
            writer
                .write(batch)
                .map_err(|e| PcsError::generic(format!("arrow-ipc: encode: {e}")))?;
            writer
                .finish()
                .map_err(|e| PcsError::generic(format!("arrow-ipc: encode: {e}")))?;
        }
        Ok(vec![buf])
    }

    fn message_shape(&self) -> Option<MessageShape> {
        Some(MessageShape::PerBatch)
    }
}

/// Factory for [`ArrowIpcTransformer`]. Reads no options.
pub struct ArrowIpcTransformerFactory;

impl TransformerFactory for ArrowIpcTransformerFactory {
    fn format_name(&self) -> &'static str {
        "arrow-ipc"
    }

    fn build(&self, _options: &ConfigValue) -> Result<Arc<dyn Transformer>, PcsError> {
        Ok(Arc::new(ArrowIpcTransformer::new()))
    }
}

struct ArrowIpcBatchReader {
    reader: StreamReader<BufReader<std::fs::File>>,
    schema: Arc<Schema>,
}

impl BatchReader for ArrowIpcBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        match self.reader.next() {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(PcsError::generic(format!("arrow-ipc: read error: {e}"))),
        }
    }
}

struct ArrowIpcBatchWriter {
    writer: StreamWriter<BufWriter<Box<dyn Write + Send>>>,
}

impl BatchWriter for ArrowIpcBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.writer
            .write(batch)
            .map_err(|e| PcsError::generic(format!("arrow-ipc: write error: {e}")))
    }

    fn finish(mut self: Box<Self>) -> Result<(), PcsError> {
        // Writes the end-of-stream marker, then flushes the `BufWriter` down
        // to the handle. The flush is what makes this load-bearing: a
        // `StreamReader` treats plain EOF as a clean end of stream, so a
        // skipped `finish` loses the buffered tail silently, up to and
        // including the schema message of a stream that wrote one batch.
        self.writer
            .finish()
            .map_err(|e| PcsError::generic(format!("arrow-ipc: finish error: {e}")))
    }
}

struct ArrowIpcMessageDecoder {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
}

impl MessageDecoder for ArrowIpcMessageDecoder {
    fn push(&mut self, payload: &[u8]) -> Result<(), PcsError> {
        let reader = StreamReader::try_new(std::io::Cursor::new(payload), None)
            .map_err(|e| PcsError::generic(format!("arrow-ipc: stream header: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| PcsError::generic(format!("arrow-ipc: decode: {e}")))?;
            if batch.schema().fields() != self.schema.fields() {
                return Err(PcsError::generic(format!(
                    "arrow-ipc: received batch with schema {:?}, expected {:?}",
                    batch.schema(),
                    self.schema
                )));
            }
            self.batches.push(batch);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        let batch = concat_batches(&self.schema, self.batches.iter())
            .map_err(|e| PcsError::generic(format!("arrow-ipc: concatenating batches: {e}")))?;
        self.batches.clear();
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::NamedTempFile;

    use pcs_transformer::ConfigMap;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).expect("batch")
    }

    fn encode(batch: &RecordBatch) -> Vec<u8> {
        let mut payloads = ArrowIpcTransformer::new()
            .encode_messages(batch)
            .expect("encode");
        assert_eq!(payloads.len(), 1, "arrow-ipc emits one message per batch");
        payloads.remove(0)
    }

    #[test]
    fn a_payload_round_trips_through_the_decoder() {
        let payload = encode(&batch(vec![7, 8]));
        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder.push(&payload).expect("push");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.num_rows(), 2);
    }

    #[test]
    fn a_window_of_payloads_is_concatenated() {
        let first = encode(&batch(vec![1, 2]));
        let second = encode(&batch(vec![3]));
        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder.push(&first).expect("first");
        decoder.push(&second).expect("second");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.num_rows(), 3);
        // `flush` reset the accumulator, so an empty window is `None`.
        assert!(decoder.flush().expect("second flush").is_none());
    }

    #[test]
    fn the_decoder_rejects_a_schema_mismatch() {
        let wrong = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let mismatched = RecordBatch::try_new(
            Arc::clone(&wrong),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .expect("batch");
        let payload = encode(&mismatched);

        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(&payload) else {
            panic!("a batch of the wrong schema must be rejected");
        };
        assert!(err.to_string().contains("expected"), "got: {err}");
    }

    #[test]
    fn a_payload_that_is_not_an_ipc_stream_is_rejected_by_its_header() {
        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(b"not an arrow stream") else {
            panic!("a non-IPC payload must be rejected");
        };
        assert!(err.message().contains("stream header"), "got: {err}");
    }

    #[test]
    fn the_factory_builds_the_format_and_reads_no_options() {
        let transformer = ArrowIpcTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("build");
        assert_eq!(transformer.format(), "arrow-ipc");
        assert_eq!(transformer.message_shape(), Some(MessageShape::PerBatch));
        assert_eq!(ArrowIpcTransformerFactory.format_name(), "arrow-ipc");
    }

    fn write_stream(transformer: &dyn Transformer, batches: &[RecordBatch]) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp file");
        let mut writer = transformer
            .open_writer(Box::new(file.reopen().expect("reopen for write")), schema())
            .expect("writer opens");
        for batch in batches {
            writer.write_batch(batch).expect("write");
        }
        writer
            .finish()
            .expect("finish writes the end-of-stream marker");
        file
    }

    fn read_all(transformer: &dyn Transformer, file: &NamedTempFile) -> Vec<RecordBatch> {
        let mut reader = transformer
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch().expect("read") {
            batches.push(batch);
        }
        batches
    }

    /// Every `v` in the order it was read.
    fn values(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("v is Int64")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[test]
    fn a_write_then_read_round_trip_preserves_every_row() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![1, 2]), batch(vec![3])]);
        assert_eq!(values(&read_all(&transformer, &file)), vec![1, 2, 3]);
    }

    #[test]
    fn one_written_batch_is_one_message_on_the_way_back() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![1, 2]), batch(vec![3])]);
        let shapes: Vec<usize> = read_all(&transformer, &file)
            .iter()
            .map(RecordBatch::num_rows)
            .collect();
        assert_eq!(shapes, vec![2, 1]);
    }

    #[test]
    fn the_schema_comes_from_the_stream() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![4])]);
        let reader = transformer
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        assert_eq!(reader.schema().fields(), schema().fields());
    }

    #[test]
    fn a_declared_schema_is_a_configuration_error() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![1])]);
        let Err(err) = transformer.open_reader(file.reopen().expect("reopen"), Some(schema()))
        else {
            panic!("arrow-ipc must reject a declared schema on the read side");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn a_run_that_writes_no_batch_still_leaves_a_readable_stream() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[]);
        let mut reader = transformer
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("the schema message alone is a readable stream");
        assert!(reader.next_batch().expect("read").is_none());
    }

    #[test]
    fn a_handle_that_is_not_an_ipc_stream_is_rejected_by_its_header() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(b"not an arrow stream").expect("write");
        file.flush().expect("flush");
        let Err(err) = ArrowIpcTransformer::new().open_reader(file.reopen().expect("reopen"), None)
        else {
            panic!("a non-IPC handle must be rejected at open");
        };
        assert!(err.message().contains("stream header"), "got: {err}");
    }

    #[test]
    fn a_written_stream_decodes_through_the_message_surface() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![5, 6])]);
        let bytes = std::fs::read(file.path()).expect("read back the written stream");

        let mut decoder = transformer
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder.push(&bytes).expect("push the whole stream");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(values(&[decoded]), vec![5, 6]);
    }

    #[test]
    fn a_message_payload_opens_through_the_read_surface() {
        let payload = encode(&batch(vec![9]));
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(&payload).expect("write");
        file.flush().expect("flush");

        let transformer = ArrowIpcTransformer::new();
        assert_eq!(values(&read_all(&transformer, &file)), vec![9]);
    }
}
