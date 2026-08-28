//! [`ArrowIpcTransformer`]: the `arrow-ipc` format, message surface only.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;

use pcs_core::error::PcsError;
use pcs_transformer::{ConfigValue, MessageDecoder, MessageShape, Transformer, TransformerFactory};

/// The `arrow-ipc` byte format: one Arrow IPC stream per message.
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

    #[test]
    fn arrow_ipc_has_no_stream_surface() {
        let Err(err) = ArrowIpcTransformer::new().open_writer(Box::new(Vec::new()), schema())
        else {
            panic!("arrow-ipc writes no byte stream");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'arrow-ipc'"), "got: {err}");
    }
}
