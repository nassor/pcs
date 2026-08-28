//! [`ChannelSink`]: in-memory [`Sink`] that sends each [`RecordBatch`] through a
//! tokio mpsc channel. The receiver collects results without file I/O.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::mpsc;

use pcs_core::error::PcsError;
use pcs_core::io::sink::Sink;

/// In-memory push sink backed by a tokio mpsc channel.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use pcs_connector_channel::ChannelSink;
/// use pcs_core::io::sink::Sink;
///
/// # #[tokio::main]
/// # async fn main() {
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("x", DataType::Float32, false),
/// ]));
/// let (mut sink, _rx) = ChannelSink::new(schema.clone(), 4);
/// sink.finish().await.unwrap();
/// # }
/// ```
pub struct ChannelSink {
    tx: mpsc::Sender<RecordBatch>,
    schema: Arc<Schema>,
    buffer_capacity: usize,
}

impl ChannelSink {
    /// Create a `ChannelSink` and the matching `Receiver`.
    ///
    /// `buffer` is the mpsc channel capacity.
    pub fn new(schema: Arc<Schema>, buffer: usize) -> (Self, mpsc::Receiver<RecordBatch>) {
        let (tx, rx) = mpsc::channel(buffer);
        (
            Self {
                tx,
                schema,
                buffer_capacity: buffer,
            },
            rx,
        )
    }

    /// Wrap an existing sender half, paired with a receiver resolved
    /// elsewhere — the [`ChannelRegistry`](crate::registry::ChannelRegistry)
    /// bridge's sink-side constructor.
    pub fn from_sender(schema: Arc<Schema>, buffer: usize, tx: mpsc::Sender<RecordBatch>) -> Self {
        Self {
            tx,
            schema,
            buffer_capacity: buffer,
        }
    }
}

#[async_trait]
impl Sink for ChannelSink {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.tx
            .send(batch.clone())
            .await
            .map_err(|e| PcsError::generic(format!("ChannelSink: channel send error: {e}")))
    }

    async fn finish(&mut self) -> Result<(), PcsError> {
        // Nothing to flush; the receiver reads at its own pace.
        Ok(())
    }

    fn pending_rows(&self) -> Option<usize> {
        // `tx.capacity()` returns remaining free slots, so in-flight = configured - remaining.
        Some(self.buffer_capacity.saturating_sub(self.tx.capacity()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn make_batch(schema: Arc<Schema>, n: i32) -> RecordBatch {
        let arr = Arc::new(Int32Array::from_iter_values(0..n));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[tokio::test]
    async fn test_channel_sink_receive_batch() {
        let schema = make_schema();
        let (mut sink, mut rx) = ChannelSink::new(schema.clone(), 4);

        let batch = make_batch(schema.clone(), 5);
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.num_rows(), 5);
    }

    #[tokio::test]
    async fn test_channel_sink_multiple_batches() {
        let schema = make_schema();
        let (mut sink, mut rx) = ChannelSink::new(schema.clone(), 8);

        for n in [3i32, 7, 11] {
            sink.write_batch(&make_batch(schema.clone(), n))
                .await
                .unwrap();
        }
        sink.finish().await.unwrap();
        drop(sink);

        let mut total = 0;
        while let Some(b) = rx.recv().await {
            total += b.num_rows();
        }
        assert_eq!(total, 21);
    }

    #[tokio::test]
    async fn test_channel_sink_eof_when_sink_dropped() {
        let schema = make_schema();
        let (sink, mut rx) = ChannelSink::new(schema.clone(), 4);
        drop(sink);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_channel_sink_schema_accessor() {
        let schema = make_schema();
        let (sink, _rx) = ChannelSink::new(schema.clone(), 1);
        assert_eq!(sink.schema().field(0).name(), "v");
    }
}
