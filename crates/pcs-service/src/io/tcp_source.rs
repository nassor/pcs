//! [`TcpIngestSource`] — live TCP [`Source`] for stream mode.
//!
//! Listens on a bind address and yields one [`RecordBatch`] per received
//! frame. Unlike file sources, it **never reaches EOF**: `next_batch` blocks
//! until the next frame arrives. That makes it usable only from the stream
//! runner (`run_mode` `kind = "stream"`); the batch loop's
//! [`drain_into_dataset`](crate::io::source::drain_into_dataset) would block
//! forever on it. [`ServiceConfig::validate`](crate::service::config::ServiceConfig::validate)
//! rejects the combination.
//!
//! ## Frame format
//!
//! Each frame is a `u32` big-endian length prefix followed by exactly that many
//! payload bytes — the same convention the Raft transport uses
//! (`distributed::consensus::transport::wire`). The payload is one Arrow IPC
//! **stream** (schema header + exactly one `RecordBatch`), as produced by
//! [`arrow_ipc::writer::StreamWriter`].
//!
//! A clean connection close *between* frames is a normal disconnect. A
//! protocol violation (oversized frame, undecodable payload, schema mismatch)
//! closes that connection only; the listener stays up and other producers are
//! unaffected.
//!
//! Multiple concurrent connections are accepted. Ordering is preserved within
//! a connection; across connections it is unspecified.

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::PcsError;
use crate::io::source::Source;

/// Live TCP ingestion source: one Arrow IPC frame in, one `RecordBatch` out.
///
/// The listener socket is bound in [`new`](Self::new) (synchronously, so bind
/// failures surface at config time) and the accept loop is spawned on the first
/// [`next_batch`](Source::next_batch) call, which is guaranteed to run inside a
/// tokio runtime.
///
/// Backpressure is the channel's: once `buffer` batches are queued, reader
/// tasks stop consuming their sockets and TCP flow control pushes back on the
/// producers.
pub struct TcpIngestSource {
    listener: Option<std::net::TcpListener>,
    local_addr: SocketAddr,
    schema: Arc<Schema>,
    tx: mpsc::Sender<RecordBatch>,
    rx: mpsc::Receiver<RecordBatch>,
    max_frame_bytes: usize,
    listener_task: Option<JoinHandle<()>>,
}

impl TcpIngestSource {
    /// Bind `bind` and prepare the ingest channel.
    ///
    /// `buffer` is the number of decoded batches that may queue before
    /// backpressure reaches the producers. `max_frame_bytes` caps a single
    /// frame; a producer that announces more has its connection closed.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] if `bind` cannot be bound.
    pub fn new(
        bind: &str,
        schema: Arc<Schema>,
        buffer: usize,
        max_frame_bytes: usize,
    ) -> Result<Self, PcsError> {
        let listener = std::net::TcpListener::bind(bind).map_err(|e| {
            PcsError::configuration(format!("TcpIngestSource: cannot bind '{bind}': {e}"))
        })?;
        listener.set_nonblocking(true).map_err(|e| {
            PcsError::configuration(format!("TcpIngestSource: set_nonblocking failed: {e}"))
        })?;
        let local_addr = listener.local_addr().map_err(|e| {
            PcsError::configuration(format!("TcpIngestSource: local_addr failed: {e}"))
        })?;

        let (tx, rx) = mpsc::channel(buffer.max(1));

        Ok(Self {
            listener: Some(listener),
            local_addr,
            schema,
            tx,
            rx,
            max_frame_bytes,
            listener_task: None,
        })
    }

    /// The address actually bound, with the ephemeral port resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Convert the std listener and spawn the accept loop. Idempotent.
    fn ensure_listening(&mut self) {
        let Some(std_listener) = self.listener.take() else {
            return;
        };
        let listener = match TcpListener::from_std(std_listener) {
            Ok(l) => l,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(error = %_e, "TcpIngestSource: cannot adopt listener into runtime");
                return;
            }
        };

        let tx = self.tx.clone();
        let schema = Arc::clone(&self.schema);
        let max_frame_bytes = self.max_frame_bytes;

        self.listener_task = Some(tokio::spawn(async move {
            loop {
                let (stream, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %_e, "TcpIngestSource: accept failed");
                        continue;
                    }
                };

                let tx = tx.clone();
                let schema = Arc::clone(&schema);
                tokio::spawn(async move {
                    read_connection(stream, tx, schema, max_frame_bytes).await;
                });
            }
        }));
    }
}

/// Read length-prefixed Arrow IPC frames from one connection until it closes
/// or violates the protocol.
async fn read_connection(
    mut stream: TcpStream,
    tx: mpsc::Sender<RecordBatch>,
    schema: Arc<Schema>,
    max_frame_bytes: usize,
) {
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            // Clean close between frames — the normal disconnect path.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "TcpIngestSource: frame header read failed");
                return;
            }
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 {
            continue;
        }
        if len > max_frame_bytes {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                frame_bytes = len,
                max_frame_bytes,
                "TcpIngestSource: oversized frame, closing connection"
            );
            return;
        }

        let mut payload = vec![0u8; len];
        if let Err(_e) = stream.read_exact(&mut payload).await {
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_e, "TcpIngestSource: truncated frame payload, closing connection");
            return;
        }

        let batch = match decode_frame(&payload, &schema) {
            Ok(batch) => batch,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "TcpIngestSource: bad frame, closing connection");
                return;
            }
        };

        // Bounded send: natural TCP backpressure. A send error means the
        // source was dropped, so this connection has nowhere to go.
        if tx.send(batch).await.is_err() {
            return;
        }
    }
}

/// Decode one Arrow IPC stream payload into its single `RecordBatch`.
fn decode_frame(payload: &[u8], schema: &Schema) -> Result<RecordBatch, PcsError> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(payload), None)
        .map_err(|e| PcsError::generic(format!("TcpIngestSource: IPC stream header: {e}")))?;

    let batch = reader
        .next()
        .ok_or_else(|| PcsError::generic("TcpIngestSource: frame contained no record batch"))?
        .map_err(|e| PcsError::generic(format!("TcpIngestSource: IPC decode: {e}")))?;

    if batch.schema().fields() != schema.fields() {
        return Err(PcsError::generic(format!(
            "TcpIngestSource: received batch with schema {:?}, expected {:?}",
            batch.schema(),
            schema
        )));
    }

    Ok(batch)
}

#[async_trait]
impl Source for TcpIngestSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        self.ensure_listening();
        // `None` here means the accept task is gone (it never drops `tx`
        // otherwise), which is terminal for this source.
        Ok(self.rx.recv().await)
    }
}

impl Drop for TcpIngestSource {
    fn drop(&mut self) {
        if let Some(handle) = self.listener_task.take() {
            handle.abort();
        }
        // Per-connection reader tasks exit on their next `tx.send`, which
        // fails once `rx` is dropped with this struct.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    #[test]
    fn new_reports_bound_port() {
        let src = TcpIngestSource::new("127.0.0.1:0", test_schema(), 4, 1024).unwrap();
        assert_ne!(src.local_addr().port(), 0);
    }

    #[test]
    fn new_rejects_unbindable_address() {
        let err = TcpIngestSource::new("256.256.256.256:1", test_schema(), 4, 1024)
            .err()
            .expect("bind must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
    }

    #[test]
    fn decode_frame_rejects_schema_mismatch() {
        let wrong = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&wrong),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1]))],
        )
        .unwrap();

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &wrong).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let err = decode_frame(&buf, &test_schema()).unwrap_err();
        assert!(err.to_string().contains("expected"), "got: {err}");
    }

    #[test]
    fn decode_frame_round_trips_matching_batch() {
        let schema = test_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7, 8]))],
        )
        .unwrap();

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let decoded = decode_frame(&buf, &schema).unwrap();
        assert_eq!(decoded.num_rows(), 2);
    }
}
