//! Stream-mode runner integration tests.
//!
//! Covers the properties the batch loop cannot provide: per-item state carry,
//! clean mid-stream cancellation, live TCP ingest, and the `run_standalone`
//! dispatch that selects this runner from `run_mode kind = "stream"`.

#![cfg(feature = "service")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use pcs_core::runtime::PipelineRuntime;
use pcs_core::{Dataset, PcsResult};
use pcs_service::io::channel_sink::ChannelSink;
use pcs_service::io::channel_source::ChannelSource;
use pcs_service::io::tcp_source::TcpIngestSource;
use pcs_service::service::builder::{BuiltService, BuiltSink, BuiltSource};
use pcs_service::service::config::{
    HttpConfig, NodeConfig, ObservabilityConfig, PipelineSpec, RunMode, ServiceConfig, ServiceMode,
    StandaloneConfig,
};
use pcs_service::service::registry::Registry;
use pcs_service::service::standalone::run_standalone;
use pcs_service::service::stream::run_stream;

const COMP: &str = "values";

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn make_batch(schema: Arc<Schema>, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
}

// ---------------------------------------------------------------------------
// CounterRuntime — records every `prior` it is handed, returns prior+1
// ---------------------------------------------------------------------------

type SeenPriors = Arc<Mutex<Vec<Option<Vec<u8>>>>>;

struct CounterRuntime {
    schema: Arc<Schema>,
    seen_priors: SeenPriors,
    calls: Arc<AtomicUsize>,
}

impl CounterRuntime {
    fn new(schema: Arc<Schema>) -> (Self, SeenPriors) {
        let seen: SeenPriors = Arc::new(Mutex::new(Vec::new()));
        let rt = Self {
            schema,
            seen_priors: Arc::clone(&seen),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        (rt, seen)
    }

    fn decode(blob: Option<&[u8]>) -> u64 {
        match blob {
            Some(b) if b.len() == 8 => u64::from_le_bytes(b.try_into().expect("8 bytes")),
            _ => 0,
        }
    }
}

#[async_trait(?Send)]
impl PipelineRuntime for CounterRuntime {
    fn name(&self) -> &str {
        "counter"
    }

    async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
        self.run_on_with_state(data, None).await.map(|_| ())
    }

    async fn run_on_with_state(
        &self,
        _data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> PcsResult<Option<Vec<u8>>> {
        self.seen_priors
            .lock()
            .unwrap()
            .push(prior.map(<[u8]>::to_vec));
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some((Self::decode(prior) + 1).to_le_bytes().to_vec()))
    }

    fn declared_components(&self) -> Vec<&str> {
        vec![COMP]
    }

    fn template_dataset(&self) -> Dataset {
        let mut dataset = Dataset::new();
        dataset.register_raw_component(COMP, Arc::clone(&self.schema));
        dataset
    }
}

fn built_service(
    sources: Vec<BuiltSource>,
    sinks: Vec<BuiltSink>,
    schema: Arc<Schema>,
) -> (BuiltService, SeenPriors) {
    let (runtime, seen) = CounterRuntime::new(schema);
    (
        BuiltService {
            runtime: Box::new(runtime),
            sources,
            sinks,
            registry: Registry::new(),
        },
        seen,
    )
}

fn channel_source(schema: Arc<Schema>, buffer: usize) -> (mpsc::Sender<RecordBatch>, BuiltSource) {
    let (tx, src) = ChannelSource::new(schema, buffer);
    (
        tx,
        BuiltSource {
            name: "test_source".to_string(),
            target_component: COMP.to_string(),
            source: Box::new(src),
        },
    )
}

fn channel_sink(schema: Arc<Schema>, buffer: usize) -> (BuiltSink, mpsc::Receiver<RecordBatch>) {
    let (sink, rx) = ChannelSink::new(schema, buffer);
    (
        BuiltSink {
            name: "test_sink".to_string(),
            source_component: COMP.to_string(),
            sink: Box::new(sink),
        },
        rx,
    )
}

// ---------------------------------------------------------------------------
// 1. Per-item state carry
// ---------------------------------------------------------------------------

/// Stream mode chains `run_on_with_state` output into the next item's `prior`.
/// The batch loop never does this — it calls `run_on` and drops guest state.
#[tokio::test]
async fn stream_carries_runtime_state_across_items() {
    let schema = test_schema();
    let (tx, source) = channel_source(Arc::clone(&schema), 8);
    let (sink, mut rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, seen) = built_service(vec![source], vec![sink], Arc::clone(&schema));

    for i in 0..5i64 {
        tx.send(make_batch(Arc::clone(&schema), &[i]))
            .await
            .unwrap();
    }
    drop(tx); // EOF

    let stats = run_stream(service, CancellationToken::new(), None)
        .await
        .unwrap();

    assert_eq!(stats.iterations, 5, "one iteration per item");
    assert_eq!(stats.rows_processed, 5);
    assert_eq!(stats.iteration_errors, 0);
    assert_eq!(stats.sink_batches_written, 5);
    assert!(
        stats.max_item_micros > 0,
        "per-item timing must be recorded"
    );
    assert!(stats.total_busy_micros >= stats.max_item_micros);

    let priors = seen.lock().unwrap();
    let decoded: Vec<u64> = priors
        .iter()
        .map(|p| CounterRuntime::decode(p.as_deref()))
        .collect();
    assert_eq!(
        decoded,
        vec![0, 1, 2, 3, 4],
        "each item must see the previous item's checkpoint"
    );

    let mut sink_rows = 0usize;
    let mut sink_batches = 0usize;
    while let Ok(batch) = rx.try_recv() {
        sink_rows += batch.num_rows();
        sink_batches += 1;
    }
    assert_eq!(sink_batches, 5, "one sink write per item");
    assert_eq!(sink_rows, 5);
}

// ---------------------------------------------------------------------------
// 2. Cancellation mid-stream
// ---------------------------------------------------------------------------

/// Cancelling while the source is idle exits cleanly, keeping the stats for the
/// items already processed.
#[tokio::test]
async fn stream_cancels_cleanly_mid_stream() {
    let schema = test_schema();
    let (tx, source) = channel_source(Arc::clone(&schema), 8);
    let (sink, _rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, _seen) = built_service(vec![source], vec![sink], Arc::clone(&schema));

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    let local = LocalSet::new();
    let handle = local.spawn_local(async move { run_stream(service, cancel_clone, None).await });

    let feed_schema = Arc::clone(&schema);
    let feeder = local.spawn_local(async move {
        for i in 0..3i64 {
            let _ = tx.send(make_batch(Arc::clone(&feed_schema), &[i])).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Hold the sender open so the source never signals EOF.
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    local
        .run_until(async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            cancel.cancel();
        })
        .await;

    let stats = local
        .run_until(async { handle.await.unwrap().unwrap() })
        .await;
    feeder.abort();

    assert_eq!(stats.iterations, 3, "all fed items processed before cancel");
    assert_eq!(stats.iteration_errors, 0);
}

// ---------------------------------------------------------------------------
// 3. Entry validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stream_requires_exactly_one_source() {
    let schema = test_schema();
    let (_tx_a, a) = channel_source(Arc::clone(&schema), 1);
    let (_tx_b, b) = channel_source(Arc::clone(&schema), 1);
    let (service, _seen) = built_service(vec![a, b], vec![], Arc::clone(&schema));

    let err = run_stream(service, CancellationToken::new(), None)
        .await
        .expect_err("two sources must be rejected");
    assert_eq!(err.category(), "configuration", "got: {err}");
    assert!(
        err.to_string()
            .contains("stream mode requires exactly one source (2 configured)"),
        "got: {err}"
    );
}

// ---------------------------------------------------------------------------
// 4. TCP ingest end-to-end
// ---------------------------------------------------------------------------

/// One frame = u32 big-endian length + one Arrow IPC stream payload.
fn frame(schema: &Arc<Schema>, values: &[i64]) -> Vec<u8> {
    let batch = make_batch(Arc::clone(schema), values);
    let mut payload: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let mut out = Vec::with_capacity(payload.len() + 4);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

async fn recv_timeout(rx: &mut mpsc::Receiver<RecordBatch>) -> RecordBatch {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("sink write timed out")
        .expect("sink channel closed")
}

/// Frames arriving over TCP drive the stream loop; an oversized frame kills
/// only its own connection, leaving the loop live for the next producer.
#[tokio::test]
async fn stream_ingests_tcp_frames_and_survives_a_bad_connection() {
    let schema = test_schema();
    let tcp = TcpIngestSource::new("127.0.0.1:0", Arc::clone(&schema), 8, 4096).unwrap();
    let addr = tcp.local_addr();

    let source = BuiltSource {
        name: "tcp_in".to_string(),
        target_component: COMP.to_string(),
        source: Box::new(tcp),
    };
    let (sink, mut rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, _seen) = built_service(vec![source], vec![sink], Arc::clone(&schema));

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move { run_stream(service, cancel_clone, None).await });

    let stats = local
        .run_until(async move {
            // Three good frames on one connection.
            let mut conn = TcpStream::connect(addr).await.unwrap();
            for i in 0..3i64 {
                conn.write_all(&frame(&schema, &[i])).await.unwrap();
            }
            conn.flush().await.unwrap();

            for i in 0..3i64 {
                let batch = recv_timeout(&mut rx).await;
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(col.value(0), i, "frames must arrive in order");
            }
            drop(conn);

            // An oversized frame: the header alone exceeds max_frame_bytes, so
            // the reader closes that connection without consuming the payload.
            let mut bad = TcpStream::connect(addr).await.unwrap();
            bad.write_all(&99_999u32.to_be_bytes()).await.unwrap();
            bad.flush().await.unwrap();
            drop(bad);

            // The listener is still up: a fresh connection still delivers.
            let mut conn = TcpStream::connect(addr).await.unwrap();
            conn.write_all(&frame(&schema, &[42])).await.unwrap();
            conn.flush().await.unwrap();
            let batch = recv_timeout(&mut rx).await;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(col.value(0), 42, "loop survives a bad connection");
            drop(conn);

            cancel.cancel();
            handle.await.unwrap().unwrap()
        })
        .await;

    assert_eq!(stats.iterations, 4, "3 good frames + 1 after the bad one");
    assert_eq!(stats.iteration_errors, 0);
}

// ---------------------------------------------------------------------------
// 5. run_standalone dispatch — the path `serve.rs` actually takes
// ---------------------------------------------------------------------------

fn stream_config() -> ServiceConfig {
    ServiceConfig {
        node: NodeConfig {
            id: 1,
            name: None,
            data_dir: std::path::PathBuf::from("/tmp/pcs-stream-test"),
        },
        mode: ServiceMode::Standalone {
            config: StandaloneConfig {
                run_mode: RunMode::Stream,
            },
        },
        pipeline: PipelineSpec {
            #[cfg(feature = "wasm")]
            wasm: None,
        },
        sources: vec![],
        sinks: vec![],
        http: HttpConfig::default(),
        observability: ObservabilityConfig::default(),
    }
}

/// `run_mode kind = "stream"` must route `run_standalone` into the stream
/// runner — per-item invocations with state carry, not the batch loop.
#[tokio::test]
async fn run_standalone_dispatches_stream_mode() {
    let schema = test_schema();
    let (tx, source) = channel_source(Arc::clone(&schema), 8);
    let (sink, mut rx) = channel_sink(Arc::clone(&schema), 16);
    let (service, seen) = built_service(vec![source], vec![sink], Arc::clone(&schema));

    for i in 0..4i64 {
        tx.send(make_batch(Arc::clone(&schema), &[i]))
            .await
            .unwrap();
    }
    drop(tx); // EOF

    let config = stream_config();
    let stats = run_standalone(service, &config, CancellationToken::new(), None)
        .await
        .unwrap();

    assert_eq!(stats.iterations, 4, "one iteration per item, not one batch");
    assert_eq!(stats.iteration_errors, 0);
    assert!(
        stats.max_item_micros > 0,
        "stream-only counters must be populated, proving the stream runner ran"
    );

    let decoded: Vec<u64> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|p| CounterRuntime::decode(p.as_deref()))
        .collect();
    assert_eq!(decoded, vec![0, 1, 2, 3], "state carried across items");

    let mut sink_batches = 0usize;
    while rx.try_recv().is_ok() {
        sink_batches += 1;
    }
    assert_eq!(sink_batches, 4);
}
