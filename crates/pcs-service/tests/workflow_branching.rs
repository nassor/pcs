//! Conditional fan-out routing end to end: a real `pcs-processor-smoketest`
//! WebAssembly component returning a `RouteDecision` from its `route` config
//! key, driven through the real `run_standalone` / `run_stream` runners with
//! real `ChannelSink`s, asserting branch-selected delivery, multi-branch fan,
//! legacy multicast and drop.
//!
//! Its own test binary because installing a meter provider is a
//! process-global one-shot; the same reason `workflow_metrics.rs` lives alone.
//!
//! ```bash
//! cargo build --release -p pcs-processor-smoketest --target wasm32-wasip2
//! cargo test --test workflow_branching -p pcs-service --features wasm,service
//! ```

#![cfg(all(feature = "wasm", feature = "service"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use pcs_connector_channel::{ChannelSink, ChannelSource};
use pcs_core::component::Component as _;
use pcs_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
use pcs_service::service::config::{
    HttpConfig, NodeConfig, ObservabilityConfig, RunMode, ServiceConfig, ServiceMode,
    StandaloneConfig, WorkflowSpec,
};
use pcs_service::service::registry::Registry;
use pcs_service::service::standalone::run_standalone;
use pcs_service::service::stream::run_stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[path = "common/smoketest.rs"]
mod smoketest;

use smoketest::{Ping, load_runtime};

fn config(run_mode: RunMode) -> ServiceConfig {
    ServiceConfig {
        node: NodeConfig {
            id: 1,
            name: None,
            data_dir: PathBuf::from("/tmp/pcs-branching-test"),
        },
        mode: ServiceMode::Standalone {
            config: StandaloneConfig { run_mode },
        },
        workflows: vec![WorkflowSpec {
            id: "w".to_string(),
            name: None,
            transformers: Vec::new(),
            sources: Vec::new(),
            #[cfg(feature = "wasm")]
            wasm: Vec::new(),
            #[cfg(feature = "plugin")]
            plugin: Vec::new(),
            sinks: Vec::new(),
            links: Vec::new(),
        }],
        http: HttpConfig::default(),
        store: None,
        observability: ObservabilityConfig::default(),
    }
}

/// The branching workflow: source `in` → processor `router` (the real wasm
/// smoketest with `config` injected) → sinks `out_a` (branch `a`) and `out_b`
/// (branch `b`), in topological order.
fn branching_built(
    config_map: HashMap<String, String>,
) -> (
    BuiltService,
    mpsc::Sender<RecordBatch>,
    mpsc::Receiver<RecordBatch>,
    mpsc::Receiver<RecordBatch>,
) {
    let (tx, source) = ChannelSource::new(Ping::schema(), 8);
    let (sink_a, rx_a) = ChannelSink::new(Ping::schema(), 8);
    let (sink_b, rx_b) = ChannelSink::new(Ping::schema(), 8);
    let nodes = vec![
        BuiltNode {
            id: "in".to_string(),
            name: None,
            type_name: "ChannelSource".to_string(),
            component: Some("Ping"),
            kind: BuiltNodeKind::Source(Box::new(source)),
            downstream: vec![BuiltEdge {
                node: 1,
                branch: None,
            }],
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
        },
        BuiltNode {
            id: "router".to_string(),
            name: None,
            type_name: "wasm".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(load_runtime(config_map)),
                kind: "wasm",
            },
            downstream: vec![
                BuiltEdge {
                    node: 2,
                    branch: Some("a".to_string()),
                },
                BuiltEdge {
                    node: 3,
                    branch: Some("b".to_string()),
                },
            ],
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
        },
        BuiltNode {
            id: "out_a".to_string(),
            name: None,
            type_name: "ChannelSink".to_string(),
            component: Some("Ping"),
            kind: BuiltNodeKind::Sink(Box::new(sink_a)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
        },
        BuiltNode {
            id: "out_b".to_string(),
            name: None,
            type_name: "ChannelSink".to_string(),
            component: Some("Ping"),
            kind: BuiltNodeKind::Sink(Box::new(sink_b)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
        },
    ];
    (
        BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(Registry::new()),
            inspector: None,
        },
        tx,
        rx_a,
        rx_b,
    )
}

fn three_row_batch() -> RecordBatch {
    RecordBatch::try_new(
        Ping::schema(),
        vec![Arc::new(UInt64Array::from(vec![1, 2, 3]))],
    )
    .expect("three-row batch")
}

#[tokio::test]
async fn routing_processor_delivers_only_to_the_selected_branch() {
    let (built, tx, mut rx_a, mut rx_b) =
        branching_built(HashMap::from([("route".to_string(), "a".to_string())]));
    tx.send(three_row_batch()).await.expect("send batch");
    drop(tx);

    run_standalone(
        built,
        &config(RunMode::OneShot),
        CancellationToken::new(),
        None,
        None,
    )
    .await
    .expect("run succeeds");

    assert_eq!(
        rx_a.recv()
            .await
            .expect("sink a received the batch")
            .num_rows(),
        3
    );
    assert!(
        rx_b.try_recv().is_err(),
        "sink b must not receive a batch routed to branch a"
    );
}

#[tokio::test]
async fn routing_processor_can_fan_to_several_branches() {
    let (built, tx, mut rx_a, mut rx_b) =
        branching_built(HashMap::from([("route".to_string(), "a,b".to_string())]));
    tx.send(three_row_batch()).await.expect("send batch");
    drop(tx);

    run_standalone(
        built,
        &config(RunMode::OneShot),
        CancellationToken::new(),
        None,
        None,
    )
    .await
    .expect("run succeeds");

    assert_eq!(rx_a.recv().await.expect("sink a").num_rows(), 3);
    assert_eq!(rx_b.recv().await.expect("sink b").num_rows(), 3);
}

#[tokio::test]
async fn legacy_processor_without_routes_multicasts_to_every_labelled_edge() {
    let (built, tx, mut rx_a, mut rx_b) = branching_built(HashMap::new());
    tx.send(three_row_batch()).await.expect("send batch");
    drop(tx);

    run_standalone(
        built,
        &config(RunMode::OneShot),
        CancellationToken::new(),
        None,
        None,
    )
    .await
    .expect("run succeeds");

    assert_eq!(rx_a.recv().await.expect("sink a").num_rows(), 3);
    assert_eq!(rx_b.recv().await.expect("sink b").num_rows(), 3);
}

#[tokio::test]
async fn stream_routing_processor_delivers_to_the_selected_branch() {
    let (built, tx, mut rx_a, mut rx_b) =
        branching_built(HashMap::from([("route".to_string(), "a".to_string())]));
    tx.send(three_row_batch()).await.expect("send item");
    drop(tx); // EOF

    run_stream(built, CancellationToken::new(), None, None)
        .await
        .expect("run succeeds");

    assert_eq!(
        rx_a.recv()
            .await
            .expect("sink a received the item")
            .num_rows(),
        3
    );
    assert!(
        rx_b.try_recv().is_err(),
        "sink b must not receive an item routed to branch a"
    );
}
