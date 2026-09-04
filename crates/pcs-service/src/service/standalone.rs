//! Standalone runner for [`BuiltService`].
//!
//! [`run_standalone`] drives a [`BuiltService`] through repeated processing
//! iterations in a single process with no distributed coordination, walking
//! every declared node in topological order each pass. It handles
//! cancellation, transient per-node errors, and run-mode pacing (one-shot,
//! continuous, or interval-based).
//!
//! ## Store-per-call semantics (WASM runtimes)
//!
//! With a `WasmPipelineRuntime`, every `run_on` call creates a fresh wasmtime
//! `Store` and resets processor linear memory. In-processor state that must
//! survive iterations (accumulators, window buffers, caches) has to be
//! round-tripped through the host with `snapshot`/`restore`, or it is
//! silently lost.
//!
//! Interval/one-shot state carry across iterations is opt-in: with
//! `store "redb" { batch_resume true }` the runner threads the processor
//! state blob as `prior` on every iteration and persists it to the local
//! redb file, so a restarted service resumes from its last save point.
//! Without the flag (the default) every iteration passes `None` as prior and
//! discards the output state, exactly as a run with no `store` block.
//!
//! ## One trace per iteration, at `debug`
//!
//! Each iteration opens a `workflow.batch` root span holding one `source.drain`
//! per source, one `runtime.run` per processor, and one `sink.write` per sink,
//! in topological order. The span closes before run-mode pacing, so its
//! duration is the iteration and not the wait after it. `runtime.run` is the
//! contextual parent of whatever the runtime opens: `pipeline.run` for a
//! native [`Pipeline`](pcs_core::Pipeline), `processor.batch` for a WASM
//! processor or a native plugin. That tree is the only host-side view of a
//! pipeline whose systems run inside a guest.
//!
//! The whole tree is `debug`: one opens per iteration, and materialising every
//! one of them costs more per item than the item. The default
//! `observability log_level="info"` therefore records none of it; set
//! `log_level="debug"` to get the per-iteration waterfall back. Error and
//! warning events do not depend on it — each names its own `workflow`,
//! `iteration` and node, so a failure is diagnosable at `info`.
//!
//! ## Example
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use tokio_util::sync::CancellationToken;
//! use pcs_service::service::standalone::{run_standalone, StandaloneStats};
//! // Build a BuiltService (via ServiceBuilder::build_all) then:
//! // let stats = run_standalone(built, &config, cancel, None, None).await?;
//! # }
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use serde::Serialize;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::Instrument as _;

use crate::dataset::Dataset;
use crate::error::PcsError;
use pcs_core::io::sink::Sink;
use pcs_core::io::source::Source;
use pcs_core::runtime::PipelineRuntime;

use super::builder::{BuiltNodeKind, BuiltService};
use super::config::StoreConfig;
use super::config::{RunMode, ServiceConfig, ServiceMode};
use super::redb_state::RedbStateClient;
#[cfg(feature = "windows")]
use super::windowing::WindowTracker;

/// Which of the three roles a node plays while it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeRunKind {
    Source,
    Processor,
    Sink,
}

impl NodeRunKind {
    fn as_str(self) -> &'static str {
        match self {
            NodeRunKind::Source => "source",
            NodeRunKind::Processor => "processor",
            NodeRunKind::Sink => "sink",
        }
    }
}

/// Cumulative counters for one workflow node, across every iteration.
#[derive(Debug, Default, Clone, Serialize)]
pub struct NodeRunStats {
    /// The node's declared id.
    pub id: String,
    /// `"source"`, `"processor"` or `"sink"`.
    pub kind: String,
    /// Rows this node produced (source, processor) or wrote (sink).
    pub rows: u64,
    /// Non-empty batches this node produced, ran, or wrote.
    pub batches: u64,
    /// Errors this node raised.
    pub errors: u64,
}

/// Diagnostic counters accumulated over a [`run_standalone`] call.
///
/// Fields are public so the HTTP control plane can expose them via `/metrics`
/// by reading from a shared `Arc<RwLock<StandaloneStats>>`.
#[derive(Debug, Default, Clone)]
pub struct StandaloneStats {
    /// Total number of completed scheduler iterations.
    pub iterations: u64,
    /// Total number of source drain calls that returned at least one row.
    pub source_batches_drained: u64,
    /// Total rows loaded from sources across all iterations.
    pub rows_processed: u64,
    /// Total number of sink drain calls that wrote at least one row.
    pub sink_batches_written: u64,
    /// Count of non-fatal errors (source, processor, or sink failures).
    pub iteration_errors: u64,
    /// Wall-clock time from the first iteration to the last, in milliseconds.
    pub total_duration_ms: u64,
    /// Sum of per-item processing time in microseconds. Stream mode only; the
    /// batch loop leaves this at 0.
    pub total_busy_micros: u64,
    /// Slowest single item, in microseconds. Stream mode only; the batch loop
    /// leaves this at 0.
    pub max_item_micros: u64,
    /// Per-node breakdown of the flat counters above, one entry per declared
    /// workflow node, in topological order.
    pub nodes: Vec<NodeRunStats>,
}

/// Write every batch staged for one sink, without finalising it.
async fn write_staged(
    sink: &mut dyn Sink,
    staged: &mut Vec<RecordBatch>,
    id: &str,
    workflow_id: &str,
    node_stat: &mut NodeRunStats,
    stats: &mut StandaloneStats,
) {
    for batch in staged.drain(..) {
        let rows = batch.num_rows() as u64;
        match sink.write_batch(&batch).await {
            Ok(()) => {
                node_stat.rows += rows;
                node_stat.batches += 1;
                stats.sink_batches_written += 1;
                crate::metrics::instruments().sink_batch(id);
            }
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    workflow = %workflow_id,
                    sink = id,
                    error = %_e,
                    "sink write error (continuing)"
                );
                stats.iteration_errors += 1;
                node_stat.errors += 1;
            }
        }
    }
}

/// Finalise one sink.
async fn finish_sink(
    sink: &mut dyn Sink,
    id: &str,
    workflow_id: &str,
    node_stat: &mut NodeRunStats,
    stats: &mut StandaloneStats,
) {
    if let Err(_e) = sink.finish().await {
        #[cfg(feature = "tracing")]
        tracing::error!(
            workflow = %workflow_id,
            sink = id,
            error = %_e,
            "sink finish error"
        );
        stats.iteration_errors += 1;
        node_stat.errors += 1;
    }
}

/// Write and finalise every sink's staged output. Used both for a normal
/// final iteration and for a cancellation that lands mid-pass, before the
/// main loop reaches every sink node on its own.
async fn flush_and_finish_all(
    sinks: &mut [Option<Box<dyn Sink>>],
    staged: &mut [Vec<RecordBatch>],
    ids: &[String],
    workflow_id: &str,
    node_stats: &mut [NodeRunStats],
    stats: &mut StandaloneStats,
) {
    for i in 0..sinks.len() {
        let Some(sink) = sinks[i].as_mut() else {
            continue;
        };
        write_staged(
            sink.as_mut(),
            &mut staged[i],
            &ids[i],
            workflow_id,
            &mut node_stats[i],
            stats,
        )
        .await;
        finish_sink(
            sink.as_mut(),
            &ids[i],
            workflow_id,
            &mut node_stats[i],
            stats,
        )
        .await;
    }
}

/// Drive a [`BuiltService`] through repeated processing iterations.
///
/// Returns [`StandaloneStats`] on success, including on cancellation, which is
/// a clean exit. `Err` is reserved for unrecoverable conditions such as an
/// internal invariant violation.
///
/// Each iteration checks cancellation, then walks every declared node in
/// topological order: a source drains into every downstream node, a processor
/// runs and forwards its output, and a sink writes whatever was staged for it.
/// Live stats publish to `live_stats` when it is `Some` so `GET /status` sees
/// current progress, then the run paces or exits according to [`RunMode`].
///
/// ## Error policy
///
/// - Source errors: log WARN, increment `iteration_errors`, stop draining that
///   source this iteration, continue to the next node.
/// - Processor errors: log ERROR, increment `iteration_errors`, record
///   `workflow_error`, skip its fan-out so a failed processor feeds nothing
///   downstream, still clear its dataset, continue.
/// - Sink errors: log ERROR, increment `iteration_errors`, continue.
/// - A `forward_into` or `append_record_batch` fan-out error: log WARN naming
///   both node ids, increment `iteration_errors`, continue.
pub async fn run_standalone(
    built: BuiltService,
    config: &ServiceConfig,
    cancel: CancellationToken,
    live_stats: Option<Arc<RwLock<StandaloneStats>>>,
    state: Option<Arc<RedbStateClient>>,
) -> Result<StandaloneStats, PcsError> {
    let run_mode = match &config.mode {
        ServiceMode::Standalone { config: sc } => sc.run_mode.clone(),
        ServiceMode::Cluster { .. } => {
            return Err(PcsError::configuration(
                "run_standalone called with a cluster-mode config; use the cluster runner instead",
            ));
        }
    };

    // Stream mode is a different loop shape entirely: one workflow invocation
    // per arriving batch, no inter-item pacing.
    if run_mode == RunMode::Stream {
        return super::stream::run_stream(built, cancel, live_stats, state).await;
    }

    let BuiltService {
        workflow_id, nodes, ..
    } = built;
    let n = nodes.len();

    // Parallel per-node vectors, so the borrow checker sees disjoint fields
    // rather than one `Vec` of trait objects being indexed twice: `runtimes`
    // and `datasets` are different fields, so a processor step borrows them
    // disjointly, and `datasets.split_at_mut(i + 1)` gives a processor's own
    // dataset and a downstream one as two non-overlapping slices.
    let mut ids: Vec<String> = Vec::with_capacity(n);
    let mut components: Vec<Option<&'static str>> = Vec::with_capacity(n);
    let mut downstream: Vec<Vec<crate::service::builder::BuiltEdge>> = Vec::with_capacity(n);
    let mut kinds: Vec<NodeRunKind> = Vec::with_capacity(n);
    let mut sources: Vec<Option<Box<dyn Source>>> = Vec::with_capacity(n);
    let mut runtimes: Vec<Option<Box<dyn PipelineRuntime>>> = Vec::with_capacity(n);
    let mut datasets: Vec<Option<Dataset>> = Vec::with_capacity(n);
    let mut sinks: Vec<Option<Box<dyn Sink>>> = Vec::with_capacity(n);
    let mut node_stats: Vec<NodeRunStats> = Vec::with_capacity(n);
    // One watermark tracker per windowed processor node; `None` everywhere
    // else. The tracker survives across iterations, so the watermark is
    // monotonic over the whole run, exactly like the guest-side state a
    // windowed processor keeps in its checkpoint blob.
    #[cfg(feature = "windows")]
    let mut trackers: Vec<Option<WindowTracker>> = Vec::with_capacity(n);

    for node in nodes {
        ids.push(node.id);
        components.push(node.component);
        downstream.push(node.downstream);
        #[cfg(feature = "windows")]
        trackers.push(node.window.map(WindowTracker::new));

        let (kind, source, runtime, dataset, sink) = match node.kind {
            BuiltNodeKind::Source(source) => (NodeRunKind::Source, Some(source), None, None, None),
            BuiltNodeKind::Processor { runtime, .. } => {
                let dataset = runtime.template_dataset();
                (
                    NodeRunKind::Processor,
                    None,
                    Some(runtime),
                    Some(dataset),
                    None,
                )
            }
            BuiltNodeKind::Sink(sink) => (NodeRunKind::Sink, None, None, None, Some(sink)),
        };
        kinds.push(kind);
        sources.push(source);
        runtimes.push(runtime);
        datasets.push(dataset);
        sinks.push(sink);
        node_stats.push(NodeRunStats {
            id: ids.last().expect("just pushed").clone(),
            kind: kind.as_str().to_string(),
            rows: 0,
            batches: 0,
            errors: 0,
        });
    }

    let mut staged: Vec<Vec<RecordBatch>> = vec![Vec::new(); n];

    let mut stats = StandaloneStats::default();
    let start = Instant::now();

    #[cfg(feature = "tracing")]
    tracing::info!(workflow = %workflow_id, mode = ?run_mode, "standalone runner starting");
    #[cfg(not(feature = "tracing"))]
    let _ = &workflow_id;
    // Interval/one-shot processor state carry, opt-in per store: with
    // `store "redb" { batch_resume true }` the runner threads the processor
    // state blob as `prior` and persists it, so a restarted service resumes
    // from its last save point. Default (no store or no flag): per-call
    // fresh-Store behaviour.
    let batch_resume = matches!(
        &config.store,
        Some(StoreConfig::Redb {
            batch_resume: true,
            ..
        })
    );
    let mut prior: Vec<Option<Vec<u8>>> = vec![None; n];
    if batch_resume && let Some(client) = &state {
        for i in 0..n {
            if matches!(kinds[i], NodeRunKind::Processor) {
                prior[i] = client.load_prior(&workflow_id, &ids[i]).await?;
            }
        }
    }

    loop {
        // One root span per iteration, which is the trace the dashboard draws.
        // Children name it as their explicit parent instead of entering it: the
        // loop awaits, and an entered guard held across an await would adopt
        // every span the runtime opens on this thread meanwhile.
        //
        // `debug`, not `info`: one tree of these opens per iteration, and the
        // default `pcs=info` filter is what keeps a subscriber from
        // materialising every one of them. `log_level="debug"` brings the
        // per-iteration traces back. Every error event below therefore names
        // its own workflow, iteration and node rather than leaning on these
        // fields.
        #[cfg(feature = "tracing")]
        let batch_span = tracing::debug_span!(
            "workflow.batch",
            workflow = %workflow_id,
            iteration = stats.iterations + 1,
            rows = tracing::field::Empty
        );

        if cancel.is_cancelled() {
            #[cfg(feature = "tracing")]
            tracing::info!(parent: &batch_span, "standalone runner cancelled, draining in-flight work");
            let flush = flush_and_finish_all(
                &mut sinks,
                &mut staged,
                &ids,
                &workflow_id,
                &mut node_stats,
                &mut stats,
            );
            #[cfg(feature = "tracing")]
            flush.instrument(batch_span.clone()).await;
            #[cfg(not(feature = "tracing"))]
            flush.await;
            break;
        }

        let iter_start = Instant::now();
        // Per-iteration progress, so `debug` alongside the span tree it
        // annotates; the runner's start and shutdown lines stay at `info`.
        #[cfg(feature = "tracing")]
        tracing::debug!(parent: &batch_span, workflow = %workflow_id, iteration = stats.iterations + 1, mode = ?run_mode, "iteration starting");

        let mut total_rows_in: u64 = 0;
        let mut cancelled_mid_pass = false;

        for i in 0..n {
            match kinds[i] {
                NodeRunKind::Source => {
                    let component =
                        components[i].expect("a source node always declares a component");
                    #[cfg(feature = "tracing")]
                    let drain_span = tracing::debug_span!(
                        parent: &batch_span,
                        "source.drain",
                        workflow = %workflow_id,
                        source = %ids[i],
                        component,
                        rows = tracing::field::Empty
                    );
                    let mut source_rows: u64 = 0;

                    loop {
                        let source = sources[i].as_mut().expect("source node keeps its source");
                        let next = tokio::select! {
                            r = source.next_batch() => Some(r),
                            _ = cancel.cancelled() => None,
                        };
                        let Some(result) = next else {
                            #[cfg(feature = "tracing")]
                            tracing::info!(parent: &batch_span, "standalone runner cancelled during source drain");
                            cancelled_mid_pass = true;
                            break;
                        };
                        match result {
                            Ok(None) => break,
                            Ok(Some(batch)) => {
                                let rows = batch.num_rows() as u64;
                                source_rows += rows;
                                total_rows_in += rows;
                                stats.source_batches_drained += 1;
                                stats.rows_processed += rows;
                                node_stats[i].rows += rows;
                                node_stats[i].batches += 1;
                                crate::metrics::instruments().source_batch(&ids[i]);
                                crate::metrics::instruments().rows(&ids[i], rows);

                                for edge in &downstream[i] {
                                    let d = edge.node;
                                    match kinds[d] {
                                        NodeRunKind::Processor => {
                                            if let Err(_e) = datasets[d]
                                                .as_mut()
                                                .expect("processor node keeps its dataset")
                                                .append_record_batch(component, batch.clone())
                                            {
                                                #[cfg(feature = "tracing")]
                                                tracing::warn!(
                                                    parent: &batch_span,
                                                    workflow = %workflow_id,
                                                    iteration = stats.iterations + 1,
                                                    from = %ids[i], to = %ids[d], error = %_e,
                                                    "fan-out append error (continuing)"
                                                );
                                                stats.iteration_errors += 1;
                                            }
                                        }
                                        NodeRunKind::Sink => staged[d].push(batch.clone()),
                                        NodeRunKind::Source => {
                                            unreachable!("a source is never a link target")
                                        }
                                    }
                                }
                            }
                            Err(_e) => {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    parent: &batch_span,
                                    workflow = %workflow_id,
                                    iteration = stats.iterations + 1,
                                    source = %ids[i],
                                    error = %_e,
                                    "source drain error (continuing)"
                                );
                                stats.iteration_errors += 1;
                                node_stats[i].errors += 1;
                                break;
                            }
                        }
                    }

                    #[cfg(feature = "tracing")]
                    drain_span.record("rows", source_rows);
                    #[cfg(not(feature = "tracing"))]
                    let _ = source_rows;

                    if cancelled_mid_pass {
                        break;
                    }
                }

                NodeRunKind::Processor => {
                    // The fan-in merge is complete once every upstream node has
                    // run: sources appended their batches directly and upstream
                    // processors forwarded their datasets. A windowed node's
                    // watermark therefore advances from everything this
                    // iteration delivered, before the runtime sees the batch.
                    #[cfg(feature = "windows")]
                    if let Some(tracker) = trackers[i].as_mut() {
                        match tracker.advance_from(
                            datasets[i]
                                .as_ref()
                                .expect("processor node keeps its dataset"),
                        ) {
                            Ok(()) => {
                                let dataset = datasets[i]
                                    .as_mut()
                                    .expect("processor node keeps its dataset");
                                dataset.insert_resource(pcs_core::windows::WindowWatermark(
                                    tracker.watermark_ms(),
                                ));
                                if tracker.has_watermark() {
                                    crate::metrics::instruments()
                                        .window_watermark(&ids[i], tracker.watermark_seconds());
                                }
                            }
                            Err(_e) => {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    parent: &batch_span,
                                    workflow = %workflow_id,
                                    iteration = stats.iterations + 1,
                                    processor = %ids[i],
                                    error = %_e,
                                    "window watermark advance error (continuing without it)"
                                );
                                #[cfg(not(feature = "tracing"))]
                                let _ = _e;
                                stats.iteration_errors += 1;
                            }
                        }
                    }

                    let rows_in = datasets[i]
                        .as_ref()
                        .expect("processor node keeps its dataset")
                        .rows() as u64;
                    // `runtime.run` is the seam the out-of-process runtimes hang
                    // from: it is the contextual parent of a native pipeline's
                    // `pipeline.run` and of a processor's host-side
                    // `processor.batch`.
                    #[cfg(feature = "tracing")]
                    let run_span = tracing::debug_span!(
                        parent: &batch_span,
                        "runtime.run",
                        workflow = %workflow_id,
                        processor = %ids[i],
                        rows_in,
                        rows_out = tracing::field::Empty
                    );
                    let runtime = runtimes[i]
                        .as_ref()
                        .expect("processor node keeps its runtime");
                    let dataset = datasets[i]
                        .as_mut()
                        .expect("processor node keeps its dataset");
                    let prior_blob = if batch_resume {
                        prior[i].as_deref()
                    } else {
                        None
                    };
                    let run = runtime.run_on_with_state_and_routes(dataset, prior_blob);
                    #[cfg(feature = "tracing")]
                    let run = run.instrument(run_span.clone());
                    let run_result = tokio::select! {
                        r = run => Some(r),
                        _ = cancel.cancelled() => None,
                    };

                    let Some(run_result) = run_result else {
                        #[cfg(feature = "tracing")]
                        tracing::info!(parent: &batch_span, "standalone runner cancelled during runtime run");
                        cancelled_mid_pass = true;
                        break;
                    };

                    match run_result {
                        Ok(out) => {
                            let rows_out = datasets[i]
                                .as_ref()
                                .expect("processor node keeps its dataset")
                                .rows() as u64;
                            #[cfg(feature = "tracing")]
                            run_span.record("rows_out", rows_out);
                            #[cfg(not(feature = "tracing"))]
                            let _ = rows_out;
                            node_stats[i].rows += rows_out;
                            node_stats[i].batches += 1;
                            // `out.state` is threaded and persisted only when
                            // interval/one-shot batch resume is opted in via
                            // `store "redb" { batch_resume true }`; otherwise
                            // it is discarded, keeping today's per-call
                            // fresh-Store behaviour.
                            if batch_resume {
                                prior[i] = out.state;
                                if let Some(client) = &state {
                                    let result = match &prior[i] {
                                        Some(blob) => {
                                            client.save_prior(&workflow_id, &ids[i], blob).await
                                        }
                                        None => client.delete_prior(&workflow_id, &ids[i]).await,
                                    };
                                    if let Err(_e) = result {
                                        #[cfg(feature = "tracing")]
                                        tracing::warn!(
                                            parent: &batch_span,
                                            workflow = %workflow_id,
                                            iteration = stats.iterations + 1,
                                            processor = %ids[i],
                                            error = %_e,
                                            "persisting processor state failed (continuing)"
                                        );
                                        #[cfg(not(feature = "tracing"))]
                                        let _ = _e;
                                    }
                                }
                            } else {
                                let _ = out.state;
                            }

                            let routes = &out.routes;
                            for name in routes.iter().flatten() {
                                if !downstream[i]
                                    .iter()
                                    .any(|e| e.branch.as_deref() == Some(name.as_str()))
                                {
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(
                                        parent: &batch_span,
                                        workflow = %workflow_id,
                                        iteration = stats.iterations + 1,
                                        processor = %ids[i],
                                        branch = %name,
                                        "routing decision names a branch no link carries (continuing)"
                                    );
                                    #[cfg(not(feature = "tracing"))]
                                    let _ = name;
                                }
                            }

                            for edge in &downstream[i] {
                                let d = edge.node;
                                if !crate::service::builder::edge_selected(routes, &edge.branch) {
                                    continue;
                                }
                                if let Some(branch) = &edge.branch {
                                    crate::metrics::instruments()
                                        .processor_branch_rows(&ids[i], branch, rows_out);
                                }
                                match kinds[d] {
                                    NodeRunKind::Processor => {
                                        let (left, right) = datasets.split_at_mut(i + 1);
                                        let src = left[i].as_ref().expect("processor dataset");
                                        let dst = right[d - i - 1]
                                            .as_mut()
                                            .expect("downstream processor dataset");
                                        if let Err(_e) = src.forward_into(dst) {
                                            #[cfg(feature = "tracing")]
                                            tracing::warn!(
                                                parent: &batch_span,
                                                workflow = %workflow_id,
                                                iteration = stats.iterations + 1,
                                                from = %ids[i], to = %ids[d], error = %_e,
                                                "fan-out forward error (continuing)"
                                            );
                                            stats.iteration_errors += 1;
                                        }
                                    }
                                    NodeRunKind::Sink => {
                                        let component = components[d]
                                            .expect("a sink node always declares a component");
                                        if let Some(batch) = datasets[i]
                                            .as_ref()
                                            .expect("processor dataset")
                                            .batch_for(component)
                                            .cloned()
                                            && batch.num_rows() > 0
                                        {
                                            staged[d].push(batch);
                                        }
                                    }
                                    NodeRunKind::Source => {
                                        unreachable!("a source is never a link target")
                                    }
                                }
                            }
                        }
                        Err(_e) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!(
                                parent: &run_span,
                                workflow = %workflow_id,
                                iteration = stats.iterations + 1,
                                processor = %ids[i],
                                error = %_e,
                                "processor error (continuing, skipping fan-out)"
                            );
                            stats.iteration_errors += 1;
                            node_stats[i].errors += 1;
                            crate::metrics::instruments().workflow_error(&workflow_id);
                            // Fall through to clear() without fanning out.
                        }
                    }

                    datasets[i]
                        .as_mut()
                        .expect("processor node keeps its dataset")
                        .clear();
                }

                NodeRunKind::Sink => {
                    let component = components[i].expect("a sink node always declares a component");
                    #[cfg(feature = "tracing")]
                    let write_span = tracing::debug_span!(
                        parent: &batch_span,
                        "sink.write",
                        workflow = %workflow_id,
                        sink = %ids[i],
                        component,
                        rows = tracing::field::Empty
                    );
                    let rows_before: u64 = staged[i].iter().map(|b| b.num_rows() as u64).sum();
                    let sink = sinks[i].as_mut().expect("sink node keeps its sink");
                    let write = write_staged(
                        sink.as_mut(),
                        &mut staged[i],
                        &ids[i],
                        &workflow_id,
                        &mut node_stats[i],
                        &mut stats,
                    );
                    #[cfg(feature = "tracing")]
                    write.instrument(write_span.clone()).await;
                    #[cfg(not(feature = "tracing"))]
                    write.await;
                    #[cfg(feature = "tracing")]
                    write_span.record("rows", rows_before);
                    #[cfg(not(feature = "tracing"))]
                    let _ = rows_before;
                }
            }
        }

        if cancelled_mid_pass {
            let flush = flush_and_finish_all(
                &mut sinks,
                &mut staged,
                &ids,
                &workflow_id,
                &mut node_stats,
                &mut stats,
            );
            #[cfg(feature = "tracing")]
            flush.instrument(batch_span.clone()).await;
            #[cfg(not(feature = "tracing"))]
            flush.await;
            stats.total_duration_ms = start.elapsed().as_millis() as u64;
            stats.nodes = node_stats.clone();
            return Ok(stats);
        }

        #[cfg(feature = "tracing")]
        batch_span.record("rows", total_rows_in);
        #[cfg(not(feature = "tracing"))]
        let _ = total_rows_in;

        let is_oneshot_final = run_mode == RunMode::OneShot;
        let cancelled_before_finish = cancel.is_cancelled();
        if is_oneshot_final || cancelled_before_finish {
            let finish = async {
                for i in 0..n {
                    if let Some(sink) = sinks[i].as_mut() {
                        finish_sink(
                            sink.as_mut(),
                            &ids[i],
                            &workflow_id,
                            &mut node_stats[i],
                            &mut stats,
                        )
                        .await;
                    }
                }
            };
            #[cfg(feature = "tracing")]
            finish.instrument(batch_span.clone()).await;
            #[cfg(not(feature = "tracing"))]
            finish.await;
        }

        stats.iterations += 1;
        crate::metrics::instruments().workflow_run(&workflow_id);
        let iter_ms = iter_start.elapsed().as_millis() as u64;

        stats.nodes = node_stats.clone();
        if let Some(shared) = &live_stats {
            *shared.write().await = stats.clone();
        }

        // Every processor dataset was already cleared per-node above; a
        // source or sink node has none to clear.

        // Per-iteration progress, so `debug` alongside the span tree it
        // annotates; the runner's shutdown summary stays at `info`.
        #[cfg(feature = "tracing")]
        tracing::debug!(
            parent: &batch_span,
            workflow = %workflow_id,
            iteration = stats.iterations,
            rows_processed = stats.rows_processed,
            duration_ms = iter_ms,
            "iteration complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = iter_ms;

        // Close the trace here: run-mode pacing is the gap between iterations,
        // not part of one.
        #[cfg(feature = "tracing")]
        drop(batch_span);

        if cancelled_before_finish {
            #[cfg(feature = "tracing")]
            tracing::info!("standalone runner cancelled after runtime, clean exit");
            break;
        }

        match &run_mode {
            RunMode::OneShot => {
                #[cfg(feature = "tracing")]
                tracing::info!("one-shot mode: exiting after first iteration");
                break;
            }

            RunMode::Continuous => {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {}
                    _ = cancel.cancelled() => {
                        #[cfg(feature = "tracing")]
                        tracing::info!("standalone runner cancelled during continuous pause");
                        break;
                    }
                }
            }

            RunMode::Interval { interval_ms } => {
                let interval = tokio::time::Duration::from_millis(*interval_ms);
                let deadline = tokio::time::Instant::now() + interval;

                loop {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let slice = remaining.min(tokio::time::Duration::from_millis(100));
                    tokio::select! {
                        _ = tokio::time::sleep(slice) => {}
                        _ = cancel.cancelled() => {
                            #[cfg(feature = "tracing")]
                            tracing::info!("standalone runner cancelled during interval sleep");
                            stats.total_duration_ms = start.elapsed().as_millis() as u64;
                            return Ok(stats);
                        }
                    }
                }
            }

            // Dispatched to `super::stream::run_stream` before the loop starts.
            RunMode::Stream => unreachable!("stream mode never reaches the batch loop"),
        }
    }

    stats.total_duration_ms = start.elapsed().as_millis() as u64;
    stats.nodes = node_stats.clone();

    #[cfg(feature = "tracing")]
    tracing::info!(
        iterations = stats.iterations,
        rows_processed = stats.rows_processed,
        iteration_errors = stats.iteration_errors,
        total_duration_ms = stats.total_duration_ms,
        "standalone runner clean shutdown"
    );

    Ok(stats)
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::pipeline::Pipeline;
    use crate::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind, BuiltService};
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, RunMode as CfgRunMode,
        ServiceMode as CfgServiceMode, StandaloneConfig,
    };
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use pcs_connector_channel::{ChannelSink, ChannelSource};
    use pcs_core::PcsResult;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn config(run_mode: CfgRunMode) -> ServiceConfig {
        ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-standalone-test"),
            },
            mode: CfgServiceMode::Standalone {
                config: StandaloneConfig { run_mode },
            },
            workflows: vec![crate::service::config::WorkflowSpec {
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
            variables: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn source_straight_to_sink_forwards_every_row() {
        let (tx, source) = ChannelSource::new(schema(), 8);
        let (sink, mut rx) = ChannelSink::new(schema(), 8);

        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        tx.send(batch.clone()).await.unwrap();
        drop(tx);

        let nodes = vec![
            BuiltNode {
                id: "in".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some("V"),
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
                id: "out".to_string(),
                name: None,
                type_name: "ChannelSink".to_string(),
                component: Some("V"),
                kind: BuiltNodeKind::Sink(Box::new(sink)),
                downstream: Vec::new(),
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
        ];
        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
        };

        let stats = run_standalone(
            built,
            &config(CfgRunMode::OneShot),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("run succeeds");

        assert_eq!(stats.rows_processed, 3);
        assert_eq!(stats.sink_batches_written, 1);
        assert_eq!(stats.nodes.len(), 2);
        assert_eq!(stats.nodes[0].id, "in");
        assert_eq!(stats.nodes[0].rows, 3);
        assert_eq!(stats.nodes[1].id, "out");
        assert_eq!(stats.nodes[1].rows, 3);

        let received = rx.recv().await.expect("sink forwarded the batch");
        assert_eq!(received.num_rows(), 3);
    }

    #[tokio::test]
    async fn processor_entry_point_runs_with_an_empty_dataset() {
        let (sink, mut rx) = ChannelSink::new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            8,
        );

        struct OrderComponent;
        impl pcs_core::component::Component for OrderComponent {
            fn name() -> &'static str {
                "Order"
            }
            fn schema() -> Arc<Schema> {
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
            }
        }

        let mut pipeline = Pipeline::new("p");
        pipeline
            .data_mut()
            .register_component::<OrderComponent>()
            .unwrap();

        let nodes = vec![
            BuiltNode {
                id: "p".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(pipeline),
                    kind: "native",
                },
                downstream: vec![BuiltEdge {
                    node: 1,
                    branch: None,
                }],
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
            BuiltNode {
                id: "out".to_string(),
                name: None,
                type_name: "ChannelSink".to_string(),
                component: Some("Order"),
                kind: BuiltNodeKind::Sink(Box::new(sink)),
                downstream: Vec::new(),
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
        ];
        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
        };

        let stats = run_standalone(
            built,
            &config(CfgRunMode::OneShot),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("run succeeds even though the processor starts from an empty dataset");

        assert_eq!(stats.iterations, 1);
        assert_eq!(stats.iteration_errors, 0);
        assert!(
            rx.try_recv().is_err(),
            "an identity run over zero rows writes nothing"
        );
    }

    /// A runtime that appends three rows to the batch dataset and reports a
    /// fixed routing decision, so the runner's delivery is what a test asserts.
    struct RoutingRuntime {
        routes: Option<Vec<String>>,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct V {
        v: i32,
    }

    impl pcs_core::component::Component for V {
        fn name() -> &'static str {
            "V"
        }
        fn schema() -> Arc<Schema> {
            schema()
        }
    }

    #[async_trait(?Send)]
    impl PipelineRuntime for RoutingRuntime {
        fn name(&self) -> &str {
            "routing"
        }

        async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
            self.run_on_with_state_and_routes(data, None)
                .await
                .map(|_| ())
        }

        async fn run_on_with_state_and_routes(
            &self,
            data: &mut Dataset,
            _prior: Option<&[u8]>,
        ) -> PcsResult<pcs_core::runtime::RuntimeOutput> {
            data.append::<V>(&[V { v: 1 }, V { v: 2 }, V { v: 3 }])?;
            Ok(pcs_core::runtime::RuntimeOutput {
                state: None,
                routes: self.routes.clone(),
            })
        }

        fn template_dataset(&self) -> Dataset {
            let mut dataset = Dataset::new();
            dataset.register_component::<V>().expect("register V");
            dataset
        }
    }

    /// A one-processor workflow with two labelled sink edges, `a` and `b`.
    fn routing_built(
        routes: Option<Vec<String>>,
    ) -> (
        BuiltService,
        tokio::sync::mpsc::Receiver<RecordBatch>,
        tokio::sync::mpsc::Receiver<RecordBatch>,
    ) {
        let (sink_a, rx_a) = ChannelSink::new(schema(), 8);
        let (sink_b, rx_b) = ChannelSink::new(schema(), 8);
        let nodes = vec![
            BuiltNode {
                id: "p".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(RoutingRuntime { routes }),
                    kind: "native",
                },
                downstream: vec![
                    BuiltEdge {
                        node: 1,
                        branch: Some("a".to_string()),
                    },
                    BuiltEdge {
                        node: 2,
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
                component: Some("V"),
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
                component: Some("V"),
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
                registry: Arc::new(crate::service::registry::Registry::new()),
                inspector: None,
            },
            rx_a,
            rx_b,
        )
    }

    #[tokio::test]
    async fn routing_processor_delivers_only_to_the_selected_branch() {
        let (built, mut rx_a, mut rx_b) = routing_built(Some(vec!["a".to_string()]));
        run_standalone(
            built,
            &config(CfgRunMode::OneShot),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("run succeeds");

        let received = rx_a.recv().await.expect("sink a received the batch");
        assert_eq!(received.num_rows(), 3);
        assert!(
            rx_b.try_recv().is_err(),
            "sink b must not receive a batch the routing decision did not select"
        );
    }

    #[tokio::test]
    async fn routing_processor_can_route_to_nowhere() {
        let (built, mut rx_a, mut rx_b) = routing_built(Some(Vec::new()));
        run_standalone(
            built,
            &config(CfgRunMode::OneShot),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("run succeeds");

        assert!(
            rx_a.try_recv().is_err(),
            "an empty routing decision delivers nowhere"
        );
        assert!(rx_b.try_recv().is_err());
    }

    #[tokio::test]
    async fn routing_processor_without_routes_multicasts_to_every_edge() {
        let (built, mut rx_a, mut rx_b) = routing_built(None);
        run_standalone(
            built,
            &config(CfgRunMode::OneShot),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("run succeeds");

        assert_eq!(rx_a.recv().await.expect("sink a").num_rows(), 3);
        assert_eq!(rx_b.recv().await.expect("sink b").num_rows(), 3);
    }

    /// A runtime that counts the rows its dataset held when run began, so a
    /// test can assert how much fan-in merged before the call.
    struct RowCounter {
        rows_seen: Arc<std::sync::Mutex<u64>>,
    }

    #[async_trait(?Send)]
    impl PipelineRuntime for RowCounter {
        fn name(&self) -> &str {
            "row-counter"
        }

        async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
            *self.rows_seen.lock().unwrap() += data.rows() as u64;
            Ok(())
        }

        fn template_dataset(&self) -> Dataset {
            let mut dataset = Dataset::new();
            dataset.register_component::<V>().expect("register V");
            dataset
        }
    }

    /// Two sources feeding one processor must merge into a single dataset
    /// before the processor runs: the windowing contract is that a processor
    /// receives the rows of every one of its inbound nodes in one batch.
    #[tokio::test]
    async fn two_sources_merge_into_one_processor() {
        let (tx_a, source_a) = ChannelSource::new(schema(), 8);
        let (tx_b, source_b) = ChannelSource::new(schema(), 8);
        let rows_seen = Arc::new(std::sync::Mutex::new(0u64));

        let batch_a = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let batch_b = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![3, 4, 5]))],
        )
        .unwrap();
        tx_a.send(batch_a).await.unwrap();
        tx_b.send(batch_b).await.unwrap();
        drop(tx_a);
        drop(tx_b);

        let nodes = vec![
            BuiltNode {
                id: "a".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some("V"),
                kind: BuiltNodeKind::Source(Box::new(source_a)),
                downstream: vec![BuiltEdge {
                    node: 2,
                    branch: None,
                }],
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
            BuiltNode {
                id: "b".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some("V"),
                kind: BuiltNodeKind::Source(Box::new(source_b)),
                downstream: vec![BuiltEdge {
                    node: 2,
                    branch: None,
                }],
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
            BuiltNode {
                id: "p".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(RowCounter {
                        rows_seen: Arc::clone(&rows_seen),
                    }),
                    kind: "native",
                },
                downstream: Vec::new(),
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
        ];
        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
        };

        let stats = run_standalone(
            built,
            &config(CfgRunMode::OneShot),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("run succeeds");

        assert_eq!(stats.rows_processed, 5);
        assert_eq!(
            *rows_seen.lock().unwrap(),
            5,
            "the processor must receive both sources' rows merged into one dataset"
        );
    }

    /// A mixed fan-in — one source and one upstream processor feeding the same
    /// downstream processor — must merge just like the all-sources case.
    #[tokio::test]
    async fn source_and_processor_fan_in_merge_into_one_processor() {
        let (tx, source) = ChannelSource::new(schema(), 8);
        let rows_seen = Arc::new(std::sync::Mutex::new(0u64));

        let batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![7, 8]))],
        )
        .unwrap();
        tx.send(batch).await.unwrap();
        drop(tx);

        // Upstream: appends three rows of its own on every run.
        struct Producer;
        #[async_trait(?Send)]
        impl PipelineRuntime for Producer {
            fn name(&self) -> &str {
                "producer"
            }
            async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
                data.append::<V>(&[V { v: 9 }, V { v: 10 }, V { v: 11 }])?;
                Ok(())
            }
            fn template_dataset(&self) -> Dataset {
                let mut dataset = Dataset::new();
                dataset.register_component::<V>().expect("register V");
                dataset
            }
        }

        let nodes = vec![
            BuiltNode {
                id: "s".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some("V"),
                kind: BuiltNodeKind::Source(Box::new(source)),
                downstream: vec![BuiltEdge {
                    node: 2,
                    branch: None,
                }],
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
            BuiltNode {
                id: "up".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(Producer),
                    kind: "native",
                },
                downstream: vec![BuiltEdge {
                    node: 2,
                    branch: None,
                }],
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
            BuiltNode {
                id: "down".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(RowCounter {
                        rows_seen: Arc::clone(&rows_seen),
                    }),
                    kind: "native",
                },
                downstream: Vec::new(),
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            },
        ];
        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
        };

        let stats = run_standalone(
            built,
            &config(CfgRunMode::OneShot),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("run succeeds");

        assert_eq!(stats.rows_processed, 2);
        assert_eq!(
            *rows_seen.lock().unwrap(),
            5,
            "the downstream processor must see the source's 2 rows and the upstream's 3"
        );
    }

    /// A windowed processor node: the runner advances the node's watermark
    /// from the merged inbound timestamps, inserts the `WindowWatermark`
    /// resource for an in-process runtime to read, and records the
    /// `pcs_window_watermark_seconds` series attributed to the node.
    #[cfg(feature = "windows")]
    #[tokio::test]
    async fn windowed_processor_tracks_watermark_from_merged_input() {
        use pcs_core::windows::{WindowSpec, WindowWatermark};

        fn trade_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("timestamp_ms", DataType::Int64, false),
                Field::new("price", DataType::Float64, false),
            ]))
        }

        let (tx_a, source_a) = ChannelSource::new(trade_schema(), 8);
        let (tx_b, source_b) = ChannelSource::new(trade_schema(), 8);
        let watermark_seen = Arc::new(std::sync::Mutex::new(i64::MIN));

        struct WatermarkReader {
            seen: Arc<std::sync::Mutex<i64>>,
        }
        #[async_trait(?Send)]
        impl PipelineRuntime for WatermarkReader {
            fn name(&self) -> &str {
                "watermark-reader"
            }
            async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
                if let Some(watermark) = data.get_resource::<WindowWatermark>() {
                    *self.seen.lock().unwrap() = watermark.as_ms();
                }
                Ok(())
            }
            fn template_dataset(&self) -> Dataset {
                let mut dataset = Dataset::new();
                dataset.register_raw_component("Trade", trade_schema());
                dataset
            }
        }

        let batch_at = |ts: i64| {
            RecordBatch::try_new(
                trade_schema(),
                vec![
                    Arc::new(arrow_array::Int64Array::from(vec![ts]))
                        as Arc<dyn arrow_array::Array>,
                    Arc::new(arrow_array::Float64Array::from(vec![1.0]))
                        as Arc<dyn arrow_array::Array>,
                ],
            )
            .unwrap()
        };
        tx_a.send(batch_at(1_000)).await.unwrap();
        tx_a.send(batch_at(2_000)).await.unwrap();
        tx_b.send(batch_at(3_000)).await.unwrap();
        drop(tx_a);
        drop(tx_b);

        let nodes = vec![
            BuiltNode {
                id: "a".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some("Trade"),
                kind: BuiltNodeKind::Source(Box::new(source_a)),
                downstream: vec![BuiltEdge {
                    node: 2,
                    branch: None,
                }],
                artifact: None,
                window: None,
            },
            BuiltNode {
                id: "b".to_string(),
                name: None,
                type_name: "ChannelSource".to_string(),
                component: Some("Trade"),
                kind: BuiltNodeKind::Source(Box::new(source_b)),
                downstream: vec![BuiltEdge {
                    node: 2,
                    branch: None,
                }],
                artifact: None,
                window: None,
            },
            BuiltNode {
                id: "p".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(WatermarkReader {
                        seen: Arc::clone(&watermark_seen),
                    }),
                    kind: "native",
                },
                downstream: Vec::new(),
                artifact: None,
                window: Some(crate::service::config::WindowConfig {
                    spec: WindowSpec::Tumbling {
                        size_ms: 30_000,
                        offset_ms: 0,
                    },
                    time_field: "timestamp_ms".to_string(),
                    key_fields: Vec::new(),
                    allowed_lateness_ms: 0,
                }),
            },
        ];
        let built = BuiltService {
            workflow_id: "w".to_string(),
            workflow_name: None,
            nodes,
            registry: Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
        };

        run_standalone(
            built,
            &config(CfgRunMode::OneShot),
            CancellationToken::new(),
            None,
            None,
        )
        .await
        .expect("run succeeds");

        assert_eq!(
            *watermark_seen.lock().unwrap(),
            3_000,
            "the watermark resource must carry the max merged timestamp"
        );

        // The series must carry the node's id, so the dashboard can attribute
        // the number to exactly this processor box.
        let text = prometheus::TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        assert!(
            text.contains("pcs_window_watermark_seconds") && text.contains("processor=\"p\""),
            "window watermark series missing from:\n{text}"
        );
    }
}
