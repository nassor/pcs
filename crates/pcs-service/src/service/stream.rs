//! Stream runner for [`BuiltService`].
//!
//! [`run_stream`] drives a [`BuiltService`] one source batch at a time: each
//! arriving [`RecordBatch`](arrow_array::RecordBatch) is appended to the one
//! source node's dataset slot, fanned out, and every remaining node runs in
//! topological order before the next item is pulled. There is no inter-item
//! sleep: latency is bounded by the workflow itself, not by a pacing timer.
//!
//! Selected by `run_mode` `kind = "stream"` in standalone mode;
//! [`run_standalone`](super::standalone::run_standalone) dispatches here.
//!
//! ## Semantics
//!
//! - **At least one source, pulled round-robin.** Config files are checked by
//!   [`ServiceConfig::validate`](super::config::ServiceConfig::validate);
//!   hand-built services are checked here. Each item is one batch from one
//!   source, in a stable rotation across the declared sources: a windowing
//!   processor fed by several streams accumulates their rows into its open
//!   windows across items, one stream's batch at a time. A source that
//!   reports EOF is dropped from the rotation while the live ones keep
//!   feeding items.
//! - **State carry.** The blob returned by a processor's `run_on_with_state` is
//!   fed back as `prior` on the next item for that same processor, so
//!   processor state survives across items even though the WASM store does
//!   not. One blob per processor node; the blobs live in loop memory only and
//!   are never checkpointed, so they are lost on restart.
//! - **At-most-once.** An item whose processor call fails drops that
//!   processor's fan-out for the item (logged and counted); `prior` is left
//!   untouched so the next item resumes from the last good state.
//! - **Sink finalisation.** Sinks are written per item but `finish()` is
//!   called once, at exit (source EOF or cancellation).
//! - **One trace per item.** A `workflow.batch` root span opens when the item
//!   arrives, holding one `runtime.run` per processor and one `sink.write`
//!   per sink. There is no `source.drain` span: the wait for input precedes
//!   the item and would otherwise dominate its latency.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
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
use super::standalone::{NodeRunStats, StandaloneStats};
#[cfg(feature = "windows")]
use super::windowing::WindowTracker;

/// Which of the three roles a node plays while it runs. Mirrors
/// `standalone::NodeRunKind`; kept as a separate (private) type because the
/// two runners never share more than the enum shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeRunKind {
    Source,
    Processor,
    Sink,
}

/// Minimum spacing between `live_stats` publishes.
///
/// A per-item `RwLock` write would dominate a sub-millisecond item budget, so
/// the shared snapshot is refreshed at most this often (plus once at exit).
const PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

/// Backoff after a source error, so a permanently failing source cannot spin
/// the loop at full speed.
const SOURCE_ERROR_BACKOFF: Duration = Duration::from_millis(10);

/// Drive a [`BuiltService`] in stream mode: one workflow pass per arriving
/// source batch.
///
/// Returns [`StandaloneStats`] on success. Cancellation and source EOF are both
/// clean exits. Returns `Err` only for configuration violations detected at
/// entry.
///
/// `stats.iterations` counts items, `stats.total_busy_micros` sums per-item
/// processing time, and `stats.max_item_micros` records the slowest item.
///
/// ## Error policy
///
/// Mirrors [`run_standalone`](super::standalone::run_standalone): log, count in
/// `iteration_errors`, continue with the next item. A source error
/// additionally backs off for 10 ms (cancellable) to avoid a hot error loop.
pub async fn run_stream(
    built: BuiltService,
    cancel: CancellationToken,
    live_stats: Option<Arc<RwLock<StandaloneStats>>>,
) -> Result<StandaloneStats, PcsError> {
    let source_count = built
        .nodes
        .iter()
        .filter(|n| matches!(n.kind, BuiltNodeKind::Source(_)))
        .count();
    if source_count == 0 {
        return Err(PcsError::configuration(
            "stream mode requires at least one source (0 configured)",
        ));
    }

    let BuiltService {
        workflow_id, nodes, ..
    } = built;
    let n = nodes.len();

    let mut ids: Vec<String> = Vec::with_capacity(n);
    let mut components: Vec<Option<&'static str>> = Vec::with_capacity(n);
    let mut downstream: Vec<Vec<crate::service::builder::BuiltEdge>> = Vec::with_capacity(n);
    let mut kinds: Vec<NodeRunKind> = Vec::with_capacity(n);
    let mut sources: Vec<Option<Box<dyn Source>>> = Vec::with_capacity(n);
    let mut runtimes: Vec<Option<Box<dyn PipelineRuntime>>> = Vec::with_capacity(n);
    let mut datasets: Vec<Option<Dataset>> = Vec::with_capacity(n);
    let mut sinks: Vec<Option<Box<dyn Sink>>> = Vec::with_capacity(n);
    let mut node_stats: Vec<NodeRunStats> = Vec::with_capacity(n);
    // One WIT checkpoint blob per processor node, fed back as `prior` on the
    // next item for that same node. The composite chain blob `StageChain`
    // used to produce no longer exists: every processor keeps its own.
    let mut prior: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    // One watermark tracker per windowed processor node, monotonic across
    // items, mirroring the standalone runner.
    #[cfg(feature = "windows")]
    let mut trackers: Vec<Option<WindowTracker>> = Vec::with_capacity(n);

    for node in nodes {
        ids.push(node.id);
        components.push(node.component);
        downstream.push(node.downstream);
        prior.push(None);
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
            kind: match kind {
                NodeRunKind::Source => "source",
                NodeRunKind::Processor => "processor",
                NodeRunKind::Sink => "sink",
            }
            .to_string(),
            rows: 0,
            batches: 0,
            errors: 0,
        });
    }

    let mut staged: Vec<Vec<RecordBatch>> = vec![Vec::new(); n];

    let mut stats = StandaloneStats::default();
    let start = Instant::now();
    let mut last_publish = Instant::now();

    // Sources rotate round-robin: each item is one batch from exactly one
    // source. `exhausted` tracks sources that reported EOF, so a finished
    // source stops being polled while the live ones keep feeding items.
    let source_indices: Vec<usize> = (0..n)
        .filter(|&i| matches!(kinds[i], NodeRunKind::Source))
        .collect();
    let mut exhausted: Vec<bool> = vec![false; n];
    let mut remaining_sources = source_indices.len();
    let mut source_cursor = 0usize;

    #[cfg(feature = "tracing")]
    tracing::info!(
        workflow = %workflow_id,
        sources = source_indices.len(),
        "stream runner starting"
    );

    loop {
        if remaining_sources == 0 {
            break;
        }
        // Advance to the next source that has not exhausted yet. The cursor
        // wraps through `source_indices`, so sources are visited in a stable
        // rotation and each item is one batch from one source.
        let source_index = loop {
            let idx = source_indices[source_cursor % source_indices.len()];
            source_cursor += 1;
            if !exhausted[idx] {
                break idx;
            }
        };

        let source = sources[source_index]
            .as_mut()
            .expect("the stream source keeps its source");
        let next = tokio::select! {
            r = source.next_batch() => r,
            _ = cancel.cancelled() => {
                #[cfg(feature = "tracing")]
                tracing::info!("stream runner cancelled while waiting for input");
                break;
            }
        };

        let batch = match next {
            Ok(None) => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    source = %ids[source_index],
                    "stream source reached EOF"
                );
                exhausted[source_index] = true;
                remaining_sources -= 1;
                continue;
            }
            Ok(Some(batch)) => batch,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(source = %ids[source_index], error = %_e, "stream source error (continuing)");
                stats.iteration_errors += 1;
                node_stats[source_index].errors += 1;
                tokio::select! {
                    _ = tokio::time::sleep(SOURCE_ERROR_BACKOFF) => {}
                    _ = cancel.cancelled() => break,
                }
                continue;
            }
        };

        let item_start = Instant::now();
        let rows = batch.num_rows() as u64;

        // The root span opens once the item is in hand, so its duration is the
        // item's latency and not the wait for input before it.
        #[cfg(feature = "tracing")]
        let batch_span = tracing::info_span!(
            "workflow.batch",
            workflow = %workflow_id,
            iteration = stats.iterations + 1,
            rows = rows
        );

        stats.source_batches_drained += 1;
        stats.rows_processed += rows;
        node_stats[source_index].rows += rows;
        node_stats[source_index].batches += 1;
        crate::metrics::instruments().source_batch(&ids[source_index]);
        crate::metrics::instruments().rows(&ids[source_index], rows);

        // Fan out the source item exactly like a batch-mode source drain.
        let component = components[source_index].expect("the stream source declares a component");
        for edge in &downstream[source_index] {
            let d = edge.node;
            match kinds[d] {
                NodeRunKind::Processor => {
                    if let Err(_e) = datasets[d]
                        .as_mut()
                        .expect("processor node keeps its dataset")
                        .append_record_batch(component, batch.clone())
                    {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(parent: &batch_span, from = %ids[source_index], to = %ids[d], error = %_e, "fan-out append error (continuing)");
                        stats.iteration_errors += 1;
                    }
                }
                NodeRunKind::Sink => staged[d].push(batch.clone()),
                NodeRunKind::Source => unreachable!("a source is never a link target"),
            }
        }

        let mut cancelled_mid_item = false;

        for i in 0..n {
            if i == source_index {
                continue;
            }
            match kinds[i] {
                // Another source: it contributes its own item, not this one.
                NodeRunKind::Source => continue,
                NodeRunKind::Processor => {
                    // Same fan-in watermark advance as the standalone runner:
                    // by the time this processor runs, every upstream node has
                    // delivered into its dataset for this item.
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
                    #[cfg(feature = "tracing")]
                    let run_span = tracing::info_span!(
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
                    let run = runtime.run_on_with_state_and_routes(dataset, prior[i].as_deref());
                    #[cfg(feature = "tracing")]
                    let run = run.instrument(run_span.clone());
                    let run_result = tokio::select! {
                        r = run => Some(r),
                        _ = cancel.cancelled() => None,
                    };

                    let Some(run_result) = run_result else {
                        #[cfg(feature = "tracing")]
                        tracing::info!(parent: &batch_span, "stream runner cancelled during runtime run");
                        cancelled_mid_item = true;
                        break;
                    };

                    match run_result {
                        Ok(out) => {
                            // The checkpoint is persisted verbatim: `None`
                            // means the processor carries no state, so it
                            // must clear `prior` too.
                            prior[i] = out.state;
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

                            let routes = &out.routes;
                            for name in routes.iter().flatten() {
                                if !downstream[i]
                                    .iter()
                                    .any(|e| e.branch.as_deref() == Some(name.as_str()))
                                {
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(
                                        parent: &batch_span,
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
                                            tracing::warn!(parent: &batch_span, from = %ids[i], to = %ids[d], error = %_e, "fan-out forward error (continuing)");
                                            stats.iteration_errors += 1;
                                        }
                                    }
                                    NodeRunKind::Sink => {
                                        let component = components[d]
                                            .expect("a sink node always declares a component");
                                        if let Some(fwd) = datasets[i]
                                            .as_ref()
                                            .expect("processor dataset")
                                            .batch_for(component)
                                            .cloned()
                                            && fwd.num_rows() > 0
                                        {
                                            staged[d].push(fwd);
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
                            tracing::error!(parent: &run_span, processor = %ids[i], error = %_e, "stream processor error (dropping item for this node)");
                            stats.iteration_errors += 1;
                            node_stats[i].errors += 1;
                            crate::metrics::instruments().workflow_error(&workflow_id);
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
                    let write_span = tracing::info_span!(
                        parent: &batch_span,
                        "sink.write",
                        workflow = %workflow_id,
                        sink = %ids[i],
                        component,
                        rows = tracing::field::Empty
                    );
                    let rows_before: u64 = staged[i].iter().map(|b| b.num_rows() as u64).sum();
                    for b in staged[i].drain(..) {
                        let sink = sinks[i].as_mut().expect("sink node keeps its sink");
                        match sink.write_batch(&b).await {
                            Ok(()) => {
                                node_stats[i].rows += b.num_rows() as u64;
                                node_stats[i].batches += 1;
                                stats.sink_batches_written += 1;
                                crate::metrics::instruments().sink_batch(&ids[i]);
                            }
                            Err(_e) => {
                                #[cfg(feature = "tracing")]
                                tracing::error!(parent: &write_span, sink = %ids[i], error = %_e, "stream sink write error (continuing)");
                                stats.iteration_errors += 1;
                                node_stats[i].errors += 1;
                            }
                        }
                    }
                    #[cfg(feature = "tracing")]
                    write_span.record("rows", rows_before);
                    #[cfg(not(feature = "tracing"))]
                    let _ = rows_before;
                }
            }
        }

        if cancelled_mid_item {
            finish_all_sinks(&mut sinks, &ids, &mut node_stats, &mut stats).await;
            stats.total_duration_ms = start.elapsed().as_millis() as u64;
            stats.nodes = node_stats.clone();
            publish(&live_stats, &stats).await;
            return Ok(stats);
        }

        let item_micros = item_start.elapsed().as_micros() as u64;
        stats.iterations += 1;
        crate::metrics::instruments().workflow_run(&workflow_id);
        stats.total_busy_micros += item_micros;
        stats.max_item_micros = stats.max_item_micros.max(item_micros);

        #[cfg(feature = "tracing")]
        drop(batch_span);

        stats.nodes = node_stats.clone();
        if live_stats.is_some() && last_publish.elapsed() >= PUBLISH_INTERVAL {
            publish(&live_stats, &stats).await;
            last_publish = Instant::now();
        }
    }

    finish_all_sinks(&mut sinks, &ids, &mut node_stats, &mut stats).await;
    stats.total_duration_ms = start.elapsed().as_millis() as u64;
    stats.nodes = node_stats.clone();
    publish(&live_stats, &stats).await;

    #[cfg(feature = "tracing")]
    tracing::info!(
        items = stats.iterations,
        rows_processed = stats.rows_processed,
        iteration_errors = stats.iteration_errors,
        total_busy_micros = stats.total_busy_micros,
        max_item_micros = stats.max_item_micros,
        total_duration_ms = stats.total_duration_ms,
        "stream runner clean shutdown"
    );

    Ok(stats)
}

/// Finalise every sink. Each item already wrote its own batches, so only
/// `finish` remains.
async fn finish_all_sinks(
    sinks: &mut [Option<Box<dyn Sink>>],
    ids: &[String],
    node_stats: &mut [NodeRunStats],
    stats: &mut StandaloneStats,
) {
    for i in 0..sinks.len() {
        let Some(sink) = sinks[i].as_mut() else {
            continue;
        };
        if let Err(_e) = sink.finish().await {
            #[cfg(feature = "tracing")]
            tracing::error!(sink = %ids[i], error = %_e, "stream sink finish error");
            stats.iteration_errors += 1;
            node_stats[i].errors += 1;
        }
    }
}

async fn publish(shared: &Option<Arc<RwLock<StandaloneStats>>>, stats: &StandaloneStats) {
    if let Some(shared) = shared {
        *shared.write().await = stats.clone();
    }
}
