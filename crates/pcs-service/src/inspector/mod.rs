//! In-process telemetry: ring buffers, a `tracing` layer, and a metric
//! exporter, read back through the control plane's JSON API.
//!
//! Nothing leaves the process. There is no collector to run, no scraper to
//! point at the service, and no storage to provision: spans, log events and
//! metric samples land in three [`TimeBoundedBuffer`]s whose retention and
//! capacity come from [`InspectorConfig`], and `/api/*` reads them back.
//!
//! ## Wiring
//!
//! | Piece | Installed by |
//! |---|---|
//! | [`InspectorLayer`] | [`init_logging`](crate::service::logging::init_logging), as one more layer on the existing registry |
//! | [`InMemoryMetricExporter`] | `pcs-service serve`, as a second `PeriodicReader` on the shared `SdkMeterProvider` |
//! | [`Topology`] | [`ServiceBuilder::build_all`](crate::service::builder::ServiceBuilder::build_all), once, through [`Inspector::set_topology`] |
//!
//! Unlike [`crate::metrics::Instruments`] this type is **not** process-global:
//! the router needs a handle and tests need isolated instances, so it is cloned
//! (every field is an `Arc`) and threaded explicitly.
//!
//! ## The `EnvFilter` applies here too
//!
//! `init_logging` installs one subscriber-wide `EnvFilter`. A `RUST_LOG` that
//! suppresses `pcs_service` spans empties the inspector's span buffer, and with
//! it the traces tab; suppressing `pcs_core` additionally empties
//! [`Snapshot::span_stats`] — the same caveat `pcs_stage_duration_seconds`
//! already carries.
//!
//! ## Which series drive which part of the dashboard
//!
//! Every name below already exists in [`crate::metrics`]; the inspector reads
//! them and introduces none.
//!
//! | `pcs_workflow_runs_total` | left-rail run counter and, under `workflow="<id>"`, each workflow card's run badge |
//! | `pcs_workflow_errors_total` | left-rail error badge and, under `workflow="<id>"`, each workflow card's error badge |
//! | `pcs_stage_duration_seconds` | per-system latency, native runtime only |
//! | `pcs_rows_processed_total` | source node throughput, source-to-processor/sink edge rate, and a sink node's throughput when a source feeds it directly |
//! | `pcs_source_batches_drained_total` | source node detail |
//! | `pcs_sink_batches_written_total` | sink node detail, and both the sink node's throughput and the processor-to-sink edge rate when `pcs_processor_rows_out_total` is empty |
//! | `pcs_liveness_counter` | omitted: a watchdog-internal heartbeat is not a user-facing number |
//! | `pcs_ready` | left-rail ready badge |
//! | `pcs_uptime_seconds` | left-rail uptime |
//! | `pcs_raft_commit_index`, `pcs_raft_term`, `pcs_raft_leader_id` | cluster facts row, shown only in cluster mode |
//! | `pcs_processor_batch_duration_seconds` | processor node latency |
//! | `pcs_processor_rows_in_total` | processor node rows in |
//! | `pcs_processor_rows_out_total` | processor node rows out, primary processor-to-sink/processor edge rate, and the downstream sink node's throughput |
//! | `pcs_processor_systems_run_total` | processor node detail |
//! | `pcs_processor_retries_total` | processor node retry badge |
//! | `pcs_processor_metric` | processor-defined metric list in the node's detail sheet |
//!
//! The six `pcs_processor_*` series and the two `pcs_workflow_*` counters
//! carry a node or workflow id attribute in addition to their unattributed,
//! process-wide-total form: a workflow can declare several processor nodes
//! and several workflows, and only the attributed form identifies which node
//! or workflow produced a sample. [`Snapshot::edges`] rates every edge from
//! its upstream node's own attributed series, source or processor; an edge
//! whose upstream node has not sampled yet is omitted rather than reported as
//! zero.

pub mod buffer;
pub mod layer;
pub mod metric_exporter;
pub mod record;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

pub use buffer::TimeBoundedBuffer;
pub use layer::InspectorLayer;
pub use metric_exporter::InMemoryMetricExporter;
pub use pcs_inspector_wire as wire;

use pcs_inspector_wire::{
    BufferStats, EdgeRate, LogRecord, MAX_POINTS, MetricSample, PROCESSOR_ATTR, PointAt, SINK_ATTR,
    SOURCE_ATTR, SeriesKind, SeriesSummary, Snapshot, SpanRecord, SpanStat, Topology, TraceDetail,
    TraceSummary,
};
use record::{level_rank, now_unix_ms};

/// Series the edge rates are read from.
const ROWS_PROCESSED: &str = "pcs_rows_processed_total";
const PROCESSOR_ROWS_OUT: &str = "pcs_processor_rows_out_total";
const SINK_BATCHES: &str = "pcs_sink_batches_written_total";

/// Span names [`Snapshot::span_stats`] groups.
const STAGE_SPAN: &str = "pipeline.stage";
const SYSTEM_SPAN: &str = "system.execute";

/// Default retention window, one hour.
fn default_retention_secs() -> u64 {
    3600
}

/// Default metric export interval, one second.
fn default_sample_interval_secs() -> u64 {
    1
}

fn default_max_spans() -> usize {
    10_000
}

fn default_max_logs() -> usize {
    10_000
}

/// One hour of samples at the default one-second interval.
fn default_max_samples() -> usize {
    3600
}

fn default_true() -> bool {
    true
}

/// How much telemetry the inspector keeps, and whether it runs at all.
///
/// Every field carries a `serde` default, so `[observability.inspector]` can be
/// omitted entirely or given one key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InspectorConfig {
    /// Master switch. `false` installs no layer, attaches no metric reader, and
    /// merges no routes, so `/api/*` and `/ui` return 404.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Whether to serve the dashboard at `/ui`. `false` keeps the JSON API.
    #[serde(default = "default_true")]
    pub ui: bool,
    /// How long a record stays readable, in seconds.
    #[serde(default = "default_retention_secs")]
    pub retention_secs: u64,
    /// How often metrics are copied out of the SDK, in seconds.
    #[serde(default = "default_sample_interval_secs")]
    pub sample_interval_secs: u64,
    /// Hard cap on retained span records.
    #[serde(default = "default_max_spans")]
    pub max_spans: usize,
    /// Hard cap on retained log records.
    #[serde(default = "default_max_logs")]
    pub max_logs: usize,
    /// Hard cap on retained metric samples.
    #[serde(default = "default_max_samples")]
    pub max_samples: usize,
}

impl Default for InspectorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ui: true,
            retention_secs: default_retention_secs(),
            sample_interval_secs: default_sample_interval_secs(),
            max_spans: default_max_spans(),
            max_logs: default_max_logs(),
            max_samples: default_max_samples(),
        }
    }
}

impl InspectorConfig {
    /// Retention as a [`Duration`].
    pub fn retention(&self) -> Duration {
        Duration::from_secs(self.retention_secs)
    }

    /// Export interval as a [`Duration`], never below one second.
    ///
    /// A zero interval would make the `PeriodicReader` collect in a tight loop.
    pub fn sample_interval(&self) -> Duration {
        Duration::from_secs(self.sample_interval_secs.max(1))
    }
}

/// Handle on the process's in-memory telemetry.
///
/// Cheap to clone: every buffer is `Arc`-backed and clones share storage.
#[derive(Debug, Clone)]
pub struct Inspector {
    spans: TimeBoundedBuffer<SpanRecord>,
    logs: TimeBoundedBuffer<LogRecord>,
    samples: TimeBoundedBuffer<MetricSample>,
    topology: Arc<OnceLock<Topology>>,
    started_at: Instant,
    ui_enabled: bool,
}

impl Inspector {
    /// Build the three buffers from `cfg`.
    pub fn new(cfg: &InspectorConfig) -> Self {
        let ttl = cfg.retention();
        Self {
            spans: TimeBoundedBuffer::new(ttl, cfg.max_spans),
            logs: TimeBoundedBuffer::new(ttl, cfg.max_logs),
            samples: TimeBoundedBuffer::new(ttl, cfg.max_samples),
            topology: Arc::new(OnceLock::new()),
            started_at: Instant::now(),
            ui_enabled: cfg.ui,
        }
    }

    /// The `tracing` layer that fills the span and log buffers.
    pub fn layer(&self) -> InspectorLayer {
        InspectorLayer::new(self.spans.clone(), self.logs.clone())
    }

    /// The metric exporter that fills the sample buffer.
    pub fn metric_exporter(&self) -> InMemoryMetricExporter {
        InMemoryMetricExporter::new(self.samples.clone())
    }

    /// Whether `/ui` should be served.
    pub fn ui_enabled(&self) -> bool {
        self.ui_enabled
    }

    /// Publish the topology. Only the first call takes effect.
    ///
    /// One process runs one pipeline, so the topology is fixed once the service
    /// is built; a second call would mean two builds raced, which is a bug in
    /// the caller rather than something to merge.
    pub fn set_topology(&self, topology: Topology) {
        if self.topology.set(topology).is_err() {
            tracing::warn!("inspector topology already set; ignoring the second publication");
        }
    }

    /// The published topology, or an empty one before `set_topology` ran.
    pub fn topology(&self) -> Topology {
        self.topology.get().cloned().unwrap_or_default()
    }

    /// Everything the dashboard needs for one frame.
    ///
    /// `window` bounds how far back the series history reaches; `ready` is the
    /// service's own readiness flag, which the inspector does not own.
    pub fn snapshot(&self, window: Duration, ready: bool) -> Snapshot {
        let now_ms = now_unix_ms();
        let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
        let cutoff = now_ms.saturating_sub(window_ms);

        let samples: Vec<MetricSample> = self
            .samples
            .read_recent()
            .into_iter()
            .filter(|s| s.at_unix_ms >= cutoff)
            .collect();

        let series = summarize_series(&samples);
        let topology = self.topology();
        let edges = edge_rates(&topology, &series);
        let span_stats = self.span_stats(cutoff);

        Snapshot {
            topology_version: topology.version,
            sampled_at_unix_ms: now_ms,
            uptime_secs: self.started_at.elapsed().as_secs(),
            ready,
            series,
            edges,
            span_stats,
            buffers: BufferStats {
                spans: self.spans.len(),
                logs: self.logs.len(),
                samples: self.samples.len(),
                dropped: self.spans.dropped() + self.logs.dropped() + self.samples.dropped(),
            },
        }
    }

    /// Newest-first trace list, at most `limit` entries.
    pub fn traces(&self, limit: usize) -> Vec<TraceSummary> {
        let spans = self.spans.read_recent();
        let mut errored: Vec<u64> = self
            .logs
            .read_recent()
            .into_iter()
            .filter(|log| log.level == "ERROR")
            .filter_map(|log| log.trace_id)
            .collect();
        errored.sort_unstable();
        errored.dedup();

        let mut roots: Vec<&SpanRecord> = Vec::new();
        let mut counts: HashMap<u64, usize> = HashMap::new();
        for span in &spans {
            *counts.entry(span.trace_id).or_default() += 1;
            if span.parent_id.is_none() {
                roots.push(span);
            }
        }

        let mut summaries: Vec<TraceSummary> = roots
            .into_iter()
            .map(|root| TraceSummary {
                trace_id: root.trace_id,
                name: root.name.clone(),
                started_unix_ms: root.started_unix_ms,
                duration_us: root.duration_us,
                span_count: counts.get(&root.trace_id).copied().unwrap_or(1),
                error: errored.binary_search(&root.trace_id).is_ok(),
            })
            .collect();

        summaries.sort_unstable_by_key(|summary| std::cmp::Reverse(summary.started_unix_ms));
        summaries.truncate(limit);
        summaries
    }

    /// Every retained span and log line belonging to one trace.
    ///
    /// Returns `None` when the trace has aged out, so the handler can answer
    /// 404 instead of an empty document that looks like a live but silent trace.
    pub fn trace(&self, trace_id: u64) -> Option<TraceDetail> {
        let mut spans: Vec<SpanRecord> = self
            .spans
            .read_recent()
            .into_iter()
            .filter(|span| span.trace_id == trace_id)
            .collect();
        if spans.is_empty() {
            return None;
        }
        spans.sort_unstable_by_key(|span| span.started_unix_ms);

        let mut logs: Vec<LogRecord> = self
            .logs
            .read_recent()
            .into_iter()
            .filter(|log| log.trace_id == Some(trace_id))
            .collect();
        logs.sort_unstable_by_key(|log| log.at_unix_ms);

        Some(TraceDetail { spans, logs })
    }

    /// Newest-first log lines, filtered at or above `min_level` when given.
    pub fn logs(&self, limit: usize, min_level: Option<&str>) -> Vec<LogRecord> {
        match min_level {
            None => self.logs.read_last(limit),
            Some(level) => {
                let max_rank = level_rank(level);
                self.logs
                    .read_last(usize::MAX)
                    .into_iter()
                    .filter(|log| level_rank(&log.level) <= max_rank)
                    .take(limit)
                    .collect()
            }
        }
    }

    /// Per-stage and per-system latency from the retained span records.
    fn span_stats(&self, cutoff_unix_ms: u64) -> Vec<SpanStat> {
        let mut grouped: HashMap<(&'static str, String), Vec<u64>> = HashMap::new();
        for span in self.spans.read_recent() {
            if span.started_unix_ms < cutoff_unix_ms {
                continue;
            }
            let (name, field) = match span.name.as_ref() {
                STAGE_SPAN => (STAGE_SPAN, "stage"),
                SYSTEM_SPAN => (SYSTEM_SPAN, "system"),
                _ => continue,
            };
            let Some((_, key)) = span.fields.iter().find(|(k, _)| k == field) else {
                continue;
            };
            grouped
                .entry((name, key.clone()))
                .or_default()
                .push(span.duration_us);
        }

        let mut stats: Vec<SpanStat> = grouped
            .into_iter()
            .map(|((span, key), mut durations)| {
                durations.sort_unstable();
                SpanStat {
                    span: std::borrow::Cow::Borrowed(span),
                    key,
                    count: durations.len(),
                    p50_us: percentile(&durations, 0.50),
                    p95_us: percentile(&durations, 0.95),
                    max_us: durations.last().copied().unwrap_or(0),
                }
            })
            .collect();
        stats.sort_unstable_by(|a, b| a.span.cmp(&b.span).then_with(|| a.key.cmp(&b.key)));
        stats
    }
}

/// Nearest-rank percentile of a sorted, non-empty slice.
fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let last = sorted.len() - 1;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "index is clamped into the slice below"
    )]
    let idx = ((last as f64) * q).round() as usize;
    sorted[idx.min(last)]
}

/// Fold every sample in the window into one [`SeriesSummary`] per
/// `(name, attrs)` pair.
///
/// Series are keyed on their attributes as well as their name, because
/// `pcs_processor_metric` carries a processor-chosen `metric` attribute and its
/// values are distinct series to the dashboard.
fn summarize_series(samples: &[MetricSample]) -> Vec<SeriesSummary> {
    // Insertion-ordered accumulation, then one sort at the end: a HashMap of
    // vectors would reorder series between polls and make the UI list jump.
    let mut order: Vec<(String, Vec<(String, String)>)> = Vec::new();
    let mut index: HashMap<(String, Vec<(String, String)>), usize> = HashMap::new();
    let mut kinds: Vec<SeriesKind> = Vec::new();
    let mut counts: Vec<u64> = Vec::new();
    let mut points: Vec<Vec<PointAt>> = Vec::new();

    for sample in samples {
        for point in &sample.series {
            let key = (point.name.clone(), point.attrs.clone());
            let slot = match index.get(&key) {
                Some(slot) => *slot,
                None => {
                    let slot = order.len();
                    index.insert(key.clone(), slot);
                    order.push(key);
                    kinds.push(point.kind);
                    counts.push(point.count);
                    points.push(Vec::new());
                    slot
                }
            };
            counts[slot] = point.count;
            points[slot].push(PointAt {
                t: sample.at_unix_ms,
                v: point.value,
            });
        }
    }

    let mut out: Vec<SeriesSummary> = Vec::with_capacity(order.len());
    for (slot, (name, attrs)) in order.into_iter().enumerate() {
        let history = &points[slot];
        let Some(newest) = history.last() else {
            continue;
        };
        let kind = kinds[slot];
        let rate = match kind {
            SeriesKind::Gauge => newest.v,
            SeriesKind::Counter | SeriesKind::Histogram => rate_of(history),
        };
        out.push(SeriesSummary {
            name,
            kind,
            attrs,
            value: newest.v,
            count: counts[slot],
            rate_per_sec: rate,
            points: decimate(history),
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.attrs.cmp(&b.attrs)));
    out
}

/// First derivative across the two newest points.
///
/// A cumulative counter that went backwards means the process restarted or the
/// SDK reset the series; reporting 0 is honest, a negative throughput is not.
fn rate_of(history: &[PointAt]) -> f64 {
    if history.len() < 2 {
        return 0.0;
    }
    let newest = history[history.len() - 1];
    let previous = history[history.len() - 2];
    let dt_ms = newest.t.saturating_sub(previous.t);
    if dt_ms == 0 {
        return 0.0;
    }
    let delta = newest.v - previous.v;
    if delta < 0.0 {
        return 0.0;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "millisecond gaps are far below f64's exact integer range"
    )]
    let dt_secs = (dt_ms as f64) / 1000.0;
    delta / dt_secs
}

/// Thin `history` to at most [`MAX_POINTS`] entries, keeping the newest.
fn decimate(history: &[PointAt]) -> Vec<PointAt> {
    if history.len() <= MAX_POINTS {
        return history.to_vec();
    }
    let stride = history.len().div_ceil(MAX_POINTS);
    let mut out: Vec<PointAt> = history.iter().copied().step_by(stride).collect();
    let newest = history[history.len() - 1];
    if out.last().map(|p| p.t) != Some(newest.t) {
        out.push(newest);
    }
    out
}

/// The rate of one series carrying `key="value"`.
fn attributed_rate(series: &[SeriesSummary], name: &str, key: &str, value: &str) -> Option<f64> {
    series
        .iter()
        .find(|s| s.name == name && s.attrs.iter().any(|(k, v)| k == key && v == value))
        .map(|s| s.rate_per_sec)
}

/// The rate of one series carrying `key1="value1"` and `key2="value2"`.
fn attributed_rate_branch(
    series: &[SeriesSummary],
    name: &str,
    key1: &str,
    value1: &str,
    key2: &str,
    value2: &str,
) -> Option<f64> {
    series
        .iter()
        .find(|s| {
            s.name == name
                && s.attrs.iter().any(|(k, v)| k == key1 && v == value1)
                && s.attrs.iter().any(|(k, v)| k == key2 && v == value2)
        })
        .map(|s| s.rate_per_sec)
}

/// Attach a throughput number to every topology edge.
///
/// Rated from the upstream node's own attributed series, keyed on its kind:
/// a source `from` reads `pcs_rows_processed_total` under `source="<from>"`,
/// a processor `from` reads `pcs_processor_rows_out_total` under
/// `processor="<from>"`. When a processor's rows-out has no sample yet and
/// its downstream is a sink, that is the native-runtime case, which writes no
/// `pcs_processor_*` series at all: the edge falls back to
/// `pcs_sink_batches_written_total` under `sink="<to>"`, in batches rather
/// than rows.
///
/// An edge whose attributed series has not been sampled yet is omitted rather
/// than reported as zero, so the output is not always one entry per
/// [`TopoEdge`](pcs_inspector_wire::TopoEdge); a lookup by `(from, to)` is the
/// contract, not position. With several processors the unattributed form of
/// each `pcs_processor_*` series is the workflow-wide sum, so there is no
/// unattributed fallback here: every edge reads its own upstream node's own
/// attributed sample or is omitted.
///
/// A bridge is rated the same way, but with the units flipped: a bridge's
/// `from` is a `ChannelSink`, whose kind the per-edge match drops, so bridges
/// get their own arm. It prefers the consuming `ChannelSource`'s own
/// `pcs_rows_processed_total` rows — the same flow measured in the better
/// unit — and falls back to the producing sink's
/// `pcs_sink_batches_written_total` batches before either side has sampled
/// rows, mirroring the rows-then-batches idiom above.
fn edge_rates(topology: &Topology, series: &[SeriesSummary]) -> Vec<EdgeRate> {
    let node_kind: HashMap<&str, &str> = topology
        .workflows
        .iter()
        .flat_map(|w| w.nodes.iter())
        .map(|node| (node.id.as_str(), node.kind.as_str()))
        .collect();

    let mut out = Vec::new();
    for edge in topology.workflows.iter().flat_map(|w| w.edges.iter()) {
        let (rate, unit) = match node_kind.get(edge.from.as_str()).copied() {
            Some("source") => {
                let Some(rate) = attributed_rate(series, ROWS_PROCESSED, SOURCE_ATTR, &edge.from)
                else {
                    continue;
                };
                (rate, "rows")
            }
            Some("processor") => {
                match &edge.branch {
                    Some(branch) => {
                        // A labelled edge is rated from its own branch series.
                        // Never the sink-batches fallback: an unchosen branch
                        // must read as absent, not as whatever the sink wrote.
                        let Some(rate) = attributed_rate_branch(
                            series,
                            PROCESSOR_ROWS_OUT,
                            PROCESSOR_ATTR,
                            &edge.from,
                            pcs_inspector_wire::BRANCH_ATTR,
                            branch,
                        ) else {
                            continue;
                        };
                        (rate, "rows")
                    }
                    None => {
                        match attributed_rate(
                            series,
                            PROCESSOR_ROWS_OUT,
                            PROCESSOR_ATTR,
                            &edge.from,
                        ) {
                            Some(rate) => (rate, "rows"),
                            None if node_kind.get(edge.to.as_str()).copied() == Some("sink") => {
                                let Some(rate) =
                                    attributed_rate(series, SINK_BATCHES, SINK_ATTR, &edge.to)
                                else {
                                    continue;
                                };
                                (rate, "batches")
                            }
                            None => continue,
                        }
                    }
                }
            }
            _ => continue,
        };
        out.push(EdgeRate {
            from: edge.from.clone(),
            to: edge.to.clone(),
            rate_per_sec: rate,
            unit: unit.to_string(),
        });
    }

    for bridge in &topology.bridges {
        // Rows from the consuming `ChannelSource`'s own drain, which is the
        // same flow measured in the better unit; the producing `ChannelSink`'s
        // batches are the fallback before either side has sampled rows.
        let (rate, unit) = match attributed_rate(series, ROWS_PROCESSED, SOURCE_ATTR, &bridge.to) {
            Some(rate) => (rate, "rows"),
            None => match attributed_rate(series, SINK_BATCHES, SINK_ATTR, &bridge.from) {
                Some(rate) => (rate, "batches"),
                None => continue,
            },
        };
        out.push(EdgeRate {
            from: bridge.from.clone(),
            to: bridge.to.clone(),
            rate_per_sec: rate,
            unit: unit.to_string(),
        });
    }
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    use pcs_inspector_wire::{SeriesPoint, TopoEdge, TopoNode, WorkflowTopology};
    use std::borrow::Cow;

    fn config() -> InspectorConfig {
        InspectorConfig {
            retention_secs: 60,
            ..InspectorConfig::default()
        }
    }

    fn counter_sample(at: u64, name: &str, value: f64) -> MetricSample {
        MetricSample {
            at_unix_ms: at,
            series: vec![SeriesPoint {
                name: name.to_string(),
                kind: SeriesKind::Counter,
                attrs: Vec::new(),
                value,
                count: 0,
            }],
        }
    }

    /// A counter sample carrying one `key="value"` attribute, as a
    /// `pcs_processor_*`/`pcs_rows_processed_total`/`pcs_sink_batches_written_total`
    /// series records it alongside its unattributed, process-wide-total form.
    fn attributed_counter_sample(
        at: u64,
        name: &str,
        key: &str,
        value_attr: &str,
        value: f64,
    ) -> MetricSample {
        MetricSample {
            at_unix_ms: at,
            series: vec![SeriesPoint {
                name: name.to_string(),
                kind: SeriesKind::Counter,
                attrs: vec![(key.to_string(), value_attr.to_string())],
                value,
                count: 0,
            }],
        }
    }

    /// A counter sample carrying two attributes, the shape a branch-attributed
    /// `pcs_processor_rows_out_total{processor=...,branch=...}` series takes.
    fn branch_counter_sample(
        at: u64,
        name: &str,
        key1: &str,
        value1: &str,
        key2: &str,
        value2: &str,
        value: f64,
    ) -> MetricSample {
        MetricSample {
            at_unix_ms: at,
            series: vec![SeriesPoint {
                name: name.to_string(),
                kind: SeriesKind::Counter,
                attrs: vec![
                    (key1.to_string(), value1.to_string()),
                    (key2.to_string(), value2.to_string()),
                ],
                value,
                count: 0,
            }],
        }
    }

    fn topo_node(id: &str, kind: &str, type_name: &str, component: Option<&str>) -> TopoNode {
        TopoNode {
            id: id.to_string(),
            kind: kind.to_string(),
            name: None,
            type_name: type_name.to_string(),
            component: component.map(str::to_string),
            runtime: None,
            window: None,
            detail: Vec::new(),
        }
    }

    /// A one-processor workflow: every named source feeds `"p"`, which feeds
    /// every named sink.
    fn topology_with(sources: &[&str], sinks: &[&str]) -> Topology {
        let mut nodes = vec![topo_node("p", "processor", "wasm", None)];
        let mut edges = Vec::new();
        for name in sources {
            nodes.push(topo_node(name, "source", "NatsSource", Some("Order")));
            edges.push(TopoEdge {
                from: (*name).to_string(),
                to: "p".to_string(),
                branch: None,
            });
        }
        for name in sinks {
            nodes.push(topo_node(name, "sink", "PostgresSink", Some("Order")));
            edges.push(TopoEdge {
                from: "p".to_string(),
                to: (*name).to_string(),
                branch: None,
            });
        }
        Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![WorkflowTopology {
                id: "quickstart".to_string(),
                name: None,
                nodes,
                edges,
            }],
            bridges: Vec::new(),
        }
    }

    /// A two-processor chain: `orders-in -> validate -> settle -> settlements`.
    fn two_processor_topology() -> Topology {
        let nodes = vec![
            topo_node("orders-in", "source", "NatsSource", Some("Order")),
            topo_node("validate", "processor", "wasm", None),
            topo_node("settle", "processor", "wasm", None),
            topo_node("settlements", "sink", "PostgresSink", Some("Order")),
        ];
        let edges = vec![
            TopoEdge {
                from: "orders-in".to_string(),
                to: "validate".to_string(),
                branch: None,
            },
            TopoEdge {
                from: "validate".to_string(),
                to: "settle".to_string(),
                branch: None,
            },
            TopoEdge {
                from: "settle".to_string(),
                to: "settlements".to_string(),
                branch: None,
            },
        ];
        Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![WorkflowTopology {
                id: "quickstart".to_string(),
                name: None,
                nodes,
                edges,
            }],
            bridges: Vec::new(),
        }
    }

    #[test]
    fn counter_rate_is_the_derivative_of_the_two_newest_points() {
        let samples = vec![
            counter_sample(1_000, ROWS_PROCESSED, 100.0),
            counter_sample(2_000, ROWS_PROCESSED, 250.0),
        ];
        let series = summarize_series(&samples);
        assert_eq!(series.len(), 1);
        assert!((series[0].rate_per_sec - 150.0).abs() < 1e-9);
        assert!((series[0].value - 250.0).abs() < f64::EPSILON);
        assert_eq!(series[0].points.len(), 2);
    }

    #[test]
    fn counter_reset_reports_zero_not_a_negative_rate() {
        let samples = vec![
            counter_sample(1_000, ROWS_PROCESSED, 900.0),
            counter_sample(2_000, ROWS_PROCESSED, 5.0),
        ];
        let series = summarize_series(&samples);
        assert!((series[0].rate_per_sec - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn gauge_rate_is_the_value_itself() {
        let samples = vec![MetricSample {
            at_unix_ms: 1_000,
            series: vec![SeriesPoint {
                name: "pcs_uptime_seconds".to_string(),
                kind: SeriesKind::Gauge,
                attrs: Vec::new(),
                value: 42.0,
                count: 0,
            }],
        }];
        let series = summarize_series(&samples);
        assert!((series[0].rate_per_sec - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn history_is_decimated_to_the_cap_and_keeps_the_newest_point() {
        let history: Vec<PointAt> = (0..1000u32)
            .map(|i| PointAt {
                t: 1000 + u64::from(i),
                v: f64::from(i),
            })
            .collect();
        let thinned = decimate(&history);
        assert!(thinned.len() <= MAX_POINTS + 1, "got {}", thinned.len());
        assert_eq!(thinned.last().expect("non-empty").t, 1999);
    }

    #[test]
    fn source_edge_reads_its_own_attributed_rate() {
        let topology = topology_with(&["orders-in"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "orders-in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "orders-in", 12.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let inbound = edges.iter().find(|e| e.to == "p").expect("source edge");
        assert_eq!(inbound.unit, "rows");
        assert!((inbound.rate_per_sec - 12.0).abs() < 1e-9);
    }

    #[test]
    fn two_sources_read_independent_attributed_rates() {
        let topology = topology_with(&["a", "b"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "a", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "a", 20.0),
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "b", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "b", 5.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let a = edges.iter().find(|e| e.from == "a").expect("a edge");
        let b = edges.iter().find(|e| e.from == "b").expect("b edge");
        assert!((a.rate_per_sec - 20.0).abs() < 1e-9);
        assert!((b.rate_per_sec - 5.0).abs() < 1e-9);
    }

    #[test]
    fn sink_edge_falls_back_to_batches_when_no_processor_rows_exist() {
        let topology = topology_with(&["orders-in"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(1_000, SINK_BATCHES, SINK_ATTR, "settlements", 0.0),
            attributed_counter_sample(2_000, SINK_BATCHES, SINK_ATTR, "settlements", 3.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let outbound = edges.iter().find(|e| e.from == "p").expect("sink edge");
        assert_eq!(outbound.unit, "batches");
        assert!((outbound.rate_per_sec - 3.0).abs() < 1e-9);
    }

    #[test]
    fn sink_edge_prefers_processor_rows_when_present() {
        let topology = topology_with(&["orders-in"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "p", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "p", 40.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let outbound = edges.iter().find(|e| e.from == "p").expect("sink edge");
        assert_eq!(outbound.unit, "rows");
        assert!((outbound.rate_per_sec - 40.0).abs() < 1e-9);
    }

    /// A labelled edge reads its own branch series: one labelled edge of a
    /// processor and one unlabelled edge of the same processor rate
    /// independently, because a branch series is the only one that names both
    /// the node and the branch.
    #[test]
    fn labelled_edge_reads_its_own_branch_series() {
        let topology = Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![WorkflowTopology {
                id: "w".to_string(),
                name: None,
                nodes: vec![
                    topo_node("validate", "processor", "wasm", None),
                    topo_node("out_high", "sink", "PostgresSink", Some("Order")),
                    topo_node("out_plain", "sink", "PostgresSink", Some("Order")),
                ],
                edges: vec![
                    TopoEdge {
                        from: "validate".to_string(),
                        to: "out_high".to_string(),
                        branch: Some("high".to_string()),
                    },
                    TopoEdge {
                        from: "validate".to_string(),
                        to: "out_plain".to_string(),
                        branch: None,
                    },
                ],
            }],
            bridges: Vec::new(),
        };
        let series = summarize_series(&[
            branch_counter_sample(
                1_000,
                PROCESSOR_ROWS_OUT,
                PROCESSOR_ATTR,
                "validate",
                pcs_inspector_wire::BRANCH_ATTR,
                "high",
                0.0,
            ),
            branch_counter_sample(
                2_000,
                PROCESSOR_ROWS_OUT,
                PROCESSOR_ATTR,
                "validate",
                pcs_inspector_wire::BRANCH_ATTR,
                "high",
                30.0,
            ),
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 40.0),
        ]);
        let edges = edge_rates(&topology, &series);

        let high = edges
            .iter()
            .find(|e| e.to == "out_high")
            .expect("labelled edge");
        assert_eq!(high.unit, "rows");
        assert!((high.rate_per_sec - 30.0).abs() < 1e-9);

        let plain = edges
            .iter()
            .find(|e| e.to == "out_plain")
            .expect("unlabelled edge");
        assert!((plain.rate_per_sec - 40.0).abs() < 1e-9);
    }

    /// An unchosen branch has no sample of its own, and the sink-batches
    /// fallback must not stand in for it: absent reads as absent, so the edge
    /// is omitted rather than showing whatever the sink wrote.
    #[test]
    fn labelled_edge_is_omitted_when_its_branch_has_no_sample() {
        let topology = Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![WorkflowTopology {
                id: "w".to_string(),
                name: None,
                nodes: vec![
                    topo_node("validate", "processor", "wasm", None),
                    topo_node("out_high", "sink", "PostgresSink", Some("Order")),
                ],
                edges: vec![TopoEdge {
                    from: "validate".to_string(),
                    to: "out_high".to_string(),
                    branch: Some("high".to_string()),
                }],
            }],
            bridges: Vec::new(),
        };
        let series = summarize_series(&[
            attributed_counter_sample(1_000, SINK_BATCHES, SINK_ATTR, "out_high", 0.0),
            attributed_counter_sample(2_000, SINK_BATCHES, SINK_ATTR, "out_high", 5.0),
        ]);
        let edges = edge_rates(&topology, &series);

        assert!(
            edges
                .iter()
                .all(|e| !(e.from == "validate" && e.to == "out_high")),
            "an unchosen branch must be omitted, not given the sink's batch rate: {edges:?}"
        );
    }

    /// Without a sample for the upstream processor's own rows-out there is
    /// nothing to rate the boundary with, and no other series may stand in
    /// for it.
    #[test]
    fn processor_edge_is_omitted_when_the_upstream_has_no_attributed_sample() {
        let topology = two_processor_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "orders-in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "orders-in", 12.0),
        ]);
        let edges = edge_rates(&topology, &series);

        let inbound = edges
            .iter()
            .find(|e| e.from == "orders-in" && e.to == "validate")
            .expect("source edge");
        assert!((inbound.rate_per_sec - 12.0).abs() < 1e-9);
        assert!(
            edges
                .iter()
                .all(|e| !(e.from == "validate" && e.to == "settle")),
            "the inter-processor edge must be omitted, not given a stand-in rate: {edges:?}"
        );
    }

    /// Rows leaving the upstream processor are the rows entering the
    /// downstream one.
    #[test]
    fn processor_edge_uses_the_upstream_processors_own_rows_out() {
        let topology = two_processor_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 30.0),
        ]);
        let edges = edge_rates(&topology, &series);

        let between = edges
            .iter()
            .find(|e| e.from == "validate" && e.to == "settle")
            .expect("inter-processor edge");
        assert!((between.rate_per_sec - 30.0).abs() < 1e-9);
        assert_eq!(between.unit, "rows");
    }

    /// Each processor's own attributed series counts only its own rows, so a
    /// chain must rate its sink edge from the last processor alone, never the
    /// process-wide sum both processors contribute to.
    #[test]
    fn sink_edge_uses_the_last_processors_own_rows_out_not_the_process_wide_sum() {
        let topology = two_processor_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 30.0),
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "settle", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "settle", 20.0),
            counter_sample(1_000, PROCESSOR_ROWS_OUT, 0.0),
            counter_sample(2_000, PROCESSOR_ROWS_OUT, 50.0),
        ]);
        let edges = edge_rates(&topology, &series);

        let outbound = edges
            .iter()
            .find(|e| e.to == "settlements")
            .expect("sink edge");
        assert!(
            (outbound.rate_per_sec - 20.0).abs() < 1e-9,
            "expected settle's own 20 rows/s, not the 50 the two processors sum to: {outbound:?}"
        );
        assert_eq!(outbound.unit, "rows");
    }

    #[test]
    fn snapshot_reports_buffer_occupancy_and_topology_version() {
        let inspector = Inspector::new(&config());
        inspector.set_topology(topology_with(&["orders-in"], &["settlements"]));
        inspector.spans.push(SpanRecord {
            trace_id: 1,
            span_id: 1,
            parent_id: None,
            name: Cow::Borrowed("pipeline.run"),
            target: Cow::Borrowed("pcs_core"),
            started_unix_ms: now_unix_ms(),
            duration_us: 10,
            fields: Vec::new(),
        });

        let snapshot = inspector.snapshot(Duration::from_secs(60), true);
        assert_eq!(snapshot.topology_version, 1);
        assert_eq!(snapshot.buffers.spans, 1);
        assert!(snapshot.ready);
        assert_eq!(
            snapshot.edges.len(),
            0,
            "no metric sample exists yet, so every edge is omitted rather than reported as zero"
        );
    }

    #[test]
    fn span_stats_group_stage_records_by_stage_field() {
        let inspector = Inspector::new(&config());
        for (stage, duration) in [("0", 100u64), ("0", 300), ("1", 900)] {
            inspector.spans.push(SpanRecord {
                trace_id: 1,
                span_id: 2,
                parent_id: Some(1),
                name: Cow::Borrowed(STAGE_SPAN),
                target: Cow::Borrowed("pcs_core::pipeline::execution"),
                started_unix_ms: now_unix_ms(),
                duration_us: duration,
                fields: vec![("stage".to_string(), stage.to_string())],
            });
        }

        let stats = inspector.span_stats(0);
        assert_eq!(stats.len(), 2, "one entry per stage index: {stats:?}");
        let first = stats.iter().find(|s| s.key == "0").expect("stage 0");
        assert_eq!(first.count, 2);
        assert_eq!(first.max_us, 300);
        let second = stats.iter().find(|s| s.key == "1").expect("stage 1");
        assert_eq!(second.p50_us, 900);
    }

    #[test]
    fn traces_are_newest_first_and_flag_errors() {
        let inspector = Inspector::new(&config());
        let base = now_unix_ms();
        for (trace, started) in [(10u64, base), (11, base + 50)] {
            inspector.spans.push(SpanRecord {
                trace_id: trace,
                span_id: trace,
                parent_id: None,
                name: Cow::Borrowed("pipeline.run"),
                target: Cow::Borrowed("pcs_core"),
                started_unix_ms: started,
                duration_us: 5,
                fields: Vec::new(),
            });
        }
        inspector.logs.push(LogRecord {
            level: Cow::Borrowed("ERROR"),
            target: Cow::Borrowed("pcs_service"),
            message: "boom".to_string(),
            at_unix_ms: base,
            span_id: Some(10),
            trace_id: Some(10),
            fields: Vec::new(),
        });

        let traces = inspector.traces(10);
        assert_eq!(traces.len(), 2);
        assert_eq!(traces[0].trace_id, 11, "newest first");
        assert!(!traces[0].error);
        assert!(traces[1].error, "trace 10 has an ERROR log");

        let detail = inspector.trace(10).expect("trace 10 retained");
        assert_eq!(detail.spans.len(), 1);
        assert_eq!(detail.logs.len(), 1);
        assert!(inspector.trace(999).is_none());
    }

    #[test]
    fn logs_filter_at_or_above_the_requested_level() {
        let inspector = Inspector::new(&config());
        for level in ["ERROR", "WARN", "INFO", "DEBUG"] {
            inspector.logs.push(LogRecord {
                level: Cow::Borrowed(level),
                target: Cow::Borrowed("test"),
                message: level.to_string(),
                at_unix_ms: now_unix_ms(),
                span_id: None,
                trace_id: None,
                fields: Vec::new(),
            });
        }

        let warn_and_above = inspector.logs(100, Some("warn"));
        assert_eq!(warn_and_above.len(), 2);
        assert_eq!(inspector.logs(100, None).len(), 4);
        assert_eq!(inspector.logs(1, None).len(), 1);
    }

    #[test]
    fn defaults_match_the_documented_config() {
        let cfg = InspectorConfig::default();
        assert!(cfg.enabled);
        assert!(cfg.ui);
        assert_eq!(cfg.retention_secs, 3600);
        assert_eq!(cfg.sample_interval(), Duration::from_secs(1));
        assert_eq!(
            InspectorConfig {
                sample_interval_secs: 0,
                ..InspectorConfig::default()
            }
            .sample_interval(),
            Duration::from_secs(1),
            "a zero interval must not make the reader spin"
        );
    }

    /// A `producer` workflow whose `ChannelSink` bridges into a `consumer`
    /// workflow's `ChannelSource`: each workflow keeps its own two nodes and
    /// own edge, and the bridge is reported separately.
    fn bridged_topology() -> Topology {
        Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![
                WorkflowTopology {
                    id: "producer".to_string(),
                    name: None,
                    nodes: vec![
                        topo_node("orders_in", "source", "FileSource", Some("Order")),
                        topo_node("bridge_out", "sink", "ChannelSink", Some("Order")),
                    ],
                    edges: vec![TopoEdge {
                        from: "orders_in".to_string(),
                        to: "bridge_out".to_string(),
                        branch: None,
                    }],
                },
                WorkflowTopology {
                    id: "consumer".to_string(),
                    name: None,
                    nodes: vec![
                        topo_node("bridge_in", "source", "ChannelSource", Some("Order")),
                        topo_node("orders_out", "sink", "FileSink", Some("Order")),
                    ],
                    edges: vec![TopoEdge {
                        from: "bridge_in".to_string(),
                        to: "orders_out".to_string(),
                        branch: None,
                    }],
                },
            ],
            bridges: vec![pcs_inspector_wire::BridgeEdge {
                channel: "bridge".to_string(),
                from: "bridge_out".to_string(),
                to: "bridge_in".to_string(),
            }],
        }
    }

    /// A bridge is rated from the consuming `ChannelSource`'s own rows
    /// first, like any source edge, even though the bridge's `from` endpoint
    /// is a sink: the rows are the same flow in the better unit.
    #[test]
    fn bridge_prefers_the_consumer_source_rows() {
        let topology = bridged_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "bridge_in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "bridge_in", 7.0),
            attributed_counter_sample(1_000, SINK_BATCHES, SINK_ATTR, "bridge_out", 0.0),
            attributed_counter_sample(2_000, SINK_BATCHES, SINK_ATTR, "bridge_out", 2.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let bridge = edges
            .iter()
            .find(|e| e.from == "bridge_out" && e.to == "bridge_in")
            .expect("bridge edge");
        assert_eq!(bridge.unit, "rows");
        assert!((bridge.rate_per_sec - 7.0).abs() < 1e-9);
    }

    /// Without the consumer's rows, the bridge falls back to the producing
    /// `ChannelSink`'s batches, in batches rather than rows.
    #[test]
    fn bridge_falls_back_to_the_producer_sink_batches() {
        let topology = bridged_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, SINK_BATCHES, SINK_ATTR, "bridge_out", 0.0),
            attributed_counter_sample(2_000, SINK_BATCHES, SINK_ATTR, "bridge_out", 2.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let bridge = edges
            .iter()
            .find(|e| e.from == "bridge_out" && e.to == "bridge_in")
            .expect("bridge edge");
        assert_eq!(bridge.unit, "batches");
        assert!((bridge.rate_per_sec - 2.0).abs() < 1e-9);
    }

    /// A bridge with no sample on either side is omitted, exactly like an
    /// edge whose upstream node has not sampled yet.
    #[test]
    fn unsampled_bridge_is_omitted() {
        let topology = bridged_topology();
        let edges = edge_rates(&topology, &[]);
        assert!(
            edges
                .iter()
                .find(|e| e.from == "bridge_out" && e.to == "bridge_in")
                .is_none()
        );
    }

    /// Edges from two workflows are each rated from their own workflow's
    /// nodes: the loop walks every workflow, not one.
    #[test]
    fn edges_from_each_workflow_are_rated() {
        let topology = bridged_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "orders_in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "orders_in", 10.0),
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "bridge_in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "bridge_in", 5.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let producer = edges
            .iter()
            .find(|e| e.to == "bridge_out")
            .expect("producer edge");
        let consumer = edges
            .iter()
            .find(|e| e.to == "orders_out")
            .expect("consumer edge");
        assert!((producer.rate_per_sec - 10.0).abs() < 1e-9);
        assert!((consumer.rate_per_sec - 5.0).abs() < 1e-9);
    }
}
