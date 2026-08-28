//! The JSON contract between the `pcs-service` inspector and its dashboard.
//!
//! Both sides of `/api/*` compile these types: the host serializes them from
//! `pcs_service::inspector`, and the `pcs-service-ui` WASM bundle deserializes
//! them in the browser. One definition, so the wire shape cannot drift.
//!
//! The crate carries `serde` and nothing else. `pcs-service` itself cannot
//! compile for `wasm32-unknown-unknown` (its non-optional `pcs-core` dependency
//! pulls tokio, and through it mio), so the shared types cannot live there
//! behind a feature: they need a crate the browser target can build.
//!
//! ## Conventions
//!
//! - Field names are `snake_case`, matching the Rust identifiers.
//! - Attribute and detail maps are arrays of two-element arrays, not JSON
//!   objects, so key order is part of the response and a client renders the
//!   same order the server produced.
//! - Every string that comes from `tracing` metadata is a
//!   [`Cow<'static, str>`](std::borrow::Cow): the host borrows the `&'static
//!   str` the macro already allocated, and the browser owns a decoded `String`.
//! - Timestamps are Unix milliseconds (`u64`), durations microseconds (`u64`)
//!   or fractional seconds (`f64`), never a locale-dependent string.

#![forbid(unsafe_code)]

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// A `(key, value)` pair as it appears on the wire: a two-element array.
pub type Pair = (String, String);

// ── Topology ─────────────────────────────────────────────────────────────────

/// The shape of the running service, as drawn by the dashboard.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// Bumped whenever the topology is replaced. A client compares it against
    /// [`Snapshot::topology_version`] to know its layout is stale.
    pub version: u64,
    /// `node.id` from config, as a string so a client never loses precision.
    pub node_id: String,
    /// `"standalone"` or `"cluster"`.
    pub mode: String,
    /// Every workflow this process runs, in declaration order.
    pub workflows: Vec<WorkflowTopology>,
    /// Cross-workflow channel bridges. Not part of any one workflow's `edges`:
    /// the two endpoints live in different workflows.
    #[serde(default)]
    pub bridges: Vec<BridgeEdge>,
}

/// The workflow: its declared identity, its nodes, and the edges between them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTopology {
    /// The workflow's declared id.
    pub id: String,
    /// Declared name, absent when the config named none.
    pub name: Option<String>,
    /// Every declared node, in topological order: a node always follows every
    /// node that links into it, within this workflow.
    pub nodes: Vec<TopoNode>,
    /// Every declared `link`.
    pub edges: Vec<TopoEdge>,
}

/// What executes a processor node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInfo {
    /// `"wasm"`, `"plugin"` or `"native"`.
    pub kind: String,
    /// The identity the runtime declares for itself: a processor's or plugin's
    /// own name, falling back to the host's pipeline name for a native runtime,
    /// which declares none.
    pub name: String,
    /// Version the runtime reports about itself; empty when it reports none.
    pub version: String,
    /// Whether the runtime carries state across batches.
    pub stateful: bool,
    /// Fingerprint of the runtime's component schemas; empty when it reports
    /// none.
    pub schema_fingerprint: String,
    /// Component names the runtime declares.
    pub declared_components: Vec<String>,
}

/// The windowing declaration of a processor node, as the dashboard reads it.
///
/// Mirrors the host's `WindowConfig` (which itself wraps the pcs-core
/// `WindowSpec`) field for field, minus pcs-core, because this crate is the
/// one type both the host and the browser can compile. The geometry fields are
/// `Option` because which ones apply depends on `kind`: tumbling and sliding
/// carry `size_ms`, sliding adds `slide_ms`, session carries `gap_ms`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WindowInfo {
    /// `"tumbling"` | `"sliding"` | `"session"`.
    pub kind: String,
    /// Fixed window size in milliseconds (tumbling, sliding).
    pub size_ms: Option<i64>,
    /// Slide interval in milliseconds (sliding).
    pub slide_ms: Option<i64>,
    /// Alignment offset in milliseconds (tumbling, sliding).
    pub offset_ms: Option<i64>,
    /// Session inactivity gap in milliseconds (session).
    pub gap_ms: Option<i64>,
    /// The event-time column every inbound component must carry.
    pub time_field: String,
    /// Grouping key columns, empty for a global window.
    pub key_fields: Vec<String>,
    /// How many milliseconds past the watermark a late row is still accepted.
    pub allowed_lateness_ms: i64,
}

/// One box in the dashboard graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopoNode {
    /// The declared id. Unique workflow-wide, so it is also the graph node id.
    pub id: String,
    /// `"source"` | `"processor"` | `"sink"`.
    pub kind: String,
    /// Declared name, absent when the config named none.
    pub name: Option<String>,
    /// Connector `type` for a source or sink, runtime kind for a processor.
    pub type_name: String,
    /// The component this node reads or writes. `None` for a processor.
    pub component: Option<String>,
    /// A processor's self-description. `None` for a source or sink.
    pub runtime: Option<RuntimeInfo>,
    /// The processor node's windowing declaration, when its config declares a
    /// `window` block. `None` for a non-windowed processor and for every
    /// source or sink. Defaults to `None` so a snapshot from a host without
    /// window support still decodes.
    #[serde(default)]
    pub window: Option<WindowInfo>,
    /// Allowlisted connector options for a source or sink, or a processor's
    /// version/stateful/artifact pairs. Never a blanket copy of the config
    /// table: a source's `config` holds DSNs and credentials.
    pub detail: Vec<Pair>,
}

/// A directed edge between two [`TopoNode`] ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopoEdge {
    /// Source node id.
    pub from: String,
    /// Destination node id.
    pub to: String,
    /// Branch name the edge carries, absent for an unlabelled link.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// A named in-process channel joining a `ChannelSink` in one workflow to a
/// `ChannelSource` in another.
///
/// Distinct from [`TopoEdge`] because it is not a declared `link`: no config
/// names both ends, they meet on the shared channel `name` alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeEdge {
    /// The channel `name` both halves declare.
    pub channel: String,
    /// The `ChannelSink` node id, in the producing workflow.
    pub from: String,
    /// The `ChannelSource` node id, in the consuming workflow.
    pub to: String,
}

// ── Metrics ──────────────────────────────────────────────────────────────────

/// The attribute key carrying a source node's id on its metric series.
pub const SOURCE_ATTR: &str = "source";
/// The attribute key carrying a processor node's id on its metric series.
///
/// Attribution is additive: every processor metric is recorded once with no
/// attributes, which is the process-wide total a `/metrics` consumer has
/// always seen, and once more under `processor="<id>"`.
pub const PROCESSOR_ATTR: &str = "processor";
/// The attribute key carrying a sink node's id on its metric series.
pub const SINK_ATTR: &str = "sink";
/// The attribute key carrying a branch name on a processor's per-edge series.
pub const BRANCH_ATTR: &str = "branch";
/// The attribute key carrying a workflow's declared id on its metric series.
///
/// Attribution is additive, as for the node keys: `pcs_workflow_runs_total`
/// and `pcs_workflow_errors_total` are each recorded once unattributed — the
/// process-wide total every `/metrics` consumer has always read — and once
/// more under `workflow="<id>"`. Summing both forms double counts.
pub const WORKFLOW_ATTR: &str = "workflow";

/// What kind of instrument produced a series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesKind {
    /// Monotonic total. [`SeriesSummary::rate_per_sec`] is its first
    /// derivative.
    Counter,
    /// Instantaneous value. `rate_per_sec` repeats the value itself.
    Gauge,
    /// Bucketed distribution. `value` is the sum, `count` the observation
    /// count, so a client can divide for the mean.
    Histogram,
}

/// One instrument's value at one export interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesPoint {
    /// Instrument name, e.g. `pcs_rows_processed_total`.
    pub name: String,
    /// Which instrument kind it came from.
    pub kind: SeriesKind,
    /// The data point's attributes, sorted by key.
    pub attrs: Vec<Pair>,
    /// Cumulative value for a counter or histogram sum, current value for a
    /// gauge.
    pub value: f64,
    /// Histogram observation count; `0` for a counter or gauge.
    pub count: u64,
}

/// Everything one metric export interval produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    /// When the export ran, Unix milliseconds.
    pub at_unix_ms: u64,
    /// Every data point in that export, flattened across scopes.
    pub series: Vec<SeriesPoint>,
}

/// One `(timestamp, value)` sample for a sparkline.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PointAt {
    /// Unix milliseconds.
    pub t: u64,
    /// The series value at `t`.
    pub v: f64,
}

/// One series as the dashboard reads it: latest value, rate, and history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesSummary {
    /// Instrument name.
    pub name: String,
    /// Which instrument kind it came from.
    pub kind: SeriesKind,
    /// The data point's attributes, sorted by key.
    pub attrs: Vec<Pair>,
    /// Newest value in the window.
    pub value: f64,
    /// Histogram observation count; `0` for a counter or gauge.
    pub count: u64,
    /// For a counter, `(newest - previous) / dt` across the two newest
    /// samples. For a gauge, the value itself. For a histogram, the rate of
    /// its sum.
    pub rate_per_sec: f64,
    /// History over the requested window, oldest first, decimated to at most
    /// [`MAX_POINTS`] entries.
    pub points: Vec<PointAt>,
}

/// How many history points a [`SeriesSummary`] carries at most.
///
/// A one-hour window at a one-second export interval is 3600 samples; sending
/// all of them per series per poll would dominate the response.
pub const MAX_POINTS: usize = 120;

/// A live throughput number for one [`TopoEdge`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeRate {
    /// Source node id.
    pub from: String,
    /// Destination node id.
    pub to: String,
    /// Items per second flowing along the edge.
    pub rate_per_sec: f64,
    /// What `rate_per_sec` counts: `"rows"` or `"batches"`. The last
    /// processor's edge to a sink falls back to batches when no processor row
    /// count exists, and says so rather than presenting batches as rows.
    pub unit: String,
}

/// Latency of one repeated span, grouped by one of its field values.
///
/// `pcs_stage_duration_seconds` is recorded with no attributes, so for a native
/// pipeline per-stage and per-system latency exists nowhere in the metric
/// series. These numbers come from the retained span records instead, grouped
/// on the field that identifies the unit of work. A wasm processor's or a
/// native plugin's own per-batch latency does exist as a series — the
/// [`PROCESSOR_ATTR`]-attributed `pcs_processor_batch_duration_seconds` —
/// because their inner spans never reach the host and leave this empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanStat {
    /// Span name the records came from, e.g. `pipeline.stage`.
    pub span: Cow<'static, str>,
    /// Value of the grouping field: the stage index, or the system name.
    pub key: String,
    /// How many retained records went into the numbers below.
    pub count: usize,
    /// Median duration, microseconds.
    pub p50_us: u64,
    /// 95th percentile duration, microseconds.
    pub p95_us: u64,
    /// Slowest retained occurrence, microseconds.
    pub max_us: u64,
}

/// How full the inspector's ring buffers are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferStats {
    /// Retained span records.
    pub spans: usize,
    /// Retained log records.
    pub logs: usize,
    /// Retained metric samples.
    pub samples: usize,
    /// Records dropped because a buffer hit its capacity bound, across all
    /// three buffers. Non-zero means the window is shorter than configured.
    pub dropped: u64,
}

/// The one document the dashboard polls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// [`Topology::version`] this snapshot was computed against.
    pub topology_version: u64,
    /// When the snapshot was taken, Unix milliseconds.
    pub sampled_at_unix_ms: u64,
    /// Seconds since the inspector was built.
    pub uptime_secs: u64,
    /// Whether the service reports itself ready.
    pub ready: bool,
    /// Every series with at least one sample in the window.
    pub series: Vec<SeriesSummary>,
    /// One entry per rated [`TopoEdge`], keyed by `(from, to)`. A
    /// processor-to-processor boundary is rated from the upstream processor's
    /// [`PROCESSOR_ATTR`]-attributed `pcs_processor_rows_out_total`. An edge
    /// whose series has no sample yet is omitted rather than reported as
    /// zero, so a lookup by `(from, to)` is the contract and position is not.
    pub edges: Vec<EdgeRate>,
    /// Per-stage and per-system latency, derived from the retained span
    /// records. Empty for a WASM-hosted pipeline: those spans open inside the
    /// guest and never reach the host.
    pub span_stats: Vec<SpanStat>,
    /// Ring-buffer occupancy.
    pub buffers: BufferStats,
}

// ── Spans and logs ───────────────────────────────────────────────────────────

/// One closed `tracing` span.
///
/// `trace_id` is the root span's `tracing` id, not a W3C trace id: this
/// telemetry never leaves the process, and minting 128-bit ids would need a
/// second id map for no local benefit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanRecord {
    /// Root span id of the tree this span belongs to.
    pub trace_id: u64,
    /// This span's id.
    pub span_id: u64,
    /// Parent span id, absent for a root span.
    pub parent_id: Option<u64>,
    /// Span name, e.g. `pipeline.stage`.
    pub name: Cow<'static, str>,
    /// Emitting module path.
    pub target: Cow<'static, str>,
    /// When the span opened, Unix milliseconds.
    pub started_unix_ms: u64,
    /// How long it stayed open, microseconds.
    pub duration_us: u64,
    /// Recorded fields, in the order the subscriber saw them.
    pub fields: Vec<Pair>,
}

/// One `tracing` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    /// `"ERROR"`, `"WARN"`, `"INFO"`, `"DEBUG"` or `"TRACE"`.
    pub level: Cow<'static, str>,
    /// Emitting module path.
    pub target: Cow<'static, str>,
    /// The event's `message` field, empty when it has none.
    pub message: String,
    /// When the event fired, Unix milliseconds.
    pub at_unix_ms: u64,
    /// Innermost open span, when the event fired inside one.
    pub span_id: Option<u64>,
    /// That span's trace id.
    pub trace_id: Option<u64>,
    /// Every field except `message`.
    pub fields: Vec<Pair>,
}

/// One row in the traces list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceSummary {
    /// Root span id of the tree.
    pub trace_id: u64,
    /// The root span's name.
    pub name: Cow<'static, str>,
    /// When the root span opened, Unix milliseconds.
    pub started_unix_ms: u64,
    /// Root span duration, microseconds.
    pub duration_us: u64,
    /// How many retained spans belong to the tree.
    pub span_count: usize,
    /// Whether any retained log line in the tree is at `ERROR`.
    pub error: bool,
}

/// Everything retained about one trace, for the waterfall view.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TraceDetail {
    /// Every span in the tree, oldest first.
    pub spans: Vec<SpanRecord>,
    /// Every log line emitted inside the tree, oldest first.
    pub logs: Vec<LogRecord>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_serialize_as_two_element_arrays() {
        let node = TopoNode {
            id: "orders-in".to_string(),
            kind: "source".to_string(),
            name: Some("Authorizations".to_string()),
            type_name: "NatsSource".to_string(),
            component: Some("Order".to_string()),
            runtime: None,
            window: None,
            detail: vec![("mode.kind".to_string(), "core".to_string())],
        };
        let json = serde_json::to_string(&node).expect("serialize");
        assert!(
            json.contains(r#""detail":[["mode.kind","core"]]"#),
            "got: {json}"
        );
    }

    #[test]
    fn series_kind_is_lowercase_on_the_wire() {
        let point = SeriesPoint {
            name: "pcs_rows_processed_total".to_string(),
            kind: SeriesKind::Counter,
            attrs: Vec::new(),
            value: 12.0,
            count: 0,
        };
        let json = serde_json::to_string(&point).expect("serialize");
        assert!(json.contains(r#""kind":"counter""#), "got: {json}");
    }

    #[test]
    fn span_record_round_trips_into_owned_strings() {
        let record = SpanRecord {
            trace_id: 1,
            span_id: 2,
            parent_id: Some(1),
            name: Cow::Borrowed("pipeline.stage"),
            target: Cow::Borrowed("pcs_core::pipeline::execution"),
            started_unix_ms: 1_750_000_000_000,
            duration_us: 1234,
            fields: vec![("stage".to_string(), "1".to_string())],
        };
        let json = serde_json::to_string(&record).expect("serialize");
        let back: SpanRecord = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, record);
        assert!(matches!(back.name, Cow::Owned(_)));
    }
}
