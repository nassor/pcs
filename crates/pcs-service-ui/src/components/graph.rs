//! The Pipelines tab: the workflow topology as an animated SVG.
//!
//! ## Why the structure is built once
//!
//! The graph's shape comes from `/api/topology`, which is fixed for the
//! process lifetime; only the numbers change. The element tree is therefore
//! created once, and every live value is a reactive attribute closure inside
//! it. Re-rendering the whole `<svg>` each second would restart the dash
//! animations and the `<animateMotion>` particles on every poll, so the flow
//! would visibly stutter at exactly 1 Hz.
//!
//! ## Colour vocabulary
//!
//! The `--dgm-*` custom properties are the same ones `docs/static/styles.css`
//! uses, and they mean the same thing here: source and sink boxes are the data
//! plane (`--dgm-hd-data`), a processor box is the host-to-WebAssembly boundary
//! (`--dgm-hd-bnd`), and control-plane facts are teal (`--dgm-hd-ctl`). They
//! are read directly as CSS variables rather than through Tailwind's utility
//! generator, because they style a handful of SVG shapes rather than layout.
//!
//! ## Where a node box's number comes from
//!
//! Every node records its own metric series under its own declared id: a source
//! under [`SOURCE_ATTR`], a processor under [`PROCESSOR_ATTR`], a sink under
//! [`SINK_ATTR`]. Each box reads the copy carrying its own id, so its
//! throughput, latency, sparkline and retry badge describe that one node. The
//! unattributed copy of the same series is the process-wide sum over every
//! node — what a `/metrics` consumer reads — so no box shows it.
//! `pcs_stage_duration_seconds` is the exception: the host's span metrics layer
//! records it with no attributes at all, so a native processor's box reads the
//! process-wide value.
//!
//! A sink is the exception in the other direction. Its own series counts
//! batches, and a batch is whatever row count the upstream stage handed over,
//! so a batch rate says nothing about records moved. Its box therefore reads
//! the rows series of the node feeding it, the same one the server rated that
//! edge from, and reports records per second. Only a native runtime, which
//! writes no per-row series at all, leaves the box on its own batch count, and
//! then it says `batches`.
//!
//! ## How the columns are chosen
//!
//! A workflow is a DAG, so a node's column is its depth: the longest path from
//! any entry node to it. The nodes of a `WorkflowTopology` arrive in
//! topological order, so one forward pass relaxing every edge settles every
//! depth. A `source -> sink` pass-through is two columns and a two-processor
//! chain is four, which is what makes every edge point forward.
//!
//! Each workflow lays out on its own `viewBox`, so two workflows never share a
//! depth column: a node's column is its depth inside its own DAG.
//! Cross-workflow channel bridges are listed in their own card below the
//! workflow cards, because an edge between two independent `<svg>`s has no
//! shared coordinate space to draw in.

use std::collections::HashMap;

use leptos::prelude::*;
use leptos::task::spawn_local;
use pcs_inspector_wire::{
    BridgeEdge, EdgeRate, LogRecord, PROCESSOR_ATTR, Pair, PointAt, SINK_ATTR, SOURCE_ATTR,
    SeriesSummary, Snapshot, TopoEdge, TopoNode, Topology, WORKFLOW_ATTR, WindowInfo,
    WorkflowTopology,
};

use crate::api;
use crate::ui::{Badge, BadgeTone, Sheet};

/// Node box geometry, in viewBox units.
const NODE_W: f64 = 180.0;
const NODE_H: f64 = 76.0;
const NODE_GAP: f64 = 18.0;
/// Horizontal pitch between adjacent columns. At `NODE_W = 180` this leaves a
/// 66px gap between box edges. The same pitch separates every depth column from
/// the next, so a source, one processor and a sink lay out pixel-identical to
/// the graph before a workflow could branch.
const COL_PITCH: f64 = 246.0;
const SOURCE_X: f64 = 24.0;
const TOP: f64 = 16.0;

/// One node with its resolved position.
#[derive(Clone)]
struct Placed {
    node: TopoNode,
    x: f64,
    y: f64,
}

impl Placed {
    fn centre_y(&self) -> f64 {
        self.y + NODE_H / 2.0
    }
}

/// The name a workflow or node shows, falling back to its id.
///
/// A name is optional on the wire: a config that named nothing leaves it
/// absent, and an id is always present and unique workflow-wide.
fn display(name: &Option<String>, id: &str) -> String {
    match name {
        Some(name) if !name.trim().is_empty() => name.clone(),
        _ => id.to_string(),
    }
}

/// Place every node in its depth column, left to right.
///
/// A node's column is the longest path from any entry node to it, so every edge
/// points forward: two chained processors sharing one column would draw as a
/// backwards hook. `nodes` arrives in topological order, so relaxing each
/// node's outgoing edges once, in that order, settles every depth. A node whose
/// id no edge names keeps depth zero and lands in the first column.
///
/// Nodes inside a column keep topology order, so the layout never reorders
/// between polls.
///
/// Returns the placed nodes, the graph height, and the `viewBox` width, which
/// grows with the deepest column.
fn layout(nodes: &[TopoNode], edges: &[TopoEdge]) -> (Vec<Placed>, f64, f64) {
    let index: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, node)| (node.id.as_str(), i))
        .collect();
    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for edge in edges {
        if let (Some(&from), Some(&to)) =
            (index.get(edge.from.as_str()), index.get(edge.to.as_str()))
        {
            outgoing[from].push(to);
        }
    }
    let mut depth: Vec<usize> = vec![0; nodes.len()];
    for (from, targets) in outgoing.iter().enumerate() {
        for &to in targets {
            depth[to] = depth[to].max(depth[from] + 1);
        }
    }

    let columns = depth.iter().copied().max().map_or(0, |deepest| deepest + 1);
    let mut by_column: Vec<Vec<usize>> = vec![Vec::new(); columns];
    for (i, &at) in depth.iter().enumerate() {
        by_column[at].push(i);
    }

    let column_height = |count: usize| {
        #[allow(clippy::cast_precision_loss, reason = "node counts are small")]
        let count = count.max(1) as f64;
        count * NODE_H + (count - 1.0) * NODE_GAP
    };
    let tallest = by_column
        .iter()
        .map(|column| column_height(column.len()))
        .fold(NODE_H, f64::max);
    let height = TOP * 2.0 + tallest;

    let mut placed = Vec::with_capacity(nodes.len());
    for (at, column) in by_column.iter().enumerate() {
        #[allow(clippy::cast_precision_loss, reason = "column counts are small")]
        let x = SOURCE_X + COL_PITCH * at as f64;
        let offset = TOP + (tallest - column_height(column.len())) / 2.0;
        for (row, &i) in column.iter().enumerate() {
            #[allow(clippy::cast_precision_loss, reason = "node counts are small")]
            let row = row as f64;
            placed.push(Placed {
                node: nodes[i].clone(),
                x,
                y: offset + row * (NODE_H + NODE_GAP),
            });
        }
    }
    #[allow(clippy::cast_precision_loss, reason = "column counts are small")]
    let last_x = SOURCE_X + COL_PITCH * columns.saturating_sub(1) as f64;
    let view_w = last_x + NODE_W + SOURCE_X;

    (placed, height, view_w)
}

/// Every distinct runtime kind the workflow's processors run under, in first
/// appearance order.
///
/// The header carried one pipeline's single runtime before; a workflow can mix
/// a wasm processor with a plugin one, so the badges say what is loaded and each
/// processor's own version, state and fingerprint moved to its detail sheet.
fn runtime_kinds(nodes: &[TopoNode]) -> Vec<String> {
    let mut kinds: Vec<String> = Vec::new();
    for kind in nodes
        .iter()
        .filter_map(|node| node.runtime.as_ref())
        .map(|runtime| &runtime.kind)
    {
        if !kinds.iter().any(|seen| seen == kind) {
            kinds.push(kind.clone());
        }
    }
    kinds
}

/// A cubic from the right edge of one box to the left edge of another.
fn edge_path(from: &Placed, to: &Placed) -> String {
    let x1 = from.x + NODE_W;
    let y1 = from.centre_y();
    let x2 = to.x;
    let y2 = to.centre_y();
    let mid = f64::midpoint(x1, x2);
    format!("M {x1:.1} {y1:.1} C {mid:.1} {y1:.1}, {mid:.1} {y2:.1}, {x2:.1} {y2:.1}")
}

/// Look up one edge's live rate by its exact `(from, to)` pair.
fn edge_rate(snapshot: &Option<Snapshot>, from: &str, to: &str) -> Option<EdgeRate> {
    snapshot.as_ref().and_then(|s| {
        s.edges
            .iter()
            .find(|e| e.from == from && e.to == to)
            .cloned()
    })
}

/// One series' numbers as a node box reads them.
struct Reading {
    /// Newest value: a counter's total, a histogram's sum.
    value: f64,
    /// Histogram observation count; `0` for a counter or a gauge.
    count: u64,
    /// Per-second rate across the two newest samples.
    rate_per_sec: f64,
    /// History over the polled window, oldest first.
    points: Vec<PointAt>,
}

/// The first series `want` accepts, read into a [`Reading`].
fn find_series(
    snapshot: &Option<Snapshot>,
    want: impl Fn(&SeriesSummary) -> bool,
) -> Option<Reading> {
    snapshot.as_ref().and_then(|snap| {
        snap.series
            .iter()
            .find(|series| want(series))
            .map(|series| Reading {
                value: series.value,
                count: series.count,
                rate_per_sec: series.rate_per_sec,
                points: series.points.clone(),
            })
    })
}

/// The value `attrs` carries under `key`, if it carries one.
fn attr<'a>(attrs: &'a [Pair], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

/// One unattributed series: the process-wide form, summed over every node.
///
/// Only read for a series no node or workflow attributes:
/// `pcs_stage_duration_seconds`, which the host's span metrics layer records
/// without fields.
fn series(snapshot: &Option<Snapshot>, name: &str) -> Option<Reading> {
    find_series(snapshot, |series| {
        series.name == name && series.attrs.is_empty()
    })
}

/// The `name` series attributed to one node id, under that node kind's `key`.
///
/// A miss means the node has recorded nothing yet, never that the number lives
/// somewhere else: the attributed copy appears with the node's first sample.
fn attributed(snapshot: &Option<Snapshot>, name: &str, key: &str, id: &str) -> Option<Reading> {
    find_series(snapshot, |series| {
        series.name == name && attr(&series.attrs, key) == Some(id)
    })
}

/// The sink's own inbound throughput, in the unit the server already picked for
/// the edge feeding it. `"records"` when the immediate upstream reports rows,
/// which is a source, or a wasm or plugin processor's
/// `pcs_processor_rows_out_total`. `"batches"` only when neither does, the
/// in-process native-runtime case with no per-row series at all.
///
/// The number comes from the upstream node's own series rather than
/// `EdgeRate::rate_per_sec` because that is what carries a history: an
/// `EdgeRate` is one current value with no points, and the box draws a
/// sparkline.
fn sink_reading(snapshot: &Option<Snapshot>, node_id: &str) -> Option<(Reading, &'static str)> {
    let snap = snapshot.as_ref()?;
    let inbound = snap.edges.iter().find(|e: &&EdgeRate| e.to == node_id)?;
    if inbound.unit == "rows" {
        let reading = attributed(
            snapshot,
            "pcs_processor_rows_out_total",
            PROCESSOR_ATTR,
            &inbound.from,
        )
        .or_else(|| {
            attributed(
                snapshot,
                "pcs_rows_processed_total",
                SOURCE_ATTR,
                &inbound.from,
            )
        })?;
        Some((reading, "records"))
    } else {
        let reading = attributed(
            snapshot,
            "pcs_sink_batches_written_total",
            SINK_ATTR,
            node_id,
        )?;
        Some((reading, "batches"))
    }
}

/// `12034.5` as `12.0k/s`, `3.2` as `3.2/s`.
fn format_rate(rate: f64, unit: &str) -> String {
    let scaled = if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}k", rate / 1_000.0)
    } else if rate >= 10.0 {
        format!("{rate:.0}")
    } else {
        format!("{rate:.1}")
    };
    format!("{scaled} {unit}/s")
}

/// Seconds as `1.2ms` / `340µs` / `2.1s`.
fn format_seconds(secs: f64) -> String {
    if secs >= 1.0 {
        format!("{secs:.2}s")
    } else if secs >= 0.001 {
        format!("{:.1}ms", secs * 1000.0)
    } else {
        format!("{:.0}µs", secs * 1_000_000.0)
    }
}

/// One line describing the whole window geometry, for the detail sheet.
fn window_spec_line(window: &WindowInfo) -> String {
    let geometry = match window.kind.as_str() {
        "sliding" => format!(
            "{} / {}",
            format_ms(window.size_ms),
            format_ms(window.slide_ms)
        ),
        "session" => format!("gap {}", format_ms(window.gap_ms)),
        _ => format_ms(window.size_ms),
    };
    let offset = window
        .offset_ms
        .filter(|&offset| offset != 0)
        .map_or_else(String::new, |offset| format!(" · offset {offset}ms"));
    format!("{} {}{offset}", window.kind, geometry)
}

/// The chip text on a windowed processor box: `⟐30s`, `⟐30s/5s` or `⟐gap5s`.
fn window_chip(window: &WindowInfo) -> String {
    match window.kind.as_str() {
        "sliding" => format!(
            "⟐{}/{}",
            format_ms(window.size_ms),
            format_ms(window.slide_ms)
        ),
        "session" => format!("⟐gap{}", format_ms(window.gap_ms)),
        _ => format!("⟐{}", format_ms(window.size_ms)),
    }
}

/// Milliseconds as a compact duration: `30000` → `30s`, `1500` → `1.5s`,
/// `500` → `500ms`.
fn format_ms(ms: Option<i64>) -> String {
    let Some(ms) = ms else {
        return "?".to_string();
    };
    if ms >= 1000 {
        if ms % 1000 == 0 {
            format!("{}s", ms / 1000)
        } else {
            format!("{:.1}s", ms as f64 / 1000.0)
        }
    } else {
        format!("{ms}ms")
    }
}

/// Epoch seconds as UTC wall-clock time, for a watermark reading.
fn format_epoch_utc(secs: f64) -> String {
    let secs = secs.max(0.0) as u64;
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02} UTC")
}

/// A 60x18 sparkline path from a series' history.
///
/// Returns an empty string for fewer than two points: a one-point line is a dot
/// that reads as noise rather than a trend.
fn sparkline(points: &[PointAt]) -> String {
    if points.len() < 2 {
        return String::new();
    }
    let min = points.iter().map(|p| p.v).fold(f64::INFINITY, f64::min);
    let max = points.iter().map(|p| p.v).fold(f64::NEG_INFINITY, f64::max);
    let span = if (max - min).abs() < f64::EPSILON {
        1.0
    } else {
        max - min
    };
    #[allow(
        clippy::cast_precision_loss,
        reason = "point counts are bounded at 121"
    )]
    let last = (points.len() - 1) as f64;
    let mut path = String::with_capacity(points.len() * 12);
    for (i, point) in points.iter().enumerate() {
        #[allow(
            clippy::cast_precision_loss,
            reason = "point counts are bounded at 121"
        )]
        let index = i as f64;
        let x = index / last * 60.0;
        let y = 18.0 - (point.v - min) / span * 18.0;
        if i == 0 {
            path.push_str(&format!("M {x:.1} {y:.1}"));
        } else {
            path.push_str(&format!(" L {x:.1} {y:.1}"));
        }
    }
    path
}

/// Dash animation period: faster with throughput, static at zero.
///
/// `8s / rate` clamped to a quarter second keeps a busy edge readable instead of
/// blurring into a solid line.
fn dash_duration(rate: f64) -> String {
    if rate <= 0.0 {
        return "0s".to_string();
    }
    let secs = (8.0 / rate.max(1.0)).clamp(0.25, 8.0);
    format!("{secs:.2}s")
}

/// Stroke width grows with the order of magnitude of the rate.
fn stroke_width(rate: f64) -> f64 {
    if rate <= 0.0 {
        return 1.0;
    }
    (1.0 + rate.max(1.0).log10()).clamp(1.0, 5.0)
}

/// The Pipelines tab.
#[component]
pub fn PipelinesView(
    #[prop(into)] topology: Signal<Option<Topology>>,
    #[prop(into)] snapshot: Signal<Option<Snapshot>>,
) -> impl IntoView {
    let (selected, set_selected) = signal::<Option<TopoNode>>(None);
    let (node_logs, set_node_logs) = signal::<Vec<LogRecord>>(Vec::new());

    let open_node = Callback::new(move |node: TopoNode| {
        if node.kind == "processor" {
            spawn_local(async move {
                if let Ok(records) = api::logs(10, None).await {
                    set_node_logs.set(records);
                }
            });
        }
        set_selected.set(Some(node));
    });

    let graph = move || {
        topology.get().map(|topo| {
            if topo.workflows.is_empty() {
                // A default `Topology` has no workflows, which is what
                // `/api/topology` returns before `build_all` publishes one.
                return view! {
                    <p class="text-sm text-muted-foreground">"no workflows declared"</p>
                }
                .into_any();
            }
            let sections: Vec<_> = topo
                .workflows
                .into_iter()
                .map(|workflow| workflow_view(workflow, snapshot, open_node))
                .collect();
            let bridges = (!topo.bridges.is_empty()).then(|| bridges_view(topo.bridges, snapshot));
            view! { <div class="space-y-4">{sections}{bridges}</div> }.into_any()
        })
    };

    let sheet_title = Signal::derive(move || {
        selected
            .get()
            .map_or_else(String::new, |node| display(&node.name, &node.id))
    });

    view! {
        <div class="space-y-4">
            <Show
                when=move || topology.get().is_some()
                fallback=|| {
                    view! {
                        <p class="text-sm text-muted-foreground">"loading topology…"</p>
                    }
                }
            >
                {graph}
            </Show>

            <Sheet
                open=Signal::derive(move || selected.get().is_some())
                title=sheet_title
                on_close=Callback::new(move |()| set_selected.set(None))
            >
                {move || {
                    selected
                        .get()
                        .map(|node| detail_view(node, snapshot, node_logs))
                }}
            </Sheet>
        </div>
    }
}

/// One workflow's card: its declared identity, its runtime badges, its own
/// run/error counters, and its own independently laid out graph.
///
/// Each workflow lays out on its own `viewBox`, so two workflows never share a
/// depth column: a node's column is its depth inside its own DAG.
fn workflow_view(
    workflow: WorkflowTopology,
    snapshot: Signal<Option<Snapshot>>,
    on_open: Callback<TopoNode>,
) -> AnyView {
    let (placed, height, view_w) = layout(&workflow.nodes, &workflow.edges);

    let by_id: HashMap<String, Placed> = placed
        .iter()
        .map(|placed| (placed.node.id.clone(), placed.clone()))
        .collect();

    // Drawn from the topology's own edge list rather than rebuilt as a
    // star: a workflow is a DAG, holding processor-to-processor and
    // source-to-sink edges no fixed shape can express. A missing
    // endpoint is skipped rather than panicking, so a future node kind
    // degrades to "not drawn" instead of a crash.
    let edge_views: Vec<_> = workflow
        .edges
        .iter()
        .filter_map(|edge| {
            let from = by_id.get(&edge.from)?.clone();
            let to = by_id.get(&edge.to)?.clone();
            Some(edge_view(from, to, edge.branch.clone(), snapshot))
        })
        .collect();

    let node_views: Vec<_> = placed
        .into_iter()
        .map(|placed| node_view(placed, snapshot, on_open, workflow.id.clone()))
        .collect();

    let title = display(&workflow.name, &workflow.id);
    // The id only earns a line of its own when it is not already the
    // title, which is every workflow the config named.
    let id = (title != workflow.id).then(|| workflow.id.clone());
    let kinds = runtime_kinds(&workflow.nodes);

    // Derived signals rather than plain closures: `Signal<T>` is `Copy`, so
    // the badges' own attribute closures can each read them without moving
    // the workflow id into every one. The attributed form is this workflow's
    // own counter; the unattributed one would be the sum across every
    // workflow the process runs.
    let runs = {
        let id = workflow.id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "pcs_workflow_runs_total",
                WORKFLOW_ATTR,
                &id,
            )
            .map_or(0.0, |reading| reading.value)
        })
    };
    let errors = {
        let id = workflow.id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "pcs_workflow_errors_total",
                WORKFLOW_ATTR,
                &id,
            )
            .map_or(0.0, |reading| reading.value)
        })
    };

    view! {
        <section class="rounded-xl border border-border bg-card p-4">
            <header class="mb-3 flex flex-wrap items-baseline gap-2">
                <h1 class="font-semibold">{title}</h1>
                {id
                    .map(|id| {
                        view! {
                            <span class="font-mono text-xs text-muted-foreground">
                                {id}
                            </span>
                        }
                    })}
                {kinds
                    .into_iter()
                    .map(|kind| {
                        view! { <Badge tone=BadgeTone::Outline>{kind}</Badge> }
                    })
                    .collect_view()}
                <Badge tone=BadgeTone::Outline>
                    {move || format!("{} runs", runs.get() as u64)}
                </Badge>
                <Show when=move || { errors.get() > 0.0 }>
                    <Badge tone=BadgeTone::Destructive>
                        {move || format!("{} errors", errors.get() as u64)}
                    </Badge>
                </Show>
            </header>
            <svg
                viewBox=format!("0 0 {view_w} {height:.0}")
                preserveAspectRatio="xMidYMid meet"
                class="w-full"
                style="max-height: 32rem"
            >
                {edge_views}
                {node_views}
            </svg>
        </section>
    }
    .into_any()
}

/// The channel bridges between workflows, listed rather than drawn: each
/// workflow renders its own `<svg>`, so an edge between two of them has no
/// shared coordinate space. Rated by the same `(from, to)` lookup as any edge.
///
/// `"idle"` rather than `0/s`: an omitted `EdgeRate` means neither side has
/// sampled yet, which is not the same as measured zero.
fn bridges_view(bridges: Vec<BridgeEdge>, snapshot: Signal<Option<Snapshot>>) -> AnyView {
    let rows: Vec<_> = bridges
        .into_iter()
        .map(|bridge| {
            let from = bridge.from.clone();
            let to = bridge.to.clone();
            let rate = move || {
                edge_rate(&snapshot.get(), &from, &to).map_or_else(
                    || "idle".to_string(),
                    |r| format_rate(r.rate_per_sec, &r.unit),
                )
            };
            view! {
                <li class="flex items-center gap-3 text-sm">
                    <Badge tone=BadgeTone::Secondary>{bridge.channel}</Badge>
                    <span class="font-mono text-xs text-muted-foreground">
                        {bridge.from} " → " {bridge.to}
                    </span>
                    <span class="ml-auto font-mono text-xs text-muted-foreground">{rate}</span>
                </li>
            }
        })
        .collect();

    view! {
        <section class="rounded-xl border border-border bg-card p-4">
            <h2 class="mb-3 font-semibold">"channel bridges"</h2>
            <ul class="space-y-2">{rows}</ul>
        </section>
    }
    .into_any()
}

/// One edge: a dashed flow line plus three particles when it is moving, and a
/// branch label chip when the edge carries one.
fn edge_view(
    from: Placed,
    to: Placed,
    branch: Option<String>,
    snapshot: Signal<Option<Snapshot>>,
) -> AnyView {
    let path = edge_path(&from, &to);
    // Derived signals rather than plain closures: `Signal<T>` is `Copy`, so the
    // same rate can be read from several attribute closures, including the
    // per-particle ones, without cloning the node ids into each.
    let rate = {
        let from_id = from.node.id.clone();
        let to_id = to.node.id.clone();
        Signal::derive(move || {
            edge_rate(&snapshot.get(), &from_id, &to_id).map_or(0.0, |edge| edge.rate_per_sec)
        })
    };

    let tooltip = {
        let from_id = from.node.id.clone();
        let to_id = to.node.id.clone();
        let branch = branch.clone();
        Signal::derive(move || {
            let mut lines = vec![format!("{from_id} → {to_id}")];
            if let Some(branch) = &branch {
                lines.push(format!("branch: {branch}"));
            }
            lines.push(edge_rate(&snapshot.get(), &from_id, &to_id).map_or_else(
                || "—".to_string(),
                |edge| format_rate(edge.rate_per_sec, &edge.unit),
            ));
            lines.join("\n")
        })
    };

    // The label sits over the edge's midpoint, where the dash animation runs
    // underneath it. The backing rect keeps the text readable over the line.
    let label = branch;
    let mid_x = f64::midpoint(from.x + NODE_W, to.x);
    let mid_y = (from.centre_y() + to.centre_y()) / 2.0;

    let particle_path = path.clone();
    view! {
        <g>
            <title>{move || tooltip.get()}</title>
            <path
                d=path.clone()
                fill="none"
                stroke="var(--dgm-edge)"
                stroke-width=move || format!("{:.1}", stroke_width(rate.get()))
                class="pcs-edge"
            />
            <path
                d=path.clone()
                fill="none"
                stroke="var(--data)"
                stroke-width=move || format!("{:.1}", stroke_width(rate.get()))
                stroke-dasharray="8 8"
                class="pcs-edge-flow"
                style=move || {
                    let duration = dash_duration(rate.get());
                    let opacity = if rate.get() > 0.0 { "0.9" } else { "0" };
                    format!("--pcs-dash-dur: {duration}; opacity: {opacity}")
                }
            />
            <Show when=move || { rate.get() > 0.0 }>
                {(0..3)
                    .map(|i| {
                        let begin = format!("{}s", f64::from(i) * 0.5);
                        let particle_path = particle_path.clone();
                        view! {
                            <circle r="3" fill="var(--data)" class="pcs-particle">
                                <animateMotion
                                    dur=move || dash_duration(rate.get() / 4.0)
                                    begin=begin
                                    repeatCount="indefinite"
                                    path=particle_path
                                />
                            </circle>
                        }
                    })
                    .collect_view()}
            </Show>
            {label
                .map(|label| {
                    let width = (10.0 + 7.0 * label.chars().count() as f64).min(140.0);
                    view! {
                        <g>
                            <rect
                                x=mid_x - width / 2.0
                                y=mid_y - 18.0
                                width=width
                                height="16"
                                rx="4"
                                fill="var(--dgm-blk)"
                                stroke="var(--dgm-edge)"
                            />
                            <text
                                x=mid_x
                                y=mid_y - 10.0
                                font-size="10"
                                text-anchor="middle"
                                dominant-baseline="middle"
                                fill="var(--foreground)"
                            >
                                {label}
                            </text>
                        </g>
                    }
                })}
        </g>
    }
    .into_any()
}

/// One node box: title, type, live number, sparkline.
fn node_view(
    placed: Placed,
    snapshot: Signal<Option<Snapshot>>,
    on_open: Callback<TopoNode>,
    workflow_id: String,
) -> AnyView {
    let node = placed.node.clone();
    let is_processor = node.kind == "processor";
    let fill = if is_processor {
        "var(--dgm-hd-bnd)"
    } else {
        "var(--dgm-hd-data)"
    };
    // Each processor carries its own runtime, so a workflow mixing a wasm
    // processor with a plugin one still reads the right series per box. The
    // wasm host and the native plugin host both record the six
    // `pcs_processor_*` series from the batch's run metrics; an in-process
    // native runtime reports no per-batch numbers and records none of them.
    let per_batch_series = node
        .runtime
        .as_ref()
        .is_some_and(|runtime| runtime.kind == "wasm" || runtime.kind == "plugin");
    let title = display(&node.name, &node.id);
    let subtitle = match &node.component {
        Some(component) => format!("{} · {component}", node.type_name),
        None => node.type_name.clone(),
    };

    // The live number differs by role: throughput on a connector, mean batch
    // latency on a processor. Every lookup carries this node's own id, so a box
    // never shows the process-wide sum over its siblings.
    let primary = {
        let node_id = node.id.clone();
        let kind = node.kind.clone();
        move || {
            let snap = snapshot.get();
            match kind.as_str() {
                "source" => attributed(&snap, "pcs_rows_processed_total", SOURCE_ATTR, &node_id)
                    .map(|reading| format_rate(reading.rate_per_sec, "records")),
                "sink" => sink_reading(&snap, &node_id)
                    .map(|(reading, unit)| format_rate(reading.rate_per_sec, unit)),
                _ => {
                    // A native runtime records none of the six series, and
                    // `pcs_stage_duration_seconds` comes from the host's span
                    // metrics layer with no attributes at all, so such a box
                    // reads the process-wide form.
                    let reading = if per_batch_series {
                        attributed(
                            &snap,
                            "pcs_processor_batch_duration_seconds",
                            PROCESSOR_ATTR,
                            &node_id,
                        )
                    } else {
                        series(&snap, "pcs_stage_duration_seconds")
                    };
                    reading.map(|reading| {
                        if reading.count == 0 {
                            "—".to_string()
                        } else {
                            #[allow(
                                clippy::cast_precision_loss,
                                reason = "observation counts stay well inside f64"
                            )]
                            let mean = reading.value / reading.count as f64;
                            format_seconds(mean)
                        }
                    })
                }
            }
            .unwrap_or_else(|| "—".to_string())
        }
    };

    let spark = {
        let node_id = node.id.clone();
        let kind = node.kind.clone();
        move || {
            let snap = snapshot.get();
            let reading = match kind.as_str() {
                "source" => attributed(&snap, "pcs_rows_processed_total", SOURCE_ATTR, &node_id),
                "sink" => sink_reading(&snap, &node_id).map(|(reading, _)| reading),
                _ if per_batch_series => attributed(
                    &snap,
                    "pcs_processor_rows_in_total",
                    PROCESSOR_ATTR,
                    &node_id,
                ),
                // A native runtime records none of the six `pcs_processor_*`
                // series, so its box traces its own workflow's iteration count
                // rather than nothing at all. The unattributed form would sum
                // every workflow the process runs.
                _ => attributed(
                    &snap,
                    "pcs_workflow_runs_total",
                    WORKFLOW_ATTR,
                    &workflow_id,
                ),
            };
            reading.map_or_else(String::new, |reading| sparkline(&reading.points))
        }
    };

    // A derived signal rather than a plain closure: `Signal<f64>` is `Copy`, so
    // the badge's own attribute closures can each read it without cloning this
    // node's id into every one of them.
    let retries = {
        let node_id = node.id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "pcs_processor_retries_total",
                PROCESSOR_ATTR,
                &node_id,
            )
            .map_or(0.0, |reading| reading.value)
        })
    };

    let tooltip = {
        let node = node.clone();
        let title = title.clone();
        let primary = primary.clone();
        Signal::derive(move || {
            let mut lines = vec![title.clone(), node.type_name.clone(), primary()];
            if let Some(component) = &node.component {
                lines.push(format!("component: {component}"));
            }
            for (key, value) in &node.detail {
                lines.push(format!("{key}: {value}"));
            }
            lines.join("\n")
        })
    };

    // The windowing chip: a teal tag in the box's top-right corner. A
    // derived signal so the `Show` gate and the chip body can both read it
    // without moving the declaration.
    let window = node.window.clone();
    let chip = Signal::derive(move || {
        window.as_ref().map(|w| {
            let label = window_chip(w);
            let width = (14.0 + 5.2 * label.chars().count() as f64).min(76.0);
            (label, width)
        })
    });

    let x = placed.x;
    let y = placed.y;
    let click_node = node.clone();

    view! {
        <g
            transform=format!("translate({x:.1}, {y:.1})")
            class="cursor-pointer"
            on:click=move |_| on_open.run(click_node.clone())
        >
            <rect
                width=format!("{NODE_W}")
                height=format!("{NODE_H}")
                rx="10"
                fill="var(--dgm-blk)"
                stroke="var(--dgm-edge)"
            />
            <rect width=format!("{NODE_W}") height="24" rx="10" fill=fill />
            <rect y="14" width=format!("{NODE_W}") height="10" fill=fill />
            <text x="12" y="17" font-size="11" font-weight="600" fill="var(--foreground)">
                {title.clone()}
            </text>
            <text x="12" y="42" font-size="10" fill="var(--muted-foreground)">
                {subtitle}
            </text>
            <text x="12" y="62" font-size="13" font-weight="600" fill="var(--foreground)">
                {primary}
            </text>
            <g transform=format!("translate({:.1}, 44)", NODE_W - 72.0)>
                <path d=spark fill="none" stroke="var(--data)" stroke-width="1.5" />
            </g>
            <Show when=move || { is_processor && retries.get() > 0.0 }>
                <g transform=format!("translate({:.1}, 6)", NODE_W - 44.0)>
                    <rect width="36" height="14" rx="7" fill="var(--destructive)" />
                    <text x="18" y="11" text-anchor="middle" font-size="9" fill="white">
                        {move || format!("↻{}", retries.get() as u64)}
                    </text>
                </g>
            </Show>
            <Show when=move || { chip.get().is_some() }>
                <g transform=format!("translate({:.1}, 6)", NODE_W - 92.0)>
                    {move || {
                        if let Some((label, width)) = chip.get() {
                            view! {
                                <>
                                    <rect
                                        width=width
                                        height="14"
                                        rx="7"
                                        fill="var(--dgm-hd-ctl)"
                                    />
                                    <text
                                        x=width / 2.0
                                        y="11"
                                        text-anchor="middle"
                                        font-size="9"
                                        fill="var(--foreground)"
                                    >
                                        {label}
                                    </text>
                                </>
                            }
                            .into_any()
                        } else {
                            ().into_any()
                        }
                    }}
                </g>
            </Show>
            <title>{move || tooltip.get()}</title>
        </g>
    }
    .into_any()
}

/// The right-hand detail panel for one node.
fn detail_view(
    node: TopoNode,
    snapshot: Signal<Option<Snapshot>>,
    node_logs: ReadSignal<Vec<LogRecord>>,
) -> AnyView {
    let is_processor = node.kind == "processor";
    let detail = node.detail.clone();
    let component = node.component.clone();
    let node_id = node.id.clone();
    // A processor's `detail` already carries its version, stateful flag and
    // artifact, so these are the rows only its `runtime` holds.
    let runtime_rows: Vec<(&str, String)> = node.runtime.map_or_else(Vec::new, |runtime| {
        let mut rows = vec![("runtime", runtime.kind), ("runtime name", runtime.name)];
        if !runtime.schema_fingerprint.is_empty() {
            rows.push(("schema fingerprint", runtime.schema_fingerprint));
        }
        if !runtime.declared_components.is_empty() {
            rows.push(("components", runtime.declared_components.join(", ")));
        }
        rows
    });

    // A derived signal rather than a plain closure: `Signal<T>` is `Copy`, so
    // both the section's `when` and its rows can read it without cloning this
    // node's id into each.
    let processor_metrics: Signal<Vec<(String, f64, u64)>> = Signal::derive(move || {
        // Only the values carrying this processor's id. The unattributed copy of
        // `pcs_processor_metric` sums every processor's guest metric of the same
        // name, which belongs on no single sheet.
        snapshot.get().map_or_else(Vec::new, |snap| {
            snap.series
                .iter()
                .filter(|series| {
                    series.name == "pcs_processor_metric"
                        && attr(&series.attrs, PROCESSOR_ATTR) == Some(node_id.as_str())
                })
                .map(|series| {
                    let label = attr(&series.attrs, "metric")
                        .map_or_else(|| "metric".to_string(), ToString::to_string);
                    (label, series.value, series.count)
                })
                .collect()
        })
    });

    let span_stats = move || {
        snapshot
            .get()
            .map_or_else(Vec::new, |snap| snap.span_stats.clone())
    };

    // The live watermark: the `pcs_window_watermark_seconds` series attributed
    // to this node, formatted as UTC wall-clock time. Absent until the host's
    // tracker has seen its first timestamp.
    let watermark = {
        // `node_id` is moved into the derive closure above, so clone from the
        // still-owned node instead.
        let node_id = node.id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "pcs_window_watermark_seconds",
                PROCESSOR_ATTR,
                &node_id,
            )
            .map_or_else(
                || "—".to_string(),
                |reading| format_epoch_utc(reading.value),
            )
        })
    };

    let window_rows: Vec<(&str, String)> = node.window.map_or_else(Vec::new, |window| {
        let mut rows = vec![
            ("window", window_spec_line(&window)),
            ("time field", window.time_field.clone()),
        ];
        if !window.key_fields.is_empty() {
            rows.push(("key fields", window.key_fields.join(", ")));
        }
        rows.push((
            "allowed lateness",
            format_ms(Some(window.allowed_lateness_ms)),
        ));
        rows
    });
    // Static for the sheet's lifetime: the gate is a plain bool and the rows
    // render from a derived signal, so neither closure moves the vec.
    let has_window_rows = !window_rows.is_empty();
    let window_rows_signal = Signal::derive(move || window_rows.clone());

    view! {
        <div class="space-y-4">
            <dl class="space-y-1 text-xs">
                <div class="flex justify-between gap-3">
                    <dt class="text-muted-foreground">"kind"</dt>
                    <dd class="font-mono">{node.kind.clone()}</dd>
                </div>
                <div class="flex justify-between gap-3">
                    <dt class="text-muted-foreground">"type"</dt>
                    <dd class="font-mono">{node.type_name.clone()}</dd>
                </div>
                <Show when={
                    let component = component.clone();
                    move || component.is_some()
                }>
                    <div class="flex justify-between gap-3">
                        <dt class="text-muted-foreground">"component"</dt>
                        <dd class="font-mono">{component.clone().unwrap_or_default()}</dd>
                    </div>
                </Show>
                {runtime_rows
                    .into_iter()
                    .map(|(key, value)| {
                        view! {
                            <div class="flex justify-between gap-3">
                                <dt class="text-muted-foreground">{key}</dt>
                                <dd class="font-mono">{value}</dd>
                            </div>
                        }
                    })
                    .collect_view()}
                {detail
                    .iter()
                    .map(|(key, value)| {
                        view! {
                            <div class="flex justify-between gap-3">
                                <dt class="text-muted-foreground">{key.clone()}</dt>
                                <dd class="font-mono">{value.clone()}</dd>
                            </div>
                        }
                    })
                    .collect_view()}
            </dl>

            <Show when=move || has_window_rows>
                <div>
                    <h3 class="mb-1 text-xs font-medium text-muted-foreground">"windowing"</h3>
                    <dl class="space-y-1 text-xs">
                        {move || {
                            window_rows_signal
                                .get()
                                .into_iter()
                                .map(|(key, value)| {
                                    view! {
                                        <div class="flex justify-between gap-3">
                                            <dt class="text-muted-foreground">{key}</dt>
                                            <dd class="font-mono">{value}</dd>
                                        </div>
                                    }
                                })
                                .collect_view()
                        }}
                        <div class="flex justify-between gap-3">
                            <dt class="text-muted-foreground">"watermark"</dt>
                            <dd class="font-mono">{move || watermark.get()}</dd>
                        </div>
                    </dl>
                </div>
            </Show>

            <Show when=move || is_processor && !processor_metrics.get().is_empty()>
                <div>
                    <h3 class="mb-1 text-xs font-medium text-muted-foreground">
                        "processor metrics"
                    </h3>
                    <dl class="space-y-1 text-xs">
                        {move || {
                            processor_metrics
                                .get()
                                .into_iter()
                                .map(|(label, value, count)| {
                                    view! {
                                        <div class="flex justify-between gap-3">
                                            <dt class="font-mono">{label}</dt>
                                            <dd class="font-mono">
                                                {format!("{value:.3} / {count}")}
                                            </dd>
                                        </div>
                                    }
                                })
                                .collect_view()
                        }}
                    </dl>
                </div>
            </Show>

            <Show when=move || is_processor && !span_stats().is_empty()>
                <div>
                    <h3 class="mb-1 text-xs font-medium text-muted-foreground">
                        "stage & system latency"
                    </h3>
                    <dl class="space-y-1 text-xs">
                        {move || {
                            span_stats()
                                .into_iter()
                                .map(|stat| {
                                    view! {
                                        <div class="flex justify-between gap-3">
                                            <dt class="font-mono">
                                                {format!("{} {}", stat.span, stat.key)}
                                            </dt>
                                            <dd class="font-mono">
                                                {format!(
                                                    "p50 {}µs · p95 {}µs",
                                                    stat.p50_us,
                                                    stat.p95_us,
                                                )}
                                            </dd>
                                        </div>
                                    }
                                })
                                .collect_view()
                        }}
                    </dl>
                </div>
            </Show>

            <Show when=move || is_processor && !node_logs.get().is_empty()>
                <div>
                    <h3 class="mb-1 text-xs font-medium text-muted-foreground">"recent logs"</h3>
                    <ul class="space-y-1 font-mono text-xs">
                        {move || {
                            node_logs
                                .get()
                                .into_iter()
                                .map(|record| {
                                    view! {
                                        <li class="text-muted-foreground">
                                            <span class="text-foreground">
                                                {record.level.to_string()}
                                            </span>
                                            " "
                                            {record.message.clone()}
                                        </li>
                                    }
                                })
                                .collect_view()
                        }}
                    </ul>
                </div>
            </Show>
        </div>
    }
    .into_any()
}
