//! The Traces tab: a trace list plus an SVG waterfall.
//!
//! ## What one trace is
//!
//! The host opens one `workflow.batch` root span per iteration, holding a
//! `source.drain` per source node, a `runtime.run` per processor node, and a
//! `sink.write` per sink node. Under `runtime.run` sits whatever the runtime
//! opens: `pipeline.run` and its stage and system spans for a native pipeline,
//! one `processor.batch` for a WASM processor or a native plugin. A processor's
//! own `pipeline.stage` and `system.execute` spans stay inside the guest,
//! because `host-io` has no span import, so a wasm processor or a native
//! plugin is one bar rather than a subtree.

use std::collections::{HashMap, HashSet};

use leptos::prelude::*;
use leptos::task::spawn_local;
use pcs_inspector_wire::{SpanRecord, Topology, TraceDetail, TraceSummary};

use crate::api;
use crate::ui::{
    Badge, BadgeTone, Button, Card, CardContent, CardHeader, CardTitle, Table, TableBody,
    TableCell, TableHead, TableHeader, TableRow, Tooltip,
};

/// How many traces the list requests.
const TRACE_LIMIT: usize = 100;

/// Waterfall geometry.
const BAR_H: f64 = 18.0;
const BAR_GAP: f64 = 4.0;
const LABEL_W: f64 = 220.0;
const TRACK_W: f64 = 460.0;
/// Horizontal label indent per tree level.
const INDENT_W: f64 = 12.0;
/// Levels beyond which the label stops indenting, so it still fits `LABEL_W`.
const MAX_INDENT_LEVEL: usize = 6;

/// The Traces tab.
#[component]
pub fn TracesView(#[prop(into)] topology: Signal<Option<Topology>>) -> impl IntoView {
    let (traces, set_traces) = signal::<Vec<TraceSummary>>(Vec::new());
    let (selected, set_selected) = signal::<Option<u64>>(None);
    let (detail, set_detail) = signal::<Option<TraceDetail>>(None);
    let (loaded, set_loaded) = signal(false);

    let refresh = move || {
        spawn_local(async move {
            if let Ok(list) = api::traces(TRACE_LIMIT).await {
                set_traces.set(list);
            }
            set_loaded.set(true);
        });
    };
    refresh();

    let select = Callback::new(move |trace_id: u64| {
        set_selected.set(Some(trace_id));
        spawn_local(async move {
            match api::trace(trace_id).await {
                Ok(value) => set_detail.set(Some(value)),
                Err(_) => set_detail.set(None),
            }
        });
    });

    // A wasm processor and a native plugin are each one bar: neither one's
    // inner spans reach the host. Said under the waterfall, where the missing
    // depth is visible, rather than in the empty state, which now only means no
    // iteration has finished.
    let one_bar_processor = move || {
        topology.get().is_some_and(|topo| {
            topo.workflows
                .iter()
                .flat_map(|w| w.nodes.iter())
                .any(|node| {
                    node.runtime
                        .as_ref()
                        .is_some_and(|rt| rt.kind == "wasm" || rt.kind == "plugin")
                })
        })
    };

    view! {
        <div class="space-y-4">
            <Card>
                <CardHeader>
                    <div class="flex items-center justify-between gap-4">
                        <CardTitle>"Traces"</CardTitle>
                        <Button on_click=Callback::new(move |()| refresh())>"refresh"</Button>
                    </div>
                </CardHeader>
                <CardContent>
                    <Show
                        when=move || !traces.get().is_empty()
                        fallback=move || {
                            view! {
                                <p class="text-sm text-muted-foreground">
                                    {move || {
                                        if loaded.get() {
                                            "No traces retained yet. One trace appears per \
                                             workflow iteration, unless RUST_LOG suppresses pcs \
                                             spans."
                                        } else {
                                            "loading traces…"
                                        }
                                    }}
                                </p>
                            }
                        }
                    >
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    <TableHead>"trace"</TableHead>
                                    <TableHead>"root span"</TableHead>
                                    <TableHead>"duration"</TableHead>
                                    <TableHead>"spans"</TableHead>
                                    <TableHead>""</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {move || {
                                    traces
                                        .get()
                                        .into_iter()
                                        .map(|summary| {
                                            let trace_id = summary.trace_id;
                                            let is_selected = Signal::derive(move || {
                                                selected.get() == Some(trace_id)
                                            });
                                            view! {
                                                <TableRow
                                                    selected=is_selected
                                                    on_click=Callback::new(move |()| {
                                                        select.run(trace_id)
                                                    })
                                                >
                                                    <TableCell>
                                                        <span class="font-mono text-xs">
                                                            {trace_id.to_string()}
                                                        </span>
                                                    </TableCell>
                                                    <TableCell>
                                                        {summary.name.to_string()}
                                                    </TableCell>
                                                    <TableCell>
                                                        <span class="font-mono text-xs">
                                                            {format_micros(summary.duration_us)}
                                                        </span>
                                                    </TableCell>
                                                    <TableCell>
                                                        {summary.span_count.to_string()}
                                                    </TableCell>
                                                    <TableCell>
                                                        <Show when=move || summary.error>
                                                            <Badge tone=BadgeTone::Destructive>
                                                                "error"
                                                            </Badge>
                                                        </Show>
                                                    </TableCell>
                                                </TableRow>
                                            }
                                        })
                                        .collect_view()
                                }}
                            </TableBody>
                        </Table>
                    </Show>
                </CardContent>
            </Card>

            <Show when=move || detail.get().is_some()>
                <Card>
                    <CardHeader>
                        <CardTitle>
                            {move || {
                                format!("Waterfall · trace {}", selected.get().unwrap_or_default())
                            }}
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        {move || detail.get().map(waterfall)}
                        <Show when=one_bar_processor>
                            <p class="mt-2 text-sm text-muted-foreground">
                                "A wasm processor or a native plugin is one bar: its \
                                 pipeline.stage and system.execute spans stay inside it, and \
                                 neither host-io nor the plugin ABI carries a span import."
                            </p>
                        </Show>
                    </CardContent>
                </Card>
            </Show>
        </div>
    }
}

/// One bar per span, x by offset from trace start, indent by tree depth.
///
/// Rows are laid out depth first, not by start time: `started_unix_ms` is
/// millisecond-resolution and one iteration is often shorter than that, so
/// sorting a five-level tree on it would interleave parents and children
/// arbitrarily.
fn waterfall(detail: TraceDetail) -> AnyView {
    let TraceDetail { spans, logs } = detail;
    if spans.is_empty() {
        return view! { <p class="text-sm text-muted-foreground">"no spans"</p> }.into_any();
    }
    let ordered = depth_first(&spans);

    let start = spans.iter().map(|s| s.started_unix_ms).min().unwrap_or(0);
    let span_end = |span: &SpanRecord| span.started_unix_ms + span.duration_us / 1000;
    let end = spans
        .iter()
        .map(span_end)
        .max()
        .unwrap_or(start)
        .max(start + 1);
    #[allow(clippy::cast_precision_loss, reason = "millisecond spans are small")]
    let total_ms = (end - start) as f64;

    #[allow(
        clippy::cast_precision_loss,
        reason = "span counts are bounded by the buffer"
    )]
    let height = TOP_PAD + ordered.len() as f64 * (BAR_H + BAR_GAP);

    let bars: Vec<_> = ordered
        .iter()
        .enumerate()
        .map(|(index, &(span, level))| {
            #[allow(clippy::cast_precision_loss, reason = "span counts are small")]
            let row = index as f64;
            let y = TOP_PAD + row * (BAR_H + BAR_GAP);
            #[allow(clippy::cast_precision_loss, reason = "millisecond offsets are small")]
            let offset_ms = (span.started_unix_ms - start) as f64;
            let width_ms = (span.duration_us as f64 / 1000.0).max(0.5);
            let w = (width_ms / total_ms * TRACK_W).clamp(1.5, TRACK_W);
            // `started_unix_ms` is millisecond-resolution while the track is
            // scaled to the trace's own span, so the last child of a trace
            // shorter than a few milliseconds rounds onto the right edge. Clamp
            // the bar back inside the track instead of drawing it off-canvas.
            let x = (LABEL_W + offset_ms / total_ms * TRACK_W).min(LABEL_W + TRACK_W - w);
            #[allow(clippy::cast_precision_loss, reason = "the level is capped at 6")]
            let depth = level.min(MAX_INDENT_LEVEL) as f64 * INDENT_W;
            let fields = span
                .fields
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(" ");
            let hover = format!(
                "{}\n{}\n{}",
                span.name,
                format_micros(span.duration_us),
                if fields.is_empty() {
                    "no fields"
                } else {
                    &fields
                }
            );

            view! {
                <g>
                    <text
                        x=format!("{depth:.0}")
                        y=format!("{:.1}", y + 13.0)
                        font-size="11"
                        fill="var(--muted-foreground)"
                    >
                        {span.name.to_string()}
                    </text>
                    <rect
                        x=format!("{x:.1}")
                        y=format!("{y:.1}")
                        width=format!("{w:.1}")
                        height=format!("{BAR_H}")
                        rx="4"
                        fill="var(--data)"
                        opacity="0.85"
                    >
                        <title>{hover}</title>
                    </rect>
                </g>
            }
        })
        .collect();

    view! {
        <div class="space-y-2">
            <svg
                viewBox=format!("0 0 {:.0} {height:.0}", LABEL_W + TRACK_W)
                preserveAspectRatio="xMidYMid meet"
                class="w-full"
            >
                {bars}
            </svg>
            <Show when={
                let has_logs = !logs.is_empty();
                move || has_logs
            }>
                <ul class="space-y-1 font-mono text-xs">
                    {logs
                        .clone()
                        .into_iter()
                        .map(|record| {
                            let fields = record
                                .fields
                                .iter()
                                .map(|(key, value)| format!("{key}={value}"))
                                .collect::<Vec<_>>()
                                .join(" ");
                            view! {
                                <li>
                                    <Tooltip content=Signal::derive({
                                        let fields = fields.clone();
                                        move || {
                                            if fields.is_empty() {
                                                "no fields".to_string()
                                            } else {
                                                fields.clone()
                                            }
                                        }
                                    })>
                                        <span class="text-foreground">
                                            {record.level.to_string()}
                                        </span>
                                    </Tooltip>
                                    " "
                                    <span class="text-muted-foreground">
                                        {record.message.clone()}
                                    </span>
                                </li>
                            }
                        })
                        .collect_view()}
                </ul>
            </Show>
        </div>
    }
    .into_any()
}

/// Order `spans` parent before child, returning each with its tree level.
///
/// A span whose `parent_id` is absent from `spans` is treated as a root: the
/// retention window can expire a parent while its children survive, and
/// dropping the orphans would hide work that ran.
fn depth_first(spans: &[SpanRecord]) -> Vec<(&SpanRecord, usize)> {
    let mut children: HashMap<Option<u64>, Vec<&SpanRecord>> = HashMap::new();
    let present: HashSet<u64> = spans.iter().map(|span| span.span_id).collect();
    for span in spans {
        let key = span.parent_id.filter(|id| present.contains(id));
        children.entry(key).or_default().push(span);
    }
    for group in children.values_mut() {
        group.sort_by_key(|span| (span.started_unix_ms, span.span_id));
    }

    // Explicit stack rather than recursion: the tree comes off the wire, so its
    // depth is not a local invariant.
    let mut ordered = Vec::with_capacity(spans.len());
    let mut stack: Vec<(&SpanRecord, usize)> = children
        .get(&None)
        .map(|roots| roots.iter().rev().map(|span| (*span, 0)).collect())
        .unwrap_or_default();
    while let Some((span, level)) = stack.pop() {
        ordered.push((span, level));
        if ordered.len() > spans.len() {
            // A parent cycle would otherwise loop forever. Cannot happen with
            // `tracing`'s ids, but the wire is not a proof.
            break;
        }
        if let Some(group) = children.get(&Some(span.span_id)) {
            stack.extend(group.iter().rev().map(|child| (*child, level + 1)));
        }
    }
    ordered
}

/// Top padding above the first waterfall bar.
const TOP_PAD: f64 = 8.0;

/// `1234` as `1.23ms`, `900` as `900µs`, `2_500_000` as `2.50s`.
pub fn format_micros(micros: u64) -> String {
    #[allow(clippy::cast_precision_loss, reason = "durations stay inside f64")]
    let us = micros as f64;
    if us >= 1_000_000.0 {
        format!("{:.2}s", us / 1_000_000.0)
    } else if us >= 1_000.0 {
        format!("{:.2}ms", us / 1_000.0)
    } else {
        format!("{us:.0}µs")
    }
}
