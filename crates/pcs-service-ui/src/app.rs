//! The dashboard shell: a fixed left rail plus three tabbed views.
//!
//! One poll drives everything on the Pipelines tab. `/api/snapshot` is the only
//! endpoint on a timer; traces and logs fetch when their tab activates or the
//! viewer asks. The timer skips a tick while the document is hidden, so a
//! backgrounded tab stops asking.

use leptos::prelude::*;
use leptos::task::spawn_local;
use std::time::Duration;

use pcs_inspector_wire::{Snapshot, Topology};

use crate::api;
use crate::components::{LogsView, PipelinesView, TracesView};
use crate::ui::{Badge, BadgeTone, Separator};

/// How much series history the graph's sparklines cover.
const WINDOW_SECS: u64 = 300;

/// Which tab is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    /// The topology graph.
    Pipelines,
    /// Trace list and waterfall.
    Traces,
    /// Log tail.
    Logs,
}

/// The dashboard root.
#[component]
pub fn App() -> impl IntoView {
    let (topology, set_topology) = signal::<Option<Topology>>(None);
    let (snapshot, set_snapshot) = signal::<Option<Snapshot>>(None);
    let (error, set_error) = signal::<Option<String>>(None);
    let (tab, set_tab) = signal(Tab::Pipelines);

    // The topology is fixed for the process lifetime, so it is fetched once.
    spawn_local(async move {
        match api::topology().await {
            Ok(value) => set_topology.set(Some(value)),
            Err(message) => set_error.set(Some(message)),
        }
    });

    let poll = move || {
        spawn_local(async move {
            match api::snapshot(WINDOW_SECS).await {
                Ok(value) => {
                    set_snapshot.set(Some(value));
                    set_error.set(None);
                }
                Err(message) => set_error.set(Some(message)),
            }
        });
    };
    poll();

    let handle = set_interval_with_handle(
        move || {
            if !document().hidden() {
                poll();
            }
        },
        Duration::from_millis(1000),
    )
    .expect("setInterval is available in every browser that can run WebAssembly");
    on_cleanup(move || handle.clear());

    view! {
        <div class="flex min-h-screen bg-background text-foreground">
            <Rail topology=topology snapshot=snapshot error=error />
            <main class="flex-1 p-6">
                <crate::ui::TabBar>
                    <crate::ui::TabButton
                        active=Signal::derive(move || tab.get() == Tab::Pipelines)
                        on_select=Callback::new(move |()| set_tab.set(Tab::Pipelines))
                    >
                        "Pipelines"
                    </crate::ui::TabButton>
                    <crate::ui::TabButton
                        active=Signal::derive(move || tab.get() == Tab::Traces)
                        on_select=Callback::new(move |()| set_tab.set(Tab::Traces))
                    >
                        "Traces"
                    </crate::ui::TabButton>
                    <crate::ui::TabButton
                        active=Signal::derive(move || tab.get() == Tab::Logs)
                        on_select=Callback::new(move |()| set_tab.set(Tab::Logs))
                    >
                        "Logs"
                    </crate::ui::TabButton>
                </crate::ui::TabBar>

                <div class="mt-6">
                    <Show when=move || tab.get() == Tab::Pipelines>
                        <PipelinesView topology=topology snapshot=snapshot />
                    </Show>
                    <Show when=move || tab.get() == Tab::Traces>
                        <TracesView topology=topology />
                    </Show>
                    <Show when=move || tab.get() == Tab::Logs>
                        <LogsView />
                    </Show>
                </div>
            </main>
        </div>
    }
}

/// Node identity, readiness, uptime and buffer occupancy.
#[component]
fn Rail(
    #[prop(into)] topology: Signal<Option<Topology>>,
    #[prop(into)] snapshot: Signal<Option<Snapshot>>,
    #[prop(into)] error: Signal<Option<String>>,
) -> impl IntoView {
    let node_id = move || {
        topology
            .get()
            .map_or_else(|| "…".to_string(), |t| t.node_id)
    };
    let mode = move || topology.get().map_or_else(|| "…".to_string(), |t| t.mode);
    let uptime = move || {
        snapshot
            .get()
            .map_or_else(|| "…".to_string(), |s| format_duration(s.uptime_secs))
    };
    let ready = move || snapshot.get().is_some_and(|s| s.ready);
    let series_value = move |name: &'static str| {
        snapshot.get().and_then(|s| {
            s.series
                .iter()
                .find(|series| series.name == name && series.attrs.is_empty())
                .map(|series| series.value)
        })
    };
    let runs = move || series_value("pcs_workflow_runs_total").unwrap_or(0.0);
    let errors = move || series_value("pcs_workflow_errors_total").unwrap_or(0.0);
    let buffers = move || snapshot.get().map(|s| s.buffers);

    view! {
        <aside class="w-64 shrink-0 border-r border-border bg-card p-6">
            <div class="text-sm font-semibold tracking-tight">"pcs-service"</div>
            <div class="mt-1 font-mono text-xs text-muted-foreground">
                {move || format!("node {} · {}", node_id(), mode())}
            </div>

            <div class="mt-4 flex flex-wrap gap-1.5">
                <Show
                    when=move || ready()
                    fallback=|| {
                        view! { <Badge tone=BadgeTone::Secondary>"not ready"</Badge> }
                    }
                >
                    <Badge tone=BadgeTone::Primary>"ready"</Badge>
                </Show>
                <Show when=move || { errors() > 0.0 }>
                    <Badge tone=BadgeTone::Destructive>
                        {move || format!("{} errors", errors() as u64)}
                    </Badge>
                </Show>
            </div>

            <Separator />

            <dl class="space-y-2 text-xs">
                <Fact label="uptime" value=Signal::derive(uptime) />
                <Fact
                    label="workflow runs"
                    value=Signal::derive(move || format!("{}", runs() as u64))
                />
                <Fact
                    label="workflows"
                    value=Signal::derive(move || {
                        topology
                            .get()
                            .map_or_else(|| "…".to_string(), |t| t.workflows.len().to_string())
                    })
                />
            </dl>

            <Separator />

            <div class="text-xs font-medium text-muted-foreground">"buffers"</div>
            <dl class="mt-2 space-y-2 text-xs">
                <Fact
                    label="spans"
                    value=Signal::derive(move || {
                        buffers().map_or_else(|| "…".to_string(), |b| b.spans.to_string())
                    })
                />
                <Fact
                    label="logs"
                    value=Signal::derive(move || {
                        buffers().map_or_else(|| "…".to_string(), |b| b.logs.to_string())
                    })
                />
                <Fact
                    label="samples"
                    value=Signal::derive(move || {
                        buffers().map_or_else(|| "…".to_string(), |b| b.samples.to_string())
                    })
                />
                <Fact
                    label="dropped"
                    value=Signal::derive(move || {
                        buffers().map_or_else(|| "…".to_string(), |b| b.dropped.to_string())
                    })
                />
            </dl>

            <Show when=move || error.get().is_some()>
                <div class="mt-4 rounded-md border border-border bg-muted p-2 font-mono text-xs text-muted-foreground">
                    {move || error.get().unwrap_or_default()}
                </div>
            </Show>
        </aside>
    }
}

/// One `label: value` row in the rail.
#[component]
fn Fact(label: &'static str, #[prop(into)] value: Signal<String>) -> impl IntoView {
    view! {
        <div class="flex items-baseline justify-between gap-2">
            <dt class="text-muted-foreground">{label}</dt>
            <dd class="font-mono">{move || value.get()}</dd>
        </div>
    }
}

/// `412s` as `6m 52s`, `9412s` as `2h 36m`.
fn format_duration(secs: u64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m {}s", s / 60, s % 60),
        s => format!("{}h {}m", s / 3600, (s % 3600) / 60),
    }
}
