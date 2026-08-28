//! The Logs tab: the newest retained events, filtered by level.

use leptos::prelude::*;
use leptos::task::spawn_local;
use pcs_inspector_wire::LogRecord;

use crate::api;
use crate::ui::{Badge, BadgeTone, Button, Card, CardContent, CardHeader, CardTitle, ScrollArea};

/// How many records the tab requests.
const LOG_LIMIT: usize = 200;

/// The level filter's options. `None` is "everything the buffer holds".
const LEVELS: [(&str, Option<&str>); 5] = [
    ("all", None),
    ("debug", Some("debug")),
    ("info", Some("info")),
    ("warn", Some("warn")),
    ("error", Some("error")),
];

/// The Logs tab.
#[component]
pub fn LogsView() -> impl IntoView {
    let (records, set_records) = signal::<Vec<LogRecord>>(Vec::new());
    let (level, set_level) = signal::<Option<&'static str>>(None);

    let load = move |level: Option<&'static str>| {
        spawn_local(async move {
            if let Ok(list) = api::logs(LOG_LIMIT, level).await {
                set_records.set(list);
            }
        });
    };
    load(None);

    view! {
        <Card>
            <CardHeader>
                <div class="flex flex-wrap items-center justify-between gap-3">
                    <CardTitle>"Logs"</CardTitle>
                    <div class="flex items-center gap-1.5">
                        {LEVELS
                            .into_iter()
                            .map(|(label, value)| {
                                view! {
                                    <button
                                        type="button"
                                        class=move || {
                                            let base = "rounded-full border px-2 py-0.5 text-xs \
                                                        font-medium transition-colors";
                                            if level.get() == value {
                                                format!(
                                                    "{base} border-transparent bg-primary \
                                                     text-primary-foreground",
                                                )
                                            } else {
                                                format!(
                                                    "{base} border-border text-muted-foreground \
                                                     hover:text-foreground",
                                                )
                                            }
                                        }
                                        on:click=move |_| {
                                            set_level.set(value);
                                            load(value);
                                        }
                                    >
                                        {label}
                                    </button>
                                }
                            })
                            .collect_view()}
                        <Button on_click=Callback::new(move |()| load(level.get()))>
                            "refresh"
                        </Button>
                    </div>
                </div>
            </CardHeader>
            <CardContent>
                <Show
                    when=move || !records.get().is_empty()
                    fallback=|| {
                        view! {
                            <p class="text-sm text-muted-foreground">
                                "No log events retained at this level."
                            </p>
                        }
                    }
                >
                    <ScrollArea height="h-[32rem]">
                        <ul class="divide-y divide-border">
                            {move || {
                                records
                                    .get()
                                    .into_iter()
                                    .map(|record| {
                                        let tone = match record.level.as_ref() {
                                            "ERROR" => BadgeTone::Destructive,
                                            "WARN" => BadgeTone::Primary,
                                            _ => BadgeTone::Outline,
                                        };
                                        let fields = record
                                            .fields
                                            .iter()
                                            .map(|(key, value)| format!("{key}={value}"))
                                            .collect::<Vec<_>>()
                                            .join(" ");
                                        view! {
                                            <li class="flex items-baseline gap-2 py-1.5">
                                                <Badge tone=tone>{record.level.to_string()}</Badge>
                                                <span class="font-mono text-xs text-muted-foreground">
                                                    {record.target.to_string()}
                                                </span>
                                                <span class="text-sm">
                                                    {record.message.clone()}
                                                </span>
                                                <Show when={
                                                    let has_fields = !fields.is_empty();
                                                    move || has_fields
                                                }>
                                                    <span class="font-mono text-xs text-muted-foreground">
                                                        {fields.clone()}
                                                    </span>
                                                </Show>
                                                <Show when={
                                                    let trace_id = record.trace_id;
                                                    move || trace_id.is_some()
                                                }>
                                                    <span class="ml-auto font-mono text-xs text-muted-foreground">
                                                        {format!(
                                                            "trace {}",
                                                            record.trace_id.unwrap_or_default(),
                                                        )}
                                                    </span>
                                                </Show>
                                            </li>
                                        }
                                    })
                                    .collect_view()
                            }}
                        </ul>
                    </ScrollArea>
                </Show>
            </CardContent>
        </Card>
    }
}
