+++
title = "Live dashboard"
description = "The /ui dashboard: an animated topology, a trace waterfall, a log tail, and how to switch it off or rebuild it."
template = "page.html"
weight = 3
+++

# Live dashboard

`pcs-service` serves a dashboard at `/ui`, on the same port as the rest of the
control plane. It reads the in-process buffers through
[the JSON API](@/service/observability.md#the-json-api) and needs nothing else
running.

Open it while a workflow runs:

```bash,name=Open the dashboard
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/ui
200

# then in a browser
open http://localhost:8080/ui
```

## What it is

A client-side-rendered Leptos application compiled to
`wasm32-unknown-unknown`. Four files are embedded in the binary and served from
`/ui`, `/ui/app.js`, `/ui/app_bg.wasm` and `/ui/app.css`, each with
`Cache-Control: no-cache`. Styling is a hand port of the shadcn/ui
`new-york-v4` recipes through Tailwind v4, so there is no node and no
`package.json` in the repository.

## The three tabs

A fixed left rail carries node id, mode, uptime, a ready badge, the workflow
count, the workflow run counter, an error badge and the three buffer counters.
Beside it, three tabs.

**Pipelines** draws one card per declared workflow, each card its own animated
SVG: one box per declared node, in depth columns from the entry nodes
rightwards, laid out independently of the other workflows. A box shows its
title, its connector type or runtime kind, its component, its live number and
a sparkline. Clicking it opens the full detail list, and for a processor the
values it reported through `pcs_processor_metric` and its last ten log lines.

<img src="../../dashboard/pipelines.png" alt="The Pipelines tab draws the multi-workflow example as one card per workflow, with a channel-bridges card below them.">

A workflow card's header carries its own run and error badges, read under
`workflow="<id>"`. A `channel bridges` card below the workflows lists every
in-process channel joining a `ChannelSink` to a `ChannelSource` across
workflows, with its live rate.

**Traces** lists the retained traces with name, start, duration, span count and
an error badge. One trace is one iteration. Selecting one renders an SVG
waterfall, one bar per span, x as the offset from trace start.

**Logs** tails the newest records with a level filter, and a row links to its
trace.

<img src="../../dashboard/logs.png" alt="The Logs tab tails the newest records with a level filter.">

One box per declared node, however many systems a processor runs: nothing
host-side can see inside a processor. A box is titled by the node's declared
`name`, or by its id when the config named none, and a labelled link draws its
branch name on a chip at the edge's midpoint. A missing box means a missing
node, so run `pcs-service validate` and compare its `workflow` line.

## Colour and motion

Source and sink boxes use `--dgm-hd-data`, the data plane. A processor box uses
`--dgm-hd-bnd`, the host to WebAssembly boundary. Control-plane facts use
`--dgm-hd-ctl`.

Edge dashes animate with a period of `clamp(0.25s, 8s / max(rate, 1), 8s)` and a
stroke width scaling with `log10(rate)`, so they speed up with throughput
without going solid. A zero rate is a static dim stroke, and motion stops under
`@media (prefers-reduced-motion: reduce)`.

## What the edge numbers mean

An edge is rated from its upstream node's own attributed series.

| Edge out of | Reads | Unit |
|---|---|---|
| a source | `pcs_rows_processed_total` under `source="<id>"` | rows |
| a processor | `pcs_processor_rows_out_total` under `processor="<id>"` | rows |
| a processor, on a labelled link | the same series under `processor="<id>", branch="<name>"` | rows |
| a processor with no rows-out sample | `pcs_sink_batches_written_total` under `sink="<id>"` | batches |

The last row is the native-runtime case, which records none of the six
`pcs_processor_*` series. Batches are never relabelled as rows. An edge with no
sample at either end is omitted, and an unchosen branch shows no number until it
carries traffic. Every node writes its own attributed copy, so two sources
feeding one processor rate their two edges separately.

## What the node numbers mean

Each box reads the copy of its series carrying its own id.

| Box | Shows | Traces |
|---|---|---|
| source | records per second from `pcs_rows_processed_total` | the same series |
| wasm or plugin processor | mean batch latency from `pcs_processor_batch_duration_seconds`, plus a retry badge from `pcs_processor_retries_total` | `pcs_processor_rows_in_total` |
| native processor | `pcs_stage_duration_seconds`, which carries no attributes, so every such box shows the same process-wide mean | `pcs_workflow_runs_total` under `workflow="<id>"`, its own workflow |
| sink | records per second from the series of the node feeding it | that same upstream series |

A sink is the one box that reads another node's series, because its own series
counts batches and a batch says nothing about records written. When no upstream
rows series exists, which is the native-runtime case, it falls back to
`pcs_sink_batches_written_total` under its own id and says `batches`. No box
shows the unattributed copy of a series: that is the process-wide sum a
`/metrics` consumer reads.

## What windowing looks like

A processor node whose config declares a `window` block receives the merged rows
of **every** inbound link. The dashboard marks it three ways.

- A teal chip in the box's top-right corner: `⟐30s` tumbling, `⟐30s/5s` sliding,
`⟐gap5s` session.
- A **windowing** section in the detail sheet: kind, geometry, time field,
grouping keys, allowed-lateness budget.
- The node's live **watermark** in that section, from
`pcs_window_watermark_seconds` under `processor="<id>"`, as UTC wall-clock time.
It is blank until the first timestamp arrives, so a blank one on a busy node
means the source is not producing.

<img src="../../dashboard/windowing.png" alt="The windowed processor box carries the 30-second window chip, and its detail sheet lists the window geometry and the live watermark.">

[Windowing](@/service/windowing.md) has the runnable example that fills one.

## Node detail is allowlisted

A connector's `config` table holds `connection.dsn`, passwords and credential
file paths, so the topology copies values through a per-`type` allowlist. A key
outside it is dropped, never masked. A `type` the allowlist does not name gets no
detail at all.

| `type` | Keys shown |
|---|---|
| `NatsSource`, `NatsSink` | `mode.kind`, `mode.stream`, `mode.subject` |
| `PostgresSource` | `mode.kind`, `mode.table` |
| `PostgresSink` | `table`, `write_mode` |
| `FileSource`, `FileSink` | `path` |
| `KafkaSource`, `KafkaSink` | `topic` |
| `tcp` | `bind` |
| `ChannelSource`, `ChannelSink` | `name` |

<img src="../../dashboard/detail.png" alt="A source node's detail sheet lists its allowlisted connector options, its component and its node facts.">

## Polling

The page fetches `/api/snapshot` once a second and skips the tick while the
document is hidden. The topology is fetched once: it does not change while the
process runs. No websocket, so the axum side stays stateless.

## What a wasm trace shows

One `workflow.batch` root span per iteration, holding one `source.drain` per
source, one `runtime.run` per processor and one `sink.write` per sink, in
topological order. A `runtime.run` over a WASM processor holds one
`processor.batch` carrying that processor's rows in and out, its systems run, its
retries and the guest's own wall time.

A guest's `pipeline.stage` and `system.execute` spans never reach the host, so a
WASM processor is one bar rather than a subtree. A native runtime nests those
three levels under `runtime.run`.

<img src="../../dashboard/traces.png" alt="The Traces tab lists the retained traces, and the selected one renders as a waterfall of its spans.">

## Switching it off

```kdl,name=The two inspector keys
observability {
    inspector enabled=#true ui=#true
}
```

`ui=#false` drops `/ui` and its three assets while keeping the JSON API, for a
service scraped by something else. `enabled=#false` drops both. Either way the
dropped routes answer 404, which is what a `curl` against `/ui` tells you.

## Rebuilding the bundle

`crates/pcs-service/assets/ui/{index.html,app.js,app_bg.wasm,app.css}` is
committed and embedded, so `cargo build -p pcs-service` never needs the wasm
toolchain. `index.html` is hand-written; the other three are generated.

```bash,name=Rebuild the UI bundle
cargo xtask ui
```

The task formats and lints the UI crate, which `cargo fmt --all` does not reach,
then builds for `wasm32-unknown-unknown`, runs `wasm-bindgen --target web`, and
runs Tailwind. It names either missing prerequisite in its exit code:

```bash,name=The two prerequisites the task names
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version <the version the lock file resolves> --locked
```

The `wasm-bindgen` CLI has to match the crate version exactly, so the task reads
it out of `crates/pcs-service-ui/Cargo.lock` and refuses a different CLI.
Tailwind needs no install: the task downloads the standalone binary into a
gitignored directory beside the crate. Commit
`crates/pcs-service/assets/ui/` afterwards, or `pcs-service` keeps serving the
previous bundle.

**Next:** [Branching](@/service/branching.md), the labelled fan-out those edge
chips are drawing.
