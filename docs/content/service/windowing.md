+++
title = "Windowing"
description = "Event-time windows in service mode: the host validates the geometry and tracks the watermark, the processor keeps the open windows and emits the closed ones."
template = "page.html"
weight = 5
+++

# Windowing

<dl class="page-facts">
<dt>What it is</dt>
<dd>An aggregate over a <strong>bounded slice of event time</strong>, cut from an unbounded stream</dd>
<dt>Reach for it when</dt>
<dd>You need per key totals, and the rows carry <strong>their own timestamp</strong></dd>
<dt>Split of work</dt>
<dd>The host tracks the watermark; the <strong>processor</strong> holds the windows and aggregates</dd>
</dl>

A stream has no end, so a total over one is never finished. A window gives it a
moment: rows whose event time falls in `[start, end)` share a window. Event time
is the timestamp in the row, not the moment the service read it, so a re-run of
the same input produces the same totals.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 246" role="img" aria-labelledby="wc-title wc-desc">
        <title id="wc-title">The watermark passing a window's end is what closes it</title>
        <desc id="wc-desc">
            An event-time axis runs from zero to ninety seconds, divided into three tumbling
            thirty-second windows. Rows are scattered across the first two windows and the
            start of the third. The watermark, the highest event timestamp seen so far,
            stands at seventy-four seconds. Windows zero and one end before it, so both are
            closed and the processor has emitted one aggregate row per key for each of them.
            Window two ends at ninety seconds, past the watermark, so it stays open and
            emits nothing yet. The watermark only advances when rows arrive, so a stream
            that stops leaves its last window open.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="42" y="52" width="196" height="96" rx="8"/>
            <rect class="hd hd-data" x="42" y="52" width="196" height="20" rx="8"/>
            <rect class="hd hd-data" x="42" y="64" width="196" height="8"/>
            <text class="t-lbl" x="54" y="67">window 0</text>
            <rect class="blk blk-data" x="242" y="52" width="196" height="96" rx="8"/>
            <rect class="hd hd-data" x="242" y="52" width="196" height="20" rx="8"/>
            <rect class="hd hd-data" x="242" y="64" width="196" height="8"/>
            <text class="t-lbl" x="254" y="67">window 1</text>
            <rect class="blk" x="442" y="52" width="196" height="96" rx="8"/>
            <rect class="hd" x="442" y="52" width="196" height="20" rx="8"/>
            <rect class="hd" x="442" y="64" width="196" height="8"/>
            <text class="t-lbl" x="454" y="67">window 2</text>
            <path class="ax" d="M42 148 H638"/>
            <text class="t-ax t-mid" x="42" y="164">0s</text>
            <text class="t-ax t-mid" x="240" y="164">30s</text>
            <text class="t-ax t-mid" x="440" y="164">60s</text>
            <text class="t-ax t-mid" x="638" y="164">90s</text>
        </g>
        <g class="anim anim-2">
            <rect class="bar-data" x="62" y="126" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="96" y="112" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="134" y="130" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="178" y="104" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="210" y="124" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="258" y="118" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="296" y="132" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="340" y="106" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="392" y="128" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="462" y="120" width="7" height="7" rx="1"/>
            <rect class="bar-data" x="504" y="134" width="7" height="7" rx="1"/>
        </g>
        <g class="anim anim-3">
            <path class="mark" d="M532 36 V152"/>
            <text class="t-sm t-ctl t-end" x="524" y="44">watermark 74s</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M140 150 V184" marker-end="url(#wc-d)"/>
            <path class="arw arw-data" d="M340 150 V184" marker-end="url(#wc-d)"/>
            <rect class="blk blk-data" x="42" y="186" width="396" height="48" rx="8"/>
            <rect class="hd hd-data" x="42" y="186" width="396" height="20" rx="8"/>
            <rect class="hd hd-data" x="42" y="198" width="396" height="8"/>
            <text class="t-lbl" x="54" y="201">closed: one aggregate row per window and key</text>
            <text class="t-sm" x="54" y="224">both ends are behind the watermark</text>
            <text class="t-sm" x="454" y="201">window 2 stays open:</text>
            <text class="t-sm" x="454" y="217">its end is ahead of the</text>
            <text class="t-sm" x="454" y="233">watermark, so it emits nothing</text>
        </g>
        <defs>
            <marker id="wc-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane: rows, and the aggregates they close into</span>
        <span class="k-control"><i></i> the watermark, tracked by the host</span>
        <span class="k-mute"><i></i> a window still open</span>
    </div>
    <figcaption class="dgm-cap">
        The watermark is the highest event timestamp seen so far, so it moves only when
        rows arrive. A stream that stops leaves its last window open.
    </figcaption>
</div>

## The config

A `window` block on a `wasm` or `plugin` node declares the geometry.

```kdl,name=A window block declares the geometry
wasm "windowed" module="pipelines/windowed.wasm" {
    window kind="tumbling" size_ms=30000 offset_ms=0 time_field="timestamp_ms" allowed_lateness_ms=5000 {
        key_field "symbol"
    }
}
```

Three kinds, each with its own geometry keys:

| `kind` | Keys | The slice it cuts |
|---|---|---|
| `tumbling` | `size_ms`, `offset_ms` | contiguous and non-overlapping, so a row belongs to exactly one |
| `sliding` | `size_ms`, `slide_ms`, `offset_ms` | one window starting every `slide_ms`, so a row belongs to several |
| `session` | `gap_ms` | one window per key, ended by `gap_ms` of event-time silence on that key |

`time_field` names the event-time column, in `Int64` milliseconds or an Arrow
timestamp type, and every component delivered to the node must carry it or the
workflow is refused at load time. `key_field` is the grouping key list: one child
per key, or several arguments on one child. Omit it for a global aggregate.

The host injects the block into the node's config as `window.kind`,
`window.size_ms`, `window.slide_ms`, `window.gap_ms`, `window.offset_ms`,
`window.time_field`, `window.key_fields` (comma separated) and
`window.allowed_lateness_ms`. A key set by hand wins over the injected value.

The block needs the `windows` feature, which the default bundle carries. Cluster
mode rejects it: a cluster node has no inbound links to draw a watermark from.

```bash,name=Validate a window block
pcs-service validate --config service.kdl

OK: workflow graph validated (components and schemas agree end to end)
```

[The dashboard](@/service/dashboard.md#what-windowing-looks-like) shows what a
windowed node looks like live.

## The watermark

The watermark is the host's estimate of stream completeness: the maximum
`time_field` value across the rows delivered to the node, monotonic over the
whole run. Every processor call advances it first, and the host publishes it as
`pcs_window_watermark_seconds` under `processor="<id>"`.

- **A window closes late, not on a clock.** No rows means no watermark
  movement, so an idle stream holds its last window open indefinitely.
- **A row behind the watermark is late.** `allowed_lateness_ms` is how far
  behind a row is still counted. Past that budget its window has already been
  emitted, and dropping it is the processor's call.

Read the current value off the metric:

```bash,name=Read a node watermark
curl -s http://localhost:8080/metrics | grep '^pcs_window_watermark_seconds'
```

## Who does what

The host never aggregates. It owns the declaration and the clock; the processor
owns the data.

| The host | The processor |
|---|---|
| validates the geometry and refuses an unsound one at load time | assigns each row to its window from the injected geometry |
| checks that every component delivered to the node carries `time_field` | keeps open windows in its checkpoint state, so they survive a batch |
| advances and reports the node's watermark | decides when a window is final and emits its rows |
| injects the geometry into the node's `config` as `window.*` keys | reads those keys back through `get-config` |

One KDL block therefore drives the aggregation, the watermark series and the
dashboard's window chip at once.

## Several streams into one window

A processor node accepts any number of inbound links, and the runner merges their
rows into one dataset before the call, so the processor never learns which link a
row came from. `run_mode kind="stream"` pulls live sources round-robin, one batch
per item, so the cross-source merge happens where the open windows already live:
in the processor's state, across calls.

## Native pipelines

A native `Pipeline` windows without any of this. `pcs-core`'s `windows` feature
carries `WindowSpec`, `WindowedSystemBuilder`, watermark tracking and the
`WindowAccumulator` component that holds open windows, and the distributed runner
persists that accumulator through its `CheckpointStore`. That is the route for
cluster mode.

An in-process runtime registered with `ServiceBuilder::with_runtime` also reads
the live watermark from the `WindowWatermark` resource on the batch dataset. A
component and a plugin cannot: resources do not cross the Arrow IPC boundary.
Hence config keys for the geometry and a metric for the watermark.

## The runnable example

`examples/windowing/windowing.kdl` runs the whole shape in one workflow. Two core
NATS subjects fan into two processors carrying the same logic, one a component and
one a plugin, so their two tables should agree row for row. It needs a clone of
the repository, Docker, and both processors built.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 270" role="img" aria-labelledby="win-title win-desc">
        <title id="win-title">Two subjects fan into two windowed processors, each writing its own PostgreSQL table</title>
        <desc id="win-desc">
            The sources sales_a and sales_b both link to both processors. window_wasm is a
            WebAssembly component and window_plugin a native plugin, and each declares the
            same window block: tumbling, thirty seconds, keyed by symbol. window_wasm writes
            closed-window rows to the table wasm_window_totals and window_plugin writes the
            same rows to plugin_window_totals.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="24" width="140" height="48" rx="8"/>
            <rect class="hd hd-data" x="0" y="24" width="140" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="140" height="8"/>
            <text class="t-lbl" x="12" y="39">sales_a</text>
            <text class="t-sm" x="12" y="60">windowing.sales.a</text>
            <rect class="blk blk-data" x="0" y="134" width="140" height="48" rx="8"/>
            <rect class="hd hd-data" x="0" y="134" width="140" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="146" width="140" height="8"/>
            <text class="t-lbl" x="12" y="149">sales_b</text>
            <text class="t-sm" x="12" y="170">windowing.sales.b</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M140 48 H260" marker-end="url(#win-d)"/>
            <path class="arw arw-data" d="M140 48 C200 48 200 158 260 158" marker-end="url(#win-d)"/>
            <path class="arw arw-data" d="M140 158 C200 158 200 48 260 48" marker-end="url(#win-d)"/>
            <path class="arw arw-data" d="M140 158 H260" marker-end="url(#win-d)"/>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="260" y="14" width="175" height="68" rx="8"/>
            <rect class="hd hd-bnd" x="260" y="14" width="175" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="260" y="26" width="175" height="8"/>
            <text class="t-lbl t-bnd" x="272" y="29">window_wasm</text>
            <text class="t-sm" x="272" y="52">wasm component</text>
            <text class="t-sm t-ctl" x="272" y="70">tumbling 30s, key symbol</text>
            <rect class="blk blk-bnd" x="260" y="124" width="175" height="68" rx="8"/>
            <rect class="hd hd-bnd" x="260" y="124" width="175" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="260" y="136" width="175" height="8"/>
            <text class="t-lbl t-bnd" x="272" y="139">window_plugin</text>
            <text class="t-sm" x="272" y="162">native plugin</text>
            <text class="t-sm t-ctl" x="272" y="180">tumbling 30s, key symbol</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M435 48 H480" marker-end="url(#win-d)"/>
            <path class="arw arw-data" d="M435 158 H480" marker-end="url(#win-d)"/>
            <rect class="blk blk-data" x="480" y="24" width="170" height="48" rx="8"/>
            <rect class="hd hd-data" x="480" y="24" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="480" y="36" width="170" height="8"/>
            <text class="t-lbl" x="492" y="39">wasm_totals</text>
            <text class="t-sm" x="492" y="60">wasm_window_totals</text>
            <rect class="blk blk-data" x="480" y="134" width="170" height="48" rx="8"/>
            <rect class="hd hd-data" x="480" y="134" width="170" height="20" rx="8"/>
            <rect class="hd hd-data" x="480" y="146" width="170" height="8"/>
            <text class="t-lbl" x="492" y="149">plugin_totals</text>
            <text class="t-sm" x="492" y="170">plugin_window_totals</text>
            <path class="ln" d="M0 220 H654"/>
            <text class="t-sm" x="0" y="240">The runner pulls the two subjects round-robin, one batch per item, and both processors receive both.</text>
            <text class="t-sm" x="0" y="256">Each merges those batches in its own state, then emits one row per closed window and symbol.</text>
        </g>
        <defs>
            <marker id="win-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the two processor runtimes</span>
        <span class="k-control"><i></i> the window block, host knowledge</span>
    </div>
    <figcaption class="dgm-cap">
        Identical inputs, identical windowing, two runtimes. The fan-in is four
        <code>link</code> lines; the merge itself happens inside each processor's own
        state, not in the runner.
    </figcaption>
</div>

Both processors declare the same block and emit one `WindowTotal` row per closed
`(window_id, symbol)` group.

```kdl,name=The block both processors declare
window kind="tumbling" size_ms=30000 time_field="timestamp_ms" allowed_lateness_ms=5000 {
    key_field "symbol"
}
```

### Build the processors

From the repository root:

```bash,name=Build both processors
cargo build --release -p windowing-wasm --target wasm32-wasip2
cargo build -p windowing-plugin
```

The plugin artifact name is platform specific. The config defaults to the Linux
name, so only macOS and Windows need `PCS_PLUGIN_LIB`:

| Platform | Plugin artifact | `PCS_PLUGIN_LIB` |
|----------|-----------------|------------------|
| Linux | `target/debug/libwindowing_plugin.so` | not needed (config default) |
| macOS | `target/debug/libwindowing_plugin.dylib` | `target/debug/libwindowing_plugin.dylib` |
| Windows | `target/debug/windowing_plugin.dll` | `target/debug/windowing_plugin.dll` |

Both `window` blocks are checked at load time, so validate first, with
`PCS_PLUGIN_LIB` set on macOS and Windows:

```bash,name=Validate the windowing config
cargo run -p pcs-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- validate \
  --config examples/windowing/windowing.kdl --strict
```

### Run it

Start the containers, then the service, then the publisher.

```bash,name=Start the containers
docker compose -f examples/windowing/docker-compose.yml up -d
```

That brings up `nats:2.11-alpine` and `postgres:18-alpine` and runs `schema.sql`
on first initialisation, creating the two tables. `PostgresSink` never issues
`CREATE TABLE`, and PostgreSQL only runs init scripts against an empty data
directory, so a volume created before `schema.sql` existed fails with
`table ... does not exist`. Recreate it with `down -v` then `up -d`, or apply the
SQL by hand:

```bash,name=Apply the schema by hand
docker compose -f examples/windowing/docker-compose.yml exec -T postgres \
  psql -U postgres -d pcs < examples/windowing/schema.sql
```

```bash,name=Start the service
cargo run -p pcs-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- serve \
  --config examples/windowing/windowing.kdl
```

Then, in another terminal:

```bash,name=Run the publisher
cargo run -p pcs-service --example windowed_publish -- --rate 20 --ts-step-ms 2000
```

On macOS and Windows add `PCS_PLUGIN_LIB` to the serve command. This config
enables the inspector, so the dashboard is on <http://127.0.0.1:8080/ui> with a
window chip and a live watermark on both processor boxes.

The publisher's flags: `--count` (0 runs until Ctrl-C, the default), `--rate`
messages per second (default 20), `--ts-step-ms` simulated milliseconds per
message (default 2000), `--url` (default `nats://localhost:4222`),
`--subject-a` / `--subject-b` (defaults `windowing.sales.a` /
`windowing.sales.b`), `--seed`.

### What each table holds

The publisher sends `timestamp_ms` (simulated event time, advancing
`--ts-step-ms` per message), `symbol` and `amount`. At the defaults the simulated
clock runs 40 seconds per wall second, so a 30-second window closes roughly
every 0.75 wall seconds.

| Table | Holds |
|-------|-------|
| `public.wasm_window_totals` | one row per closed (window, symbol) group from the wasm processor |
| `public.plugin_window_totals` | the same rows from the plugin processor |

Both carry `window_id` (the window index, so the window starts at
`window_id * 30000` ms), `symbol`, `count` and `sum`. The sinks upsert on
`(window_id, symbol)`, so a late re-fire updates a row instead of duplicating it.

```sql,name=Read the window totals
SELECT * FROM public.wasm_window_totals ORDER BY window_id, symbol;
```

Rows for every `window_id` except the newest is the correct result: a window
closes only once the watermark passes its end.

**Next:** [Operating pcs-service](@/operations/running-pcs.md), the same binary
under a supervisor.
