+++
title = "Observability"
description = "Four HTTP probes, in-process ring buffers instead of a collector, a read-only JSON API, and what readiness and shutdown actually mean."
template = "page.html"
weight = 2
+++

# Observability

`pcs-service` answers four probes on its control plane and keeps its own recent
telemetry in memory. Nothing has to be installed alongside it: no collector, no
scraper, no storage.

## The HTTP control plane

Four probes on one axum router, bound to `http.bind`, behind a 10-second request
timeout. The router and the watchdog are their own tokio tasks, so a batch in
flight never stops them answering.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 350" role="img" aria-labelledby="svc-h-title svc-h-desc">
        <title id="svc-h-title">The control plane, the shared state, and the run loop</title>
        <desc id="svc-h-desc">
            Three tokio tasks share one ServiceState. The axum router serves health,
            ready, metrics and status. The run loop drains sources, calls the runtime,
            drains sinks, then publishes a statistics snapshot into the shared state. A
            watchdog task increments a liveness counter once a second, which is what the
            health endpoint reads. Because the router and the run loop are separate tasks,
            all four endpoints answer while a batch is in flight.
        </desc>
        <text class="t-title" x="0" y="14">One process, three tasks</text>
        <text class="t-sm" x="0" y="30">the run loop never touches a socket</text>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="44" width="200" height="160" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="44" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="58" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="59">axum Router</text>
            <rect class="row" x="8" y="74" width="184" height="20" rx="3"/>
            <text class="t-sm" x="16" y="88">GET /health   200 | 503</text>
            <rect class="row" x="8" y="98" width="184" height="20" rx="3"/>
            <text class="t-sm" x="16" y="112">GET /ready    200 | 503</text>
            <rect class="row" x="8" y="122" width="184" height="20" rx="3"/>
            <text class="t-sm" x="16" y="136">GET /metrics  text 0.0.4</text>
            <rect class="row" x="8" y="146" width="184" height="20" rx="3"/>
            <text class="t-sm" x="16" y="160">GET /status   JSON</text>
            <text class="t-sm" x="16" y="184">10 s timeout, then 408</text>
        </g>
        <path class="arw arw-ctl" d="M232 124 H203" marker-end="url(#svc-hc)"/>
        <g class="anim anim-2">
            <rect class="blk blk-ctl" x="232" y="44" width="200" height="160" rx="8"/>
            <rect class="hd hd-ctl" x="232" y="44" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="232" y="58" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="244" y="59">ServiceState</text>
            <text class="t-sm" x="244" y="92">everything an endpoint</text>
            <text class="t-sm" x="244" y="112">answers from</text>
            <text class="t-sm t-data" x="244" y="146">the newest stats snapshot</text>
            <text class="t-sm" x="244" y="184">cloned per request</text>
        </g>
        <path class="arw arw-data" d="M452 124 H435" marker-end="url(#svc-hd)"/>
        <g class="anim anim-3">
            <rect class="blk blk-data" x="452" y="44" width="202" height="160" rx="8"/>
            <rect class="hd hd-data" x="452" y="44" width="202" height="22" rx="8"/>
            <rect class="hd hd-data" x="452" y="58" width="202" height="8"/>
            <text class="t-lbl t-data" x="464" y="59">run loop</text>
            <rect class="row" x="460" y="74" width="186" height="20" rx="3"/>
            <text class="t-sm" x="468" y="88">1  drain sources</text>
            <rect class="row" x="460" y="98" width="186" height="20" rx="3"/>
            <text class="t-sm" x="468" y="112">2  runtime.run_on(data)</text>
            <rect class="row" x="460" y="122" width="186" height="20" rx="3"/>
            <text class="t-sm" x="468" y="136">3  drain sinks</text>
            <rect class="row-w" x="460" y="146" width="186" height="20" rx="3"/>
            <text class="t-sm" x="468" y="160">4  publish stats, clear</text>
            <text class="t-sm" x="468" y="184">5  pace by run_mode</text>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-ctl" x="232" y="228" width="200" height="48" rx="8"/>
            <rect class="hd hd-ctl" x="232" y="228" width="200" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="232" y="242" width="200" height="8"/>
            <text class="t-lbl t-ctl" x="244" y="243">watchdog task</text>
            <text class="t-sm" x="244" y="266">liveness += 1 per second</text>
            <path class="arw arw-ctl" d="M332 226 V208" marker-end="url(#svc-hc)"/>
            <text class="t-sm" x="0" y="242">/status reads the snapshot</text>
            <text class="t-sm" x="0" y="256">published at step 4, so it</text>
            <text class="t-sm t-data" x="0" y="270">lags by one batch at most</text>
            <text class="t-sm" x="452" y="242">/health compares it with</text>
            <text class="t-sm" x="452" y="256">uptime and returns 503</text>
            <text class="t-sm t-ctl" x="452" y="270">after 5 s of silence</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 296 H654"/>
            <text class="t-sm" x="0" y="320">serve awaits the runner inline, and races it against the shutdown signal.</text>
        </g>
        <defs>
            <marker id="svc-hc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control)"/>
            </marker>
            <marker id="svc-hd" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the data plane and what it publishes</span>
        <span class="k-control"><i></i> the control plane</span>
    </div>
    <figcaption class="dgm-cap">
        Every endpoint answers from shared state, never by asking the runner anything. They
        stay responsive under load, and <b>none of them can tell you how the current batch
        is going</b>. <code>/status</code> only moves when an iteration finishes.
    </figcaption>
</div>

| Endpoint | Body | Status |
|---|---|---|
| `GET /health` | `{ status, uptime_seconds, liveness_counter }` | 200 while the watchdog counter is within 5 s of uptime; 503 once it falls behind. |
| `GET /ready` | `{ status }`, either `ready` or `not_ready` | 200 or 503, from one `AtomicBool`. |
| `GET /metrics` | Prometheus exposition, version 0.0.4 | Always 200. Fed by the OpenTelemetry Prometheus exporter. |
| `GET /status` | `node_id`, `node_name`, `mode`, `uptime_seconds`, `build.version`, plus `standalone` or `cluster` | Always 200. The block that does not match the mode is `null`. |

In standalone mode the `standalone` block carries `iterations`,
`rows_processed`, `source_batches_drained`, `sink_batches_written` and
`iteration_errors`. No `ClusterProbe` is wired into `serve`, so `"cluster"`
reports `null` even when the node is clustered; the Raft gauges are on
`/metrics`. What a healthy process answers:

```bash,name=Probing the three endpoints
curl -s http://localhost:8080/ready
{"status":"ready"}

curl -s http://localhost:8080/status | jq '.standalone.iteration_errors'
0

curl -s http://localhost:8080/metrics | grep '^pcs_rows_processed_total'
```

`http disabled=#true` turns the whole control plane off, the JSON API and the
dashboard with it. The nineteen Prometheus series and their writers are in
[Tracing](@/tracing.md).

## What `/ready` means

`/ready` flips once `ServiceBuilder::build` has returned and the HTTP task is
spawned, so a process answering 200 has a loaded component matching every link
and a listening control plane. It does not wait for the first successful
iteration: read `iterations` on `/status` for that.

## Graceful shutdown

`ShutdownCoordinator::wait_for_signal` owns the root `CancellationToken`. It
handles Ctrl-C always and `SIGTERM` on unix, and the HTTP server is wired
through `with_graceful_shutdown`, so an in-flight request finishes.

`serve` races the runner against that signal. On shutdown it drains the HTTP and
watchdog tasks within a 30-second budget, flushes spans, and logs
`pcs-service stopped cleanly` as its last line. A process still alive at the end
of the budget exits 1. That last line is the check: a stop without it was not a
drain.

On Windows there is no true `SIGTERM` equivalent, so only Ctrl-C drains. A
service manager that stops the process any other way gets none.

## In-process capture

Spans, log events and metric samples land in three time-bounded ring buffers,
and the control plane reads them back as JSON.

| Buffer | Default cap | Filled by |
|---|---|---|
| spans | 10 000 | the capture layer, on span close |
| log events | 10 000 | the capture layer, on event |
| metric samples | 3 600 | the in-memory metric exporter, once per interval |

Each buffer is bounded twice: by a retention window and by a hard entry count.
Time evictions run first on every push, then capacity ones. A capacity eviction
is counted, and the total appears as `buffers.dropped` in `/api/snapshot`, so a
buffer sized too small says so rather than quietly forgetting. A `log_level` or
`RUST_LOG` that suppresses `pcs_service` spans empties the span buffer, and the
dashboard's traces tab with it.

A processor reaches `tracing` through the WIT `host-io::log` import, so field
content is untrusted input. The visitor truncates any value at 512 bytes on a
UTF-8 boundary, caps a record at 32 fields, and appends
`("truncated", "true")` when either bound bites.

## Span levels

Nine span names, split by level. The five host spans are the per-item runner
tree: one whole tree opens per item.

| Span | Level | Opened by |
|---|---|---|
| `workflow.batch` | `debug` | the runners, per item or claim |
| `source.drain` | `debug` | the standalone runner, per source |
| `runtime.run` | `debug` | the runners, per processor |
| `sink.write` | `debug` | the runners, per sink |
| `processor.batch` | `debug` | the WASM and plugin hosts, per `run-batch` |
| `pipeline.run` | `info` | `pcs-core`, per native pipeline run |
| `pipeline.stage` | `info` | `pcs-core`, per stage |
| `system.execute` | `info` | `pcs-core`, per system |
| `task_attempt` | `info`, retries only | `pcs-core`'s two retry drivers |

The default `log_level="info"` builds the filter `pcs=info`, so none of the five
`debug` spans is materialised. The Traces tab then shows traces rooted at
`pipeline.run` rather than `workflow.batch`, and carries no per-item runner spans
at all. A workflow of only WASM components or native plugins contributes nothing
at `info`, because a guest's own spans never reach the host: its Traces tab is
empty. `log_level="debug"` is the escape hatch, and restores the full per-item
waterfall from `workflow.batch` down to `processor.batch`.

`task_attempt` is the one `info` span usually absent as well. A first attempt
that succeeds opens no span, so a clean run shows zero of them.

That split is a cost decision. On the reference machine, per-item stream latency
measured through the service's own subscriber is about 4.6 µs at the default
`info` and about 7.4 µs at `debug`; built without the `tracing` feature it is
about 1.9 µs. Reach for `debug` to read one workflow's waterfall, not to run on.

## Configuration

```kdl,name=The observability block
observability {
    inspector {
        enabled #true            // #false: no capture layer, no /api/*, no /ui
        ui #true                 // #false: keeps /api/*, drops /ui
        retention_secs 3600
        sample_interval_secs 1
        max_spans 10000
        max_logs 10000
        max_samples 3600
    }
}
```

Those are the defaults: capture is on, and the whole node may be omitted or
given one key. `enabled #false` installs no layer, attaches no metric reader and
merges no routes, so the endpoints below answer **404** rather than 403. Confirm
which way a running process is set:

```bash,name=Check whether capture is on
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/api/snapshot
200
```

## The JSON API

| Route | Body |
|---|---|
| `GET /api/topology` | the node, its mode, and the workflow graph |
| `GET /api/snapshot?window_secs=60` | one document with series, edge rates, span statistics and buffer occupancy |
| `GET /api/traces?limit=100` | newest-first trace summaries |
| `GET /api/traces/{trace_id}` | the spans and log lines of one trace, 404 once it ages out |
| `GET /api/logs?limit=200&level=warn` | newest-first log records, filtered at or above `level` |

`window_secs` is capped at 24 hours and `limit` at 1000. Every endpoint reads the
buffers and returns: none mutate anything, and none block a pipeline. `trace_id`
and `span_id` are process-local, so they do not correlate with an OTLP
collector's ids.

`/api/snapshot` is the one document the dashboard polls:

```json,name=The snapshot the dashboard polls
{
  "topology_version": 1,
  "sampled_at_unix_ms": 1750000000000,
  "uptime_secs": 412,
  "ready": true,
  "series": [
    { "name": "pcs_rows_processed_total", "kind": "counter", "attrs": [],
      "value": 148213.0, "count": 0, "rate_per_sec": 12034.5,
      "points": [{ "t": 1750000000000, "v": 148213.0 }] },
    { "name": "pcs_rows_processed_total", "kind": "counter",
      "attrs": [["source", "orders-in"]],
      "value": 148213.0, "count": 0, "rate_per_sec": 12034.5,
      "points": [{ "t": 1750000000000, "v": 148213.0 }] },
    { "name": "pcs_processor_rows_out_total", "kind": "counter",
      "attrs": [["processor", "validate"]],
      "value": 148213.0, "count": 0, "rate_per_sec": 12034.5,
      "points": [{ "t": 1750000000000, "v": 148213.0 }] }
  ],
  "edges": [
    { "from": "orders-in", "to": "validate", "rate_per_sec": 12034.5,
      "unit": "rows" },
    { "from": "validate", "to": "settle", "rate_per_sec": 12034.5,
      "unit": "rows" }
  ],
  "span_stats": [],
  "buffers": { "spans": 812, "logs": 4110, "samples": 3600, "dropped": 0 }
}
```

`rate_per_sec` is the first derivative across the two newest samples for a
counter or a histogram, and the raw value for a gauge. A counter that went
backwards reports 0. `points` is thinned to at most 120 entries.

Nine series carry a node id: `pcs_rows_processed_total` and
`pcs_source_batches_drained_total` under `source`,
`pcs_sink_batches_written_total` under `sink`, and the six `pcs_processor_*`
series under `processor`. Each appears twice, once with `attrs: []` for the
process-wide total and once under the id of the node that wrote it, so adding
the two counts every row twice. Attributes are sorted by key, which makes the
branch-attributed `pcs_processor_rows_out_total` read
`[["branch", "<name>"], ["processor", "<id>"]]`.

`span_stats` carries p50, p95 and max per stage and per system, derived from the
retained spans because `pcs_stage_duration_seconds` carries no attributes. It is
empty for a workflow of only WASM components and native plugins: their
`pipeline.stage` and `system.execute` spans never reach the host.

Fetch one and read it yourself:

```bash,name=Read the snapshot by hand
curl -s 'http://localhost:8080/api/snapshot?window_secs=60' \
  | jq '{buffers, edges: [.edges[] | {from, to, rate_per_sec}]}'
```

## Scope

The control plane has no authentication, no TLS and no rate limiting. Bind it to
a loopback address or an internal network, and put a reverse proxy in front if it
has to be reachable further.

**Next:** [Live dashboard](@/service/dashboard.md), the reader this JSON API was
written for.
