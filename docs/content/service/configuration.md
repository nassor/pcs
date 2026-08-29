+++
title = "Configuration"
description = "Every key in the service config, which features add which nodes, and the four load-time gates that refuse to start."
template = "page.html"
weight = 1
+++

# Configuration

One KDL document describes the whole process. `${VAR}` placeholders are
substituted from the environment before the parse. An unset `${VAR}` is an
error; `${VAR:-default}` falls back.

## The file structure

Seven top-level keys. `node` and `workflow` are required; the rest have defaults.

| Key | Holds | Default |
|---|---|---|
| `mode` | which runner: `"standalone"` or `"cluster"` | `"standalone"` |
| `node` | `id`, optional `name`, `data_dir` | required |
| `run_mode` | how a standalone run paces itself | `kind="continuous"` |
| `workflow` | the graph: transformers, sources, processors, sinks, links | required |
| `store` | the persistent store: a `store "tikv"` block | none, required in cluster mode |
| `http` | the control-plane address, or `disabled` | `bind="0.0.0.0:8080"` |
| `observability` | log format and level, OTLP export, the inspector | pretty at `info` |

```kdl,name=The top level of a service config
mode "standalone"

// id is a u64 and stable across restarts. data_dir must be non-empty; only the
// cluster runner writes there.
node id=1 name="pcs-1" data_dir="/var/lib/pcs/node-1"

// continuous | one_shot | interval | stream
run_mode kind="continuous"

// The leading argument is the workflow id. Every node lives inside this block.
workflow "orders" name="Orders" {
    // transformer, source, wasm, plugin, sink and link nodes
}

http bind="0.0.0.0:8080" disabled=#false

observability log_format="pretty" log_level="info" trace_sample_ratio=1.0
```

`workflow` may repeat. Standalone runs every declared block; cluster mode takes
exactly one. Fill one in next.

## The workflow structure

Six node kinds live inside `workflow`. The leading argument of each is its id,
unique across the whole workflow.

| Node | Declares | Links |
|---|---|---|
| `transformer` | `format`, an optional `options` child | none: a source or sink names it |
| `source` | `type`, `component`, optional `transformer`, optional `retry`, a `config` child | outbound only |
| `wasm` | optional `module`, optional `sha3_256`, `config` and `window` children | inbound and outbound |
| `plugin` | `library`, otherwise the same as `wasm` | inbound and outbound |
| `sink` | `type`, `component`, optional `transformer`, optional `retry`, a `config` child | inbound only |
| `link` | `from`, `to`, optional `branch` | it is the edge |

One source, one processor, one sink is the whole shape:

```kdl,name=A workflow with one source one processor and one sink
workflow "orders" name="Orders" {
    // A declared byte format. Each source and sink that moves bytes names one.
    transformer "orders_parquet" format="parquet"

    // type is the factory lookup key. component names the rows this node
    // produces, and the processor it feeds must declare that component.
    source "orders_in" type="FileSource" component="Order" transformer="orders_parquet" {
        // config is opaque: the factory reads it verbatim.
        config path="/data/in/orders.parquet"
    }

    // A wasm32-wasip2 component implementing pcs:pipeline/pcs-pipeline.
    wasm "enrich" name="Order enrichment" module="pipelines/orders.wasm" {
        // Optional integrity check, verified before compilation. A leading
        // "sha3-256:" prefix is accepted.
        // sha3_256="6f1c...c4"

        // Strings the processor pulls with the host-io get-config import. The
        // processor parses numerics itself.
        config fx_eur="1.08" batch_size="500"
    }

    sink "orders_out" type="FileSink" component="Order" transformer="orders_parquet" {
        // schema_fields is the output schema, where the format needs one.
        config path="/data/out/orders.parquet" {
            schema_fields "id" type="Int64" nullable=#false
            schema_fields "usd_amount" type="Float64" nullable=#false
        }
    }

    // Declaration order wires nothing. Only a link connects two nodes.
    link from="orders_in" to="enrich"
    link from="enrich" to="orders_out"
}
```

Six graph rules, all enforced before anything runs:

- Every id is unique across transformers, sources, processors and sinks.
- A `link` names two declared ids, and no `from`/`to` pair repeats.
- Nothing links into a source; nothing links out of a sink.
- The graph is acyclic.
- Every source has an outbound link, every sink an inbound one.
- Both ends of a link agree on their components, and on each component's Arrow
  fields field for field.

Prove the file before you deploy it:

```bash,name=Validate a config and what it prints
pcs-service validate --config service.kdl

OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
  node.id:  1
  node.name: pcs-1
  mode:     standalone
  workflow: orders
  processors: pipelines/orders.wasm
  sources:  1
  sinks:    1
  http.bind: 0.0.0.0:8080
  log_level: info
OK: all declared types resolved in built-in registry
```

A `type` no registered factory claims is a warning and still exits 0: a config
aimed at a custom binary names factories the stock binary does not know.
`--strict` makes those warnings fail. A `FileSink` opens its output file while
its factory runs, `validate` included, so the parent directory must exist
first. The file is created when it is missing and appended to when it is not;
`truncate #true` in the sink's `config` replaces it instead.

### Chaining processors

Two processors linked in sequence run against the same in-memory dataset, with
no re-encoding between them.

```kdl,name=Two processors in sequence
link from="orders_in" to="enrich"
link from="enrich" to="settle"
link from="settle" to="orders_out"
```

The upstream must declare every component the downstream declares. A
disagreement fails the build with an error naming the link, so `validate` catches
it.

### A plugin instead of a component

A `plugin` node loads a shared library with `dlopen`. The key is `library`
rather than `module`; a `link` treats both node kinds the same way.

```kdl,name=A plugin node loads a shared library
plugin "audit" library="/var/lib/pcs/plugins/liborders.so" {
    // sha3_256="sha3-256:6f1c...c4"
    config fx_eur="1.08"
}
```

`plugin` is not in the default bundle: build with `--features plugin` to make the
node bind, then follow [Native plugins](@/native/plugins.md).

### Branching and windowing

A `link` takes a `branch` name, and the upstream processor picks which branches
each batch reaches: [Branching](@/service/branching.md). A `wasm` or `plugin`
node takes a `window` block declaring event-time geometry, and the host tracks
that node's watermark: [Windowing](@/service/windowing.md).

### Retrying connector operations

Every `sink` retries a failed write with exponential backoff before the error
reaches the runner: 4 attempts, a 100 ms base delay, 2.0x growth, a 30 s cap and
0.1 jitter, the same policy as a pipeline system retry. A `source` takes the same
wrapper everywhere except `run_mode kind="stream"`, where the stream runner's own
re-poll already is the retry loop and owns the source's error policy. EOF from a
source is not an error and is not retried.

The asymmetry follows from what a lost call costs. A `write_batch` the runner
cannot re-drive is lost rows, so a sink is always wrapped. A stream source that
fails is polled again 10 ms later, so wrapping it would stack one backoff on
another.

An optional `retry` child on a `source` or `sink` overrides the policy for that
node. `max_attempts=1` disables retrying.

```kdl,name=A source that retries with a longer backoff
source "orders_in" type="NatsSource" component="Order" {
    retry max_attempts=8 base_delay_ms=500 multiplier=2.0 max_delay_ms=30000 jitter=0.1
    config subject="orders"
}
```

| Key | Default | Meaning |
|---|---|---|
| `max_attempts` | 4 | total attempts including the first; 1 disables retrying |
| `base_delay_ms` | 100 | delay before the second attempt |
| `multiplier` | 2.0 | growth factor per attempt, at least 1.0 |
| `max_delay_ms` | 30000 | ceiling on the computed delay |
| `jitter` | 0.1 | fraction of the delay randomised, 0.0 to 1.0 |

The wrapper retries every error, not just connection failures. A connector's
own reconnect settings, such as the postgres `connection.reconnect` block,
compose beneath it: the connector re-establishes the session, and if it still
fails the wrapper retries the operation. `retry` must sit beside `config`,
never inside it, because each connector's `config` is strict about its own
keys.

## Pacing a standalone run

`run_mode` sets what happens between iterations. It applies to standalone only.

| `kind` | Between iterations | Extra key |
|---|---|---|
| `continuous` | wait 100 ms, walk the graph again. The default. | none |
| `one_shot` | exit after the first pass | none |
| `interval` | wait `interval_ms`, then again | `interval_ms` |
| `stream` | no iterations: one pass per arriving batch | none |

```kdl,name=Run the workflow every five seconds
run_mode kind="interval" interval_ms=5000
```

`stream` needs at least one declared source, and a live source such as `tcp`
needs `stream`. [Stream mode](@/service/_index.md#stream-mode) has the rest.

## The control plane keys

```kdl,name=The http and observability blocks
http bind="0.0.0.0:8080" disabled=#false

// log_format is "pretty" or "json". trace_sample_ratio is parent-based
// trace-id-ratio sampling, 0.0 to 1.0.
observability log_format="pretty" log_level="info" trace_sample_ratio=1.0 {
    // OTLP/HTTP collector root; omit to disable span export.
    // otlp_endpoint "http://127.0.0.1:4318"

    // In-process capture, its JSON API and the dashboard. Every key has a
    // default, so the whole node may be omitted.
    inspector {
        enabled #true
        ui #true
        retention_secs 3600
        sample_interval_secs 1
        max_spans 10000
        max_logs 10000
        max_samples 3600
    }
}
```

`http bind` must parse as a socket address. `disabled=#true` turns the control
plane off entirely, the JSON API and the dashboard with it. `--log-level` and
`--log-format` override this block on the command line. Start the service, then
confirm the address answers:

```bash,name=Confirm the control plane is listening
curl -s http://localhost:8080/ready
{"status":"ready"}
```

`log_level` sets more than verbosity: it decides which spans exist. The default
`"info"` omits the per-item runner traces, because `workflow.batch`,
`source.drain`, `runtime.run`, `sink.write` and `processor.batch` are all `debug`
spans. The dashboard's Traces tab then shows traces rooted at `pipeline.run`, and
`log_level="debug"` is the escape hatch that restores the whole per-item
waterfall. It costs roughly 4.6 µs per item at `info` against 7.4 µs at `debug`,
so `debug` is for reading one workflow, not for running on. Levels per span name
are in [Observability](@/service/observability.md#span-levels).

## The store block

One `store "tikv"` block names the TiKV the process talks to. It needs a binary
built with `--features tikv-store`; a `store` block without it is a refusal, not
an ignored section.

```kdl,name=The store block
store "tikv" {
    // One or several PD endpoints, host:port.
    pd_endpoints "10.0.0.1:2379" "10.0.0.2:2379"
    key_prefix "pcs"       // alphanumerics, '.', '_' and '-'; default "pcs"
    timeout_ms 10000       // per-operation client timeout
    lease_ttl_ms 90000     // claim lease TTL, at least 10000
    batch_resume #false    // carry processor state across interval and one_shot runs
}
```

What it holds depends on the mode. Standalone and stream runs persist source
cursors and processor priors, so a restart resumes instead of replaying from the
start. Cluster mode adds the partitions, the claims and the checkpoints, which
is why `mode "cluster"` refuses to start without this block.

`key_prefix` is the whole isolation boundary. Two deployments sharing one TiKV
stay apart by choosing different prefixes.

## The cluster header

In cluster mode the top of the document changes: drop `run_mode`, add the Raft
timings and the peer list, and declare the store.

```kdl,name=The cluster header
mode "cluster"
bootstrap #true              // true on exactly one node, on first bring-up only
election_timeout_ms 1500
heartbeat_interval_ms 300
snapshot_log_interval 10000  // compact the raft log every N applied entries

// Every member, including this node. node id must appear here.
// addr is the Raft transport address, not the HTTP control-plane port.
peer id=1 addr="10.0.0.1:9000"
peer id=2 addr="10.0.0.2:9000"
peer id=3 addr="10.0.0.3:9000"
```

These timings drive elections, not claims. The claim lease is
`store "tikv" { lease_ttl_ms }`, and it is the only lease knob.

The workflow that follows carries exactly one `wasm` or `plugin` node and no
`source`, `sink` or `link`. `validate` reports `mode: cluster` once the header is
right; [the distributed runner](@/distributed.md) covers the rest.

## Which features add which nodes

The default bundle covers `service`, `wasm`, five connectors and five
transformers, so every node above binds out of the box. For a narrower binary:

- `service` registers no source, sink or format by itself. Each `connector-*`
  feature adds one connector, each `transformer-*` one byte format, and all of
  them imply `service` plus `inspector`.
- `service-cluster` is `service` plus `distributed-raft`. `mode "cluster"` needs
  it and `tikv-store` together.
- Neither implies `wasm`.

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
Without <code>wasm</code>, <code>WorkflowSpec</code> has <b>no <code>wasm</code>
field</b> while still rejecting unknown keys, so a <code>wasm</code> node in a
<code>--no-default-features --features service</code> build is a parse failure
rather than an ignored key. Every example config in the repository declares that
node.
</p>
</div>

```bash,name=Building a narrower binary
# Single node, WASM pipelines, CSV in and out.
cargo build --release -p pcs-service --bin pcs-service \
  --no-default-features --features mimalloc,connector-file,transformer-csv,wasm

# Same, plus cluster mode.
cargo build --release -p pcs-service --bin pcs-service \
  --no-default-features \
  --features mimalloc,service-cluster,tikv-store,connector-file,transformer-csv,wasm
```

Then run `validate` with the binary you built: a node kind its features do not
carry fails the parse, and a `type` they do not carry is named in a warning.

## Connector types

The `type` strings on sources and sinks are registry keys. Each pair below
arrives with one `connector-*` feature.

| Sources | Sinks | Feature | Required `config` keys |
|---|---|---|---|
| `FileSource` | `FileSink` | `connector-file` | `path`, plus `schema_fields` where the format needs it |
| `HttpSource` | `HttpSink` | `connector-http` | `url`, plus `schema_fields` where the format needs it |
| `PostgresSource` | `PostgresSink` | `connector-postgresql` | `name`, `connection`, `schema_fields`, plus `mode` or `table` |
| `tcp` | `tcp` | `connector-tcp` | `bind` on the source, `connect` on the sink, `schema_fields` on both |
| `ChannelSource` | `ChannelSink` | `connector-channel` | `schema_fields`. In-process, for tests |
| `KafkaSource` | `KafkaSink` | `connector-kafka` | `brokers`, `topic`, `schema_fields` |
| `NatsSource` | `NatsSink` | `connector-nats` | `connection`, `mode`, `schema_fields` |

`connector-kafka` is the one pair outside the default bundle, and each connector
page carries its own `config` keys: [Connectors](@/connectors/_index.md).

A node that moves bytes names a declared `transformer`, whose `format` is `csv`,
`ndjson`, `parquet`, `avro` or `arrow-ipc`, and the format decides whether
`schema_fields` is required. There is no default format anywhere.
`PostgresSource`, `PostgresSink`, `ChannelSource` and `ChannelSink` carry rows
rather than bytes and take no `transformer`. See
[Transformers](@/transformers/_index.md).

## What is not a key

`workflow` holds no `systems` node and no `components` node, and `wasm` takes no
`watch` property: nothing in the service builds a `System` from a type name, so
those keys are parse errors.

`WorkflowSpec`, `TransformerSpec`, `SourceSpec`, `SinkSpec`, `WasmSpec` and
`PluginSpec` reject unknown keys, so a typo inside `workflow` fails the parse.
`ServiceConfig` and `ObservabilityConfig` do not: an unrecognised key at the top
level or under `observability` is accepted and ignored.

## What it refuses to start on

Four checks, in a fixed order, each one a refusal rather than a warning. Nothing
about your component is touched until gate 2.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 452" role="img" aria-labelledby="svc-g-title svc-g-desc">
        <title id="svc-g-title">The four load-time gates a pcs-service start must pass</title>
        <desc id="svc-g-desc">
            Four gates run in order. First the config file is read, environment placeholders are
            substituted, and the document is parsed strictly and cross-validated, which is where
            the graph rules on ids and links are enforced. Second
            the WASM module is read, digest-checked, compiled and instantiated, which is
            where wasmtime matches the WIT world. Third every declared link is checked end
            to end: the components at its two ends must match and their Arrow fields must be
            identical. Fourth, in cluster mode only, the processor's Arrow schema fingerprint
            is compared with the fingerprint recorded in this node's persisted checkpoints.
        </desc>
        <text class="t-title" x="0" y="14">Load order</text>
        <text class="t-sm" x="0" y="30">each gate is a refusal to start, not a warning</text>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="44" width="430" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="44" width="430" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="58" width="430" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="59">1 &nbsp;ServiceConfig::load</text>
            <text class="t-sm" x="12" y="80">read the file, substitute ${VAR}, parse the KDL strictly</text>
            <text class="t-sm" x="12" y="94">then validate(): data_dir, peer ids, store block, bind addr</text>
            <text class="t-sm" x="12" y="108">unique node ids, link endpoints, no link cycle</text>
            <text class="t-lbl t-ctl" x="448" y="59">rejects</text>
            <text class="t-sm" x="448" y="80">a link into a source</text>
            <text class="t-sm" x="448" y="94">a duplicate node id</text>
            <text class="t-sm" x="448" y="108">cluster with no store</text>
        </g>
        <path class="arw arw-bnd" d="M215 120 V132" marker-end="url(#svc-b)"/>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="0" y="136" width="430" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="136" width="430" height="22" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="150" width="430" height="8"/>
            <text class="t-lbl t-bnd" x="12" y="151">2 &nbsp;PipelineRuntimeLoader::load</text>
            <text class="t-sm" x="12" y="172">read the module bytes, check the optional sha3_256</text>
            <text class="t-sm" x="12" y="186">compile, then instantiate against pcs:pipeline@0.3.0</text>
            <text class="t-sm" x="12" y="200">describe() is called once here, not at the first batch</text>
            <text class="t-lbl t-bnd" x="448" y="151">rejects</text>
            <text class="t-sm" x="448" y="172">a digest mismatch</text>
            <text class="t-sm" x="448" y="186">a missing import</text>
            <text class="t-sm" x="448" y="200">a trap in describe()</text>
        </g>
        <path class="arw arw-ctl" d="M215 212 V224" marker-end="url(#svc-c)"/>
        <g class="anim anim-3">
            <rect class="blk blk-ctl" x="0" y="228" width="430" height="76" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="228" width="430" height="22" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="242" width="430" height="8"/>
            <text class="t-lbl t-ctl" x="12" y="243">3 &nbsp;validate_workflow_graph</text>
            <text class="t-sm" x="12" y="264">every link end to end: the components at its two</text>
            <text class="t-sm" x="12" y="278">ends must match and their Arrow fields be identical.</text>
            <text class="t-sm" x="12" y="292">Runs inside ServiceBuilder::build, before it returns</text>
            <text class="t-lbl t-ctl" x="448" y="243">rejects</text>
            <text class="t-sm" x="448" y="264">sink 'orders_out' reads</text>
            <text class="t-sm" x="448" y="278">'Order', which the</text>
            <text class="t-sm" x="448" y="292">processor never declares</text>
        </g>
        <path class="arw arw-data" d="M215 304 V316" marker-end="url(#svc-d)"/>
        <g class="anim anim-4">
            <rect class="blk blk-data" x="0" y="320" width="430" height="76" rx="8"/>
            <rect class="hd hd-data" x="0" y="320" width="430" height="22" rx="8"/>
            <rect class="hd hd-data" x="0" y="334" width="430" height="8"/>
            <text class="t-lbl t-data" x="12" y="335">4 &nbsp;validate_schema_fingerprint</text>
            <text class="t-sm" x="12" y="356">the processor's Arrow schema fingerprint against the one</text>
            <text class="t-sm" x="12" y="370">written into this node's persisted checkpoints</text>
            <text class="t-sm" x="12" y="384">cluster mode only: inside run_cluster, once TiKV answers</text>
            <text class="t-lbl t-data" x="448" y="335">rejects</text>
            <text class="t-sm" x="448" y="356">a schema change laid</text>
            <text class="t-sm" x="448" y="370">on top of checkpoints</text>
            <text class="t-sm" x="448" y="384">of the older shape</text>
        </g>
        <g class="anim anim-4">
            <path class="ln" d="M0 420 H654"/>
            <text class="t-sm" x="0" y="440">Gates 1 to 3 also run under <tspan class="t-ctl">pcs-service validate</tspan>. Only <tspan class="t-ctl">serve</tspan> reaches gate 4.</text>
        </g>
        <defs>
            <marker id="svc-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control)"/>
            </marker>
            <marker id="svc-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary)"/>
            </marker>
            <marker id="svc-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> state already persisted</span>
        <span class="k-control"><i></i> config and host checks</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
    </div>
    <figcaption class="dgm-cap">
        Gate 3 is the only one that compares two things you wrote: a component name in
        your config against a component name in your Rust. It is also the only gate that
        <b>silently passes</b> when a runtime declares nothing. An empty component list
        opts that link out of the comparison rather than failing it.
    </figcaption>
</div>

`pcs-service validate` runs gates 1 to 3 and exits, so a stale `component` name
is caught without moving any data. Only `serve` reaches gate 4.

**Next:** [Observability](@/service/observability.md), the four probes and the
in-process buffers behind them.
