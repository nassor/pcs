+++
title = "Service"
description = "The pcs-service binary: one config file, two run modes, four commands, and a workflow that runs until you stop it."
template = "section.html"
sort_by = "weight"

[extra]
kicker = "Service"
+++

`pcs-service` reads one config file, loads the workflow it declares, and runs it
until you stop it. Everything it will refuse to start on, it refuses at load
time.

<dl class="page-facts">
<dt>In one line</dt>
<dd>A KDL config plus a <code>.wasm</code> component becomes a <strong>long-running process with health checks</strong></dd>
<dt>You need</dt>
<dd>The default feature bundle, which covers the wasm runtime, five connectors and five transformers</dd>
<dt>Read this if</dt>
<dd>You have a pipeline that works and now need it to run unattended, behind a readiness probe, on a schedule</dd>
</dl>

[Installation](@/quickstart/installation.md) puts the binary on your PATH.

## The commands

| Command | What it does |
|---|---|
| `serve` | Start. `--node-id N` overrides `node.id`; `--port P` overrides the port in `http.bind`, and `--port 0` prints the OS-assigned address on stdout. |
| `validate` | Load-time gates 1 to 3, then exit. `--strict` promotes unknown factory types to errors. |
| `status --addr URL` | One summary line from `/status`. `--full` prints the whole JSON document. |
| `cluster init` | Pre-flight only: confirms `mode "cluster"` and `bootstrap #true`, then tells you to run `serve`. It does not start the node. |
| `cluster status --addr URL` | The `cluster` field of `/status`. |
| `cluster join --leader URL`, `cluster leave` | Not wired to the control plane. Both print the manual procedure, editing the `peer` nodes on every node and restarting, and exit 0. |

`--config` and `--addr` are global. `--config` defaults to `pcs.kdl` and reads
`PCS_CONFIG`; `--addr` reads `PCS_ADDR`. `--log-level` and `--log-format`
override `observability`.

Start with `validate`: it is the one command that touches nothing.
[Configuration](@/service/configuration.md) writes the file it reads.

## The two run modes

The top-level `mode` key picks the runner. Both drive the same `PipelineRuntime`
trait object, so a processor never learns which one is running it.

| | `mode "standalone"` | `mode "cluster"` |
|---|---|---|
| **Feature** | `service`, in the default bundle | `service-cluster`, opt-in |
| **Coordination** | None. One process, one dataset. | Raft over TCP between the declared `peer` nodes. |
| **Ingest** | Each `source` node drains into the nodes its links feed, every iteration. | `PartitionSource`: batches are claimed, not read. A `source`, `sink` or `link` node is rejected. |
| **Pacing** | `run_mode`: `continuous`, `one_shot`, `interval`, or `stream`. | As fast as claims arrive, until cancelled. |
| **On restart** | Re-reads its sources from the beginning. | Resumes from the checkpoint recorded under its lease. |
| **Uses `node.data_dir`** | No. Required to be non-empty, never written. | Yes: `raft-log.redb`, `cluster-app.redb`, `bootstrap.lock`, `node-id`. |

A standalone iteration checks for cancellation, then walks every declared node
once in topological order, so a node always runs after every node that links
into it. A source drains to EOF, a processor calls `run_on` and forwards the
result, a sink writes whatever was staged for it. The stats snapshot publishes at
the end of the pass, then `run_mode` paces the next one.

Errors do not stop the loop. A source failure stops draining that source for
this iteration; a processor failure skips that processor's fan-out, so nothing
downstream of it is fed and every sink is **still** written. All of them log and
increment `iteration_errors`, which `/status` reports.

A WASM processor node runs on a blocking thread rather than the async worker,
and only Arrow IPC bytes cross onto that thread and back. A processor may also
deliver to some of its downstream links instead of all of them:
[Branching](@/service/branching.md).

The loop is running when the counters move:

```bash,name=Watch the iteration counters move
curl -s http://localhost:8080/status \
  | jq '.standalone | {iterations, rows_processed, iteration_errors}'

{
  "iterations": 41,
  "rows_processed": 148213,
  "iteration_errors": 0
}
```

<div class="note">
<span class="note-label">Constraint</span>
<p>
Cluster mode runs exactly one <code>wasm</code> or <code>plugin</code> node and
rejects every <code>source</code>, <code>sink</code> and <code>link</code> node at
config-validation time, before anything starts. The cluster runner ingests through
<code>PartitionSource</code>, a pull mechanism, so a declared <code>Source</code>
would be silently ignored and the workflow would sit idle. Register batches from a
producer instead.
</p>
</div>

### Stream mode

`run_mode kind="stream"` replaces that loop with a per-item one. Each
`RecordBatch` a source yields, typically one row, walks the graph in topological
order before the next item is pulled. Latency is bounded by the workflow, not by
a pacing timer. Sinks are written per item but `finish()` is called once, at
exit.

| | `kind="stream"` |
|---|---|
| **Sources** | At least one, enforced at config-validation time. Several are pulled round-robin, one batch per item. Cluster mode is not supported. |
| **Invocation** | One `run-batch` call per received batch. No coalescing: the producer chooses the item size. |
| **Processor state** | The checkpoint blob one item returns is handed back as the next item's `prior`, one blob per processor node, so processor state survives even though the store does not. |
| **Durability** | At-most-once. The state blob lives in loop memory only: it is never checkpointed and is lost on restart. A failed item is logged, counted, and dropped; `prior` is left at the last good value. |
| **Stats** | `iterations` counts items; `total_busy_micros` and `max_item_micros` report per-item cost. `/status` is refreshed at most every 100 ms, since a per-item lock write would dominate the budget. |

Three source types need it. `type="tcp"` is always live; `NatsSource` and
`KafkaSource` are live unless their config sets `stop_at_end #true`. A live
source never reaches EOF, so config validation rejects one outside stream mode
with `source type '<name>' never reaches EOF`. The `tcp` frame format lives in
[TCP ingest](@/connectors/tcp.md).

[Running it!](@/quickstart/running-it.md) drives a live NATS source end to end,
with a publisher you can start and stop.

## Your own sources, sinks, transformers, and runtime

A `type` string in the config is a key into a registry of factories. To add your
own, implement one trait and register it before `build()`. The factory receives
that node's `config` plus a `ConnectorContext` carrying the transformer the host
resolved, and returns a boxed `Source` or `Sink`.

```rust,name=A sink factory for your own connector
use pcs_connector::{ConfigValue, ConnectorContext, SinkFactory};
use pcs_core::PcsError;
use pcs_core::io::sink::Sink;

struct ClickHouseSinkFactory;

impl SinkFactory for ClickHouseSinkFactory {
    // This is the string the config writes as `type="ClickHouseSink"`.
    fn type_name(&self) -> &'static str { "ClickHouseSink" }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, PcsError> {
        let url = config
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PcsError::configuration("ClickHouseSink needs a 'url'"))?;
        // The transformer is whichever declared `transformer` id this sink
        // named. Naming the connector puts it in the error a sink that named
        // none gets.
        let transformer = ctx.transformer("ClickHouseSink")?;
        Ok(Box::new(ClickHouseSink::connect(url, transformer)?))
    }
}
```

Then assemble the service yourself. `register_builtin_factories` adds the
connectors and transformers whose features are on, and nothing else.
`register_source`, `register_sink` and `register_transformer` chain your own on;
registering the same name twice replaces the earlier factory.

```rust,name=Assembling the service yourself
use pcs_service::service::config::ServiceConfig;
use pcs_service::service::factories::register_builtin_factories;
use pcs_service::service::{ServiceBuilder, run_standalone};
use tokio_util::sync::CancellationToken;

let config = ServiceConfig::load("service.kdl")?;

// `build()` constructs every declared node and validates every link end to end
// before it returns.
let built = register_builtin_factories(ServiceBuilder::new())
    .register_source(MongoSourceFactory)
    .register_sink(ClickHouseSinkFactory)
    .register_transformer(ProtobufTransformerFactory)
    .build(&config)?;

// `None` opts out of publishing live stats to /status.
let stats = run_standalone(built, &config, CancellationToken::new(), None).await?;
```

For a native Rust processor, declare its `wasm` node with no `module` and hand
the builder a runtime keyed by that node's id. Any `Box<dyn PipelineRuntime>`
works, and a `Pipeline` is one.

```rust,name=Handing the builder a native runtime
let built = ServiceBuilder::new()
    .with_runtime("enrich", Box::new(my_pipeline))
    .register_sink(ClickHouseSinkFactory)
    .build(&config)?;
```

A node naming a `module` or a `library` loads that artifact and never looks at
`with_runtime`. A node naming neither takes the runtime registered under its own
id, and `build()` errors naming the node when nothing is registered for it.

The stock binary names every type it cannot resolve, which is the list your
registrations have to cover:

```bash,name=What the stock binary says about a custom type
pcs-service validate --config service.kdl

WARNING: no sink factory registered for type 'ClickHouseSink' (required by sink 'orders_out')
NOTE: 1 unknown type(s) above are not in the built-in registry. They may be
user-defined types registered at serve time. Use --strict to treat these as errors.
```

In your own binary `build()` is the check: it constructs every declared node, so
an unregistered `type` is an error there rather than a warning.

## What a runtime says about itself

A host holding a `Box<dyn PipelineRuntime>` has five methods: `name()`,
`run_on()`, `template_dataset()`, `declared_components()` and
`descriptor_info()`.

`name()` is the node id the host gave the runtime. `descriptor_info()` is the
identity the runtime declares for itself: `name`, `version`, `stateful` and
`schema_fingerprint`, where `name` is a component's `describe()` name or a
plugin's manifest name. [The dashboard](@/service/dashboard.md) prints those four
fields on every processor node, which is how you confirm the artifact running is
the one you built.

**Next:** [Configuration](@/service/configuration.md), the KDL file and the
workflow inside it.
