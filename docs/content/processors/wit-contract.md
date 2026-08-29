+++
title = "The WIT contract"
description = "A walk through crates/pcs-processor/wit/pipeline.wit: every record, what the host does with each field, and the two commands that prove your component exports the world."
template = "page.html"
weight = 1
aliases = ["/guests/wit-contract/"]
+++

# The WIT contract

WIT, the WebAssembly Interface Type language, describes a component's imports
and exports as typed interfaces. A **world** is a named bundle of them: what a
component must import to run, and what it must export to be useful. Compile a
component against a world and the toolchain, not the host, checks that you
implemented it.

PCS ships one world. The whole file is
`crates/pcs-processor/wit/pipeline.wit`, 124 lines, and it ends with this:

```wit,name=The world pipeline.wit ends with
package pcs:pipeline@0.3.0;

world pcs-pipeline {
    import host-io;
    export pipeline;
}
```

That is the reason the contract is a world and not an SDK. An SDK is a library
in one language, and depending on it makes that language part of the contract.
A world is a type signature. `pcs-processor` is a convenience for Rust authors
that happens to satisfy it.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 180" role="img" aria-labelledby="wit-title wit-desc">
        <title id="wit-title">The pcs-pipeline world: one export, one import</title>
        <desc id="wit-desc">
            The processor exports the pipeline interface, whose two functions are describe, a
            control plane call made once at load, and run-batch, the data plane call made once
            per batch. The processor imports the host-io interface, whose three functions log,
            metric and get-config run leftward into the host. Nothing else crosses the
            boundary: there is no filesystem, no network and no clock.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="46" width="160" height="72" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="46" width="160" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="58" width="160" height="8"/>
            <text class="t-lbl" x="12" y="61">pcs-service</text>
            <text class="t-sm" x="12" y="82">wasmtime host</text>
            <text class="t-sm" x="12" y="95">host_impl.rs</text>
            <text class="t-sm" x="12" y="108">bindings.rs</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="490" y="46" width="170" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="490" y="46" width="170" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="490" y="58" width="170" height="8"/>
            <text class="t-lbl" x="502" y="61">your component</text>
            <text class="t-sm" x="502" y="82">world pcs-pipeline</text>
            <text class="t-sm" x="502" y="95">export pipeline</text>
            <text class="t-sm" x="502" y="108">import host-io</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl t-mid" x="325" y="26">export pipeline</text>
            <text class="t-sm t-ctl t-mid" x="325" y="46">describe() once, at load</text>
            <path class="arw arw-ctl" d="M160 52 H490" marker-end="url(#wit-c)"/>
            <text class="t-sm t-mid" x="325" y="74">run-batch(input, prior) once per batch</text>
            <path class="arw arw-data" d="M160 80 H490" marker-end="url(#wit-d)"/>
            <path class="arw arw-data" d="M490 98 H160" marker-end="url(#wit-d)"/>
            <text class="t-sm t-mid" x="325" y="110">result&lt;run-result, run-error&gt;</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M490 136 H160" marker-end="url(#wit-b)"/>
            <text class="t-sm t-bnd t-mid" x="325" y="132">import host-io</text>
            <text class="t-sm t-mid" x="325" y="152">log, metric, get-config</text>
            <text class="t-sm t-mid" x="325" y="172">no filesystem, no network, no clock</text>
        </g>
        <defs>
            <marker id="wit-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control)"/>
            </marker>
            <marker id="wit-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
            <marker id="wit-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> control plane: describe, and host-io</span>
        <span class="k-data"><i></i> data plane: Arrow IPC bytes both ways</span>
        <span class="k-boundary"><i></i> the component boundary</span>
    </div>
    <figcaption class="dgm-cap">
        The host side of both interfaces is generated. <code>bindings.rs</code> is 20 lines of
        <code>wasmtime::component::bindgen!</code> pointed at a vendored copy of the WIT the processor
        compiles against, with <code>tests/wit_vendored.rs</code> keeping the two copies from
        drifting.
    </figcaption>
</div>

## interface types

Two type aliases carry every byte that moves:

```wit,name=The two type aliases
type ipc-bytes = list<u8>;
type checkpoint = list<u8>;
```

`ipc-bytes` is a whole serialised `Dataset`, framed as described in
[the wire format](@/reference/wire-format.md). `checkpoint` is opaque: the host
persists it verbatim through `CheckpointStore` and never parses it, so the
layout is entirely the processor's business.

### component-descriptor

```wit,name=The component descriptor record
record component-descriptor {
    name: string,
    arrow-schema-ipc: list<u8>,
}
```

`name` is the string a source's or sink's `component` property must match.
`arrow-schema-ipc` is a schema-only Arrow IPC
stream, a `StreamWriter` opened on the schema and immediately finished with no
batches; the host reads it with `StreamReader::schema()` to build the template
dataset that sources and sinks are cast against.

### pipeline-descriptor

```wit,name=The pipeline descriptor record
record pipeline-descriptor {
    name: string,
    version: string,
    components: list<component-descriptor>,
    stateful: bool,
    schema-fingerprint: string,
}
```

| field | WIT type | what the host does with it |
|---|---|---|
| `name` | `string` | Identity. Prefixes processor log lines and appears in `/status` |
| `version` | `string` | Identity only. The host does not compare it against anything |
| `components` | `list<component-descriptor>` | `validate_workflow_graph` checks the `component` at each end of every declared `link` against this list, and fails the load when one is missing or its Arrow fields differ |
| `stateful` | `bool` | Declares that the processor intends to return a `checkpoint`. A stateless processor returns `none` |
| `schema-fingerprint` | `string` | `validate_schema_fingerprint` compares it against the fingerprint the node's persisted checkpoints were written with. A cluster node refuses to start on a mismatch rather than mixing layouts |

Do not reimplement the fingerprint hash in your processor. Generate it from the
canonical schema at build time and embed it as a constant, the way all six
polyglot processors do. [The wire format](@/reference/wire-format.md) gives the
algorithm, for the one case where you have to.

### run-metrics and run-result

```wit,name=The run metrics and run result records
record run-metrics {
    wall-ns: u64,
    rows-in: u64,
    rows-out: u64,
    systems-run: u32,
    retries: u32,
}

record run-result {
    output: ipc-bytes,
    checkpoint: option<checkpoint>,
    metrics: run-metrics,
    routes: option<list<string>>,
}
```

| field | meaning |
|---|---|
| `wall-ns` | Time the processor spent in `run-batch`, processor measured |
| `rows-in` | Rows the processor read out of `input` |
| `rows-out` | Rows in `output`. Differs from `rows-in` only if the processor filtered |
| `systems-run` | Systems the processor executed for this batch |
| `retries` | Retry attempts the processor's own pipeline consumed |
| `output` | The mutated dataset, framed as `ipc-bytes` |
| `checkpoint` | The blob handed back as the next call's `prior`, or `none` |
| `routes` | Branch names this batch's output is delivered to. `none` keeps legacy multicast; `some([])` delivers nowhere |

`WasmPipelineRuntime` reads all four fields of a `run-result`. The five
`run-metrics` numbers become the `pcs_processor_batch_duration_seconds`,
`pcs_processor_rows_in_total`, `pcs_processor_rows_out_total`,
`pcs_processor_systems_run_total` and `pcs_processor_retries_total` series on
`/metrics`, so filling the record honestly is what an operator sees. Anything
the record has no field for goes through `host-io::metric`, which lands in the
`pcs_processor_metric` histogram labelled with the name you pass.

`routes` is the routing channel. A pipeline routes its output by inserting a
`RouteDecision` resource into the batch dataset before the systems finish:

```rust,name=Routing a batch to one branch
data.insert_resource(RouteDecision(vec!["high".to_string()]));
```

The host reads it after the systems run and delivers the output only to the
links whose `branch` names one of those values. Absent, the host multicasts to
every downstream link.

### run-error

```wit,name=The run error variant
variant run-error {
    retryable(string),
    permanent(string),
    schema-mismatch(string),
}
```

| variant | host behaviour |
|---|---|
| `retryable(string)` | Releases the claim and retries on the next tick |
| `permanent(string)` | Acks the claim, logs, surfaces it to `/status` |
| `schema-mismatch(string)` | Refuses to replay. **Must never come out of `run-batch`** |

`schema-mismatch` is reserved for a future load-time check. A schema problem
discovered mid-batch is a processor bug and must be collapsed into `permanent`.
A processor panic or trap surfaces as `permanent` too, but the operator loses
the batch, so returning a structured error is strictly better.

## interface host-io

```wit,name=The host-io interface
enum log-level { trace, debug, info, warn, error }

log: func(level: log-level, target: string, message: string);
metric: func(name: string, value: f64);
get-config: func(key: string) -> option<string>;
```

`crates/pcs-service/src/wasm/host_impl.rs` implements all three:

- `log` bridges to `tracing`, one macro per level, tagged with the pipeline name
  and the processor's `target`. Without the `tracing` feature it falls back to
  stderr.
- `metric` routes to the Prometheus exporter the service layer owns. `value` is
  `f64` to cover counters, gauges and histograms with one signature.
- `get-config` is a lookup in the `wasm` node's `config` keys, cloned per
  call. Values are strings and the processor parses numerics itself.

What is absent is the point: no filesystem, no network, no clock, no
environment. A processor that needs a rate table gets it through
`get-config`, and a processor that needs to report something gets `log` and
`metric`. WASI imports are linked because transitive dependencies need them,
not as a capability grant: the host builds its `WasiCtx` with no
`inherit_*` calls.

## interface pipeline

```wit,name=The pipeline interface
describe: func() -> pipeline-descriptor;
run-batch: func(input: ipc-bytes, prior: option<checkpoint>)
    -> result<run-result, run-error>;
```

`describe` runs once, at load. `run-batch` runs once per batch, each time
against a fresh wasmtime `Store`, so `prior` is the only state that reaches it
from the previous call. `prior` is `none` on the first batch after a cold start.

`describe` is the easy one to get wrong, because nothing local catches it. Every
`run-batch` still succeeds with a wrong component list or a stale fingerprint;
the failure surfaces later as a cluster node refusing to start, or as a link
whose `component` the host cannot match. Assert on your `describe`
output in a test, and generate the schema bytes and the fingerprint from one
canonical definition.

## Verify the world

Two commands, and neither needs the host:

Windows (PowerShell):

```powershell
wasm-tools validate --features component-model my-processor.wasm
wasm-tools component wit my-processor.wasm | Select-String 'pcs:pipeline'
```

The second must print both halves of the world:

```text,name=Expected wasm-tools output
  import pcs:pipeline/host-io@0.3.0;
  export pcs:pipeline/pipeline@0.3.0;
```

If it does not, stop. Nothing downstream will work, and the fix is almost
always the bindings step rather than the processor code.
