+++
title = "The WIT contract"
description = "A walk through crates/pcs-guest/wit/pipeline.wit: every record, what the host does with each field, and the two commands that prove your component exports the world."
template = "page.html"
weight = 1
+++

# The WIT contract

WIT, the WebAssembly Interface Type language, describes a component's imports
and exports as typed interfaces. A **world** is a named bundle of them: what a
component must import to run, and what it must export to be useful. Compile a
component against a world and the toolchain, not the host, checks that you
implemented it.

PCS ships one world. The whole file is
`crates/pcs-guest/wit/pipeline.wit`, 116 lines, and it ends with this:

```wit
package pcs:pipeline@0.2.0;

world pcs-pipeline {
    import host-io;
    export pipeline;
}
```

That is the reason the contract is a world and not an SDK. An SDK is a library
in one language, and depending on it makes that language part of the contract. A
world is a type signature. `pcs-guest` is a convenience for Rust authors that
happens to satisfy it.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 180" role="img" aria-labelledby="wit-title wit-desc">
        <title id="wit-title">The pcs-pipeline world: one export, one import</title>
        <desc id="wit-desc">
            The guest exports the pipeline interface, whose two functions are describe, a
            control plane call made once at load, and run-batch, the data plane call made once
            per batch. The guest imports the host-io interface, whose three functions log,
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
        The host side of both interfaces is generated. <code>bindings.rs</code> is 26 lines of
        <code>wasmtime::component::bindgen!</code> pointed at the same WIT directory the guest
        compiles against, so the host cannot drift from the contract.
    </figcaption>
</div>

## interface types

Two type aliases carry every byte that moves:

```wit
type ipc-bytes = list<u8>;
type checkpoint = list<u8>;
```

`ipc-bytes` is a whole serialised `Dataset`, framed as described in
[the wire format](@/reference/wire-format.md). `checkpoint` is opaque: the host
persists it verbatim through `CheckpointStore` and never parses it, so the
layout is entirely the guest's business.

### component-descriptor

```wit
record component-descriptor {
    name: string,
    arrow-schema-ipc: list<u8>,
}
```

`name` is the string a source's `target_component` and a sink's
`source_component` must match. `arrow-schema-ipc` is a schema-only Arrow IPC
stream, a `StreamWriter` opened on the schema and immediately finished with no
batches; the host reads it with `StreamReader::schema()` to build the template
dataset that sources and sinks are cast against.

### pipeline-descriptor

```wit
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
| `name` | `string` | Identity. Prefixes guest log lines and appears in `/status` |
| `version` | `string` | Identity only. The host does not compare it against anything |
| `components` | `list<component-descriptor>` | `validate_io_coverage` checks every configured `target_component` and `source_component` against this list, and fails the load if one is missing |
| `stateful` | `bool` | Declares that the guest intends to return a `checkpoint`. A stateless guest returns `none` |
| `schema-fingerprint` | `string` | `validate_schema_fingerprint` compares it against the fingerprint the node's persisted checkpoints were written with. A cluster node refuses to start on a mismatch rather than mixing layouts |

Do not reimplement the fingerprint hash in your guest. Generate it from the
canonical schema at build time and embed it as a constant, the way all six
polyglot stages do. [The wire format](@/reference/wire-format.md) gives the
algorithm, for the one case where you have to.

### run-metrics and run-result

```wit
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
}
```

| field | meaning |
|---|---|
| `wall-ns` | Time the guest spent in `run-batch`, guest measured |
| `rows-in` | Rows the guest read out of `input` |
| `rows-out` | Rows in `output`. Differs from `rows-in` only if the guest filtered |
| `systems-run` | Systems the guest executed for this batch |
| `retries` | Retry attempts the guest's own pipeline consumed |
| `output` | The mutated dataset, framed as `ipc-bytes` |
| `checkpoint` | The blob handed back as the next call's `prior`, or `none` |

`WasmPipelineRuntime` reads `output` and `checkpoint` from a `run-result` and
drops `metrics`. Fill the record anyway, because it is the guest's own
accounting, but push anything an operator has to see through `host-io::metric`.

### run-error

```wit
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
discovered mid-batch is a guest bug and must be collapsed into `permanent`. A
guest panic or trap surfaces as `permanent` too, but the operator loses the
batch, so returning a structured error is strictly better.

## interface host-io

```wit
enum log-level { trace, debug, info, warn, error }

log: func(level: log-level, target: string, message: string);
metric: func(name: string, value: f64);
get-config: func(key: string) -> option<string>;
```

`crates/pcs-service/src/wasm/host_impl.rs` implements all three:

- `log` bridges to `tracing`, one macro per level, tagged with the pipeline name
  and the guest's `target`. Without the `tracing` feature it falls back to
  stderr.
- `metric` routes to the Prometheus exporter the service layer owns. `value` is
  `f64` to cover counters, gauges and histograms with one signature.
- `get-config` is a lookup in the `[pipeline.wasm.config]` table, cloned per
  call. Values are strings and the guest parses numerics itself.

What is absent is the point: no filesystem, no network, no clock, no
environment. A guest that needs a rate table gets it through `get-config`, and a
guest that needs to report something gets `log` and `metric`. WASI imports are
linked because transitive dependencies need them, not as a capability grant: the
host builds its `WasiCtx` with no `inherit_*` calls.

## interface pipeline

```wit
describe: func() -> pipeline-descriptor;
run-batch: func(input: ipc-bytes, prior: option<checkpoint>)
    -> result<run-result, run-error>;
```

`describe` runs once, at load. `run-batch` runs once per batch, each time
against a fresh wasmtime `Store`, so `prior` is the only state that reaches it
from the previous call. `prior` is `none` on the first batch after a cold start.

`describe` is the easy one to get wrong, because nothing local catches it. Every
`run-batch` still succeeds with a wrong component list or a stale fingerprint;
the failure surfaces later as a cluster node refusing to start, or as a source
whose `target_component` the host cannot match. Assert on your `describe`
output in a test, and generate the schema bytes and the fingerprint from one
canonical definition.

## Verify the world

Two commands, and neither needs the host:

```bash
wasm-tools validate --features component-model my-stage.wasm
wasm-tools component wit my-stage.wasm | grep 'pcs:pipeline'
```

The second must print both halves of the world:

```text
  import pcs:pipeline/host-io@0.2.0;
  export pcs:pipeline/pipeline@0.2.0;
```

If it does not, stop. Nothing downstream will work, and the fix is almost
always the bindings step rather than the guest code.
