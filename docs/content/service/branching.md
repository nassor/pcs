+++
title = "Branching"
description = "Conditional fan-out: a processor names branches for the batch it produced, and the host delivers that batch only to the links carrying those names."
template = "page.html"
weight = 4
+++

# Branching

<dl class="page-facts">
<dt>What it is</dt>
<dd>Delivery a processor decides: <strong>one list of branch names per batch</strong></dd>
<dt>Reach for it when</dt>
<dd>One inbound stream has to leave through <strong>different sinks by content</strong></dd>
<dt>Granularity</dt>
<dd>The <strong>batch</strong>, not the row. In stream mode a batch is one message</dd>
</dl>

Every `link` delivers by default: a node's output reaches all of its downstream
links. Branching makes that conditional. A processor names one or more branches
for the batch it just produced, and the host delivers that batch only to the
links carrying those names. It never looks at a row to decide.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 246" role="img" aria-labelledby="brc-title brc-desc">
        <title id="brc-title">A processor returns a branch name and the host delivers to that link alone</title>
        <desc id="brc-desc">
            The source orders_in hands one batch to the processor classify, which is a
            WebAssembly component or a native plugin. classify returns a routing decision
            naming the single branch eu. Two labelled links leave it: one labelled eu to the
            sink eu_out and one labelled us to the sink us_out. Only the eu link carries this
            batch, drawn in the data-plane colour; the us link is drawn muted because the
            decision did not name it, so us_out receives nothing from this batch.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="76" width="130" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="76" width="130" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="88" width="130" height="8"/>
            <text class="t-lbl" x="12" y="91">orders_in</text>
            <text class="t-sm" x="12" y="114">one batch</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M130 104 H210" marker-end="url(#brc-d)"/>
            <rect class="blk blk-bnd" x="210" y="66" width="190" height="76" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="66" width="190" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="78" width="190" height="8"/>
            <text class="t-lbl t-bnd" x="222" y="81">classify</text>
            <text class="t-sm" x="222" y="104">wasm or plugin</text>
            <text class="t-sm t-bnd" x="222" y="124">RouteDecision(["eu"])</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M400 104 C450 104 450 54 500 54" marker-end="url(#brc-d)"/>
            <text class="t-sm t-data t-end" x="492" y="48">eu</text>
            <rect class="blk blk-data" x="500" y="30" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="500" y="30" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="500" y="42" width="160" height="8"/>
            <text class="t-lbl" x="512" y="45">eu_out</text>
            <text class="t-sm" x="512" y="66">carries this batch</text>
        </g>
        <g class="anim anim-4">
            <path class="arw" d="M400 104 C450 104 450 154 500 154" marker-end="url(#brc-m)"/>
            <text class="t-sm t-end" x="492" y="148">us</text>
            <rect class="blk" x="500" y="130" width="160" height="48" rx="8"/>
            <rect class="hd" x="500" y="130" width="160" height="20" rx="8"/>
            <rect class="hd" x="500" y="142" width="160" height="8"/>
            <text class="t-lbl" x="512" y="145">us_out</text>
            <text class="t-sm" x="512" y="166">not named, gets nothing</text>
            <path class="ln" d="M0 202 H654"/>
            <text class="t-sm" x="0" y="222">The host reads the decision after the systems run, then walks the node's outbound links once.</text>
            <text class="t-sm" x="0" y="238">A node labels every outbound link or none, so an unconditional copy is taken upstream of the router.</text>
        </g>
        <defs>
            <marker id="brc-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
            <marker id="brc-m" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--muted-foreground)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane: the batch and the link it took</span>
        <span class="k-boundary"><i></i> the processor, and the decision it returns</span>
        <span class="k-mute"><i></i> a link this batch did not select</span>
    </div>
    <figcaption class="dgm-cap">
        Routing is a filter over one node's outbound edges. Sinks receive the same Arrow
        buffers either way, so a second branch costs no re-encoding.
    </figcaption>
</div>

## When you need it

Reach for a branch when one stream has to leave through different sinks
depending on what is in it: priority classes to different queues, regions to
different tables, rejects to a quarantine file.

Three neighbouring problems are not branching:

- **The same rows in two places.** Two unlabelled links off one node already
  multicast.
- **Dropping rows.** A decision covers the whole batch. Per-row filtering is
  `mark_dead` and `compact` inside the processor.
- **Throughput.** Splitting work across instances is the
  [distributed runner](@/distributed.md) claiming partitions.

The batch is the unit of a decision, so `run_mode kind="stream"` gives one
decision per message and `continuous` gives one per drain.

## The config

A `branch` key on a `link` labels it, and the name is matched against what the
upstream processor returns.

```kdl,name=A branch key labels each link
workflow "orders" {
    wasm "classify" module="pipelines/classify.wasm"

    link from="orders_in" to="classify"

    link from="classify" to="eu_out" branch="eu"
    link from="classify" to="us_out" branch="us"
}
```

What the host does with each batch:

| The batch's decision | Where the output goes |
|---|---|
| none returned | every downstream link, labelled or not |
| `["eu"]` | the links labelled `eu` |
| `["eu", "us"]` | the links labelled `eu` and the links labelled `us` |
| `[]` | nowhere: the batch is dropped |
| a name no link carries | that name delivers nowhere; the host logs a warning and carries on |

A decision selects labelled links only, so a deciding processor whose links
carry no labels delivers nothing.

Three shapes are refused at load time, before the first batch:

| Refused | Why |
|---|---|
| `branch` on a link out of a source | a source multicasts unconditionally, so the label would lie |
| one node labelling some outbound links and not others | all or none, otherwise the unlabelled ones would silently duplicate every branch |
| a name outside `^[A-Za-z0-9][A-Za-z0-9_-]*$`, or over 64 bytes | branch names share the id charset |

Check the labels before running anything. `validate` refuses all three shapes,
and the grammar sits in
[the workflow structure](@/service/configuration.md#the-workflow-structure).

```bash,name=Validate a branching config
pcs-service validate --config service.kdl

OK: workflow graph validated (components and schemas agree end to end)
```

## The processor side

A processor decides by inserting a `RouteDecision` resource into the batch
dataset before its systems finish.

```rust,name=The processor inserts a RouteDecision
data.insert_resource(RouteDecision(vec!["eu".to_string()]));
```

It is a resource rather than a component because resources do not cross the
Arrow IPC boundary, so the decision never appears in the output rows. The SDK
macro reads it after the systems run and reports the names in
`run-result.routes`:
[the WIT contract](@/processors/wit-contract.md#run-metrics-and-run-result). A
native plugin uses the same type from `pcs-plugin`.

## Watching a branch

Each labelled edge reports its own throughput. The dashboard draws the branch
name on a chip at the edge's midpoint and reads `pcs_processor_rows_out_total`
under `processor="<id>", branch="<name>"`. An edge that stays blank while its
sibling moves is an unreachable branch.

## The runnable example

`examples/branching/branching.kdl` is one stream workflow carrying every way
output fans out. A NATS core subject feeds it in `run_mode kind="stream"`, so
both routers decide per message. It needs a clone of the repository, a NATS
broker, and the two processors.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 366" role="img" aria-labelledby="br-title br-desc">
        <title id="br-title">One source multicasts to three nodes, and each router writes one of its two branches</title>
        <desc id="br-desc">
            The NATS source node in feeds three downstream nodes over unlabelled links: the
            sink out_mirror, the WebAssembly processor router_wasm and the native plugin
            router_plugin. router_wasm has two labelled links, high to out_wasm_high and low
            to out_wasm_low, and router_plugin has two more, premium to out_plugin_premium
            and standard to out_plugin_standard. Each router writes only the branch its
            decision names, so the five CSV files divide the stream between them.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="100" width="124" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="100" width="124" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="112" width="124" height="8"/>
            <text class="t-lbl" x="12" y="115">in</text>
            <text class="t-sm" x="12" y="138">NatsSource</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M124 128 C167 128 167 54 210 54" marker-end="url(#br-d)"/>
            <path class="arw arw-data" d="M124 128 H210" marker-end="url(#br-d)"/>
            <path class="arw arw-data" d="M124 128 C167 128 167 198 210 198" marker-end="url(#br-d)"/>
            <rect class="blk blk-data" x="210" y="30" width="150" height="48" rx="8"/>
            <rect class="hd hd-data" x="210" y="30" width="150" height="20" rx="8"/>
            <rect class="hd hd-data" x="210" y="42" width="150" height="8"/>
            <text class="t-lbl" x="222" y="45">out_mirror</text>
            <text class="t-sm" x="222" y="66">out-mirror.csv</text>
            <rect class="blk blk-bnd" x="210" y="100" width="150" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="100" width="150" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="112" width="150" height="8"/>
            <text class="t-lbl t-bnd" x="222" y="115">router_wasm</text>
            <text class="t-sm" x="222" y="138">wasm component</text>
            <rect class="blk blk-bnd" x="210" y="170" width="150" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="170" width="150" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="210" y="182" width="150" height="8"/>
            <text class="t-lbl t-bnd" x="222" y="185">router_plugin</text>
            <text class="t-sm" x="222" y="208">native plugin</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M360 128 C425 128 425 54 490 54" marker-end="url(#br-d)"/>
            <text class="t-sm t-data t-end" x="482" y="48">high</text>
            <path class="arw arw-data" d="M360 128 C425 128 425 124 490 124" marker-end="url(#br-d)"/>
            <text class="t-sm t-data t-end" x="482" y="118">low</text>
            <rect class="blk blk-data" x="490" y="30" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="490" y="30" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="42" width="160" height="8"/>
            <text class="t-lbl" x="502" y="45">out_wasm_high</text>
            <text class="t-sm" x="502" y="66">out-wasm-high.csv</text>
            <rect class="blk blk-data" x="490" y="100" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="490" y="100" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="112" width="160" height="8"/>
            <text class="t-lbl" x="502" y="115">out_wasm_low</text>
            <text class="t-sm" x="502" y="136">out-wasm-low.csv</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M360 198 C425 198 425 194 490 194" marker-end="url(#br-d)"/>
            <text class="t-sm t-data t-end" x="482" y="188">premium</text>
            <path class="arw arw-data" d="M360 198 C425 198 425 264 490 264" marker-end="url(#br-d)"/>
            <text class="t-sm t-data t-end" x="482" y="240">standard</text>
            <rect class="blk blk-data" x="490" y="170" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="490" y="170" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="182" width="160" height="8"/>
            <text class="t-lbl" x="502" y="185">out_plugin_premium</text>
            <text class="t-sm" x="502" y="206">out-plugin-premium.csv</text>
            <rect class="blk blk-data" x="490" y="240" width="160" height="48" rx="8"/>
            <rect class="hd hd-data" x="490" y="240" width="160" height="20" rx="8"/>
            <rect class="hd hd-data" x="490" y="252" width="160" height="8"/>
            <text class="t-lbl" x="502" y="255">out_plugin_standard</text>
            <text class="t-sm" x="502" y="276">out-plugin-standard.csv</text>
            <path class="ln" d="M0 316 H654"/>
            <text class="t-sm" x="0" y="336">An unlabelled link always delivers, so out_mirror, router_wasm and router_plugin each see every message.</text>
            <text class="t-sm" x="0" y="352">A labelled link delivers only when that router's decision names its branch.</text>
        </g>
        <defs>
            <marker id="br-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the two processor runtimes</span>
    </div>
    <figcaption class="dgm-cap">
        One workflow, three splits. The source split is unconditional; each router split
        is one decision per batch, and the sinks divide the stream because of it.
    </figcaption>
</div>

Seven `link` lines are the whole graph. The source's three carry no `branch`, so
`out_mirror` keeps a verbatim copy of the stream the two routers split.

```kdl,name=Seven links are the whole graph
link from="in" to="out_mirror"
link from="in" to="router_wasm"
link from="in" to="router_plugin"

link from="router_wasm" to="out_wasm_high" branch="high"
link from="router_wasm" to="out_wasm_low" branch="low"

link from="router_plugin" to="out_plugin_premium" branch="premium"
link from="router_plugin" to="out_plugin_standard" branch="standard"
```

Both routers read the first row's `priority`. The wasm one names `high` or
`low`, the plugin maps the same values to `premium` and `standard`, and that is
why one publisher fills four files.

### Build the processors

From the repository root:

```bash,name=Build both processors
cargo build --release -p branching-wasm --target wasm32-wasip2
cargo build -p branching-plugin
```

The plugin artifact name is platform specific. The config defaults to the Linux
name, so only macOS and Windows need `PCS_PLUGIN_LIB`:

| Platform | Plugin artifact | `PCS_PLUGIN_LIB` |
|----------|-----------------|------------------|
| Linux | `target/debug/libbranching_plugin.so` | not needed (config default) |
| macOS | `target/debug/libbranching_plugin.dylib` | `target/debug/libbranching_plugin.dylib` |
| Windows | `target/debug/branching_plugin.dll` | `target/debug/branching_plugin.dll` |

A mislabelled graph is caught before anything runs, so validate first, with
`PCS_PLUGIN_LIB` set on macOS and Windows:

```bash,name=Validate the branching config
cargo run -p pcs-service --features connector-file,transformer-csv,wasm,plugin -- validate \
  --config examples/branching/branching.kdl --strict
```

### Run it

Start NATS, then the service, then the publisher. `PCS_OUT_DIR` names the
directory the five sink files land in, and it must exist first.

```bash,name=Start NATS
docker run -d --name pcs-nats -p 4222:4222 nats:2.11-alpine
```

```bash,name=Start the service
mkdir -p /tmp/pcs-branching

PCS_OUT_DIR=/tmp/pcs-branching \
cargo run -p pcs-service --features connector-file,transformer-csv,wasm,plugin -- serve \
  --config examples/branching/branching.kdl
```

Then, in another terminal:

```bash,name=Run the publisher
cargo run -p pcs-service --example branching_publish -- --rate 50
```

On macOS and Windows add `PCS_PLUGIN_LIB` to the serve command. The config writes
forward-slash paths, so `PCS_OUT_DIR=C:/pcs-branching` works as it stands.

The publisher's flags: `--count` (0 runs until Ctrl-C, the default), `--rate`
messages per second (default 50), `--url` (default `nats://localhost:4222`),
`--subject` (default `branching.orders`), `--seed`.

### What each output file holds

The publisher draws `priority` 50/50 between `"high"` and `"low"`, so all four
branches fill continuously while it runs.

| Output | Holds |
|--------|-------|
| `out-mirror.csv` | every message, in arrival order |
| `out-wasm-high.csv` | the `"high"` messages |
| `out-wasm-low.csv` | the `"low"` messages |
| `out-plugin-premium.csv` | the `"high"` messages |
| `out-plugin-standard.csv` | the `"low"` messages |

Each router pair partitions the same stream under different names, so
`out-wasm-high.csv` and `out-plugin-premium.csv` hold the same rows. Every
`FileSink` appends and writes its CSV header once. The mirror's line count
splitting across each pair is the proof:

```bash,name=Confirm each branch got its share
wc -l /tmp/pcs-branching/*.csv
```

**Next:** [Windowing](@/service/windowing.md), the other thing a processor node
declares in its own block.
