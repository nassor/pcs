+++
title = "WASM Processors"
description = "The pipeline contract is a WIT world, not an SDK. Any component that exports describe and run-batch is a PCS pipeline."
template = "section.html"
sort_by = "weight"
aliases = ["/guests/", "/polyglot/", "/polyglot/writing-a-guest/", "/guests/six-languages/", "/guests/four-languages/", "/processors/four-languages/", "/processors/six-languages/"]

[extra]
kicker = "WASM Processors"
+++

`pcs-service` does not know Rust. It knows one WIT world: `pcs:pipeline@0.3.0`.
Anything that compiles to that shape is a processor.

## What a processor is

A **processor** is a WebAssembly component: a `.wasm` file built against the
[Component Model](https://component-model.bytecodealliance.org/), targeting
WASI 0.2. It is not a Rust artifact. Rust's `wasm32-wasip2` target is one way
to produce one, and Go, Python, TypeScript, Kotlin and C# each have their own.
`pcs-service` loads whichever `.wasm` your config names and never learns what
built it.

The whole contract is two exported functions and one imported interface:

- **`describe() -> pipeline-descriptor`** runs once, when the processor loads.
  It reports its name, version, the Arrow schema of every component it
  declares, a `schema-fingerprint`, and whether it is stateful.
- **`run-batch(input: ipc-bytes, prior: option<checkpoint>) -> result<run-result, run-error>`**
  runs once per batch. `input` is Arrow IPC bytes; a successful `run-result`
  carries Arrow IPC bytes back, plus metrics and an optional `checkpoint`.
- **`host-io`** is the interface the processor *imports*, not exports:
  `get-config`, `metric`, `log`. Three things a sandboxed component cannot do
  for itself.

The host builds a **fresh wasmtime `Store` for every `run-batch` call**.
Nothing a processor keeps in a global or a struct field survives to the next
call, only what it puts in `checkpoint`. The host persists that blob verbatim
and hands it back as the next call's `prior`. That is the only channel for
state that has to cross a batch boundary.

Filling those records in by hand is optional. Each language in this section has
a small SDK that reads a row type declared in that language: it derives the
Arrow schema, the `schema-fingerprint` and the descriptor from the declaration,
decodes the batch into row values, runs the transforms, re-encodes, and folds
any failure into `run-error::permanent`. The declaration is the schema, which is
how the six polyglot stages report one fingerprint from six independently
written row types.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 170" role="img" aria-labelledby="gd-title gd-desc">
        <title id="gd-title">The shape of a PCS processor: two calls in, one loop back</title>
        <desc id="gd-desc">
            pcs-service calls describe on the processor once, at load, to learn its component
            schemas and fingerprint. It then calls run-batch once per batch, passing Arrow IPC
            input and the prior checkpoint, and gets back a run-result or a run-error. The
            processor exports exactly those two functions and imports host-io for config, metrics
            and logging. A fresh wasmtime Store backs every run-batch call, so any state the
            processor keeps must round-trip through the checkpoint the host holds and replays as
            the next call's prior.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="40" width="170" height="56" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="40" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="52" width="170" height="8"/>
            <text class="t-lbl" x="12" y="55">pcs-service</text>
            <text class="t-sm" x="12" y="76">wasmtime host</text>
            <text class="t-sm" x="12" y="89">fresh Store per batch</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="450" y="40" width="210" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="450" y="40" width="210" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="450" y="52" width="210" height="8"/>
            <text class="t-lbl" x="462" y="55">your processor &middot; .wasm</text>
            <text class="t-sm t-bnd" x="462" y="76">exports pipeline</text>
            <text class="t-sm t-bnd" x="462" y="89">imports host-io</text>
            <text class="t-sm" x="462" y="102">any language, WASI 0.2</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl t-mid" x="310" y="44">describe() &rarr; descriptor, once at load</text>
            <path class="arw arw-ctl" d="M170 50 H450" marker-end="url(#gd-c)"/>
            <text class="t-sm t-mid" x="310" y="62">run-batch(input, prior)</text>
            <path class="arw arw-data" d="M170 68 H450" marker-end="url(#gd-d)"/>
            <path class="arw arw-data" d="M450 86 H170" marker-end="url(#gd-d)"/>
            <text class="t-sm t-mid" x="310" y="98">run-result | run-error</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M25 96 V130 H105 V96" marker-end="url(#gd-b)"/>
            <text class="t-sm t-bnd t-mid" x="65" y="144">checkpoint &rarr; prior</text>
        </g>
        <defs>
            <marker id="gd-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control)"/>
            </marker>
            <marker id="gd-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
            <marker id="gd-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> describe(), and whatever the processor calls into host-io</span>
        <span class="k-data"><i></i> run-batch: Arrow IPC bytes in, Arrow IPC bytes or an error out</span>
        <span class="k-boundary"><i></i> checkpoint: the one thing that survives a fresh Store</span>
    </div>
    <figcaption class="dgm-cap">
        <code>describe()</code> runs once, at load, which makes the descriptor the easiest
        part to get wrong: a component list that disagrees with the workflow is refused by
        the load-time graph check, and a schema that disagrees with the host's registration
        surfaces as a failed batch. An SDK derives both from one row type, so the two cannot
        drift apart. <code>run-batch</code> runs every batch against a <b>new</b>
        <code>Store</code>: nothing survives in a global or a struct field, only in
        <code>checkpoint</code>.
    </figcaption>
</div>

## Six languages, one pipeline

The claim above has a demonstration. `examples/polyglot/` runs one `Order`
component through six stages in six languages, chained through the same
wasmtime host `pcs-service` uses. Each stage writes one part of the row and
reads what an earlier stage wrote, so a wrong codec in any language produces
visibly wrong numbers rather than a silent byte difference. Versions of every
toolchain are pinned in `examples/polyglot/PINS.md`.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 250" role="img" aria-labelledby="pg-title pg-desc">
        <title id="pg-title">One Order batch through six WebAssembly processors, one per language</title>
        <desc id="pg-desc">
            Six rows of Order enter validate-go, written in Go, which writes valid. Its
            output feeds enrich-py, written in Python, which writes usd_amount. That feeds
            score-ts, written in TypeScript, which writes risk_score and flagged. The stream
            then wraps to a second row of stages and feeds fee-kt, written in Kotlin, which
            writes fee. That feeds tier-cs, written in C sharp, which writes review_tier.
            That feeds settle-rs, written in Rust, which writes settlement and is the only
            stage whose state survives to the next batch, via a checkpoint that loops back
            into it rather than downstream.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="36" width="70" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="36" width="70" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="48" width="70" height="8"/>
            <text class="t-lbl" x="12" y="51">Order</text>
            <text class="t-sm" x="12" y="72">6 rows</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M70 64 H92" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="92" y="36" width="120" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="92" y="36" width="120" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="92" y="48" width="120" height="8"/>
            <text class="t-lbl" x="104" y="51">validate-go</text>
            <text class="t-sm" x="104" y="72">Go</text>
            <text class="t-sm t-data" x="104" y="85">+ valid</text>
            <path class="arw arw-data" d="M212 64 H234" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="234" y="36" width="130" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="234" y="36" width="130" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="234" y="48" width="130" height="8"/>
            <text class="t-lbl" x="246" y="51">enrich-py</text>
            <text class="t-sm" x="246" y="72">Python</text>
            <text class="t-sm t-data" x="246" y="85">+ usd_amount</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M364 64 H386" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="386" y="36" width="140" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="386" y="36" width="140" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="386" y="48" width="140" height="8"/>
            <text class="t-lbl" x="398" y="51">score-ts</text>
            <text class="t-sm" x="398" y="72">TypeScript</text>
            <text class="t-sm t-data" x="398" y="85">+ risk_score</text>
            <text class="t-sm t-data" x="398" y="98">+ flagged</text>
            <path class="arw arw-data" d="M526 64 H556 V124 H32 V150" marker-end="url(#pg-d)"/>
        </g>
        <g class="anim anim-4">
            <rect class="blk blk-bnd" x="0" y="150" width="120" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="150" width="120" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="0" y="162" width="120" height="8"/>
            <text class="t-lbl" x="12" y="165">fee-kt</text>
            <text class="t-sm" x="12" y="186">Kotlin</text>
            <text class="t-sm t-data" x="12" y="199">+ fee</text>
            <path class="arw arw-data" d="M120 178 H142" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="142" y="150" width="130" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="142" y="150" width="130" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="142" y="162" width="130" height="8"/>
            <text class="t-lbl" x="154" y="165">tier-cs</text>
            <text class="t-sm" x="154" y="186">C#</text>
            <text class="t-sm t-data" x="154" y="199">+ review_tier</text>
            <path class="arw arw-data" d="M272 178 H294" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="294" y="150" width="130" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="294" y="150" width="130" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="294" y="162" width="130" height="8"/>
            <text class="t-lbl" x="306" y="165">settle-rs</text>
            <text class="t-sm" x="306" y="186">Rust</text>
            <text class="t-sm t-data" x="306" y="199">+ settlement</text>
            <path class="arw arw-bnd" d="M414 206 V228 H334 V206" marker-end="url(#pg-b)"/>
            <text class="t-sm t-bnd t-mid" x="374" y="242">checkpoint</text>
        </g>
        <defs>
            <marker id="pg-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
            <marker id="pg-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> Order rows, and the field each stage writes</span>
        <span class="k-boundary"><i></i> the WebAssembly component boundary</span>
    </div>
    <figcaption class="dgm-cap">
        Every arrow between stages is a length-prefixed Arrow IPC stream. The loop under
        <code>settle-rs</code> is different: <code>run-result.checkpoint</code> never goes
        downstream. It comes back to the same processor as the next call's <code>prior</code>.
    </figcaption>
</div>

Only `settle-rs` is stateful. Its `Ledger` survives the batch boundary through
`run-result.checkpoint`, the channel the fresh-Store rule above leaves: the
host persists that blob verbatim and hands it back as the next call's
`prior`. The ledger nets the Kotlin stage's `fee` out of the Python stage's
`usd_amount`, over the rows the C# stage cleared, so one number depends on
five languages agreeing on the same bytes.

| stage | language | toolchain | writes |
|---|---|---|---|
| `validate-go` | Go | `componentize-go` | `valid` |
| `enrich-py` | Python | `componentize-py` | `usd_amount` |
| `score-ts` | TypeScript | `jco` | `risk_score`, `flagged` |
| `fee-kt` | Kotlin | `wasm-tools` | `fee` |
| `tier-cs` | C# | `componentize-dotnet` | `review_tier` |
| `settle-rs` | Rust | `cargo` | `settlement` |

Every stage exercises `host-io`: `metric` and `log` in all six, `get-config`
in five (the Python stage reads three FX keys, the Kotlin stage one key per
region).

### Run it

```bash,name=Build the six components then drive the chain
# Build all six components. Needs Go, Python, Node, a JDK and .NET.
cargo xtask polyglot

# Drive the chain and print both batches. `tracing` is what surfaces processor metrics.
cargo run -p pcs-service --features wasm,tracing --example polyglot_orders

# The same assertions, automated.
cargo test -p pcs-service --features wasm --test polyglot_chain -- --nocapture
```

The driver prints all six `describe()` responses first. All six report the
same `schema_fingerprint` and `declared_components: ["Order"]`, and the
driver exits non-zero if any disagrees with the live fingerprint of the
canonical Rust schema, which catches a stale generated constant before it
corrupts a column.

### Why five of the six carry their own codec

The non-Rust stages do not link an Arrow library. Each depends on its
language's [SDK package](@/reference/arrow-ipc-packages.md), which implements
just enough of the IPC format to read the columns a stage needs and to
overwrite fixed-width value bytes in place. `settlement` is the only
variable-length output, and it belongs to the Rust stage, because rewriting a
`Utf8` column needs a real Arrow writer. The bytes all five implement are
specified in [the wire format](@/reference/wire-format.md).

A standard-library-only codec is a deliberate constraint of the example, not
a recommendation: a stage then depends on one package with no transitive
dependencies of its own. Reach for the real binding first when one survives
your componentizer. [The SDK packages page](@/reference/arrow-ipc-packages.md)
carries the per-language odds.

[The WIT contract](@/processors/wit-contract.md) walks `pipeline.wit` record by
record. [Processors](@/processors/languages.md) is the toolchain recipe, one per
language.
