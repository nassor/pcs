+++
title = "Six languages, one pipeline"
description = "One Order workload as six WebAssembly components in Go, Python, TypeScript, Kotlin, C# and Rust, chained through the same wasmtime host pcs-service uses."
template = "page.html"
weight = 8
aliases = ["/guests/four-languages/"]
+++

# Six languages, one pipeline

Every other page on this site shows Rust, because the engine, the host and the
guest SDK are Rust. **The pipeline contract is the `pcs:pipeline@0.2.0` WIT
world.** A guest is any WebAssembly component that imports
`pcs:pipeline/host-io` and exports `pcs:pipeline/pipeline`'s two functions,
`describe` and `run-batch`. The Rust SDK (`pcs-guest`) is a convenience for
code already written in Rust, not the interface.

`examples/polyglot/` runs one `Order` component through six stages in six
languages, chained through the same wasmtime host that `pcs-service` uses.

## The example

Each stage writes one part of the row and reads what an earlier stage wrote. A
wrong codec in any language produces visibly wrong numbers rather than a silent
byte difference.

| # | stage | language | toolchain | writes | host-io it exercises |
|---|-------|----------|-----------|--------|----------------------|
| 1 | `validate-go` | Go | `componentize-go` 0.4.1 | `valid` | `get-config`, `metric`, `log` |
| 2 | `enrich-py` | Python | `componentize-py` 0.25.0 | `usd_amount` | `get-config` (three FX keys), `metric`, `log` |
| 3 | `score-ts` | TypeScript | `jco` 1.30.0 | `risk_score`, `flagged` | `get-config`, `metric`, `log` |
| 4 | `fee-kt` | Kotlin | Kotlin 2.4.0 plus `wasm-tools` 1.246.2 | `fee` | `get-config` (one key per region), `metric`, `log` |
| 5 | `tier-cs` | C# | `componentize-dotnet` on .NET 10 | `review_tier` | `get-config`, `metric`, `log` |
| 6 | `settle-rs` | Rust | `cargo-component` 0.21.1 | `settlement` | `metric`, `log`, **checkpoint** |

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 250" role="img" aria-labelledby="pg-title pg-desc">
        <title id="pg-title">One Order batch through six WebAssembly guests, one per language</title>
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
        downstream. It comes back to the same guest as the next call's <code>prior</code>.
    </figcaption>
</div>

Only `settle-rs` is stateful. Its `Ledger` survives the batch boundary through
`run-result.checkpoint`: the host persists that blob verbatim and hands it back
as the next call's `prior`. That is the only channel, because the host builds a
fresh wasmtime `Store` for every `run-batch`.

The ledger is also where the chain closes. It nets the Kotlin stage's `fee` out
of the Python stage's `usd_amount`, over the rows the C# stage cleared, so one
number depends on five languages agreeing on the same bytes.

## Run it

```bash
# Build all six components. Needs Go, Python, Node, a JDK and .NET; see examples/polyglot/PINS.md.
bash scripts/build-polyglot.sh

# Drive the chain and print both batches. `tracing` is what surfaces guest metrics.
cargo run -p pcs-service --features wasm,tracing --example polyglot_orders

# The same assertions, automated.
cargo test -p pcs-service --features wasm --test polyglot_chain -- --nocapture
```

The driver prints all six `describe()` responses first. All six report the same
`schema_fingerprint` and `declared_components: ["Order"]`. The driver exits
non-zero if any of them disagrees with the live fingerprint of the canonical
Rust schema, which catches a stale generated constant before it corrupts a
column.

## Five of the six share one Arrow codec

The non-Rust guests do not link an Arrow library. They depend on
[`pcs-arrow-ipc`](@/reference/arrow-ipc-packages.md), which implements just
enough of the IPC format to read the columns a stage needs and to **overwrite
fixed-width value bytes in place**, returning the input buffer mutated. Each
stage therefore carries one dependency, itself standard-library-only. The recipes
are [Go](@/guests/go.md), [Python](@/guests/python.md),
[TypeScript](@/guests/typescript.md), [Kotlin](@/guests/kotlin.md) and
[C#](@/guests/csharp.md); [Rust](@/guests/rust.md) needs no codec because
`pcs-guest` re-exports the Arrow crates. The bytes all five implement are
specified in [the wire format](@/reference/wire-format.md).

That shows up in the schema: `settlement` is the only variable-length *output*,
and it belongs to the Rust stage. Rewriting a `Utf8` column means rewriting its
offsets buffer, its values buffer, and the RecordBatch flatbuffer that
describes both, which needs a real Arrow writer.

Every other output is fixed-width and can be patched byte-wise. Between them the
five packaged stages write every fixed-width type the format has: `Boolean` for
`valid` and `flagged`, `Float64` for `usd_amount`, `risk_score` and `fee`,
`Int64` for `review_tier`.

## When your language has a real Arrow library, use it

A standard-library-only codec is a deliberate constraint of the example: a stage
then depends on one package with no transitive dependencies of its own. It is not
a recommendation.

For a production guest, reach for the real binding first:
[`arrow-go`](https://github.com/apache/arrow-go),
[`apache-arrow`](https://www.npmjs.com/package/apache-arrow) on npm,
[`pyarrow`](https://arrow.apache.org/docs/python/), or
[`Apache.Arrow`](https://www.nuget.org/packages/Apache.Arrow) on NuGet. None of
them is verified here, and each has to survive its own componentizer. `arrow-go`
is documented as incompatible with TinyGo, `pyarrow` has no `wasm32-wasi` wheel,
and `apache-arrow` under StarlingMonkey is untested.

`Apache.Arrow` is pure C# with no native dependency, which makes it the most
plausible of the six to come through intact. Kotlin has the worst odds:
[`arrow-java`](https://github.com/apache/arrow-java) is a JVM library, so a
Kotlin multiplatform build reaches it from a `jvm` target and not from
`wasmWasi`. Check yours before you commit to it. A real binding buys schema
evolution and variable-length writes; `pcs-arrow-ipc` is what ships when no
upstream binding survives its componentizer.

## The cost of a fresh Store

The host constructs a fresh wasmtime `Store` per `run-batch`. For a compiled
guest that is cheap. For an interpreted one the language runtime re-initialises
every batch: measurable for Python, negligible for Go and Rust. Larger batches
amortise it.
