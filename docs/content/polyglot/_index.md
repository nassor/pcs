+++
title = "Pipelines in any language"
description = "The pipeline contract is a WIT world, not an SDK. A worked example runs one workload through four WebAssembly guests written in Go, Python, JavaScript and Rust."
template = "section.html"
sort_by = "title"

[extra]
kicker = "Any language"
+++

Every other page on this site shows Rust, because the engine, the host and the
guest SDK are Rust. That is an accident of who wrote them. **The pipeline
contract is the `pcs:pipeline@0.2.0` WIT world** — a guest is any WebAssembly
component that imports `pcs:pipeline/host-io` and exports
`pcs:pipeline/pipeline`'s two functions, `describe` and `run-batch`. The Rust
SDK (`pcs-guest`) is a convenience for people already writing Rust. It is not
the interface.

`examples/polyglot/` is the proof: one `Order` component, four stages, four
languages, chained through the same wasmtime host that `pcs-service` uses.

## The example

Each stage writes exactly one part of the row and reads what the previous stage
wrote, so if any language's codec is wrong the chain produces visibly wrong
numbers rather than a silent byte difference.

| # | stage | language | toolchain | writes | host-io it exercises |
|---|-------|----------|-----------|--------|----------------------|
| 1 | `validate-go` | Go | `componentize-go` 0.4.1 | `valid` | `get-config`, `metric`, `log` |
| 2 | `enrich-py` | Python | `componentize-py` 0.25.0 | `usd_amount` | `get-config` (three FX keys), `metric`, `log` |
| 3 | `score-js` | JavaScript | `jco` 1.30.0 | `risk_score`, `flagged` | `get-config`, `metric`, `log` |
| 4 | `settle-rs` | Rust | `cargo-component` 0.21.1 | `settlement` | `metric`, `log`, **checkpoint** |

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 150" role="img" aria-labelledby="pg-title pg-desc">
        <title id="pg-title">One Order batch through four WebAssembly guests, one per language</title>
        <desc id="pg-desc">
            Five rows of Order enter validate-go, written in Go, which writes valid. Its
            output feeds enrich-py, written in Python, which writes usd_amount. That feeds
            score-js, written in JavaScript, which writes risk_score and flagged. That feeds
            settle-rs, written in Rust, which writes settlement and is the only stage whose
            state survives to the next batch, via a checkpoint that loops back into it rather
            than downstream.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="40" width="70" height="56" rx="8"/>
            <rect class="hd hd-data" x="0" y="40" width="70" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="70" height="8"/>
            <text class="t-lbl" x="12" y="55">Order</text>
            <text class="t-sm" x="12" y="76">5 rows</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M70 68 H90" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="90" y="40" width="110" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="90" y="40" width="110" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="90" y="52" width="110" height="8"/>
            <text class="t-lbl" x="102" y="55">validate-go</text>
            <text class="t-sm" x="102" y="76">Go</text>
            <text class="t-sm t-data" x="102" y="89">+ valid</text>
        </g>
        <g class="anim anim-3">
            <path class="arw arw-data" d="M200 68 H220" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="220" y="40" width="130" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="220" y="40" width="130" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="220" y="52" width="130" height="8"/>
            <text class="t-lbl" x="232" y="55">enrich-py</text>
            <text class="t-sm" x="232" y="76">Python</text>
            <text class="t-sm t-data" x="232" y="89">+ usd_amount</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M350 68 H370" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="370" y="40" width="150" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="370" y="40" width="150" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="370" y="52" width="150" height="8"/>
            <text class="t-lbl" x="382" y="55">score-js</text>
            <text class="t-sm" x="382" y="76">JavaScript</text>
            <text class="t-sm t-data" x="382" y="89">+ risk_score</text>
            <text class="t-sm t-data" x="382" y="102">+ flagged</text>
            <path class="arw arw-data" d="M520 68 H540" marker-end="url(#pg-d)"/>
            <rect class="blk blk-bnd" x="540" y="40" width="120" height="56" rx="8"/>
            <rect class="hd hd-bnd" x="540" y="40" width="120" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="540" y="52" width="120" height="8"/>
            <text class="t-lbl" x="552" y="55">settle-rs</text>
            <text class="t-sm" x="552" y="76">Rust</text>
            <text class="t-sm t-data" x="552" y="89">+ settlement</text>
            <path class="arw arw-bnd" d="M650 96 V118 H570 V96" marker-end="url(#pg-b)"/>
            <text class="t-sm t-bnd t-mid" x="610" y="132">checkpoint</text>
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
        downstream — it comes back to the same guest as the next call's <code>prior</code>.
    </figcaption>
</div>

Only `settle-rs` is stateful. Its `Ledger` survives the batch boundary through
`run-result.checkpoint`, which the host persists verbatim and hands back as the
next call's `prior` — the only channel available, because the host builds a fresh
wasmtime `Store` for every `run-batch`.

## Run it

```bash
# Build all four components (needs Go, Python, Node — see examples/polyglot/PINS.md).
bash scripts/build-polyglot.sh

# Drive the chain and print both batches. `tracing` is what surfaces guest metrics.
cargo run -p pcs-service --features wasm,tracing --example polyglot_orders

# The same assertions, automated.
cargo test -p pcs-service --features wasm --test polyglot_chain -- --nocapture
```

The driver prints all four `describe()` responses first. All four report the
same `schema_fingerprint` and `declared_components: ["Order"]`, and the driver
exits non-zero if any of them disagrees with the live fingerprint of the
canonical Rust schema — that is how a stale generated constant gets caught
instead of silently corrupting a column.

## Three of the four hand-roll the Arrow codec

The non-Rust guests do not link an Arrow library. They implement just enough of
the IPC format to read the columns they need and to **overwrite fixed-width value
bytes in place**, returning the input buffer mutated. That keeps each stage
dependent on nothing beyond its language's standard library, which is what makes
the contract documentable in the first place — see
[the wire format](@/polyglot/wire-format.md).

The consequence is visible in the schema: `settlement` is the only variable-length
*output*, and it belongs to the Rust stage. Rewriting a `Utf8` column means
rewriting its offsets buffer, its values buffer, and the RecordBatch flatbuffer
that describes both — which needs a real Arrow writer. Every other column is
fixed-width and can be patched byte-wise.

## One honest cost

The host constructs a fresh wasmtime `Store` per `run-batch`. For a compiled
guest that is cheap. For an interpreted one it means the language runtime
re-initialises every batch: measurable for Python, negligible for Go and Rust.
If you are putting Python on a hot path, batch size is the knob that matters.
