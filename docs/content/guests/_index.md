+++
title = "WebAssembly guests"
description = "The pipeline contract is a WIT world, not an SDK. Any component that exports describe and run-batch is a PCS pipeline."
template = "section.html"
sort_by = "weight"
aliases = ["/polyglot/", "/polyglot/writing-a-guest/"]

[extra]
kicker = "WebAssembly guests"
+++

`pcs-service` does not know Rust. It knows one WIT world: `pcs:pipeline@0.2.0`.
Anything that compiles to that shape is a guest.

## What a guest is

A **guest** is a WebAssembly component: a `.wasm` file built against the
[Component Model](https://component-model.bytecodealliance.org/), targeting
WASI 0.2. It is not a Rust artifact. `cargo-component` is one way to produce
one, and Go, Python, TypeScript, Kotlin and C# each have their own.
`pcs-service` loads whichever `.wasm` your config names and never learns what
built it.

The whole contract is two exported functions and one imported interface:

- **`describe() -> pipeline-descriptor`** runs once, when the guest loads. It
  reports its name, version, the Arrow schema of every component it declares,
  a `schema-fingerprint`, and whether it is stateful.
- **`run-batch(input: ipc-bytes, prior: option<checkpoint>) -> result<run-result, run-error>`**
  runs once per batch. `input` is Arrow IPC bytes; a successful `run-result`
  carries Arrow IPC bytes back, plus metrics and an optional `checkpoint`.
- **`host-io`** is the interface the guest *imports*, not exports:
  `get-config`, `metric`, `log`. Three things a sandboxed component cannot do
  for itself.

The host builds a **fresh wasmtime `Store` for every `run-batch` call**.
Nothing a guest keeps in a global or a struct field survives to the next call,
only what it puts in `checkpoint`. The host persists that blob verbatim and
hands it back as the next call's `prior`. That is the only channel for state
that has to cross a batch boundary.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 170" role="img" aria-labelledby="gd-title gd-desc">
        <title id="gd-title">The shape of a PCS guest: two calls in, one loop back</title>
        <desc id="gd-desc">
            pcs-service calls describe on the guest once, at load, to learn its component
            schemas and fingerprint. It then calls run-batch once per batch, passing Arrow IPC
            input and the prior checkpoint, and gets back a run-result or a run-error. The
            guest exports exactly those two functions and imports host-io for config, metrics
            and logging. A fresh wasmtime Store backs every run-batch call, so any state the
            guest keeps must round-trip through the checkpoint the host holds and replays as
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
            <text class="t-lbl" x="462" y="55">your guest &middot; .wasm</text>
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
        <span class="k-control"><i></i> describe(), and whatever the guest calls into host-io</span>
        <span class="k-data"><i></i> run-batch: Arrow IPC bytes in, Arrow IPC bytes or an error out</span>
        <span class="k-boundary"><i></i> checkpoint: the one thing that survives a fresh Store</span>
    </div>
    <figcaption class="dgm-cap">
        <code>describe()</code> runs once, which makes it easy to under-test: get
        <code>schema-fingerprint</code> or the component list wrong and every local
        <code>run-batch</code> still succeeds. The failure surfaces as
        <code>validate_schema_fingerprint</code> on a cluster node that refuses to start.
        <code>run-batch</code> runs every batch against a <b>new</b> <code>Store</code>:
        nothing survives in a global or a struct field, only in <code>checkpoint</code>.
    </figcaption>
</div>

## Before you start

Point every toolchain below at the same WIT package. It is the single canonical
copy. Do not vendor it.

```text
crates/pcs-guest/wit/pipeline.wit
```

[The WIT contract](@/guests/wit-contract.md) walks that file record by record.
You need `wasm-tools` once, regardless of language:

```bash
cargo install wasm-tools --locked --version 1.246.2
```

All six recipes end with these two commands:

```bash
wasm-tools validate --features component-model <component>.wasm
wasm-tools component wit <component>.wasm | grep 'pcs:pipeline'
```

The second command must print a world importing `pcs:pipeline/host-io@0.2.0`
and exporting `pcs:pipeline/pipeline@0.2.0`. If it does not, stop. Nothing past
this point will work, and the fix is almost always the bindings step, not the
guest code.

## Choose your language

Each page is built around a guest that lives in this repository and that CI
builds.

| Language | Arrow codec | Toolchain | Verified runtime | Reach for it when |
|---|---|---|---|---|
| [**Rust**](@/guests/rust.md) | No, `pcs-guest` provides it | `cargo-component` 0.21.1 | Rust 1.95+ | You are already in the workspace, or ceremony and performance both matter |
| [**Go**](@/guests/go.md) | `github.com/nassor/pcs/packages/arrow-ipc-go` | `componentize-go` 0.4.1 | Go 1.25.5+ (CI: 1.26.3) | Your team ships Go already, and the transform is field-level logic rather than text |
| [**Python**](@/guests/python.md) | `pcs-arrow-ipc` | `componentize-py` 0.25.0 | Python 3.10+ (CI: 3.14) | Fastest to prototype. Keep batches large, because CPython re-initialises every call |
| [**TypeScript**](@/guests/typescript.md) | `@nassor/pcs-arrow-ipc` | `jco` 1.30.0, `typescript` 5.9.3 | Node 24.12+ (CI: 24) | The team is TypeScript native, and you can budget time for the gotchas |
| [**Kotlin**](@/guests/kotlin.md) | `io.github.nassor:pcs-arrow-ipc` | Kotlin 2.4.0, the `wit-bindgen` Kotlin fork, Gradle 8.14.4+ | JDK 21 (CI: Temurin 21.0.12), and a host on wasmtime 47.0.0+ for Wasm GC | The logic already sits in a Kotlin multiplatform module, and you can budget the extra componentization pass |
| [**C#**](@/guests/csharp.md) | `Pcs.ArrowIpc` | `componentize-dotnet` on .NET 10 | .NET SDK 10 (CI: 10.0.400) | The team is .NET native, and one `dotnet build` emitting a finished component is worth an experimental NuGet feed |

<div class="note">
<span class="note-label">If your language has no Arrow library</span>

Go, Python, TypeScript, Kotlin and C# have no WASI 0.2 friendly Arrow IPC
library today, so the five share one packaged codec,
[`pcs-arrow-ipc`](@/reference/arrow-ipc-packages.md). Rust does not need it,
because `pcs-guest` re-exports the Arrow crates and handles IPC.

[The wire format](@/reference/wire-format.md) specifies the bytes `run-batch`
receives and must return: segment framing, the flatbuffer field ids, buffer
layouts per type, and what a guest that can only overwrite fixed-width bytes in
place may and may not do. That specification is what a sixth language
implements.

</div>

Versions above are the ones CI installs. The full pin list, including the
toolchain caveats that cost an hour each, lives in `examples/polyglot/PINS.md`.
