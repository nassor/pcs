+++
title = "Processors"
description = "Six toolchains against one WIT world: what each language needs, which Arrow codec it pulls in, and the two commands that prove the component you built is a processor."
template = "page.html"
weight = 2
+++

# Processors

One world, six recipes. Each page below builds a processor that lives in this
repository and that CI builds, so every command on it has run.

## Before you start

Point every toolchain at the same WIT package. It is the single canonical copy.
Do not vendor it.

```text,name=The one canonical WIT package
crates/pcs-processor/wit/pipeline.wit
```

You need `wasm-tools` once, regardless of language:

```bash,name=Install wasm-tools once
cargo install wasm-tools --locked --version 1.246.2
```

Runs the same on Linux, macOS and Windows (PowerShell).

All six recipes end with these two commands:

```bash,name=The two commands every recipe ends with
wasm-tools validate --features component-model <component>.wasm
wasm-tools component wit <component>.wasm | grep 'pcs:pipeline'
```

Windows (PowerShell):

```powershell
wasm-tools validate --features component-model <component>.wasm
wasm-tools component wit <component>.wasm | Select-String 'pcs:pipeline'
```

The second command must print a world importing `pcs:pipeline/host-io@0.3.0`
and exporting `pcs:pipeline/pipeline@0.3.0`. If it does not, stop. Nothing past
this point will work, and the fix is almost always the bindings step, not the
processor code.

## Choose your language

| Language | SDK (codec inside) | Toolchain | Verified runtime |
|---|---|---|---|
| [**Rust**](@/processors/rust.md) | No, `pcs-processor` provides it | `cargo build --target wasm32-wasip2` | Rust 1.95+ |
| [**Go**](@/processors/go.md) | `pcs-sdk-go` | `componentize-go` 0.4.1 | Go 1.25.5+ (CI: 1.26.3) |
| [**Python**](@/processors/python.md) | `pcs-sdk` | `componentize-py` 0.25.0 | Python 3.10+ (CI: 3.14) |
| [**TypeScript**](@/processors/typescript.md) | `@nassor/pcs-sdk` | `jco` 1.30.0, `typescript` 5.9.3 | Node 24.12+ (CI: 24) |
| [**Kotlin**](@/processors/kotlin.md) | `pcs-sdk-kt` | Kotlin 2.4.0, `wit-bindgen` fork, Gradle 8.14.4+ | JDK 21 (Temurin 21.0.12), wasmtime 47.0.0+ for Wasm GC |
| [**C#**](@/processors/csharp.md) | `Pcs.Sdk` | `componentize-dotnet` on .NET 10 | .NET SDK 10 (CI: 10.0.400) |

<div class="note">
<span class="note-label">If your language has no Arrow library</span>

Go, Python, TypeScript, Kotlin and C# have no WASI 0.2 friendly Arrow IPC
library today, so each language's SDK carries its codec internally:
[`the SDK packages`](@/reference/arrow-ipc-packages.md). Rust does not need it,
because `pcs-processor` re-exports the Arrow crates and handles IPC.

[The wire format](@/reference/wire-format.md) specifies the bytes `run-batch`
receives and must return: segment framing, the flatbuffer field ids, buffer
layouts per type, and what a processor that can only overwrite fixed-width
bytes in place may and may not do. That specification is what a sixth language
implements.

</div>

Versions above are the ones CI installs. The full pin list, including the
toolchain caveats that cost an hour each, lives in `examples/polyglot/PINS.md`.

Six of them at once, chained through one host, is
[six languages, one pipeline](@/processors/_index.md#six-languages-one-pipeline).
