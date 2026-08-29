+++
title = "The SDK packages"
description = "pcs-sdk for Go, Python, TypeScript, Kotlin and C#: one package per language, the codec inside each, install lines, the shared codec API, and what the codecs refuse to write."
template = "page.html"
weight = 2
+++

# The SDK packages

A WebAssembly processor has to decode the batch the host hands it. Five of the six
languages in [the polyglot example](@/processors/_index.md#six-languages-one-pipeline) have no Arrow
library that survives their componentizer, so each language's zero-ceremony
authoring SDK carries a standard-library-only codec internally: one package per
language resolves both the authoring API and the wire format. The codec keeps
its original package/module/namespace name inside its SDK.

Each codec is a decoder plus in-place setters, written against
[the wire format](@/reference/wire-format.md) and nothing else. No transitive
dependencies in any of the five packages.

## Coordinates

All five SDKs release in lockstep. One wire format, one version. The Kotlin KSP
symbol processor and the C# source generator are build-time companions packed
with their runtime; they are not additional runtime packages.

| Language | Coordinate | Codec import |
|---|---|---|
| Go | `github.com/nassor/pcs/packages/pcs-sdk-go` | subpackage `arrowipc` |
| Python | `pcs-sdk` | `pcs_sdk.arrow_ipc` |
| TypeScript | `@nassor/pcs-sdk` | internal `./arrow_ipc.ts`, re-exported |
| Kotlin | `io.github.nassor:pcs-sdk-kt` (+ KSP `pcs-sdk-kt-ksp`) | `io.github.nassor.pcs.arrowipc` |
| C# | `Pcs.Sdk` (+ generator `Pcs.Sdk.Generators`) | `Pcs.ArrowIpc` |

The Python wheel, the npm tarball, the NuGet package and a tarball of the Maven
repository are assets of the `sdk-v0.1.0` GitHub release. Go resolves through
the module proxy from the `packages/pcs-sdk-go/v0.1.0` tag, and Kotlin resolves
from the Maven repository this site serves.

## Install

Every install command below runs the same on Linux, macOS and Windows
(PowerShell).

Go. The import path's last element is `pcs-sdk-go`; the codec is the `arrowipc`
subpackage:

```go,name=Install the Go SDK
// go get github.com/nassor/pcs/packages/pcs-sdk-go@v0.1.0
import pcs "github.com/nassor/pcs/packages/pcs-sdk-go"
import arrowipc "github.com/nassor/pcs/packages/pcs-sdk-go/arrowipc"
```

Python. `componentize-py` resolves imports once, during its pre-init snapshot,
from the directories named by `-p`, and the flag defaults to `.`:

```bash,name=Install the Python SDK then build
pip install pcs_sdk-0.1.0-py3-none-any.whl
componentize-py -d <wit-dir> -w pcs-pipeline componentize app \
    -p . -p <site-packages> -o processor.wasm
```

TypeScript. `jco componentize` bundles with Rolldown under
`platform: "neutral"`, where `resolve.mainFields` is empty; the package ships an
`exports` map and compiled JavaScript so a bare specifier resolves anyway:

```bash,name=Install the TypeScript SDK
npm install @nassor/pcs-sdk
```

Kotlin, from the Maven repository served by this site, with the KSP processor
alongside the runtime:

```kotlin,name=Install the Kotlin SDK and the KSP processor
repositories {
    maven("https://nassor.github.io/pcs/maven")
    mavenCentral()
}

dependencies {
    implementation("io.github.nassor:pcs-sdk-kt:0.1.0")
    add("kspWasmWasi", "io.github.nassor:pcs-sdk-kt-ksp:0.1.0")
}
```

C#. A plain `net10.0` assembly: only the component project carries
`RuntimeIdentifier=wasi-wasm`, `SelfContained` and `PublishTrimmed`, and the
codec uses no reflection, so it needs no trimmer root:

```bash,name=Install the C# SDK
dotnet nuget add source <download-dir> -n pcs-local
dotnet add package Pcs.Sdk --version 0.1.0
```

## Codec API

One shape, five spellings. Parse takes ownership of a mutable copy of the input;
the setters write into it; the output accessor hands the same bytes back as
`run-result.output`.

| Operation | Go | Python | TypeScript | Kotlin | C# |
|---|---|---|---|---|---|
| Parse | `arrowipc.Parse(b)` | `PcsStream(b)` | `new PcsStream(b)` | `PcsStream.parse(b)` | `new PcsStream(b)` |
| Component lookup | `s.Component(n)` | `s.component(n)` | `s.component(n)` | `s.component(n)` | `s.Component(n)` |
| Row count | `b.Rows` | `b.rows` | `b.rows` | `b.rows` | `b.Rows` |
| Int64 column | `b.Int64s(f)` | `b.int64s(f)` | `b.int64s(f)` | `b.int64s(f)` | `b.Int64s(f)` |
| Float64 column | `b.Float64s(f)` | `b.float64s(f)` | `b.float64s(f)` | `b.float64s(f)` | `b.Float64s(f)` |
| Boolean column | `b.Bools(f)` | `b.bools(f)` | `b.bools(f)` | `b.bools(f)` | `b.Bools(f)` |
| Utf8 column | `b.Strings(f)` | `b.strings(f)` | `b.strings(f)` | `b.strings(f)` | `b.Strings(f)` |
| Int64 setter | `b.SetInt64(f,r,v)` | `b.set_int64(f,r,v)` | `b.setInt64(f,r,v)` | `b.setInt64(f,r,v)` | `b.SetInt64(f,r,v)` |
| Float64 setter | `b.SetFloat64(f,r,v)` | `b.set_float64(f,r,v)` | `b.setFloat64(f,r,v)` | `b.setFloat64(f,r,v)` | `b.SetFloat64(f,r,v)` |
| Boolean setter | `b.SetBool(f,r,v)` | `b.set_bool(f,r,v)` | `b.setBool(f,r,v)` | `b.setBool(f,r,v)` | `b.SetBool(f,r,v)` |
| Output bytes | `s.Buf` | `s.to_bytes()` | `s.toBytes()` | `s.toWit()` | `s.Buffer` |
| Base64 decode | `arrowipc.DecodeBase64(t)` | `decode_base64(t)` | `decodeBase64(t)` | `decodeBase64(t)` | `ArrowIpc.DecodeBase64(t)` |

`decodeBase64` is there because a processor embeds its component's Arrow
schema as a generated base64 constant. It means the processor imports one
package, not two.

Int64 values are the language's widest integer: `bigint` in TypeScript, `Long` in
Kotlin, `long` in C#.

Malformed input is an error, never a trap. Go returns an `error`, Python raises
`ValueError`, TypeScript throws `Error`, Kotlin throws `ArrowIpcException` and C#
throws `ArrowIpcException`. A processor that traps gives the host an opaque wasm
failure instead of the `run-error::permanent` message it can report.

## What the codecs refuse

None of the five codecs writes a flatbuffer, so each one refuses four things:

- **No `Utf8` write.** Changing a string resizes the values buffer and
  invalidates the offsets buffer and the RecordBatch flatbuffer that describes
  both. A processor that needs to write a string column needs a real Arrow writer,
  which is why the Rust stage owns `settlement` in the polyglot chain.
- **No dictionary batches.** A segment holds exactly one Schema message then one
  RecordBatch message. A DictionaryBatch in between is rejected during framing.
- **No compressed bodies.** `RecordBatch.compression` present is an error.
- **No validity writes.** A non-nullable field carries an all-ones validity
  bitmap from arrow-rs, and an in-place value write never has to touch it.

## Conformance corpus

`packages/arrow-ipc-conformance/` pins all five codecs to one answer about
which streams are valid. The `manifest.json` lists the cases; each `vectors/*.pcs`
holds one binary stream. A case is `accept` or `reject`: an accept case carries
the components, row count and column values a codec must read back, a reject
case a `reason`. The reason is the contract, because error text is local to
each language; a codec maps each reason to whatever substring its own message
uses. Every SDK suite runs the corpus, so a sixth implementation has an
acceptance suite the day it starts.

The corpus is generated from a real `Dataset::write_ipc` stream, each malformed
vector derived by editing those bytes in place, so no vector is a hand-forged
flatbuffer that could drift from what arrow-rs emits. Regenerate it after any
wire format or `Order` schema change:

```bash,name=Regenerate the conformance corpus
cargo run -p pcs-service --features conformance --example conformance_vectors -- emit
```

Two reference rules are host-side, not codec rules, so the corpus deliberately
has no vector for either: the `__alive` cross-check on a component's row count,
and 8-byte buffer alignment, which is a property the writer guarantees rather
than a rule a reader enforces.

## Compatibility

The five codecs target the byte layout of `arrow-ipc = "=59.2.0"`, which the PCS
workspace exact-pins as the host to processor wire format, and the
`pcs:pipeline@0.3.0` WIT world that carries it. A processor built against these
packages talks to a host built from the same pin.

Version 0.1.0 of all five decodes what
`cargo run -p pcs-service --features wasm --example polyglot_schema_emit -- emit`
writes to `examples/polyglot/generated/fixture_input.pcs`. Each package's test
suite asserts exactly that, column by column, against the JSON the same command
emits.

## License

The `packages/` subtree is Apache-2.0. The engine crates are AGPL-3.0-only.

## Where to go next

- [The wire format](@/reference/wire-format.md): what a sixth language
  implements.
- [Six languages, one pipeline](@/processors/_index.md#six-languages-one-pipeline): the chain that
  consumes all five packages.
