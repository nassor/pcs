+++
title = "The Arrow codec packages"
description = "pcs-arrow-ipc for Go, Python, TypeScript, Kotlin and C#: one wire format, five standard-library-only decoders, install lines, the shared API, and what they refuse to write."
template = "page.html"
weight = 2
+++

# The Arrow codec packages

A WebAssembly guest has to decode the batch the host hands it. Five of the six
languages in [the polyglot example](@/guests/six-languages.md) have no Arrow
library that survives their componentizer, so they share one codec, published per
language as `pcs-arrow-ipc`.

Each is a decoder plus in-place setters, written against
[the wire format](@/reference/wire-format.md) and nothing else. No transitive
dependencies in any of the five.

## Coordinates

All five release in lockstep. One wire format, one version.

| Language | Coordinate | Import |
|---|---|---|
| Go | `github.com/nassor/pcs/packages/arrow-ipc-go` | `arrowipc` |
| Python | `pcs-arrow-ipc` | `pcs_arrow_ipc` |
| TypeScript | `@nassor/pcs-arrow-ipc` | `@nassor/pcs-arrow-ipc` |
| Kotlin | `io.github.nassor:pcs-arrow-ipc` | `io.github.nassor.pcs.arrowipc` |
| C# | `Pcs.ArrowIpc` | `Pcs.ArrowIpc` |

The Python wheel, the npm tarball, the NuGet package and a tarball of the Maven
repository are assets of the `arrow-ipc-v0.1.0` GitHub release. Go resolves
through the module proxy from the `packages/arrow-ipc-go/v0.1.0` tag, and Kotlin
resolves from the Maven repository this site serves.

## Install

Go. The import path's last element is `arrow-ipc-go`, so name the package
identifier:

```go
// go get github.com/nassor/pcs/packages/arrow-ipc-go@v0.1.0
import arrowipc "github.com/nassor/pcs/packages/arrow-ipc-go"
```

Python. `componentize-py` resolves imports once, during its pre-init snapshot,
from the directories named by `-p`, and the flag defaults to `.`:

```bash
pip install pcs_arrow_ipc-0.1.0-py3-none-any.whl
componentize-py -d <wit-dir> -w pcs-pipeline componentize app \
    -p . -p <site-packages> -o guest.wasm
```

TypeScript. `jco componentize` bundles with Rolldown under
`platform: "neutral"`, where `resolve.mainFields` is empty; the package ships an
`exports` map and compiled JavaScript so a bare specifier resolves anyway:

```bash
npm install @nassor/pcs-arrow-ipc
```

Kotlin, from the Maven repository served by this site:

```kotlin
repositories {
    maven("https://nassor.github.io/pcs/maven")
    mavenCentral()
}

dependencies {
    implementation("io.github.nassor:pcs-arrow-ipc:0.1.0")
}
```

C#. A plain `net10.0` assembly: only the component project carries
`RuntimeIdentifier=wasi-wasm`, `SelfContained` and `PublishTrimmed`, and the
codec uses no reflection, so it needs no trimmer root:

```bash
dotnet nuget add source <download-dir> -n pcs-local
dotnet add package Pcs.ArrowIpc --version 0.1.0
```

## API

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

`decodeBase64` is there because a guest embeds its component's Arrow schema as a
generated base64 constant. It means the guest imports one library, not two.

Int64 values are the language's widest integer: `bigint` in TypeScript, `Long` in
Kotlin, `long` in C#.

Malformed input is an error, never a trap. Go returns an `error`, Python raises
`ValueError`, TypeScript throws `Error`, Kotlin throws `ArrowIpcException` and C#
throws `ArrowIpcException`. A guest that traps gives the host an opaque wasm
failure instead of the `run-error::permanent` message it can report.

## What the codecs refuse

None of the five writes a flatbuffer, so each one refuses four things:

- **No `Utf8` write.** Changing a string resizes the values buffer and
  invalidates the offsets buffer and the RecordBatch flatbuffer that describes
  both. A guest that needs to write a string column needs a real Arrow writer,
  which is why the Rust stage owns `settlement` in the polyglot chain.
- **No dictionary batches.** A segment holds exactly one Schema message then one
  RecordBatch message. A DictionaryBatch in between is rejected during framing.
- **No compressed bodies.** `RecordBatch.compression` present is an error.
- **No validity writes.** A non-nullable field carries an all-ones validity
  bitmap from arrow-rs, and an in-place value write never has to touch it.

Everything a guest does not touch is returned byte-identical: the framing words,
both flatbuffers per segment, and the trailing `__alive` bitmap.

## Compatibility

The five target the byte layout of `arrow-ipc = "=59.2.0"`, which the PCS
workspace exact-pins as the host to guest wire format, and the
`pcs:pipeline@0.2.0` WIT world that carries it. A guest built against these
packages talks to a host built from the same pin.

Version 0.1.0 of all five decodes what
`cargo run -p pcs-service --features wasm --example polyglot_orders -- emit`
writes to `examples/polyglot/generated/fixture_input.pcs`. Each package's test
suite asserts exactly that, column by column, against the JSON the same command
emits.

## License

The `packages/` subtree is Apache-2.0. The engine crates are AGPL-3.0-only.

## Where to go next

- [The wire format](@/reference/wire-format.md): what a sixth language
  implements.
- [Six languages, one pipeline](@/guests/six-languages.md): the chain that
  consumes all five.
