+++
title = "A Go guest"
description = "componentize-go, the export package it expects you to write, and the packaged Arrow codec the stage depends on."
template = "page.html"
weight = 3
+++

# A Go guest

`componentize-go` is the Bytecode Alliance's current Go recommendation. Standard
Go, not TinyGo: the TinyGo component tooling page carries a "not currently being
maintained" banner pointing here.

Every block below is from `examples/polyglot/stages/go-validate/`, stage 1 of the
polyglot example. It reads `amount` and writes `valid`.

## 1. Install

Requires **Go 1.25.5 or newer**; CI verifies 1.26.3.

```bash
go install github.com/bytecodealliance/componentize-go@v0.4.1
```

<div class="note note-warn">
<span class="note-label">componentize-go on Windows</span>

`go install` puts a thin wrapper on `PATH` that downloads the real binary on
first use. It asks for `componentize-go-windows-amd64.tar.gz`, while the v0.4.1
release only publishes a `.zip`, so the wrapper 404s. Download the `.zip` from
the release page and put `componentize-go.exe` on `PATH` yourself; overwriting
the wrapper in `%GOPATH%\bin` is fine. Linux and macOS are unaffected.

</div>

## 2. Generate bindings and build

Global flags come **before** the subcommand:

```bash
componentize-go -d ../../../../crates/pcs-guest/wit -w pcs-pipeline bindings --format
go mod edit \
    -require=github.com/nassor/pcs/packages/arrow-ipc-go@v0.0.0 \
    -replace=github.com/nassor/pcs/packages/arrow-ipc-go=../../../../packages/arrow-ipc-go
componentize-go -d ../../../../crates/pcs-guest/wit -w pcs-pipeline build -o validate-go.wasm
```

<div class="note note-warn">
<span class="note-label">componentize-go owns go.mod</span>

`bindings` **rewrites `go.mod`** to `module wit_component` every time it runs,
so every intra-module import is `wit_component/<pkg>`. Commit `go.mod` with that
module name; `examples/polyglot/stages/go-validate/go.mod` does.

The rewrite is from a fixed template, one `require` and nothing else, so every
other dependency is dropped. That is why the `go mod edit` above sits between
`bindings` and `build`: `build` never touches the file.

</div>

## 3. The export package

`bindings` writes `wit_exports.go` plus one package per WIT interface, and
expects **you** to supply the export package. It imports
`export_pcs_pipeline_pipeline` by that exact path and calls exactly `Describe`
and `RunBatch`. Add `--generate-stubs` to have componentize-go write the two
panicking signatures the first time.

The full file is
`examples/polyglot/stages/go-validate/export_pcs_pipeline_pipeline/exports.go`.
Its imports name the three generated packages:

```go
package export_pcs_pipeline_pipeline

import (
    "fmt"
    "strconv"
    "strings"
    "time"

    witTypes "go.bytecodealliance.org/pkg/wit/types"

    arrowipc "github.com/nassor/pcs/packages/arrow-ipc-go"
    hostio "wit_component/pcs_pipeline_host_io"
    "wit_component/pcs_pipeline_types"
)
```

`Describe` returns generated constants. Encoding an Arrow schema flatbuffer would
mean shipping a writer, and the fingerprint is derived from the canonical Rust
`Order` definition, so both are emitted at build time into the export package
and embedded. `orderSchema` decodes the base64 constant once, at package init:

```go
func Describe() pcs_pipeline_types.PipelineDescriptor {
    schema, err := orderSchema()
    if err != nil {
        // describe() has no error arm in the WIT world. A corrupt generated
        // constant is reported here and then surfaces as a load-time failure
        // when the host tries to parse the empty schema.
        hostio.Log(hostio.LogLevelError, logTarget, err.Error())
    }
    return pcs_pipeline_types.PipelineDescriptor{
        Name:    stageName,
        Version: stageVersion,
        Components: []pcs_pipeline_types.ComponentDescriptor{{
            Name:           componentName,
            ArrowSchemaIpc: schema,
        }},
        Stateful:          false,
        SchemaFingerprint: OrderFingerprint,
    }
}
```

`RunBatch` takes the WIT `option<checkpoint>` as `witTypes.Option[[]uint8]` and
returns `witTypes.Result[RunResult, RunError]`:

```go
func RunBatch(input []uint8, prior witTypes.Option[[]uint8]) witTypes.Result[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError] {
    _ = prior
    started := time.Now()

    minAmount, err := configFloat(minAmountKey, minAmountDefault)
    if err != nil {
        return failure("%v", err)
    }

    stream, err := arrowipc.Parse(input)
    if err != nil {
        return failure("parse input stream: %v", err)
    }
    batch, err := stream.Component(componentName)
    if err != nil {
        return failure("locate %s batch: %v", componentName, err)
    }
    amounts, err := batch.Float64s(fieldAmount)
    if err != nil {
        return failure("read %s.%s: %v", componentName, fieldAmount, err)
    }

    invalid := 0
    for row, amount := range amounts {
        valid := amount > minAmount
        if !valid {
            invalid++
        }
        if err := batch.SetBool(fieldValid, row, valid); err != nil {
            return failure("write %s.%s row %d: %v", componentName, fieldValid, row, err)
        }
    }

    hostio.Metric("validate.invalid_rows", float64(invalid))
    hostio.Log(hostio.LogLevelInfo, logTarget, fmt.Sprintf(
        "%s: %d of %d rows valid at %s=%g",
        stageName, batch.Rows-invalid, batch.Rows, minAmountKey, minAmount,
    ))

    rows := uint64(batch.Rows)
    return witTypes.Ok[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError](pcs_pipeline_types.RunResult{
        Output:     stream.Buf,
        Checkpoint: witTypes.None[[]uint8](),
        Metrics: pcs_pipeline_types.RunMetrics{
            WallNs:     uint64(time.Since(started).Nanoseconds()),
            RowsIn:     rows,
            RowsOut:    rows,
            SystemsRun: 1,
            Retries:    0,
        },
    })
}
```

Every failure path goes through one helper, and none of them panic. A Go panic
inside a component traps the instance, and the host then sees an opaque wasm
trap instead of a reason:

```go
func failure(format string, args ...any) witTypes.Result[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError] {
    message := fmt.Sprintf(format, args...)
    hostio.Log(hostio.LogLevelError, logTarget, stageName+": "+message)
    return witTypes.Err[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError](
        pcs_pipeline_types.MakeRunErrorPermanent(message),
    )
}
```

`permanent` is the right arm here: the same bytes and the same config would fail
again, so a retry buys nothing.

## 4. The Arrow codec

Go has no WASI 0.2 friendly Arrow IPC library. Two options, and the example
takes the first:

1. **Add the package.** `go get github.com/nassor/pcs/packages/arrow-ipc-go@v0.1.0`
   pulls 862 lines using nothing outside the standard library: segment splitting,
   the flatbuffer reads for Schema and RecordBatch, typed column readers, and
   in-place setters for fixed-width fields.
2. **Write your own** against
   [the wire format](@/reference/wire-format.md), which specifies the framing,
   the field ids, and the buffer layout per type.

Either way the pattern is the same: parse, read the columns you need, overwrite
fixed-width value bytes in place, and hand the same buffer back. That is why
`RunResult.Output` above is `stream.Buf`, the input array mutated. It also fixes
the limit: writing a `Utf8` column changes the offsets buffer, the values buffer
and the RecordBatch flatbuffer, so a byte-mutating guest cannot do it.

## 5. Test, then validate

The codec's tests live with the codec, in its own module:

```bash
cd packages/arrow-ipc-go && go test ./...
```

<div class="note note-warn">
<span class="note-label"><code>go test ./...</code> does not work in the stage</span>

The generated packages use `//go:wasmimport`, which does not compile for the
host target, so a bare `go test ./...` fails on them inside the stage module.
Host-side tests belong in a module of their own.

</div>

```bash
wasm-tools validate --features component-model validate-go.wasm
wasm-tools component wit validate-go.wasm | grep 'pcs:pipeline'
```

```text
  import pcs:pipeline/host-io@0.2.0;
  export pcs:pipeline/pipeline@0.2.0;
```

## 6. Run it

`crates/pcs-service/examples/configs/standalone_polyglot.toml` runs a single
guest under the service. It names the Python stage, and the same config runs a
Go guest by swapping two things: `[pipeline.wasm] module` to point at
`validate-go.wasm`, and the `[pipeline.wasm.config]` keys to the ones this stage
reads. Everything else, the CSV source, the CSV sink and the eleven
`schema_fields` entries, is a property of the `Order` component rather than the
language.

## Where to go next

- [The WIT contract](@/guests/wit-contract.md): every record `Describe` fills
  in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes the codec
  implements.
- [Six languages, one pipeline](@/guests/six-languages.md): this stage in its
  chain.
