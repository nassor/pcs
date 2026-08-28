+++
title = "A Go processor"
description = "componentize-go, a row struct pcs-sdk-go reads the schema off, and the export package the bindings generator expects you to write."
template = "page.html"
weight = 4
aliases = ["/guests/go/"]
+++

# A Go processor

`componentize-go` is the Bytecode Alliance's current Go recommendation. Standard
Go, not TinyGo: the TinyGo component tooling page carries a "not currently being
maintained" banner pointing here.

Every block below is from `examples/polyglot/stages/go-validate/`, stage 1 of the
polyglot example. It reads `amount` and writes `valid`. The stage is one struct,
one transform function and two export wrappers. `pcs-sdk-go` reads the Arrow
schema and the schema fingerprint off the struct by reflection, decodes the
host's stream into `Order` values, runs the transform, and re-encodes.

## 1. Install

Requires **Go 1.25.5 or newer**; CI verifies 1.26.3.

```bash,name=Install componentize-go
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

```bash,name=Generate bindings then build the component
componentize-go -d ../../../../crates/pcs-processor/wit -w pcs-pipeline bindings --format
go mod edit \
    -require=github.com/nassor/pcs/packages/pcs-sdk-go@v0.0.0 \
    -replace=github.com/nassor/pcs/packages/pcs-sdk-go=../../../../packages/pcs-sdk-go
componentize-go -d ../../../../crates/pcs-processor/wit -w pcs-pipeline build -o validate-go.wasm
```

The `replace` directive points at this repository's `packages/`, so the SDK
resolves from source. The SDK's own module also carries the codec, as the
internal `arrowipc` subpackage.

<div class="note note-warn">
<span class="note-label">componentize-go owns go.mod</span>

`bindings` **rewrites `go.mod`** to `module wit_component` every time it runs,
so every intra-module import is `wit_component/<pkg>`. Commit `go.mod` with that
module name; `examples/polyglot/stages/go-validate/go.mod` does.

The rewrite is from a fixed template, one `require` and nothing else, so the SDK
dependency is dropped with everything else. That is why the `go mod edit` above
sits between `bindings` and `build`: `build` never touches the file.

</div>

## 3. The row type

The struct is the schema. Field order is wire order, and it is the order the
fingerprint hashes, so a reordering is a wire change:

```go,name=The row struct the schema comes from
// Order is the chain's row type: these twelve columns in this order are the
// cross-language contract every stage agrees on.
type Order struct {
    ID               int64   `pcs:"id"`
    Region           string  `pcs:"region"`
    Currency         string  `pcs:"currency"`
    Amount           float64 `pcs:"amount"`
    Valid            bool    `pcs:"valid"`
    UsdAmount        float64 `pcs:"usd_amount"`
    UsdAmountDisplay string  `pcs:"usd_amount_display"`
    RiskScore        float64 `pcs:"risk_score"`
    Flagged          bool    `pcs:"flagged"`
    Fee              float64 `pcs:"fee"`
    ReviewTier       int64   `pcs:"review_tier"`
    Settlement       string  `pcs:"settlement"`
}
```

The Go type name is the component name, so this is the host's `Order`. A `pcs`
tag names the column; without one the column is the lower snake case of the
field name, which is what `UsdAmountDisplay` already spells. This stage tags
every field anyway, because the names are shared with five other languages and a
Go rename must not silently move a column.

Four field kinds map to the wire format's four types: `int64` to `Int64`,
`float64` to `Float64`, `bool` to `Boolean`, `string` to `Utf8`. Anything else
panics, and so does an embedded or unexported field. Those are authoring
mistakes, and a stage that declares its processor as a package-level var hits
them when the component is derived rather than mid-batch.

## 4. The transform

A transform runs over one row and writes to it through a pointer, so a field
write is a column write. `pcs.New` takes them in the order they run, and
`Bind` attaches the host bindings:

{% raw %}
```go,name=The validate transform and the bound processor
var stage = pcs.New(stageName, stageVersion,
    pcs.Transform("validate", func(row *Order, cfg pcs.Config) error {
        minAmount, err := cfg.Float64(minAmountKey, minAmountDefault)
        if err != nil {
            return err
        }
        row.Valid = row.Amount > minAmount

        // Counted unconditionally, so a batch with nothing to reject still
        // reports a zero rather than dropping the series.
        invalid := 0.0
        if !row.Valid {
            invalid = 1
        }
        cfg.Count(invalidRowsMetric, invalid)
        return nil
    }),
).Bind(host{})
```
{% endraw %}

`pcs.Config` is the whole of the host a transform can reach. `Float64` returns
the fallback for an absent or blank key and an error for a value that will not
parse, because a misconfigured floor defaulting to zero is worse than a refused
batch. `Count` adds to a named counter and the processor reports one metric
observation per counter after the last system, so a per-row call costs one
addition rather than one host call.

Declaring `stage` as a package-level var is what makes `describe` a field read:
the schema derivation and the descriptor encoding happen once, at
instantiation.

## 5. The export package

`bindings` writes `wit_exports.go` plus one package per WIT interface, and
expects **you** to supply the export package. It imports
`export_pcs_pipeline_pipeline` by that exact path and calls exactly `Describe`
and `RunBatch`. Add `--generate-stubs` to have componentize-go write the two
panicking signatures the first time.

The full file is
`examples/polyglot/stages/go-validate/export_pcs_pipeline_pipeline/exports.go`.
Its imports name the SDK and the two generated packages:

```go,name=The export package imports
package export_pcs_pipeline_pipeline

import (
    witTypes "go.bytecodealliance.org/pkg/wit/types"

    pcs "github.com/nassor/pcs/packages/pcs-sdk-go"
    hostio "wit_component/pcs_pipeline_host_io"
    "wit_component/pcs_pipeline_types"
)
```

The SDK cannot import those bindings itself. `componentize-go bindings`
regenerates them into whichever stage module it runs in, always under the module
name `wit_component`, so the import path is not unique across stages. The stage
bridges them in three methods, which is also where the log target is named:

```go,name=The three host bindings
type host struct{}

func (host) GetConfig(key string) (string, bool) {
    value := hostio.GetConfig(key)
    if !value.IsSome() {
        return "", false
    }
    return value.Some(), true
}

func (host) Log(level pcs.LogLevel, message string) {
    hostio.Log(hostio.LogLevel(level), logTarget, message)
}

func (host) Metric(name string, value float64) { hostio.Metric(name, value) }
```

`Describe` copies the SDK's descriptor into the generated records:

```go,name=Describe copies the SDK descriptor
func Describe() pcs_pipeline_types.PipelineDescriptor {
    descriptor := stage.Describe()

    components := make([]pcs_pipeline_types.ComponentDescriptor, len(descriptor.Components))
    for i, c := range descriptor.Components {
        components[i] = pcs_pipeline_types.ComponentDescriptor{
            Name:           c.Name,
            ArrowSchemaIpc: c.ArrowSchemaIPC,
        }
    }
    return pcs_pipeline_types.PipelineDescriptor{
        Name:              descriptor.Name,
        Version:           descriptor.Version,
        Components:        components,
        Stateful:          descriptor.Stateful,
        SchemaFingerprint: descriptor.SchemaFingerprint,
    }
}
```

`RunBatch` takes the WIT `option<checkpoint>` as `witTypes.Option[[]uint8]` and
returns `witTypes.Result[RunResult, RunError]`. Every failure the SDK can refuse
comes back as an error, and the stage folds it into one arm:

```go,name=RunBatch folds every failure into one arm
func RunBatch(input []uint8, prior witTypes.Option[[]uint8]) witTypes.Result[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError] {
    _ = prior

    outcome, err := stage.RunBatch(input)
    if err != nil {
        message := err.Error()
        hostio.Log(hostio.LogLevelError, logTarget, stageName+": "+message)
        return witTypes.Err[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError](
            pcs_pipeline_types.MakeRunErrorPermanent(message),
        )
    }

    return witTypes.Ok[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError](pcs_pipeline_types.RunResult{
        Output:     outcome.Output,
        Checkpoint: witTypes.None[[]uint8](),
        Metrics: pcs_pipeline_types.RunMetrics{
            WallNs:     outcome.Metrics.WallNs,
            RowsIn:     outcome.Metrics.RowsIn,
            RowsOut:    outcome.Metrics.RowsOut,
            SystemsRun: outcome.Metrics.SystemsRun,
            Retries:    0,
        },
        Routes: witTypes.None[[]string](),
    })
}
```

`permanent` is the right arm here: the same bytes and the same config would fail
again, so a retry buys nothing. Nothing panics, because a Go panic inside a
component traps the instance and the host then sees an opaque wasm trap instead
of a reason. `Checkpoint` is none and `prior` is ignored, which is what
`stateful: false` in the descriptor promises.

## 6. The schema fingerprint

`pipeline-descriptor.schema-fingerprint` is derived, not embedded. The SDK hashes
the component name, the schema version as four little-endian bytes, then every
field name in declaration order, with FNV-1a, over the components sorted by
name. Names and versions only: adding a field changes the value, retyping one
does not.

Every language's SDK walks those same bytes, so the six polyglot stages report
one value from six independently written declarations. The driver
`examples/polyglot/polyglot_orders.rs` and the `polyglot_chain` integration test
load all six and compare their fingerprints against each other, and exit
non-zero on any disagreement.

## 7. Test, then validate

A transform is an ordinary function and a processor built without `Bind` drops
its logs and metrics, so both test on the host. The SDK's own suite and the
codec's now live in one module, so one command covers both:

```bash,name=Run the SDK test suite
cd packages/pcs-sdk-go && go test ./...
```

<div class="note note-warn">
<span class="note-label"><code>go test ./...</code> does not work in the stage</span>

The generated packages use `//go:wasmimport`, which does not compile for the
host target, so a bare `go test ./...` fails on them inside the stage module.
Host-side tests belong in a module of their own.

</div>

```bash,name=Validate the finished component
wasm-tools validate --features component-model validate-go.wasm
wasm-tools component wit validate-go.wasm | grep 'pcs:pipeline'
```

```text,name=Expected wasm-tools output
  import pcs:pipeline/host-io@0.3.0;
  export pcs:pipeline/pipeline@0.3.0;
```

## 8. Run it

`examples/configs/standalone_polyglot.kdl` runs a single processor under the
service. It names the Python stage, and the same config runs a Go processor by
swapping two things: the `wasm` node's `module` to point at `validate-go.wasm`,
and its `config` keys to the ones this stage reads. Everything else, the
`FileSource` and `FileSink` pair with `format="csv"` and the twelve
`schema_fields` entries, is a property of the `Order` component rather than the
language.

## Where to go next

- [The WIT contract](@/processors/wit-contract.md): every record the descriptor
  fills in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes the SDK encodes.
- [Six languages, one pipeline](@/processors/_index.md#six-languages-one-pipeline): this stage in its
  chain.
