+++
title = "A C# processor"
description = "componentize-dotnet on .NET 10: one dotnet build for a finished component, three attributes a source generator turns into the export, and the experimental NuGet feed the restore needs."
template = "page.html"
weight = 8
aliases = ["/guests/csharp/"]
+++

# A C# processor

`componentize-dotnet` is a Bytecode Alliance layer on the .NET SDK, not an
announced .NET 10 feature. It is the shortest toolchain of the six: `dotnet
build` runs wit-bindgen, compiles to wasm through NativeAOT LLVM, and links the
wasi-libc adapter, so the output is already a component. There is no
`wasm-tools component new` step.

Every block below is from `examples/polyglot/stages/csharp-tier/`, stage 5 of the
polyglot example. It reads `flagged` and `risk_score` and writes `review_tier`,
the escalation level the Rust stage turns into a settlement decision. The whole
stage is `TierStage.cs`: a row class, one transform method and one assembly
attribute. `Pcs.Sdk`'s source generator writes the export.

## 1. Install

Requires the **.NET 10 SDK**; CI verifies 10.0.400. There is no
`dotnet workload install` step. Everything else arrives through NuGet, which is
where the first surprise is.

<div class="note note-warn">
<span class="note-label">A nuget.config is mandatory</span>

`Microsoft.DotNet.ILCompiler.LLVM`, the AOT backend that targets `wasi-wasm`,
ships only on the `dotnet-experimental` feed. Without this file the restore of
`BytecodeAlliance.Componentize.DotNet.Wasm.SDK` resolves and its native
toolchain dependency does not, and the error names the missing package rather
than the missing feed. `examples/polyglot/stages/csharp-tier/nuget.config`:

```xml,name=nuget.config adds the experimental feed
<configuration>
  <packageSources>
    <clear />
    <add key="dotnet-experimental" value="https://pkgs.dev.azure.com/dnceng/public/_packaging/dotnet-experimental/nuget/v3/index.json" />
    <add key="nuget" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
```

`<clear />` keeps the restore independent of whatever feeds the machine has
configured globally.

</div>

The first build also downloads wasi-sdk 29.0 into `~/.wasi-sdk/`, about 535 MB
over the wire and 1.3 GB unpacked. Budget for it once per machine.

## 2. The project file

There is no bindings command. The `<Wit>` item points the SDK at the canonical
WIT package and the build generates from it:

```xml,name=The project file and its WIT item
<PropertyGroup>
  <OutputType>Library</OutputType>
  <TargetFramework>net10.0</TargetFramework>
  <RootNamespace>PolyglotTier</RootNamespace>
  <AssemblyName>tier-cs</AssemblyName>
  <RuntimeIdentifier>wasi-wasm</RuntimeIdentifier>
  <SelfContained>true</SelfContained>
  <PublishTrimmed>true</PublishTrimmed>
  <InvariantGlobalization>true</InvariantGlobalization>
</PropertyGroup>

<ItemGroup>
  <ProjectReference Include="../../../../packages/pcs-sdk-cs/Pcs.Sdk.csproj" />
  <ProjectReference Include="../../../../packages/pcs-sdk-cs/generator/Pcs.Sdk.Generators.csproj"
                    OutputItemType="Analyzer"
                    ReferenceOutputAssembly="false" />
  <PackageReference Include="BytecodeAlliance.Componentize.DotNet.Wasm.SDK" Version="0.8.0-preview00011" />
  <Wit Include="../../../../crates/pcs-processor/wit" World="pcs-pipeline" />
</ItemGroup>
```

`PublishAot` and `AllowUnsafeBlocks` come from the SDK's own props. Setting them
in the project would only be able to break them.

The SDK is a plain `net10.0` assembly that also carries the codec, in the
`Pcs.ArrowIpc` namespace. Only this project carries
`RuntimeIdentifier=wasi-wasm`, `SelfContained` and `PublishTrimmed`, and nothing
reflects, so the trimmer needs no root: the generator bakes every field accessor
and transform call into this compilation.

One detail of the references is worth copying: analyzers do not travel across a
`ProjectReference`, so an in-repo consumer names the generator itself; a
`PackageReference` to `Pcs.Sdk` carries it in `analyzers/dotnet/cs/` instead. The
generated export catches `ArrowIpcException` by name, which resolves from the
same `Pcs.Sdk` assembly.

<div class="note note-warn">
<span class="note-label">Name the host ILCompiler yourself</span>

The LLVM variant of the ILCompiler opts out of the SDK's
`ResolvedILCompilerPack` resolution, so nothing adds the compiler for the machine
you are building on. The publish then fails asking for it by name, for example
"Add a PackageReference for `runtime.win-x64.Microsoft.DotNet.ILCompiler.LLVM`".
Add it, versioned in lockstep with the backend, with the RID derived so one
project cross-compiles from Windows, Linux and macOS:

```xml,name=Naming the host ILCompiler by RID
<PropertyGroup>
  <IlcLlvmVersion>10.0.0-rc.1.26306.1</IlcLlvmVersion>
</PropertyGroup>

<ItemGroup>
  <PackageReference Include="runtime.$(NETCoreSdkPortableRuntimeIdentifier).Microsoft.DotNet.ILCompiler.LLVM" Version="$(IlcLlvmVersion)" />
</ItemGroup>
```

</div>

```bash,name=The whole build in one command
dotnet build -c Release
```

That is the whole build, and it is `build` rather than `publish`: the SDK hangs a
`Publish` target off `Build`. The finished component lands at
`bin/Release/net10.0/wasi-wasm/publish/tier-cs.wasm`. `wit-bindgen` 0.58.0 stamps
the generated files and arrives transitively, so the version to pin is the SDK's.

## 3. The row type

`[PcsComponent]` marks one component. The class name is the wire component name,
and its public settable properties are the columns, in declaration order:

```csharp,name=The row type and the assembly attribute
using Pcs.Sdk;

[assembly: PcsProcessor("polyglot-tier-cs", "0.1.0", LogTarget = "tier")]

namespace PolyglotTier
{
    [PcsComponent]
    public sealed class Order
    {
        public long Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public string Currency { get; set; } = string.Empty;
        public double Amount { get; set; }
        public bool Valid { get; set; }
        public double UsdAmount { get; set; }
        public string UsdAmountDisplay { get; set; } = string.Empty;
        public double RiskScore { get; set; }
        public bool Flagged { get; set; }
        public double Fee { get; set; }
        public long ReviewTier { get; set; }
        public string Settlement { get; set; } = string.Empty;
    }
}
```

Wire names are the snake_case of the property names, so `UsdAmountDisplay`
addresses `usd_amount_display`, and `[PcsField("name")]` overrides one. `long`,
`double`, `bool` and `string` are the wire format's four types; any other
property type is a compile error naming the property. The class needs a public
parameterless constructor and settable properties, because the SDK materialises
one instance per row.

`[assembly: PcsProcessor]` is required: the generator emits nothing without it.
Its arguments become `pipeline-descriptor.name` and `.version`, and `LogTarget`
is the `host-io::log` target the host bridges onto tracing, defaulting to the
name.

## 4. The transform

`[PcsTransform]` marks a static method taking `(TRow row)` or
`(TRow row, PcsConfig config)`. It mutates the row, and the SDK re-encodes every
column afterwards, so a transform may write any field, a string included:

```csharp,name=The tier transform
public static class TierStage
{
    private const string ReviewScoreKey = "review_score";
    private const double ReviewScoreDefault = 0.2;

    // Escalation levels, in the order the Rust stage reads them.
    private const long TierClear = 0;
    private const long TierReview = 1;
    private const long TierHold = 2;

    [PcsTransform]
    public static void Tier(Order row, PcsConfig config)
    {
        double reviewScore = config.GetDouble(ReviewScoreKey, ReviewScoreDefault);
        if (row.Flagged)
        {
            row.ReviewTier = TierHold;
            PcsHost.Count("tier.hold_rows");
        }
        else if (row.RiskScore >= reviewScore)
        {
            row.ReviewTier = TierReview;
            PcsHost.Count("tier.review_rows");
        }
        else
        {
            row.ReviewTier = TierClear;
        }
    }
}
```

`PcsConfig` reads the config the host injected through the `config` node inside
the service config's `wasm` node. `GetString`, `GetDouble`, `GetInt64` and
`GetBool` all take a fallback for an absent or blank key and throw
`PcsProcessorException` for a value that will not parse, because a
misconfiguration is worth failing the batch on. Lookups are memoised, so a
per-row read is one call across the component boundary per batch.

`PcsHost` is the other half: `Log`, `Metric` and `Count`. `Metric` observes once
per call, and `Count` accumulates into a batch-scoped counter the SDK reports as
one observation when the batch ends. A transform sees one row at a time, so a
per-row `Metric` call would report a batch of six rows as six observations of
one. `PcsHost` is bound only while `run-batch` is executing, so a call from
outside a batch fails loudly rather than writing into a stale channel.

Transforms run in `Order` order, then source declaration order, and each runs
over the whole batch before the next one starts, the way a pcs `System` does.

## 5. What the generator emits

componentize-dotnet runs wit-bindgen as an MSBuild target before `csc`, so its
output is already sitting in `obj/.../wit_bindgen/` as ordinary `<Compile>` items
when the generator runs in the same Roslyn pass. The generated file therefore
implements `IPipelineExports` directly, in the namespace and under the class name
wit-bindgen's C# backend dictates:
`PcsPipelineWorld.wit.Exports.pcs.pipeline.v0_3_0.PipelineExportsImpl`, with
`Describe` and `RunBatch` static, because `PipelineExportsInterop` calls both by
unqualified name. Neither name is a choice, and neither is a partial class you
have to declare.

What it bakes in is one component binding per component, holding a getter and a
setter delegate per property and one delegate per transform, all resolved at
compile time. Nothing in the emitted code or in `Pcs.Sdk` reads a `Type` or an
attribute at run time, which is what keeps the processor correct under
`PublishTrimmed` and NativeAOT-LLVM: `GetCustomAttributes` there is exactly the
pattern that silently loses members. An author's mistake is a compile error with
a location instead of a failure inside a wasm component.

The error arm is the mapping worth knowing before you write your first stage.
`result<T, E>` is not a return value here. A failure is a thrown
`WitException<E>`, and the generated glue converts only that exception into the
WIT error arm, so it catches everything else and rethrows it as one:

```csharp,name=How the generated glue reports a permanent error
throw new global::PcsPipelineWorld.WitException<ITypesImports.RunError>(
    ITypesImports.RunError.Permanent(reason), 0);
```

`permanent` is the arm the WIT contract designates for bad input shape and
processor bugs, and `schema-mismatch` must never come out of `run-batch`.
Anything that unwinds out of the component instead traps the instance, and the
host then sees an opaque wasm trap with no reason at all. `Describe` has no error
arm at all in the WIT world, so a failure there is logged and returns an empty
descriptor, which the host rejects with a readable reason.

The rest of the mapping:

| WIT | C# |
|-----|-----|
| `record` | `struct` with public fields and a positional constructor |
| `variant` | class with a `Tag` byte and `Tags` constants: `ITypesImports.RunError.Permanent(msg)` |
| `enum` | `enum`, arms SHOUTY_SNAKE_CASE: `IHostIoImports.LogLevel.ERROR` |
| `option<T>` | nullable: `byte[]?`, `string?` |
| `list<u8>` | `byte[]` |
| `result<T, E>` | return `T`, throw `WitException<E>` |
| imported interface | static methods on the interface: `IHostIoImports.GetConfig(key)` |

The processor is stateless: `prior` is ignored, `checkpoint` is null and
`describe` reports `stateful: false`, which is the contract those three make
together. Every segment the processor did not declare is forwarded untouched,
the `__alive` bitmap included, because the host replaces the whole partition
dataset with what `run-batch` returns.

## 6. The schema fingerprint

`pipeline-descriptor.schema-fingerprint` is derived, not embedded. `PcsRuntime`
hashes each component's name, its schema version as four little-endian bytes, and
its field names in schema order, with FNV-1a, over the components sorted by name.
Names only: adding a field changes the value, retyping one does not. All of that
is already in the bindings the generator baked, so the value cannot drift from
the schema the processor actually reads.

Every language's SDK walks those same bytes, so the six polyglot stages report
one value from six independently written declarations. The driver
`examples/polyglot/polyglot_orders.rs` and the `polyglot_chain` integration test
load all six and compare their fingerprints against each other, and exit
non-zero on any disagreement.

## 7. Test, then validate

The SDK is a host-targeted project with no wasi RID, so its test suite runs
under `dotnet test` without a wasm runtime. It drives a processor through stub
bindings, and the codec in the same assembly decodes the real
`examples/polyglot/generated/fixture_input.pcs`:

```bash,name=Run the SDK test suite
cd packages/pcs-sdk-cs && dotnet test tests
```

Then the two commands every recipe ends with:

```bash,name=Validate the finished component
wasm-tools validate --features component-model tier-cs.wasm
wasm-tools component wit tier-cs.wasm | grep 'pcs:pipeline'
```

```text,name=Expected wasm-tools output
  import pcs:pipeline/types@0.3.0;
  import pcs:pipeline/host-io@0.3.0;
  export pcs:pipeline/pipeline@0.3.0;
```

## 8. Run it

`examples/configs/standalone_polyglot.kdl` runs a single processor under the
service. It names the Python stage, and the same config runs this one by swapping
two things: the `wasm` node's `module` to point at `tier-cs.wasm`, and its
`config` keys to `review_score="0.2"`. That key is the risk score at or above
which an unflagged row still earns a look. It is the only config value this stage
reads, and it has a default, so an absent key runs rather than failing.

## Where to go next

- [The WIT contract](@/processors/wit-contract.md): every record the descriptor
  fills in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes `Pcs.ArrowIpc`
  implements.
- [Six languages, one pipeline](@/processors/_index.md#six-languages-one-pipeline): this stage in its
  chain.
