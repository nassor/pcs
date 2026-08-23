+++
title = "A C# guest"
description = "componentize-dotnet on .NET 10: one dotnet build for a finished component, the experimental NuGet feed it cannot restore without, and the generated namespace shape."
template = "page.html"
weight = 7
+++

# A C# guest

`componentize-dotnet` is a Bytecode Alliance layer on the .NET SDK, not an
announced .NET 10 feature. It is the shortest toolchain of the six: `dotnet
build` runs wit-bindgen, compiles to wasm through NativeAOT LLVM, and links the
wasi-libc adapter, so the output is already a component. There is no
`wasm-tools component new` step.

Every block below is from `examples/polyglot/stages/csharp-tier/`, stage 5 of the
polyglot example. It reads `flagged` and `risk_score` and writes `review_tier`,
the escalation level the Rust stage turns into a settlement decision.

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

```xml
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

## 2. Generate bindings and build

There is no bindings command. The `<Wit>` item points the SDK at the canonical
WIT package and the build generates from it:

```xml
<PropertyGroup>
  <OutputType>Library</OutputType>
  <TargetFramework>net10.0</TargetFramework>
  <AssemblyName>tier-cs</AssemblyName>
  <RuntimeIdentifier>wasi-wasm</RuntimeIdentifier>
  <SelfContained>true</SelfContained>
  <PublishTrimmed>true</PublishTrimmed>
  <InvariantGlobalization>true</InvariantGlobalization>
</PropertyGroup>

<ItemGroup>
  <ProjectReference Include="../../../../packages/arrow-ipc-cs/Pcs.ArrowIpc.csproj" />
  <PackageReference Include="BytecodeAlliance.Componentize.DotNet.Wasm.SDK" Version="0.8.0-preview00011" />
  <Wit Include="../../../../crates/pcs-guest/wit" World="pcs-pipeline" />
</ItemGroup>
```

`PublishAot` and `AllowUnsafeBlocks` come from the SDK's own props. Setting them
in the project would only be able to break them.

The referenced codec is a plain `net10.0` assembly. Only this project carries
`RuntimeIdentifier=wasi-wasm`, `SelfContained` and `PublishTrimmed`, and the codec
uses no reflection, so the trimmer needs no root for it.

<div class="note note-warn">
<span class="note-label">Name the host ILCompiler yourself</span>

The LLVM variant of the ILCompiler opts out of the SDK's
`ResolvedILCompilerPack` resolution, so nothing adds the compiler for the machine
you are building on. The publish then fails asking for it by name, for example
"Add a PackageReference for `runtime.win-x64.Microsoft.DotNet.ILCompiler.LLVM`".
Add it, versioned in lockstep with the backend, with the RID derived so one
project cross-compiles from Windows, Linux and macOS:

```xml
<PropertyGroup>
  <IlcLlvmVersion>10.0.0-rc.1.26306.1</IlcLlvmVersion>
</PropertyGroup>

<ItemGroup>
  <PackageReference Include="runtime.$(NETCoreSdkPortableRuntimeIdentifier).Microsoft.DotNet.ILCompiler.LLVM" Version="$(IlcLlvmVersion)" />
</ItemGroup>
```

</div>

```bash
dotnet build -c Release
```

That is the whole build, and it is `build` rather than `publish`: the SDK hangs a
`Publish` target off `Build`. The finished component lands at
`bin/Release/net10.0/wasi-wasm/publish/tier-cs.wasm`. `wit-bindgen` 0.58.0 stamps
the generated files and arrives transitively, so the version to pin is the SDK's.

## 3. The export class

The generated namespace carries a version segment, so the bindings for this
world live under `PcsPipelineWorld.wit.Exports.pcs.pipeline.v0_2_0` and
`PcsPipelineWorld.wit.Imports.pcs.pipeline.v0_2_0`. The shared `types` interface
lands under `wit.Imports` even though an exported function returns its records,
which is why `Describe` returns an `ITypesImports.PipelineDescriptor`.

Your class name and namespace are fixed. wit-bindgen emits
`PipelineExportsInterop`, which calls `PipelineExportsImpl.Describe` and
`PipelineExportsImpl.RunBatch` by unqualified name from inside the exports
namespace, so the implementation has to live there under that name. Both methods
are `static`.

The full file is `examples/polyglot/stages/csharp-tier/PipelineExports.cs`:

```csharp
using System.Diagnostics;
using System.Globalization;
using PcsPipelineWorld.wit.Imports.pcs.pipeline.v0_2_0;
using PolyglotTier;

namespace PcsPipelineWorld.wit.Exports.pcs.pipeline.v0_2_0;

public sealed class PipelineExportsImpl : IPipelineExports
```

`Describe` returns generated constants. Encoding an Arrow schema flatbuffer
would mean shipping a writer, and the fingerprint is derived from the canonical
Rust `Order` definition, so both are emitted at build time and embedded:

```csharp
    public static ITypesImports.PipelineDescriptor Describe()
    {
        byte[] schema;
        try
        {
            schema = ArrowIpc.DecodeBase64(SchemaGen.OrderSchemaIpcBase64);
        }
        catch (ArrowIpcException e)
        {
            // describe() has no error arm in the WIT world. A corrupt generated
            // constant is reported here and then surfaces as a load-time failure
            // when the host tries to parse the empty schema, which is a far better
            // diagnostic than trapping the instance.
            IHostIoImports.Log(IHostIoImports.LogLevel.ERROR, LogTarget, e.Message);
            schema = [];
        }

        return new ITypesImports.PipelineDescriptor(
            StageName,
            StageVersion,
            [new ITypesImports.ComponentDescriptor(ComponentName, schema)],
            stateful: false,
            SchemaGen.OrderFingerprint);
    }
```

`RunBatch` takes `list<u8>` as `byte[]` and `option<checkpoint>` as `byte[]?`,
and returns the success type directly. `review_tier` is an `Int64`, so the write
is eight bytes per row into a fixed-width value buffer:

```csharp
    public static ITypesImports.RunResult RunBatch(byte[] input, byte[]? prior)
    {
        _ = prior;
        long started = Stopwatch.GetTimestamp();

        string phase = "read config";
        try
        {
            double reviewScore = ConfigFloat(ReviewScoreKey, ReviewScoreDefault);

            phase = "parse input stream";
            PcsStream stream = new(input);

            phase = $"locate {ComponentName} batch";
            ArrowBatch batch = stream.Component(ComponentName);

            phase = $"read {ComponentName}.{FieldFlagged}";
            bool[] flagged = batch.Bools(FieldFlagged);

            phase = $"read {ComponentName}.{FieldRiskScore}";
            double[] risk = batch.Float64s(FieldRiskScore);

            phase = $"write {ComponentName}.{FieldReviewTier}";
            int review = 0;
            int hold = 0;
            for (int row = 0; row < batch.Rows; row++)
            {
                long tier;
                if (flagged[row])
                {
                    tier = TierHold;
                    hold++;
                }
                else if (risk[row] >= reviewScore)
                {
                    tier = TierReview;
                    review++;
                }
                else
                {
                    tier = TierSettle;
                }
                batch.SetInt64(FieldReviewTier, row, tier);
            }
```

`phase` is the one piece of bookkeeping worth the line. The codec's own messages
name the component, the field and the row, so the catch block only has to say
what the stage was trying to do.

The error arm is the mapping worth knowing before you write your first stage.
`result<T, E>` is not a return value here. A failure is a thrown
`WitException<E>`, and the generated glue converts only that exception into the
WIT error arm:

```csharp
    private static WitException<ITypesImports.RunError> Failure(string message)
    {
        IHostIoImports.Log(IHostIoImports.LogLevel.ERROR, LogTarget, $"{StageName}: {message}");
        return new WitException<ITypesImports.RunError>(ITypesImports.RunError.Permanent(message), 0);
    }
```

Callers write `throw Failure(...)` so the compiler still sees the path
terminating. Anything else that unwinds out of the component traps the instance,
and the host then sees an opaque wasm trap instead of a reason. That is why
`RunBatch` ends with a bare `catch (Exception e)` folding into the same permanent
arm.

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

## 4. The Arrow codec

[`Apache.Arrow`](https://www.nuget.org/packages/Apache.Arrow) is pure C# with no
native dependency, which makes it the most plausible of the six languages'
bindings to come through its componentizer intact. It is unverified here, so the
stage references `Pcs.ArrowIpc` instead: 903 lines against the base class library
alone, covering segment splitting, the flatbuffer reads for Schema and
RecordBatch, typed column readers, and in-place setters for fixed-width fields.

```bash
dotnet nuget add source <download-dir> -n pcs-local
dotnet add package Pcs.ArrowIpc --version 0.1.0
```

Alternatively, write your own against
[the wire format](@/reference/wire-format.md).

`stream.Buffer` returns the input array mutated, which is why this stage can
write `review_tier`, an `Int64`, and could not write a `Utf8` column.

## 5. Test, then validate

The codec's test project targets the host with no wasi RID, so it runs under
`dotnet test` without a wasm runtime. It lives with the codec:

```bash
cd packages/arrow-ipc-cs && dotnet test tests
```

Ten cases, decoding the real `examples/polyglot/generated/fixture_input.pcs`.
Then the two commands every recipe ends with:

```bash
wasm-tools validate --features component-model tier-cs.wasm
wasm-tools component wit tier-cs.wasm | grep 'pcs:pipeline'
```

```text
  import pcs:pipeline/types@0.2.0;
  import pcs:pipeline/host-io@0.2.0;
  export pcs:pipeline/pipeline@0.2.0;
```

## 6. Run it

`crates/pcs-service/examples/configs/standalone_polyglot.toml` runs a single
guest under the service. It names the Python stage, and the same config runs
this one by swapping two things: `[pipeline.wasm] module` to point at
`tier-cs.wasm`, and the `[pipeline.wasm.config]` keys to `review_score = "0.2"`.
That key is the risk score at or above which an unflagged row still earns a look.
It is the only config value this stage reads, and it has a default, so an absent
key runs rather than failing.

## Where to go next

- [The WIT contract](@/guests/wit-contract.md): every record `Describe` fills
  in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes `Pcs.ArrowIpc`
  implements.
- [Six languages, one pipeline](@/guests/six-languages.md): this stage in its
  chain.
