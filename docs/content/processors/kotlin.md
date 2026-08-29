+++
title = "A Kotlin processor"
description = "Kotlin 2.4.0's experimental Component Model support: three annotations a KSP processor turns into the export glue, the two wasm-tools passes Gradle does not run, and the Wasm GC proposals the host has to allow."
template = "page.html"
weight = 7
aliases = ["/guests/kotlin/"]
+++

# A Kotlin processor

Kotlin 2.4.0 is the first release with experimental Component Model support, and
two consequences shape everything below. WIT bindings come from JetBrains'
[`wit-bindgen` fork](https://github.com/Kotlin/wit-bindgen), not from Gradle.
And Gradle emits a core wasm module, so componentization is a separate
`wasm-tools` pass.

Every block below is from `examples/polyglot/stages/kotlin-fee/`, stage 4 of the
polyglot example. It reads `valid`, `region` and `usd_amount`, and writes `fee`.
It is the only stage that reads a `Utf8` column to drive a decision: the rate
comes from the config key `fee_<region>`, so a string in the data selects a
value the host injected.

The whole stage is a data class and two annotated functions. `pcs-sdk-kt-ksp`
reads the annotations at compile time and generates the row accessor and the
`pcs:pipeline/pipeline` export; `pcs-sdk-kt` is the runtime that export calls.

## 1. Install

Requires **JDK 21** and **Gradle 8.14.4 or newer**; CI verifies Temurin
21.0.12. Kotlin 2.4.0 itself needs no install: `build.gradle.kts` pins the
version and Gradle fetches the compiler. The bindings generator is a fork on its
own branch, and there is no released version to pin:

```bash,name=Install the bindings generator and wasm-tools
cargo install wit-bindgen-cli --git https://github.com/Kotlin/wit-bindgen --branch kotlin
cargo install wasm-tools --locked --version 1.246.2
```
Runs the same on Linux, macOS and Windows (PowerShell).

That puts `wit-bindgen` v0.57.1 on `PATH`. The last build step also needs
`wasi_snapshot_preview1.reactor.wasm` from the wasmtime v48.0.1 release assets.
`cargo xtask polyglot` downloads it into `examples/polyglot/generated/` when it
is absent, and `PCS_WASI_ADAPTER` overrides the path.

<div class="note note-warn">
<span class="note-label">The host must allow three Wasm proposals</span>

Kotlin/Wasm compiles to Wasm GC, so the component uses the gc,
exception-handling and function-references proposals. wasmtime enables all three
by default from 47.0.0, and PCS pins 48.0.1, so `pcs-service` loads this
component unchanged. An older host has to set `Config::wasm_gc`,
`wasm_exceptions` and `wasm_function_references` explicitly, or instantiation
fails on a validation error naming a GC type.

</div>

## 2. build.gradle.kts

Two dependencies and one KSP configuration:

```kotlin,name=build.gradle.kts with the two dependencies
plugins {
    kotlin("multiplatform") version "2.4.0"
    id("com.google.devtools.ksp") version "2.3.11"
}

repositories {
    mavenLocal()
    mavenCentral()
}

kotlin {
    wasmWasi {
        binaries.executable()
        nodejs()
    }

    sourceSets {
        val wasmWasiMain by getting {
            dependencies {
                implementation("io.github.nassor:pcs-sdk-kt:0.1.0")
            }
        }
    }
}

dependencies {
    add("kspWasmWasi", "io.github.nassor:pcs-sdk-kt-ksp:0.1.0")
}
```

The KSP configuration is `kspWasmWasi`: the name derives from the target, and a
KSP processor runs on the JVM-hosted compiler whatever the target is, so the
processor artifact is an ordinary JVM jar. KSP 2.3.11 is the newest release, and
KSP's versioning tracks the Kotlin version from 2.3.0 with no 2.4.x line, so
2.3.11 is what pairs with Kotlin 2.4.0.

`pcs-sdk-kt` also carries the codec, in the `io.github.nassor.pcs.arrowipc`
package, so one dependency resolves both. `mavenLocal()` is where both it and
the KSP processor come from, published by the first step of section 3. The
packages are also served as a Maven repository by this site:

```kotlin,name=Resolving the packages from this site
repositories {
    maven("https://nassor.github.io/pcs/maven")
    mavenCentral()
}
```

## 3. Generate bindings and build

Five build steps, after the two local publications an in-repo build needs.
`--kotlin-imports` names the package the generator will look for your
implementation in:

Linux/macOS:

```bash,name=Publish the SDK then generate bindings and build
for p in pcs-sdk-kt pcs-sdk-kt-ksp; do
  (cd ../../../../packages/$p && gradle publishToMavenLocal)
done
wit-bindgen kotlin --kotlin-imports 'impl.*' ../../../../crates/pcs-processor/wit \
  --out-dir src/wasmWasiMain/kotlin/bindings
gradle compileProductionExecutableKotlinWasmWasiOptimize
wasm-tools component embed ../../../../crates/pcs-processor/wit <core>.wasm -o <embedded>.wasm
wasm-tools component new <embedded>.wasm \
  --adapt wasi_snapshot_preview1=wasi_snapshot_preview1.reactor.wasm \
  -o fee-kt.wasm
```

Windows (PowerShell):

```powershell
foreach ($p in @("pcs-sdk-kt", "pcs-sdk-kt-ksp")) {
  Push-Location ..\..\..\..\packages\$p
  gradle publishToMavenLocal
  Pop-Location
}
wit-bindgen kotlin --kotlin-imports 'impl.*' ..\..\..\..\crates\pcs-processor\wit --out-dir src/wasmWasiMain/kotlin/bindings
gradle compileProductionExecutableKotlinWasmWasiOptimize
wasm-tools component embed ..\..\..\..\crates\pcs-processor\wit <core>.wasm -o <embedded>.wasm
wasm-tools component new <embedded>.wasm --adapt wasi_snapshot_preview1=wasi_snapshot_preview1.reactor.wasm -o fee-kt.wasm
```

The two publications run in that order, because each one resolves the previous
as a real artifact: Gradle module metadata plus the `wasmWasi` klib. Resolving
them that way is what proves the publications are consumable. `cargo xtask
polyglot` runs both before the build.

`wit-bindgen` writes `PcsPipeline.kt`, `InternalPcsPipeline.kt` and
`runtime/ComponentSupport.kt` under `bindings/`. They are gitignored.

<div class="note note-warn">
<span class="note-label">Gradle stops one step short</span>

`compileProductionExecutableKotlinWasmWasiOptimize` produces a **core
module**: no WIT metadata, no WASI adapter, not loadable by a component host.
The last two steps are what make it a component. `embed` attaches the world to
the module, and `new` links the reactor adapter so the processor's
`wasi_snapshot_preview1` imports resolve. `cargo xtask polyglot` runs all five
in order.

</div>

## 4. The row type

`@PcsComponent` marks the row type of the one component this processor operates
on. It must be a `data class` whose constructor takes every field, because the
generated `decode` calls that constructor:

```kotlin,name=The row type as an annotated data class
@PcsComponent
data class Order(
    val id: Long,
    val region: String,
    val currency: String,
    val amount: Double,
    var valid: Boolean = false,
    var usdAmount: Double = 0.0,
    var usdAmountDisplay: String = "",
    var riskScore: Double = 0.0,
    var flagged: Boolean = false,
    var fee: Double = 0.0,
    var reviewTier: Long = 0,
    var settlement: String = "",
)
```

Declaration order is schema order, and schema order feeds the buffer walk and the
fingerprint, so reordering the properties is a wire change. A `val` is an input
the processor reads; a `var` is an output it may write. Wire names are the
snake_case of the property names, so `usdAmount` is `usd_amount`.

`Long`, `Double`, `Boolean` and `String` map to `Int64`, `Float64`, `Boolean` and
`Utf8`. A nullable or any other type is a compile error from KSP, naming the
property.

## 5. The transform and the processor

`@PcsTransform` marks a function taking the row type and a `PcsConfig`. It
mutates the row it is handed and returns nothing:

{% raw %}
```kotlin,name=The fee transform
@PcsTransform
fun fee(row: Order, config: PcsConfig) {
    row.fee = if (row.valid) row.usdAmount * config.double("fee_${row.region}", 0.0) else 0.0
    if (row.valid) {
        config.metric("fee.charged_rows", 1.0)
        config.metric("fee.total_usd", row.fee)
    }
}
```
{% endraw %}

`PcsConfig` is the whole of `host-io` a transform can reach, and both of its
methods exist to keep the component boundary quiet. `double` memoises every key
it resolves, so a per-row lookup costs one `get-config` call per distinct region.
`metric` accumulates into a counter the runtime reports once when the batch ends.

An absent or unparseable rate folds into the default rather than failing the
batch. `get-config` hands values over as strings and gives the host no way to see
why a batch failed, so a misconfigured region charges nothing and shows up in
`fee.charged_rows`. Telling absent from unparseable means reading the raw
`get-config` string, which is `PcsHost.config`.

`@PcsProcessor` marks the builder. Its arguments are `pipeline-descriptor.name`,
`.version` and the `tracing` target the runtime's per-batch summary is bridged
to:

```kotlin,name=The processor builder
@PcsProcessor("polyglot-fee-kt", "0.1.0", "polyglot::fee_kt")
fun build(): PcsPipeline = PcsPipeline.of(::fee)
```

`PcsPipeline.of` takes the transforms in the order they run, each over every row
of the batch before the next one starts. `main()` exists only because
`binaries.executable()` requires an entry point; the host drives the component
through the `pipeline` export and never calls it.

## 6. What the build generates

The three annotations carry no behaviour. KSP reads them and emits two files:
`impl.OrderCodec`, the typed row accessor, and `impl.PipelineImpl`, the export
object. Generated rather than reflected because Kotlin/Wasm has no reflection at
all: `kotlin-reflect` is JVM only, so a property name exists in a `wasmWasi`
binary only if the build wrote it there. The accessor's `decode` and `encode` are
fully typed, so a batch of `n` rows costs one primitive array per column and no
per-cell boxing.

Package `impl` is not a choice. `wit-bindgen kotlin --kotlin-imports 'impl.*'`
generates a trampoline that resolves `PipelineImpl.describe()` and
`PipelineImpl.runBatch()` from that package by those exact names, so the
annotated declarations live there too, and KSP refuses a processor annotated
anywhere else.

`PipelineImpl` folds every failure into `run-error::permanent`, the arm the WIT
contract designates for bad input shape and processor bugs. Nothing throws past
that boundary: an exception reaching the trampoline traps the instance, and the
host then sees an opaque wasm trap instead of a reason. `schema-mismatch` must
never come out of `run-batch`.

<div class="note note-warn">
<span class="note-label">A Kotlin processor may call only the WIT imports</span>

`run-metrics.wall-ns` is 0 unconditionally, and it is the single most expensive
thing to discover here. Kotlin/Wasm's `wasmWasi` target reaches the outside world
through WASI preview 1, and `wasm-tools component new --adapt` routes those calls
through the adapter, where they trap inside the finished component. So
`kotlin.time.TimeSource.Monotonic`, `kotlin.random.Random` and `println` are all
unusable. This stage may call `host-io` and nothing else, which is why it reports
no timing. The Go, Python and TypeScript stages report real values.

</div>

The rest of the WIT mapping, which the generated glue is written against:

| WIT | Kotlin |
|-----|--------|
| `record` | class with `var` fields and a positional constructor |
| `variant` | sealed interface, one class per arm: `Types.RunError.Permanent` |
| `enum` | `enum class`, arms SHOUTY_SNAKE_CASE: `HostIo.LogLevel.INFO` |
| `option<T>` | nullable `T?` |
| `list<u8>` | boxed `List<UByte>` |
| `result<T, E>` | `kotlin.Result<T>`, `E` inside `ComponentException` |
| imported interface | companion object: `HostIo.getConfig(key)` |

`list<u8>` as a boxed `List<UByte>` is the one mapping with a price attached. A
large payload pays a conversion at the boundary, so the runtime moves to a
`ByteArray` on the way in and back with `asUByteArray().asList()` on the way out.

## 7. The schema fingerprint

`pipeline-descriptor.schema-fingerprint` is derived, not embedded. `pcsFingerprint`
hashes the component name, the version's four little-endian bytes, then every
field name in declaration order, with FNV-1a, and renders eight lowercase hex
digits. Names and versions only: adding a field changes the value, retyping one
does not.

Its input is the generated field list the same build derived the wire schema
from, so the two cannot drift. Every language's SDK walks those same bytes, so
the six polyglot stages report one value from six independently written
declarations. The driver `examples/polyglot/polyglot_orders.rs` and the
`polyglot_chain` integration test load all six and compare their fingerprints
against each other, and exit non-zero on any disagreement.

## 8. Test, then validate

The SDK runtime is one `commonMain` source set, so the source that runs inside
the component is byte for byte the source `jvmTest` exercises against real wire
bytes and a recording fake host. The codec sits in that same source set:

Linux/macOS:

```bash,name=Run the SDK test suite
cd packages/pcs-sdk-kt && gradle jvmTest
```

Windows (PowerShell):

```powershell
cd packages\pcs-sdk-kt; gradle jvmTest
```

```bash,name=Validate the finished component
wasm-tools validate --features component-model fee-kt.wasm
wasm-tools component wit fee-kt.wasm | grep 'pcs:pipeline'
```
Windows (PowerShell):

```powershell
wasm-tools validate --features component-model fee-kt.wasm
wasm-tools component wit fee-kt.wasm | Select-String 'pcs:pipeline'
```

```text,name=Expected wasm-tools output
  import pcs:pipeline/host-io@0.3.0;
  export pcs:pipeline/pipeline@0.3.0;
```

## 9. Run it

`examples/configs/standalone_polyglot.kdl` runs a single processor under the
service. It names the Python stage, and the same config runs this one by swapping
two things: the `wasm` node's `module` to point at `fee-kt.wasm`, and its
`config` keys to `fee_emea="0.012"`, `fee_apac="0.008"` and `fee_amer="0.010"`.
An unknown region charges zero rather than failing, so a missing key is visible
in `fee.charged_rows` and in the output column.

## Where to go next

- [The WIT contract](@/processors/wit-contract.md): every record the descriptor
  fills in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes the codec
  inside `pcs-sdk-kt` implements.
- [Six languages, one pipeline](@/processors/_index.md#six-languages-one-pipeline): this stage in its
  chain.
