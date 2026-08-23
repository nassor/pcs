+++
title = "A Kotlin guest"
description = "Kotlin 2.4.0's experimental Component Model support: the JetBrains wit-bindgen fork, the two wasm-tools passes Gradle does not run, and the Wasm GC proposals the host has to allow."
template = "page.html"
weight = 6
+++

# A Kotlin guest

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

## 1. Install

Requires **JDK 21** and **Gradle 8.14.4 or newer**; CI verifies Temurin
21.0.12. Kotlin 2.4.0 itself needs no install: `build.gradle.kts` pins the
version and Gradle fetches the compiler. The bindings generator is a fork on its
own branch, and there is no released version to pin:

```bash
cargo install wit-bindgen-cli --git https://github.com/Kotlin/wit-bindgen --branch kotlin
cargo install wasm-tools --locked --version 1.246.2
```

That puts `wit-bindgen` v0.57.1 on `PATH`. The last build step also needs
`wasi_snapshot_preview1.reactor.wasm` from the wasmtime v47.0.3 release assets.
`scripts/build-polyglot.sh` downloads it into `examples/polyglot/generated/` when
it is absent, and `PCS_WASI_ADAPTER` overrides the path.

<div class="note note-warn">
<span class="note-label">The host must allow three Wasm proposals</span>

Kotlin/Wasm compiles to Wasm GC, so the component uses the gc,
exception-handling and function-references proposals. wasmtime enables all three
by default from 47.0.0, and PCS pins 47.0.3, so `pcs-service` loads this
component unchanged. An older host has to set `Config::wasm_gc`,
`wasm_exceptions` and `wasm_function_references` explicitly, or instantiation
fails on a validation error naming a GC type.

</div>

## 2. Generate bindings and build

Five steps. `--kotlin-imports` names the package the generator will look for your
implementation in:

```bash
(cd ../../../../packages/arrow-ipc-kt && gradle publishToMavenLocal)
wit-bindgen kotlin --kotlin-imports 'impl.*' ../../../../crates/pcs-guest/wit \
  --out-dir src/wasmWasiMain/kotlin/bindings
gradle compileProductionExecutableKotlinWasmWasiOptimize
wasm-tools component embed ../../../../crates/pcs-guest/wit <core>.wasm -o <embedded>.wasm
wasm-tools component new <embedded>.wasm \
  --adapt wasi_snapshot_preview1=wasi_snapshot_preview1.reactor.wasm \
  -o fee-kt.wasm
```

Step 1 is only for an in-repo build: the stage resolves the codec from
`mavenLocal()`, so the package publishes there first. A release install resolves
from the Pages Maven repository instead, as in section 4.

Step 2 writes `PcsPipeline.kt`, `InternalPcsPipeline.kt` and
`runtime/ComponentSupport.kt` under `bindings/`. They are gitignored.

<div class="note note-warn">
<span class="note-label">Gradle stops one step short</span>

`compileProductionExecutableKotlinWasmWasiOptimize` produces a **core module**:
no WIT metadata, no WASI adapter, not loadable by a component host. The last two
steps are what make it a component. `embed` attaches the world to the module, and
`new` links the reactor adapter so the guest's `wasi_snapshot_preview1` imports
resolve. `scripts/build-polyglot.sh` runs all five in order.

</div>

## 3. The export object

The generated `bindings` package declares three interfaces: `HostIo` for the
imports, `Types` for the shared records, and `Pipeline` for the export. You
supply one object implementing `Pipeline`, and its name is fixed. The generated
trampoline in `InternalPcsPipeline.kt` resolves `PipelineImpl` from the package
`--kotlin-imports` named, and calls exactly `describe` and `runBatch`.

The full file is
`examples/polyglot/stages/kotlin-fee/src/wasmWasiMain/kotlin/impl/Main.kt`:

```kotlin
package impl

import bindings.HostIo
import bindings.Pipeline
import bindings.Types
import bindings.runtime.ComponentException
import io.github.nassor.pcs.arrowipc.PcsStream
import io.github.nassor.pcs.arrowipc.decodeBase64
```

`describe` returns generated constants. Encoding an Arrow schema flatbuffer
would mean shipping a writer, and the fingerprint is derived from the canonical
Rust `Order` definition, so both are emitted at build time into the `impl`
package and embedded. `orderSchemaIpc` decodes the base64 constant on first use:

```kotlin
@OptIn(ExperimentalUnsignedTypes::class)
object PipelineImpl : Pipeline {
    override fun describe(): Types.PipelineDescriptor {
        val schema = try {
            orderSchemaIpc.asUByteArray().asList()
        } catch (e: Throwable) {
            HostIo.log(
                HostIo.LogLevel.ERROR,
                LOG_TARGET,
                "decode the embedded Order schema: ${e.message}",
            )
            emptyList()
        }
        return Types.PipelineDescriptor(
            STAGE_NAME,
            STAGE_VERSION,
            listOf(Types.ComponentDescriptor(COMPONENT_NAME, schema)),
            false,
            ORDER_FINGERPRINT,
        )
    }
```

`runBatch` takes the WIT `list<u8>` as `List<UByte>` and `option<checkpoint>` as
a nullable, and returns `kotlin.Result`. One `get-config` call per distinct
region rather than per row, because every call crosses the component boundary:

```kotlin
    override fun runBatch(
        input: List<UByte>,
        prior: List<UByte>?,
    ): Result<Types.RunResult> {
        try {
            val stream = PcsStream.parse(input)
            val batch = stream.component(COMPONENT_NAME)
            val valid = batch.bools(FIELD_VALID)
            val regions = batch.strings(FIELD_REGION)
            val usdAmounts = batch.float64s(FIELD_USD_AMOUNT)

            val rates = HashMap<String, Double>()
            var charged = 0
            var total = 0.0
            for (row in 0 until batch.rows) {
                var fee = 0.0
                if (valid[row]) {
                    val region = regions[row]
                    val rate = rates.getOrPut(region) { feeRate(region) }
                    fee = usdAmounts[row] * rate
                    charged++
                    total += fee
                }
                batch.setFloat64(FIELD_FEE, row, fee)
            }

            val rows = batch.rows.toULong()
            return Result.success(
                Types.RunResult(
                    stream.toWit(),
                    null,
                    Types.RunMetrics(0uL, rows, rows, 1u, 0u),
                )
            )
        } catch (e: Throwable) {
            return failure(e.message ?: e.toString())
        }
    }
```

<div class="note note-warn">
<span class="note-label">A Kotlin guest may call only the WIT imports</span>

That `0uL` is `run-metrics.wall-ns`, and it is the single most expensive thing to
discover here. Kotlin/Wasm's `wasmWasi` target reaches the outside world through
WASI preview 1, and `wasm-tools component new --adapt` routes those calls through
the adapter, where they trap inside the finished component. So
`kotlin.time.TimeSource.Monotonic`, `kotlin.random.Random` and `println` are all
unusable. This stage may call `host-io` and nothing else, which is why it reports
no timing. The Go, Python and TypeScript stages report real values.

</div>

Every failure path goes through one helper, and none of them throws past the
boundary. An exception reaching the generated trampoline traps the instance, and
the host then sees an opaque wasm trap instead of a reason:

```kotlin
private fun failure(message: String): Result<Types.RunResult> {
    HostIo.log(HostIo.LogLevel.ERROR, LOG_TARGET, "$STAGE_NAME: $message")
    return Result.failure(ComponentException(Types.RunError.Permanent(message)))
}
```

That is also the whole error protocol: `result<T, E>` becomes
`kotlin.Result<T>`, and the WIT error payload travels inside
`ComponentSupport`'s `ComponentException`. The rest of the mapping:

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
large payload pays a conversion at the boundary, so the codec moves to a
`UByteArray` on the way in and back with `asList()` on the way out.

`main()` exists only because `binaries.executable()` requires an entry point.
The host drives the component through the `pipeline` export and never calls it.

## 4. The Arrow codec

Kotlin's Arrow route is JVM only. [`arrow-java`](https://github.com/apache/arrow-java)
resolves from a multiplatform `jvm` target and not from `wasmWasi`, so the stage
depends on `io.github.nassor:pcs-arrow-ipc` instead: 690 lines of standard
library Kotlin covering segment splitting, the flatbuffer reads for Schema and
RecordBatch, typed column readers, and in-place setters for fixed-width fields.

```kotlin
repositories {
    maven("https://nassor.github.io/pcs/maven")
    mavenCentral()
}

kotlin {
    sourceSets {
        val wasmWasiMain by getting {
            dependencies {
                implementation("io.github.nassor:pcs-arrow-ipc:0.1.0")
            }
        }
    }
}
```

The whole codec is in the package's `commonMain`, so one source compiles for
`wasmWasi` and for the JVM. `gradle jvmTest` in the package therefore exercises
the bytes the component runs. Alternatively, write your own against
[the wire format](@/reference/wire-format.md).

`stream.toWit()` returns the input buffer mutated, which is why this stage can
write `fee`, a `Float64`, and could not write a `Utf8` column.

## 5. Test, then validate

The codec test runs on the JVM against the real generated fixture, in the
package:

```bash
cd packages/arrow-ipc-kt && gradle jvmTest
```

```bash
wasm-tools validate --features component-model fee-kt.wasm
wasm-tools component wit fee-kt.wasm | grep 'pcs:pipeline'
```

```text
  import pcs:pipeline/host-io@0.2.0;
  export pcs:pipeline/pipeline@0.2.0;
```

## 6. Run it

`crates/pcs-service/examples/configs/standalone_polyglot.toml` runs a single
guest under the service. It names the Python stage, and the same config runs
this one by swapping two things: `[pipeline.wasm] module` to point at
`fee-kt.wasm`, and the `[pipeline.wasm.config]` keys to `fee_emea = "0.012"`,
`fee_apac = "0.008"` and `fee_amer = "0.010"`. An unknown region fails the batch
rather than defaulting, so the config has to cover every region in the data.

## Where to go next

- [The WIT contract](@/guests/wit-contract.md): every record `describe` fills
  in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes
  `pcs-arrow-ipc` implements.
- [Six languages, one pipeline](@/guests/six-languages.md): this stage in its
  chain.
