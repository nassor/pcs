// Implements the `pcs:pipeline/pipeline` export for stage 4 of the polyglot
// example, the Kotlin guest.
//
// It reads `valid`, `region` and `usd_amount` and writes `fee`. It is the only
// stage that reads a `Utf8` column to drive a decision: the fee rate comes from
// the config key `fee_<region>`, so a region string in the data selects a value
// the host injected.
//
// The object name and the package are not a choice. `wit-bindgen kotlin` was
// run with `--kotlin-imports 'impl.*'`, so the generated export trampoline in
// `bindings/InternalPcsPipeline.kt` resolves `PipelineImpl` from this package
// and calls exactly `describe` and `runBatch`.
//
// # Why there is no Arrow dependency
//
// Writing `fee` means overwriting eight bytes per row in a fixed-width value
// buffer, so this stage mutates the input Arrow IPC bytes in place and hands the
// same buffer back. The codec is `io.github.nassor:pcs-arrow-ipc`, an ordinary
// Gradle dependency, which documents the format and what in-place mutation
// cannot do. Everything else in the stream, including the trailing `__alive`
// bitmap, passes through untouched.
//
// # Why nothing here throws past the boundary
//
// An exception escaping into the generated trampoline traps the instance, and
// the host then sees an opaque wasm trap instead of a reason. Every failure path
// is folded into `run-error::permanent`, which the WIT contract designates for
// bad input shape and guest bugs; `schema-mismatch` must never come out of
// `run-batch`.
//
// # Why `wall-ns` is zero
//
// Kotlin/Wasm's `wasmWasi` target reaches the outside world through WASI
// preview 1 imports, and `wasm-tools component new --adapt` routes those through
// the preview 1 adapter. Every such call traps inside the finished component:
// `kotlin.time.TimeSource.Monotonic`, `kotlin.random.Random` and `println` are
// all unusable here. A Kotlin guest may call exactly the imports the WIT world
// declares, which is `host-io` and nothing else, so this stage reports no
// timing. The Go, Python and TypeScript stages do report real values.

package impl

import bindings.HostIo
import bindings.Pipeline
import bindings.Types
import bindings.runtime.ComponentException
import io.github.nassor.pcs.arrowipc.PcsStream
import io.github.nassor.pcs.arrowipc.decodeBase64

private const val STAGE_NAME = "polyglot-fee-kt"
private const val STAGE_VERSION = "0.1.0"
private const val COMPONENT_NAME = "Order"
private const val LOG_TARGET = "polyglot::fee_kt"

private const val FIELD_VALID = "valid"
private const val FIELD_REGION = "region"
private const val FIELD_USD_AMOUNT = "usd_amount"
private const val FIELD_FEE = "fee"

/** Config keys are `fee_` plus the region string the row carries. */
private const val FEE_KEY_PREFIX = "fee_"

/**
 * The canonical `Order` schema-only Arrow IPC stream the host parses out of
 * `component-descriptor.arrow-schema-ipc`.
 *
 * Decoded once from the generated constant rather than on every `describe` call,
 * and shared, so callers must not mutate it.
 */
private val orderSchemaIpc: ByteArray by lazy(LazyThreadSafetyMode.NONE) {
    decodeBase64(ORDER_SCHEMA_IPC_BASE64)
}

@OptIn(ExperimentalUnsignedTypes::class)
object PipelineImpl : Pipeline {
    /**
     * Reports the stage identity and the one component it operates on.
     *
     * The schema bytes and the fingerprint are generated constants rather than
     * values computed here: encoding an Arrow schema flatbuffer would mean
     * shipping a writer, and the fingerprint is derived from the canonical Rust
     * `Order` definition. The driver and the integration test both fail loudly
     * if either constant drifts from that definition.
     *
     * `describe` has no error arm in the WIT world. A corrupt generated constant
     * is reported here and then surfaces as a load-time failure when the host
     * tries to parse an empty schema.
     */
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

    /**
     * Charges every valid row its region's fee rate.
     *
     * `prior` is ignored and `checkpoint` is null: this stage keeps no state
     * across batches, which is what `stateful: false` in [describe] promises the
     * host.
     */
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

            // One host call per distinct region rather than per row: get-config
            // crosses the component boundary and the region set is tiny.
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

            HostIo.metric("fee.charged_rows", charged.toDouble())
            HostIo.metric("fee.total_usd", total)
            HostIo.log(
                HostIo.LogLevel.INFO,
                LOG_TARGET,
                "$STAGE_NAME: charged $charged of ${batch.rows} rows " +
                    "across ${rates.size} regions, total fee $total",
            )

            val rows = batch.rows.toULong()
            return Result.success(
                Types.RunResult(
                    stream.toWit(),
                    null,
                    // wall-ns is 0: no clock is reachable from a Kotlin guest.
                    Types.RunMetrics(0uL, rows, rows, 1u, 0u),
                )
            )
        } catch (e: Throwable) {
            return failure(e.message ?: e.toString())
        }
    }
}

/**
 * Reads a host-injected fee rate.
 *
 * The WIT contract hands config over as strings and leaves numeric parsing to
 * the guest, so an absent or unparseable value is a misconfiguration worth
 * failing on rather than silently defaulting. An unknown region reaching this
 * point means the data and the config disagree, which no retry fixes.
 */
private fun feeRate(region: String): Double {
    val key = FEE_KEY_PREFIX + region
    val raw = HostIo.getConfig(key)
        ?: error("no config key \"$key\" for region \"$region\"")
    return raw.toDoubleOrNull() ?: error("config \"$key\" is \"$raw\", which is not a number")
}

/**
 * Logs and returns the permanent error arm.
 *
 * Nothing this stage can hit is worth a host retry: the same bytes and the same
 * config would fail again.
 */
private fun failure(message: String): Result<Types.RunResult> {
    HostIo.log(HostIo.LogLevel.ERROR, LOG_TARGET, "$STAGE_NAME: $message")
    return Result.failure(ComponentException(Types.RunError.Permanent(message)))
}

/**
 * Required by `binaries.executable()`, and never called.
 *
 * The host drives this component through the `pipeline` export, so the entry
 * point exists only to satisfy the Kotlin/Wasm executable link step.
 */
fun main() {
}
