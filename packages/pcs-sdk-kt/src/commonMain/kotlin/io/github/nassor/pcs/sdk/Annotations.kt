// The three annotations a Kotlin PCS processor is written with.
//
// Nothing reads them at run time. `packages/pcs-sdk-kt-ksp` reads them at compile
// time and generates the per-component accessor plus the `impl.PipelineImpl`
// export glue, because Kotlin/Wasm has no reflection at all: `kotlin-reflect` is
// JVM only, and even a property name is unavailable to a `wasmWasi` binary unless
// something wrote it into the program.
//
// They keep the default retention rather than `SOURCE`. KSP resolves annotations
// from a klib dependency by qualified name, which the spike behind this design
// verified with the default; there is nothing to gain from making that path any
// less like the one that was measured.

package io.github.nassor.pcs.sdk

/**
 * Marks the row type of the one component this processor operates on.
 *
 * The class must be a `data class` whose constructor takes every field. A `val`
 * property is an input the processor reads; a `var` property is an output it may
 * write. Declaration order is schema order, and schema order feeds both the
 * buffer walk and [pcsFingerprint], so reordering the properties is a wire
 * change.
 *
 * Property types map to Arrow as `Long` to `Int64`, `Double` to `Float64`,
 * `Boolean` to `Boolean` and `String` to `Utf8`. The wire field name is
 * [pcsWireName] of the property name.
 *
 * [version] lands in the segment's `__pcs_schema_version` metadata and in the
 * fingerprint.
 */
@Target(AnnotationTarget.CLASS)
annotation class PcsComponent(val version: Int = 1)

/**
 * Marks one transform: `fun name(row: R, config: PcsConfig)`, where `R` is the
 * [PcsComponent] row type.
 *
 * A transform mutates the row it is handed and returns nothing. Transforms run in
 * the order [PcsPipeline.of] registers them, each over every row of the batch
 * before the next one starts.
 */
@Target(AnnotationTarget.FUNCTION)
annotation class PcsTransform

/**
 * Marks the pipeline builder: `fun name(): PcsPipeline`.
 *
 * This is the anchor the generated export glue is built around. [name] and
 * [version] become `pipeline-descriptor.name` and `.version`; [logTarget] is the
 * `tracing` target the per-batch log line is bridged to, defaulting to [name].
 *
 * The generated `PipelineImpl` object is emitted into package `impl`, because
 * `wit-bindgen kotlin` is run with `--kotlin-imports 'impl.*'` and its export
 * trampoline resolves `PipelineImpl.describe()` and `PipelineImpl.runBatch()`
 * from there by those exact names. The annotated function must therefore live in
 * package `impl` too.
 */
@Target(AnnotationTarget.FUNCTION)
annotation class PcsProcessor(
    val name: String,
    val version: String,
    val logTarget: String = "",
)
