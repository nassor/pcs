#!/usr/bin/env bash
#
# Build the six polyglot guest components into examples/polyglot/build/.
#
# One PCS workload, six languages, one WIT world. Each stage is a separate
# WebAssembly component exporting `pcs:pipeline@0.2.0`:
#
#   validate-go.wasm   Go          writes `valid`
#   enrich-py.wasm     Python      writes `usd_amount`
#   score-ts.wasm      TypeScript  writes `risk_score`, `flagged`
#   fee-kt.wasm        Kotlin      writes `fee`
#   tier-cs.wasm       C#          writes `review_tier`
#   settle-rs.wasm     Rust        writes `settlement`, keeps a cross-batch ledger
#
# Steps:
#   1. Regenerate examples/polyglot/generated/ from pcs_polyglot_order::Order.
#      This is the single source of the schema bytes and the fingerprint that
#      the five non-Rust guests embed as constants.
#   2. Copy the per-language generated constants into each stage directory.
#   3. Build each stage with its own toolchain.
#   4. Collect the six artifacts into examples/polyglot/build/.
#   5. Validate each one with wasm-tools and confirm it exports
#      pcs:pipeline@0.2.0.
#
# Toolchain versions are pinned in examples/polyglot/PINS.md. Each check below
# has its own exit code so a CI failure names the missing tool:
#
#   2 cargo-component   3 wasm-tools   4 Go   5 componentize-go
#   6 componentize-py   7 Node/npm   8 build produced no artifact
#   9 Gradle           10 wit-bindgen  11 dotnet   12 curl
#
# A contributor with only one toolchain can build one stage:
#
#   bash scripts/build-polyglot.sh --only=rust
#   bash scripts/build-polyglot.sh --only=go,ts
#   bash scripts/build-polyglot.sh --only=kotlin,csharp
#
# `--only` still runs step 1, because every stage depends on the generated
# constants being in sync with the Rust schema.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# Path to the canonical WIT package. The relative form is what the toolchains
# that run from inside a stage directory need; the absolute form is for the ones
# invoked from the repo root.
WIT_DIR_REL="../../../../crates/pcs-guest/wit"
WIT_DIR="${REPO_ROOT}/crates/pcs-guest/wit"
GENERATED="${REPO_ROOT}/examples/polyglot/generated"
BUILD_DIR="${REPO_ROOT}/examples/polyglot/build"
STAGES="${REPO_ROOT}/examples/polyglot/stages"

# Kotlin's toolchain emits a WASI preview 1 core module, so componentizing it
# needs the preview 1 adapter that ships with the wasmtime the workspace pins.
# Downloaded once into the gitignored generated/ directory.
WASMTIME_TAG="v47.0.3"
WASI_ADAPTER="${PCS_WASI_ADAPTER:-${GENERATED}/wasi_snapshot_preview1.reactor.wasm}"

ONLY="rust,go,python,ts,kotlin,csharp"
for arg in "$@"; do
    case "${arg}" in
        --only=*) ONLY="${arg#--only=}" ;;
        -h|--help)
            sed -n '2,39p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "[polyglot] ERROR: unknown argument '${arg}'" >&2
            echo "[polyglot] usage: bash scripts/build-polyglot.sh [--only=rust,go,python,ts,kotlin,csharp]" >&2
            exit 1
            ;;
    esac
done

wants() { [[ ",${ONLY}," == *",$1,"* ]]; }

log() { echo "[polyglot] $*"; }

require() {
    # require <command> <exit-code> <install hint>
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "[polyglot] ERROR: $1 not found on PATH" >&2
        echo "[polyglot] install with: $3" >&2
        echo "[polyglot] see examples/polyglot/PINS.md for the pinned versions" >&2
        exit "$2"
    fi
}

log "repo: ${REPO_ROOT}"
log "stages: ${ONLY}"

log "generating schema constants and fixtures from pcs_polyglot_order::Order..."
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit

for f in order_schema.ipc order_fingerprint.txt fixture_input.pcs fixture_input.json \
         schema_gen.go schema_gen.py schema_gen.ts SchemaGen.kt SchemaGen.cs; do
    if [[ ! -f "${GENERATED}/${f}" ]]; then
        echo "[polyglot] ERROR: emit did not produce ${GENERATED}/${f}" >&2
        exit 8
    fi
done
log "fingerprint: $(cat "${GENERATED}/order_fingerprint.txt")"

if wants rust; then
    require cargo-component 2 "cargo install cargo-component --locked --version 0.21.1"
    log "building Rust stage (settle)..."
    cargo component build --release -p polyglot-settle-wasm --target wasm32-wasip2
    # The artifact lands under wasm32-wasip1 even though the build targets
    # wasm32-wasip2: cargo-component compiles the core module for wasip1 and
    # adapts it into a component, keeping the pre-adapter directory name.
    RUST_ARTIFACT="${REPO_ROOT}/target/wasm32-wasip1/release/polyglot_settle_wasm.wasm"
fi

if wants go; then
    require go 4 "https://go.dev/dl/ (1.25.5 or newer)"
    require componentize-go 5 "go install github.com/bytecodealliance/componentize-go@v0.4.1"
    log "building Go stage (validate)..."
    cp "${GENERATED}/schema_gen.go" "${STAGES}/go-validate/export_pcs_pipeline_pipeline/schema_gen.go"
    (
        cd "${STAGES}/go-validate"
        # `bindings` rewrites go.mod to `module wit_component`. That module name
        # is dictated by componentize-go.
        componentize-go -d "${WIT_DIR_REL}" -w pcs-pipeline bindings --format
        # `bindings` regenerates go.mod from a fixed template, `module
        # wit_component` plus one require, and drops everything else. `build`
        # never touches the file, so the codec dependency is re-declared here.
        go mod edit \
            -require=github.com/nassor/pcs/packages/arrow-ipc-go@v0.0.0 \
            -replace=github.com/nassor/pcs/packages/arrow-ipc-go=../../../../packages/arrow-ipc-go
        componentize-go -d "${WIT_DIR_REL}" -w pcs-pipeline build -o validate-go.wasm
    )
    GO_ARTIFACT="${STAGES}/go-validate/validate-go.wasm"
fi

if wants python; then
    require componentize-py 6 "pip install componentize-py==0.25.0"
    log "building Python stage (enrich)..."
    cp "${GENERATED}/schema_gen.py" "${STAGES}/python-enrich/schema_gen.py"
    (
        cd "${STAGES}/python-enrich"
        # No `bindings` call here. Its output is type-checker stubs for editors:
        # `componentize` regenerates the real bindings itself and never reads
        # them off disk. It is also not idempotent, so a second run fails with
        # "Cannot create a file when that file already exists". See PINS.md for
        # generating the stubs by hand.
        #
        # No --stub-wasi: the bundled CPython needs the real WASI imports, which
        # the host supplies via wasmtime_wasi::p2::add_to_linker_sync.
        # `-p` names every directory componentize-py resolves imports from, and
        # it defaults to `.`, so naming the codec means naming both. Resolution
        # happens once, during the pre-init snapshot: the component has no
        # runtime filesystem, so the path cannot be supplied later.
        componentize-py -d "${WIT_DIR_REL}" -w pcs-pipeline componentize app \
            -p . -p "${REPO_ROOT}/packages/arrow-ipc-py/src" \
            -o enrich-py.wasm
    )
    PY_ARTIFACT="${STAGES}/python-enrich/enrich-py.wasm"
fi

if wants ts; then
    require node 7 "https://nodejs.org/ (24.12 or newer)"
    require npm 7 "ships with Node"
    log "building TypeScript stage (score)..."
    cp "${GENERATED}/schema_gen.ts" "${STAGES}/ts-score/schema_gen.ts"

    # The stage links the codec with `file:`, so `dist/` has to exist before
    # `npm ci` resolves it.
    (
        cd "${REPO_ROOT}/packages/arrow-ipc-ts"
        if [[ -f package-lock.json ]]; then npm ci; else npm install; fi
        npm run --silent build
    )
    (
        cd "${STAGES}/ts-score"
        if [[ -f package-lock.json ]]; then npm ci; else npm install; fi
        # jco writes the WIT world's TypeScript declarations that score.ts and
        # wit.d.ts type themselves against, then type-checks before emitting.
        # componentize itself never reads a type: it strips them.
        npm run --silent typecheck

        # `jco componentize` bundles a TypeScript entrypoint on its own, so
        # `--bundle` is implied: StarlingMonkey's loader cannot resolve the
        # `@nassor/pcs-arrow-ipc` import at wizer time.
        #
        # `--disable fetch-event` on top of `--disable http` is what drops the
        # `wasi:http/types` import. Without it the component fails to instantiate
        # against the host, which links WASI but not wasi:http: "component
        # imports instance `wasi:http/types@0.2.10`, but a matching
        # implementation was not found in the linker".
        #
        # Clocks, random and stdio stay enabled (the default): disabling clocks
        # makes Date.now() return garbage, and this stage reports timing in
        # run-metrics.
        npx jco componentize score.ts \
            --wit "${WIT_DIR_REL}" \
            --world-name pcs-pipeline \
            --disable http \
            --disable fetch-event \
            -o score-ts.wasm
    )
    TS_ARTIFACT="${STAGES}/ts-score/score-ts.wasm"
fi

if wants kotlin; then
    require gradle 9 "https://gradle.org/install/ (8.14.4 or newer, on JDK 21)"
    require wit-bindgen 10 \
        "cargo install wit-bindgen-cli --git https://github.com/Kotlin/wit-bindgen --branch kotlin"
    require wasm-tools 3 "cargo install wasm-tools --locked --version 1.246.2"
    if [[ ! -f "${WASI_ADAPTER}" ]]; then
        require curl 12 "ships with macOS, Windows and most Linux distributions"
        log "fetching the wasmtime ${WASMTIME_TAG} WASI preview 1 reactor adapter..."
        curl -sSfL -o "${WASI_ADAPTER}" \
            "https://github.com/bytecodealliance/wasmtime/releases/download/${WASMTIME_TAG}/wasi_snapshot_preview1.reactor.wasm"
    fi

    log "building Kotlin stage (fee)..."
    cp "${GENERATED}/SchemaGen.kt" \
        "${STAGES}/kotlin-fee/src/wasmWasiMain/kotlin/impl/SchemaGen.kt"

    # The stage resolves `io.github.nassor:pcs-arrow-ipc` from mavenLocal(), so
    # the codec has to be published there before Gradle configures the stage.
    (
        cd "${REPO_ROOT}/packages/arrow-ipc-kt"
        gradle --quiet --console=plain publishToMavenLocal
    )

    # Kotlin has no Gradle-native WIT step and no Gradle-native componentizer, so
    # three tools run around the compile:
    #   1. JetBrains' wit-bindgen fork writes the bindings. `--kotlin-imports`
    #      names the package the generated trampoline resolves `PipelineImpl`
    #      from, which is why the guest object lives in `impl`.
    #   2. Gradle produces a core wasm module, not a component.
    #   3. `component embed` attaches the world's component type and
    #      `component new` wraps the module, with the preview 1 adapter covering
    #      the clock and random imports the Kotlin runtime links against.
    wit-bindgen kotlin --kotlin-imports 'impl.*' "${WIT_DIR}" \
        --out-dir "${STAGES}/kotlin-fee/src/wasmWasiMain/kotlin/bindings"
    (
        cd "${STAGES}/kotlin-fee"
        gradle --quiet --console=plain compileProductionExecutableKotlinWasmWasiOptimize
    )
    KT_OUT="${STAGES}/kotlin-fee/build/compileSync/wasmWasi/main/productionExecutable/optimized"
    wasm-tools component embed "${WIT_DIR}" "${KT_OUT}/fee-kt.wasm" \
        -o "${KT_OUT}/fee-kt-embedded.wasm"
    wasm-tools component new "${KT_OUT}/fee-kt-embedded.wasm" \
        --adapt "wasi_snapshot_preview1=${WASI_ADAPTER}" \
        -o "${STAGES}/kotlin-fee/fee-kt.wasm"
    KT_ARTIFACT="${STAGES}/kotlin-fee/fee-kt.wasm"
fi

if wants csharp; then
    require dotnet 11 "https://dotnet.microsoft.com/download/dotnet/10.0 (SDK 10)"
    log "building C# stage (tier)..."
    cp "${GENERATED}/SchemaGen.cs" "${STAGES}/csharp-tier/SchemaGen.cs"
    (
        cd "${STAGES}/csharp-tier"
        # `dotnet build` is the whole story: componentize-dotnet publishes as
        # part of the build and the NativeAOT link step embeds the component
        # type, so the output is already a component. The first run downloads
        # wasi-sdk into ~/.wasi-sdk/, which is slow and about 535 MB.
        dotnet build -c Release --nologo
    )
    CS_ARTIFACT="${STAGES}/csharp-tier/bin/Release/net10.0/wasi-wasm/publish/tier-cs.wasm"
fi

mkdir -p "${BUILD_DIR}"

collect() {
    # collect <source> <dest-name>
    if [[ ! -f "$1" ]]; then
        echo "[polyglot] ERROR: expected $1 to exist after the build" >&2
        exit 8
    fi
    cp "$1" "${BUILD_DIR}/$2"
    log "collected $2 ($(wc -c <"${BUILD_DIR}/$2" | tr -d ' ') bytes)"
}

# `wants x && collect ...` would abort under `set -e` whenever the guard is
# false, because an AND-list's non-zero status is not one of the exemptions.
if wants go;     then collect "${GO_ARTIFACT}"   validate-go.wasm; fi
if wants python; then collect "${PY_ARTIFACT}"   enrich-py.wasm;   fi
if wants ts;     then collect "${TS_ARTIFACT}"   score-ts.wasm;    fi
if wants kotlin; then collect "${KT_ARTIFACT}"   fee-kt.wasm;      fi
if wants csharp; then collect "${CS_ARTIFACT}"   tier-cs.wasm;     fi
if wants rust;   then collect "${RUST_ARTIFACT}" settle-rs.wasm;   fi

require wasm-tools 3 "cargo install wasm-tools --locked --version 1.246.2"

for f in "${BUILD_DIR}"/*.wasm; do
    name="$(basename "${f}")"
    wasm-tools validate --features component-model "${f}"
    if ! wasm-tools component wit "${f}" | grep -q 'pcs:pipeline/pipeline@0.2.0'; then
        echo "[polyglot] ERROR: ${name} does not export pcs:pipeline/pipeline@0.2.0" >&2
        exit 8
    fi
    log "validated ${name}: exports pcs:pipeline/pipeline@0.2.0"
done

log "PASS: components in ${BUILD_DIR}"
log "next: cargo run -p pcs-service --features wasm,tracing --example polyglot_orders"
