#!/usr/bin/env bash
#
# Build the four polyglot guest components into examples/polyglot/build/.
#
# One PCS workload, four languages, one WIT world. Each stage is a separate
# WebAssembly component exporting `pcs:pipeline@0.2.0`:
#
#   validate-go.wasm   Go          writes `valid`
#   enrich-py.wasm     Python      writes `usd_amount`
#   score-js.wasm      JavaScript  writes `risk_score`, `flagged`
#   settle-rs.wasm     Rust        writes `settlement`, keeps a cross-batch ledger
#
# Steps:
#   1. Regenerate examples/polyglot/generated/ from pcs_polyglot_order::Order.
#      This is the single source of the schema bytes and the fingerprint that
#      the three non-Rust guests embed as constants.
#   2. Copy the per-language generated constants into each stage directory.
#   3. Build each stage with its own toolchain.
#   4. Collect the four artifacts into examples/polyglot/build/.
#   5. Validate each one with wasm-tools and confirm it really does export
#      pcs:pipeline@0.2.0.
#
# Toolchain versions are pinned in examples/polyglot/PINS.md. Each check below
# has its own exit code so a CI failure names the missing tool:
#
#   2 cargo-component   3 wasm-tools   4 Go   5 componentize-go
#   6 componentize-py   7 Node/npm     8 build produced no artifact
#
# A contributor with only one toolchain can build one stage:
#
#   bash scripts/build-polyglot.sh --only=rust
#   bash scripts/build-polyglot.sh --only=go,js
#
# `--only` still runs step 1, because every stage depends on the generated
# constants being in sync with the Rust schema.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

WIT_DIR_REL="../../../../crates/pcs-guest/wit"
GENERATED="${REPO_ROOT}/examples/polyglot/generated"
BUILD_DIR="${REPO_ROOT}/examples/polyglot/build"
STAGES="${REPO_ROOT}/examples/polyglot/stages"

ONLY="rust,go,python,js"
for arg in "$@"; do
    case "${arg}" in
        --only=*) ONLY="${arg#--only=}" ;;
        -h|--help)
            sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "[polyglot] ERROR: unknown argument '${arg}'" >&2
            echo "[polyglot] usage: bash scripts/build-polyglot.sh [--only=rust,go,python,js]" >&2
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

# ---------------------------------------------------------------------------
# Step 1 — regenerate the canonical constants and fixtures.
# ---------------------------------------------------------------------------
log "generating schema constants and fixtures from pcs_polyglot_order::Order..."
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit

for f in order_schema.ipc order_fingerprint.txt fixture_input.pcs fixture_input.json \
         schema_gen.go schema_gen.py schema_gen.js; do
    if [[ ! -f "${GENERATED}/${f}" ]]; then
        echo "[polyglot] ERROR: emit did not produce ${GENERATED}/${f}" >&2
        exit 8
    fi
done
log "fingerprint: $(cat "${GENERATED}/order_fingerprint.txt")"

# ---------------------------------------------------------------------------
# Step 2 — Rust stage.
# ---------------------------------------------------------------------------
if wants rust; then
    require cargo-component 2 "cargo install cargo-component --locked --version 0.21.1"
    log "building Rust stage (settle)..."
    cargo component build --release -p polyglot-settle-wasm --target wasm32-wasip2
    # The artifact lands under wasm32-wasip1 even though the build targets
    # wasm32-wasip2: cargo-component compiles the core module for wasip1 and
    # adapts it into a component, keeping the pre-adapter directory name.
    RUST_ARTIFACT="${REPO_ROOT}/target/wasm32-wasip1/release/polyglot_settle_wasm.wasm"
fi

# ---------------------------------------------------------------------------
# Step 3 — Go stage.
# ---------------------------------------------------------------------------
if wants go; then
    require go 4 "https://go.dev/dl/ (1.25.5 or newer)"
    require componentize-go 5 "go install github.com/bytecodealliance/componentize-go@v0.4.1"
    log "building Go stage (validate)..."
    cp "${GENERATED}/schema_gen.go" "${STAGES}/go-validate/arrowipc/schema_gen.go"
    (
        cd "${STAGES}/go-validate"
        # `bindings` rewrites go.mod to `module wit_component` — that module
        # name is dictated by componentize-go, not chosen by us.
        componentize-go -d "${WIT_DIR_REL}" -w pcs-pipeline bindings --format
        componentize-go -d "${WIT_DIR_REL}" -w pcs-pipeline build -o validate-go.wasm
    )
    GO_ARTIFACT="${STAGES}/go-validate/validate-go.wasm"
fi

# ---------------------------------------------------------------------------
# Step 4 — Python stage.
# ---------------------------------------------------------------------------
if wants python; then
    require componentize-py 6 "pip install componentize-py==0.25.0"
    log "building Python stage (enrich)..."
    cp "${GENERATED}/schema_gen.py" "${STAGES}/python-enrich/schema_gen.py"
    (
        cd "${STAGES}/python-enrich"
        # No `bindings` call here on purpose. Its output is type-checker stubs
        # for your editor: `componentize` regenerates the real bindings itself
        # and never reads them off disk. It is also not idempotent — a second
        # run fails with "Cannot create a file when that file already exists".
        # Generate them by hand once if you want IDE support; see PINS.md.
        #
        # No --stub-wasi: the bundled CPython needs the real WASI imports, which
        # the host supplies via wasmtime_wasi::p2::add_to_linker_sync.
        componentize-py -d "${WIT_DIR_REL}" -w pcs-pipeline componentize app -o enrich-py.wasm
    )
    PY_ARTIFACT="${STAGES}/python-enrich/enrich-py.wasm"
fi

# ---------------------------------------------------------------------------
# Step 5 — JavaScript stage.
# ---------------------------------------------------------------------------
if wants js; then
    require node 7 "https://nodejs.org/ (22 or newer)"
    require npm 7 "ships with Node"
    log "building JavaScript stage (score)..."
    cp "${GENERATED}/schema_gen.js" "${STAGES}/js-score/schema_gen.js"
    (
        cd "${STAGES}/js-score"
        if [[ -f package-lock.json ]]; then npm ci; else npm install; fi
        # Three flags, all load-bearing:
        #
        # `--bundle` is mandatory, not an optimisation: score.js imports
        # ./arrow-ipc.js, and StarlingMonkey's loader cannot resolve relative
        # modules at wizer time.
        #
        # `--disable fetch-event` on top of `--disable http` is what actually
        # drops the `wasi:http/types` import. Without it the component fails to
        # instantiate against the host, which links WASI but not wasi:http:
        # "component imports instance `wasi:http/types@0.2.10`, but a matching
        # implementation was not found in the linker".
        #
        # Keep clocks, random and stdio enabled (they are on by default):
        # disabling clocks makes Date.now() return garbage and this stage
        # reports timing in run-metrics.
        npx jco componentize score.js \
            --wit "${WIT_DIR_REL}" \
            --world-name pcs-pipeline \
            --disable http \
            --disable fetch-event \
            --bundle \
            -o score-js.wasm
    )
    JS_ARTIFACT="${STAGES}/js-score/score-js.wasm"
fi

# ---------------------------------------------------------------------------
# Step 6 — collect.
# ---------------------------------------------------------------------------
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
if wants js;     then collect "${JS_ARTIFACT}"   score-js.wasm;    fi
if wants rust;   then collect "${RUST_ARTIFACT}" settle-rs.wasm;   fi

# ---------------------------------------------------------------------------
# Step 7 — validate.
# ---------------------------------------------------------------------------
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

log "PASS — components in ${BUILD_DIR}"
log "next: cargo run -p pcs-service --features wasm,tracing --example polyglot_orders"
