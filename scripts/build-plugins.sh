#!/usr/bin/env bash
#
# Build the two native plugin fixtures: shared libraries the host loads with
# dlopen, not WebAssembly components.
#
#   target/debug/<pre>pcs_plugin_smoketest<suf>        Rust, the host test fixture
#   examples/plugins/settle-go/<pre>settle_go<suf>     Go, the cross-language proof
#
# <pre> and <suf> are the platform's shared library prefix and suffix, so both
# artifacts match std::env::consts::DLL_PREFIX and DLL_SUFFIX and a Rust test
# builds either path the same way.
#
# Steps:
#   1. cargo build -p pcs-plugin-smoketest.
#   2. Regenerate examples/polyglot/generated/ from pcs_polyglot_order::Order and
#      copy schema_gen.go into the Go plugin, rewriting its package clause. The
#      Go plugin embeds the `Order` schema bytes and fingerprint as constants, so
#      they must come from the same emit the WASM stages use.
#   3. go build -buildmode=c-shared the Go plugin, in place.
#
# Toolchain versions are pinned in examples/plugins/settle-go/PINS.md. Every
# check runs before any work and has its own exit code, so a CI failure names the
# missing tool instead of dying inside a compiler:
#
#   2 cargo   3 Go   4 a C compiler for cgo   5 build produced no artifact
#
# A contributor with only one toolchain can build one plugin:
#
#   bash scripts/build-plugins.sh --only=rust
#   bash scripts/build-plugins.sh --only=go
#
# `--only=go` still needs cargo, because step 2 regenerates the schema constants
# the Go plugin compiles in.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

GENERATED="${REPO_ROOT}/examples/polyglot/generated"
GO_PLUGIN="${REPO_ROOT}/examples/plugins/settle-go"

# The platform's shared library naming, matching Rust's DLL_PREFIX and
# DLL_SUFFIX. Windows is the one target without the `lib` prefix.
case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN* | Windows_NT) LIB_PRE=""; LIB_SUF=".dll" ;;
    Darwin) LIB_PRE="lib"; LIB_SUF=".dylib" ;;
    *) LIB_PRE="lib"; LIB_SUF=".so" ;;
esac

RUST_ARTIFACT="${REPO_ROOT}/target/debug/${LIB_PRE}pcs_plugin_smoketest${LIB_SUF}"
GO_LIB="${LIB_PRE}settle_go${LIB_SUF}"
GO_ARTIFACT="${GO_PLUGIN}/${GO_LIB}"

ONLY="rust,go"
for arg in "$@"; do
    case "${arg}" in
        --only=*) ONLY="${arg#--only=}" ;;
        -h | --help)
            sed -n '2,33p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "[plugins] ERROR: unknown argument '${arg}'" >&2
            echo "[plugins] usage: bash scripts/build-plugins.sh [--only=rust,go]" >&2
            exit 1
            ;;
    esac
done

wants() { [[ ",${ONLY}," == *",$1,"* ]]; }

log() { echo "[plugins] $*"; }

require() {
    # require <command> <exit-code> <install hint>
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "[plugins] ERROR: $1 not found on PATH" >&2
        echo "[plugins] install with: $3" >&2
        echo "[plugins] see examples/plugins/settle-go/PINS.md for the pinned versions" >&2
        exit "$2"
    fi
}

require_cgo() {
    # cgo shells out to the compiler `go env CC` names, and -buildmode=c-shared
    # cannot work without it. Checking here turns "gcc: executable file not
    # found" into a named failure.
    local cc
    cc="$(go env CC 2>/dev/null || true)"
    [[ -n "${cc}" ]] || cc="cc"
    if command -v "${cc}" >/dev/null 2>&1; then
        return 0
    fi
    echo "[plugins] ERROR: no C compiler for cgo: '${cc}' is not on PATH" >&2
    echo "[plugins] the Go plugin is a cgo shared library and cannot build without one" >&2
    echo "[plugins] install with: Linux 'apt install build-essential'," >&2
    echo "[plugins]               macOS 'xcode-select --install'," >&2
    echo "[plugins]               Windows mingw-w64 via 'winget install -e --id MSYS2.MSYS2'" >&2
    echo "[plugins]               then 'pacman -S mingw-w64-x86_64-gcc' and put its bin/ on PATH" >&2
    echo "[plugins] see examples/plugins/settle-go/PINS.md for the pinned versions" >&2
    exit 4
}

artifact() {
    # artifact <path>
    if [[ ! -f "$1" ]]; then
        echo "[plugins] ERROR: expected $1 to exist after the build" >&2
        exit 5
    fi
    log "built $(basename "$1") ($(wc -c <"$1" | tr -d ' ') bytes)"
}

log "repo: ${REPO_ROOT}"
log "plugins: ${ONLY}"

# Every toolchain check first. Step 2 needs cargo whichever plugin was asked
# for, so cargo is unconditional.
require cargo 2 "https://rustup.rs"
if wants go; then
    require go 3 "https://go.dev/dl/ (1.25 or newer)"
    require_cgo
fi

if wants rust; then
    log "building Rust plugin fixture (pcs-plugin-smoketest)..."
    cargo build -p pcs-plugin-smoketest
    artifact "${RUST_ARTIFACT}"
fi

log "generating schema constants from pcs_polyglot_order::Order..."
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit

for f in schema_gen.go order_fingerprint.txt; do
    if [[ ! -f "${GENERATED}/${f}" ]]; then
        echo "[plugins] ERROR: emit did not produce ${GENERATED}/${f}" >&2
        exit 5
    fi
done
log "fingerprint: $(cat "${GENERATED}/order_fingerprint.txt")"

if wants go; then
    # The generated file names the package the WASM stage's binding directory
    # needs. A c-shared plugin is package main, so the clause is rewritten and
    # nothing else is.
    log "copying Order schema constants into the Go plugin..."
    sed 's/^package .*/package main/' "${GENERATED}/schema_gen.go" >"${GO_PLUGIN}/schema_gen.go"

    log "building Go plugin (settle-go)..."
    (
        cd "${GO_PLUGIN}"
        CGO_ENABLED=1 go build -buildmode=c-shared -o "${GO_LIB}" .
    )
    artifact "${GO_ARTIFACT}"
fi

log "PASS"
if wants rust; then log "rust: ${RUST_ARTIFACT}"; fi
if wants go; then log "go:   ${GO_ARTIFACT}"; fi
