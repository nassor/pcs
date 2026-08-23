#!/usr/bin/env bash
#
# CI driver for the host↔guest Arrow IPC round-trip regression test. It catches
# `arrow-ipc` version drift between pcs-core (host) and pcs-guest (guest) before
# drift can corrupt checkpoints in production.
#
# Steps:
#   1. Ensure the wasm32-wasip2 toolchain target is installed.
#   2. Build the pcs-guest-smoketest WebAssembly component (release profile).
#   3. Run the host-side wasm_roundtrip integration test, which loads the
#      .wasm via WasmPipelineRuntime, drives a RecordBatch through run-batch,
#      and asserts byte-exact IPC equality on the round-trip.
#
# The artifact lands under `target/wasm32-wasip1/release/` even though the
# build targets `wasm32-wasip2`: cargo-component 0.21.1 compiles the core
# module for wasip1 and adapts it into a wasip2 component, keeping the
# pre-adapter directory name.
#
# The fixture is deliberately trivial: one component, a single u64 field, zero
# systems, so the pipeline is an identity. Any byte difference between the
# BEFORE and AFTER IPC snapshots therefore means arrow-ipc drift, not a guest
# logic bug.
#
# Cold runs are slow: cargo-component installation plus the first wasm32-wasip2
# build of arrow-ipc and pcs-core. CI should cache `target/` and
# `~/.cargo/registry` between runs.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

echo "[guest-ipc-roundtrip] repo: ${REPO_ROOT}"

# Exact versions live in crates/pcs-guest/PINS.md; this script only installs them.
echo "[guest-ipc-roundtrip] ensuring wasm32-wasip2 target is installed..."
rustup target add wasm32-wasip2

if ! command -v cargo-component >/dev/null 2>&1; then
    echo "[guest-ipc-roundtrip] ERROR: cargo-component not installed" >&2
    echo "[guest-ipc-roundtrip] install with: cargo install cargo-component --locked --version 0.21.1" >&2
    exit 2
fi

echo "[guest-ipc-roundtrip] cargo-component: $(cargo component --version)"

# Release profile: the test asserts the canonical release output path.
echo "[guest-ipc-roundtrip] building pcs-guest-smoketest (release)..."
cargo component build \
    --release \
    -p pcs-guest-smoketest \
    --target wasm32-wasip2

WASM_PATH="${REPO_ROOT}/target/wasm32-wasip1/release/pcs_guest_smoketest.wasm"
if [[ ! -f "${WASM_PATH}" ]]; then
    echo "[guest-ipc-roundtrip] ERROR: expected ${WASM_PATH} to exist after build" >&2
    exit 3
fi
echo "[guest-ipc-roundtrip] smoketest built: $(ls -lh "${WASM_PATH}" | awk '{print $5}')"

# The test name and crate are pinned so later pcs-service test additions do not
# silently join this gate.
echo "[guest-ipc-roundtrip] running host-side round-trip test..."
cargo test \
    --test wasm_roundtrip \
    -p pcs-service \
    --features wasm \
    -- \
    --nocapture

echo "[guest-ipc-roundtrip] PASS"
