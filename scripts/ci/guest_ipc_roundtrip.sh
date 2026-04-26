#!/usr/bin/env bash
#
# CI guest round-trip RecordBatch against host arrow-ipc pin.
#
# This script is the CI driver for the host↔guest Arrow IPC round-trip
# regression test. It catches `arrow-ipc` version drift between pcs-core (host)
# and pcs-guest (guest) before drift can corrupt checkpoints in production.
#
# Steps:
#   1. Ensure the wasm32-wasip2 toolchain target is installed.
#   2. Build the pcs-guest-smoketest WebAssembly component (release profile).
#   3. Run the host-side wasm_roundtrip integration test, which loads the
#      .wasm via WasmPipelineRuntime, drives a RecordBatch through run-batch,
#      and asserts byte-exact IPC equality on the round-trip.
#
# The test fixture (pcs-guest-smoketest) is intentionally trivial — one
# component with a single u64 field, zero systems. The smoketest is an
# identity pipeline, so any byte difference between the BEFORE and AFTER IPC
# snapshots indicates arrow-ipc drift, NOT a logic bug in the guest.
#
# Target: must complete in under 2 minutes on CI with a warm cargo cache.
# Cold runs may approach 5 minutes due to cargo-component installation and
# the first wasm32-wasip2 build of arrow-ipc + pcs-core; CI should cache
# `target/` and `~/.cargo/registry` between runs.

set -euo pipefail

# Repo root regardless of where this script is invoked from.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

echo "[guest-ipc-roundtrip] repo: ${REPO_ROOT}"

# Required toolchain pieces. PINS.md in crates/pcs-guest documents the exact
# versions; this script just ensures they are installed.
echo "[guest-ipc-roundtrip] ensuring wasm32-wasip2 target is installed..."
rustup target add wasm32-wasip2

if ! command -v cargo-component >/dev/null 2>&1; then
    echo "[guest-ipc-roundtrip] ERROR: cargo-component not installed" >&2
    echo "[guest-ipc-roundtrip] install with: cargo install cargo-component --locked --version 0.21.1" >&2
    exit 2
fi

echo "[guest-ipc-roundtrip] cargo-component: $(cargo component --version)"

# Step 1: build the smoketest .wasm component (release profile keeps the
# binary at ~2-3 MB and matches the assertion the test makes about the
# canonical output path).
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

# Step 2: run the host-side integration test. The test name and crate are
# pinned because we want CI to be specific — any future test additions to
# pcs-service should not silently get pulled into this gate.
echo "[guest-ipc-roundtrip] running host-side round-trip test..."
cargo test \
    --test wasm_roundtrip \
    -p pcs-service \
    --features wasm \
    -- \
    --nocapture

echo "[guest-ipc-roundtrip] PASS"
