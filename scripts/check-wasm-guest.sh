#!/usr/bin/env bash
set -euo pipefail

rustup target add wasm32-wasip2
cargo check \
    --manifest-path crates/pcs-core/Cargo.toml \
    --target wasm32-wasip2 \
    --no-default-features \
    --features guest
