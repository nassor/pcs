---
name: WASM toolchain pins (Phase 3 decision, 2026-04-15)
description: Recommended version pins for wasmtime, cargo-component, wasm-tools, wit-bindgen for PCS WASM Component Model pivot
type: project
---

Recommended pins verified 2026-04-15 from crates.io. Plan intentionally did not hard-code; wasm-lead owns this decision.

- `wasmtime = "=43.0.1"` (crates.io max_stable_version, pub 2026-04-09). Plan referenced "31" but that is stale — 43 is current stable line. Component Model + epoch interruption stable. `wasmtime::component::bindgen!` + `Linker` used in `pcs-service/src/wasm/`.
- `cargo-component = "=0.21.1"` (pub 2025-03-18, latest on crates.io). Requires Rust edition 2024 compatible. Wraps wit-bindgen internally — do not add wit-bindgen directly to guest Cargo.toml.
- `wasm-tools = "=1.246.2"` (pub 2026-04-03). Used for validate + component wit. Repo currently has 1.242.0 installed — acceptable for now, bump in CI.
- `wit-bindgen = "0.56.0"` (pub 2026-04-14) — only if direct use ever needed. Default path is cargo-component's transitive pin.

**Risks / caveats:**
- cargo-component 0.21.1 is ~13 months old relative to today (2026-04-15). Bytecode Alliance may have newer work in-tree. Before Phase 3.2 (task #13), double-check whether 0.22+ exists but is unreleased; plan B is wit-bindgen direct + hand-rolled cargo bindings.
- wasmtime 43 → cargo-component 0.21.1 compat: cargo-component primarily produces components; the host wasmtime version is independent, but the WIT spec versions supported by cargo-component's bundled wit-parser must match the component-model spec wasmtime 43 expects. Verify with a hello-world round-trip in Phase 3.2.
- `arrow-ipc = "=58.1.0"` MUST be identical in pcs-core, pcs-guest, pcs-service. This is the single most load-bearing pin. CI gate in Phase 3.10 (task #21) enforces.

**Installed now on dev host (2026-04-15):** wasm-tools 1.242.0. wasmtime + cargo-component NOT installed. Phase 3.2 bootstrap will: `cargo install wasmtime-cli --locked --version 43.0.1`, `cargo install cargo-component --locked --version 0.21.1`, `rustup target add wasm32-wasip2`.
