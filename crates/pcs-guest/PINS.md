# Toolchain pins — `pcs-guest`

This crate targets the WebAssembly Component Model. Host and guest share an
exact-pinned Arrow IPC crate, and the tooling versions below are what the
team has validated against the `pcs:pipeline@0.1.0` WIT package.

## Required tools

| Tool              | Version  | Install                                                       |
| ----------------- | -------- | ------------------------------------------------------------- |
| `wasmtime`        | 43.0.1   | `cargo install wasmtime-cli --locked --version 43.0.1`        |
| `cargo-component` | 0.21.1   | `cargo install cargo-component --locked --version 0.21.1`    |
| `wasm-tools`      | 1.246.2  | `cargo install wasm-tools --locked --version 1.246.2`        |
| Rust target       | `wasm32-wasip2` | `rustup target add wasm32-wasip2`                    |

## Load-bearing crate pin

`arrow-ipc = "=58.1.0"` is exact-pinned in the workspace (`Cargo.toml`).
Both the host and any guest built against this SDK MUST link against this
exact version, because Arrow IPC is the wire format across the Component
Model boundary. A patch-release drift here can silently corrupt `Dataset`
round-trips between host and guest. Do NOT relax this pin without the
round-trip CI job.

## WIT smoke check

Run from the repo root to confirm the WIT parses cleanly:

```
wasm-tools component wit crates/pcs-guest/wit/pipeline.wit > /dev/null
```

Exit code 0 = parse succeeded. Non-zero = structural WIT error; diff against
the committed `pipeline.wit` before investigating further.

## Upgrade policy

- **`wasmtime`**: upgrade the host first, verify component loads via the
  `pcs-service` load-time validation suite, then update the pin here. Bumps
  across majors require re-running every integration test that touches
  `wasmtime::component::bindgen!`.
- **`cargo-component`**: upgrade freely within its minor line if a newer
  release fixes a bug affecting pcs-guest consumers. Log the bump in this
  file with a one-line justification.
- **`wasm-tools`**: upgrade freely; it is invoked only by CI and local
  tooling, not at runtime.
- **`arrow-ipc`**: DO NOT BUMP without coordination. The pin is load-bearing
  for on-disk checkpoint format stability AND host↔guest wire-format
  compatibility. See the workspace `Cargo.toml` comment.

## Known version caveats (as of 2026-04-15)

- `cargo-component` 0.21.1 is the latest published on crates.io. If a newer
  release exists upstream but is unpublished, a direct `wit-bindgen`
  invocation is a documented fallback — but `cargo-component` is preferred
  because it wraps `wit-bindgen` internally and handles WIT discovery.
- `wit-bindgen` is NOT a direct dependency of this crate. `cargo-component`
  pulls it transitively. Do not add it to `Cargo.toml` unless you are
  bypassing `cargo-component` entirely.
