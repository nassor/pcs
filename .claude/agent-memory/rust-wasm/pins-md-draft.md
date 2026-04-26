---
name: PINS.md draft for crates/pcs-guest/
description: Content to drop into crates/pcs-guest/PINS.md as part of task #12
type: project
---

Ready-to-drop content for `crates/pcs-guest/PINS.md` when task #12 lands. Captures toolchain pins per team-lead's 2026-04-15 approval.

```markdown
# Toolchain pins — `pcs-guest`

This crate targets the WebAssembly Component Model. Host and guest share an
exact-pinned Arrow IPC crate, and the tooling versions below have been tested
to produce a valid component against the `pcs:pipeline@0.1.0` WIT package.

## Required tools

| Tool              | Version  | Install                                              |
| ----------------- | -------- | ---------------------------------------------------- |
| `wasmtime`        | 43.0.1   | `cargo install wasmtime-cli --locked --version 43.0.1` |
| `cargo-component` | 0.21.1   | `cargo install cargo-component --locked --version 0.21.1` |
| `wasm-tools`      | 1.246.2  | `cargo install wasm-tools --locked --version 1.246.2` |
| Rust target       | `wasm32-wasip2` | `rustup target add wasm32-wasip2` |

## Load-bearing crate pin

`arrow-ipc = "=58.1.0"` is exact-pinned in the workspace. Both the host and
any guest produced by `pcs-guest` MUST link against this exact version,
because Arrow IPC is the wire format across the Component Model boundary.
A patch-release drift here can silently corrupt `Dataset` round-trips between
host and guest. Do NOT relax this pin without a CI round-trip test
(see task #21).

## Upgrade policy

- `wasmtime` — upgrade the host first, verify component loads, then update the
  pin here. Bumps require re-running the load-time validation suite in
  `pcs-service/src/wasm/loader.rs`.
- `cargo-component` — upgrade freely within its major line if a newer release
  fixes a bug affecting pcs-guest consumers. Log the bump in this file with
  a one-line justification.
- `wasm-tools` — upgrade freely; it is invoked only by CI and local tooling,
  not at runtime.
- `arrow-ipc` — DO NOT BUMP without coordination. See workspace `Cargo.toml`.

## Known version caveats (2026-04-15)

- `cargo-component` 0.21.1 is the latest on crates.io as of this writing
  (published March 2025). If a newer version exists upstream but unreleased,
  consider direct `wit-bindgen` as a fallback.
- `wit-bindgen` is NOT a direct dependency — `cargo-component` wraps it
  internally. Do not add it to `Cargo.toml` unless you are bypassing
  `cargo-component` entirely.
```

Task ownership note: team-lead confirmed this document should land as part of task #12, not a separate task. File it at `crates/pcs-guest/PINS.md`.
