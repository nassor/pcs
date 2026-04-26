---
name: cargo-component workflow — pcs-guest (rlib) + pcs-guest-smoketest (cdylib)
description: Cargo.toml metadata, build commands, profile flags for task #13's two-crate layout
type: reference
---

**Fact:** Task #13 ships **two sibling workspace crates** (locked with wasm-lead 2026-04-15):
- `crates/pcs-guest/` — rlib SDK. Users depend on it. Owns `wit/pipeline.wit`. NOT a component.
- `crates/pcs-guest-smoketest/` — cdylib. Trivial echo pipeline using pcs-guest. Build fixture for #13 acceptance + CI fixture for #21 (arrow-ipc round-trip).

**Why:** pcs-guest is a library, not a component. Users write their own cdylib crate that depends on it. The smoketest crate is what `cargo component build` actually targets, and doubles as a CI-fixture so #13 and #21 share one artifact.

---

### `crates/pcs-guest/Cargo.toml` (SDK rlib)

```toml
[package]
name = "pcs-guest"
version = "0.1.0"
edition = "2024"
publish = false

[lib]
crate-type = ["rlib"]

[dependencies]
pcs-core    = { path = "../pcs-core", default-features = false, features = ["guest"] }
arrow-ipc   = "=58.1.0"          # MUST match pcs-core's pin exactly
pollster    = "0.4"              # sync executor for guest async (matches pcs-core pin)
serde       = { version = "1", features = ["derive"] }
serde_json  = "1"                # eager parse of init(config) JSON
# NOTE: NO wit-bindgen-rt and NO wit-bindgen here. pcs-guest is a pure rlib —
# bindings are generated in the caller (smoketest / user guest) crate by
# cargo-component. See MEMORY.md export-pipeline-macro-design.md for rationale.

# NO [package.metadata.component] — this crate is not itself a component.
# Downstream crates (pcs-guest-smoketest, user guests) point their own
# [package.metadata.component.target.path] at "../pcs-guest/wit".
```

### `crates/pcs-guest-smoketest/Cargo.toml` (cdylib fixture)

```toml
[package]
name = "pcs-guest-smoketest"
version = "0.1.0"
edition = "2024"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
pcs-guest = { path = "../pcs-guest" }
# pcs-core transitively, arrow-ipc transitively — exact pin enforced by pcs-guest.

[package.metadata.component]
package = "pcs:smoketest"

[package.metadata.component.target]
path = "../pcs-guest/wit"
world = "pcs-pipeline"
```

### Workspace `[profile.release]` for guest binaries

```toml
[profile.release]
opt-level     = "z"
lto           = "fat"
codegen-units = 1
panic         = "abort"
strip         = true
```

Shrinks the .wasm. Expect 2–8 MB for the smoketest after wasm-opt -Oz on top.

---

### Build & validate commands

```bash
rustup target add wasm32-wasip2
cargo install cargo-component --locked    # pin version at implementation time
cargo install wasm-tools --locked

# Acceptance test for task #13:
cargo build -p pcs-guest --target wasm32-wasip2 --features guest
cargo component build -p pcs-guest-smoketest --release --target wasm32-wasip2
wasm-tools validate --features component-model \
    target/wasm32-wasip2/release/pcs_guest_smoketest.wasm
wasm-tools component wit target/wasm32-wasip2/release/pcs_guest_smoketest.wasm

# Optional shrink:
wasm-opt -Oz -o target/.../pcs_guest_smoketest.opt.wasm \
             target/.../pcs_guest_smoketest.wasm
```

---

### Gotchas

- `wasm-bindgen` is NOT used. That's the browser JS interop stack. We target server-side wasmtime via Component Model.
- Target must be `wasm32-wasip2`, not `wasip1` or `unknown-unknown`. Component Model requires wasip2.
- `pcs-core`'s `guest` feature (task #7) must disable rayon, num_cpus, and any tokio runtime dependency. If any transitive dep pulls those in on wasip2, the build fails with a cryptic linker error. #6 audit complete, #7 in progress.
- `arrow-ipc = "=58.1.0"` MUST be identical between pcs-core and pcs-guest. Use `cargo tree -p pcs-guest-smoketest -i arrow-ipc` to confirm before CI.
- `wit-bindgen-rt` version floats; cargo-component picks it. Don't pin unless forced.
- `[package.metadata.component.target.path]` is relative to the crate root — `"../pcs-guest/wit"` from inside `crates/pcs-guest-smoketest/`.
- Owner of the `wasm-tools component wit` CI gate: **me, in #13**. wasm-lead runs it locally during #12 authoring as one-shot sanity; no CI gate in #12.
- Do NOT wire `wasi:filesystem` or `wasi:http` into the linker anywhere. WIT imports are `host-io` only.
