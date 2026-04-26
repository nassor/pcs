---
name: Task 13 — pcs-guest SDK scope
description: Deliverables, dependencies, and acceptance criteria for task #13 (Phase 3.2 pcs-guest SDK crate)
type: project
---

**Fact:** Task #13 creates **two sibling workspace crates** (layout locked with wasm-lead 2026-04-15):
- `crates/pcs-guest/` — **rlib** SDK. Re-exports pcs-core types (Component, System, Pipeline, Dataset, SystemMeta, WriteSet, ...), owns the canonical `wit/pipeline.wit`, defines `export_pipeline!` macro, `host-io` bridge helpers (`pcs_guest::log::*`, `pcs_guest::config::<T>()`), pulls in `serde_json` + `pollster` + `wit-bindgen-rt`. NOT itself a component.
- `crates/pcs-guest-smoketest/` — **cdylib** using pcs-guest. Trivial echo pipeline. This is what `cargo component build` targets. Doubles as the CI fixture for task #21 (arrow-ipc round-trip gate).

**Revised acceptance (approved by wasm-lead):**
> `cargo component build -p pcs-guest-smoketest --target wasm32-wasip2` succeeds; `wasm-tools validate --features component-model` passes; `cargo build -p pcs-guest --target wasm32-wasip2 --features guest` builds the rlib.

**Why:** Pipeline authors need a single dependency to write a PCS pipeline as a WASM component without hand-writing WIT bindings or IPC marshalling. Splitting rlib + smoketest cdylib avoids leaking a dummy pipeline type into the SDK surface, and sidesteps cargo-component's historically-fiddly examples/ handling.

**How to apply:**
- `crates/pcs-guest/Cargo.toml`: depend on `pcs-core` with `default-features = false, features = ["guest"]` (task #7's deliverable — disables rayon, provides sync executor).
- `crates/pcs-guest-smoketest/Cargo.toml`: `crate-type = ["cdylib"]`, depends on pcs-guest, points `package.metadata.component.target.path = "../pcs-guest/wit"` for shared WIT source.
- The `wasm-tools component wit` parse check is a local one-shot during #12 authoring; **I own the CI gate in #13** via `cargo component build`.
- `serde_json` is approved as a pcs-guest dep (wasm32-wasip2 clean; wasm-lead confirming in #7 audit).
- **Eager config parse** in `init()`: stash `serde_json::Value` in `OnceLock<serde_json::Value>`, not raw string.
- Blocks: #18 (order_processing example), #21 (CI round-trip).
- Blocked by: #12 (WIT design) and #7 (pcs-core guest feature). Both must land first. #7 itself requires Phase 1 (#2, #3, #4, #6) — the workspace split and wasm32 audit.
- **Adjacent dependency: task #22** (extend `RunStats` with `retries_this_batch: u32`). Filed by wasm-lead, blocked on #3. Landing inside Phase 1 alongside the module move. Without #22, the `run-metrics.retries` field in the WIT `run-result` has to be hardcoded to 0 in the macro — acceptable fallback but not the target state.
