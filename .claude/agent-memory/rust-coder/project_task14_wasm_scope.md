---
name: Task #14 WasmPipelineRuntime Scope
description: Scope constraints for Phase 3.3 wasmtime host integration — standalone only, KeyPartition excluded
type: project
---

Task #14 (Phase 3.3: wasmtime host integration) is **standalone WASM only**. Distributed+WASM is a future phase.

**Why:** `KeyPartition` resource is TypeId-keyed and cannot cross the IPC boundary. dist-expert flagged this; distributed+WASM bridge deferred to a later phase.

**How to apply:**
- `WasmPipelineRuntime::run_on(&mut Dataset)` ignores any host-injected resources (they won't cross the boundary)
- Skip `KeyPartition` handling entirely in #14 — no runner.rs wire-up for WASM path
- Add module-level comment in `crates/pcs-service/src/wasm/runner.rs` documenting this limitation
- Add TODO for v0.2 WIT: how to deliver runtime context (partition ordinal, `KeyPartition`) to the guest — likely via `init(config)` JSON or a reserved component row
- dist-expert owns the distributed+WASM bridge in a later phase
