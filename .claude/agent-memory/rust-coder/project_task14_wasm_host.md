---
name: Task #14 wasmtime host integration
description: wasm/ module structure, generated type paths, HasSelf pattern, TypesHost gotcha
type: project
---

wasmtime 43 host integration landed in `crates/pcs-service/src/wasm/` behind `--features wasm`.

**Why:** WASM component model pivot — users ship `.wasm` pipelines; host serializes Dataset to Arrow IPC, calls guest `run-batch`, deserializes output.

**Key findings for Phase 3 coders:**

- `bindgen!` syntax for wasmtime 43: NO `async: false` key (not valid). Drop it. Sync is default.
- Generated world struct name: `PcsPipeline` (from world `pcs-pipeline`).
- `add_to_linker` needs `HasSelf`: `PcsPipeline::add_to_linker::<_, HasSelf<_>>(&mut linker, |s| s)`.
- WIT `types` interface generates an empty `pcs::pipeline::types::Host` marker trait — every store-data type must implement it alongside `host_io::Host`. Easy to miss.
- `call_run_batch` takes `Option<&Vec<u8>>` (not `Option<&[u8]>`) for checkpoint arg.
- `WasmEngine::new()` spawns a tokio task (epoch ticker) — tests must use `#[tokio::test]`.
- `Component` (wasmtime) doesn't impl `Debug` → can't derive Debug on structs containing it. Use `.err().expect(...)` not `.unwrap_err()` in tests.
- `declared_components()` lifetime problem: names from `Mutex<Option<PipelineDescriptor>>` can't escape the guard. Solution: `OnceLock<Vec<String>>` populated once during `describe()`.

**Module layout:**
- `bindings.rs`: `bindgen!` call + re-exports (`HostIo`, `LogLevel`, `TypesHost`, `PipelineDescriptor`, `RunError`, `RunResult`, `ComponentDescriptor`)
- `engine.rs`: `WasmEngine` (wraps `wasmtime::Engine`, epoch tick 100ms)
- `host_impl.rs`: `HostState` (name + config HashMap), implements `TypesHost` + `HostIo`
- `runner.rs`: `WasmPipelineRuntime` (compile once, fresh Store per call, `OnceLock` for component names)

**Pending:** `template_dataset()` on `PipelineRuntime` trait awaiting architect sign-off. Needed for cluster.rs:215 to get schema-only Dataset without concrete Pipeline.

**How to apply:** When building #15 (PipelineRuntimeLoader), use `WasmPipelineRuntime::from_bytes` then call `.describe()` then `.init(config_json)` in that order.
