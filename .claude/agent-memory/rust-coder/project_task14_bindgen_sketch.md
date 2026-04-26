---
name: Task #14 wasmtime bindgen! Module Layout Sketch
description: Pre-implementation sketch of pcs-service/src/wasm/ module layout for WasmPipelineRuntime using wasmtime 43; all decisions confirmed by team-lead
type: project
---

wasmtime stable: 43.0.1 (pinned at implementation time per plan). Async disabled — guest is sync, host wraps in spawn_blocking.

## All decisions confirmed by team-lead

1. **Store reuse via Mutex** — approved for standalone. Comment: single-instance only; distributed+WASM (future phase) will likely need per-claim fresh instances.
2. **Box::leak for component names** — approved, matches registry.rs pattern. Bounded count, one-time at describe().
3. **spawn_blocking scope** — entire run-batch + IPC decode inside blocking task; IPC encode before spawn (22ms for 1M rows per bench, non-blocking).
4. **Epoch ticker** — lives on Engine (Arc), not per-Store. Tokio task at service startup calls `engine.increment_epoch()` every 100ms. `store.set_epoch_deadline(N)` bounds each run_on at ~N×100ms. Trap on epoch exceed must surface as `Err(PcsError::...)` not panic (chaos test #19 verifies claim released).
5. **async: false on bindgen!** — approved, re-visit if Component Model async matures.

## WIT path (confirmed)

Canonical WIT at `crates/pcs-guest/wit/pipeline.wit` — pcs-guest owns it (#12 + #13).
bindings.rs uses relative path:
```rust
wasmtime::component::bindgen!({
    world: "pcs-pipeline",
    path: "../pcs-guest/wit",
    async: false,
});
```
Fallback if cargo-component resolver rejects relative path: workspace-level symlink `wit/ -> crates/pcs-guest/wit`.

## Module layout: crates/pcs-service/src/wasm/

### mod.rs
Re-exports `WasmPipelineRuntime`, `WasmEngine`. Public surface only.

### engine.rs
Singleton `WasmEngine` (Arc-shared across all loaded modules):

```rust
pub struct WasmEngine {
    engine: wasmtime::Engine,
    linker: wasmtime::component::Linker<WasmHostState>,
}

impl WasmEngine {
    pub fn new() -> PcsResult<Arc<Self>> { ... }
}
```

- `wasmtime::Config` with `epoch_interruption(true)`, async disabled
- All host-io import functions added to `Linker` once at construction via `host_impl::add_to_linker`
- Epoch ticker spawned once at service startup (outside WasmEngine, in service init):
  ```rust
  // In service startup — not inside WasmEngine to avoid circular Arc
  let engine_ref = Arc::clone(&engine.engine);
  tokio::spawn(async move {
      let mut interval = tokio::time::interval(Duration::from_millis(100));
      loop { interval.tick().await; engine_ref.increment_epoch(); }
  });
  ```

### bindings.rs
```rust
wasmtime::component::bindgen!({
    world: "pcs-pipeline",
    path: "../pcs-guest/wit",
    async: false,
});
```
Generates: `PcsPipeline` (caller struct for guest exports), host import trait(s).
File is nearly empty — macro generates everything.

### host_impl.rs

```rust
pub struct WasmHostState {
    /// Opaque config JSON blob passed at load time via `pipeline.wasm.config`.
    config: serde_json::Value,
    /// Prometheus handle for metric() calls; None in test/standalone-no-metrics mode.
    prometheus_handle: Option<Arc<prometheus::Registry>>,
}

impl WasmHostState {
    pub fn new(config: serde_json::Value, prometheus_handle: Option<Arc<prometheus::Registry>>) -> Self { ... }
}
```

Implements generated `HostIo` trait on `WasmHostState`:
- `log(level: u32, message: String)` → `#[cfg(feature = "tracing")] tracing::event!(...)` mapping level 0→ERROR, 1→WARN, 2→INFO, 3→DEBUG, 4→TRACE
- `metric(name: String, value: f64)` → prometheus counter/gauge push via handle
- `get_config(key: String) -> Option<String>` → `self.config[&key]` as JSON string

`pub fn add_to_linker(linker: &mut Linker<WasmHostState>) -> PcsResult<()>` — called once from engine.rs

### loader.rs

```rust
pub struct WasmLoader;

impl WasmLoader {
    pub async fn load(
        path: &std::path::Path,
        sha256: Option<&str>,
        config: serde_json::Value,
        engine: &Arc<WasmEngine>,
        prometheus_handle: Option<Arc<prometheus::Registry>>,
    ) -> PcsResult<WasmPipelineRuntime> { ... }
}
```

Steps:
1. `tokio::fs::read(path)` — async file read
2. Optional sha256 integrity check (hex-encoded SHA-256 of raw bytes)
3. `wasmtime::component::Component::from_binary(&engine.engine, &bytes)` — may be expensive; runs on blocking pool if >1MB
4. `engine.linker.instantiate(&mut store, &component)` → `PcsPipeline::new(&mut store, &instance)`
5. Call `instance.call_describe(&mut store)` → `PipelineDescriptor`
6. `Box::leak` component name strings → `Vec<&'static str>`
7. Return `WasmPipelineRuntime { name, components, schema_fingerprint, inner: Mutex::new(WasmRuntimeInner { store, instance, last_checkpoint: None }) }`

Validation (step 5.5 — called from ServiceBuilder, not loader):
- Declared components cover all source/sink `target_component` / `source_component` in `ServiceConfig`
- `schema_fingerprint` matches any persisted checkpoint's fingerprint (if resuming)

### runner.rs

Module-level comment (required per team-lead):
```
// Phase 3: standalone WASM only.
//
// WasmPipelineRuntime::run_on ignores all Resource values in the incoming Dataset.
// Resources are TypeId-keyed and not serialized by Dataset::write_ipc — they are
// present in the Dataset on the host but invisible to the guest. KeyPartition and
// other host-injected partition context fall into this category.
//
// Distributed + WASM (delivering KeyPartition to the guest via init(config) JSON or
// a reserved component row) is deferred to v0.2. dist-expert owns that bridge.
// When that work lands, this standalone Mutex<WasmRuntimeInner> pattern will likely
// change to per-claim fresh Store instances to avoid cross-claim state leakage.
```

```rust
pub struct WasmPipelineRuntime {
    name: String,
    /// Component names declared by the guest's describe() call, Box::leaked to 'static.
    components: Vec<&'static str>,
    /// Arrow schema fingerprint from describe(); used at load time to reject
    /// checkpoint-version mismatches. Stored here for ServiceBuilder validation.
    schema_fingerprint: u64,
    /// Epoch deadline in ticks (each tick = 100ms). Default: 300 ticks = ~30s per batch.
    epoch_deadline: u64,
    inner: tokio::sync::Mutex<WasmRuntimeInner>,
}

struct WasmRuntimeInner {
    store: wasmtime::Store<WasmHostState>,
    instance: PcsPipeline,
    /// Checkpoint bytes from the last successful run-batch, passed as `prior` next call.
    last_checkpoint: Option<Vec<u8>>,
}
```

`impl PipelineRuntime for WasmPipelineRuntime`:
```
run_on(&self, data: &mut Dataset):
  1. Serialize: let ipc_bytes = data.write_ipc()?  [on async thread — fast]
  2. Lock inner (tokio::sync::Mutex, async-aware)
  3. Set epoch deadline: inner.store.set_epoch_deadline(self.epoch_deadline)
  4. Clone prior checkpoint bytes for move into blocking closure
  5. tokio::task::spawn_blocking(move || {
         let result = inner.instance.call_run_batch(
             &mut inner.store, &ipc_bytes, prior.as_deref()
         );
         result  // RunResult | RunError | trap
     }).await
  6. On Ok(run_result):
     - Dataset::read_ipc(run_result.output_ipc) → overwrite *data
     - inner.last_checkpoint = run_result.checkpoint
     - return Ok(())
  7. On Err(trap or RunError):
     - Map to PcsError::Generic("wasm guest trap: {msg}") or PcsError::SystemExecution
     - Do NOT update last_checkpoint (keeps prior for replay)
     - return Err(e)  [caller: DistributedRunner releases claim, not acks]
```

Note: `tokio::sync::Mutex` (not `std::sync::Mutex`) because `run_on` is async and we `.await` across the lock boundary. The lock is held through the `spawn_blocking` await, which is intentional — only one `run_on` at a time per runtime instance.

## Remaining open question

`call_run_batch` exact generated signature depends on WIT type definitions from #12. Specifically:
- `ipc-bytes` type alias: likely `list<u8>` → `&[u8]` in Rust
- `checkpoint` type: likely `list<u8>` → `Vec<u8>`
- `run-result` record fields: `output-ipc: list<u8>`, `checkpoint: option<list<u8>>`
- `run-error` variant: `{ message: string }` or richer?

These shapes resolve when task #12 (WIT freeze) completes.
