---
name: export_pipeline! macro design sketch
description: Prep-work design for the attribute macro that wires user Pipeline to WIT exports
type: project
---

**Fact:** `export_pipeline!` is the guest SDK's entry point. User writes a `fn build() -> Pipeline` that constructs a fully-configured `Pipeline` (components registered, systems added). The macro generates the `impl Guest for Component` block that `cargo-component`/`wit-bindgen` requires, holding pipeline state in a `OnceLock` / `Mutex` static.

**Why:** WIT bindgen generates a `Guest` trait per exported interface. Authors shouldn't have to implement `describe`, `init`, `run-batch`, `snapshot`, `restore` by hand — each is mechanical marshalling (IPC bytes <-> Dataset, pollster::block_on for the async DAG, schema fingerprint from SchemaRegistry).

**How to apply (sketch — adjust to final WIT from task #12):**

```rust
// User crate:
use pcs_guest::prelude::*;

#[pcs_guest::export_pipeline]
fn build() -> Pipeline {
    Pipeline::builder("order_processing")
        .with::<Order>()
        .with_system(EnrichOrder)
        .build()
}
```

Macro expansion target (conceptual — bindings live in the CALLER crate, not pcs-guest):

```rust
// In the user's guest crate (cargo-component generates `crate::bindings` from
// the WIT at `../pcs-guest/wit/`):
const _: () = {
    use crate::bindings::exports::pcs::pipeline::pipeline::{Guest, ...};
    use pcs_guest::__rt::{Dataset, Pipeline, OnceLock, Mutex, pollster};

    static PIPELINE: OnceLock<Mutex<Pipeline>> = OnceLock::new();

    fn pipeline_mut() -> std::sync::MutexGuard<'static, Pipeline> {
        PIPELINE.get_or_init(|| Mutex::new(user_build_fn())).lock().unwrap()
    }

    struct __PcsComponent;

    impl Guest for __PcsComponent {
        fn describe() -> PipelineDescriptor { /* schemas → IPC schema bytes, fingerprint */ }
        fn init(config: String) -> Result<(), String> { /* parse serde_json::Value, stash */ }
        fn run_batch(input: Vec<u8>, prior: Option<Vec<u8>>) -> Result<RunResult, RunError> {
            let mut p = pipeline_mut();
            let mut dataset = Dataset::read_ipc(&mut &input[..])
                .map_err(|e| RunError::Permanent(format!("ipc decode: {e}")))?;
            if let Some(cp) = prior { /* restore dataset from checkpoint IPC */ }
            pollster::block_on(p.run_on(&mut dataset))
                .map_err(pcs_guest::__rt::pcs_error_to_run_error)?;
            let mut out = Vec::new();
            dataset.write_ipc(&mut out)
                .map_err(|e| RunError::Permanent(format!("ipc encode: {e}")))?;
            Ok(RunResult { output: out, checkpoint: None, metrics: /* from last_stats + timer */ })
        }
        fn snapshot() -> Result<Vec<u8>, String> { /* pipeline.data.write_ipc */ }
        fn restore(cp: Vec<u8>) -> Result<(), String> { /* Dataset::read_ipc into template */ }
    }

    crate::bindings::export!(__PcsComponent with_types_in crate::bindings);
};
```

**Bindgen path layout — verified against cargo-component 0.21.1 + wit-bindgen 0.41 output** (2026-04-15, during #13 implementation):

The WIT groups exports under `interface pipeline` AND defines shared records in `interface types`. cargo-component generates type aliases inside the `exports::...::pipeline` module ONLY for types the pipeline interface directly `use`s via `use types.{...}`. Types reached transitively (nested inside records) are NOT re-aliased and must be imported from the package-level `types` module directly.

For `pcs:pipeline@0.1.0` this means:
- **Available in `crate::bindings::exports::pcs::pipeline::pipeline::*`**: `Guest`, `PipelineDescriptor`, `IpcBytes`, `Checkpoint`, `RunResult`, `RunError` (all directly used by the `pipeline` interface).
- **Must import from `crate::bindings::pcs::pipeline::types::*`**: `ComponentDescriptor` (reached via `PipelineDescriptor.components`), `RunMetrics` (reached via `RunResult.metrics`).

The macro uses both paths — importing `PipelineDescriptor` / `RunResult` / `RunError` from the exports side and pulling `ComponentDescriptor` / `RunMetrics` from the types side. Missing either causes `E0432: unresolved import`.

**Other cargo-component integration facts** (learned during #13):
- cargo-component generates `src/bindings.rs` as a regular file on the wasm32-wasip2 target. The user crate declares `#[allow(warnings)] mod bindings;` to pull it in.
- On non-wasm host targets, `bindings.rs` does NOT exist. To keep `cargo check --workspace` green, gate BOTH the `mod bindings;` declaration AND the `export_pipeline!` invocation behind `#[cfg(target_arch = "wasm32")]`. The smoketest crate uses this pattern and stays host-compilable as an empty cdylib.
- `wit-bindgen-rt = { version = "0.44.0", features = ["bitflags"] }` must be a direct dep of any cdylib crate that cargo-component builds — this is the runtime crate the generated bindings link against. cargo-component's `cargo component new` template adds this automatically; hand-rolled crates (like the smoketest) must add it explicitly.
- cargo-component 0.21.1 outputs components at `target/wasm32-wasip1/debug/...wasm` even when invoked with `--target wasm32-wasip2` — the final component wrap step changes the target name to wasip1. This is expected; `wasm-tools validate` and `wasm-tools component wit` work on that path.

**Key design notes:**
1. **Bindings ownership: caller-side, not pcs-guest-side** (decision 2026-04-15 pre-#13 claim, advisor check).
   - pcs-guest is a **pure rlib** with NO `wit_bindgen::generate!` and NO `wit-bindgen` direct dependency.
   - The smoketest crate (and every user guest crate) has `[package.metadata.component]` + `[package.metadata.component.target.path = "../pcs-guest/wit"]`. `cargo component build` on that crate generates bindings in its own namespace using pcs-guest's WIT.
   - `export_pipeline!` emits paths rooted at `crate::bindings::exports::pcs::pipeline::pipeline::Guest` (caller's bindings).
   - Why not have pcs-guest run `wit_bindgen::generate!` itself: duplicate/conflicting types when smoketest's `cargo component build` ALSO generates bindings from the same WIT. Doing it caller-side gives each guest crate cargo-component's full pipeline (component wrapping, adapter embedding) with zero manual `wasm-tools component new` post-step. Standard flow.
2. The macro emits `crate::bindings::export!(__PcsComponent ...)` — this is the cargo-component handshake, required.
3. `pollster::block_on` runs the async DAG synchronously inside the sandbox. `Pipeline::run_on` is already `async fn` — it must only await at `System::run` call points (risk #0 in the plan).
4. State lives in a `OnceLock<Mutex<Pipeline>>` because WIT exports are `fn` (not `&mut self`). Guest is single-threaded in wasip2 so `Mutex` contention is zero — use it for `Send`/`Sync` plumbing only.
5. `describe()` must compute schema fingerprint from `pipeline.data.schemas()` — use `SchemaRegistry::fingerprint` if it exists, else hash the component schemas directly.
6. First call to any export triggers `build()` via `OnceLock` lazy init — simpler than a separate `init()` doing the build.
7. `init(config)` stashes the YAML/JSON config string somewhere retrievable (static `OnceLock<String>`). Guest code accesses it via a `pcs_guest::config()` helper.

**WIT-aligned updates (per wasm-lead draft 2026-04-15):**
- Exports grouped under `interface pipeline`, so bindgen path is `bindings::exports::pcs::pipeline::pipeline::Guest` (nested). Macro sketch already uses nested — confirmed compatible.
- `run-result` carries `run-metrics { wall-ns, rows-in, rows-out, systems-run, retries }` as a sub-record, not a flat tuple list. Macro fills from `Pipeline::last_stats()` + in-macro wall-clock.
- `run-error` variant arms: `retryable(string)`, `permanent(string)`, `schema-mismatch(string)`. PcsError mapping **final after dist-expert review 2026-04-15**:
  - `retryable` ← `RetryExhausted`, `SystemExecution` **only**. Wraps host releases the claim; runner exits current tick with Err; service tick re-claims next interval.
  - `permanent` ← `ComponentNotFound`, `ResourceNotFound`, `EntityNotFound`, `Configuration`, `Scheduler`, `Store`, **`Generic`**. Wraps host acks (batch lost), surfaces to operator.
  - `schema-mismatch` — **restore() ONLY**. `run-batch` must not emit this variant. wasm-lead adding WIT doc comment.
  - `LeaseExpired` dropped from guest mapping entirely — guests don't own leases.
  - **`Generic` flip rationale**: earlier "`Generic → retryable`" decision was based on the wrong mechanism model. I verified `src/distributed/runner.rs:406`: the inner runner loop already exits with Err on any `run_on` failure — not a silent infinite loop. The re-claim hazard is at the **outer service tick loop**. For an *unknown* error variant, losing one batch loudly (permanent) beats silently re-processing it forever (retryable) until an operator notices log noise. Guest authors who want retry semantics should construct `PcsError::SystemExecution` or `::RetryExhausted` explicitly. Task #24 (claim-level retry cap, replaces deleted #23) tracks `release_attempts` in `MasterBatchRecord` persisted via the Raft state machine — survives tick invocations.
  - **Guest-trap override**: traps (panics, OOM, stack overflow, epoch interrupt, JIT errors) caught at `WasmPipelineRuntime::run_on` are synthesized to `PcsError::SystemExecution` and then **re-bucketed to `permanent`** for v0.1.0, overriding the normal `SystemExecution → retryable` rule. Reason: trap causes empirically dominated by deterministic bugs (unreachable, panic, JIT); `u32::MAX` default cap means retryable would infinite-loop. Post-#24, consider splitting via `wasmtime::Trap` downcast (Option B). The override lives in coder-host's #14 wrapper, not in the macro — the macro never sees traps.
- `host-io::log` is an enum `log-level { trace, debug, info, warn, error }`, not a free string. Bridge helper maps `tracing::Level` → enum.
- `get-config(key: string) -> option<string>` is per-key, not a single blob.
- `init(config: string)` receives full YAML `config:` block as JSON. Two viable patterns:
  - **Eager (wasm-lead's suggestion, recommended)**: parse once in `init()` into a `serde_json::Value`, stash in `OnceLock<serde_json::Value>`. Per-key lookups go against the Value. Simpler lifetime, no lazy-parse retry paths to reason about.
  - Lazy: stash raw string, parse on first access. More code, no benefit.
  Going with eager.
- `describe()` iterates `SchemaRegistry` and emits one `component-descriptor` per registered component, with `arrow-schema-ipc` = Arrow IPC schema-message bytes.
- Checkpoint bytes v1 = `Dataset::write_ipc` of internal template data + accumulator state. Windows revisit later.

**Defensive trap-avoidance in the macro** (because host maps residual traps to `permanent`, pre-#24 we want to convert as many trap-like paths as possible into structured returns for better operator diagnostics):

- `Dataset::read_ipc(&input[..])` in `run_batch` → `.map_err(|e| RunError::Permanent(format!("ipc decode: {e}")))` instead of `.unwrap()`. Already returns `Result`, just propagate.
- `Dataset::write_ipc(&mut buf)` → same pattern. Buffer OOM is the likely culprit and should surface as `permanent` with a clear message.
- `serde_json::from_str(config)` in `init` → return `Err(String)` directly from the WIT `init` export. That's the cleanest path since `init` already has a `result<_, string>` return type.
- `build()` panic during `OnceLock` lazy init: NOT catchable without `std::panic::catch_unwind`, which is fine in wasm32-wasip2 as of wit-bindgen 0.44 but adds noise. **Don't catch.** If `build()` panics, the guest traps on first call and the host surfaces it as `permanent` via the trap override. Expected to be extremely rare (build() is pure Rust constructing a Pipeline — any panic is a programming error).
- User `.unwrap()` inside `System::run`: unavoidable. Guest author's responsibility to return `Err(PcsError::...)` instead. Document this clearly in the SDK's crate-level doc comment.

**Avoid:**
- Do NOT make `build()` async. Initialization must be synchronous inside the guest. If the user needs async init, do it inside the first `run-batch`.
- Do NOT re-export tokio from pcs-guest. Pollster is the only executor the guest should see.
- Do NOT add `wasi:filesystem` or `wasi:http` imports to the WIT world. Only `host-io` (log/metric/get-config).
- Do NOT wrap user systems in `catch_unwind` on the macro side. Adds weight, encourages sloppy error handling, and the host's trap catcher already provides a safety net.
