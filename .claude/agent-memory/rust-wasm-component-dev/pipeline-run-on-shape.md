---
name: Pipeline::run_on shape — what the guest SDK must call
description: Signature and semantics of the existing run_on entry point the macro wraps
type: reference
---

**Fact:** The macro-generated `run_batch` impl will call `pipeline.run_on(&mut dataset).await` inside a `pollster::block_on`. The current signature is `pub async fn run_on(&self, data: &mut Dataset) -> PcsResult<()>` in `src/pipeline/execution.rs:375`. After Phase 1, this moves to `crates/pcs-core/src/pipeline/execution.rs`.

**Why:** `run_on` is explicitly the escape hatch for external runners (DistributedRunner uses it today). Its `&self` plus `&mut Dataset` shape is exactly what the guest needs: one persistent pipeline template + a fresh dataset per batch.

**How to apply:**
- Guest SDK creates Pipeline once (OnceLock) via user's `build()` function — the template.
- Each `run-batch` call: deserialize input IPC → new Dataset → `run_on(&mut dataset).await` → serialize back.
- The template's own `pipeline.data` is NOT touched (same pattern as DistributedRunner).
- Retry, DAG staging, parallel-system dispatch all happen inside `run_on` — no guest changes needed.
- `run_on` validates fields against `data.schemas()` on first call per schema registry — cached via `OnceLock`. This is fine for the guest since schemas don't drift batch-to-batch.

**Risk hooks:**
- `run_on` uses `run_parallel_stage` which (in current code) relies on rayon for ParallelSystem. Task #7 (pcs-core guest feature) must replace this with a sequential fallback. Verify that branch compiles for wasm32-wasip2 before task #13's build succeeds.
- `run_on` returns `PcsResult<()>`. The macro converts `PcsError` variants into the WIT `run-error` variant cases, **final lock 2026-04-15 after dist-expert review**:
  - `retryable(string)` ← `RetryExhausted`, `SystemExecution`. Host's `WasmPipelineRuntime::run_on` wrapper releases the claim; runner exits current tick with Err; service tick loop re-claims the same batch next interval.
  - `permanent(string)` ← `ComponentNotFound`, `ResourceNotFound`, `EntityNotFound`, `Configuration`, `Scheduler`, `Store`, **`Generic`**. Host acks the claim (batch lost), surfaces to operator.
  - `schema-mismatch(string)` — restore()-only. `run-batch` must NOT emit it.
  - `LeaseExpired` dropped from guest mapping entirely — guests don't own leases (dist-expert).

**Mechanism correction (important for task #23):** I verified the runner code directly. At `src/distributed/runner.rs:406`, on any `run_on` Err the runner calls `release_with_log` and **returns Err up the call stack** — it does NOT silently loop on the same batch inside the inner `loop { }`. The retry hazard is in the **outer service tick loop**, not the runner's inner loop: on the next interval, `claim_next_batch` re-selects the still-Pending batch and the cycle repeats. Task #23 (claim-level retry cap) must therefore track per-claim-id attempt count **across tick invocations** — a naive counter in the inner loop is insufficient.

**Why `Generic → permanent` instead of retryable** (dist-expert review, confirmed against runner code): the asymmetry that matters for an *unknown* error is ack-vs-release semantics. Releasing means the service tick loop re-processes the same batch forever until someone notices the log noise. Acking means losing one batch but raising it loudly; operators can resubmit a lost batch, they can't un-retry a silently-reprocessed one. Guest authors who want retry semantics must construct `PcsError::SystemExecution` or `::RetryExhausted` explicitly — that's the correct signal.

**Guest-trap mapping — v0.1.0 final** (dist-expert + wasm-lead, locked 2026-04-15):

Guest traps (panics, OOM, stack overflow, epoch interrupts, JIT errors) are caught at the host `WasmPipelineRuntime::run_on` wrapper in task #14 and synthesized into `PcsError::SystemExecution`. **That synthesized variant maps to `permanent`, NOT `retryable`**, overriding the normal `SystemExecution → retryable` rule.

Why the trap-specific override:
- Trap causes split ~three ways: (1) guest logic bugs like `unreachable!()` / divide-by-zero / panic! — *permanent* (re-trap on replay); (2) resource exhaustion under load — *retryable with cap*; (3) wasmtime infra (JIT/linker) — *permanent*. Categories 1 and 3 empirically dominate.
- Host can't cheaply distinguish the three without `wasmtime::Trap` downcast, which is Option B (deferred to post-#24).
- Epistemic parallel to the `Generic → permanent` flip: host doesn't know whether this trap is a bug or transient pressure. Defaulting to retryable assumes the minority case.
- Critical timing constraint: #24 (claim-level retry cap) does **not** ship alongside #12/#13/#14. Production default is `max_claim_releases = u32::MAX`, i.e. no cap. Mapping traps to retryable is a **guaranteed** tick-loop infinite retry in production, not a theoretical hazard.
- Post-#24 we can revisit with real data on which trap category appears in practice.

**Implication for the macro**: no code change on my side. The trap mapping lives entirely in `WasmPipelineRuntime::run_on` (coder-host, task #14). Traps don't round-trip through the macro — they're the component call failing. But the macro should defensively catch as many trap-like paths as possible and convert them to structured `PcsError::Generic` / `::Configuration` returns so they get the same `permanent` bucket via the normal mapping rules:

- `Dataset::read_ipc(&input[..])` failure → `PcsError::Generic` (malformed IPC)
- `Dataset::write_ipc(&mut buf)` failure → `PcsError::Generic` (OOM-adjacent; rare)
- `OnceLock<Mutex<Pipeline>>` lazy init: user's `build()` panic is not catchable without `catch_unwind`; let it trap, host maps to permanent
- `serde_json::from_str(config)` in `init()` → return `Err(String)` from WIT `init` export directly (not run-batch)
- User `.unwrap()` inside `System::run` → unavoidable, traps, host maps to permanent

Net: traps become the exception, not the norm, and when they do occur they get the same safe-default bucket as unknown error shapes.
