---
name: WIT draft for pcs:pipeline@0.1.0 (task #12 prep)
description: Pre-task-12 scratch draft of the WIT package; ready to drop into crates/pcs-guest/wit/pipeline.wit when #11 unblocks
type: project
---

Drop-in draft — file will become `crates/pcs-guest/wit/pipeline.wit` once task #12 is claimed. Plan sections: "WASM boundary — WIT interface", `pipeline-descriptor`, sync-guest-async-host.

```wit
package pcs:pipeline@0.1.0;

// NOTE: intentionally no `required-host-io-version` field on pipeline-descriptor
// in v0.1.0. The WIT package version (`@0.1.0`) already signals host-io
// compatibility; adding a forward-compat knob before we know what backward
// incompat looks like is speculative. Revisit for v0.2 if host-io grows.

/// Types shared by the pipeline interface.
interface types {
    /// Arrow IPC stream bytes (schema + batches). Host serializes a Dataset
    /// on input; guest re-serializes the (possibly mutated) Dataset on output.
    type ipc-bytes = list<u8>;

    /// Opaque guest-owned checkpoint blob. Host persists verbatim via
    /// pcs-core CheckpointStore; guest owns the layout.
    type checkpoint = list<u8>;

    /// One component declared by the guest pipeline. `arrow-schema-ipc`
    /// is the Arrow IPC schema-message bytes for that component's RecordBatch
    /// shape. Host validates source/sink target_component names against this
    /// list at load time.
    record component-descriptor {
        name: string,
        arrow-schema-ipc: list<u8>,
    }

    /// Stable identity of the pipeline. Host gates config + checkpoint
    /// compatibility on this. `schema-fingerprint` is computed from the
    /// concatenation of all component schemas (see pcs-core SchemaRegistry::fingerprint).
    record pipeline-descriptor {
        name: string,
        version: string,
        components: list<component-descriptor>,
        stateful: bool,
        schema-fingerprint: string,
    }

    /// Lightweight metrics the guest reports after each batch. Host surfaces
    /// to prometheus via host-io::metric.
    record run-metrics {
        wall-ns: u64,
        rows-in: u64,
        rows-out: u64,
        systems-run: u32,
        retries: u32,
    }

    record run-result {
        output: ipc-bytes,
        checkpoint: option<checkpoint>,
        metrics: run-metrics,
    }

    /// Structured errors so host can distinguish "release claim and retry"
    /// from "ack claim and surface to operator".
    ///
    /// Variant naming matches DistributedRunner vocabulary:
    /// `retryable` → runner calls `release_claim` (runner.rs:286/350/371/387/405)
    /// `permanent` → runner calls `ack_claim` (runner.rs:410)
    ///
    /// v0.1.0 is deliberately a three-arm variant. A `host-denied(string)`
    /// arm for host-io capability refusals was considered and rejected:
    /// v0.1.0's host-io surface (log / metric / get-config) cannot fail,
    /// so the slot would be dead weight. When v0.2 grows the host-io
    /// surface (e.g. filesystem or network), the WIT package version bump
    /// to 0.2.0 is itself the right moment to add the variant — no need
    /// to reserve it speculatively here.
    variant run-error {
        /// Transient — retry-worthy failure. Host releases claim, retries
        /// next tick. Maps to `release_claim`.
        retryable(string),
        /// Permanent — bad input shape, guest logic bug, unknown error.
        /// Host acks claim, logs, surfaces to /status. Maps to `ack_claim`.
        permanent(string),
        /// Fingerprint mismatch. Emitted ONLY from `restore()`. `run-batch`
        /// MUST NOT emit this; any mid-batch schema error is folded into
        /// `permanent`. Host refuses to replay.
        schema-mismatch(string),
    }
}

/// Host capabilities the guest may call during init or run-batch.
/// Deliberately minimal in v1: logging, metric counters, static config read.
/// NO filesystem, NO network, NO clock (epoch-driven cancellation only).
interface host-io {
    use types.{run-metrics};

    enum log-level { trace, debug, info, warn, error }

    /// Structured log line bridged to tracing on the host.
    log: func(level: log-level, target: string, message: string);

    /// Increment or observe a named metric. Host routes to prometheus.
    /// Value is f64 to cover counters, gauges, and histograms.
    metric: func(name: string, value: f64);

    /// Pull a string-typed config value the host injected via
    /// `pipeline.wasm.config` in service YAML. Returns none if absent.
    /// Guest is expected to parse numerics itself.
    get-config: func(key: string) -> option<string>;
}

/// The sole guest export. All functions are sync from the WIT perspective;
/// the host wraps each call in spawn_blocking + epoch interruption.
interface pipeline {
    use types.{pipeline-descriptor, ipc-bytes, checkpoint, run-result, run-error};

    /// Called once at load time. Host validates the descriptor against
    /// ServiceConfig (declared components cover sources/sinks, schema
    /// fingerprint matches any persisted checkpoint).
    describe: func() -> pipeline-descriptor;

    /// Called once after describe(), before any run-batch. Host passes the
    /// full `pipeline.wasm.config` YAML block as a JSON string. Guest parses
    /// and stores locally. Returning err aborts service startup.
    init: func(config: string) -> result<_, string>;

    /// The hot path. Host marshals the partition batch to Arrow IPC and
    /// hands it in alongside any prior checkpoint. Guest runs its internal
    /// Pipeline::run_on DAG against a fresh Dataset, returns IPC bytes +
    /// optional new checkpoint + metrics.
    ///
    /// Invariant: run-batch MUST NOT return run-error::schema-mismatch.
    /// Schema/fingerprint validation is a startup concern and lives only
    /// in describe() + restore(). A mismatch surfacing mid-batch is a
    /// guest bug and must be collapsed to `permanent`.
    run-batch: func(input: ipc-bytes, prior: option<checkpoint>) -> result<run-result, run-error>;

    /// Emit a point-in-time checkpoint without running a batch. Used by
    /// CheckpointStrategy::EveryNStages path if the guest chooses to snapshot
    /// between ticks. May return err if the guest has no persistent state.
    snapshot: func() -> result<checkpoint, string>;

    /// Restore from a persisted checkpoint during cold-start recovery.
    /// Returning err forces the host to refuse startup.
    restore: func(cp: checkpoint) -> result<_, string>;
}

world pcs-pipeline {
    import host-io;
    export pipeline;
}
```

**Decisions / departures from plan text:**
1. Plan lists top-level funcs on the guest; I grouped them into an `interface pipeline` so the world export is `export pipeline` — matches cargo-component conventions and gives cleaner bindgen names.
2. Added `run-metrics` sub-record (plan said "rows+metrics" loose). Explicit fields keep the ABI stable and give the host a concrete shape to feed prometheus without parsing free-form strings.
3. Added `run-error` as a variant with three arms (retryable / permanent / schema-mismatch) instead of plan's flat `run-error`. Host needs the distinction to decide release-vs-ack on a claim — collapsing loses information.
4. `host-io::metric` takes `f64` — counter/gauge/histogram type is a host concern (prometheus labels). Keeps the WIT surface small.
5. NO clock / NO filesystem / NO network imports. Plan says "host owns IO"; this enforces it at the WIT level rather than trusting the guest.
6. `config: string` (JSON) not `config: list<tuple<string,string>>`. YAML `config:` block is already JSON-compatible; guest parses once in `init`.
7. `init` separate from `describe` — host calls describe first (cheap, no state), validates, then calls init (may allocate). Plan implied this ordering but didn't require the split.
8. **run-batch must not emit `schema-mismatch`** — enforced by comment invariant in the WIT. Fingerprint mismatch is a cold-start concern (describe/restore only). Guest macro folds any mid-batch schema error into `permanent`.

**PcsError → run-error mapping (FROZEN 2026-04-15, wasm-guest + dist-expert + team-lead + wasm-lead aligned):**
- `retryable` ← `RetryExhausted`, `SystemExecution` **when returned as a value by the guest** (`Ok(Err(SystemExecution))` through the WIT `result<_, run-error>` — the guest chose to signal retry)
- `permanent` ← `ComponentNotFound`, `ResourceNotFound`, `EntityNotFound`, `Configuration`, `Scheduler`, `Store`, `Generic`
- `permanent` ← **also** `SystemExecution` **when synthesized by the host wrapper from a guest trap** (`wasmtime::Error` caught at `WasmPipelineRuntime::run_on`, see trap semantics section). This is a trap-specific override of the normal `SystemExecution → retryable` rule. Coder-host must implement the distinction in #14: guest-value-returned errors take the normal path; host-synthesized-from-trap errors take the override path. Easiest implementation: synthesize `PcsError::system_execution("guest trap: ...")` and set a trap-source flag that the bucket-assigner checks, OR use a separate error path entirely (e.g. direct `run-error::permanent` construction without going through `PcsError` at all).
- `schema-mismatch` → emitted only from `restore()`, never `run-batch` (invariant in WIT comment).
- `LeaseExpired` → dropped from guest-emit bucket. Guests don't own leases; if one is ever synthesized by the macro it's a bug — rewrite to `permanent("guest emitted LeaseExpired — bug, guest does not own a lease")`.
- `host-denied` → **NOT in v0.1.0.** Team-lead overrode the earlier reservation decision on 2026-04-15: v0.1.0 is a three-arm variant only. When v0.2 grows the host-io surface (filesystem/network), the WIT minor bump adds the variant cleanly; reserving a dead slot now is speculative. Macro has no arm for it.

**Why `Generic` → `permanent` (not retryable):** `Generic` is the user-code catch-all. Defaulting unknown errors to `retryable` means every unhandled guest exception re-releases the claim forever via the service tick loop — loudly losing one batch is strictly better than silently re-processing it every tick. Guest authors who want retry semantics must construct `PcsError::SystemExecution` or `PcsError::RetryExhausted` explicitly. "Unknown → retry forever" is the wrong default for a catch-all variant.

**Runner re-entry mechanism (verified 2026-04-15, `src/distributed/runner.rs:393-414`):** On `(Some(e), false)` the runner calls `release_with_log` AND `return Err(e)` (line 406), unwinding out of `run()`. The runner does NOT silently re-loop on the same batch inside its inner `loop { }`. The hazard for infinite-retry-on-unknown lives at the **service tick loop** that re-invokes the runner on the next interval — same outcome (batch cycles Pending→Claimed→Pending→Claimed→...) but at tick cadence, not inner-loop cadence. #23's counter-on-MasterBatchRecord design is correct because the counter persists across tick invocations in the Raft state machine — the motivation framing in #23's description ("inside the runner's inner loop") needs correction but the design itself holds.

**Trap semantics (LOCKED 2026-04-15, Option A — dist-expert + team-lead + wasm-lead):**
`WasmPipelineRuntime::run_on` catches every `wasmtime::Error` and synthesizes `PcsError::SystemExecution("guest trap: {detail}")` which maps to **`permanent`** in v0.1.0.

The trap taxonomy splits three ways and only one arm is legitimately transient:
1. **Guest logic bugs** (`unreachable!()`, divide-by-zero, `panic!()`, array OOB) — re-trap deterministically on replay with same input. **Permanent.**
2. **Resource exhaustion** (OOM, stack overflow, epoch timeout) — might succeed under different load. Retryable in principle, but only safe with a cap.
3. **Wasmtime infrastructure failures** (JIT compile failure, module init, linker errors) — host-side, won't improve on retry of same module. **Permanent.**

Categories 1 and 3 dominate empirically; category 2 is the minority. Mapping all traps to `retryable` inverts the probability mass and creates an infinite tick-loop for category 1/3 failures because v0.1.0 ships with `max_claim_releases = u32::MAX` until #24 lands. No safety net.

Therefore: v0.1.0 flat-maps all traps to `permanent`. Fail loud, single-batch loss, operator resubmits. Epistemically consistent with `Generic → permanent` — host cannot discriminate transient from terminal, so default to safe loss.

**Revisit path:** task #25 ("Split trap mapping by wasmtime::Trap kind") is filed and blocked on #24 + #14 + #19. Once #24's cap is enforced by default, #25 downcasts the `wasmtime::Error` and routes `Trap::OutOfFuel` / `StackOverflow` / `MemoryOutOfBounds` to `PcsError::RetryExhausted → retryable`, leaving logic traps and infrastructure failures on `permanent`. Category 2 becomes safe because the cap bounds the blast radius.

**Macro-side defensive catches (wasm-guest, captured for coder-host's #14 planning):**
The guest SDK macro reduces trap surface area by converting common `.unwrap()`-equivalents into structured `Err(String)` / `RunError::Permanent(msg)` returns **before** they panic-unwind. This means the trap path is the exception rather than the rule for common error modes. Five paths identified:

1. `Dataset::read_ipc(&input[..])` (host sent malformed IPC) → `.map_err(|e| RunError::Permanent(format!("ipc decode: {e}")))`
2. `Dataset::write_ipc(&mut buf)` (buffer OOM) → `.map_err` → `Permanent`
3. `serde_json::from_str(config)` in `init()` (non-JSON config) → return `Err(String)` directly; WIT `init` already has `result<_, string>` return type
4. User's `build()` panic during `OnceLock<Mutex<Pipeline>>` lazy init — NOT caught. `catch_unwind` is weight and `build()` panicking is rare programming error. Falls through to host trap catcher → `permanent`.
5. User `.unwrap()` inside their `System::run` — unavoidable from SDK side. Documented in `pcs-guest` crate doc: author should return `Err(PcsError::...)` explicitly instead.

Net effect on the host bucketing: common macro-internal errors arrive as structured `RunError::Permanent` via the normal `Ok(Err(...))` WIT path — NOT via trap-synthesis. Both still land in `permanent`, so the operator experience is consistent regardless of which path surfaced the error. The trap-specific-override only fires on paths 4 and 5 (user-authored code panics), where it's the correct fallback for bugs the SDK can't anticipate.

**Host wrapper trap handling (for task #14 planning):**
`WasmPipelineRuntime::run_on` MUST catch every `wasmtime::Error` (traps, epoch interruption, linker failures) exhaustively and translate to `Err(PcsError::system_execution("guest trap: {detail}"))`. NEVER let a panic unwind into the runner's tokio `select!` at runner.rs:343 — the sibling renewal branch won't drop cleanly and `release_with_log` at line 405 won't run. Dist-expert's reasoning: "the `release_with_log` call immediately after won't run because we'd be in an unwind, not an `Err` return." This is a #14 acceptance criterion, not a WIT concern — but flag it in the WIT doc's "host integration notes" section so #14 gets it.

**#19 chaos test assertion shape (agreed with dist-expert):**
Assert on the observable outcome, not the trap type. Template off `test_checkpoint_failure_releases_not_acks` at runner.rs:741.
1. Guest unconditionally traps on 2nd batch (`unreachable!()` or divide-by-zero).
2. Run `DistributedRunner` through `WasmPipelineRuntime` via `Box<dyn PipelineRuntime>`.
3. Assert: first batch acked, second batch `release_count == 1 && ack_count == 0`, runner returns `Err(PcsError::...)`.
4. Optional: assert PcsError message contains "trap" or "guest" so regressions don't silently reclassify traps as success.

**Task #24 (replaces deleted #23, decoupled from #19):** "Add claim-level retry cap to DistributedRunner". Counter on `MasterBatchRecord.release_attempts: u32` (state machine, not claim), reset-on-ack, increments in both `apply_release_claim` and `apply_reclaim_expired`, versioned `StoredMasterBatch` enum for postcard migration, runner-local cap enforcement via idempotent `ConsensusCommand::PoisonBatch { batch_id }`, `RunnerConfig::max_claim_releases: u32` default 5 (`0` = unlimited). Owned by dist-expert. Blocked on #10. Does NOT block #19 — chaos test ships with `max_claim_releases = u32::MAX` as default. ~1 week scope.

**Task #22** (owned by coder-tests, blocked on #3 — now unblocked as of 2026-04-15): extend `RunStats` with `retries_this_batch: u32` so `run-metrics.retries` can be populated from `Pipeline::last_stats()` after `run_on` in the `export_pipeline!` macro expansion.

**v0.1.0 scope boundary (team-lead locked, 2026-04-15):** standalone WASM only. Distributed WASM (guest running under `DistributedRunner`) is a future phase. The WIT v0.1.0 does NOT need to carry distributed-only runtime context across the boundary.

**Deferred to v0.2 — KeyPartition and TypeId-keyed resources:**
- `DistributedRunner` currently delivers `KeyPartition` to a pipeline via `Dataset::insert_resource::<KeyPartition>()` at `src/distributed/runner.rs:251`. `Resource` is TypeId-keyed and cannot cross an IPC boundary — there is no stable wire identity for a Rust `TypeId`.
- For distributed WASM guests (v0.2), runtime context must flow via one of:
  1. `init(config: string)` extended to accept a per-batch runtime-context JSON blob (re-called at the top of `run-batch`? or a separate `set-runtime-context(json)` function?).
  2. A reserved component row in the input IPC (e.g. component `__pcs_runtime__` with one row carrying KeyPartition as columnar data). Cleaner: reuses existing Arrow machinery, no new WIT surface.
  3. A new `run-batch-with-context(input, context, prior)` variant (adds one arm to the interface).
- Decision deferred to v0.2 design when distributed WASM actually becomes a task. v0.1.0 assumes no distributed-only resources need to cross.
- Action for #14 (wasmtime host integration): the `WasmPipelineRuntime` impl will NOT call `Dataset::insert_resource` on the partition dataset it hands to `run-batch`. Any resource the host injects today for native pipelines is dropped on the WASM path. Document this as a known v0.1.0 limitation in the host module.

**Phase 0 bench result (coder-bench, 2026-04-15):** 1M rows = 33ms IPC round-trip. Well under the 10% overhead threshold from the plan's risk #1. `PipelineRuntime::run_on(&mut Dataset)` trait shape is validated — no WIT redesign pressure toward zero-copy / stream<T>. Flat `ipc-bytes` on `run-batch` stays.

**Validation checklist before merging (task #12 completion):**
- [ ] `wasm-tools component wit crates/pcs-guest/wit/` parses cleanly.
- [ ] `wasm-tools validate --features component-model` on a trivial guest built against this world passes.
- [ ] bindgen-generated host types compile against `wasmtime::component` 43.0.1.
- [ ] Round-trip: a minimal guest echoes input IPC back unchanged → host reads equivalent RecordBatch.

**Open questions to surface to team-lead before finalizing:**
- Do we want `describe` to also return a minimum `required-host-io-version` field? Useful for forward-compat once host-io grows. Probably overkill for v0.1.0.
- Should `run-batch` return `ipc-bytes` or a `record { schema-fingerprint, batches: list<u8> }`? Current design assumes IPC stream already contains the schema — it does, via arrow-ipc. Keeping it flat.
