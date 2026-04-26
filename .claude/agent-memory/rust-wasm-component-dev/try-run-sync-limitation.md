---
name: pcs-core try_run_sync limitation
description: Why the pcs-guest macro must use Pipeline::run_on, not Pipeline::try_run_sync, despite team-lead's #13 spec saying otherwise
type: project
---

**Fact:** `Pipeline::try_run_sync()` at `crates/pcs-core/src/pipeline/execution.rs:637` is unworkable as the sole entry point for a freshly-`build()`'d Pipeline because it does NOT call `ensure_plan()`. Line 638: `let stages = self.stages.get()?.as_ref().ok()?.clone();` — returns `None` if stages haven't been built. The plan is built only as a side effect of `Pipeline::run`, `run_with_io`, or `run_on`. `ensure_plan` itself is `pub(super)` (`crates/pcs-core/src/pipeline/dag.rs:315`), not callable from outside the pipeline module.

**Why:** team-lead's #13 spec said "Call `Pipeline::try_run_sync()` inside run-batch (NOT async run — pcs-core guest path is sync-only via pollster)." The intent was right (avoid spinning up a tokio runtime in the guest) but the API choice was wrong — `try_run_sync` is a "run after plan is built" entry, not a complete one-call-from-fresh-state entry.

**How to apply:**
- The `export_pipeline!` macro in pcs-guest uses `pollster::block_on(pipeline.run_on(&mut dataset))` instead. `run_on` under `#[cfg(not(feature = "runtime"))]` (the guest path, see `crates/pcs-core/src/pipeline/execution.rs:548`) is rayon-free and tokio-free — it's an `async fn` driven by `pollster::block_on`, the same mechanism `try_run_sync` uses internally at line 649. Functionally equivalent execution model, but `run_on` builds the plan via `ensure_plan` and applies retry logic.
- If the macro ever needs to switch to `try_run_sync` (e.g. to skip retry-applied behavior, or to read updated `last_stats`), pcs-core needs one of: (a) make `try_run_sync` call `ensure_plan` internally, or (b) expose a public `Pipeline::build_plan(&mut self)` method. Either is a small architect-owned change. Surfaced to team-lead in the #13 completion message; not yet filed as a follow-up task.
- Related issue: neither `run_on` nor `try_run_sync` updates `Pipeline::last_stats`. The macro hardcodes `RunMetrics.systems_run = 0` and `retries = 0` for honesty rather than reading stale values. Documented in the macro body. Future fix is the same architect change — thread per-call stats through the entry point.

**Filed during #13 implementation 2026-04-15. Do not relitigate without re-reading execution.rs:637-664 and dag.rs:315.**
