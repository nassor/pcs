---
name: Task 13 blocking chain
description: Full upstream dependency chain for task #13 (pcs-guest SDK)
type: project
---

**Fact:** Task #13 is blocked by #12 (WIT design). #7 (pcs-core guest feature) — the other historical blocker — **completed 2026-04-15**. All of Phase 1 is now done or in flight; Phase 2 is starting.

**Why:** #13 needs the WIT file (from #12) to drive cargo-component. The pcs-core `guest` feature (#7) is now in place, so compiling pcs-guest for `wasm32-wasip2` is unblocked on that side.

**How to apply:** While blocked, poll TaskList; don't start coding until #12 resolves. Prep work done: export_pipeline macro sketch, cargo-component workflow, final error mapping with defensive catches, atomic-manifest rule noted.

**Upstream chain as of 2026-04-15:**

```
#13 (pcs-guest SDK)        ← blocked only on #12
└── #12 (WIT design — pcs:pipeline@0.1.0)   ← pending, ready to claim when #11 unblocks
    └── #11 (BuiltService downcast hatch)    ← pending
        └── #9 (PipelineRuntime trait)        ← in_progress
            └── #4 (service/io/distributed move)  ← completed
                └── (Phase 1 modules)             ← all completed
```

**Phase 1 status (2026-04-15):** #1, #2, #3, #4, #6, #7, #8, #22 all completed. #5 in_progress (bench split). Phase 1 essentially done.

**Phase 2 status:** #9 in_progress (PipelineRuntime trait — architect). Once it lands, #10 and #11 can start in parallel. After #11, wasm-lead can claim #12. My realistic unblock: when #9 → #11 → #12 lands. Order of magnitude of days, not weeks.

**wasm-guest role:** stay idle, poll, stay ready. When #12 lands: claim #13, pull wasm-lead's finalized `wit/pipeline.wit` from agent memory or wherever it ships, scaffold both crates in ONE atomic commit (per team-rule-atomic-manifest.md), verify `cargo component build -p pcs-guest-smoketest --target wasm32-wasip2` + `wasm-tools validate --features component-model`, then mark complete.
