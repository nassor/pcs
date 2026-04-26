---
name: Team rule — ask before editing other agents' in-flight designs
description: Stop and ping the owner + team-lead before modifying code belonging to another agent's in-flight task
type: feedback
---

**Rule:** If I'm working on task X and need to modify code that belongs to in-flight task Y owned by another agent, **stop, SendMessage the owner + team-lead, wait for ack**. Do NOT unilaterally edit trait definitions, public API surfaces, design-review artifacts, or anything else another agent is in the middle of.

**Exceptions** (edit directly, no ping required):
- Stale build state from a prior merge: dead `[[bench]]`/`[[bin]]`/`[[test]]` entries, orphaned imports after `git mv`, cargo manifest tidy-up that follows obviously from someone else's already-completed work.
- Trivial typo fixes in comments/doc.
- My own task's internal code (obviously).

**Everything else**: ask first. Especially load-bearing types:
- Public trait definitions (bounds, supertraits, associated types, method signatures)
- Struct fields on owned types from another task
- Error enum variants
- Public module/crate exports
- YAML schema or WIT interface shapes being authored by someone else
- Config types threaded through multiple crates

**The right protocol:**
```
SendMessage <owner> + team-lead:
  summary: "need to modify <item> during <my task>"
  message: |
    hit <error> in <file> during <task>.
    proposed fix: <diff or description>.
    impacts <downstream agents>.
    OK to apply?
```

**Why:** Team-lead flagged this 2026-04-15 after coder-bench hit a `Sink: !Sync` compile error during #5 and fixed it by modifying architect's in-flight `PipelineRuntime` trait from #9 (`?Send` + drop `Sync`) without pinging. Fix was probably correct but broke architect's ownership of #9 and forced coder-tests, dist-expert, and architect all to re-validate the trait shape. 30 seconds of coordination avoids 30 minutes of rework.

**How to apply (for task #13 specifically):**
- The WIT file (`crates/pcs-guest/wit/pipeline.wit`) is wasm-lead's #12 deliverable. I author it as part of #13 only after #12 completes and wasm-lead hands off. Do NOT preemptively drop text into that file during prep.
- The `PipelineRuntime` trait is architect's #9 deliverable. pcs-guest uses `Pipeline::run_on` directly (per plan), not the `PipelineRuntime` trait. If something in #13 needs the trait shape to change, STOP and ping architect + team-lead.
- The `RunStats.retries_this_batch` field (#22) is coder-tests's deliverable. If my macro needs a different field shape, STOP and ping coder-tests + team-lead.
- Coder-host owns `WasmPipelineRuntime::run_on` in #14 — including the trap synthesis + permanent override. If my macro interacts with that path (it shouldn't; traps bypass the macro entirely), STOP and ping coder-host + team-lead.
- Manifest edits: follow the atomic-manifest rule — one commit with both crate dirs + `[workspace.members]` addition. Don't leave intermediate state visible.

**Meta-principle:** idle time is cheap, coordination churn is expensive. If unsure whether an edit crosses the line, ask.
