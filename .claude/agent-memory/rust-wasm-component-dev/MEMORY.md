# Memory Index

- [Task 13 scope](task-13-pcs-guest-sdk.md) — **#13 COMPLETED 2026-04-15.** pcs-guest SDK rlib + pcs-guest-smoketest cdylib shipped; export_pipeline! macro live; spec-correction on try_run_sync surfaced to team-lead.
- [export_pipeline macro design](export-pipeline-macro-design.md) — macro wiring, final PcsError→run-error mapping, defensive catches, bindgen path layout
- [cargo-component cdylib workflow](cargo-component-cdylib-workflow.md) — two-crate layout Cargo.toml sketches, build cmds, profile flags
- [Try_run_sync limitation](try-run-sync-limitation.md) — pcs-core try_run_sync requires a pre-built plan; ensure_plan is pub(super); guest macro must use run_on instead
- [Pipeline run_on shape](pipeline-run-on-shape.md) — run_on signature, final error mapping, trap override rules, runner.rs:406 mechanism
- [Atomic manifest rule](team-rule-atomic-manifest.md) — team rule: workspace manifest must parse at every intermediate commit
- [Respect aborts rule](team-rule-respect-aborts.md) — team rule: halt immediately on abort, even mid-edit
- [Design review rule](team-rule-design-review.md) — team rule: ping owner + team-lead before editing another agent's in-flight design
