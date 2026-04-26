---
name: ECS Rewrite — ecs-rewrite branch
description: Context on the ECS rewrite completed on branch ecs-rewrite, new module structure and doc conventions established
type: project
---

The ecs-rewrite branch (completed 2026-04-11) replaced the Workflow/Task FSM model with an ECS-based architecture. All 11 tasks completed; 140 tests pass, clippy and fmt clean.

**lib.rs top-level docs written 2026-04-11** — comprehensive `//!` documentation added covering: Quick Start with two-system pipeline, Core Concepts (Entity/Component/Resource/System/Pipeline/Retry/Store), a standalone Querying section with tuple-query example, Error Handling table of variants, Module Overview table, Feature Flags section, and Getting Started commands. All 17 doc tests pass.

**New modules added:**
- `src/world.rs` — Entity, World, Component, Resource, WorldQuery, Read<T>
- `src/system.rs` — System trait, SystemMeta, SystemConfig
- `src/retry.rs` — RetryMode, run_with_retries (extracted from task.rs)
- `src/pipeline.rs` — Pipeline with topological DAG sort and parallel execution
- `src/error.rs` — updated with ECS-specific error variants

**Why:** Architectural shift from FSM-oriented workflow orchestration to ECS-based processing pipelines, enabling more composable and data-driven processing patterns.

**How to apply:** When writing docs for this codebase, the primary abstractions are now Entity/Component/Resource/System/Pipeline, not Task/Node/Workflow. The old Task/Node traits may still exist for backwards compatibility — verify before referencing them.

**Doc conventions established in world.rs, system.rs, pipeline.rs:**
- Module-level `//!` uses named concept sections with runnable doc-test examples per section
- system.rs `//!`: "What is a System?", "Retry Integration", then three examples (basic impl, data access declaration, fail-fast config)
- pipeline.rs: existing `//!` covers staging model; `Pipeline` struct doc carries a full runnable end-to-end example
- Per-method docs are a single sentence for obvious methods; 2-4 sentences for non-obvious behavior
- `# Errors` sections list each variant with a dash, condition on the same line; `run_with_timeout` covers both the timeout variant and pass-through errors from `run`
- Thread-safety guidance belongs on the struct doc, not on individual methods
- Module-level examples are fully runnable and pass `cargo test --doc`; method examples use `no_run` or `ignore` only when local types make them uncollectable
