---
name: Canudo ECS Rewrite
description: Summary of the ECS rewrite completed on the ecs-rewrite branch — new module structure, API, and conventions
type: project
---

Canudo was rewritten from an FSM-based workflow engine to an ECS-style async processing engine on the `ecs-rewrite` branch. The rewrite is complete as of 2026-04-11.

**Why:** Architectural shift from FSM (Task/Node/Workflow) to ECS (Entity/Component/System/Pipeline) for better composability and type-safe data flow.

**New module structure:**
- `src/world.rs` — `Entity`, `World`, `Component`, `Resource`, `WorldQuery`, `Read<T>`
- `src/system.rs` — `System` trait, `SystemMeta` (declares read/write access patterns)
- `src/pipeline.rs` — `Pipeline` with DAG-based stage scheduling and per-system retry
- `src/retry.rs` — `RetryMode`, `SystemConfig`, `run_with_retries`
- `src/error.rs` — `CanudoError` with ECS variants: `SystemExecution`, `ComponentNotFound`, `EntityNotFound`, `ResourceNotFound`, `Pipeline`, `Configuration`, `RetryExhausted`, `Generic`
- `src/scheduler.rs` — `Scheduler` wrapping `Pipeline` (not `Workflow`)
- `src/store/` — unchanged `MemoryStore`/`KeyValueStore`

**Old modules removed:** `src/task.rs`, `src/node.rs`, `src/workflow.rs`

**Prelude exports:** `CanudoError`, `CanudoResult`, `Entity`, `KeyValueStore`, `MemoryStore`, `Pipeline`, `Read`, `RetryMode`, `System`, `SystemConfig`, `SystemMeta`, `World`, `async_trait`

**How to apply:** When working on this codebase, use the ECS API. `Workflow`, `Task`, `Node` are gone. Systems implement `System` trait with `meta()` + `async run(&self, world: &mut World)`. Pipelines are built with `Pipeline::new()` + `pipeline.add_system(...)` + `pipeline.run(&mut world).await`.
