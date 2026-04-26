---
name: Pass 2 World→Pipeline Rename
description: Two-pass rename complete: World→Pipeline (data container), old Pipeline→Scheduler (executor). Key type names and file locations after rename.
type: project
---

Pass 2 of the two-pass rename is complete (branch: fixes).

**Renamed types:**
- `World` → `Pipeline` (columnar data container, `src/pipeline.rs`)
- `WorldBuilder` → `PipelineBuilder`
- `drain_into_world` → `drain_into_pipeline`
- `drain_world` → `drain_pipeline`

**Renamed files:**
- `src/world.rs` → `src/pipeline.rs`
- `benches/world.rs` → `benches/pipeline.rs`

**Preserved intentionally:**
- `PipelineSpec` struct and YAML `pipeline:` key in `src/service/config.rs` — describes the abstract workload, not the `Pipeline` Rust type
- `__canudo_component` Arrow IPC metadata key — serialization format, breaking it breaks persistence

**Key module paths after rename:**
- `crate::pipeline::Pipeline` (was `crate::world::World`)
- `crate::pipeline::PipelineBuilder` (was `crate::world::WorldBuilder`)
- `crate::scheduler::Scheduler` (was `crate::pipeline::Pipeline` before Pass 1)

**Why:** The original `World` name carried ECS/game-dev connotations; `Pipeline` better describes a columnar data container that data flows through. The original `Pipeline` was actually a DAG scheduler, now named `Scheduler`.

**How to apply:** All new code uses `Pipeline` for the data container and `Scheduler` for the DAG executor. The `pipeline:` YAML key and `PipelineSpec` struct remain unchanged for config backward compatibility.
