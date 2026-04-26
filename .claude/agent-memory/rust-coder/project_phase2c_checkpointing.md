---
name: Phase 2c Distributed Window Checkpointing
description: WindowAccumulator component, accumulator_store helpers, runner integration, partition filter — Phase 2c completed
type: project
---

WindowAccumulator component added as a `Component` stored in World for durable cross-run aggregate state.

**Key design decisions:**
- `ACCUMULATOR_STAGE_SENTINEL = u32::MAX` in `src/distributed/checkpoint.rs` — no upper-bound check in state machine
- `KeyPartition` resource injected into world by runner; read by `WindowedSystem` for partition filtering
- Accumulator save failure follows release-not-ack path (same as stage checkpoint failures)
- `World::write_component_ipc<C>` serializes single component to IPC (keeps payload small)
- `migrate_to_current(batch)` handles schema versioning: `version=None` treated as v1, `version>1` rejects

**New files:**
- `src/windows/accumulator.rs` — `WindowAccumulator` + `Component` impl + `migrate_to_current`
- `src/distributed/accumulator_store.rs` — `load_accumulator_state` / `save_accumulator_state`

**Modified files:**
- `src/windows/mod.rs` — added `pub mod accumulator`
- `src/windows/system.rs` — partition filter block (`#[cfg(feature = "distributed")]`), `flush_accumulator` method
- `src/distributed/mod.rs` — re-exports `KeyPartition`, `ACCUMULATOR_STAGE_SENTINEL`
- `src/distributed/runner.rs` — `KeyPartition` type, `partition_mask` in RunnerConfig, load/save hooks
- `src/world.rs` — added `write_component_ipc<C>` helper
- `src/lib.rs` — prelude re-exports `WindowAccumulator`, `CURRENT_ACCUMULATOR_VERSION`, `KeyPartition`

**How to apply:** When touching distributed runner or windowed systems, `KeyPartition` resource controls partition routing; `WindowAccumulator` component must be registered in world_factory for persistence to activate.

**Why:** Accumulator uses Component/World path to reuse IPC/checkpoint infrastructure; sentinel stage_idx avoids new CheckpointStore API; partition filter inside WindowedSystem keeps runner generic.
