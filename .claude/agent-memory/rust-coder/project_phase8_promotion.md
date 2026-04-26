---
name: Phase 8 Promotion Complete
description: ECS path deleted, Arrow path promoted to src/, Arrow* prefixes removed, version bumped to 1.0.0-alpha.1
type: project
---

Phase 8 completed on 2026-04-11. The ECS-style old path has been deleted and the Arrow-backed columnar path is now the only path.

**Why:** This was the point-of-no-return promotion — all Arrow* types are now canonical types with no prefix.

**Key changes:**
- `src/arrow/` directory deleted; all files promoted to `src/`
- All `Arrow*` prefixes dropped: `ArrowWorld` → `World`, `ArrowSystem` → `System`, `ArrowPipeline` → `Pipeline`, `ParallelArrowSystem` → `ParallelSystem`, `ArrowSystemMeta` → `SystemMeta`, `arrow_system_fn` → `system_fn`, distributed types similarly renamed
- Feature flags: `arrow` removed (unconditional), `scheduler`/`distributed`/`distributed-consensus`/`distributed-consensus-raft` removed; new flags: `io`, `distributed`, `distributed-raft`
- Old deleted: `src/world.rs` (ECS), `src/system.rs` (ECS), `src/pipeline.rs` (ECS), `src/store/`, `src/scheduler.rs`, `src/distributed/` (old)
- Surviving shared: `src/error.rs`, `src/retry.rs` (used by new path, updated to use `arrow-distributed` → `distributed` feature gates)
- Version bumped: `0.8.0` → `1.0.0-alpha.1`
- Migration guide: `docs/migration/0.x-to-1.0.md`

**Test matrix final state:**
- Default features: 126 tests
- `--features io`: 160 tests
- `--features distributed`: 165 tests
- `--features distributed-raft`: 172 tests
- Doc tests: 22

**How to apply:** The codebase is now at 1.0.0-alpha.1. Phase 9 (ecosystem hooks) is next.
