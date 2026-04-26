---
name: ECS Performance Research Findings
description: Key optimization patterns from production Rust ECS libraries applicable to Canudo's processing engine
type: project
---

ECS performance research applied to Canudo's pipeline and system execution.

**Why:** The ecs-rewrite branch introduced ECS-style entity-component storage. Understanding production ECS patterns (bevy_ecs, hecs, specs, shipyard) helps prioritize optimizations.

**Key findings (as of 2026-04-11):**

1. **TypedColumn<T> storage (IMPLEMENTED in world.rs)**: Replacing `Vec<Option<Box<dyn Any>>>` with `TypedColumn<T>` (typed `Vec<Option<T>>` behind `dyn AnyColumn`) eliminates per-entity heap allocation. This was the single biggest win: 37-66% improvement across all benchmarks.

2. **TypeId no-op hasher (ALREADY DONE)**: `TypeIdHasher` avoids re-hashing TypeId values that are already internally hashed. Used by bevy, hecs.

3. **Flat execution plan (IMPLEMENTED in pipeline.rs)**: Replaced `Vec<Vec<usize>>` stage representation with flat `run_order` array + `stage_offsets`. Marginal improvement for small pipelines but better cache behavior for larger ones.

4. **Stack-allocated level computation (IMPLEMENTED in pipeline.rs)**: For pipelines with <= 16 systems, Kahn's algorithm uses stack arrays instead of heap Vecs, avoiding 5-6 allocations during plan construction.

5. **`run_sync` fast path (IMPLEMENTED in system.rs)**: Optional synchronous execution method on `System` trait that bypasses `#[async_trait]` boxing overhead. `FnSystem` implements it automatically.

6. **async_trait overhead**: Cannot be eliminated for `dyn System` because native async fn in traits is not object-safe. The `run_sync` opt-in is the mitigation.

7. **Archetype storage**: Production ECS engines (bevy) group entities by component set for cache-friendly iteration. Not implemented in Canudo; current column storage is adequate for workflow-scale entity counts.

8. **Unsafe pointer casts for column access (IMPLEMENTED in world.rs)**: Replaced safe `downcast_ref`/`downcast_mut` with `unsafe` pointer casts in `get`, `get_mut`, `insert`, `remove`, `typed_column`, `get_column_mut`. The TypeId HashMap key already guarantees the concrete type, making the runtime TypeId comparison redundant.

9. **`all_no_retry` fast path (IMPLEMENTED in pipeline.rs)**: When all systems use `SystemConfig::minimal()`, the pipeline skips the per-system retry branch entirely in `run_no_retry()`. Eliminates one conditional per system in the hot loop.

10. **`try_run_sync()` on Pipeline (IMPLEMENTED in pipeline.rs)**: Fully synchronous pipeline execution that bypasses async runtime overhead when all systems implement `run_sync`. Returns `None` if any system needs async. Measured ~100ns faster than async path for small pipelines.

11. **Benchmark isolation with `iter_batched` (IMPLEMENTED in benchmarks)**: Separating world setup from measured execution reveals that ~40% of the "system_1000_entities" benchmark time is world construction. Pipeline benchmarks now have `run_only` and `sync_only` variants for clean measurement.

12. **Sparse set vs dense Vec**: Dense `Vec<Option<T>>` (column storage) is optimal for iteration-heavy workloads. Sparse sets trade iteration speed for fast add/remove. Canudo's column storage is the right choice for workflow-scale entity counts.

13. **HashMap vs Vec for small N (TypeIdSet)**: For N < ~8-12 elements, sorted Vec with linear scan beats HashMap. The `TypeIdSet` with inline capacity 2 is already optimal for the common case of 0-2 component type declarations per system.

**How to apply:** The primary per-entity access bottleneck (downcast overhead) is resolved. Pipeline overhead is minimal (~100ns per system call). Remaining optimization opportunities: (a) archetype storage for cache-friendly multi-component iteration at large entity counts, (b) bulk insert APIs to avoid per-entity HashMap lookups when creating new components, (c) parallel stage execution when `System::run` gains `&World` support.
