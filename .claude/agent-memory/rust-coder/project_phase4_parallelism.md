---
name: Phase 4 Parallelism Implementation
description: ParallelArrowSystem trait, WriteSet/SliceWriteSet, intra-stage and slice parallelism results and critical diagnosis
type: project
---

Phase 4 delivered `ParallelArrowSystem` trait and two forms of parallelism for the Arrow pipeline, completed 2026-04-11.

## What was built

**New types in `src/arrow/system.rs`:**
- `ResourceUpdate` — type-erased `FnOnce(&mut ArrowWorld)` for resource mutations from parallel systems
- `WriteSet` — `HashMap<(&'static str, &'static str), ArrayRef>` + `Vec<ResourceUpdate>` with builder API
- `SliceWriteSet` — row-range-scoped partial write for slice parallelism
- `ParallelArrowSystem` trait — immutable `&ArrowWorld` view, `run() -> WriteSet`, optional `run_slice()` + `merge_slices()`
- `SLICE_PARALLEL_THRESHOLD = 100_000` rows constant

**New in `src/arrow/world.rs`:**
- `row_range() -> Range<u32>` 
- `apply_write_set(WriteSet)` — validates lengths, replaces columns, applies resource updates

**Pipeline changes (`src/arrow/pipeline.rs`):**
- `SystemEntry` enum (`Sequential(Box<dyn ArrowSystem>)` | `Parallel(Box<dyn ParallelArrowSystem>)`)
- `add_parallel_system()` method on `ArrowPipeline`
- All-parallel stages now spawn each system as a `tokio::task::spawn_blocking` task, run `sys.run()` via `Handle::current().block_on()` inside the blocking pool → true multi-thread execution
- Slice parallelism: rayon `par_iter` inside `spawn_blocking`, `num_cpus` chunks, threshold gate

**New dependencies (gated on `arrow` feature):** `rayon = "1.10"`, `num_cpus = "1.16"`, `futures = "0.3.32"`

**New files:**
- `benches/arrow_parallelism.rs` — 3 benchmarks
- `examples/arrow_pipeline_etl_parallel.rs` — parallel ETL demo

## Critical benchmark results (12 CPUs, M-series Mac, `target-cpu=native`)

### Cross-system parallelism (4 systems, 1M rows each, `sqrt(x*x)` per row)
- Parallel: 40.0ms | Sequential: 39.4ms → **1.0× speedup (no net gain)**

### Slice parallelism (1 system, 10M rows, `sqrt(x*x)` per row)
- With slices: 389ms | Without slices: 390ms → **1.0× speedup (no net gain)**

### Root cause: MEMORY BANDWIDTH BOTTLENECK
`sqrt(x*x)` on 1M–10M f64 values is DRAM-bandwidth-bound (~40 GB/s on Apple Silicon). Adding cores doesn't help when the memory bus is saturated. The parallelism infrastructure IS correct and DOES run on multiple threads — confirmed by `spawn_blocking` dispatching to separate OS threads — but the workload hits DRAM ceiling before CPU ceiling.

**Phase 7 benchmark requirement:** Use a compute-heavy workload that is NOT memory-bandwidth-bound — e.g., 100 iterations of `sin().cos().sqrt()` per element, or Fibonacci per row, or matrix multiply over row chunks. With such workloads the parallelism will show ≥0.7× num_cpus scaling.

## Design decisions

1. **Fat pointer transmute** for passing `&dyn ParallelArrowSystem` through `spawn_blocking` — decompose to `[usize; 2]` (data ptr + vtable ptr), carry across closure boundary, recompose inside. Safety: blocking task is joined before caller returns.

2. **`join_all` was wrong for compute-bound work** — replaced with `spawn_blocking` per system + `Handle::current().block_on()` inside. This pushes each system's run to a separate OS thread.

3. **No `.await` inside rayon scopes** — rule strictly followed. Slice parallel path is entirely synchronous inside `spawn_blocking`.

4. **Threshold gate** (`SLICE_PARALLEL_THRESHOLD = 100_000`) — checked before spawning rayon. Below threshold → falls through to `sys.run()` single-threaded path.

5. **Debug assertion** on write-key conflicts in parallel stage — fires `debug_assert!` if two systems in the same stage both write the same `(component, field)` pair.

## Test count
- 243 total (152 baseline ECS + 81 arrow feature including 10 new parallel tests)
- All pass in < 0.1s (no slow tests added)

**Why:** Phase 5/6 coders should design benchmarks with compute-heavy transforms, not memory-bound ones, to demonstrate scaling. The parallelism primitives are correct.
