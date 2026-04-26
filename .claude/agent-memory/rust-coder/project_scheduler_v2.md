---
name: Scheduler V2 (Task #20)
description: Cross-pipeline DAG scheduling with DependencyKind, priorities, backpressure, and RunStats
type: project
---

Scheduler V2 completed. Key additions:

**New types in `src/scheduler.rs`:**
- `DependencyKind::Order` (run after) vs `DependencyKind::Data` (skip if predecessor produced 0 rows)
- `BackpressureSpec::Predicate(Box<dyn Fn(&Pipeline) -> bool + Send + Sync>)` — `#[cfg(feature = "io")]` `BackpressureSpec::Channel { component, max_pending }`
- `PipelineConfig { dependencies, priority, backpressure }` with builder methods `.after()`, `.priority()`, `.backpressure()`
- `Scheduler::add_pipeline_with_config()` added; existing `add_pipeline()` unchanged for backwards compat
- Kahn's topo sort in `build_stages()` — emits `PcsError::configuration` for unknown dep names, `PcsError::scheduler` for cycles
- `OnceLock<Result<Vec<Vec<usize>>, PcsError>>` for cached stage plan (same pattern as pipeline)

**`RunStats` in `src/pipeline.rs`:**
- `pub struct RunStats { rows_produced: isize, systems_run: usize, duration_millis: u64 }` with `#[derive(Copy, Clone, Debug, Default)]`
- `Pipeline::last_stats() -> RunStats` accessor
- `run()` sets `last_stats`; `run_with_io()` also sets it for the full IO cycle (overwrites `run()`'s copy)

**`Sink::pending_rows()` default (`src/io/sink.rs`):**
- Default returns `None`; `ChannelSink` overrides with `Some(buffer_capacity - tx.capacity())`

**`ChannelSink` (`src/io/channel_sink.rs`):**
- Added `buffer_capacity: usize` field (stored at construction time — `tx.capacity()` returns remaining, not configured)

**`Pipeline::sink_pending_rows()` (`src/pipeline/registration.rs`):**
- `#[cfg(feature = "io")]` method for backpressure probe from `BackpressureSpec::Channel`

**Prelude:** `BackpressureSpec`, `DependencyKind`, `PipelineConfig`, `RunStats` all re-exported.

**`examples/scheduler_dag.rs`** — 4-demo end-to-end example.

**Why:** BackpressureSpec has closure variant → manual `Debug` impl (prints `Predicate(<fn>)`). `BackpressureSpec::Channel` gated behind `#[cfg(feature = "io")]` — all match sites need matching cfg arm.

**How to apply:** `read_component`/`write_component` on `SystemMeta` takes `&'static str`, not a generic — `SystemMeta::new("x").read_component("MyComp")`.
