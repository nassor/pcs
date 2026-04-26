---
name: Phase 2e Streaming Semantics — Watermarks, Late Data, Side-Output
description: WatermarkState, WindowContext, ProcessWindowFn update, SideOutput<DroppedLate>, late re-firing FSM, WindowedSystemBuilder.allowed_lateness
type: project
---

Phase 2e streaming semantics are complete. All modules live under `src/windows/` gated by `--features windows`.

**New modules:**
- `watermark.rs` — `WatermarkState { current_watermark: i64, allowed_lateness: i64 }`. Key methods: `advance(ts)`, `is_beyond_lateness(ts)`, `is_late_but_acceptable(ts)`, `is_on_time(ts)`. Uses `checked_sub` with `None → false` for overflow safety.

**Modified modules:**
- `function.rs` — `WindowContext { window_id, window_start, window_end, is_late_firing, watermark }`. `ProcessWindowFn::process` signature changed from `(window_id, batch)` to `(&WindowContext, batch)`.
- `result.rs` — `SideOutput<T>` generic container; `DroppedLate` tag struct; `WindowResults` gains `late_batches: Vec<RecordBatch>` and `side_output: SideOutput<DroppedLate>`.
- `system.rs` — `WindowedSystem` gains `watermark: Option<Mutex<WatermarkState>>` and `emitted_windows: Mutex<HashSet<(i64, i64)>>`. New method `apply_watermark_filter`. `aggregate_groups` returns `(schema, on_time_batches, late_batches)`.

**Builder:**
```rust
WindowedSystemBuilder::new()
    .source("Component", "ts_ms")
    .window(WindowSpec::Tumbling { size_ms: 2000, offset_ms: 0 })
    .function(WindowFunction::Reduce { input_field: "value", aggregate: ReduceAggregate::Sum })
    .allowed_lateness(1000)  // enables watermark tracking; 0 = drop all out-of-order
    .build()?;
```
Without `.allowed_lateness()`, watermark is disabled (legacy batch mode).

**Watermark semantics:**
- Classify rows using PRE-ADVANCE watermark (from previous batch), THEN advance from current batch timestamps. This prevents on-time rows in the same batch from being incorrectly classified as late.
- `is_beyond_lateness` uses linter's guard: `if allowed_lateness >= current_watermark { return false; }` (treated as infinite tolerance when lateness > watermark).
- `emitted_windows` tracks `(window_id, key_hash)` pairs for re-firing detection across runs.

**Key gotchas:**
- Early-return path (all rows dropped): put `dropped_side_output` INTO `WindowResults.side_output`, not as a separate world resource. Test reads `results.side_output`, not `world.get_resource::<SideOutput<DroppedLate>>()`.
- `ProcessWindowFn::Process` now WORKS (previously returned error). `prepare_aggregate_inputs` builds a sorted source batch for Process variants; placeholder schema is `Schema::empty()`.
- Window re-firing: the `is_late_firing` flag indicates the WINDOW was previously emitted, NOT that the incoming row is late. A late-but-acceptable row going to a window that hasn't fired yet still results in an on-time first firing (goes to `batches`, not `late_batches`).

**Test counts:**
- lib tests: 218 pass
- integration tests (tests/windows_integration.rs): 21 pass (added 6 Phase 2e tests)
