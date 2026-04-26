---
name: Phase 1 Windows — Tumbling + Keyed Windowed Aggregation
description: WindowedSystem builder API, Phase 1 constraints (Sum/Float64 only), integration test patterns for windows feature
type: project
---

Phase 1 windowed aggregation is complete and tested. All modules live under `src/windows/` gated behind `--features windows`.

**Module layout:**
- `spec.rs` — `WindowSpec::Tumbling { size_ms, offset_ms }` + `assign_tumbling(ts, size, offset) -> i64`
- `hash.rs` — `compute_key_hash(&[&ArrayRef]) -> Int64Array`, `compute_global_hash(n) -> Int64Array`; FNV-1a; casts all cols to Utf8
- `time.rs` — `to_ms_array(&ArrayRef) -> Result<Int64Array, CanoError>`; handles Int64/Timestamp{Milli,Second,Micro,Nano}
- `function.rs` — `ReduceAggregate` (Sum/Min/Max/Count/Mean), `WindowFunction::Reduce{input_field, aggregate}`, `WindowFunction::Process(Box<dyn ProcessWindowFn>)`
- `result.rs` — `WindowResults { schema: Arc<Schema>, batches: Vec<RecordBatch> }`; one batch per group; methods: `new(schema)`, `total_rows()`, `is_empty()`
- `system.rs` — `WindowedSystem` (implements `System` via `#[async_trait]`), `WindowedSystemBuilder`

**Builder API (`WindowedSystemBuilder`):**
```rust
WindowedSystemBuilder::new()
    .source("ComponentName", "time_field_name")   // required
    .keyed_by(&["key_field"])                      // optional; omit for global window
    .window(WindowSpec::Tumbling { size_ms, offset_ms })  // required
    .function(WindowFunction::Reduce { input_field: "value", aggregate: ReduceAggregate::Sum })  // required
    .build()  // -> Result<WindowedSystem, CanoError>
```

**Phase 1 constraints:**
- Only `ReduceAggregate::Sum` is wired end-to-end (others return `CanoError::Generic`)
- Only `Float64` value columns work for Sum (use `arrow_cast::cast` first for other types)
- `WindowFunction::Process` returns `CanoError::Generic` (unimplemented)

**Result schema:** each output batch has 3 columns: `window_id (Int64)`, `key_hash (Int64)`, `sum_<field> (Float64)`.

**Integration tests:** `tests/windows_integration.rs` (5 tests) — requires importing `use canudo::system::System` to call `.run().await` on `WindowedSystem`.

**Why:** The `System` trait in `async_trait` requires the trait to be in scope for method dispatch in integration tests.
