---
name: Phase 5 Source/Sink IO Layer
description: Source/Sink traits, format implementations (Parquet/JSON/CSV/Channel), CastingSource, pipeline integration; design decisions and gotchas
type: project
---

Phase 5 delivered the ingestion/egress layer under `src/arrow/io/`. Gated on new `arrow-io` feature that pulls in `arrow-json`, `arrow-csv`, `parquet` crates at version `=58.1.0`.

**Test result:** 243 → 277 tests (34 new arrow-io tests). All pass.

**Files created:**
- `src/arrow/io/mod.rs` — module root, re-exports
- `src/arrow/io/source.rs` — `Source` trait + `drain_into_world` helper
- `src/arrow/io/sink.rs` — `Sink` trait + `drain_world` helper
- `src/arrow/io/channel_source.rs` / `channel_sink.rs` — mpsc-backed in-memory transport
- `src/arrow/io/json_source.rs` / `json_sink.rs` — NDJSON via arrow_json
- `src/arrow/io/csv_source.rs` / `csv_sink.rs` — CSV via arrow_csv
- `src/arrow/io/parquet_source.rs` / `parquet_sink.rs` — Parquet via parquet crate
- `src/arrow/io/cast.rs` — `cast_batch()`, `CastingSource`, `build_target_schema()`
- `examples/arrow_pipeline_parquet_etl.rs` — end-to-end example

**Files modified:**
- `Cargo.toml` — added arrow-io feature + 3 deps + example block
- `src/arrow/mod.rs` — added `pub mod io;`
- `src/arrow/pipeline.rs` — added sources/sinks fields to ArrowPipeline, add_source/add_sink/run_with_io methods
- `src/arrow/world.rs` — added `append_record_batch`, `register_raw_component`, `batch_for`
- `src/arrow/schema.rs` — added `register_raw`

**Key design decisions:**
1. `drain_into_world`/`drain_world` use `?Sized` bound so they work with `Box<dyn Source>` via coercion.
2. `ParquetSink` wraps `ArrowWriter` in `Mutex<Option<...>>` because ArrowWriter is Send but not Sync; async_trait requires Sync.
3. `ArrowWriter::close()` is used in `finish()` (not `.finish()`) — the parquet crate uses `close` for the consuming finaliser.
4. All source implementations buffer entire file in memory at construction (Phase 5 simplification). Streaming deferred to later phase.
5. `SchemaCast` redesigned as a pure function `cast_batch()` + `CastingSource<S>` wrapper, NOT a system — because ArrowWorld schemas are immutable after registration.
6. The example uses a `PaddedParquetSource` inner struct to show how to adapt schemas at the Source layer.

**Gotchas for Phase 6/7:**
- `arrow_json::reader::infer_json_schema` takes `usize` not `u64` for the limit parameter.
- `#[async_trait]` requires `Send + Sync` on the impl type; any non-Sync field must be wrapped in `Mutex`.
- `CsvSink` relies on BufWriter drop for flush; no explicit `finish()` call needed on arrow_csv::Writer.
- The `arrow-distributed` cfg warning in `src/arrow/mod.rs` is pre-existing (Phase 6 planning stub).
