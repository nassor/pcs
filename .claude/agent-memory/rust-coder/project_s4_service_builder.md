---
name: Service S4: Factory Registry + ServiceBuilder
description: Phase S4 complete; registry traits, ServiceBuilder, built-in IO factories; key design decisions and gotchas recorded
type: project
---

Phase S4 delivered the factory registry and ServiceBuilder for Canudo's service layer.

**Why:** Turn YAML config (ServiceConfig from S3) into concrete runtime artifacts — World, Pipeline, Vec<BuiltSource>, Vec<BuiltSink> — without knowing concrete types at the call site.

**How to apply:** Coders building standalone runner (S5), cluster runner (S6), HTTP endpoints (S7), or CLI (S8) depend on `BuiltService` from `src/service/builder.rs`.

## Files created

- `src/service/registry.rs` (543 lines) — Four factory traits + Registry struct
- `src/service/builder.rs` (758 lines) — ServiceBuilder + BuiltService + BuiltSource/BuiltSink
- `src/service/factories/mod.rs` (91 lines) — `register_builtin_factories` convenience fn
- `src/service/factories/generic_component.rs` (263 lines) — Schema-from-YAML component
- `src/service/factories/parquet.rs` (255 lines) — ParquetSource/SinkFactory + shared `parse_schema_fields`
- `src/service/factories/json.rs` (128 lines) — JsonSource/SinkFactory
- `src/service/factories/csv.rs` (149 lines) — CsvSource/SinkFactory
- `src/service/factories/channel.rs` (181 lines) — ChannelSource/SinkFactory
- `src/service/mod.rs` (43 lines) — module re-exports

## Pipeline additions

Added to `src/pipeline.rs`:
- `add_system_boxed(&mut self, Box<dyn System>)` at line 501
- `add_parallel_system_boxed(&mut self, Box<dyn ParallelSystem>)` at line 511

## World API

No new World methods needed. `register_raw_component(&mut self, &'static str, Arc<Schema>)` already existed (Phase 5) and is used by `ComponentFactory::register` default impl. The `Box::leak` trick converts runtime String to `&'static str` for component name registration.

## Key design decisions

1. **BuiltSystem enum** — factory returns `BuiltSystem::Sequential(Box<dyn System>)` or `::Parallel(Box<dyn ParallelSystem>)`. Caller pattern-matches and calls appropriate pipeline method.

2. **ComponentFactory::register default impl** — uses `Box::leak` to produce a `&'static str` from runtime instance name. One allocation per component instance (small count, acceptable).

3. **Box<dyn Source/Sink> not Debug** — `unwrap_err()` on `Result<Box<dyn Source>, _>` fails at compile time. Use `.err().expect("...")` instead in tests.

4. **BuiltService manual Debug impl** — shows only counts (sources_count, sinks_count) since trait objects are not Debug.

5. **Shared `parse_schema_fields`** in `parquet.rs` — reused by json.rs and csv.rs via `super::parquet::parse_schema_fields`. Supports type aliases (bool/boolean, float/float32, double/float64, string/varchar/utf8).

6. **ChannelSourceFactory drops tx immediately** — produces EOF-on-first-poll source. For real use, build channel manually and bypass factory.

7. **JSON sink `from_path`** takes `(path, schema)` — unlike ParquetSink which derives schema from batches. All IO factories need `schema_fields` in config for the Sink::schema() method.

## BuiltService surface for downstream coders

```rust
pub struct BuiltService {
    pub world: World,
    pub pipeline: Pipeline,
    pub sources: Vec<BuiltSource>,  // .name, .target_component, .source
    pub sinks: Vec<BuiltSink>,      // .name, .source_component, .sink
    pub registry: Registry,         // retain for lifetime management
}
```

Standalone runner (S5): drain sources → pipeline.run(&mut world) → drain sinks → call sink.finish().
Cluster runner (S6): wrap in DistributedRunner, world is the per-batch state, sources/sinks called per batch.

## Test count

277 tests pass with `--features service`. 126 pass without (no regression).
