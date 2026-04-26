---
name: Canudo API Surface
description: Current public API (v1.0.0-alpha.1) — Arrow-based columnar engine, key types, SystemMeta method names, prelude contents
type: project
---

## Version

1.0.0-alpha.1 — Arrow-native rewrite. Old ECS/FSM/Task/Node/Workflow API is entirely removed.

## Core Traits (src/system.rs, src/component.rs)

- `Component` — `name() -> &'static str` + `schema() -> Arc<Schema>`. Derives `Serialize + Deserialize`.
- `System` — `meta(&self) -> SystemMeta` + `async run(&self, world: &mut World)` + optional `run_sync`. `#[async_trait]`.
- `ParallelSystem` — slice-parallel variant; `run_slice(&self, world: &World, range: Range<usize>) -> (WriteSet, Vec<ResourceUpdate>)`.
- `Source` / `Sink` — IO traits (feature `io`). `Source::next_batch()` / `Sink::write_batch()`.
- `PartitionSource` / `CheckpointStore` — distributed traits (feature `distributed`).

## Key Types

- `World` — columnar container. Methods: `register_component::<C>() -> Result<(), CanudoError>`, `register_raw_component(name: &'static str, schema: Arc<Schema>)`, `append::<C>(&[C]) -> Result<Range<Row>, CanudoError>`, `column::<C>(field: &str) -> Option<&ArrayRef>`, `columns::<C>() -> Option<&RecordBatch>`, `replace_batch::<C>(batch: RecordBatch) -> Result<(), CanudoError>`, `rows() -> usize`, `live_rows() -> usize`, `mark_dead(row: Row)`, `compact() -> Result<(), CanudoError>`, `write_ipc(&mut W)`, `read_ipc(&mut R) -> Result<Self>`, `insert_resource::<R>(r)`, `get_resource::<R>() -> Option<&R>`, `get_resource_mut::<R>() -> Option<&mut R>`.
- `SystemMeta` — field-granular metadata. Builder methods: `.read(component: &'static str, field: &'static str)`, `.write(component, field)`, `.read_component(component)`, `.write_component(component)`, `.read_resource::<R>()`, `.write_resource::<R>()`.
- `system_fn(meta: SystemMeta, f: impl Fn(&mut World) -> Result<(), CanudoError> + ...) -> impl System` — closure-based system.
- `Pipeline` — `.add_system(s)`, `.run(&mut world) -> Result<(), CanudoError>`, `.stages() -> Option<Vec<...>>`.
- `CanudoError` / `CanudoResult<T>` — error type. Variants: `SystemExecution`, `ComponentNotFound`, `EntityNotFound`, `ResourceNotFound`, `Store`, `Pipeline`, `Configuration`, `RetryExhausted`, `Generic` (+ `Distributed`, `LeaseExpired` with `distributed`).
- `RetryMode` — `None`, `Fixed`, `ExponentialBackoff`. `SystemConfig` wraps it; returned by `System::config()`.
- `Row` — stable row index (u32 generation). Invalidated by `compact`.
- `SchemaRegistry` — returned by `world.schemas()`.

## CRITICAL: SystemMeta method names

- Field-level: `.read("Component", "field")` and `.write("Component", "field")` — takes `&'static str`, NOT generics.
- Component-level: `.read_component("Component")` and `.write_component("Component")` — takes `&'static str`.
- There is NO `.read_component_field::<T>()` or `.write_component_field::<T>()` — these names do NOT exist.
- There is NO `world.len::<T>()` — use `world.rows()` or `world.live_rows()`.
- Column downcast pattern: `col.as_any().downcast_ref::<Float64Array>()` — NOT `.as_primitive::<Float64Type>()`.

## Feature Flags (Cargo.toml)

- `tracing` — tracing spans
- `io` — Source/Sink, Parquet/CSV/JSON/channel; `DataFusionSource`, `ParquetCheckpointStore`
- `datafusion` — implies `io`; adds DataFusion integration
- `distributed` — `PartitionSource`, `CheckpointStore`, `DistributedRunner`, `RedbSharedStore`, TCP transport
- `distributed-raft` — implies `distributed`; adds openraft: `ArrowRedbLogStore`, `ArrowRedbStateMachine`, `ArrowRaftDriver`, `TcpNetworkFactory`
- `service` — implies `io` + `distributed` + `tracing`; adds axum HTTP control plane, YAML config, Prometheus, `canudo-service` binary. Does NOT include `distributed-raft`.
- `service-cluster` — implies `service` + `distributed-raft`

## Prelude (`canudo::prelude::*`)

`CanudoError, CanudoResult, Component, FieldAccess, ParallelSystem, Pipeline, ResourceUpdate, RetryMode, Row, SchemaRegistry, SliceWriteSet, System, SystemConfig, SystemMeta, World, WriteSet, system_fn, async_trait`

## Distributed distributed re-exports (canudo::distributed::*)

`Checkpoint, CheckpointStore, RedbSharedStore, BatchClaim, MAX_LOG_ENTRY_BYTES, PartitionSource, DistributedRunner, RunnerConfig, CheckpointStrategy, ParquetCheckpointStore` (if `io` enabled)

## IO module paths

- `canudo::io::datafusion_source::DataFusionSource` (feature `datafusion`)
- `canudo::io::source::{Source, drain_into_world}` (feature `io`)
- `canudo::io::sink::Sink` (feature `io`)
- `canudo::distributed::parquet_checkpoint::ParquetCheckpointStore` (feature `io` + `distributed`)
