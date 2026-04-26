# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

PCS is a distributed batch processing engine for Rust built on Apache Arrow. Edition 2024, MSRV 1.95.0.

## Commands

```bash
cargo build                                                  # Build
cargo test --lib                                             # Run unit tests
cargo test --doc                                             # Run doc tests
cargo fmt --all -- --check                                   # Check formatting
cargo clippy --all-targets --all-features -- -D warnings     # Lint (treats warnings as errors)
cargo bench --bench pipeline                                 # Run a single benchmark
cargo check --examples                                       # Verify examples compile
cargo run --example scheduler_etl                            # Run an example
cargo run --example distributed_scheduler --features distributed  # Distributed example
cargo run --example scheduler_etl_parallel                   # Parallel stage example
cargo audit                                                  # Security audit
```

## Feature Flags

- `tracing` — integrates with the `tracing` crate for observability
- `io` — adds `Source`/`Sink` traits with Parquet, CSV, JSON, and channel support
- `datafusion` — enables DataFusion integration (implies `io`)
- `distributed` — core distributed traits: `PartitionSource`, `CheckpointStore`, `DistributedRunner`; redb storage backend; TCP transport
- `distributed-raft` — adds openraft-based log storage, state machine, snapshot, and Raft node driver (implies `distributed`)
- `service` — production binary (`pcs-service`) with axum HTTP control plane, TOML config, metrics, and standalone runner (implies `io`, `distributed`, `distributed-raft`, `tracing`)
- `service-cluster` — layers cluster/Raft mode on top of `service` (implies `service`, `distributed-raft`)

## Architecture

### Columnar Processing Model

- **`Component` trait** (`src/component.rs`): Any type that provides a `name() -> &'static str` and an Arrow `Schema`. Data serializes via `serde_arrow`.
- **`Dataset`** (`src/pipeline.rs`): Arrow-backed columnar data container. Stores one `RecordBatch` per registered component; all batches share the same row count. Holds `SchemaRegistry`, `ResourceMap`, and an alive bitmap. Supports batch `append`, soft-delete (`mark_dead`), compaction, and IPC round-trip serialization. Was called `Pipeline` prior to the April 2026 refactor. Builder: `DatasetBuilder`.
- **`Row`** (`src/row.rs`): Stable row index (`u32`). Invalidated by `compact`.
- **`Resource`** (`src/resource.rs`): Boxed Rust singleton stored in `Dataset`, keyed by `TypeId`. Not columnar.
- **`System` trait** (`src/system.rs`): Processing logic with `meta()` (declares field-level read/write access), `async fn run(&self, data: &mut Dataset)`, and optional sync fast-path `run_sync`. Created via struct impl or `system_fn` closure helper.
- **`Pipeline`** (`src/pipeline.rs`): Self-contained workload: `{ name, data: Dataset, systems, DAG stages, sources, sinks }`. Builds a dependency graph from `SystemMeta`, topologically sorts into stages, and runs them with per-system retry. Was called `Scheduler` prior to the April 2026 refactor. Builder: `PipelineBuilder`.
- **`Scheduler`** (`src/scheduler.rs`): Multi-pipeline orchestrator: `{ pipelines: Vec<Pipeline> }`. Drives several independent workloads from one process. Methods: `add_pipeline`, `tick` (sequential), `tick_parallel` (concurrent; each pipeline owns its own `Dataset` so no data contention).

### Dataset API (`src/pipeline.rs`)

```rust
let mut dataset = Dataset::new();
dataset.register_component::<Price>()?;          // must precede append
dataset.append::<Price>(&rows)?;                 // returns Range<Row>
let col = dataset.column::<Price>("value");       // -> Option<ArrayRef>
dataset.mark_dead(row);                          // soft delete
dataset.compact();                               // filter dead rows
dataset.write_ipc(&mut buf)?;                    // serialize
let dataset2 = Dataset::read_ipc(&buf)?;         // deserialize
```

### Pipeline API (`src/pipeline.rs`)

```rust
// Inline construction
let mut pipeline = Pipeline::new("etl");
pipeline.register_component::<Price>()?;         // forwards to self.data
pipeline.append::<Price>(&rows)?;
pipeline.add_system(EnrichPrice);
pipeline.run().await?;                           // validate + DAG + retry

// Builder pattern
let pipeline = Pipeline::builder("etl")
    .with::<Price>()
    .with_resource(TaxRate(0.1))
    .with_system(EnrichPrice)
    .build();
```

`run_on(&self, data: &mut Dataset)` is the escape hatch for `DistributedRunner`: it executes the system DAG against an external dataset without touching the template pipeline's own data, sources, or sinks.

### System & SystemMeta (`src/system.rs`)

`SystemMeta` declares data access at field granularity via `(component_name, field_name)` pairs. The pipeline uses this to build a conflict graph and group non-conflicting systems into the same stage.

```rust
SystemMeta::new("enrich")
    .read("Order", "id")
    .write("Order", "total")
    .read_component("Price")       // expands to all fields of Price
    .read_resource::<TaxRate>();
```

Conflict rules (B registered after A):
1. Write-after-read: A writes F, B reads F → B depends on A
2. Read-after-write: A reads F, B writes F → B depends on A
3. Write-write: A writes F, B writes F → B depends on A
4. Resource conflicts remain TypeId-level

System trait signatures:
- `async fn run(&self, data: &mut Dataset) -> PcsResult<()>` — exclusive access
- `async fn run(&self, data: &Dataset) -> PcsResult<WriteSet>` — `ParallelSystem` read-only pass

### Pipeline DAG Scheduling (`src/pipeline.rs`)

`Pipeline::run()` validates all declared fields against `self.data.schemas()`, builds the stage graph from `SystemMeta` declarations, then runs stages sequentially. Each system run is wrapped in the retry logic from `SystemConfig`. Sources are drained into `self.data` before systems run; sinks are drained from `self.data` after.

### Scheduler Orchestration (`src/scheduler.rs`)

`Scheduler` owns a `Vec<Pipeline>` and provides two tick modes:

- `tick()`: runs each pipeline sequentially in registration order. Simpler to reason about; prefer when pipelines are fast or when deterministic ordering matters.
- `tick_parallel()`: drives all pipelines concurrently via `futures::try_join_all`. Each pipeline exclusively owns its `Dataset` — no shared mutable state — so parallel execution is trivially sound. Prefer when pipelines are IO-heavy or CPU-bound and independent.

Use `Scheduler` when running multiple independent workloads from one process.

### IO Layer (`src/io/`, feature-gated)

`Source` and `Sink` traits for reading/writing Arrow data. Built-in implementations: Parquet, JSON Lines, CSV, and in-memory channel transport. Pipeline integrates via `drain_into_dataset` / `drain_dataset`.

### Retry (`src/retry.rs`)

`RetryMode`: `None`, `Fixed`, or `ExponentialBackoff` (default: 3 retries, 100ms base, 2.0x multiplier, 30s cap, 0.1 jitter). `SystemConfig` wraps a `RetryMode` and is returned by `System::config()`.

### Error types (`src/error.rs`)

`PcsError` variants: `SystemExecution`, `ComponentNotFound`, `EntityNotFound`, `ResourceNotFound`, `Store`, `Scheduler`, `Configuration`, `RetryExhausted`, `Generic`. With `distributed` feature: `Distributed`, `LeaseExpired`. Alias: `PcsResult<T>`.

### Distributed Processing (`src/distributed/`, feature-gated)

Multi-instance batch execution with at-least-once processing semantics.

**Core traits and types** (`distributed` feature):
- `PartitionSource` — claims/acks/releases row-range batches across instances
- `CheckpointStore` — persists Arrow IPC snapshots for crash recovery
- `DistributedRunner` + `RunnerConfig` — holds a `Pipeline` template. For each claimed batch it calls a `world_factory()` closure to produce a fresh `Dataset`, then calls `pipeline.run_on(&mut partition_dataset)` to execute the system DAG. The template's own `data`, sources, and sinks are **never used** — data arrives via `PartitionSource`, state is saved via `CheckpointStore`.
- `CheckpointStrategy` — checkpoint frequency: `EveryStage`, `EveryNStages`, `None`
- `RedbSharedStore` — implements `PartitionSource` + `CheckpointStore` over redb; single-node (direct apply) or multi-node (proposes through Raft channel)
- `ConsensusCommand` / `ConsensusResponse` — deterministic state machine command types
- `ParquetCheckpointStore` — archival checkpoint store (requires `io` + `distributed`)

**Raft integration** (`distributed-raft` feature):
- `ArrowRedbLogStore` — implements openraft's `RaftLogStorage` over redb (log-only file)
- `ArrowRedbStateMachine` — implements openraft's `RaftStateMachine`; applies `ConsensusCommand` ops to a separate redb file
- `ArrowRaftDriver` + `ArrowRaftDriverConfig` + `ArrowRaftDriverHandle` — manages openraft node lifecycle with proposal channel
- `PcsTypeConfig` — openraft type configuration (`NodeId=u64`, `D=String`, `R=String`)
- `TcpNetworkFactory` / `TcpNetwork` — implements openraft's `RaftNetworkV2` with length-prefixed TCP framing

### Service Layer (`src/service/`, `src/bin/pcs-service/`, feature-gated)

Requires the `service` feature. Provides a production-ready binary (`pcs-service`) with TOML-driven config, factory registry, HTTP control plane, and standalone/cluster runners.

Key types:
- `ServiceConfig` / `ServiceMode` — top-level TOML config schema (`mode = "standalone"` or `mode = "cluster"`)
- `ServiceBuilder` / `BuiltService` — assembles scheduler, sources, and sinks from config + registered factories
- `SystemFactory` / `SourceFactory` / `SinkFactory` / `ComponentFactory` — extension points for registering custom types
- `run_standalone` / `run_cluster` — runner entry points
- HTTP control plane: `/health`, `/ready`, `/metrics`, `/status` (axum-backed)

CLI subcommands: `serve`, `validate`, `status`, `cluster init`, `cluster join`, `cluster leave`, `cluster status`.

## Conventions

- All async traits use `#[async_trait]`.
- Tracing instrumentation is behind `#[cfg(feature = "tracing")]` conditional compilation.
- Public API is re-exported through `pcs::prelude::*` (see `src/lib.rs`).
- Tests live in `#[cfg(test)]` modules within each source file.
- Benchmarks use Criterion in `benches/`.
