# AGENTS.md

Guidance for coding agents working in this repository.

## Project

PCS is a distributed batch processing engine for Rust built on Apache Arrow. Pipelines compile to
WebAssembly components; a host binary loads them at runtime. Edition 2024, MSRV 1.95.0.

The repository root is a **virtual manifest**: there is no root `src/`. Every command that names a
target needs `-p <crate>`.

## Commands

```bash
cargo build                                                  # Build (workspace default members)
cargo test --workspace --all-features                        # Full suite, what CI runs
cargo test --workspace --all-features --doc                  # Doc tests
cargo fmt --all -- --check                                   # Check formatting
cargo clippy --all-targets --all-features -- -D warnings     # Lint (warnings are errors)
scripts/bench.sh tpch_q6                                     # Benchmarks, always via the harness
cargo check --examples                                       # Verify examples compile
cargo run -p pcs-service --example scheduler_etl             # Run an example
cargo run -p pcs-service --example first_pipeline            # The native tutorial's example
cargo run -p pcs-service --example distributed_scheduler --features distributed
cargo run -p pcs-service --example scheduler_etl_parallel
cargo audit                                                  # Security audit (needs cargo-audit)
bash scripts/build-polyglot.sh                               # Build the six polyglot guests
```

### WASM fixture prerequisites

`cargo fmt --all -- --check` walks every `mod` declaration ignoring `#[cfg(...)]`, so the
cargo-component-generated `src/bindings.rs` files must exist on disk even though the host build
never compiles them. Both are gitignored. Generate them first:

```bash
rustup target add wasm32-wasip2
cargo install cargo-component --locked --version 0.21.1
cargo component build -p order-processing-wasm --target wasm32-wasip2
cargo component build -p polyglot-settle-wasm --target wasm32-wasip2
cargo component build --release -p pcs-guest-smoketest --target wasm32-wasip2
```

The third build is also a prerequisite for `cargo test`, because
`crates/pcs-service/tests/wasm_roundtrip.rs` asserts the artifact exists. Output lands under
`target/wasm32-wasip1/`: cargo-component 0.21.1 compiles the core module for wasip1 and adapts it
into a wasip2 component, keeping the pre-adapter directory name.

### Native plugin fixture prerequisite

`crates/pcs-plugin-smoketest` is a `cdylib`, so `cargo test --workspace` does not build it: a
cdylib-only member has no test target for cargo to reach. `crates/pcs-service/tests/plugin_roundtrip.rs`
asserts the artifact exists, so build it first:

```bash
cargo build -p pcs-plugin-smoketest
```

The artifact name is platform specific: `target/debug/libpcs_plugin_smoketest.so`,
`.dylib` on macOS, `pcs_plugin_smoketest.dll` on Windows. The test resolves it through
`std::env::consts::DLL_PREFIX` and `DLL_SUFFIX`, so it follows whichever profile the test ran under.

### Conformance corpus

`packages/arrow-ipc-conformance/` pins all five Arrow IPC codecs to one answer about which streams
are valid. Vectors and manifest are committed, so a codec's suite needs no Rust toolchain.
Regenerate after any wire format or `Order` schema change, then commit the result:

```bash
cargo run -p pcs-service --features conformance --example conformance_vectors -- emit
```

The `conformance` feature exists to enable `arrow-ipc/lz4` for the generator alone, so a normal
build still cannot write a compressed record batch and must reject one.

## Workspace layout

```
crates/
├── pcs-core/            # Engine primitives: Dataset, Pipeline, System, Scheduler, Component.
│                        # Arrow-only dependencies; compiles for host and wasm32-wasip2 guest.
├── pcs-guest/           # Guest SDK: re-exports pcs-core plus the export_pipeline! macro.
│                        # Owns the canonical WIT package at wit/pipeline.wit.
├── pcs-guest-smoketest/ # Minimal guest component used as a CI fixture (arrow-ipc drift gate,
│                        # config delivery, cross-batch state).
└── pcs-service/         # Host: wasmtime runtime, IO formats, distributed/Raft, HTTP control
                         # plane, TOML config, and the pcs-service binary.
examples/wasm/order_processing/   # A realistic guest pipeline built with cargo-component.
examples/polyglot/                # Six guest components (Go, Python, TypeScript, Kotlin, C#, Rust)
                                  # against one WIT world, plus the shared Order schema crate. See
                                  # PINS.md there.
packages/                         # Apache-2.0 subtree, unlike the rest of the repository. One
                                  # Arrow IPC codec per language, released as `pcs-arrow-ipc`;
                                  # the five non-Rust polyglot stages consume it as a dependency.
├── arrow-ipc-go/        # module github.com/nassor/pcs/packages/arrow-ipc-go, package arrowipc
├── arrow-ipc-py/        # pcs-arrow-ipc, import pcs_arrow_ipc
├── arrow-ipc-ts/        # @nassor/pcs-arrow-ipc
├── arrow-ipc-kt/        # io.github.nassor:pcs-arrow-ipc, wasmWasi and jvm targets
└── arrow-ipc-cs/        # Pcs.ArrowIpc
packages/VERSION                  # The one version all five declare. scripts/pack-arrow-ipc.sh
                                  # asserts every manifest matches it.
docs/                             # Zola site: content/*.md front matter, templates/*.html prose.
                                  # Sections: native/ (overview + tutorial), guests/ (overview,
                                  # WIT contract, one page per language, the six-language
                                  # example), reference/ (wire format), operations/, benchmarks/.
                                  # Pages under a section are markdown through page.html or
                                  # section.html; page.html renders no title, so such a page
                                  # opens its body with its own `#` heading.
docs/figures/bench_figures.py     # Owns every chart on the benchmarks page. The SVG in
                                  # content/benchmarks/_index.md is generated between
                                  # `<!-- fig:NAME -->` markers. Edit the numbers here and re-run;
                                  # never edit the markup.
docs/config.toml                  # `[[extra.nav]]` is the reading order. base.html renders the
                                  # sidebar, the breadcrumb and the previous/next pager from it,
                                  # so a new page is added there, not in the template.
docs/search-index.py              # Builds public/search-index.json from the rendered HTML, split
                                  # at `<h2>` boundaries. Runs after `zola build`, because nine
                                  # concept pages keep their prose in templates/.
docs/build-local.py               # zola build + relative-URL rewrite + search index, for
                                  # browsing public/ over file:// or a local server.
```

## Feature flags

### `pcs-core`

- `runtime` (**default**): tokio and rayon stage parallelism. Disable for wasm guest builds.
- `guest`: wasm32-wasip2 target, sequential-only execution driven by the `pollster` sync executor.
- `windows`: windowed aggregation. `WindowedSystem`, `WindowSpec`, watermarks, `WindowAccumulator`.
- `io`: the `Source`/`Sink` traits plus in-memory channel implementations (implies `runtime`).
- `distributed`: types shared with the host's distributed layer (implies `runtime`).
- `tracing`: `tracing` crate integration.

### `pcs-service`

- `io`: Parquet, CSV, and JSON `Source`/`Sink` implementations. The formats live here, not in core.
- `datafusion`: DataFusion source (implies `io`).
- `windows`: forwards `pcs-core/windows`.
- `wasm`: wasmtime host. `WasmEngine`, `WasmPipelineRuntime`, the `bindgen!` host bindings.
- `distributed`: `PartitionSource`, `CheckpointStore`, `DistributedRunner`, redb store, TCP
  transport.
- `distributed-raft`: openraft log store, state machine, snapshot, node driver (implies
  `distributed`).
- `service`: the `pcs-service` binary. axum HTTP control plane, TOML config, metrics, standalone
  runner (implies `io`, `distributed`, `tracing`). It does **not** imply `distributed-raft`.
- `service-cluster`: cluster/Raft mode on top of `service` (implies `service`, `distributed-raft`).

## Architecture

### `pcs-core`, columnar engine

- **`Component` trait** (`src/component.rs`): any type providing `name() -> &'static str` and an
  Arrow `Schema`. Rows serialize via `serde_arrow`.
- **`Dataset`** (`src/dataset.rs` plus the nine-file `src/dataset/` submodule): Arrow-backed
  columnar container. One `RecordBatch` per registered component; the IPC format requires every
  component to hold exactly the dataset's row count. Holds a `SchemaRegistry`, a `ResourceMap`, and
  an alive bitmap. Supports batch `append`, soft delete (`mark_dead`), compaction, and IPC
  round-trip. Builder: `DatasetBuilder`. Canonical path: `pcs_core::dataset::Dataset`.
- **`Row`** (`src/row.rs`): stable row index (`u32`). Invalidated by `compact`.
- **`Resource`** (`src/resource.rs`): boxed Rust singleton stored in `Dataset`, keyed by `TypeId`.
  Not columnar, and **not** serialized by `write_ipc`. The guest SDK relies on that to keep
  cross-batch state out of the data plane.
- **`System` trait** (`src/system.rs`): `meta()` declares field-level read/write access,
  `async fn run(&self, data: &mut Dataset)` does the work, `run_sync` is an optional sync fast path.
  Written as a struct impl or via the `system_fn` closure helper.
- **`Pipeline`** (`src/pipeline.rs` plus `src/pipeline/`): self-contained workload
  `{ name, data: Dataset, systems, DAG stages, sources, sinks }`. Builds a conflict graph from
  `SystemMeta`, topologically sorts it into stages, and runs them with per-system retry. Builder:
  `PipelineBuilder`.
- **`Scheduler`** (`src/scheduler.rs`): multi-pipeline orchestrator over `Vec<Pipeline>`. `tick()`
  runs every pipeline once, walking a dependency DAG built from `PipelineConfig`. Reachable from
  library code only, because `ServiceConfig.pipeline` is singular and the TOML-driven binary never
  builds one.

#### Dataset API

```rust
let mut dataset = Dataset::new();
dataset.register_component::<Price>()?;          // must precede append
dataset.append::<Price>(&rows)?;                 // returns Range<Row>
let col = dataset.column::<Price>("value");      // -> Option<ArrayRef>
dataset.mark_dead(row);                          // soft delete
dataset.compact();                               // filter dead rows
dataset.write_ipc(&mut buf)?;                    // serialize
let dataset2 = Dataset::read_ipc(&mut &buf[..])?;
```

#### Pipeline API

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

`run_on(&self, data: &mut Dataset)` is the escape hatch for hosts that own their own dataset. It
executes the system DAG against an external dataset without touching the template pipeline's data,
sources, or sinks. `run_on_with_stats` is the same call returning the per-call `RunStats`, which is
how the guest SDK fills the WIT `run-metrics` record.

#### System & SystemMeta (`src/system.rs`)

`SystemMeta` declares data access at field granularity via `(component_name, field_name)` pairs. The
pipeline uses this to build a conflict graph and group non-conflicting systems into one stage.

```rust
SystemMeta::new("enrich")
    .read("Order", "id")
    .write("Order", "total")
    .read_component("Price")       // expands to all fields of Price
    .read_resource::<TaxRate>();
```

Conflict rules (B registered after A):
1. Write-after-read: A writes F, B reads F, so B depends on A
2. Read-after-write: A reads F, B writes F, so B depends on A
3. Write-write: A writes F, B writes F, so B depends on A
4. Resource conflicts remain TypeId-level

System trait signatures:
- `async fn run(&self, data: &mut Dataset) -> PcsResult<()>` for exclusive access
- `async fn run(&self, data: &Dataset) -> PcsResult<WriteSet>` for `ParallelSystem`, a read-only
  pass

#### Retry (`src/retry.rs`)

`RetryMode`: `None`, `Fixed`, or `ExponentialBackoff` (default: 3 retries, 100 ms base, 2.0x
multiplier, 30 s cap, 0.1 jitter). `SystemConfig` wraps a `RetryMode` and is returned by
`System::config()`. Every system run goes through one of two drivers that share the attempt-counting
core: `run_with_retries` (async, `tokio::time::sleep`) and `run_with_retries_blocking`
(`std::thread::sleep`, for the rayon/`spawn_blocking` stage path).

#### Error types (`src/error.rs`)

`PcsError` variants: `SystemExecution`, `ComponentNotFound`, `EntityNotFound`, `ResourceNotFound`,
`Store`, `Scheduler`, `Configuration`, `RetryExhausted`, `Generic`. With the `distributed` feature:
`Distributed`, `LeaseExpired`. Alias: `PcsResult<T>`. `PartialEq`/`Eq` are derived.

#### Windowed aggregation (`src/windows/`, `windows` feature)

`WindowedSystem` and `WindowedSystemBuilder` assign rows to tumbling, sliding, or session windows,
track watermarks, aggregate per key, and publish results as a `WindowResults` resource.
`WindowAccumulator` is the component that carries open-window state across batches; the host
persists it through `CheckpointStore`.

### `pcs-guest`, guest SDK

Owns `wit/pipeline.wit`, the canonical `pcs:pipeline@0.2.0` WIT package. The guest exports exactly
two functions:

- `describe()`: name, version, component schemas, schema fingerprint, stateful flag.
- `run-batch(input, prior)`: Arrow IPC in, Arrow IPC out, plus metrics and an updated state blob.

`export_pipeline!(build)` wires a `fn() -> Pipeline` to those exports and emits `pcs_config_get` and
`pcs_config_parse` into the caller's crate. The WIT bindings are caller-side, so the accessors must
be too. `export_pipeline!(build, state = C)` additionally installs a `GuestState<C>` resource on the
batch dataset before the systems run and serializes it back into `run-result.checkpoint` afterwards.
State is a resource rather than a registered component because resources do not round-trip through
Arrow IPC, so guest state never leaks into the output.

The host creates a fresh wasmtime `Store` per call, so `prior`/`checkpoint` is the only channel by
which guest state survives a batch boundary.

### `pcs-service`, host

#### IO layer (`src/io/`)

`pcs-core`'s `io` feature provides the `Source`/`Sink` traits, the schema-cast helpers, and the
in-memory channel implementations. The file formats (Parquet, JSON Lines, CSV) and the DataFusion
source live in `crates/pcs-service/src/io/`, which re-exports the core traits so
`pcs_service::io::source::Source` resolves. Pipelines integrate via `drain_into_dataset` and
`drain_dataset`.

#### Distributed processing (`src/distributed/`)

Multi-instance batch execution with at-least-once semantics. `pcs-core`'s `distributed` feature
contributes shared types only; all runner code lives here.

**`distributed` feature:**
- `PartitionSource`: claims, acks, and releases row-range batches across instances
- `CheckpointStore`: persists Arrow IPC snapshots for crash recovery
- `DistributedRunner` and `RunnerConfig`: holds a `Box<dyn PipelineRuntime>` template. Per claimed
  batch it calls `world_factory()` for a fresh `Dataset`, loads the window accumulator and the
  runtime's opaque state blob, calls `runtime.run_on_with_state(&mut partition_data, prior)`, then
  checkpoints and acks. The template's own data, sources, and sinks are never used.
- `CheckpointStrategy`: `EveryStage`, `EveryNStages`, `None`
- `RedbSharedStore`: `PartitionSource` plus `CheckpointStore` over redb. Single-node applies
  directly; multi-node proposes through the Raft driver
- `ConsensusCommand` and `ConsensusResponse`: deterministic state machine command types
- `accumulator_store` and `guest_state_store`: free functions that park the window accumulator and
  the runtime state blob under reserved `stage_idx` sentinels (`ACCUMULATOR_STAGE_SENTINEL`,
  `GUEST_STATE_STAGE_SENTINEL`)
- `ParquetCheckpointStore`: archival checkpoint store (needs `io` plus `distributed`)

**`distributed-raft` feature:**
- `ArrowRedbLogStore`: openraft `RaftLogStorage` over a log-only redb file
- `ArrowRedbStateMachine`: openraft `RaftStateMachine`, applies `ConsensusCommand` to a separate file
- `validate_store_consistency`: refuses startup when the state machine is behind what the log store
  purged. Called by `ArrowRaftDriver::start`
- `ArrowRaftDriver`, `ArrowRaftDriverConfig`, `ArrowRaftDriverHandle`: openraft node lifecycle with
  a proposal channel
- `PcsTypeConfig`: openraft type configuration (`D = ConsensusCommand`, `R = ConsensusResponse`)
- `TcpNetworkFactory`, `TcpNetwork`, `RaftTcpServer`: `RaftNetworkV2` over length-prefixed TCP

#### WASM host (`src/wasm/`, `wasm` feature)

`WasmEngine` owns the wasmtime `Engine` and epoch ticker. `WasmPipelineRuntime` implements
`pcs_core::runtime::PipelineRuntime`: it serializes the dataset to Arrow IPC, calls the guest's
`run-batch` on a fresh `Store`, and reads the result back. `bindings.rs` is 26 lines of
`wasmtime::component::bindgen!` pointed at `../pcs-guest/wit`, so host bindings cannot drift.
`host_impl.rs` implements the `host-io` imports (`log`, `metric`, `get-config`).

#### Service layer (`src/service/`, `src/bin/pcs-service/`)

Requires the `service` feature. TOML-driven config, factory registry, HTTP control plane, and
standalone/cluster runners.

Key types:
- `ServiceConfig` and `ServiceMode`: TOML schema (`mode = "standalone"` or `mode = "cluster"`)
- `PipelineSpec` and `WasmSpec`: where the pipeline comes from. `#[serde(deny_unknown_fields)]`
  means a key the service cannot honour is a parse error, not a silently dropped section. There is
  no TOML path for declaring systems or components.
- `ServiceBuilder` and `BuiltService`: assembles the runtime, sources, and sinks from config plus
  registered factories. The runtime is either `[pipeline.wasm]` or `with_runtime(...)`.
- `Registry`, `SourceFactory`, `SinkFactory`: the whole extension surface.
- `validate_io_coverage` and `validate_schema_fingerprint`: load-time gates. Every declared
  `target_component`/`source_component` must be in the runtime's `declared_components()`, and a
  cluster node refuses to start when the pipeline's Arrow schema fingerprint differs from the one
  its persisted checkpoints were written with.
- `run_standalone` and `run_cluster`: runner entry points
- HTTP control plane: `/health`, `/ready`, `/metrics`, `/status` (axum-backed)

CLI subcommands: `serve`, `validate`, `status`, `cluster init`, `cluster join`, `cluster leave`,
`cluster status`.

## Conventions

- All async traits use `#[async_trait]`; `PipelineRuntime` uses `#[async_trait(?Send)]`.
- Tracing instrumentation is behind `#[cfg(feature = "tracing")]`, and every such site keeps a
  `#[cfg(not(feature = "tracing"))]` fallback so the value is still consumed.
- Public API is re-exported through `pcs_core::prelude::*` and `pcs_service::prelude::*`. There is no
  crate named `pcs`.
- Tests live in `#[cfg(test)]` modules within each source file; integration tests are in each crate's
  `tests/`. Docker-dependent chaos tests soft-skip with a `SKIP:` line when no daemon is present.
- Benchmarks use Criterion in each crate's `benches/`. Run them through `scripts/bench.sh`, never
  bare `cargo bench`. The harness fixes `RUSTFLAGS`, compiles as a separate step so criterion does
  not share the machine with rustc, and takes the benchmark binary from cargo's own `Executable`
  line. Cargo's metadata hash encodes profile, features, and flags, so binaries built under
  different configurations coexist in `target/release/deps/` and picking the wrong one silently
  re-measures a stale build. Published figures are taken unpinned; `--affinity` is for A/B
  comparison only.
- `arrow-ipc = "=59.2.0"` is exact-pinned workspace-wide. It is the host to guest wire format and the
  on-disk checkpoint format. See `crates/pcs-guest/PINS.md` before touching it.
- Documentation and code comments: read `.agents/skills/writing-pcs-docs/SKILL.md` before editing
  `README.md`, anything under `docs/`, or `///`/`//!` comments. Current state only, no optimization
  history or task references. No mermaid, ASCII art, or tables as diagrams; use SVG.
