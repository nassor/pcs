<div align="center">
  <img src="docs/static/logo.svg" alt="PCS Logo" width="180">

  <h1>PCS</h1>

  <p><strong>Batch pipelines as WebAssembly components. Write them in any language.</strong></p>

[![Website](https://img.shields.io/badge/docs-nassor.github.io%2Fpcs-2f81f7)](https://nassor.github.io/pcs/)
[![CI](https://github.com/nassor/pcs/actions/workflows/ci.yml/badge.svg)](https://github.com/nassor/pcs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-AGPL--3.0--only-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.95%2B-orange)](https://www.rust-lang.org)
[![Status](https://img.shields.io/badge/status-experimental-yellow)](#project-status)

</div>

## What it is

PCS (Pipeline Component System) is a columnar batch processing engine built on Apache Arrow.

You write transforms as plain structs. Each one declares **which Arrow fields it reads and which it
writes**. That declaration is the only scheduling input PCS needs. It builds a dependency graph from
the field overlaps, groups work that cannot conflict into stages it can run concurrently, retries
what fails, and optionally spreads the work across a cluster.

You never write a stage list.

The engine and the host are Rust. The *pipeline* contract is the `pcs:pipeline@0.3.0` WIT world, so
any language that compiles to a WASI 0.2 component can implement it. The Rust processor SDK is a
convenience, not a requirement. See
[WebAssembly processors](https://nassor.github.io/pcs/processors/).

## What it is for

Reach for PCS when:

- Work arrives as **batches of 100k to 100M rows**, or as a **stream of individual items** you want
  processed one at a time.
- The transform is **imperative code** that SQL expresses awkwardly.
- Schemas are **wide**, tens to hundreds of columns, of which each step touches a few.
- **Recovery time** is a design constraint, not an afterthought.

Look elsewhere when you want SQL (use [DataFusion](https://datafusion.apache.org/)), or you have
fewer than ~10k rows total and a `Vec` would do. For per-item processing, run the same pipeline in
[stream mode](https://nassor.github.io/pcs/service/): sub-millisecond per item in-process. Batch mode
remains the default for throughput.

The name is a nod to ECS (Entity Component System) from game development. Where ECS organises game
entities as components that systems act on each frame, PCS organises a data `Pipeline` as
`Component`s that `System`s transform in field-granular DAG order.

## End to end

PCS is service-first. You do not deploy a binary containing your pipeline. You deploy `pcs-service`
once and hand it WebAssembly components.

<p align="center">
  <img src="docs/static/end-to-end.svg" width="880"
       alt="End to end: you write Component, System, and Pipeline structs; cargo build produces pipeline.wasm, a wasm32-wasip2 component; pcs-service loads the component named in pcs.kdl and drives Arrow IPC from sources through the host to sinks; optionally the host coordinates a Raft cluster with row-range leases and checkpoints.">
</p>

The processor component owns the DAG, the stage plan, and retry. The host owns IO, checkpointing,
distribution, and the HTTP control plane. Data crosses the boundary as **Arrow IPC bytes and nothing
else**, so your pipeline never opens a socket or a file.

## Why columns

Storing each field as a contiguous Arrow column, rather than a row per record, changes what the
machine has to move:

- A system reading 3 of 50 columns loads **24 MB instead of 400 MB** per million rows.
- Handing a batch to the next stage is an `Arc` clone: one atomic increment, no copy.
- Checkpointing is a contiguous buffer write, so recovery decodes **6x faster** than a row-oriented
  equivalent at 1M rows.

Numbers and methodology: [benchmark results](https://nassor.github.io/pcs/benchmarks/).

## Quick start

```bash
rustup target add wasm32-wasip2
cargo install wasm-tools    --locked --version 1.246.2
```

Run a working pipeline without writing anything:

```bash
cargo run -p pcs-service --example scheduler_etl
```

Then compile that same style of pipeline to a WebAssembly component and confirm it exports the PCS
world:

```bash
cargo build --release -p order-processing-wasm --target wasm32-wasip2

wasm-tools validate --features component-model \
    target/wasm32-wasip2/release/order_processing_wasm.wasm
```

`rustc` links a `wasm32-wasip2` cdylib into a Component Model component itself, so plain
`cargo build` is the whole toolchain: no componentizer, no preview1 adapter step.

For your own component, your own system, and the config that runs it, follow
**[Build your first pipeline](https://nassor.github.io/pcs/native/tutorial/)**.

## Six languages, one pipeline

`examples/polyglot/` implements a single `Order` workload as six separate WebAssembly components,
chained by a Rust driver through the same host `pcs-service` uses. Every stage exports the same WIT
world; nothing but the language differs.

| # | stage | language | toolchain | writes |
|---|-------|----------|-----------|--------|
| 1 | `validate-go` | Go | `componentize-go` | `valid` |
| 2 | `enrich-py` | Python | `componentize-py` | `usd_amount` |
| 3 | `score-ts` | TypeScript | `jco` | `risk_score`, `flagged` |
| 4 | `fee-kt` | Kotlin | Gradle plus `wit-bindgen` and `wasm-tools` | `fee` |
| 5 | `tier-cs` | C# | `componentize-dotnet` | `review_tier` |
| 6 | `settle-rs` | Rust | `cargo build --target wasm32-wasip2` | `settlement` plus a cross-batch ledger |

```bash
cargo xtask polyglot
cargo run -p pcs-service --features wasm,tracing --example polyglot_orders
```

Details, the byte-level contract, and a per-language recipe, with the
[six-language example](https://nassor.github.io/pcs/processors/#six-languages-one-pipeline)
on the WASM Processors page.

## Documentation

| | |
|---|---|
| **[Build your first pipeline](https://nassor.github.io/pcs/native/tutorial/)** | A native pipeline in nine steps, from the component to the stage plan |
| **[Dataset & Components](https://nassor.github.io/pcs/dataset/)** | How data is stored, appended, deleted, and serialised |
| **[Systems](https://nassor.github.io/pcs/systems/)** | Writing a transform and declaring its field access |
| **[Pipeline](https://nassor.github.io/pcs/pipeline/)** | Stage derivation and per-system retry |
| **[Scheduler](https://nassor.github.io/pcs/scheduler/)** | Several pipelines in one process, with dependencies |
| **[Sources & Sinks](https://nassor.github.io/pcs/io/)** | Getting rows in and out |
| **[Distributed Runner](https://nassor.github.io/pcs/distributed/)** | Row-range leases, checkpoints, Raft |
| **[Service](https://nassor.github.io/pcs/service/)** | KDL config schema, validation gates, HTTP control plane |
| **[Operating pcs-service](https://nassor.github.io/pcs/operations/running-pcs/)** | Deployment, tuning, failure modes |
| **[Tracing](https://nassor.github.io/pcs/tracing/)** | Spans, metrics, and the Prometheus endpoint |
| **[WASM Processors](https://nassor.github.io/pcs/processors/)** | The WIT contract, a recipe per language, and the six-language example |
| **[The SDK packages](https://nassor.github.io/pcs/reference/arrow-ipc-packages/)** | One `pcs-sdk` per language, the Arrow codec inside each |

Also in this repo: [WASM processor examples](./examples/wasm/), the [polyglot
example](./examples/polyglot/), [Rust-native examples](./examples/native/), the
[branching example](./examples/branching/), the
Apache-2.0 [SDK packages](./packages/), and toolchain pins for [Rust
processors](./crates/pcs-processor/PINS.md) and [the other
languages](./examples/polyglot/PINS.md).

## Workspace

| Crate | Contents |
|---|---|
| `pcs-core` | `Dataset`, `Component`, `System`, `Pipeline`, `Scheduler`, and the `Source`/`Sink` traits. Arrow-only dependencies; used by both host and processor. |
| `pcs-config` | The configuration language: parses KDL into `ConfigValue`, the value type every factory reads. |
| `pcs-connector` | The factory contract every connector implements: `SourceFactory`, `SinkFactory`. |
| `pcs-connector-channel` | In-memory mpsc `Source` and `Sink`. |
| `pcs-connector-datafusion` | `Source` over a DataFusion SQL query. |
| `pcs-connector-file` | Local-file `Source` and `Sink`. A transformer supplies the format. |
| `pcs-connector-kafka` | Kafka `Source` and `Sink`. |
| `pcs-connector-nats` | NATS `Source` and `Sink`, core subjects or JetStream. |
| `pcs-connector-postgresql` | PostgreSQL `Source` and `Sink`. |
| `pcs-connector-tcp` | Live TCP `Source` that listens and `Sink` that dials, over one length-prefixed frame. |
| `pcs-processor` | Processor SDK. Re-exports `pcs-core`, provides `export_pipeline!`, owns the canonical WIT at `wit/pipeline.wit`. |
| `pcs-processor-smoketest` | Minimal processor component used by CI to gate the Arrow IPC wire format. |
| `pcs-plugin` | Native plugin host: loads a shared library through the `pcs-plugin-abi` C ABI. |
| `pcs-plugin-abi` | The C ABI a native plugin exports. |
| `pcs-plugin-smoketest` | Minimal native plugin used by CI as a fixture. |
| `pcs-service` | Host binary: wasmtime, distribution, config, HTTP, and the factory registry. |
| `pcs-transformer` | The byte-format contract: `Transformer`, `BatchReader`, `BatchWriter`, `MessageDecoder`, and the `TransformerRegistry`. |
| `pcs-transformer-arrow-ipc` | Arrow IPC message codec, format `arrow-ipc`. |
| `pcs-transformer-avro` | Avro object container file reader and writer plus message codec, format `avro`. |
| `pcs-transformer-csv` | CSV reader and writer, format `csv`. |
| `pcs-transformer-ndjson` | Newline-delimited JSON reader, writer, and message codec, format `ndjson`. |
| `pcs-transformer-parquet` | Parquet reader and writer, format `parquet`. |
| `polyglot-settle-wasm` | Rust stage of the polyglot example: writes `settlement`, keeps the ledger. |

## Building from source

```bash
cargo build --features service,wasm          # standalone binary, no connectors
cargo build --features connector-file,transformer-csv,wasm   # with the file connector reading CSV
cargo build --features service-cluster,wasm  # with Raft cluster support
cargo test --workspace --all-features     # full suite
cargo clippy --all-targets --all-features -- -D warnings
```

## Project status

This is a playground project exploring two things:

1. **How far specialised AI coding agents can maintain a non-trivial Rust codebase**, spanning
   multiple crates, a binary, and a WebAssembly component, with minimal human intervention in
   maintenance and review.
2. **The design space of a Rust-native batch engine with WebAssembly extensibility.**

It is **not production-ready** and the crates are not published to crates.io. Contributions and
feedback are very welcome.

## License

Licensed under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE).
