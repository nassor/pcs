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
cargo nextest run --workspace --all-features                 # Fast suite: skips Docker/chaos tests, run this constantly
cargo nextest run --workspace --all-features --profile ci --run-ignored all  # Full suite, what CI runs; last step of a plan
cargo nextest run -p pcs-service --all-features --profile ci --test connector_matrix --run-ignored ignored-only  # Connector matrix; --profile ci required since --test can't bypass default's exclusion, see Testing below
cargo test --workspace --all-features --doc                  # Doc tests (nextest does not run these)
cargo fmt --all -- --check                                   # Check formatting
cargo clippy --all-targets --all-features -- -D warnings     # Lint (warnings are errors)
cargo xtask bench tpch_q6                                    # Benchmarks, always via the harness
cargo check --examples                                       # Verify examples compile
cargo run -p pcs-service --example scheduler_etl             # Run an example
cargo run -p pcs-service --example first_pipeline            # The native tutorial's example
cargo run -p pcs-service --example distributed_scheduler --features distributed  # No external service
cargo run -p pcs-service --example scheduler_etl_parallel
cargo audit                                                  # Security audit (needs cargo-audit)
cargo xtask polyglot                                         # Build the six polyglot processors
cargo xtask quickstart                                       # Build the two Quick Start processors
cargo xtask ui                                               # Rebuild the /ui dashboard bundle
cargo xtask validate                                         # Validate example configs: build+run the registry, parse-check the rest
cargo xtask demo <name>                                      # Build and run an example pipeline
cargo xtask --help                                           # Every task the runner carries
```

`cargo nextest run` needs `cargo install cargo-nextest --locked --version 0.9.143` first; see
"Testing" below for what the two profiles cover and why the fast one is the default day-to-day
command.

`crates/pcs-service-ui` is **excluded** from the workspace (`[workspace] exclude`), so neither
`cargo fmt --all` nor `cargo clippy --all-targets` reaches it. `cargo xtask ui` runs both of
its gates itself; run them directly when changing that crate without rebuilding the bundle:

```bash
cargo fmt --manifest-path crates/pcs-service-ui/Cargo.toml -- --check
cargo clippy --manifest-path crates/pcs-service-ui/Cargo.toml --target wasm32-unknown-unknown -- -D warnings
```

### WASM fixture prerequisite

Each Rust processor generates its WIT bindings in-macro via `wit_bindgen::generate!`, so nothing
has to be produced on disk before `cargo fmt --all -- --check`. The one build `cargo test` needs
is the smoketest component: both `crates/pcs-service/tests/wasm_roundtrip.rs` and
`crates/pcs-service/tests/processor_metrics.rs` assert the artifact exists, through the shared
`crates/pcs-service/tests/common/smoketest.rs` fixture.

```bash
rustup target add wasm32-wasip2
cargo build --release -p pcs-processor-smoketest --target wasm32-wasip2
```

No `cargo-component`: `rustc` links a `wasm32-wasip2` cdylib into a Component Model component
itself, so plain `cargo build` writes the finished component to
`target/wasm32-wasip2/release/pcs_processor_smoketest.wasm` with no preview1 core module and no
adapter step. `.cargo/config.toml` adds `-C target-feature=+simd128` for that target only, so
every processor's core module ships the SIMD proposal; wasmtime enables it by default.

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

### Kafka connector build prerequisite

`pcs-connector-kafka` vendors librdkafka through `librdkafka-sys`'s `cmake-build` feature, so
`cmake` and a C toolchain (MSVC Build Tools on Windows) must be on `PATH` before `cargo build`
reaches it. On POSIX targets that build also needs libcurl's development headers
(`libcurl4-openssl-dev` on Debian/Ubuntu, the equivalent elsewhere; CI installs them in the `test`,
`distributed_chaos`, `wasm_processor` and `polyglot` jobs, every job that compiles a pcs-service
test or example target): the `config.h` cmake generates defines `WITH_OAUTHBEARER_OIDC` as `0`
rather than leaving it undefined, and `rdkafka_conf.c` gates its `#include <curl/curl.h>` on
`#ifdef`, so that header is
compiled even though the build is configured with `-DWITH_CURL=0`. Windows builds escape it
because `WITHOUT_WIN32_CONFIG` leaves the macro undefined. No other repository build step needs
any of these.

### Dashboard bundle prerequisites

`crates/pcs-service/assets/ui/{index.html,app.js,app_bg.wasm,app.css}` is the one committed home
for the dashboard bundle: `cargo xtask ui` writes the three generated files straight there, next
to the hand-written `index.html`, and `include_str!`/`include_bytes!` in
`crates/pcs-service/src/service/inspector_api.rs` embeds them from there. It lives under
`pcs-service`'s own directory rather than `pcs-service-ui`'s (which carries no committed bundle of
its own) because `cargo package`/`publish` never includes files outside the package being packaged;
an `include_str!` reaching into `pcs-service-ui` (itself excluded from the workspace) would drop out
of a published `pcs-service` tarball. Committed, so `cargo build -p pcs-service` needs no wasm
toolchain. Regenerating it does:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.127 --locked   # must equal the resolved crate version
cargo xtask ui
```

The CLI version must match what `crates/pcs-service-ui/Cargo.lock` resolves for the `wasm-bindgen`
crate exactly; a mismatch is a hard runtime panic in the browser, not a warning, so the task reads
the version from the lock file and refuses to run on a mismatch. It also downloads the Tailwind
v4.3.3 standalone binary into the gitignored `crates/pcs-service-ui/.tools/`, so there is no node and
no `package.json` anywhere in this repository. `assets/ui/index.html` is hand-written and never
generated.

### Quick Start component prerequisites

`examples/quickstart/` needs two components that `cargo xtask quickstart` produces into the
gitignored `examples/quickstart/build/`: `validate-go.wasm` (the unmodified
`examples/polyglot/stages/go-validate`) and `settle-cs.wasm`
(`examples/quickstart/stages/csharp-settle`). Three toolchains, all pinned in
`examples/polyglot/PINS.md`: `componentize-go` 0.4.1 with Go 1.25.5+, .NET SDK 10, and `wasm-tools`
1.246.2. Nothing in `cargo test` depends on either artifact.

### Conformance corpus

`packages/arrow-ipc-conformance/` pins all five Arrow IPC codecs to one answer about which streams
are valid. Vectors and manifest are committed, so a codec's suite needs no Rust toolchain.
Regenerate after any wire format or `Order` schema change, then commit the result:

```bash
cargo run -p pcs-service --features conformance --example conformance_vectors -- emit
```

The `conformance` feature exists to enable `arrow-ipc/lz4` for the generator alone, so a normal
build still cannot write a compressed record batch and must reject one.

### Testing

Tests run through [`cargo-nextest`](https://nexte.st), not bare `cargo test`, via
`.config/nextest.toml`. `cargo install cargo-nextest --locked --version 0.9.143` installs it. Two
nextest profiles cover most of the workspace, plus one suite that neither runs by default; which
to reach for depends on why you're running tests:

- `cargo nextest run --workspace --all-features` (the `default` profile): the everyday command.
  It compiles the same `--all-features` closure as always, but skips every test that needs a
  Docker daemon: the testcontainers-backed connector suites
  (`pcs-connector-kafka`/`-nats`/`-postgresql`/`-s3`'s `tests/`), the two whole Raft chaos
  binaries (`transport_chaos`, `distributed_harness_smoke`), the Docker-gated cluster tests in
  `raft_consensus_chaos` (its `unit` and `idempotency` modules are Docker-free and stay), and one
  Docker-backed test apiece in `pcs-service`'s
  `kafka_service`/`nats_service`/`postgres_service`/`s3_service`. Run this constantly; it should
  stay fast regardless of how large the Docker-backed suites grow. The embedded store suite
  (`redb_store`) and the other distributed suites (`distributed_scheduler`, `runner_chaos`,
  `checkpoint_chaos`, `wasm_chaos`, `distributed_processor_state`) need no daemon either: the
  first drives a local redb file, the rest the in-memory store fixture in
  `tests/common/memory_store.rs`.
- `cargo nextest run --workspace --all-features --profile ci --run-ignored all` (the `ci`
  profile): the full suite, including everything `default` skips and any `#[ignore]`d test (today,
  `pcs-service`'s lib unit test `test_second_init_returns_error` (a global-tracing-subscriber
  race), `distributed_integration_chaos.rs`'s `full_stack_chaos_monkey_60s`, a ~70-100s five-node
  Raft-cluster-behind-Toxiproxy chaos run asserting that every node converges on the same applied
  index and that the term advances under combined latency, bandwidth, reset and partition faults,
  plus `connector_matrix.rs`'s `full_matrix`: `ci`'s `default-filter = "all()"` carries none of
  `default`'s exclusions, so this command also pays for the matrix's four containers).
  This is the full suite to run as the last verification step of a plan, not something to reach
  for on every edit. `ci.yml` splits it into three: the `test` job runs `--profile ci` without
  `--run-ignored`, a separate `distributed_chaos` job runs only `full_stack_chaos_monkey_60s`
  (`--test distributed_integration_chaos --run-ignored ignored-only`), and a separate
  `connector_matrix` job runs only `full_matrix`, so that neither test ever sits on `test`'s
  critical path. Regenerating or reordering the Docker/chaos exclusion list itself lives only in
  `.config/nextest.toml`; nothing here restates it, so that file is the one place to update when a
  test moves between the two profiles.
- `crates/pcs-service/tests/connector_matrix.rs` (the `heavy-docker` test group, `ci.yml`'s
  `connector_matrix` job): covers 1152 cases, one per `{source connector, sink connector, byte
  format, processor runtime}` tuple over 8 connectors on each end, 6 formats (the five
  transformers plus the absence of one, for the two connectors that carry `RecordBatch`es
  natively) and 3 processor runtimes (native pipeline, WASM component, native plugin), plus one
  maximal workflow declaring every node kind at once. One format applies to both the source and
  the sink of a case; the independent source-format x sink-format cross product (20736 cases) is
  deliberately not taken, because a mixed pair adds no connector or transformer coverage over the
  two paired cases that already cover each half. `full_matrix` is the one test here that starts a
  container: nextest gives each test *binary*, not each test, its own process, so it alone starts
  exactly one container per external resource (Kafka, NATS, PostgreSQL, MinIO/S3) for its whole
  run, isolating every case by a unique topic, subject, table, object prefix, file path or
  OS-assigned port and running them all concurrently in-process. A rejected case asserts both the
  refusal and where it lands: `build` (`ServiceConfig::load` or `ServiceBuilder::build_all`
  returns an error containing a predicted fragment), `run` (the service builds but the runner
  reports a non-fatal error and no row reaches the sink), or `no rows` (a clean run delivering
  nothing, the way a `tcp` source does when it cannot build a decoder for its format). A build
  refusal touches no live resource, so that coverage holds with no Docker daemon at all; a case
  whose resource has no reachable container is skipped individually instead, so Docker-free
  combinations still run. `full_matrix` is excluded from the `default` profile and `#[ignore]`d,
  so `--profile ci` skips it too; it reaches CI only through the `connector_matrix` job (`cargo
  nextest run -p pcs-service --all-features --profile ci --test connector_matrix --run-ignored
  ignored-only`). `--profile ci` is required there: `--test` selects a cargo build target, not a
  nextest filter, so it cannot bypass `default`'s exclusion of `full_matrix` itself. The job
  builds both the wasm and the native-plugin smoketest fixtures first. The same file also holds
  `dimensions_cover_the_registry`, a Docker-free, non-`#[ignore]`d test asserting the real
  factory registry's source count, sink count and registered transformer formats agree exactly
  with this file's `CONNECTORS`/`FORMATS` lists above; it starts no container and runs under the
  `default` profile like any other fast test.

`cargo test --workspace --all-features --doc` remains a separate, always-bare-`cargo-test` step in
both this file and `ci.yml`: nextest does not run doctests.

Docker-backed tests soft-skip rather than fail when no daemon is reachable: each crate's
`tests/common/mod.rs` exposes a `try_start() -> Option<Container>` that starts a
`testcontainers::GenericImage`, catches the error, prints `SKIP: ... unavailable: {e}`, and returns
`None`; every such `#[tokio::test]` opens with `let Some(x) = common::try_start().await else {
return; };`. This is what lets the `ci` profile run unconditionally on a runner that may or may not
have Docker, and it needs no nextest-side accommodation.
The Raft chaos harness sharpens that rule: only the container step soft-skips.
`RaftClusterHarness::try_start` returns `None` when the Docker daemon cannot supply the Toxiproxy
container, and every step after it panics: host-port resolution, per-edge proxy creation, node
startup, and the `await_listening` check that a node really accepted on its reserved port. That is
what stops a green run from hiding a broken harness.

Every Docker-backed test starts its own fresh container (or, for a Raft chaos test, its own
Toxiproxy container plus an N-node cluster with one proxy per directed edge) with OS-assigned host
ports and nanosecond-unique resource names (topics, subjects, streams). For the connector suites
that per-test isolation is the whole story, and it is what makes running them concurrently safe.
`cargo test`'s default behavior runs one test binary at a time to completion before starting the
next, so today's `kafka_roundtrip`/`nats_roundtrip`/`sink`/`source_cursor`/`source_logical`
binaries each get their own turn; nextest schedules every test from every binary into one global
thread pool (`test-threads`, default `num-cpus`), so Docker-backed tests from different crates run
alongside each other instead of one binary finishing before the next starts.

The Raft chaos suites and the connector matrix are the exception, and the one place a runner
feature does the work. The `heavy-docker` test group caps `max-threads = 1` over
`transport_chaos`, `raft_consensus_chaos`, `distributed_harness_smoke`,
`distributed_integration_chaos` and `connector_matrix`, so those five take the machine one test at
a time. Unlike a connector test, which needs only its own container, each Raft chaos binary
contends for the Docker daemon, for host ports, and for enough CPU to run 3 to 5 raft nodes while
asserting on election and log-convergence deadlines; `connector_matrix` holds four containers
(Kafka, NATS, PostgreSQL, MinIO/S3) at once for its whole run instead. The group is declared once
and applied through an override in **both** profiles: `distributed_integration_chaos` and
`connector_matrix` are in it even though `default` never reaches either binary, because `ci` does.
Adding another heavy-Docker binary to the group means adding it to both `filter` expressions.

## Workspace layout

```
crates/
├── pcs-core/                # Engine primitives: Dataset, Pipeline, System, Scheduler, Component,
│                            # plus the Source/Sink traits and the schema cast helpers.
│                            # Arrow-only dependencies; compiles for host and wasm32-wasip2 processor.
├── pcs-config/              # The configuration language: parses KDL into ConfigValue (an alias for
│                            # serde_json::Value) plus ConfigMap, and exports from_kdl_str,
│                            # one_or_many and substitute_env_vars. ConfigValue is the value type in
│                            # the SourceFactory/SinkFactory/TransformerFactory signatures, so every
│                            # connector and transformer reaches it through pcs-connector or
│                            # pcs-transformer, which re-export it.
├── pcs-connector/           # The factory contract: SourceFactory, SinkFactory, ConnectorContext
│                            # (resolves the format key through the transformer registry),
│                            # parse_schema_fields, parse_optional_schema_fields. Sits below
│                            # pcs-service so a connector needs no dependency on the host.
├── pcs-connector-channel/     # ChannelSource, ChannelSink: in-memory mpsc transport.
├── pcs-connector-datafusion/  # DataFusionSource. No factory: it needs a live SessionContext.
├── pcs-connector-file/        # FileSource, FileSink: local-file IO, format from a transformer.
├── pcs-connector-http/        # HttpSource, HttpSink: one GET spooled through a transformer, and one
│                              # self-contained document per batch back out. No request at build.
├── pcs-connector-kafka/       # KafkaSource, KafkaSink: librdkafka-backed, one topic or several.
├── pcs-connector-nats/        # NatsSource, NatsSink: core subject pub/sub or JetStream, chosen by
│                              # a mode node's kind key.
├── pcs-connector-postgresql/  # PostgresSource, PostgresSink: native (non-JDBC) client, TLS, and
│                              # polling / cdc_trigger / cdc_logical read modes.
├── pcs-connector-s3/          # S3Source, S3Sink: any S3-compatible endpoint, timestamped
│                              # object keys, row/byte/age flush thresholds.
├── pcs-connector-tcp/         # TcpIngestSource, TcpSink: live length-prefixed frames, decoded and
│                              # encoded by a transformer. The source listens, the sink dials, both
│                              # register as "tcp". Framing is transport, decoding is format.
├── pcs-processor/           # Processor SDK: re-exports pcs-core, the export_pipeline! macro, and
│                            # pcs_macros::{Component, transform, fold, processor} plus
│                            # Config/Error/Result, so a processor crate depends only on
│                            # pcs-processor. Owns the canonical WIT package at wit/pipeline.wit;
│                            # pcs-service vendors a byte-identical copy at its own wit/pipeline.wit
│                            # because `cargo package` cannot reach outside a crate's own directory.
├── pcs-processor-smoketest/ # Minimal processor component used as a CI fixture (arrow-ipc drift gate,
│                            # config delivery, cross-batch state).
├── pcs-plugin/              # Native plugin host: dlopen a cdylib exporting the pcs-plugin-abi C ABI.
├── pcs-plugin-abi/          # The C ABI itself: describe/run-batch symbols and their layout.
├── pcs-plugin-smoketest/    # Minimal native plugin used as a CI fixture. cdylib only.
├── pcs-inspector-wire/      # The inspector's JSON contract: Topology, Snapshot, SpanRecord and
│                            # friends. serde only, so both the host and the browser can compile it;
│                            # pcs-service itself cannot target wasm32-unknown-unknown.
├── pcs-macros/              # Proc-macro crate: #[derive(Component)], #[transform], #[fold],
│                            # #[processor] — re-exported through pcs-processor, so a processor
│                            # crate never depends on it directly.
├── pcs-service/             # Host: wasmtime runtime, distributed/Raft, HTTP control plane,
│                            # config loading, the factory Registry, and the pcs-service binary.
│                            # It ships no Source, Sink, or Transformer of its own.
├── pcs-service-ui/          # The /ui live dashboard: CSR Leptos, wasm32-unknown-unknown only, so
│                            # NOT a workspace member. Its committed bundle lives at
│                            # crates/pcs-service/assets/ui/, rebuilt by `cargo xtask ui`.
├── pcs-transformer/           # The byte-format contract: Transformer, BatchReader, BatchWriter,
│                              # MessageDecoder, TransformerFactory, TransformerRegistry.
├── pcs-transformer-arrow-ipc/ # ArrowIpcTransformer: PerBatch messages only, no stream surface.
├── pcs-transformer-avro/      # AvroTransformer: object container files plus PerRow messages,
│                              # framed single-object or Confluent. Options compression, schema_id.
├── pcs-transformer-csv/       # CsvTransformer: stream read and write. Option has_headers.
├── pcs-transformer-ndjson/    # NdjsonTransformer: stream plus PerRow messages. Option infer_max.
└── pcs-transformer-parquet/   # ParquetTransformer: stream read and write, Snappy, estimated_rows.
examples/wasm/order_processing/   # A realistic processor pipeline, built for wasm32-wasip2.
examples/polyglot/                # Six processor components (Go, Python, TypeScript, Kotlin, C#,
                                  # Rust) against one WIT world. Each of the six now declares its
                                  # own Order row type on its language SDK (Go struct tags, Python
                                  # dataclass, TS schema builder, Kotlin data class, C# class, Rust
                                  # derive). generated/ is regenerated by the polyglot_schema_emit
                                  # example for the Quick Start and plugin builds only; the driver
                                  # asserts the six fingerprints agree pairwise. See PINS.md there.
examples/quickstart/              # The runnable Quick Start: NATS to PostgreSQL through the reused
                                  # Go stage and a purpose-built C# stage, two chained pcs-service
                                  # processes, compose file and schema.sql. Built by
                                  # `cargo xtask quickstart` into the gitignored build/.
examples/native/                  # Single-file pcs-core/pcs-service tutorials and feature demos:
                                  # first_pipeline, scheduler_etl(_parallel|_dag), window_aggregation,
                                  # distributed_scheduler, distributed_windowed, windowed_fan_in,
                                  # stream_latency. The two distributed_* demos run on a local
                                  # redb store, so they need no external service.
                                  # Declared as `[[example]]` targets of the pcs-service crate.
examples/distributed_fulfillment/ # A 3-node PCS Raft cluster showcase: field-granular DAG
                                  # scheduling, checkpointing into the raft-replicated
                                  # cluster-app.redb, Docker Compose deployment of the three
                                  # nodes. One `[[example]]` target of pcs-service.
examples/configs/                 # Runnable KDL configs for the pcs-service binary itself (not
                                  # `[[example]]` targets), one per connector plus standalone/cluster
                                  # templates. See its own README.md for the feature-to-config table.
examples/plugins/                 # Native-plugin proofs: native_plugin.rs (a pcs-service example
                                  # loading the pcs-plugin-smoketest fixture) plus settle-go/, the Go
                                  # cross-language plugin `cargo xtask plugins` builds.
examples/branching/               # One long-running stream workflow (branching.kdl) demonstrating
                                  # every fan-out split: a core-subject NatsSource multicasting to a
                                  # mirror sink and two routing processors — the branching-wasm
                                  # component processor and the branching-plugin native plugin —
                                  # each delivering per message to branch-labelled FileSinks. The
                                  # branching_publish pcs-service example feeds the stream. Demo
                                  # processors in wasm/ and plugin/; see its README.md.
examples/windowing/               # Beam-style windowing in one service workflow (windowing.kdl): two
                                  # NATS sources fan into a windowed wasm processor and a windowed
                                  # native plugin (the same logic, duplicated like the branching
                                  # pair), each writing closed-window totals to its own PostgreSQL
                                  # table. The windowed_publish pcs-service example feeds both
                                  # subjects with advancing simulated timestamps. Processors in
                                  # wasm/ and plugin/; see its README.md.
examples/conformance/             # conformance_vectors.rs, the pcs-service example that regenerates
                                  # packages/arrow-ipc-conformance/'s corpus.
examples/connectors/              # One-off connector-crate examples: postgres_roundtrip.rs,
                                  # datafusion_interop.rs, scheduler_parquet_etl.rs.
packages/                         # Apache-2.0 subtree, unlike the rest of the repository. One
                                  # SDK package per language, released as `pcs-sdk`; each SDK
                                  # carries its Arrow IPC codec internally, so the five non-Rust
                                  # polyglot stages consume one package apiece.
├── pcs-sdk-go/          # module github.com/nassor/pcs/packages/pcs-sdk-go, package pcs,
│                        #   codec subpackage arrowipc
├── pcs-sdk-py/          # pcs-sdk, import pcs_sdk, codec submodule pcs_sdk.arrow_ipc
├── pcs-sdk-ts/          # @nassor/pcs-sdk, codec module src/arrow_ipc.ts
├── pcs-sdk-kt/          # io.github.nassor:pcs-sdk-kt, wasmWasi and jvm targets,
│                        #   codec package io.github.nassor.pcs.arrowipc
├── pcs-sdk-kt-ksp/      # io.github.nassor:pcs-sdk-kt-ksp, JVM-only KSP export-glue processor
└── pcs-sdk-cs/          # Pcs.Sdk, codec namespace Pcs.ArrowIpc; generator/ is its Roslyn
                         #   source generator for the export glue
packages/VERSION                  # The one version all five declare. `cargo xtask pack-sdk`
                                  # asserts every manifest matches it.
docs/                             # Zola site: content/*.md front matter, templates/*.html prose.
                                  # Sections: quickstart/ (installation + running it), service/
                                  # (overview, configuration, observability, dashboard), native/
                                  # (overview + tutorial), processors/ (overview, WIT contract, one
                                  # page per language, the six-language example), reference/ (wire
                                  # format), operations/, benchmarks/.
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
xtask/                            # The task runner behind `cargo xtask <command>`: quickstart,
                                  # polyglot, plugins, ui, bench, pack-sdk, validate, demo,
                                  # check-wasm-processor, processor-ipc-roundtrip. One module per
                                  # command, zero dependencies, so it drives Go, .NET, npm, Gradle
                                  # and wasm-tools identically on Windows, Linux and macOS. Exit
                                  # codes are documented per module and CI reads them.
                                  # validate and demo (examples.rs) inject a `variables` block
                                  # into example configs, so they run with no OS env export.
                                  # validate also parses every examples/configs/*.kdl file
                                  # through pcs-service validate --connectors-only, discovered
                                  # from the directory rather than a hand-maintained list.
```

## Feature flags

### `pcs-core`

- `runtime` (**default**): tokio and rayon stage parallelism. Disable for wasm processor builds.
- `processor`: wasm32-wasip2 target, sequential-only execution driven by the `pollster` sync executor.
- `windows`: windowed aggregation. `WindowedSystem`, `WindowSpec`, watermarks, `WindowAccumulator`.
- `io`: the `Source`/`Sink` traits and the schema cast helpers (implies `runtime`).
- `distributed`: types shared with the host's distributed layer (implies `runtime`).
- `tracing`: `tracing` crate integration. Gates the events and the three nested spans
  `pipeline/execution.rs` opens (`pipeline.run`, `pipeline.stage`, `system.execute`).

### `pcs-service`

The **default** bundle is `mimalloc`, `service`, `wasm`, `windows`, `parquet-checkpoint`, every
connector except Kafka, and every transformer, so `cargo install pcs-service` yields a runnable
binary with no flags. `connector-kafka` stays opt-in because `librdkafka-sys` builds vendored C and
needs `cmake` plus a C toolchain; `distributed-raft` and `service-cluster` stay opt-in because a
cluster node is a deliberate deployment choice.

  `connector-channel`, `connector-file`, `connector-http`, `connector-kafka`, `connector-nats`,
  `connector-postgresql`, `connector-s3`, `connector-tcp`: one per connector crate. Each pulls
  the crate in and registers its factories in `register_builtin_factories` (each implies
  `service`).
  `connector-kafka` and `connector-nats` imply `transformer-ndjson` and `connector-tcp` implies
  `transformer-arrow-ipc`, their default formats. `connector-file` and `connector-http` imply no
  transformer: `format` is required there, so the config picks.
- `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`, `transformer-ndjson`,
  `transformer-parquet`: one per transformer crate, registering its factory under its `format`
  name (each implies `service`).
- `parquet-checkpoint`: `ParquetCheckpointStore`, the archival checkpoint store (implies
  `distributed`).
- `windows`: forwards `pcs-core/windows`.
- `metrics`: the OpenTelemetry instruments in `src/metrics.rs`. Pulls `dep:opentelemetry` and
  nothing else, and stands alone: a library embedder can compile the writers without the HTTP
  control plane.
- `inspector`: the in-process telemetry in `src/inspector/`. Time-bounded ring buffers for spans,
  log events and metric samples, fed by one `tracing` layer and one in-memory
  `PushMetricExporter`; read back through the JSON API in `src/service/inspector_api.rs` and the
  `/ui` dashboard. Implies `tracing` and `metrics`, and pulls `dep:pcs-inspector-wire`,
  `dep:tracing-subscriber` and `dep:opentelemetry_sdk`. `service` implies it. Not under `service/`,
  so a library embedder can capture without the axum control plane.
- `wasm`: wasmtime host. `WasmEngine`, `WasmPipelineRuntime`, the `bindgen!` host bindings.
- `plugin`: native plugin host. `NativePluginRuntime` dlopens a shared library exporting the
  `pcs-plugin-abi` C ABI, validates its manifest, and runs each batch through `run-batch`.
- `distributed`: `PartitionSource`, `CheckpointStore`, `DistributedRunner`, `CheckpointStrategy`,
  plus `RedbSharedStore`, which serves both traits over a local redb file. This is the feature
  that pulls `dep:redb`, so `RedbSharedStore::single_node` is a working store with no consensus
  at all, which is what makes the two `distributed_*` examples runnable without a cluster.
- `distributed-raft`: the openraft node that replicates the application state:
  `ArrowRedbLogStore` (the raft log in its own redb file), `ArrowRedbStateMachine` (the
  application tables in `cluster-app.redb`), the driver, and the request/response TCP peer
  transport. `RedbSharedStore::multi_node` proposes every mutation through it (implies
  `distributed`).
- `service`: the `pcs-service` binary. axum HTTP control plane, KDL config through
  `dep:pcs-config`, metrics, standalone runner (implies `pcs-core/io`, `distributed`,
  `parquet-checkpoint`, `tracing`, `metrics`, `inspector`). Also pulls `opentelemetry-otlp` and
  `tracing-opentelemetry` for OTLP span export, and `dep:postcard` for the source cursors
  `RedbStateClient` persists.
  It does **not** imply `distributed-raft`.
- `service-cluster`: cluster/Raft mode on top of `service` (implies `service`, `distributed-raft`).
  A cluster node keeps its state under `node.data_dir`, so a cluster binary needs no other
  feature: `--features service-cluster`.

### `pcs-core`, columnar engine

- **`Component` trait** (`src/component.rs`): any type providing `name() -> &'static str` and an
  Arrow `Schema`. Rows serialize via `serde_arrow`.
- **`Dataset`** (`src/dataset.rs` plus the nine-file `src/dataset/` submodule): Arrow-backed
  columnar container. One `RecordBatch` per registered component; a component may hold fewer rows
  than the dataset's row count (a windowing processor's reduced result component), never more. Holds
  a `SchemaRegistry`, a `ResourceMap`, and an alive bitmap. Supports batch `append`, soft delete
  (`mark_dead`), compaction, and IPC round-trip. Builder: `DatasetBuilder`. Canonical path:
  `pcs_core::dataset::Dataset`.
- **`Row`** (`src/row.rs`): stable row index (`u32`). Invalidated by `compact`.
- **`Resource`** (`src/resource.rs`): boxed Rust singleton stored in `Dataset`, keyed by `TypeId`.
  Not columnar, and **not** serialized by `write_ipc`. The processor SDK relies on that to keep
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
  library code only, because `ServiceConfig.pipeline` is singular and the config-driven binary never
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
how the processor SDK fills the WIT `run-metrics` record.

**`PipelineRuntime`** (`src/runtime.rs`, `runtime` feature) is the host-side seam a swappable
backend implements: `name`, `run_on`, `run_on_with_state`, `declared_components`,
`descriptor_info`, `template_dataset`. `descriptor_info() -> RuntimeDescriptorInfo { name, version,
stateful, schema_fingerprint }` has a default empty body and is the one generic way a host holding
`Box<dyn PipelineRuntime>` can read what an out-of-process runtime says about **itself**:
`WasmPipelineRuntime` maps its cached `describe()` record and `NativePluginRuntime` its validated
manifest, while `Pipeline` keeps the default because a native pipeline has no self-description
beyond `name()`. `RuntimeDescriptorInfo::name` is not `name()`: the latter is the name the host gave
the pipeline, which is the literal `"service"` for every config-loaded WASM module.

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

### `pcs-processor`, processor SDK

Owns `wit/pipeline.wit`, the canonical `pcs:pipeline@0.3.0` WIT package. The processor exports exactly
two functions:

- `describe()`: name, version, component schemas, schema fingerprint, stateful flag.
- `run-batch(input, prior)`: Arrow IPC in, Arrow IPC out, plus metrics and an updated state blob.

The `#[processor]` attribute macro (`pcs-macros`, re-exported through `pcs-processor`) is the
zero-ceremony path: it embeds the WIT, auto-cfgs the wasm32 target, and emits the log/metric/config
helpers. `export_pipeline!` remains the wiring macro for the examples that predate it — windowing,
branching, and the smoketest fixture.
`export_pipeline!(build)` wires a `fn() -> Pipeline` to those exports and emits `pcs_config_get` and
`pcs_config_parse` into the caller's crate. The WIT bindings are caller-side, so the accessors must
be too. `export_pipeline!(build, state = C)` additionally installs a `ProcessorState<C>` resource on the
batch dataset before the systems run and serializes it back into `run-result.checkpoint` afterwards.
State is a resource rather than a registered component because resources do not round-trip through
Arrow IPC, so processor state never leaks into the output. A `RouteDecision` resource is the routing
channel: the macro reads it after the systems run and reports its branch names in
`run-result.routes`, which the host uses to deliver the output only to the links whose `branch`
names one of them (absent = legacy multicast).

The host creates a fresh wasmtime `Store` per call, so `prior`/`checkpoint` is the only channel by
which processor state survives a batch boundary.

### `pcs-service`, host

#### IO layer

`pcs-core`'s `io` feature provides the `Source`/`Sink` traits and the schema-cast helpers. A
connector moves bytes and a transformer turns bytes into `RecordBatch`es and back, so transport
`-channel`, `-file`, `-kafka`, `-nats`, `-postgresql`, `-s3`, `-tcp`. Transformers: `pcs-transformer-arrow-ipc`, `-avro`,
`-csv`, `-ndjson`, `-parquet`, selected by a `format` key. `pcs-connector` holds the
`SourceFactory`/`SinkFactory` contract plus `ConnectorContext`, which resolves that key against
the `TransformerRegistry` in `pcs-transformer`; both sit below `pcs-service` so neither depends
on the host. `pcs-service` owns the `Registry` and `register_builtin_factories`, extended through
`register_source`, `register_sink`, and `register_transformer`. Pipelines integrate via
`drain_into_dataset` and `drain_dataset`.

#### Distributed processing (`src/distributed/`)

Multi-instance batch execution with at-least-once semantics. `pcs-core`'s `distributed` feature
contributes shared types only; all runner code lives here. The application state, meaning master
batches, row-range claims, checkpoints and instance heartbeats, is replicated by the PCS raft
itself and applied into each node's own redb file.

**`distributed` feature:**
- `PartitionSource`: claims, acks, and releases row-range batches across instances
- `CheckpointStore`: persists Arrow IPC snapshots for crash recovery
- `DistributedRunner` and `RunnerConfig`: holds a `Box<dyn PipelineRuntime>` template. Per claimed
  batch it calls `world_factory()` for a fresh `Dataset`, loads the window accumulator and the
  runtime's opaque state blob, calls `runtime.run_on_with_state(&mut partition_data, prior)`, then
  checkpoints and acks. The template's own data, sources, and sinks are never used.
- `CheckpointStrategy`: `EveryStage`, `EveryNStages`, `None`
- `MAX_LOG_ENTRY_BYTES` (1 MiB, `partition.rs`): caps the Arrow IPC payload of a registered master
  batch, and is the `CheckpointStore::max_checkpoint_bytes` trait default, which
  `RedbSharedStore` does not raise because a checkpoint travels inside a raft log entry
- `accumulator_store` and `processor_state_store`: free functions that park the window accumulator and
  the runtime state blob under reserved `stage_idx` sentinels (`ACCUMULATOR_STAGE_SENTINEL`,
  `PROCESSOR_STATE_STAGE_SENTINEL`)
- `ParquetCheckpointStore`: archival checkpoint store (needs `parquet-checkpoint`)

**`distributed-raft` feature** (`src/distributed/consensus/`), the openraft node and everything it
replicates:
- `types.rs`: `PcsTypeConfig` (`openraft::declare_raft_types!`), `ConsensusCommand` and
  `ConsensusResponse`. Every command carrying wall-clock time carries it as a `now_at_propose`
  field, so the state machine stays deterministic on replay
- `state_machine/`: the redb application tables (`arrow_master_batches`, `arrow_claims`,
  `arrow_claims_by_batch`, `arrow_checkpoints`, `arrow_instances`, `arrow_pending_batches`,
  `arrow_sm_meta`) plus the apply handlers, queries and snapshot IO. Records are JSON-encoded;
  only openraft-native persistence uses postcard
- `storage/`: `ArrowRedbLogStore` (`RaftLogReader` + `RaftLogStorage`, calling
  `IOFlushed::io_completed` only after the redb commit) and `ArrowRedbStateMachine`
  (`RaftStateMachine`, `SnapshotData = Cursor<Vec<u8>>`), plus `validate_store_consistency`
- `ArrowRaftDriver`, `ArrowRaftDriverConfig`, `ArrowRaftDriverHandle`:
  `start(config, log_db_path, app_db_path)` opens both redb files, validates them against each
  other, and spawns the proposal loop, whose `write_command` maps openraft's `ForwardToLeader`
  onto `transport::forward_proposal`. The handle exposes `propose`, `app_db`, `metrics`,
  `initialize`, `spawn_tcp_server` and `shutdown`
- `transport/`: length-prefixed request/response TCP with a `serde_json` body,
  `MAX_FRAME_BYTES` 16 MiB. `TcpNetworkFactory`/`TcpNetwork` implement `RaftNetworkV2` over a
  peer pool with a circuit breaker; `RaftTcpServer` answers `AppendEntries`, `Vote`, snapshot
  chunks and `ProposalForward`
- `RedbSharedStore` (`consensus/store.rs`): `SingleNode` applies commands to the local redb file,
  `MultiNode` proposes through the driver and reads from the state machine's own database. Claim
  leases default to `DEFAULT_LEASE_TTL_MILLIS` (90 s) and a propose is bounded by
  `CLUSTER_PROPOSE_TIMEOUT` (30 s)
- `RedbStateClient` (`src/service/redb_state.rs`, `service` feature): the standalone/stream
  persistence path. Config bytes, processor priors and source cursors in one local unreplicated
  redb file declared by `store "redb"`. Stream mode persists cursors and priors whenever a store
  is configured; interval/one-shot state carry is opt-in via
  `store "redb" { batch_resume #true }`

#### WASM host (`src/wasm/`, `wasm` feature)

`WasmEngine` owns the wasmtime `Engine` and epoch ticker. `WasmPipelineRuntime` implements
`pcs_core::runtime::PipelineRuntime`: it serializes the dataset to Arrow IPC, calls the processor's
`run-batch` on a fresh `Store`, and reads the result back. `bindings.rs` is 26 lines of
`wasmtime::component::bindgen!` pointed at `../pcs-processor/wit`, so host bindings cannot drift.
`host_impl.rs` implements the `host-io` imports (`log`, `metric`, `get-config`); `metric` records
the `pcs_processor_metric` histogram, and `runner.rs` records the five `run-metrics` numbers off
every `run-result`.

#### Service layer (`src/service/`, `src/bin/pcs-service/`)

Requires the `service` feature. KDL-driven config, factory registry, HTTP control plane, and
standalone/cluster runners.

Key types:
- `ServiceConfig` and `ServiceMode`: the config schema (`mode "standalone"` or `mode "cluster"`)
- `StoreConfig`: the top-level `store "redb"` block (`path`, `batch_resume`), the local
  unreplicated store the standalone and stream runners persist through. `mode "cluster"` rejects
  a `store` block, because a cluster node's state is the raft-replicated `cluster-app.redb` under
  `node.data_dir`
- `WorkflowSpec`: one declared workflow (standalone may declare several; cluster mode takes
  exactly one). Declared `transformer`, `source`, `wasm`, `plugin` and `sink` nodes, each with a
  mandatory id and an optional name, plus `link` nodes carrying `from`, `to` and an optional
  `branch`. `#[serde(deny_unknown_fields)]` means a key the service cannot honour is a parse
  error, not a silently dropped section. Systems and components cannot be declared in the config
  file. `WorkflowSpec::validate` enforces the load-time graph rules: unique/charset-valid ids,
  every `link` naming a declared node, the graph is acyclic, every source has an outbound link
  and every sink an inbound one, a processor may be fed by any mix of sources and processors
  (the fan-in merge that windowing processors rely on), cluster mode declares exactly one
  processor and no source/sink/link, `run_mode kind="stream"` declares at least one source
  (pulled round-robin), no source outside stream mode is one that never reaches EOF, and every
  `window` block is geometrically sane. The check that a window's time field is carried by every
  component delivered to the node is a builder-time gate in `src/service/validation.rs`.
- `ServiceBuilder` and `BuiltService`: assembles one `BuiltNode` per declared node, in topological
  order, from config plus registered factories. A `BuiltNode` holds its declared id/name/type_name,
  its component (`None` for a processor), a `BuiltNodeKind::{Source,Processor,Sink}` and its
  `downstream` indices into `BuiltService::nodes`. A `wasm`/`plugin` node with no `module`/`library`
  gets its runtime from `with_runtime(id, ..)`, keyed by that node's declared id. With
  `with_inspector(...)` the build also publishes the `Topology` into that inspector, because the
  builder is the only place that knows every node's concrete kind and detail.
- `Registry` plus `pcs_connector::{SourceFactory, SinkFactory}`, re-exported from
  `pcs_service::service::registry`: the whole extension surface.
- `build_topology(&ServiceConfig, &[&[BuiltNode]], version)` (`src/service/topology.rs`): what the
  dashboard draws. One `TopoNode` per `BuiltNode`, in the same topological order, and connector
  options copied through a per-`type` allowlist keyed on the string `SourceFactory::type_name`
  returns — never a blanket copy of `SourceSpec.config`, which holds DSNs and credentials. Keys
  outside the allowlist are dropped, not masked. A `TopoEdge` is one declared `link`, by node id
  directly: node ids are exactly the declared config ids, with no synthetic prefixing or chain
  indexing.
- `validate_workflow_graph(workflow_id, &[BuiltNode])` (`src/service/validation.rs`): the load-time
  schema-agreement gate `ServiceBuilder::build_all` runs on every link — matching components and
  field-for-field identical Arrow schemas at both ends — and `validate_schema_fingerprint`: a
  cluster node refuses to start when the runtime's Arrow schema fingerprint differs from the one
  its persisted checkpoints were written with.
- `run_standalone`, `run_stream` and `run_cluster`: runner entry points. `run_standalone` and
  `run_stream` walk `BuiltService::nodes` directly: each iteration fans a source's batch out to its
  `downstream` nodes, runs every processor in topological order, and stages/writes every sink.
  `run_stream` requires at least one source node and pulls them round-robin, one batch per item.
  `run_cluster` validates `node.data_dir`, whose only four files are `bootstrap.lock`,
  `raft-log.redb`, `cluster-app.redb` and `node-id`, starts the `ArrowRaftDriver`, binds its
  peer listener through `spawn_tcp_server` (eagerly, so a taken address fails at startup rather
  than leaving a member no peer can reach), builds a `RedbSharedStore::multi_node` over the
  driver's own application database with the configured `lease_ttl_ms`, waits for raft to
  settle, then re-enters the `DistributedRunner` loop until cancellation: `run` returns on an
  empty work pool, and a node that exited there would drop its vote before an operator
  registered anything. `bootstrap.lock` and `node-id` are written on **every** node once it
  first reports a leader, which is what makes a follower's own restart pass
  `validate_data_dir`. Shutdown aborts the listener, signals the driver and awaits its task,
  because that task is what closes both redb files.
- HTTP control plane: `/health`, `/ready`, `/metrics`, `/status` (axum-backed), plus the inspector's
  `/api/*` and `/ui` when `observability.inspector.enabled` is set. Those routes are **merged**
  rather than gated inside a handler, so a disabled inspector 404s instead of answering 403.
- `init_logging(&ObservabilityConfig, node_id) -> (TelemetryGuard, Option<Inspector>)`: installs the
  subscriber and, when `observability.otlp_endpoint` is set, the OTLP span exporter. The `Inspector`
  is `None` when capture is disabled, in which case no capture layer is installed at all. The caller
  **must** `telemetry.shutdown().await` before exit; dropping the guard does not flush, because
  `set_tracer_provider` keeps a clone in a process-lifetime static.
- `SpanMetricsLayer`: turns each `pipeline.stage` span into a `pcs_stage_duration_seconds` sample.

CLI subcommands: `serve`, `validate`, `status`, `cluster init`, `cluster join`, `cluster leave`,
`cluster status`. `--config`/`-c` (env `PCS_CONFIG`) defaults to `pcs.kdl`, so `pcs-service serve`
needs no flags; a missing file surfaces as
`error: Configuration error: reading config file pcs.kdl: <os error>` with exit code 1.

#### Observability (`src/metrics.rs`, `src/service/span_metrics.rs`)

Nineteen series, thirteen service and six `pcs_processor_*`, each with a real writer. The full table
of series to writers lives in `docs/templates/tracing.html`.

Node attribution is additive, on nine of the nineteen. `pcs_rows_processed_total` and
`pcs_source_batches_drained_total` are each recorded once with no attributes — the process-wide
total every `/metrics` consumer has always read — and once more under `pcs_inspector_wire::SOURCE_ATTR`
(`source="<id>"`) naming the source node that produced them. `pcs_sink_batches_written_total` does
the same under `SINK_ATTR` (`sink="<id>"`), and all six `pcs_processor_*` series do the same under
`PROCESSOR_ATTR` (`processor="<id>"`). The unattributed value is the sum across every node of that
kind, so a query that adds both forms double counts. `pcs_processor_metric` carries both keys when
attributed, sorted by key: `[("metric", <name>), ("processor", <id>)]`.

`Instruments` copies the dual-impl shape of `crates/pcs-connector-postgresql/src/metrics.rs`: one
real struct under `metrics`, one zero-sized no-op struct without it, identical method surface.
Instruments are process-global, because `ServiceConfig::validate` enforces node ids unique across
every declared workflow, so there is no per-workflow handle to thread through
`WasmPipelineRuntime`, `HostState` or `DistributedRunner`. What those types carry
is the declared id instead: `WasmPipelineRuntime::with_identity(workflow_id, processor_id)` and
`NativePluginRuntime::with_identity`, set by `ServiceBuilder` from the node's own id, and
`HostState.processor_id`, which attributes a `host-io::metric` call.

Four constraints that are not visible from a call site:

- Instruments bind to whichever meter provider is installed when they are first built, so
  `pcs_service::metrics::init()` must run **after** `opentelemetry::global::set_meter_provider`.
- The Prometheus exporter is built with `without_counter_suffixes()`. Instrument names already end
  in `_total`; without it the endpoint exports `pcs_workflow_runs_total_total`.
- Installing a meter provider is a process-global one-shot, and many `pcs-service` lib tests write
  metrics. A lib test that asserts on a series reads `crate::metrics::test_registry()` rather than
  installing its own; an integration test that needs its own provider gets its own test binary,
  which is why `tests/metrics_series.rs` and `tests/processor_metrics.rs` hold one test each.
- `pcs_stage_duration_seconds` is derived host-side from the `pipeline.stage` span a native
  `pcs_core::Pipeline` opens per system, because `pcs-core` carries no metrics dependency and
  `RunStats` has no per-system breakdown. The `EnvFilter` is subscriber-wide, so a filter
  suppressing `pcs_core` spans also stops that histogram.

`host-io::metric` names come from processor code, so distinct names are capped at
`MAX_PROCESSOR_METRIC_NAMES` (256) and further names are dropped after one warning. The native
plugin ABI's `metric` callback is the counterpart of `host-io::metric` but writes no series;
`NativePluginRuntime` records the same six `pcs_processor_*` series as the wasm host from its
per-batch metrics.

#### In-process inspector (`src/inspector/`, `inspector` feature)

Everything the `/api/*` endpoints and the `/ui` dashboard read, captured and served without a
collector, a scraper or external storage. Nothing leaves the process.

- `TimeBoundedBuffer<T>` (`buffer.rs`): a `RwLock<VecDeque>` bounded by **both** a TTL and a hard
  entry cap, drained on every `push` inside the write lock the pusher already holds. Capacity
  evictions are counted and surfaced as `buffers.dropped`; TTL expiry is not, because that is the
  buffer working as configured. A poisoned lock is recovered with `PoisonError::into_inner`, never
  unwrapped: a panicking consumer must not disable telemetry.
- `InspectorLayer` (`layer.rs`): one `tracing_subscriber::Layer` capturing spans **and** events in a
  single pass, installed by `init_logging` on the existing registry. A second span pipeline would
  double-instrument every span `pcs-core` opens. Processor code reaches `tracing` through the WIT
  `host-io::log` import, so field content is untrusted: values are truncated at `MAX_FIELD_BYTES`
  (512), records capped at `MAX_FIELDS` (32), and `("truncated","true")` appended when either bites.
- `InMemoryMetricExporter` (`metric_exporter.rs`): a `PushMetricExporter` on a second
  `PeriodicReader` attached to the **same** `SdkMeterProvider` as the Prometheus exporter, at
  `Temporality::Cumulative`. `ResourceMetrics` derives only `Debug` and is a single buffer the reader
  overwrites every interval, so `export` must copy out owned `SeriesPoint`s synchronously before
  returning — it can neither store nor clone its argument.
- `Inspector` (`mod.rs`): the handle. Unlike `metrics::Instruments` it is **not** process-global —
  the router needs one and tests need isolated instances — so it is cloned (every field is an `Arc`)
  and threaded explicitly. Its module doc carries the full series-to-dashboard-element table.
- `record.rs` re-exports the `pcs-inspector-wire` shapes rather than redefining them, so the buffers
  hold exactly what `/api/*` serves. `trace_id`/`span_id` are `tracing`'s own `span::Id` values, not
  W3C trace ids.
- Edge rates come from each node's own attributed series. An edge between two processors is rated
  from the upstream processor's `PROCESSOR_ATTR`-attributed `pcs_processor_rows_out_total`, and a
  processor-to-sink edge falls back to the sink's own `SINK_ATTR`-attributed
  `pcs_sink_batches_written_total` when no processor row count exists, in batches rather than rows.
  A source-to-processor edge reads the source's own `SOURCE_ATTR`-attributed
  `pcs_rows_processed_total`. An edge whose upstream node has not sampled yet is omitted rather than
  reported as zero, so `Snapshot::edges` is a lookup by `(from, to)`, not a fixed one-entry-per-edge
  list.
- No new metric series. The inspector reads the existing nineteen and adds `Snapshot::span_stats`,
  per-system p50/p95/max derived from retained spans, because `pcs_stage_duration_seconds` is
  recorded with no attributes and per-system latency exists nowhere else for a native pipeline.
  Empty for a wasm-hosted processor, whose spans open inside the guest. A wasm processor's own
  per-batch latency is the `PROCESSOR_ATTR`-attributed `pcs_processor_batch_duration_seconds`
  instead.
- Host-side spans are what fills the traces tab, and their level is what decides whether it has
  anything in it. The five runner spans are **`debug`**: `workflow.batch` (the root each runner
  iteration or claim opens), `source.drain` per source, `runtime.run` per processor, `sink.write`
  per sink, and `processor.batch` from the WASM and plugin hosts. The four `pcs-core` names are
  **`info`**: `pipeline.run`, `pipeline.stage`, `system.execute`, plus `task_attempt`, which opens
  **only on a retry** so a clean run produces none. One whole `debug` tree opens per item, so the
  default `log_level="info"` (filter `pcs=info`) materialises none of it and the traces tab shows
  `pipeline.run`-rooted traces only; `log_level="debug"` restores the per-item waterfall, at roughly
  4.6 µs/item against 7.4 µs/item on the reference machine. Because the runner spans may not exist,
  every runner error and warning names its own `workflow`, `iteration` and node field rather than
  relying on a parent span. `layer.rs`'s module doc carries the authoritative name/level/fields
  table.
- `runtime.run` is the contextual parent of whatever the runtime opens:
  `pipeline.run` for a native `Pipeline`, `processor.batch` for a WASM processor or a native plugin.
  The runners parent their events on the batch span rather than entering it, because the loop awaits
  and an entered guard would adopt every span tokio polls on that thread meanwhile.
  `WasmPipelineRuntime` is the exception in reverse: it re-enters its span inside the
  `spawn_blocking` closure, which is what puts a processor's `host-io::log` lines in the trace.
  `span_stats` is unaffected: it still groups only `pipeline.stage` and `system.execute`, the spans
  a native `pcs_core::Pipeline` opens.
- The `EnvFilter` is subscriber-wide, so a `RUST_LOG` suppressing `pcs_service` empties the span
  buffer and the traces tab, and suppressing `pcs_core` additionally empties `span_stats` — the
  same caveat `pcs_stage_duration_seconds` carries.

## Conventions

- All async traits use `#[async_trait]`; `PipelineRuntime` uses `#[async_trait(?Send)]`.
- Tracing instrumentation is behind `#[cfg(feature = "tracing")]`, and every such site keeps a
  `#[cfg(not(feature = "tracing"))]` fallback so the value is still consumed.
- Metric call sites carry no `#[cfg]`: `crate::metrics::Instruments` has an identical, `#[inline]`,
  empty method surface when the `metrics` feature is off. Do not wrap a metric call in
  `#[cfg(feature = "metrics")]`.
- Public API is re-exported through `pcs_core::prelude::*` and `pcs_service::prelude::*`. There is no
  crate named `pcs`.
- Every runnable example lives under the top-level `examples/` directory, grouped by topic
  (`examples/native/`, `examples/connectors/`, `examples/plugins/`, `examples/conformance/`,
  `examples/configs/`, plus the per-showcase directories `examples/quickstart/`,
  `examples/polyglot/`, `examples/distributed_fulfillment/`, `examples/wasm/`,
  `examples/branching/`, `examples/windowing/`). A crate's `Cargo.toml` still declares the
  `[[example]]` target — that is
  what makes `cargo run -p <crate> --example <name>` work and lets `required-features` gate it —
  but its `path` always points into `examples/...` rather than a `<crate>/examples/` subtree, so
  the example's source never spreads across crates. Add a new example under the topic directory it
  belongs to (or a new one), never back under a crate's own directory.
- Tests live in `#[cfg(test)]` modules within each source file; integration tests are in each crate's
  `tests/`, run through `cargo nextest`. See "Testing" for the fast/full profile split, the
  Docker soft-skip convention, and why testcontainers-backed tests are safe to run in parallel.
- Every new source connector, sink connector, transformer, and processor runtime must be added to
  `crates/pcs-service/tests/connector_matrix.rs`'s dimension lists in the same change that
  introduces it: its entry in the capability table (supported, or rejected at build, at run, or
  with no rows) and its node in the maximal workflow. A connector or transformer not in the matrix
  is not considered wired; that same file's `dimensions_cover_the_registry` test fails when a
  registered factory or transformer format falls outside the matrix's `CONNECTORS`/`FORMATS`
  lists, so an omission here is caught, not just documented.
- Benchmarks use Criterion in each crate's `benches/`. Run them through `cargo xtask bench`, never
  bare `cargo bench`. The harness fixes `RUSTFLAGS`, compiles as a separate step so criterion does
  not share the machine with rustc, and takes the benchmark binary from cargo's own `Executable`
  line. Cargo's metadata hash encodes profile, features, and flags, so binaries built under
  different configurations coexist in `target/release/deps/` and picking the wrong one silently
  re-measures a stale build. Published figures are taken unpinned; `--affinity` is for A/B
  comparison only.
- Code quality is a finishing check, not optional: at the end of every task that touches
  `**/*.rs`, walk `.agents/skills/rust-best-practices/SKILL.md`'s review checklist against every
  changed file — ownership/error-handling intent, no unproven `unwrap`/`expect`/`panic!`,
  Clippy-clean, public-item docs, and test coverage of the new behavior and its error paths — in
  addition to the `cargo fmt`/`cargo clippy`/`cargo test` gates already required above.
- Performance is a finishing check, not optional: `cargo clippy --all-targets --all-features -- -D
  warnings` (already required above) covers Clippy's Perf lint group on every task. A change that
  touches a hot path (`Dataset`/`System`/`Pipeline` execution in `pcs-core`, Arrow IPC ser/de at the
  `pcs-service` wasm host/processor boundary, or the `distributed` checkpoint and redb store paths)
  or a build profile/allocator/dependency setting also needs
  `.agents/skills/rust-performance/SKILL.md`'s triage workflow, validated with
  `cargo xtask bench <name>` before/after.
- `arrow-ipc = "=59.2.0"` is exact-pinned workspace-wide. It is the host to processor wire format and the
  on-disk checkpoint format. See `crates/pcs-processor/PINS.md` before touching it.
- Documentation and code comments: read `.agents/skills/writing-pcs-docs/SKILL.md` before editing
  `README.md`, anything under `docs/`, or `///`/`//!` comments. Current state only, no optimization
  history or task references. No mermaid, ASCII art, or tables as diagrams; use SVG.
