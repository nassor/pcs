# PCS Service Example Configs

This directory contains runnable TOML configurations for `pcs-service`.

## Required features

Every config in this directory declares a `[pipeline.wasm]` table, and that key
only exists when the `wasm` feature is on. `service` does **not** imply it, so
`--features service` alone rejects these files with
`unknown field 'wasm', there are no fields`. Build with:

| Config | Features |
|--------|----------|
| `standalone.toml`, `standalone_wasm.toml`, `extension_example.toml` | `service,wasm` |
| `cluster.toml` | `service-cluster,wasm` |

## Built-in factories

The stock `pcs-service` binary ships with the following factory types.

### Sources (require `--features io`, included by `service`)

| TOML `type`     | Description                               | Required config keys               |
|-----------------|-------------------------------------------|------------------------------------|
| `CsvSource`     | Reads a CSV file into a component column  | `path`, `schema_fields`            |
| `JsonSource`    | Reads a newline-delimited JSON file       | `path`, `schema_fields`            |
| `ParquetSource` | Reads a Parquet file                      | `path`                             |
| `ChannelSource` | In-process channel (testing/internal)     | (none)                             |

### Sinks (require `--features io`, included by `service`)

| TOML `type`    | Description                                | Required config keys               |
|----------------|--------------------------------------------|------------------------------------|
| `CsvSink`      | Writes a CSV file from a component column  | `path`, `schema_fields`            |
| `JsonSink`     | Writes a newline-delimited JSON file       | `path`, `schema_fields`            |
| `ParquetSink`  | Writes a Parquet file                      | `path`, `schema_fields`            |
| `ChannelSink`  | In-process channel (testing/internal)      | (none)                             |

**Important**: file-backed sinks (`CsvSink`, `JsonSink`, `ParquetSink`) open
(create) the output file immediately when the factory is built — including
during `pcs-service validate`. The parent directory must exist before running
`validate` or `serve`. The example configs use `/tmp/` directly to avoid this.

### Components and systems

There are no component or system factories: the TOML has no path for declaring
either. The pipeline is supplied one of two ways —

- `[pipeline.wasm]` names a WASM guest component, which reports its components
  through the `describe()` export, or
- a custom binary hands `ServiceBuilder::with_runtime` a
  `Box<dyn PipelineRuntime>` and omits the `[pipeline]` table entirely.

A `[[pipeline.systems]]` or `[[pipeline.components]]` key is now a parse error
rather than a silently-dropped section.

### Supported Arrow types for `schema_fields`

`Boolean`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`,
`UInt64`, `Float32`, `Float64`, `Utf8`, `LargeUtf8`, `Binary`, `Date32`,
`Date64`. All names are case-insensitive.

---

## Standalone vs cluster mode

| Feature              | `mode = "standalone"`           | `mode = "cluster"`                       |
|----------------------|---------------------------------|------------------------------------------|
| Feature flag         | `service`                       | `service-cluster`                        |
| Consensus            | None                            | Raft (openraft)                          |
| `[[sources]]` allowed | Yes                            | No — validation error if declared        |
| `[[sinks]]` allowed  | Yes                             | Yes                                      |
| Ingestion mechanism  | `Source` trait (file/channel)   | `PartitionSource` (distributed pull)     |
| Crash recovery       | Restart from source             | Checkpoint + lease semantics             |
| Minimum nodes        | 1                               | 1 (1-node Raft is valid for testing)     |

---

## Files in this directory

| File                      | Description                                        |
|---------------------------|----------------------------------------------------|
| `standalone.toml`         | Runnable single-node config using built-in types   |
| `cluster.toml`            | Runnable cluster template using built-in types     |
| `standalone_wasm.toml`    | Standalone config that loads a WASM guest pipeline |
| `extension_example.toml`  | Non-runnable template showing user-defined types   |
| `fixtures/orders.csv`     | Tiny CSV fixture used by `standalone.toml`         |

---

## How to run the standalone example

```bash
# Build the service binary (once).
cargo build --features service,wasm --bin pcs-service

# Validate the config (no side-effects; exits 0 on success).
cargo run --features service,wasm --bin pcs-service -- validate \
  --config examples/configs/standalone.toml --strict

# Run the pipeline (reads fixtures/orders.csv, writes /tmp/pcs-standalone-orders-out.csv).
cargo run --features service,wasm --bin pcs-service -- serve \
  --config examples/configs/standalone.toml
```

The process exits after one pipeline iteration because `run_mode.kind = "one_shot"`
is set. Check `/tmp/pcs-standalone-orders-out.csv` for the output.

Expected output from `validate --strict`:

```
OK: config is structurally valid
  node.id:  1
  node.name: pcs-standalone
  mode:     standalone
  pipeline: pipelines/orders.wasm
  sources:  1
  sinks:    1
  http.bind: 127.0.0.1:0
  log_level: info
OK: all declared types resolved in built-in registry
```

---

## How to validate the cluster example

Cluster mode requires `--features service-cluster`. Validating with the base
`service` feature parses the TOML correctly but attempting to `serve` will fail.

```bash
PCS_NODE_ID=1 PCS_DATA_DIR=/tmp/pcs-node-1 \
cargo run --features service-cluster,wasm --bin pcs-service -- validate \
  --config examples/configs/cluster.toml --strict
```

Expected output:

```
OK: config is structurally valid
  node.id:  1
  node.name: pcs-node-1
  mode:     cluster
  pipeline: pipelines/events.wasm
  sources:  0
  sinks:    1
  http.bind: 0.0.0.0:8080
  log_level: info
OK: all declared types resolved in built-in registry
```

To run a three-node cluster you need three processes, each with a distinct
`PCS_NODE_ID` and `PCS_DATA_DIR`, with `PCS_BOOTSTRAP=true` on exactly one
node during the first bring-up. See the comments in `cluster.toml` and
`docs/operations/running-pcs.md` for the step-by-step procedure.

---

## How to extend pcs-service with user factories

The stock binary calls `register_builtin_factories(ServiceBuilder::new())`. Fork
`src/bin/pcs-service/main.rs` (or write your own binary) and add your own
factories before calling `builder.build(&config)`:

```rust
use pcs_service::service::ServiceBuilder;
use pcs_service::service::factories::register_builtin_factories;

let builder = register_builtin_factories(ServiceBuilder::new())
    .register_source(MyKafkaSourceFactory)
    .register_sink(MyPostgresSinkFactory);

let built = builder.build(&config)?;
```

Sources and sinks are the whole factory surface. The pipeline itself is either a
`[pipeline.wasm]` module or a `Box<dyn PipelineRuntime>` passed to
`ServiceBuilder::with_runtime`.

See `extension_example.toml` for a commented config showing all the types you
would register in a real order-processing service. Validate it to see the
unknown-factory warning behavior:

```bash
cargo run --features service,wasm --bin pcs-service -- validate \
  --config examples/configs/extension_example.toml
# exits 0, warns about unknown types (KafkaSource, PostgresSink, etc.)

cargo run --features service,wasm --bin pcs-service -- validate \
  --config examples/configs/extension_example.toml --strict
# exits 1 — unknown types are errors in --strict mode
```
