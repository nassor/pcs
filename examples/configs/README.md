# PCS Service Example Configs

This directory contains runnable KDL configurations for `pcs-service`.

## Required features

Every config in this directory declares a `wasm` node inside its `workflow`
block, and that node only exists when the `wasm` feature is on. `service` does
**not** imply it, so `--features service` alone rejects these files with
`unknown field 'wasm', there are no fields`. Each `type="..."` also needs the
feature that registers its factory. Build with:

| Config | Features |
|--------|----------|
| `standalone.kdl`, `standalone_wasm.kdl`, `standalone_polyglot.kdl`, `extension_example.kdl` | `connector-file,transformer-csv,wasm` |
| `standalone_plugin.kdl` | `connector-file,transformer-csv,plugin` |
| `postgresql.kdl` | `connector-postgresql,wasm` |
| `s3.kdl` | `connector-s3,transformer-csv,wasm` |
| `cluster.kdl` | `service-cluster,connector-file,transformer-csv,wasm` |
| `tcp.kdl` | `connector-tcp,wasm` |
| `http.kdl` | `connector-http,transformer-csv,wasm` |
| `tikv.kdl` | `tikv-store,connector-file,transformer-csv` |

## Built-in factories

Each factory lives in the crate that owns it and reaches the registry only when
its `pcs-service` feature is on. `service` alone registers nothing.

A connector moves bytes and a transformer decides what they mean, so a config
that names a file needs both: a `source`/`sink` node with `type="FileSource"`/
`type="FileSink"` from `connector-file`, and a declared `transformer "..."
format="csv"` node from `transformer-csv` that the source/sink references by id
through its own `transformer="..."` property. Kafka, NATS and TCP resolve their
byte format the same way and default to none: a byte-carrying source or sink
always names its `transformer` explicitly, there is no implicit default format.

### Sources

| config `type`     | Description                              | Required config keys      | Crate | Feature |
|-------------------|------------------------------------------|---------------------------|-------|---------|
| `FileSource`      | Reads a local file in whatever format its `transformer` names | `path`       | `pcs-connector-file` | `connector-file` |
| `HttpSource`      | One GET, decoded in whatever format its `transformer` names | `url`, plus `schema_fields` where the format needs it | `pcs-connector-http` | `connector-http` |
| `PostgresSource`  | Polling, outbox or `pgoutput` reads      | `name`, `connection`, `mode`, `schema_fields` | `pcs-connector-postgresql` | `connector-postgresql` |
| `KafkaSource`     | Consumes a Kafka topic                   | `brokers`, `topic`, `schema_fields` | `pcs-connector-kafka` | `connector-kafka` |
| `S3Source`        | Lists a prefix once and drains every object in key order | `connection`, `schema_fields` | `pcs-connector-s3` | `connector-s3` |
| `tcp`             | Live framed messages off a listener, stream mode only | `bind`, `schema_fields`   | `pcs-connector-tcp` | `connector-tcp` |
| `ChannelSource`   | In-process channel (testing/internal)    | `schema_fields`           | `pcs-connector-channel` | `connector-channel` |

### Sinks

| config `type`   | Description                               | Required config keys      | Crate | Feature |
|-----------------|-------------------------------------------|---------------------------|-------|---------|
| `FileSink`      | Writes a local file in whatever format its `transformer` names | `path`, `schema_fields`, optional `truncate` | `pcs-connector-file` | `connector-file` |
| `HttpSink`      | One request per batch, body written by its `transformer` | `url`, `schema_fields`, optional `method` | `pcs-connector-http` | `connector-http` |
| `PostgresSink`  | `COPY FORMAT binary`, optional upsert     | `name`, `connection`, `table`, `schema_fields` | `pcs-connector-postgresql` | `connector-postgresql` |
| `KafkaSink`     | Produces to a Kafka topic                 | `brokers`, `topic`, `schema_fields` | `pcs-connector-kafka` | `connector-kafka` |
| `S3Sink`        | Accumulates rows and uploads one object per flush | `connection`, `schema_fields` | `pcs-connector-s3` | `connector-s3` |
| `tcp`           | Dials a peer and writes one length-prefixed frame per message | `connect`, `schema_fields` | `pcs-connector-tcp` | `connector-tcp` |
| `ChannelSink`   | In-process channel (testing/internal)     | `schema_fields`           | `pcs-connector-channel` | `connector-channel` |

### Retry

Every `source` and `sink` retries a failed operation with exponential backoff
by default: 4 attempts, a 100 ms base, 2.0x growth, a 30 s cap and 0.1 jitter.
An optional `retry` child on a `source` or `sink` overrides the policy per
node; `max_attempts=1` disables retrying. `standalone.kdl` and
`postgresql.kdl` carry explicit `retry` blocks, and
`docs/content/service/configuration.md` documents every key.

### Transformers

A workflow declares each byte format it needs as its own `transformer "id"
format="..."` node, and every source/sink that moves bytes in that format
names it through its own `transformer="id"` property. `options` is an
optional table handed to the format's own factory.

| `format`      | Stream read | Stream write | Message codec | Schema rule | Crate | Feature |
|---------------|-------------|--------------|---------------|-------------|-------|---------|
| `csv`         | yes         | yes          | no            | `schema_fields` required | `pcs-transformer-csv` | `transformer-csv` |
| `ndjson`      | yes         | yes          | one per row   | inferred when absent | `pcs-transformer-ndjson` | `transformer-ndjson` |
| `parquet`     | yes         | yes          | no            | read from the file; `schema_fields` rejected on a source | `pcs-transformer-parquet` | `transformer-parquet` |
| `avro`        | yes         | yes          | one per row   | read from the file; `schema_fields` rejected on a source, required on a sink and on the message surface | `pcs-transformer-avro` | `transformer-avro` |
| `arrow-ipc`   | no          | no           | one per batch | `schema_fields` required | `pcs-transformer-arrow-ipc` | `transformer-arrow-ipc` |

`options`: `csv` takes `has_headers` (bool, default `#true`), `ndjson`
takes `infer_max` (integer, default `1024`), `avro` takes `compression` (string,
one of `null`, `deflate`, `snappy`, `zstd`, default `null`) and `schema_id`
(integer, the Confluent registry id), and the other two take none.

**Important**: `FileSink` opens the output file as soon as the factory is
built, `pcs-service validate` included, so the parent directory must exist
before running `validate` or `serve`. The file is created when it is missing
and its existing bytes are kept: rows land after them. Set `truncate #true` in
the sink's `config` to replace the file on every build instead, which is what
the example configs do.

### Components and systems

There are no component or system factories: the config file has no path for
declaring either. Each processor node in the workflow is supplied one of two
ways:

- a `wasm`/`plugin` node names a WASM or native-plugin processor component,
  which reports its components through the `describe()` export, or
- a custom binary hands `ServiceBuilder::with_runtime` a
  `Box<dyn PipelineRuntime>` keyed by that node's declared id, and the node
  itself omits `module`/`library`.

A `systems` or `components` node under `workflow` is a parse error, not a
silently dropped section.

### Supported Arrow types for `schema_fields`

`Boolean`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`,
`UInt64`, `Float32`, `Float64`, `Utf8`, `LargeUtf8`, `Binary`, `Date32`,
`Date64`. All names are case-insensitive.

---

## Standalone vs cluster mode

| Feature              | `mode "standalone"`             | `mode "cluster"`                         |
|----------------------|---------------------------------|------------------------------------------|
| Feature flag         | `service`                       | `service-cluster`                        |
| Consensus            | None                            | Raft (openraft)                          |
| `source` nodes allowed | Yes                           | No, validation error if declared         |
| `sink` nodes allowed  | Yes                             | No, validation error if declared         |
| `link` nodes allowed  | Yes                             | No, validation error if declared         |
| Ingestion mechanism  | `Source` trait (file/channel)   | `PartitionSource` (distributed pull)     |
| Crash recovery       | Restart from source             | Checkpoint + lease semantics             |
| Minimum nodes        | 1                               | 1 (1-node Raft is valid for testing)     |

A cluster-mode workflow declares exactly one processor node (`wasm` or
`plugin`) and nothing else: the distributed runner ingests through
`PartitionSource` and checkpoints its output, so there is no local sink to
declare either.

---

## Files in this directory

| File                      | Description                                        |
|---------------------------|----------------------------------------------------|
| `standalone.kdl`          | Runnable single-node config using built-in types   |
| `cluster.kdl`             | Runnable cluster template using built-in types     |
| `standalone_wasm.kdl`     | Standalone config that loads a WASM processor pipeline |
| `extension_example.kdl`   | Non-runnable template showing user-defined types   |
| `fixtures/orders.csv`     | Tiny CSV fixture used by `standalone.kdl`          |
| `standalone_polyglot.kdl` | Runs the Python processor from `examples/polyglot/` |
| `kafka.kdl`               | Runnable config driving Kafka at both ends, needs a broker |
| `nats.kdl`                | Runnable config driving NATS at both ends, needs a server |
| `s3.kdl`                  | Runnable config driving S3 at both ends, needs a bucket |
| `tcp.kdl`                 | Runnable config driving TCP at both ends, listens and dials |
| `http.kdl`                | Runnable config driving HTTP at both ends, needs an endpoint |

---

## How to run the standalone example

```bash
# Build the service binary (once).
cargo build --features connector-file,transformer-csv,wasm --bin pcs-service

# Validate the config (no side-effects; exits 0 on success).
cargo run --features connector-file,transformer-csv,wasm --bin pcs-service -- validate \
  --config examples/configs/standalone.kdl --strict

# Run the pipeline (reads fixtures/orders.csv, writes /tmp/pcs-standalone-orders-out.csv).
cargo run --features connector-file,transformer-csv,wasm --bin pcs-service -- serve \
  --config examples/configs/standalone.kdl
```

The process exits after one pipeline iteration because `run_mode` sets
`kind="one_shot"`. Check `/tmp/pcs-standalone-orders-out.csv` for the output.

Expected output from `validate --strict`:

```
OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
  node.id:  1
  node.name: pcs-standalone
  mode:     standalone
  workflow: orders
  processors: pipelines/orders.wasm
  sources:  1
  sinks:    1
  http.bind: 127.0.0.1:0
  log_level: info
OK: all declared types resolved in built-in registry
```

---

## How to validate the cluster example

Cluster mode requires `--features service-cluster`. Validating with the base
`service` feature parses the config correctly but attempting to `serve` will fail.

```bash
PCS_NODE_ID=1 PCS_DATA_DIR=/tmp/pcs-node-1 \
cargo run --features service-cluster,connector-file,transformer-csv,wasm --bin pcs-service -- validate \
  --config examples/configs/cluster.kdl --strict
```

Expected output:

```
OK: workflow graph validated (components and schemas agree end to end)
OK: config is structurally valid
  node.id:  1
  node.name: pcs-node-1
  mode:     cluster
  workflow: events
  processors: pipelines/events.wasm
  sources:  0
  sinks:    0
  http.bind: 0.0.0.0:8080
  log_level: info
OK: all declared types resolved in built-in registry
```

To run a three-node cluster you need three processes, each with a distinct
`PCS_NODE_ID` and `PCS_DATA_DIR`, with `PCS_BOOTSTRAP=true` on exactly one
node during the first bring-up. See the comments in `cluster.kdl` and
`docs/operations/running-pcs.md` for the step-by-step procedure.

---

## How to extend pcs-service with user factories

The stock binary calls `register_builtin_factories(ServiceBuilder::new())`. Fork
`src/bin/pcs-service/main.rs` (or write your own binary) and add your own
factories before calling `builder.build_all(&config)`:

```rust
use pcs_connector::{SinkFactory, SourceFactory};
use pcs_service::service::ServiceBuilder;
use pcs_service::service::factories::register_builtin_factories;

let builder = register_builtin_factories(ServiceBuilder::new())
    .register_source(MyMongoSourceFactory)
    .register_sink(MyClickHouseSinkFactory);

let built = builder.build_all(&config)?;
```

Sources, sinks and transformers are the whole factory surface;
`register_transformer` adds a byte format the same way. A processor node is
either a `wasm`/`plugin` node naming a module/library, or a
`Box<dyn PipelineRuntime>` passed to `ServiceBuilder::with_runtime` keyed by
that node's declared id, with `module`/`library` omitted on the node itself.

See `extension_example.kdl` for a commented config showing all the types you
would register in a real order-processing service. Validate it to see the
unknown-factory warning behavior:

```bash
cargo run --features connector-file,transformer-csv,wasm --bin pcs-service -- validate \
  --config examples/configs/extension_example.kdl
# exits 0, warns about unknown types (MongoSource, ClickHouseSink)

cargo run --features connector-file,transformer-csv,wasm --bin pcs-service -- validate \
  --config examples/configs/extension_example.kdl --strict
# exits 1, unknown types are errors in --strict mode
```
