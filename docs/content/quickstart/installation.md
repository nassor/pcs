+++
title = "Installation"
description = "One install command, no feature flags. What the default bundle contains, what stays opt-in, and the three toolchains the tutorial needs."
template = "page.html"
weight = 1
+++

# Installation

<dl class="page-facts">
<dt>In one line</dt>
<dd>One <code>cargo install</code>, then <code>pcs-service serve</code> reads <code>pcs.kdl</code></dd>
<dt>You need</dt>
<dd>Rust 1.95 or newer. The tutorial adds Go, .NET and <code>wasm-tools</code></dd>
<dt>Read this if</dt>
<dd>You want the binary on your PATH before running the tutorial</dd>
</dl>

Next page: [Running it!](@/quickstart/running-it.md).

## Install the binary

The crates are not published, so install from a checkout:

```bash,name=Install from a checkout
git clone https://github.com/nassor/pcs
cd pcs
cargo install --path crates/pcs-service
```

No `--features` flag. The default bundle is what a running service needs:
`service` for the binary target itself, `wasm` for the component runtime,
`mimalloc` for the allocator, `windows` for the platform shims,
`parquet-checkpoint` for checkpoint encoding, seven connectors and five
transformers.

| Group | In the default bundle |
|---|---|
| Connectors | `connector-channel`, `connector-file`, `connector-http`, `connector-nats`, `connector-postgresql`, `connector-s3`, `connector-tcp` |
| Transformers | `transformer-ndjson`, `transformer-csv`, `transformer-parquet`, `transformer-avro`, `transformer-arrow-ipc` |
| Runtime | `service`, `wasm`, `mimalloc`, `windows`, `parquet-checkpoint` |

Check it:

```bash,name=Check the install
pcs-service --version
```

## What stays opt-in

Four features are not in the bundle, each for a reason.

`connector-kafka` needs `cmake` and a C toolchain, because `librdkafka-sys`
builds vendored C. Defaulting it would break the install on a machine that has
neither. Add it when you need Kafka:

```bash,name=Add Kafka support
cargo install --path crates/pcs-service --features connector-kafka
```

`tikv-store` pulls `tikv-client`, and tonic and prost behind it. It carries
`TikvSharedStore`, the shared store the distributed runner claims from, and
`TikvStateClient`, which persists configs, source cursors and processor priors.

`distributed-raft` and `service-cluster` carry the PCS Raft stack: the redb raft
log, the node driver and the TCP peer transport. A cluster node is a deployment
decision, so `mode "cluster"` asks for a binary built with the cluster feature
and TiKV together:

```bash,name=Build a cluster node
cargo install --path crates/pcs-service --features service-cluster,tikv-store
```

See [Operating pcs-service](@/operations/running-pcs.md).

## The config file it looks for

`--config` defaults to `pcs.kdl` in the current directory, so a service with
its config next to it starts with no flags:

```bash,name=Starting with the config next to the binary
pcs-service serve
```

`-c` is the short form and `PCS_CONFIG` the environment variable. A missing file
is a refusal, not a fallback:

```text,name=What a missing config file prints
error: Configuration error: reading config file pcs.kdl: The system cannot find the file specified. (os error 2)
```

Exit code 1. Every command that reads a config accepts the same flag, so
`pcs-service validate` checks the file `serve` would load.

## Toolchains for the tutorial

[Running it!](@/quickstart/running-it.md) builds two WebAssembly components, one
Go and one C#. Three toolchains, all pinned in `examples/polyglot/PINS.md`:

| Tool | Version | For |
|---|---|---|
| `componentize-go` | 0.4.1, with Go 1.25.5 or newer | the Go processor |
| .NET SDK | 10 | the C# processor |
| `wasm-tools` | 1.246.2 | validating both components |

```bash,name=Install the processor toolchains
go install github.com/bytecodealliance/componentize-go@v0.4.1
cargo install wasm-tools --locked --version 1.246.2
```

The .NET SDK comes from
[dotnet.microsoft.com](https://dotnet.microsoft.com/download/dotnet/10.0). The
C# stage needs no `dotnet workload`: its `nuget.config` names the
`dotnet-experimental` feed the LLVM ILCompiler lives on, and the first build
downloads wasi-sdk 29.0, about 535 MB, into `~/.wasi-sdk/`. That download is
published for x86_64 only.

Three toolchains, not the five the six-language polyglot example wants. Python,
TypeScript and Kotlin are not involved here. `examples/polyglot/PINS.md` carries
the per-tool caveats, including the `componentize-go` wrapper that fails on
Windows and the `.zip` to download instead.

## NATS and PostgreSQL

The tutorial needs a NATS server and a PostgreSQL database. A compose file in
the repository brings up both, on the same images the connector test suites
exercise:

```bash,name=Bring up NATS and PostgreSQL
docker compose -f examples/quickstart/docker-compose.yml up -d
```

NATS 2.11 on port 4222 with JetStream enabled, PostgreSQL 18 on port 5432 with
the `pcs` database. `examples/quickstart/schema.sql` is mounted into the
container's initialisation directory, so the destination table exists before
the first write.

Bring your own instead, and point the configs at them with two environment
variables:

```bash,name=Point the configs at your own servers
export PCS_NATS_URL=nats://nats.internal:4222
export PCS_PG_DSN=postgres://user:secret@db.internal:5432/pcs
```

Both configs read `${VAR:-default}` placeholders, so neither needs editing.
