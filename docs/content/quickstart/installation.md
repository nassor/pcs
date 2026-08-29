+++
title = "Installation"
description = "One cargo install from a checkout, no feature flags. Then the three toolchains the tutorial needs: Go, .NET, and wasm-tools."
template = "page.html"
weight = 1
+++

# Installation

<dl class="page-facts">
<dt>In one line</dt>
<dd>One <code>cargo install</code> from a checkout, then <code>pcs-service serve</code> reads <code>pcs.kdl</code></dd>
<dt>You need</dt>
<dd>Rust 1.95 or newer, and <code>git</code>. The tutorial adds Go, .NET and <code>wasm-tools</code></dd>
<dt>Read this if</dt>
<dd>You want the binary on your PATH before running the tutorial</dd>
</dl>

Next page: [Running it!](@/quickstart/running-it.md).

## Step 1: install the binary

The crates are not published to crates.io, so install from a checkout. The
commands run the same on Linux, macOS and Windows (PowerShell):

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
git clone https://github.com/nassor/pcs
cd pcs
cargo install --path crates/pcs-service
```

No `--features` flag. The default bundle is what a running service needs:
the `service` binary target, the `wasm` component runtime, `mimalloc`,
`parquet-checkpoint`, seven connectors and five transformers.

| Group | In the default bundle |
|---|---|
| Connectors | `connector-channel`, `connector-file`, `connector-http`, `connector-nats`, `connector-postgresql`, `connector-s3`, `connector-tcp` |
| Transformers | `transformer-ndjson`, `transformer-csv`, `transformer-parquet`, `transformer-avro`, `transformer-arrow-ipc` |
| Runtime | `service`, `wasm`, `mimalloc`, `windows`, `parquet-checkpoint` |

Check the install:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
pcs-service --version
```

Expected output: `pcs-service 0.1.0`.

## Step 2: the toolchains for the tutorial

[Running it!](@/quickstart/running-it.md) builds one WebAssembly component, a
Rust processor. The component builds with plain `cargo`, but the tutorial
validates it with `wasm-tools`, and the Docker Compose quickstart it points to
afterwards builds a Go component and a C# component. Install all three now.
The versions are pinned in `examples/polyglot/PINS.md`.

Linux/macOS:

```bash
go install github.com/bytecodealliance/componentize-go@v0.4.1
cargo install wasm-tools --locked --version 1.246.2
```

Windows (PowerShell): the same two commands, then a workaround. The
`componentize-go` wrapper that `go install` puts on your PATH downloads its
real binary on first use and asks for a `.tar.gz` the release does not publish.
Download `componentize-go-windows-amd64.zip` from the v0.4.1 release page,
extract it, and put `componentize-go.exe` on your PATH (overwriting the
wrapper in `%GOPATH%\bin` is fine):

```powershell
go install github.com/bytecodealliance/componentize-go@v0.4.1
cargo install wasm-tools --locked --version 1.246.2
```

`componentize-go` needs Go 1.25.5 or newer. Install it from
[go.dev/dl](https://go.dev/dl).

The .NET SDK 10 comes from
[dotnet.microsoft.com](https://dotnet.microsoft.com/download/dotnet/10.0); the
installer runs the same on all three platforms. The C# stage needs no
`dotnet workload`: its `nuget.config` names the `dotnet-experimental` feed the
LLVM ILCompiler lives on, and the first build downloads wasi-sdk 29.0, about
535 MB, into `~/.wasi-sdk/`, published for x86_64 only.

Check all three:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
go version
dotnet --version
wasm-tools --version
```

Each prints its version and exits 0.

## What stays opt-in

Four features are not in the bundle, each for a reason. Add them only when you
need them, with the same `cargo install` from a checkout:

- `connector-kafka` needs `cmake` and a C toolchain, because `librdkafka-sys`
  builds vendored C. Add it when you need Kafka:
  `cargo install --path crates/pcs-service --features connector-kafka`.
- `tikv-store` pulls `tikv-client` (tonic and prost behind it). It carries
  `TikvSharedStore`, the shared store the distributed runner claims from.
- `distributed-raft` and `service-cluster` carry the PCS Raft stack. A cluster
  node is a deliberate deployment choice, built with
  `cargo install --path crates/pcs-service --features service-cluster,tikv-store`.

See [Operating pcs-service](@/operations/running-pcs.md).

## Next

[Running it!](@/quickstart/running-it.md) builds a processor component, runs it
through `pcs-service`, and reads the result. It takes about 15 minutes.
