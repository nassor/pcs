# Branching: three fan-out splits in one workflow

Every `link` in a workflow delivers by default, so a node's output reaches all
of its downstream links at once. A `branch` name on a link makes delivery
conditional: the upstream processor names branches for the batch it just
produced, and the host delivers that batch only to the links carrying those
names. What routing is, when to reach for it, and what load-time validation
refuses: `docs/content/service/branching.md`. This file is the commands.

`branching.kdl` is a long-running `pcs-service` stream carrying every way
output fans out, each split to its own sink:

| Split | Edge | Where each batch goes |
|-------|------|----------------------|
| Source | `in` → `out_mirror`, `router_wasm`, `router_plugin` | Every downstream, unconditionally |
| WASM processor | `router_wasm` → `out_wasm_high` / `out_wasm_low` | Only the branch the decision names |
| Plugin | `router_plugin` → `out_plugin_premium` / `out_plugin_standard` | Only the branch the decision names |

A NATS core subject feeds the workflow in `run_mode kind="stream"`, so every
message is its own batch and the routers decide per message. The publisher
(`branching_publish`, a `pcs-service` example like the Quick Start's
`quickstart_publish`) draws priority 50/50 between `"high"` and `"low"`, so
both branches of both processor splits fire continuously while it runs.

## Build the processors

The same two commands work on every platform, from the repository root:

```bash
cargo build --release -p branching-wasm --target wasm32-wasip2
cargo build -p branching-plugin
```

The plugin artifact name is platform specific. The config's default library
path (`target/debug/libbranching_plugin.so`) is the Linux name, so only macOS
and Windows need `PCS_PLUGIN_LIB`:

| Platform | Plugin artifact | `PCS_PLUGIN_LIB` |
|----------|-----------------|------------------|
| Linux | `target/debug/libbranching_plugin.so` | not needed (config default) |
| macOS | `target/debug/libbranching_plugin.dylib` | `target/debug/libbranching_plugin.dylib` |
| Windows | `target/debug/branching_plugin.dll` | `target/debug/branching_plugin.dll` |

## Run it

Start NATS, then the service, then the publisher. `PCS_OUT_DIR` names the
directory the five sink files land in; it must exist before `serve` starts.

### NATS

```bash
docker run -d --name pcs-nats -p 4222:4222 nats:2.11-alpine
```

Stop it with `docker rm -f pcs-nats`.

### Linux

```bash
mkdir -p /tmp/pcs-branching

PCS_OUT_DIR=/tmp/pcs-branching \
cargo run -p pcs-service --features connector-file,transformer-csv,wasm,plugin -- serve \
  --config examples/branching/branching.kdl
```

Then, in another terminal:

```bash
cargo run -p pcs-service --example branching_publish -- --rate 50
```

### macOS

The same as Linux, with `PCS_PLUGIN_LIB` set on the serve command:

```bash
mkdir -p /tmp/pcs-branching

PCS_OUT_DIR=/tmp/pcs-branching \
PCS_PLUGIN_LIB=target/debug/libbranching_plugin.dylib \
cargo run -p pcs-service --features connector-file,transformer-csv,wasm,plugin -- serve \
  --config examples/branching/branching.kdl
```

### Windows (PowerShell)

```powershell
mkdir C:\pcs-branching

$env:PCS_OUT_DIR = "C:/pcs-branching"
$env:PCS_PLUGIN_LIB = "target/debug/branching_plugin.dll"
cargo run -p pcs-service --features connector-file,transformer-csv,wasm,plugin -- serve --config examples/branching/branching.kdl
```

Then, in another terminal:

```powershell
cargo run -p pcs-service --example branching_publish -- --rate 50
```

The config's paths use forward slashes, which work on Windows too; the
`C:/pcs-branching` output path above follows the same rule.

## What each file holds

The publisher's messages carry `priority "high"` or `priority "low"`.

| Output | Holds |
|--------|-------|
| `out-mirror.csv` | every message, in arrival order |
| `out-wasm-high.csv` | the `"high"` messages |
| `out-wasm-low.csv` | the `"low"` messages |
| `out-plugin-premium.csv` | the `"high"` messages |
| `out-plugin-standard.csv` | the `"low"` messages |

`out_mirror` proves the source split: every batch is written there verbatim
while the same batch feeds both routers. The wasm router (`examples/branching/
wasm`) reads the first row's priority and inserts a `RouteDecision` naming
branch `high` or `low`; the plugin (`examples/branching/plugin`) maps the same
values to branches `premium` and `standard`. The host delivers each batch only
to the links whose `branch` the decision names, which is what separates the
sinks. The CSV header is written once, before the first batch, so each file
stays a readable table however long the stream runs.

The publisher's flags: `--count` (0 runs until Ctrl-C, the default), `--rate`
messages per second (default 50), `--url` (default `nats://localhost:4222`),
`--subject` (default `branching.orders`), `--seed`.

Validate the config first, if you like (set `PCS_PLUGIN_LIB` as in the table
above on macOS and Windows):

```bash
cargo run -p pcs-service --features connector-file,transformer-csv,wasm,plugin -- validate \
  --config examples/branching/branching.kdl --strict
```
