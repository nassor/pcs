# Windowing: two NATS streams, one fan-in, two windowed processors

A `window` block turns an unbounded stream into per-key aggregates over
bounded slices of event time. The host validates the geometry, tracks the
node's watermark and injects the declaration into the processor's config; the
processor keeps the open windows in its checkpoint state and emits the closed
ones. What a window is, what closes one and every key in the block:
`docs/content/service/windowing.md`. This file is the commands.

`windowing.kdl` is that in one long-running `pcs-service` stream. Two core
NATS subjects fan into two processors carrying identical logic, one a
WebAssembly component and one a native plugin, so their two tables should
agree row for row.

| Stage | Node | What happens |
|-------|------|--------------|
| Sources | `sales_a`, `sales_b` | two core NATS subjects, pulled round-robin, one batch per item |
| WASM processor | `window_wasm` | merges both streams' batches into 30s tumbling windows, emits closed windows |
| Plugin | `window_plugin` | the identical logic as a native plugin |
| Sinks | `wasm_totals`, `plugin_totals` | one PostgreSQL table per processor |

Both nodes declare the same block: tumbling, 30 000 ms, keyed by `symbol`,
5 000 ms of allowed lateness.

## Prerequisites

- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`
- A Docker daemon, for the NATS and PostgreSQL containers

## Build the processors

The same two commands work on every platform, from the repository root:

```text
cargo build --release -p windowing-wasm --target wasm32-wasip2
cargo build --release -p windowing-plugin
```

The plugin artifact name is platform specific. The config's default library
path (`target/release/libwindowing_plugin.so`) is the Linux name, so only macOS
and Windows need `PCS_PLUGIN_LIB`:

| Platform | Plugin artifact | `PCS_PLUGIN_LIB` |
|----------|-----------------|------------------|
| Linux | target/release/libwindowing_plugin.so | not needed (config default) |
| macOS | target/release/libwindowing_plugin.dylib | target/release/libwindowing_plugin.dylib |
| Windows | target/release/windowing_plugin.dll | target/release/windowing_plugin.dll |

## Run it

Start NATS and PostgreSQL, then the service, then the publisher.

1. Start the containers:

```text
docker compose -f examples/windowing/docker-compose.yml up -d
```

The compose file brings up `nats:2.11-alpine` and `postgres:18-alpine` and
runs `schema.sql` on first initialisation, which creates the two tables.
PostgreSQL only runs the init scripts when its data directory is empty: if
the volume was initialised before `schema.sql` existed (or by another
project's compose file), the sinks fail with `table ... does not exist`.
Recreate the volume (`docker compose -f examples/windowing/docker-compose.yml
down -v`, then `up -d` again) or apply the SQL by hand:

```text
docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d pcs < examples/windowing/schema.sql
```
Windows (PowerShell):

```powershell
Get-Content examples/windowing/schema.sql | docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d pcs
```

Stop everything with `docker compose -f examples/windowing/docker-compose.yml
down -v`.

2. Start the service.

Linux:

```text
cargo run -p pcs-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- serve \
  --config examples/windowing/windowing.kdl
```
Windows (PowerShell), on one line, with `$env:PCS_PLUGIN_LIB` set as in the
table above:

```powershell
cargo run -p pcs-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- serve --config examples/windowing/windowing.kdl
```

macOS: the same, with `PCS_PLUGIN_LIB=target/release/libwindowing_plugin.dylib`
set on the command. Windows (PowerShell): the same, with
`$env:PCS_PLUGIN_LIB = "target/release/windowing_plugin.dll"`.

3. Publish, in another terminal:

```text
cargo run -p pcs-service --example windowed_publish -- --rate 20 --ts-step-ms 2000
```

## What each table holds

The publisher's messages carry `timestamp_ms` (simulated event time,
advancing `--ts-step-ms` per message), `symbol` (AAPL, GOOG or MSFT) and
`amount` (1.0 to 100.0). At the defaults, 20 messages per second and 2000 ms
of simulated time each, the simulated clock runs 40 seconds per wall second,
so a 30-second window closes roughly every 0.75 wall seconds and the tables
fill continuously.

| Table | Holds |
|-------|-------|
| `public.wasm_window_totals` | one row per closed (window, symbol) group from the wasm processor |
| `public.plugin_window_totals` | the same rows from the plugin processor |

Both tables have the same columns: `window_id` (tumbling window index; the
window start in milliseconds is `window_id * 30000`), `symbol`, `count` and
`sum`. The sinks upsert on `(window_id, symbol)`, so a re-run of the
publisher or a late re-fire within the lateness budget updates a row instead
of duplicating it. The last window seen before the publisher stops stays
open: a window only closes once the watermark passes its end, and the
watermark only moves with the data.

```text
docker compose -f examples/windowing/docker-compose.yml exec -T postgres psql -U postgres -d pcs -c 'SELECT * FROM public.wasm_window_totals ORDER BY window_id, symbol;'
```

## What the dashboard shows

Open http://127.0.0.1:8080/ui. Both processor boxes carry the `⟐30s` window
chip; their detail sheets list the window geometry, the time field, the key
field, the lateness budget, and the live watermark in UTC. The processors
also report `window.open`, `window.closed` and `window.late_rows` through
host-io::metric, which appear in each sheet's processor-metrics section. The
two boxes stay in lockstep: identical inputs, identical windowing logic, two
tables.

## The publisher's flags

`--count` (0 runs until Ctrl-C, the default), `--rate` messages per second
(default 20), `--ts-step-ms` simulated milliseconds per message (default
2000), `--url` (default `nats://localhost:4222`), `--subject-a` /
`--subject-b` (defaults `windowing.sales.a` / `windowing.sales.b`), `--seed`.

Validate the config first, if you like (set `PCS_PLUGIN_LIB` as in the table
above on macOS and Windows):

```text
cargo run -p pcs-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- validate \
  --config examples/windowing/windowing.kdl --strict
```
Windows (PowerShell), on one line:

```powershell
cargo run -p pcs-service --features connector-nats,connector-postgresql,transformer-ndjson,wasm,plugin -- validate --config examples/windowing/windowing.kdl --strict
```

## Files

| File / directory | What it is |
|------------------|------------|
| `windowing.kdl` | the stream workflow with the fan-in and both windowed processors |
| `windowed_publish.rs` | the `pcs-service` example that feeds both NATS subjects |
| `schema.sql` | the two `window_totals` tables, mounted into the Postgres container |
| `docker-compose.yml` | NATS 2.11 and PostgreSQL 18 |
| `wasm/` | the `windowing-wasm` processor component (cdylib, wasm32-wasip2) |
| `plugin/` | the `windowing-plugin` native plugin (cdylib) |
