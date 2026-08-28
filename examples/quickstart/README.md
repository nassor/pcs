# Quick Start: NATS to PostgreSQL through two processors

One `pcs-service` instance, one config file, one pipeline running two
WebAssembly stages written in two languages against one Arrow schema. The
narrative lives in the documentation (`docs/content/quickstart/running-it.md`);
this file is the commands.

```
publisher --NDJSON--> authorizations.raw
                         |
                         v  one pcs-service, one pipeline (quickstart.kdl)
                      [ validate-go.wasm ] --> [ settle-cs.wasm ]
                        Go: writes valid        C#: writes fee, review_tier
                         |
                         v
                      public.settlements
```

The `Order` schema also carries `usd_amount_display`, a column the Python
stage in the full polyglot pipeline derives. This Quick Start has no Python
stage, so the publisher writes it empty and it stays empty end to end.

## Prerequisites

`componentize-go` 0.4.1 with Go 1.25.5+, .NET SDK 10, and `wasm-tools` 1.246.2,
all pinned in `examples/polyglot/PINS.md`. Docker for NATS and PostgreSQL, or
your own instances of both.

## Run it

```bash
# 1. Build both processor components.
cargo xtask quickstart

# 2. Start NATS and PostgreSQL. schema.sql creates the destination table on
#    first initialisation; PostgresSink never issues CREATE TABLE.
docker compose -f examples/quickstart/docker-compose.yml up -d

# 3. Start the service. No --features flags: the default bundle covers NATS,
#    PostgreSQL, ndjson and the wasm runtime.
cargo run -p pcs-service -- serve -c examples/quickstart/quickstart.kdl

# 4. Publish 5000 authorisations at 500/s. `--count 0` instead runs until
#    Ctrl-C, for watching the dashboard against a live stream.
cargo run -p pcs-service --example quickstart_publish --features connector-nats -- \
  --count 5000 --rate 500

# 5. Watch the dashboard while it runs. The Pipelines tab draws both stages,
#    chained source to Go stage to C# stage to sink.
#    http://127.0.0.1:8080/ui

# 6. Read the result.
docker compose -f examples/quickstart/docker-compose.yml exec -T postgres \
  psql -U postgres -d pcs -c \
  'select review_tier, count(*), round(sum(fee)::numeric, 2) as fees
     from settlements group by review_tier order by review_tier;'
```

`review_tier` is the settlement decision the C# stage writes: `0` settled, `1`
held for review (amount above `hold_above`), `2` rejected (the Go stage found the
amount below `min_amount`).

## Tear down

```bash
docker compose -f examples/quickstart/docker-compose.yml down -v
```

## What this does and does not promise

The service runs under the streaming standalone runner, which is
**at-most-once**: it acknowledges nothing and keeps no checkpoints, so a service
killed mid-batch loses that batch. At-least-once is `DistributedRunner`'s
territory, covered under *Running at scale*.

## Files

| File | What it is |
|---|---|
| `quickstart.kdl` | the one service: NATS in, Go stage, C# stage, PostgreSQL out |
| `docker-compose.yml` | NATS 2.11 and PostgreSQL 18, host ports 4222 and 5432 |
| `schema.sql` | the `settlements` table, mounted into the Postgres container |
| `stages/csharp-settle/` | the C# processor, written for this tutorial |
| `build/` | the two components `cargo xtask quickstart` produces |

The Go processor is `examples/polyglot/stages/go-validate`, reused unmodified: a
built component does not care which pipeline loads it, only that the pipeline's
declared components match what its `describe()` reports.
