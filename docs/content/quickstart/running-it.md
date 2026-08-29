+++
title = "Running it!"
description = "A live NATS stream through a Go processor and a C# processor into PostgreSQL: one pcs-service process, one config file, two linked WebAssembly processors in one workflow, one Arrow schema, one dashboard."
template = "page.html"
weight = 2
+++

# Running it!

<dl class="page-facts">
<dt>In one line</dt>
<dd>NATS in, two WebAssembly processors in two languages, <strong>PostgreSQL out</strong></dd>
<dt>You need</dt>
<dd>Everything from the Installation page, and a checkout of the repository</dd>
<dt>Read this if</dt>
<dd>You want to watch a real workflow move rows before reading any reference page</dd>
</dl>

The stack lives in `examples/quickstart/`. A publisher writes card
authorisations as NDJSON onto a NATS subject. One service reads that subject,
validates each authorisation with a Go processor, prices and tiers it with a C#
processor in the same process, and upserts the result into PostgreSQL.

Install first if you have not: [Installation](@/quickstart/installation.md).

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 212" role="img" aria-labelledby="qs-title qs-desc">
        <title id="qs-title">The Quick Start stack: a publisher, one pcs-service running two processors, PostgreSQL</title>
        <desc id="qs-desc">
            A publisher sends NDJSON authorisations to the NATS subject
            authorizations.raw. One pcs-service process, configured by
            quickstart.kdl, reads that subject and runs two linked WebAssembly
            processors against the same in-memory dataset. The Go processor
            validate-go.wasm writes the valid column, then the C# processor
            settle-cs.wasm writes the fee and review_tier columns. The service
            upserts all eleven columns into the PostgreSQL table public.settlements
            and serves one dashboard on port 8080.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="56" width="136" height="68" rx="8"/>
            <rect class="hd hd-data" x="0" y="56" width="136" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="68" width="136" height="8"/>
            <text class="t-lbl" x="12" y="71">publisher</text>
            <text class="t-sm" x="12" y="92">NDJSON</text>
            <text class="t-sm" x="12" y="108">authorizations.raw</text>
        </g>
        <g class="anim anim-2">
            <path class="arw arw-data" d="M136 90 H160" marker-end="url(#qs-d)"/>
            <rect class="blk blk-bnd" x="166" y="36" width="360" height="112" rx="8"/>
            <rect class="hd hd-bnd" x="166" y="36" width="360" height="22" rx="8"/>
            <rect class="hd hd-bnd" x="166" y="50" width="360" height="8"/>
            <text class="t-lbl t-bnd" x="178" y="51">pcs-service &middot; quickstart.kdl</text>
        </g>
        <g class="anim anim-3">
            <rect class="blk blk-bnd" x="178" y="68" width="152" height="44" rx="6"/>
            <text class="t-lbl" x="188" y="86">validate-go.wasm</text>
            <text class="t-sm" x="188" y="102">writes valid</text>
            <path class="arw arw-data" d="M330 90 H342" marker-end="url(#qs-d)"/>
            <rect class="blk blk-bnd" x="346" y="68" width="168" height="44" rx="6"/>
            <text class="t-lbl" x="356" y="86">settle-cs.wasm</text>
            <text class="t-sm" x="356" y="102">writes fee, review_tier</text>
            <text class="t-sm t-ctl" x="178" y="134">/ui on :8080</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-data" d="M526 90 H556" marker-end="url(#qs-d)"/>
            <rect class="blk blk-data" x="562" y="64" width="98" height="52" rx="8"/>
            <text class="t-lbl" x="574" y="86">Postgres</text>
            <text class="t-sm" x="574" y="104">settlements</text>
            <path class="ln" d="M0 166 H654"/>
            <text class="t-sm" x="0" y="186">One process, one workflow. NATS carries the inbound NDJSON only.</text>
            <text class="t-sm" x="0" y="202">Between the two processors the dataset stays in memory, so nothing is re-encoded.</text>
        </g>
        <defs>
            <marker id="qs-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> data plane</span>
        <span class="k-boundary"><i></i> the WebAssembly boundary</span>
        <span class="k-control"><i></i> control plane</span>
    </div>
    <figcaption class="dgm-cap">
        Two <code>wasm</code> nodes joined by an explicit <code>link</code> run
        two processors against the same dataset, with nothing re-encoded between
        them. Both must declare the same components and report the same Arrow
        schema fingerprint.
    </figcaption>
</div>

## Build the two components

```bash,name=Build the two components
cargo xtask quickstart
```

The task regenerates the shared `Order` schema, builds the Go processor with
`componentize-go`, builds the C# processor with `dotnet build -c Release`, collects
both into `examples/quickstart/build/`, and validates each with `wasm-tools`
against the `pcs:pipeline/pipeline@0.3.0` export. It exits with a distinct code
per missing tool.

The Go processor is `examples/polyglot/stages/go-validate`, reused unmodified: it
reads `amount` and writes `valid = amount > min_amount`. The C# processor,
`examples/quickstart/stages/csharp-settle`, is written for this tutorial: it
reads `valid` and `amount` and writes `fee` and `review_tier`.

## Start NATS and PostgreSQL

```bash,name=Start NATS and PostgreSQL
docker compose -f examples/quickstart/docker-compose.yml up -d
```

`schema.sql` is mounted into the container's initialisation directory and runs
once, on first bring-up of an empty data directory. It has to: `PostgresSink`
never issues `CREATE TABLE` and matches columns by name against the live table.

## The service

`examples/quickstart/quickstart.kdl`, the parts that decide behaviour:

```kdl,name=The parts of quickstart.kdl that decide behaviour
mode "standalone"

node id=1 name="quickstart" data_dir="${PCS_DATA_DIR:-/tmp/pcs-quickstart}"

run_mode kind="stream"

workflow "quickstart" {
    transformer "ndjson_fmt" format="ndjson"

    source "authorizations-raw" type="NatsSource" component="Order" transformer="ndjson_fmt" {
        config {
            batch_size 500
            poll_timeout_ms 500

            connection {
                servers "${PCS_NATS_URL:-nats://localhost:4222}"
                name "quickstart"
            }

            mode kind="core" subject="authorizations.raw"

            // All eleven Order fields, in schema order.
            schema_fields "id" type="int64" nullable=#false
        }
    }

    wasm "validate" module="examples/quickstart/build/validate-go.wasm" {
        config min_amount="0.50"
    }

    wasm "settle" module="examples/quickstart/build/settle-cs.wasm" {
        config fee_bps="290" fee_fixed="0.30" hold_above="1000"
    }

    sink "settlements" type="PostgresSink" component="Order" {
        config {
            name "settlements"
            table "public.settlements"
            write_mode "upsert"
            conflict_columns "id"
            dedupe_order_column "id"

            connection {
                dsn "${PCS_PG_DSN:-postgres://postgres:pcs@127.0.0.1:5432/pcs}"
                application_name "pcs-quickstart"
                sslmode "disable"
            }

            // The same eleven fields.
            schema_fields "id" type="int64" nullable=#false
        }
    }

    link from="authorizations-raw" to="validate"
    link from="validate" to="settle"
    link from="settle" to="settlements"
}

http bind="127.0.0.1:8080"

observability log_level="info" {
    inspector enabled=#true ui=#true
}
```

`kind="stream"` is the runner. A core NATS subscription never reports EOF, so
the interval runner would spin on a source that is never done. The stream runner
drives a live source item by item, and requires exactly one declared source,
which this config has.

One `workflow` node declares the whole graph: the byte format, the source, both
processors, the sink, and one `link` per edge. `ServiceBuilder::build` assembles
one `BuiltNode` per declared node in topological order, so `validate` runs before
`settle`, both against the same in-memory `Dataset`. The Go processor writes
`valid`, then the C# processor reads it and writes `fee` and `review_tier`.
Nothing is re-encoded between them, and no transport sits between them either.

Both processors must declare the same components and report the same Arrow schema
fingerprint. `validate_workflow_graph` runs inside `build`, before it returns, and
rejects a link whose two ends disagree on either, naming the link.

Each `config` node belongs to the `wasm` node holding it, and reaches that
processor as strings through the WIT
`host-io::get-config` import, which the processor parses itself. `min_amount`
is the line below which an authorisation is a card-testing probe, not a
purchase. `fee_bps` and `fee_fixed` are the fee in basis points of the amount
plus a flat component, and a valid authorisation above `hold_above` is held for
manual review instead of settled.

Both the source and the sink declare all eleven `Order` fields in
`schema_fields`, non-nullable, in schema order. The full list is in the file. The
source needs them because a processor mutates the Arrow buffer in place and
cannot add columns, so every field either processor touches has to be present from
the first decode. `PostgresSink::check_schema` needs them because it requires the
incoming batch to match `schema_fields` field for field, in order: it projects
nothing, and a batch with a different column count is rejected rather than
partially written. `schema.sql` has the same eleven columns, and the four fields
neither processor writes, `usd_amount`, `risk_score`, `flagged` and `settlement`,
land as the zero values the publisher sent.

`write_mode "upsert"` on `id` makes a replayed batch idempotent, so re-running
the publisher does not duplicate rows. `dedupe_order_column` is required when one
batch can repeat a conflict key: without it PostgreSQL raises `ON CONFLICT DO
UPDATE command cannot affect row a second time`.

## Check the config, then start it

```bash,name=Check the config first
pcs-service validate -c examples/quickstart/quickstart.kdl
```

`validate` reads the config, compiles both components, calls each `describe()`
and walks every `link`. A stale `component`, or a processor that disagrees on the
schema fingerprint, fails here rather than at the first batch. No `--features` flag:
NATS, PostgreSQL, ndjson and the wasm runtime are all in the default bundle.

```bash,name=Start the service
pcs-service serve -c examples/quickstart/quickstart.kdl &
```

## Publish some authorisations

```bash,name=Publish five thousand authorisations
cargo run -p pcs-service --example quickstart_publish --features connector-nats -- \
  --count 5000 --rate 500
```

Five thousand authorisations at 500 a second. Amounts come from a seeded
three-bucket mixture spanning 0.01 to 5000.00, which straddles both processors'
thresholds, so every branch of both fires. `--url`, `--subject` and `--seed` are
the other flags; the seed is a fixed constant, so a re-run reproduces the same
rows and the same table contents.

`--count 0` publishes until Ctrl-C instead of stopping at a fixed total, for
watching the dashboard against a live stream rather than a completed run.

## Watch it

The service serves one dashboard: <http://127.0.0.1:8080/ui>

The Pipelines tab draws one box per declared node in four depth columns:
`authorizations-raw`, `validate`, `settle`, `settlements`. A box is titled by its
declared `name` when the config gives one and by its id otherwise, which is what
these four show, and its second line is the connector `type` or the runtime kind.
Every box carries its own live numbers, read from the series attributed to its id,
and clicking a processor box opens its version, its statefulness and the artifact
file it loaded. Edge dashes speed up as throughput rises. The Logs tab tails both
processors' `host-io::log` lines. The Traces tab shows one trace per item under
the stream runner, with a bar per `runtime.run` and one for `sink.write`; the wait
for input precedes the item, so no source drain bar appears.
[Live dashboard](@/service/dashboard.md) covers all three.

## Read the result

```bash,name=Read the settlements out of PostgreSQL
docker compose -f examples/quickstart/docker-compose.yml exec -T postgres \
  psql -U postgres -d pcs -c \
  'select review_tier, count(*), round(sum(fee)::numeric, 2) as fees
     from settlements group by review_tier order by review_tier;'
```

`review_tier` is the settlement decision:

| Value | Meaning |
|---|---|
| `0` | settled |
| `1` | held for manual review, amount above `hold_above` |
| `2` | rejected, the Go processor found the amount below `min_amount` |

## Why a tier code and not a settlement string

`Order` has a `settlement` text column, and the C# processor does not write it.
Both processors mutate the incoming Arrow IPC buffer in place and hand the same
bytes back, which is what a processor can do without shipping an Arrow writer.
In-place mutation can overwrite a fixed-width value and nothing else.
`settlement` is `Utf8`, a variable-width column whose offsets buffer would have
to move, and the `Pcs.Sdk` codec rejects that write rather than corrupting
the stream. `review_tier` is `Int64`, so it is writable, and the table stores the
tier.

## What this does and does not promise

The service runs under the streaming standalone runner, which is
**at-most-once**. It acknowledges nothing and keeps no checkpoints, so a service
killed mid-batch loses that batch. The state blob each processor returns is
handed back as the next item's `prior`. Neither config here declares a `store`
block, so that blob lives in loop memory and is gone on restart.

At-least-once is `DistributedRunner`'s territory, covered under
[Distributed Runner](@/distributed.md).

## Tear down

```bash,name=Tear the containers down
docker compose -f examples/quickstart/docker-compose.yml down -v
```
