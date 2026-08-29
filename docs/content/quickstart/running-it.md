+++
title = "Running it!"
description = "Build a Rust WebAssembly processor, run it through pcs-service with a minimal file-based config, and read the result. About 15 minutes, no Docker needed."
template = "page.html"
weight = 2
+++

# Running it!

<dl class="page-facts">
<dt>In one line</dt>
<dd>CSV in, one <strong>WebAssembly processor</strong>, CSV out, and a live <code>/health</code> endpoint</dd>
<dt>You need</dt>
<dd>Everything from the Installation page, and a checkout of the repository</dd>
<dt>Read this if</dt>
<dd>You want to see a real workflow move rows before reading any reference page</dd>
</dl>

Install first if you have not: [Installation](@/quickstart/installation.md).

You build one processor component, the Rust port of the
`scheduler_etl` example, and run it under `pcs-service` with a config you
write yourself. A CSV file of five transactions goes in, the processor
validates each row and converts it to USD, and a CSV file comes out. The
whole route needs no Docker, no Go and no .NET; those join in the
[Docker Compose quickstart](@/quickstart/running-it.md#next-the-docker-compose-quickstart)
at the end.

## Step 1: build the processor component

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
cargo build --release -p order-processing-wasm --target wasm32-wasip2
```

`rustc` links a `wasm32-wasip2` cdylib into a Component Model component
itself, so plain `cargo build` is the whole toolchain. The finished component
lands at `target/wasm32-wasip2/release/order_processing_wasm.wasm`.

Check that it exports the PCS world:

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
wasm-tools validate --features component-model target/wasm32-wasip2/release/order_processing_wasm.wasm
```

Prints nothing and exits 0. The component exports two functions,
`describe()` and `run-batch`, which is the whole
[WIT contract](@/processors/wit-contract.md).

## Step 2: write a minimal config

Create `my-first-pcs.kdl` in the repository root. Every key comes from
`examples/configs/standalone_wasm.kdl` and `examples/configs/standalone.kdl`,
which use the same shape:

```kdl,name=my-first-pcs.kdl
mode "standalone"

node id=1 name="first-pipeline" data_dir="${PCS_DATA_DIR:-/tmp/pcs-first}"

run_mode kind="interval" interval_ms=5000

workflow "orders" {
    transformer "csv_fmt" format="csv" {
        options has_headers=#true
    }

    source "csv_orders" type="FileSource" component="Transaction" transformer="csv_fmt" {
        config {
            path "examples/configs/fixtures/order_processing_input.csv"
            schema_fields "id" type="UInt64" nullable=#false
            schema_fields "amount" type="Float64" nullable=#false
            schema_fields "currency" type="Utf8" nullable=#false
            schema_fields "valid" type="Boolean" nullable=#false
            schema_fields "usd_amount" type="Float64" nullable=#false
        }
    }

    wasm "process_orders" module="target/wasm32-wasip2/release/order_processing_wasm.wasm" {
        config fx_eur="1.08" fx_gbp="1.27" fx_jpy="0.0067" fx_cad="0.74"
    }

    sink "csv_out" type="FileSink" component="Transaction" transformer="csv_fmt" {
        config {
            path "/tmp/pcs-first-out.csv"
            truncate #true
            schema_fields "id" type="UInt64" nullable=#false
            schema_fields "amount" type="Float64" nullable=#false
            schema_fields "currency" type="Utf8" nullable=#false
            schema_fields "valid" type="Boolean" nullable=#false
            schema_fields "usd_amount" type="Float64" nullable=#false
        }
    }

    link from="csv_orders" to="process_orders"
    link from="process_orders" to="csv_out"
}

http bind="127.0.0.1:8080"

observability log_format="pretty" log_level="info"
```

What each part does:

- `mode "standalone"` runs one process with no distributed coordination.
- `run_mode kind="interval" interval_ms=5000` re-runs the workflow every five
  seconds, so the service stays alive between iterations. `kind="one_shot"`
  would exit after the first iteration instead.
- One `workflow` node declares the whole graph. The `transformer "csv_fmt"`
  node names the byte format; both the source and the sink reference it by id.
- The `source` is a `FileSource` reading `order_processing_input.csv`, decoded
  into the five fields of the `Transaction` component.
- The `wasm` node names the component you built. The `config` block holds the
  FX rates the processor reads through the `host-io` `get-config` import.
- The `sink` is a `FileSink`; `truncate #true` replaces the output file on
  every start instead of appending to it.
- Each `link` is one edge of the graph: source to processor, processor to sink.
- `http bind="127.0.0.1:8080"` turns on the control plane, and
  `observability` sets the log format and level.

The paths are relative to the directory you run from, so run everything from
the repository root. The output directory must exist before `FileSink` opens
the file. On Linux and macOS `/tmp` always does; on Windows the path
`/tmp/pcs-first-out.csv` resolves to `C:\tmp\pcs-first-out.csv` on the current
drive, so create it first:

Linux/macOS: nothing to do.

Windows (PowerShell):

```powershell
New-Item -ItemType Directory -Force C:\tmp
```

## Step 3: validate the config

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
pcs-service validate -c my-first-pcs.kdl
```

`validate` reads the config, compiles the component, calls `describe()`, and
walks every `link` checking that both ends agree on the schema. It prints
`OK: workflow graph validated (components and schemas agree end to end)` and
exits 0. A processor that reports a different component list or schema
fingerprint fails here rather than at the first batch.

## Step 4: run it

```bash,name=Runs the same on Linux, macOS and Windows (PowerShell)
pcs-service serve -c my-first-pcs.kdl
```

The service starts, drains the CSV into the dataset, hands the batch to the
processor, and writes the result. Five seconds later it drains again, finds
the file source at EOF, and idles. Leave it running.

## Step 5: observe the service

In a second terminal, ask the control plane:

Linux/macOS:

```bash
curl http://127.0.0.1:8080/health
```

Windows (PowerShell):

```powershell
Invoke-RestMethod http://127.0.0.1:8080/health
```

Expected output, a JSON document with a live `liveness_counter` that ticks up
once per second:

```json
{"status":"alive","uptime_seconds":7,"liveness_counter":7}
```

`/status` carries the workflow counters:

```json
{"node_id":1,"node_name":"first-pipeline","mode":"standalone","uptime_seconds":7,"build":{"version":"0.1.0"},"cluster":null,"standalone":[{"workflow_id":"orders","iterations":1,"rows_processed":5,"source_batches_drained":1,"sink_batches_written":1,"iteration_errors":0,"total_busy_micros":0,"max_item_micros":0}]}
```

`/ready` returns `{"status":"ready"}`, and `/metrics` serves the Prometheus
exposition. All four endpoints are described under
[Service](@/service/_index.md).

## Step 6: read the result

The sink wrote `pcs-first-out.csv` (on Windows, `C:\tmp\pcs-first-out.csv`).
Open it. The input fixture has five rows:

```text
id,amount,currency,valid,usd_amount
1,120.0,EUR,false,0.0
2,4300.0,USD,false,0.0
3,75.5,GBP,false,0.0
4,-50.0,EUR,false,0.0
5,900000.0,JPY,false,0.0
```

In the output, the processor has filled both empty columns: `valid` is `true`
where the amount is positive, and `usd_amount` is the amount converted at the
configured rates (EUR 1.08, GBP 1.27, JPY 0.0067, USD 1.0). Row 4 has a
negative amount, so its `valid` stays `false`.

Stop the service with Ctrl-C. `serve` shuts down between iterations, so an
interrupt never lands mid-batch.

## Next: the Docker Compose quickstart

The route above moves a static file. The full Quick Start in
[`examples/quickstart/`](https://github.com/nassor/pcs/tree/main/examples/quickstart)
moves live data: a publisher writes card authorisations to a NATS subject, one
`pcs-service` process runs two linked WebAssembly processors in two languages
against the same in-memory dataset, and upserts the result into PostgreSQL,
with a live dashboard on port 8080.

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
            upserts all twelve columns into the PostgreSQL table
            public.settlements and serves one dashboard on port 8080.
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

It needs Docker (NATS 2.11 and PostgreSQL 18), the Go and C# toolchains from
the Installation page, and about ten minutes. The commands, the `quickstart.kdl`
config, and the expected `review_tier` breakdown are all in
[`examples/quickstart/README.md`](https://github.com/nassor/pcs/blob/main/examples/quickstart/README.md).
