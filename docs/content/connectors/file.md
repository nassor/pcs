+++
title = "File"
description = "One transport for every local file. The byte format is a declared transformer, named by the required transformer key."
template = "subpage.html"
weight = 2

[[extra.facts]]
label = "Crate"
value = "<code>pcs-connector-file</code>"
[[extra.facts]]
label = "Feature"
value = "<code>connector-file</code>, plus a <code>transformer-*</code> feature for the format"
[[extra.facts]]
label = "Config type"
value = "<code>FileSource</code>, <code>FileSink</code>"
[[extra.facts]]
label = "Transformer key"
value = "Required, and the format is never inferred from the extension"
+++

## What it does

One transport for every local file. The byte format comes from the transformer the node's required
`transformer` key names, so this crate reads no byte's meaning.

Reading runs on a dedicated OS thread feeding a bounded channel of four batches. The file is never
materialised in full and the executor never blocks on the disk.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>open_async moves the open and the metadata read off the executor; create appends, create_truncating replaces</em></div>

```rust
use pcs_connector_file::{FileSink, FileSource};

FileSource::open           (&Path, Arc<dyn Transformer>, Option<Arc<Schema>>) -> Result<Self>
FileSource::open_async     (&Path, Arc<dyn Transformer>, Option<Arc<Schema>>) -> Result<Self>
FileSink::create           (&Path, Arc<dyn Transformer>, Arc<Schema>)         -> Result<Self>
FileSink::create_truncating(&Path, Arc<dyn Transformer>, Arc<Schema>)         -> Result<Self>
```

</div>

`FileSource::estimated_rows` forwards what the reader reported at open time, so it is `Some` only
for a format that counts rows without reading them.

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>the transformer node and the source and sink that name it, all inside one workflow</em></div>

```kdl
transformer "orders_csv" format="csv" {
    options has_headers=#true
}

source "orders_in" type="FileSource" component="Order" transformer="orders_csv" {
    config path="/data/orders.csv" {
        schema_fields "id" type="Int64" nullable=#false
    }
}

sink "orders_out" type="FileSink" component="Order" transformer="orders_csv" {
    config path="/data/orders-out.csv" {
        schema_fields "id" type="Int64" nullable=#false
    }
}
```

</div>

## Config keys

| Key | Type | Required |
|---|---|---|
| `path` | string | yes, on both halves |
| `schema_fields` | list of fields | optional on a source, required on a sink |
| `truncate` | bool | optional on a sink, default `#false` |

`truncate #true` makes the sink replace the file instead of appending to it. The byte format is not
a `config` key: `transformer` is a property of the `source` or `sink` node itself, and the host
resolves it before the factory runs. Both factories hand-parse `ConfigValue` with no
`deny_unknown_fields`, so an unrecognised key is ignored rather than rejected.

## Schema and format

A source's `schema_fields` is what the format decides on:
[csv](@/transformers/csv.md) requires it, [ndjson](@/transformers/ndjson.md) infers when it is
absent, and [parquet](@/transformers/parquet.md), [avro](@/transformers/avro.md) and
[arrow-ipc](@/transformers/arrow-ipc.md) refuse it because the file carries its own. A `FileSink`
requires it whatever the transformer, because that is the schema the rows are written with.

`parquet` is the only format that reports `estimated_rows`, summed from row-group metadata without
reading any data.

## Sharp edge: the sink appends, and a terminated format cannot

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
<code>FileSink::create</code> runs during <code>build</code>, <code>pcs-service validate</code>
included, so the parent directory has to exist already. It creates the file when it is missing and
keeps every byte already in it, while <code>truncate #true</code> empties it at build instead.
<code>parquet</code>, <code>avro</code> and <code>arrow-ipc</code> each terminate their output
with a footer or an end-of-stream marker, so a second writer's output appended to an existing
file does not read back. Those configs want <code>truncate #true</code> or a path of their own;
<code>ndjson</code>, and <code>csv</code> without headers, append cleanly.
</p>
</div>

## Errors you can hit

| Message | Raised by |
|---|---|
| `FileSource config requires a 'path' string field` | the factory, before construction |
| `FileSource moves bytes and needs a 'transformer' key naming a declared transformer` | the shared context, when the node declared none |
| `FileSource: cannot open {path:?}: {e}` | opening the input |
| `FileSource: spawn_blocking panic: {e}` | open_async when the blocking task panics |
| `FileSink config requires a 'path' string field` | the factory, before construction |
| `FileSink: cannot create {path:?}: {e}` | creating the output under `truncate #true` |
| `FileSink: cannot open {path:?} for append: {e}` | opening the output in the default appending mode |
| `FileSink: cannot clone handle for {path:?}: {e}` | creating the output handle |
| `FileSink: write_batch called after finish` | writing to a finished sink |
| `FileSink: sync failed: {e}` | syncing the file after a batch, or at finish |

## Worked example

`cargo run -p pcs-connector-file --example scheduler_parquet_etl` runs a pipeline over Parquet.
`crates/pcs-connector-file/tests/round_trip.rs` takes csv, ndjson and parquet through this one
transport. `examples/configs/standalone.kdl` is a runnable service config over
`examples/configs/fixtures/orders.csv`:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Bash</span><em>validating examples/configs/standalone.kdl with connector-file and transformer-csv</em></div>

```bash
cargo run --features connector-file,transformer-csv,wasm --bin pcs-service -- \
    validate --config examples/configs/standalone.kdl
```

</div>
