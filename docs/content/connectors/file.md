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
<div class="code-cap"><span>Rust</span><em>open_async is the async twin: it moves the open and the metadata read off the executor</em></div>

```rust
use pcs_connector_file::{FileSink, FileSource};

FileSource::open      (&Path, Arc<dyn Transformer>, Option<Arc<Schema>>) -> Result<Self>
FileSource::open_async(&Path, Arc<dyn Transformer>, Option<Arc<Schema>>) -> Result<Self>
FileSink::create      (&Path, Arc<dyn Transformer>, Arc<Schema>)         -> Result<Self>
```

</div>

`FileSource::estimated_rows` forwards what the reader reported at open time, so it is `Some` only
for a format that counts rows without reading them.

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>the transformer node and the source that names it, both inside one workflow</em></div>

```kdl
transformer "orders_csv" format="csv" {
    options has_headers=#true
}

source "orders_in" type="FileSource" component="Order" transformer="orders_csv" {
    config path="/data/orders.csv" {
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

The byte format is not a `config` key: `transformer` is a property of the `source` or `sink` node
itself, and the host resolves it before the factory runs. Both factories hand-parse `ConfigValue`
with no `deny_unknown_fields`, so an unrecognised key is ignored rather than rejected.

## Schema and format

A source's `schema_fields` is what the format decides on:
[csv](@/transformers/csv.md) requires it, [ndjson](@/transformers/ndjson.md) infers when it is
absent, and [parquet](@/transformers/parquet.md) and [avro](@/transformers/avro.md) refuse it
because the file carries its own. A `FileSink` requires it whatever the transformer, because that
is the schema the rows are written with.

`parquet` is the only format that reports `estimated_rows`, summed from row-group metadata without
reading any data.

## Sharp edge: the sink opens its file when the factory runs

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
<code>FileSink::create</code> runs during <code>build</code>, and it creates and truncates the
output path. That happens under <code>pcs-service validate</code> too, so validating a config
empties the file it names and the parent directory has to exist already.
<code>examples/configs/standalone.kdl</code> says so where it declares its
sink.
</p>
</div>

## Errors you can hit

| Message | Raised by |
|---|---|
| `FileSource config requires a 'path' string field` | the factory, before construction |
| `FileSource moves bytes and needs a 'transformer' key naming a declared transformer` | the shared context, when the node declared none |
| `FileSource: cannot open {path:?}: {e}` | opening the input |
| `FileSink: cannot create {path:?}: {e}` | creating the output |
| `FileSink: write_batch called after finish` | writing to a finished sink |

## Worked example

`cargo run -p pcs-connector-file --example scheduler_parquet_etl` runs a pipeline over Parquet.
`crates/pcs-connector-file/tests/round_trip.rs` takes csv, ndjson and parquet through this one
transport. `examples/configs/standalone.kdl` is a runnable service config over
`examples/configs/fixtures/orders.csv`:

<div class="code">
<div class="code-cap"><span>Bash</span><em>validating examples/configs/standalone.kdl with connector-file and transformer-csv</em></div>

```bash
cargo run --features connector-file,transformer-csv,wasm --bin pcs-service -- \
    validate --config examples/configs/standalone.kdl
```

</div>
