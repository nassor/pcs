+++
title = "csv"
description = "Text with no types of its own, so reading needs a declared schema."
template = "subpage.html"
weight = 1

[[extra.facts]]
label = "Crate"
value = "<code>pcs-transformer-csv</code>"
[[extra.facts]]
label = "Feature"
value = "<code>transformer-csv</code>"
[[extra.facts]]
label = "Surfaces"
value = "Stream read and write; no message codec"
[[extra.facts]]
label = "Declared schema"
value = "Required when reading"
+++

## What it does

`CsvTransformer` reads and writes comma separated text through `arrow-csv`. The format carries
no types, so the declared schema is what governs them. It has no message codec, which makes it
a file format only.

## options

| Key | Type | Default |
|---|---|---|
| `has_headers` | bool | `true` |

A non-bool is `csv: option 'has_headers' must be a boolean`. The factory reads that one key and
ignores anything else in the table.

## Schema

Reading requires a declared schema. Without one the build fails with
`csv: reading needs a declared schema; add schema_fields`.

Writing takes the columns of each batch it is handed. The writer stores no schema at all, so
`schema_fields` on a `FileSink` is the connector's requirement, not this format's.

## Surfaces

Stream read and write. There is no message codec, so a Kafka, NATS or `tcp` source or sink naming
a `csv` transformer fails at build: `KafkaSink: format 'csv' has no message codec` from the
`message_shape` gate, and `format 'csv' does not support decoding discrete messages` from the
decoder `TcpIngestSource::new` opens. A `tcp` sink is the one that waits, because
`encode_messages` needs a batch: its first write fails with
`format 'csv' does not support encoding discrete messages`.

## Header rows

`has_headers` drives the header on both halves: the reader expects one, the writer emits one, so
a round trip is symmetric. `finish` flushes the last block explicitly rather than leaving it to
`Drop`, which would swallow the error.

## Example

<div class="code">
<div class="code-cap"><span>KDL</span><em>both nodes sit inside one workflow</em></div>

```kdl
transformer "csv_fmt" format="csv" {
    options has_headers=#true
}

source "orders_in" type="FileSource" component="Order" transformer="csv_fmt" {
    config path="/data/orders.csv" {
        schema_fields "id" type="Int64" nullable=#false
        schema_fields "total" type="Float64" nullable=#false
    }
}
```

</div>
