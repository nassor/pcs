+++
title = "parquet"
description = "Self describing columnar files: the footer carries the schema and the row counts."
template = "subpage.html"
weight = 3

[[extra.facts]]
label = "Crate"
value = "<code>pcs-transformer-parquet</code>"
[[extra.facts]]
label = "Feature"
value = "<code>transformer-parquet</code>"
[[extra.facts]]
label = "Surfaces"
value = "Stream read and write; no message codec"
[[extra.facts]]
label = "Declared schema"
value = "Refused when reading"
+++

## What it does

`ParquetTransformer` reads and writes Parquet through the `parquet` crate. The file describes
itself: the footer carries every column, its type, and the row count per row group.

## options

None. The factory takes the options table and reads no key from it.

## Schema

Refused when reading: `parquet: the file carries its own schema; remove schema_fields`.

Required when writing, and baked into the writer at creation. That is a type-level requirement
rather than a check, so the message you see comes from the connector:
`FileSink config requires a 'schema_fields' list`.

## Surfaces

Stream read and write. There is no message codec, so a Kafka, NATS or `tcp` source or sink naming
a `parquet` transformer fails at build: `KafkaSink: format 'parquet' has no message codec` from
the `message_shape` gate, and `format 'parquet' does not support decoding discrete messages` from
the decoder `TcpIngestSource::new` opens. A `tcp` sink is the one that waits, because
`encode_messages` needs a batch: its first write fails with
`format 'parquet' does not support encoding discrete messages`.

## estimated_rows

The only format that reports it. The row counts are summed from row-group metadata that the reader
builder already holds, so no data page is read. A file with no rows reports nothing rather than
zero.

## Compression

Snappy, fixed. It is not a knob.

## Example

<div class="code">
<div class="code-cap"><span>KDL</span><em>one declared transformer serves both ends</em></div>

```kdl
transformer "pq" format="parquet"

source "orders_in" type="FileSource" component="Order" transformer="pq" {
    config path="/data/orders.parquet"
}

sink "orders_out" type="FileSink" component="EnrichedOrder" transformer="pq" {
    config path="/data/enriched.parquet" {
        schema_fields "id" type="Int64" nullable=#false
        schema_fields "revenue" type="Float64" nullable=#false
    }
}
```

</div>
