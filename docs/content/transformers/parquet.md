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
value = "All four; message shape <code>PerBatch</code>"
[[extra.facts]]
label = "Declared schema"
value = "Refused reading a file, required writing and on messages"
+++

## What it does

`ParquetTransformer` reads and writes Parquet through the `parquet` crate, as a file and as
discrete messages. Both are the same thing: the format describes itself, and its footer carries
every column, its type, and the row count per row group.

## options

None. The factory takes the options table and reads no key from it.

## Schema

Refused when reading: `parquet: the file carries its own schema; remove schema_fields`.

Required when writing, and baked into the writer at creation. That is a type-level requirement
rather than a check, so the message you see comes from the connector:
`FileSink config requires a 'schema_fields' list`.

On the message surface the declared schema is required and is not handed to the reader: a payload
carries its own. It is the expectation each payload is checked against, field for field, so a
producer writing other columns is `parquet: received batch with schema ..., expected ...` rather
than a silent append.

## Surfaces

All four: stream read, stream write, message decode and message encode, with shape `PerBatch`.

A Parquet file ends in a footer, so a payload has to be a whole file and a batch is the smallest
unit the format has. That is not free: the magic bytes, the per-column page headers and the Thrift
footer come to about 470 bytes for a three-column row type before a single row is written, paid
once per payload. A message transport carrying this format wants batches, not rows.

A payload is read through `bytes::Bytes`, because the footer at the end of a file means the reader
needs random access inside it and `parquet` implements that for a file handle and for `Bytes`.
A payload that is not a Parquet file at all is `parquet: file header: ...`.

One window is one batch: every payload's row groups are concatenated on flush.

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
