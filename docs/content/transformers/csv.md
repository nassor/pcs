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
value = "All four; message shape <code>PerRow</code>"
[[extra.facts]]
label = "Declared schema"
value = "Required reading a file and on messages"
+++

## What it does

`CsvTransformer` reads and writes comma separated text through `arrow-csv`, as a stream and as
discrete messages. The format carries no types, so the declared schema is what governs them.

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

On the message surface the declared schema builds the decoder directly, and it is the only thing
that names the columns: a payload carries no header row.

## Surfaces

All four: stream read, stream write, message decode and message encode, with shape `PerRow`.

Encoding emits one payload per row, no header line and no record terminator. Each row is written
through its own writer rather than splitting one encoding of the batch on newlines, because a
quoted field may carry a newline of its own.

Decoding feeds each payload plus a newline into the streaming decoder, so a producer that left a
terminator on its payload and one that did not both decode to one row. An empty payload is
`csv: empty payload`. One window is one batch however many payloads it holds.

## Header rows

`has_headers` governs the stream surface alone: the reader expects a header row, the writer emits
one, so a stream round trip is symmetric. `finish` flushes the last block explicitly rather than
leaving it to `Drop`, which would swallow the error.

The message surface has no header row in either direction, whatever the option says. A payload is
one record, which leaves no line to spare, and the decoder is handed the declared schema. A stream
option that changed message framing would leave a topic readable only by a consumer that guessed
the producer's setting.

## Types

Text with a declared schema round-trips every Arrow type the schema names, unsigned integers
included: the declared type parses the field whatever the digits look like. The one value CSV
cannot carry is an empty string, which it writes as nothing and reads back as a null.

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
