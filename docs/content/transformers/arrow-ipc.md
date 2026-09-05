+++
title = "arrow-ipc"
description = "Arrow's own stream format on both surfaces: one message per batch, and one stream per file."
template = "subpage.html"
weight = 5

[[extra.facts]]
label = "Crate"
value = "<code>pcs-transformer-arrow-ipc</code>"
[[extra.facts]]
label = "Feature"
value = "<code>transformer-arrow-ipc</code>"
[[extra.facts]]
label = "Surfaces"
value = "Stream reader and writer, plus a message codec; message shape <code>PerBatch</code>"
[[extra.facts]]
label = "Declared schema"
value = "Required on a message node, refused on a stream read"
+++

## What it does

`ArrowIpcTransformer` moves Arrow's own stream format, over a message transport and as whole
files. There is no decoding cost worth naming: the payload already holds Arrow arrays.

`connector-tcp` pulls in `transformer-arrow-ipc`, so that feature alone is runnable over framed
Arrow. Every other deployment turns the feature on itself: `connector-kafka` and `connector-nats`
pull in `transformer-ndjson`, and `connector-file`, `connector-http` and `connector-s3` pull in
no format at all.

## options

None. The factory takes the options table and reads no key from it.

## Schema

On a message node the declared schema is required, and enforced. Every batch inside a payload is
checked against it, and a mismatch is
`arrow-ipc: received batch with schema {:?}, expected {:?}`.

The requirement is type-level rather than a check in this crate, so a missing declaration is a
connector error: `tcp config requires a 'schema_fields' list`.

On a stream read the schema comes from the stream, so a declared one is refused with
`arrow-ipc: the stream carries its own schema; remove schema_fields`. A `FileSource` therefore
declares none, and an `HttpSource` or `S3Source` keeps `schema_fields` for the link check and
reads with `schema_from "body"` or `schema_from "object"`.

## Surfaces

Both, on one encapsulation: the Arrow stream format, a schema message then one message per batch
then an end-of-stream marker. `open_writer` writes the schema message at open, so a run that
writes no batch still leaves a readable, zero-row stream, and `open_reader` reads it at open, so a
handle that is not an IPC stream is refused before a batch is parsed. No row count is available
without reading, so `estimated_rows` is always absent.

The bytes are interchangeable across the two surfaces: the stream a `FileSink` wrote decodes
through the message decoder, and one Kafka payload opens as a reader.

## One payload, one stream

`encode_messages` writes exactly one IPC stream per call, a schema header and one batch, and returns
a single payload.

Decoding reads the stream header eagerly, so a payload that is not an IPC stream fails at
`arrow-ipc: stream header: {e}` rather than part way through. `flush` concatenates everything banked
since the last flush into one batch.

Because the shape is `PerBatch`, a Kafka `key_field` is refused: there is no single row to key.

## Example

<div class="code">
<div class="code-cap"><span>KDL</span><em>a tcp source straight to a Kafka sink, both on one declared transformer</em></div>

```kdl
run_mode kind="stream"

workflow "ticks" {
    transformer "ipc" format="arrow-ipc"

    source "ticks_in" type="tcp" component="Tick" transformer="ipc" {
        config {
            bind "0.0.0.0:9500"

            schema_fields "price" type="Float64" nullable=#false
        }
    }

    sink "ticks_out" type="KafkaSink" component="Tick" transformer="ipc" {
        config {
            brokers "localhost:9092"
            topic "ticks-enriched"

            schema_fields "price" type="float64" nullable=#false
        }
    }

    link from="ticks_in" to="ticks_out"
}
```

</div>

<div class="code">
<div class="code-cap"><span>KDL</span><em>a file source and sink on the stream surface: the source reads its schema out of the file</em></div>

```kdl
workflow "ticks_files" {
    transformer "ipc" format="arrow-ipc"

    source "ticks_in" type="FileSource" component="Tick" transformer="ipc" {
        config path="/data/ticks.arrows"
    }

    sink "ticks_out" type="FileSink" component="Tick" transformer="ipc" {
        config path="/data/ticks-enriched.arrows" {
            truncate #true

            schema_fields "price" type="Float64" nullable=#false
        }
    }

    link from="ticks_in" to="ticks_out"
}
```

</div>

`truncate #true` is what makes the sink's output readable: a stream ends with an end-of-stream
marker, so a second run appended to the same file does not read back.
