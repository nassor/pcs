+++
title = "arrow-ipc"
description = "Arrow's own stream format, one message per batch, for TCP and Kafka only."
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
value = "Message codec only; message shape <code>PerBatch</code>"
[[extra.facts]]
label = "Declared schema"
value = "Required, and enforced per batch"
+++

## What it does

`ArrowIpcTransformer` moves Arrow's own stream format over a message transport. There is no
decoding cost worth naming: the payload already holds Arrow arrays.

`connector-tcp` pulls in `transformer-arrow-ipc`, so that feature alone is runnable over framed
Arrow. A Kafka deployment that wants this format turns the feature on itself, because
`connector-kafka` pulls in `transformer-ndjson` instead.

## options

None. The factory takes the options table and reads no key from it.

## Schema

Required, and enforced. Every batch inside a payload is checked against the declared schema, and a
mismatch is `arrow-ipc: received batch with schema {:?}, expected {:?}`.

The requirement is type-level rather than a check in this crate, so a missing declaration is a
connector error: `tcp config requires a 'schema_fields' list`.

## Surfaces

Message codec only, with shape `PerBatch`. There is no stream read or write, so this is not a file
format: a `FileSource` naming it fails with
`format 'arrow-ipc' does not support reading a byte stream`.

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
