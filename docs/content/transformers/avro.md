+++
title = "avro"
description = "Container files on the stream surface, single-object or Confluent framing on the message surface."
template = "subpage.html"
weight = 4

[[extra.facts]]
label = "Crate"
value = "<code>pcs-transformer-avro</code>"
[[extra.facts]]
label = "Feature"
value = "<code>transformer-avro</code>"
[[extra.facts]]
label = "Surfaces"
value = "All four; message shape <code>PerRow</code>"
[[extra.facts]]
label = "Declared schema"
value = "Refused reading a file, required writing and on messages"
+++

## What it does

`AvroTransformer` reads and writes Avro object container files on the stream surface, and framed
single records on the message surface. The two surfaces disagree about the schema, because a
container file carries its own header and a message does not.

## options

| Key | Type | Default |
|---|---|---|
| `compression` | string | `null`, one of `null`, `deflate`, `snappy`, `zstd` |
| `schema_id` | integer | absent, and it must fit in a `u32` |

`compression` applies to the container writer only. `schema_id` is a Confluent registry id, and it
turns on the Confluent framing in both directions.

The four rejections are `avro: option 'compression' must be a string`,
`avro: option 'compression' must be one of null, deflate, snappy, zstd`,
`avro: option 'schema_id' must be an integer` and
`avro: option 'schema_id' must fit in a u32`.

## Schema

Refused when reading a container file, whose header carries it:
`avro: the file carries its own schema; remove schema_fields`.

Required for writing a file, and required on the message surface, where it does two jobs. It is
converted to the Avro form of the schema, and a schema with no Avro form is
`avro: the declared schema has no Avro form: {e}`. It is also the cast target: a decoded batch is
cast to the declared columns with `safe = false`, because Avro has no `int8` of its own and narrow
integers travel as `int`. A value that does not fit is
`avro: casting to the declared schema: {e}`.

## Surfaces

All four: stream read, stream write, message decode and message encode, with shape `PerRow`.

## Framing

Single-object encoding, `0xC3 0x01` followed by an 8-byte Rabin fingerprint, is always accepted.
The Confluent prefix, `0x00` followed by a 4-byte big-endian registry id, is accepted only when
`schema_id` is set. Otherwise the payload is
`avro: payload carries the Confluent prefix; set option 'schema_id' to its registry id`, and
a payload with neither prefix is
`avro: payload is not framed; expected single-object encoding (0xC3 0x01) or the Confluent prefix
(0x00)`.

A decoder binds one fingerprint algorithm for its life, so there is one decoder per framing. A
window mixing both still decodes in arrival order.

`schema_id` also selects the framing on the way out, so setting it makes the encoder emit Confluent
framed payloads.

## Example

<div class="code">
<div class="code-cap"><span>KDL</span><em>one format, two declared instances: a compressed container file and a Confluent framed topic</em></div>

```kdl
transformer "avro_file" format="avro" {
    options compression="zstd"
}

transformer "avro_topic" format="avro" {
    options schema_id=42
}

source "orders_in" type="FileSource" component="Order" transformer="avro_file" {
    config path="/data/orders.avro"
}

source "orders_stream" type="KafkaSource" component="Order" transformer="avro_topic" {
    config {
        brokers "localhost:9092"
        topic "orders-raw"

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>
