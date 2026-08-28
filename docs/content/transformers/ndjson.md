+++
title = "ndjson"
description = "One JSON object per line, on files and on messages alike, and the only format that infers a schema."
template = "subpage.html"
weight = 2

[[extra.facts]]
label = "Crate"
value = "<code>pcs-transformer-ndjson</code>"
[[extra.facts]]
label = "Feature"
value = "<code>transformer-ndjson</code>"
[[extra.facts]]
label = "Surfaces"
value = "All four; message shape <code>PerRow</code>"
[[extra.facts]]
label = "Declared schema"
value = "Optional when reading a file, required on messages"
+++

## What it does

`NdjsonTransformer` reads and writes newline delimited JSON through `arrow-json`, on files and on
messages alike. It is the only format that can work out its own schema, and the only one
implementing all four capability methods.

## options

| Key | Type | Default |
|---|---|---|
| `infer_max` | integer | `1024` |

`infer_max` caps how many records schema inference reads. A non-integer is
`ndjson: option 'infer_max' must be an integer`, and zero is
`ndjson: option 'infer_max' must be at least 1`.

## Schema

Optional when reading a file. Absent, the schema is inferred from the first `infer_max` records,
and the inference seeks back to the start itself, so the reader below still sees every record.

On the message surface the declared schema builds the decoder directly. There is no inference
path there, because a decoder is created once and then fed payloads one at a time.

## Surfaces

All four: stream read, stream write, message decode and message encode, with shape `PerRow`.
`connector-kafka` and `connector-nats` each pull in `transformer-ndjson`, so either feature alone
is runnable against a topic of JSON records.

## Messages

Encoding emits one payload per row and carries no line terminator of its own: the writer's
newlines are stripped back out before the payloads are returned.

Decoding feeds each payload plus a newline into the streaming decoder and loops, because a
decode reports how many bytes it consumed and that may be fewer than it was handed.

## Example

<div class="code">
<div class="code-cap"><span>KDL</span><em>every byte-carrying node names its transformer; there is no default</em></div>

```kdl
transformer "orders_json" format="ndjson"

source "orders_in" type="KafkaSource" component="Order" transformer="orders_json" {
    config {
        brokers "localhost:9092"
        topic "orders-raw"
        group_id "pcs-orders"

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

<div class="code">
<div class="code-cap"><span>KDL</span><em>a file source with no schema, inferring from the first 4096 records</em></div>

```kdl
transformer "orders_json" format="ndjson" {
    options infer_max=4096
}

source "orders_in" type="FileSource" component="Order" transformer="orders_json" {
    config path="/data/orders.ndjson"
}
```

</div>
