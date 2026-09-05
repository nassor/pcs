+++
title = "Kafka"
description = "librdkafka in both directions, with every broker property reachable and topics created on first use."
template = "subpage.html"
weight = 4

[[extra.facts]]
label = "Crate"
value = "<code>pcs-connector-kafka</code>"
[[extra.facts]]
label = "Feature"
value = "<code>connector-kafka</code>, which implies <code>transformer-ndjson</code>"
[[extra.facts]]
label = "Config type"
value = "<code>KafkaSource</code>, <code>KafkaSink</code>"
[[extra.facts]]
label = "Transformer key"
value = "Required on both halves: neither resolves a format on its own"
+++

## What it does

The source consumes one or more topics with librdkafka's `StreamConsumer`; the sink produces with a
`FutureProducer`. Every librdkafka property is reachable through the `properties` node, and topics
are created on first use unless you opt out.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>both are synchronous and open no connection</em></div>

```rust
use pcs_connector_kafka::{
    KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig,
};

KafkaSource::new(KafkaSourceConfig, Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
KafkaSink::new  (KafkaSinkConfig,   Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
```

</div>

Each one validates the config, asks the transformer for its message shape, and builds a client.
librdkafka connects lazily, so `pcs-service validate` stays broker free.

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>named keys PCS interprets; everything else goes in the properties node</em></div>

```kdl
transformer "orders_json" format="ndjson"

source "orders_in" type="KafkaSource" component="Order" transformer="orders_json" {
    config {
        brokers "localhost:9092"
        topic "orders-raw"
        group_id "pcs-orders"
        stop_at_end #true

        provision create=#true partitions=3 replication_factor=1

        properties "session.timeout.ms"="45000"

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

## Source config keys

| Key | Default |
|---|---|
| `brokers` | required |
| `topic` | required, comma separated for several |
| `group_id` | `"pcs"` |
| `batch_size` | `1000` |
| `poll_timeout_ms` | `1000` |
| `auto_offset_reset` | `"earliest"`, one of `earliest`, `latest`, `none` |
| `commit_on_drain` | `true` |
| `stop_at_end` | `false` |
| `compacted` | `false`, one-shot materialized snapshot: latest value per key, tombstones remove keys, read from log start to a captured high watermark, then EOF; requires `key_field` |
| `key_field` | absent, column the raw message key is written to; compacted mode only |
| `provision` | see below |
| `properties` | empty |
| `schema_fields` | empty |

## Sink config keys

| Key | Default |
|---|---|
| `brokers` | required |
| `topic` | required, one topic |
| `key_field` | absent |
| `tombstones` | `false`, a row whose columns other than `key_field` are all null is produced with a NULL payload, a Kafka tombstone; requires `key_field` |
| `flush_timeout_ms` | `30000` |
| `provision` | see below |
| `properties` | empty |
| `schema_fields` | empty |

`transformer` is a property of the `source` or `sink` node, not a `config` key, and both halves
need one: this connector moves bytes and resolves no format itself. A node with none is
`KafkaSource moves bytes and needs a 'transformer' key naming a declared transformer`.

Both config structs are `#[serde(deny_unknown_fields)]`, so a misspelled key fails to parse and the
factory reports `KafkaSource config: {e}`.

## provision

| Key | Default |
|---|---|
| `create` | `true` |
| `partitions` | `1` |
| `replication_factor` | `1` |
| `config` | empty |
| `timeout_ms` | `10000` |

With `create=#false` no admin call is made at all, and a missing topic surfaces as a broker error
from the consumer or the producer instead. A topic that already exists counts as success, so
concurrent provisioning is safe.

## properties

The `properties` node is applied last, after this connector's own defaults, so it overrides any of
them and there is no second way to set the same thing. One key is refused:
`KafkaSource config: set the brokers with 'brokers', not properties.bootstrap.servers`.

## Stream mode, stop_at_end and compacted

<div class="note note-warn">
<span class="note-label">Stream mode required</span>
<p>
<code>KafkaSource</code> counts as a live source unless <code>stop_at_end=#true</code> or
<code>compacted=#true</code>, and config validation refuses a live source outside stream mode:
<code>source type 'KafkaSource' never reaches EOF; it requires standalone mode with run_mode
kind="stream"</code>. Stream mode allows any number of sources, pulled round-robin. Both opt-outs
are read off the raw config table, so each has to be a real boolean: <code>stop_at_end="true"</code>
in quotes is ignored and the source stays live.
</p>
</div>

With `stop_at_end=#true` a window closes as soon as every assigned partition has reported
end-of-partition, so a source that has read the whole topic hands its rows over at once instead of
waiting out `poll_timeout_ms`. The poll that reports EOF still costs one whole window: librdkafka
reports end-of-partition once per fetch position and not again while that position is unchanged, so
the drained source has nothing left to break on and ends on its deadline. Messages the consumer has
already taken from the broker are held on the source, so a caller that drops a `next_batch` future
— as the stream runner's one-second source prime does — gets them from the next call rather than
losing them.

## Delivery

At-least-once. The source commits the previous batch's offsets at the start of the next poll, so a
crash between the two replays that batch.

A topic that never appears is
`KafkaSource: poll failed: topic(s) {:?} were never visible to the consumer; if provision.create =
false, they may not exist`.

## Formats

Every registered format carries a message codec, so all five run on either half. A transformer
that declared none would be refused with
`KafkaSink: format '{format}' has no message codec`, and the source's twin.

`key_field` needs a format that emits one message per row, so
[arrow-ipc](@/transformers/arrow-ipc.md) and [parquet](@/transformers/parquet.md) are refused with
`KafkaSink config: 'key_field' needs a row-per-message format; '{format}' emits one message per
batch`. [csv](@/transformers/csv.md), [ndjson](@/transformers/ndjson.md) and
[avro](@/transformers/avro.md) all qualify.

## Transport

Plaintext only. The crate enables librdkafka's `cmake-build` and nothing else, no `ssl` and no
`sasl`, so a deployment needing either turns it on in its own build.

## Worked example

`examples/configs/kafka.kdl` is a commented config covering both halves:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Bash</span><em>validating examples/configs/kafka.kdl with the connector-kafka feature</em></div>

```bash
cargo run --features connector-kafka,wasm --bin pcs-service -- \
    validate --config examples/configs/kafka.kdl
```

</div>

`crates/pcs-connector-kafka/tests/kafka_roundtrip.rs` runs fourteen tests against a real broker in
a container, and prints `SKIP:` and passes when no Docker daemon is reachable.
