+++
title = "Channel"
description = "An in-memory mpsc pair dressed as IO: the transport for tests and for bridging one workflow to another in the same process."
template = "subpage.html"
weight = 1

[[extra.facts]]
label = "Crate"
value = "<code>pcs-connector-channel</code>"
[[extra.facts]]
label = "Feature"
value = "<code>connector-channel</code>"
[[extra.facts]]
label = "Config type"
value = "<code>ChannelSource</code>, <code>ChannelSink</code>"
[[extra.facts]]
label = "Transformer key"
value = "None: the connector carries no byte format"
+++

## What it does

A `tokio` mpsc pair dressed as IO, for testing a pipeline without touching the filesystem and for
bridging one workflow to another in the same process. A `ChannelSink` in one workflow and a
`ChannelSource` in another meet by declared `name`: whichever factory builds first creates the
pair through the host's channel registry, and the second `take`s the other end.

EOF is the sender being dropped: `recv` yields `None` once every `Sender` is gone. The sink is
always the channel's only sender, so the producer workflow finishing (its `ChannelSink` node
finalising) is what signals the consumer's `ChannelSource` EOF.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>each constructor returns the paired endpoint</em></div>

```rust
use pcs_connector_channel::{ChannelSink, ChannelSource};

ChannelSource::new(Arc<Schema>, buffer: usize) -> (mpsc::Sender<RecordBatch>, Self)
ChannelSink::new  (Arc<Schema>, buffer: usize) -> (Self, mpsc::Receiver<RecordBatch>)
```

</div>

`ChannelSink::pending_rows` reports how many batches are sitting in the channel, counted from the
slots the sender has taken. The `Sink` trait calls that an approximate row count, and it is exact
only when every batch holds one row.

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>no transformer key: batches arrive already typed</em></div>

```kdl
workflow "producer" {
    // ...
    sink "out" type="ChannelSink" component="Trade" {
        config name="trades" buffer=8 {
            schema_fields "id" type="Int64" nullable=#false
        }
    }
}

workflow "consumer" {
    source "in" type="ChannelSource" component="Trade" {
        config name="trades" buffer=8 {
            schema_fields "id" type="Int64" nullable=#false
        }
    }
    // ...
}
```

</div>

The two halves can sit in the same workflow or in two different ones declared in the same config
file; either way they resolve through the one process-wide channel registry
`register_builtin_factories` attaches by default. Pass a different bridge with
`ServiceBuilder::with_channel_bridge` to share a registry across services built separately.

## Config keys

| Key | Type | Default |
|---|---|---|
| `name` | string | required |
| `buffer` | integer | `8` |
| `schema_fields` | list of fields | required |

Both factories hand-parse `ConfigValue` with no `deny_unknown_fields`, so an unrecognised key in the
`config` node is ignored rather than rejected.

## Each channel is exactly one source and one sink

<div class="note note-warn">
<span class="note-label">Load-time rule</span>
<p>
<code>ServiceConfig::validate</code> rejects a <code>name</code> declared by two
<code>ChannelSink</code>s, two <code>ChannelSource</code>s, or only one half, before the process
ever builds a connector. The paired halves must also agree on schema and <code>buffer</code>; a
mismatch is a configuration error from the registry at build time.
</p>
</div>

## Errors you can hit

| Message | Raised by |
|---|---|
| `ChannelSource requires a channel bridge; register one via ServiceBuilder::with_channel_bridge` | the factory, when no bridge is registered |
| `ChannelSource config requires a 'name' key naming the shared channel` | the factory, before resolving the bridge |
| `ChannelSource config requires a 'schema_fields' list` | the factory, before construction |
| `channel '<name>': declares a ChannelSink but no ChannelSource` | `ServiceConfig::validate`, at load time |
| `channel '<name>': more than one ChannelSink declared` | `ServiceConfig::validate`, at load time |
| `channel '<name>': the paired ChannelSource and ChannelSink declare different schemas` | the channel registry, at build time |
| `channel '<name>': buffer <n> differs from the paired half's buffer <m>` | the channel registry, at build time |
| `ChannelSource: received batch with schema {:?}, expected {:?}` | a batch that does not match the declared schema |
| `ChannelSink: channel send error: {e}` | writing after the receiver is gone |

## Where it is exercised

`examples/native/stream_latency.rs` and `crates/pcs-service/tests/stream_mode.rs` build the pair
directly in Rust. `crates/pcs-service/tests/channel_bridge.rs` drives the config-declared,
two-workflow path end to end through `ServiceBuilder::build_all`.
