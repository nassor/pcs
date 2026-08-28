+++
title = "TCP ingest"
description = "A live source: length-prefixed frames off a listener socket, decoded by the transformer you name."
template = "subpage.html"
weight = 6

[[extra.facts]]
label = "Crate"
value = "<code>pcs-connector-tcp</code>"
[[extra.facts]]
label = "Feature"
value = "<code>connector-tcp</code>, which implies <code>transformer-arrow-ipc</code>"
[[extra.facts]]
label = "Config type"
value = "<code>tcp</code>, lowercase"
[[extra.facts]]
label = "Transformer key"
value = "Required: the source resolves no format on its own"
+++

## What it does

A live source. It never reaches EOF, so only the stream runner can drive it and config validation
rejects it in any other mode.

The listener socket is bound inside the constructor, so a busy port or a bad address fails at
config time rather than on the first batch.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>local_addr resolves an ephemeral port</em></div>

```rust
use pcs_connector_tcp::TcpIngestSource;

TcpIngestSource::new(
    bind: &str,
    schema: Arc<Schema>,
    buffer: usize,
    max_frame_bytes: usize,
    transformer: Arc<dyn Transformer>,
) -> Result<Self>

source.local_addr() -> SocketAddr
```

</div>

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>inside a workflow under run_mode kind="stream"; the source still needs an outbound link</em></div>

```kdl
transformer "ipc" format="arrow-ipc"

source "ticks" type="tcp" component="Tick" transformer="ipc" {
    config {
        bind "0.0.0.0:9500"
        buffer 64                   // decoded batches queued before backpressure
        max_frame_bytes 8388608     // 8 MiB

        schema_fields "price" type="Float64" nullable=#false
    }
}
```

</div>

## Config keys

| Key | Default |
|---|---|
| `bind` | required |
| `buffer` | `64` decoded batches queued before backpressure |
| `max_frame_bytes` | `8388608` |
| `schema_fields` | required |

`transformer` is a property of the `source` node, not a `config` key, and it is required: this
source moves bytes and resolves no format itself. A node with none is
`tcp moves bytes and needs a 'transformer' key naming a declared transformer`.

The factory hand-parses `ConfigValue` with no `deny_unknown_fields`, so an unrecognised key is
ignored rather than rejected.

## Framing

One frame is a `u32` big-endian length prefix followed by exactly that many payload bytes. Decoding
those bytes belongs to the transformer, and the decoder is opened once per connection, so a frame
carrying several batches yields their concatenation.

A clean close between frames is a normal disconnect. An oversized frame, a truncated payload, a
frame that decodes to no batch, and a schema mismatch each close that one connection and leave the
listener and every other producer running. Each one is logged: `TcpIngestSource: oversized frame,
closing connection`, `TcpIngestSource: truncated frame payload, closing connection`,
`TcpIngestSource: frame decoded no batch` and `TcpIngestSource: bad frame, closing connection`. A
schema mismatch arrives inside the last of those, as
`arrow-ipc: received batch with schema {:?}, expected {:?}`.

## Sharp edge: a transformer with no message codec fails per connection

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
<code>TcpSourceFactory::build</code> takes the transformer the node names and nothing more. It
never asks for a decoder, so a <code>csv</code> transformer passes
<code>pcs-service validate</code> as long as <code>transformer-csv</code> is compiled in. The
missing capability appears when a connection arrives: <code>open_message_decoder</code> fails, the
connection is closed with <code>TcpIngestSource: cannot decode this format, closing
connection</code>, and the listener keeps accepting. A <code>transformer</code> node naming a
format no transformer is registered for still fails at build.
</p>
</div>

## Errors you can hit

| Message | Raised by |
|---|---|
| `tcp source config requires a 'bind' string` | the factory, before construction |
| `tcp moves bytes and needs a 'transformer' key naming a declared transformer` | the shared context, when the node declared none |
| `TcpIngestSource: cannot bind '{bind}': {e}` | binding the listener |
| `source type 'tcp' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | config validation, in any other run mode |

## Where it is exercised

`crates/pcs-service/tests/stream_mode.rs` drives it end to end: three good frames, then an
oversized one that closes its connection, then a fourth good frame on a fresh connection, with the
listener still up. No file under `examples/configs/` declares `type="tcp"`, so
the two nodes above are the worked declaration.
