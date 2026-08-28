+++
title = "TCP"
description = "Two halves of one frame: a source that listens and a sink that dials, with the payload decoded and encoded by the transformer you name."
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
value = "<code>tcp</code>, lowercase, for the source and the sink alike"
[[extra.facts]]
label = "Transformer key"
value = "Required: neither half resolves a format on its own"
+++

## What it does

Two halves of the same frame. The source listens on `bind` and yields one batch per frame it
receives. The sink dials `connect` and writes one frame per message the transformer encodes.

The source is live: it never reaches EOF, so only the stream runner can drive it and config
validation rejects `type="tcp"` on a `source` node in any other mode. That rule reads source nodes
only, so a `tcp` sink runs in every run mode.

The listener socket is bound inside the source's constructor, so a busy port or a bad address fails
at config time rather than on the first batch. The sink resolves its address there too, and dials on
the first batch.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>local_addr resolves an ephemeral port, peer_addr the dialled one</em></div>

```rust
use pcs_connector_tcp::{TcpIngestSource, TcpSink};

TcpIngestSource::new(
    bind: &str,
    schema: Arc<Schema>,
    buffer: usize,
    max_frame_bytes: usize,
    transformer: Arc<dyn Transformer>,
) -> Result<Self>

source.local_addr() -> SocketAddr

TcpSink::connect(
    connect: &str,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
) -> Result<Self>

sink.peer_addr() -> SocketAddr
```

</div>

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>a source needs run_mode kind="stream"; both nodes still need their links</em></div>

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

sink "forward" type="tcp" component="EnrichedTick" transformer="ipc" {
    config {
        connect "collector.internal:9600"

        schema_fields "price" type="Float64" nullable=#false
    }
}
```

</div>

## Config keys

Source:

| Key | Default |
|---|---|
| `bind` | required |
| `buffer` | `64` decoded batches queued before backpressure |
| `max_frame_bytes` | `8388608` |
| `schema_fields` | required |

Sink:

| Key | Default |
|---|---|
| `connect` | required |
| `schema_fields` | required |

`transformer` is a property of the `source` or `sink` node, not a `config` key, and it is required on
both: this connector moves bytes and resolves no format itself. A node with none is
`tcp moves bytes and needs a 'transformer' key naming a declared transformer`.

The factory hand-parses `ConfigValue` with no `deny_unknown_fields`, so an unrecognised key is
ignored rather than rejected.

## Framing

One frame is a `u32` big-endian length prefix followed by exactly that many payload bytes. The sink
writes the framing the source reads, so the two halves are wire compatible: a sink in one service
feeds a source in another when both name the same format.

Decoding and encoding those bytes belongs to the transformer. On the source the decoder is opened
once per connection, so a frame carrying several batches yields their concatenation. On the sink one
batch becomes one frame per encoded message, which for `arrow-ipc` is one frame per batch.

A clean close between frames is a normal disconnect, and the sink's `finish` closes its write half
that way. On the source an oversized frame, a truncated payload, a frame that decodes to no batch,
and a schema mismatch each close that one connection and leave the listener and every other producer
running. Each one is logged: `TcpIngestSource: oversized frame, closing connection`,
`TcpIngestSource: truncated frame payload, closing connection`, `TcpIngestSource: frame decoded no
batch` and `TcpIngestSource: bad frame, closing connection`. A schema mismatch arrives inside the
last of those, as `arrow-ipc: received batch with schema {:?}, expected {:?}`.

## Sharp edge: a transformer with no message codec fails per connection

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
<code>TcpSourceFactory::build</code> takes the transformer the node names and nothing more. It
never asks for a decoder, so a <code>csv</code> transformer passes
<code>pcs-service validate</code> as long as <code>transformer-csv</code> is compiled in. The
missing capability appears when a connection arrives: <code>open_message_decoder</code> fails, the
connection is closed with <code>TcpIngestSource: cannot decode this format, closing
connection</code>, and the listener keeps accepting. The sink half fails the same way on its first
batch, where <code>encode_messages</code> returns
<code>format 'csv' does not support encoding discrete messages</code>. A
<code>transformer</code> node naming a format no transformer is registered for still fails at build.
</p>
</div>

## Sharp edge: the sink dials late and never redials

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
<code>TcpSink::connect</code> resolves the address and opens no socket, so
<code>pcs-service validate</code> passes while the peer is down. The dial happens on the first
batch, which is where an unreachable peer surfaces as
<code>TcpSink: cannot connect to {peer}</code>. One connection then serves the sink's whole
lifetime: there is no reconnect, so a peer that goes away mid-run fails every following write on
that same socket.
</p>
</div>

## Errors you can hit

| Message | Raised by |
|---|---|
| `tcp source config requires a 'bind' string` | the source factory, before construction |
| `tcp sink config requires a 'connect' string` | the sink factory, before construction |
| `tcp moves bytes and needs a 'transformer' key naming a declared transformer` | the shared context, when the node declared none |
| `TcpIngestSource: cannot bind '{bind}': {e}` | binding the listener |
| `TcpSink: cannot resolve 'connect' address '{connect}': {e}` | resolving the sink's address at build |
| `TcpSink: 'connect' address '{connect}' resolved to no address` | the same, when resolution yields nothing |
| `TcpSink: cannot connect to {peer}: {e}` | the first batch's dial |
| `TcpSink: message of {n} bytes does not fit the u32 frame length prefix` | framing a payload above 4 GiB |
| `TcpSink: writing a frame header to {peer} failed: {e}` | the socket write |
| `TcpSink: writing a {len} byte frame payload to {peer} failed: {e}` | the socket write |
| `TcpSink: flushing the socket to {peer} failed: {e}` | `finish` |
| `TcpSink: shutting down the socket to {peer} failed: {e}` | `finish` |
| `source type 'tcp' never reaches EOF; it requires standalone mode with run_mode kind="stream"` | config validation, for a source node in any other run mode |

## Where it is exercised

`crates/pcs-service/tests/tcp_connector.rs` drives both halves from one config: a producer pushes
frames at the source, the workflow passes them through, and the sink's peer decodes the frames back
to the same rows. The same file pins the validation asymmetry, that a `tcp` sink is accepted in every
run mode while a `tcp` source is not.

`crates/pcs-service/tests/stream_mode.rs` covers the source alone: three good frames, then an
oversized one that closes its connection, then a fourth good frame on a fresh connection, with the
listener still up.

`examples/configs/tcp.kdl` is the worked declaration of both nodes.
