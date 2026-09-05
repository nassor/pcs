+++
title = "HTTP"
description = "One GET in, one request per batch out. The body is a whole document in whatever format the transformer you name writes."
template = "subpage.html"
weight = 9

[[extra.facts]]
label = "Crate"
value = "<code>pcs-connector-http</code>"
[[extra.facts]]
label = "Feature"
value = "<code>connector-http</code>, in the default bundle, plus a <code>transformer-*</code> feature for the format"
[[extra.facts]]
label = "Config type"
value = "<code>HttpSource</code>, <code>HttpSink</code>"
[[extra.facts]]
label = "Transformer key"
value = "Required: neither half resolves a format on its own"
+++

## What it does

The source is one GET. The response body is spooled to a temp file, decoded through the
transformer's stream read surface, and the source reports EOF when that stream ends. It is finite,
so every run mode can drive it.

The sink is one request per batch. Each body is a self-contained document in the configured format,
written by a fresh writer over a fresh buffer, then sent.

Neither half touches the network while it is built. The GET happens on the first batch and the first
POST on the first batch written, so `pcs-service validate` needs no reachable endpoint.

HTTPS needs no keys. rustls verifies the peer against the platform trust store, through the `ring`
crypto backend the rest of the workspace links.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>the source's schema is the declared one, so it is Option; the sink's is not</em></div>

```rust
use pcs_connector_http::{HttpSink, HttpSource};

HttpSource::new(
    url: &str,
    declared: Option<Arc<Schema>>,
    transformer: Arc<dyn Transformer>,
    headers: Vec<(String, String)>,
    timeout: Duration,
) -> Result<Self>

HttpSink::new(
    url: &str,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    method: &str,
    headers: Vec<(String, String)>,
    timeout: Duration,
) -> Result<Self>
```

</div>

`HttpSource::estimated_rows` forwards what the reader reported, so it is `None` until the body has
arrived and `Some` after it only for a format that counts rows without reading them.

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>headers is a nested table, one entry per line</em></div>

```kdl
transformer "orders_csv" format="csv" {
    options has_headers=#true
}

source "orders_in" type="HttpSource" component="Order" transformer="orders_csv" {
    config {
        url "https://data.internal/orders.csv"
        timeout_ms 30000
        headers {
            accept "text/csv"
        }

        schema_fields "id" type="Int64" nullable=#false
    }
}

sink "orders_out" type="HttpSink" component="Order" transformer="orders_csv" {
    config {
        url "https://ingest.internal/v1/orders"
        method "POST"
        headers {
            "content-type" "text/csv"
        }

        schema_fields "id" type="Int64" nullable=#false
    }
}
```

</div>

## Config keys

Source:

| Key | Default |
|---|---|
| `url` | required |
| `headers` | none |
| `timeout_ms` | `30000`, the whole-request budget |
| `schema_fields` | optional, and the format decides; required by `schema_from "body"` |
| `schema_from` | `"config"`, the other value being `"body"`; source only |

Sink:

| Key | Default |
|---|---|
| `url` | required |
| `method` | `POST` |
| `headers` | none |
| `timeout_ms` | `30000`, the whole-request budget |
| `schema_fields` | required |

`transformer` is a property of the `source` or `sink` node, not a `config` key, and it is required on
both: this connector moves bytes and resolves no format itself. A node with none is
`HttpSource moves bytes and needs a 'transformer' key naming a declared transformer`.

Both factories hand-parse `ConfigValue` with no `deny_unknown_fields`, so an unrecognised key is
ignored rather than rejected.

## Bodies

On the sink, one batch is one request and one request is one whole document. The transformer opens a
writer over an empty buffer, takes the batch, and finishes, so `csv` emits its header row every
time, `ndjson` a block of lines, and `parquet` and `avro` one complete container with its footer.
Nothing accumulates between batches, which is why `finish` has nothing to do and why there is no
flush threshold to configure.

On the source, one GET is the whole stream. `schema_from` decides what the format is handed:
`"config"`, the default, hands over `schema_fields`, which
[csv](@/transformers/csv.md) requires and [ndjson](@/transformers/ndjson.md) infers without;
`"body"` hands over nothing, which is the only thing
[parquet](@/transformers/parquet.md) and [avro](@/transformers/avro.md) accept. In `"body"` mode
the schema the body turned out to carry must equal `schema_fields` field for field, and the
mismatch is a configuration error naming both. Decoding runs on a dedicated OS thread feeding a
bounded channel of four batches, so the executor never blocks on it.

## Sharp edge: validate does not touch the network

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
Both constructors build an HTTP client and open no connection, so
<code>pcs-service validate</code> passes against a url that is down, a host that does not resolve,
and a certificate the trust store rejects. All three surface during <code>serve</code>, on the first
batch, as <code>HttpSource: cannot GET {url}</code> or <code>HttpSink: cannot POST {url}</code>.
There is no retry and no reconnect: a request the endpoint refuses fails the run.
</p>
</div>

## Sharp edge: a self-describing format still needs schema_fields

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
<code>HttpSource::schema</code> reports the declared schema, and an empty one when the config
declared none, because nothing can be known about a body that has not arrived. The graph is
validated against that, so <code>schema_from "body"</code> still requires
<code>schema_fields</code>: it withholds them from the format, not from the graph check. A
<code>"body"</code> source without them is
<code>HttpSource: schema_from "body" needs a 'schema_fields' list to check the body's own schema
against</code>. Reading <code>parquet</code> or <code>avro</code> with the default
<code>schema_from "config"</code> fails on the first batch instead, where the format refuses the
declared schema.
</p>
</div>

## Sharp edge: HTTPS verification cannot be turned off

<div class="note note-warn">
<span class="note-label">Sharp edge</span>
<p>
There is no <code>insecure</code>, <code>verify</code> or <code>ca_file</code> key. Roots come
from <code>rustls-platform-verifier</code>, the machine's own trust store, not from a set bundled
into the binary, and a certificate it does not trust fails the request. A private CA has to be
installed there; a self-signed endpoint reached over a trusted network is a plain
<code>http://</code> url.
</p>
</div>

## Errors you can hit

| Message | Raised by |
|---|---|
| `HttpSource config requires a 'url' string field` | the source factory, before construction |
| `HttpSink config requires a 'url' string field` | the sink factory, before construction |
| `HttpSource config.headers must be a table of string values` | the factory, for a `headers` key that is not a table |
| `HttpSink config.headers['{name}'] must be a string` | the factory, for a non-string header value |
| `HttpSource moves bytes and needs a 'transformer' key naming a declared transformer` | the shared context, when the node declared none |
| `HttpSink: '{method}' is not a valid HTTP method: {e}` | the sink's constructor |
| `HttpSource: '{name}' is not a valid header name: {e}` | either constructor, per header |
| `HttpSink: header '{name}' has an invalid value: {e}` | either constructor, per header |
| `HttpSource: cannot GET {url}: {e}` | the request, including a refused connection, a TLS rejection and a timeout |
| `HttpSource: status {status} from {url}` | a response outside 2xx |
| `HttpSource: spool file: {e}`, `HttpSource: spool write: {e}`, `HttpSource: spool rewind: {e}` | spooling the body to its temp file |
| `HttpSource: spawn_blocking panic: {e}` | the blocking task that spools and opens the reader |
| `HttpSource config.schema_from must be "config" or "body"` | the source factory, for any other value |
| `HttpSource: schema_from "body" needs a 'schema_fields' list to check the body's own schema against` | the source's constructor |
| `HttpSource: body from {url} carries schema [...] but the config declared [...]` | the first batch, a `schema_from "body"` mismatch |
| `parquet: the file carries its own schema; remove schema_fields` | the first batch, for a self-describing format read with `schema_from "config"` |
| `HttpSink: cannot {METHOD} {url}: {e}` | the request |
| `HttpSink: status {status} from {url}` | a response outside 2xx |
| `format 'arrow-ipc' does not support writing a byte stream` | the sink's first batch, for a format with no stream write surface |

## Where it is exercised

`crates/pcs-connector-http/tests/round_trip.rs` drives both halves against a hand rolled
`TcpListener` server: the source reads a served csv body, the sink's captured bodies decode back to
the rows that went in, and the request count pins one request per batch. A served parquet body
covers `schema_from "body"`, including a body whose schema is not the declared one.

`crates/pcs-service/tests/http_connector.rs` runs the same pair from one config through
`run_standalone` in one-shot mode.

`examples/configs/http.kdl` is the worked declaration of both nodes.
