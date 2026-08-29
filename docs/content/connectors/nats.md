+++
title = "NATS"
description = "Core subject pub/sub or JetStream, chosen by one kind key, with every connection, auth, consumer and publish knob named."
template = "subpage.html"
weight = 5

[[extra.facts]]
label = "Crate"
value = "<code>pcs-connector-nats</code>"
[[extra.facts]]
label = "Feature"
value = "<code>connector-nats</code>, which implies <code>transformer-ndjson</code>"
[[extra.facts]]
label = "Config type"
value = "<code>NatsSource</code>, <code>NatsSink</code>"
[[extra.facts]]
label = "Transformer key"
value = "Required on both halves: neither resolves a format on its own"
+++

## What it does

One `mode` node decides which NATS the connector speaks. `kind="core"` is plain subject pub/sub: no
persistence, no acks, and a `queue_group` to spread one subject across instances.
`kind="jetstream"` is a durable stream, read through a pull consumer and written with publish acks.

`async-nats` has no string-keyed property bag, so there is no passthrough table. Every knob is a
named key.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>both are synchronous and open no connection</em></div>

```rust
use pcs_connector_nats::{
    NatsSink, NatsSinkConfig, NatsSource, NatsSourceConfig,
};

NatsSource::new(NatsSourceConfig, Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
NatsSink::new  (NatsSinkConfig,   Arc<Schema>, Arc<dyn Transformer>) -> Result<Self>
```

</div>

Each one validates the config and asks the transformer for its message shape. Connecting,
provisioning and creating the consumer happen on the first `next_batch` or `write_batch`, so
`pcs-service validate` stays server free. Every mode struct carries a `Default` matching the config
defaults, so a config built in Rust names only the keys it changes.

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>a JetStream source over a durable pull consumer</em></div>

```kdl
transformer "orders_json" format="ndjson"

source "orders_in" type="NatsSource" component="Order" transformer="orders_json" {
    config {
        batch_size 1000
        stop_at_end #true

        connection {
            servers "nats://localhost:4222"
        }

        mode kind="jetstream" {
            stream "ORDERS"
            durable_name "pcs-orders"
            filter_subjects "orders.new"
        }

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

## Top-level config keys

| Key | Default | Half |
|---|---|---|
| `connection` | required | both |
| `mode` | required | both |
| `batch_size` | `1000` | source |
| `poll_timeout_ms` | `1000` | source |
| `stop_at_end` | `false` | source |
| `schema_fields` | empty | both |

`transformer` is a property of the `source` or `sink` node, not a `config` key, and both halves
need one: this connector moves bytes and resolves no format itself. A node with none is
`NatsSource moves bytes and needs a 'transformer' key naming a declared transformer`.

Every struct is `#[serde(deny_unknown_fields)]`, so a misspelled key fails to parse and the factory
reports `NatsSource config: {e}`.

## connection

| Key | Default |
|---|---|
| `servers` | required, one or more URLs |
| `name` | absent |
| `connect_timeout_ms` | `5000` |
| `request_timeout_ms` | `10000`, `0` waits forever |
| `ping_interval_ms` | `60000` |
| `max_reconnects` | `0`, meaning unlimited |
| `reconnect_delay_ms` | `0`, keeping the client's own backoff |
| `retry_on_initial_connect` | `true` |
| `subscription_capacity` | `65536` |
| `client_capacity` | `2048` |
| `read_buffer_capacity` | `65535` |
| `no_echo` | `false` |
| `inbox_prefix` | absent, so `_INBOX` |
| `ignore_discovered_servers` | `false` |
| `retain_servers_order` | `false` |
| `auth` | `kind="none"` |
| `tls` | nothing required |

`servers` accepts `nats://`, `tls://`, `ws://` and `wss://`, and a bare `host:port` means
`nats://host:port` on port 4222. Each entry is parsed during validation, which reads no file and
opens no socket, so a typo is caught before the service starts:
`NatsSource: connection.servers entry '{s}' is not a NATS URL: {e}`.

## connection.auth

`kind` picks the scheme, and every secret has a `_file` twin for a secret mount. Exactly one of each
pair is set, or validation reports
`NatsSource: connection.auth kind = "token" needs exactly one of 'token' or 'token_file'`.

| `kind` | Keys |
|---|---|
| `none` | nothing, and what an absent table means |
| `token` | `token` or `token_file` |
| `user_password` | `user`, plus `password` or `password_file` |
| `nkey` | `seed` or `seed_file` |
| `credentials` | `path` to a `.creds` file holding a JWT and its seed |

## connection.tls

| Key | Default |
|---|---|
| `require` | `false` |
| `tls_first` | `false` |
| `root_certificates` | absent, so the OS trust store |
| `client_certificate` | absent |
| `client_key` | absent |

A `tls://` server URL requires encryption on its own; `require=#true` does the same for a
`nats://` URL. `tls_first` performs the handshake before the `INFO` exchange, needs
`handshake_first` on the server, and implies `require`. Mutual TLS needs both certificate keys or
neither: `NatsSource: connection.tls needs both 'client_certificate' and 'client_key', or neither`.

## mode kind="core"

| Key | Default | Half |
|---|---|---|
| `subject` | required, comma separated for several | both |
| `queue_group` | absent | source |
| `subject_field` | absent | sink |
| `headers` | empty | sink |
| `header_fields` | empty | sink |
| `reply_subject` | absent | sink |
| `flush_timeout_ms` | `30000` | sink |
| `flush_every_batch` | `true` | sink |

Core NATS drops a message with no subscriber, so a source that has not polled yet has not
subscribed yet. Every subscriber in one `queue_group` sees a disjoint share of the subject, which is
how a core subject spreads across PCS instances.

## mode kind="jetstream", source

| Key | Default |
|---|---|
| `stream` | required |
| `durable_name` | absent, so an ephemeral consumer |
| `consumer_name` | absent, so `durable_name` |
| `description` | absent |
| `filter_subjects` | empty, meaning every subject the stream captures |
| `deliver_policy` | `kind="all"` |
| `ack_policy` | `"explicit"`, or `"all"` or `"none"` |
| `double_ack` | `false` |
| `ack_wait_ms`, `max_deliver`, `max_ack_pending`, `max_waiting`, `max_batch`, `max_bytes`, `max_expires_ms`, `inactive_threshold_ms`, `num_replicas`, `rate_limit_bps`, `sample_frequency` | `0`, keeping the server default |
| `memory_storage`, `headers_only` | `false` |
| `replay_policy` | `"instant"`, or `"original"` |
| `backoff_ms` | empty |
| `metadata` | empty |
| `fetch_expires_ms` | `5000` |
| `fetch_max_bytes` | `0`, so `batch_size` alone bounds a window |
| `heartbeat_ms` | `0`, keeping the client default |
| `domain` | absent |
| `api_prefix` | absent, so `$JS.API` |
| `api_timeout_ms` | `5000` |
| `stream_provision` | see below |

`deliver_policy.kind` is one of `all`, `last`, `new`, `last_per_subject`, `by_start_sequence` with
`start_sequence`, or `by_start_time` with an RFC 3339 `start_time`. A malformed timestamp is caught
during validation.

`domain` and `api_prefix` are two spellings of the same prefix, so setting both is refused:
`NatsSource config: 'mode.domain' and 'mode.api_prefix' are two ways to say the same thing; set
one`.

## mode kind="jetstream", sink

| Key | Default |
|---|---|
| `stream` | required |
| `subject` | required, and it must be one `stream` captures |
| `subject_field` | absent |
| `headers`, `header_fields` | empty |
| `message_id_field` | absent |
| `expected_stream` | `false` |
| `await_ack` | `true` |
| `api_timeout_ms` | `5000` |
| `ack_timeout_ms` | `30000` |
| `max_ack_inflight` | `5000` |
| `backpressure_on_inflight` | `true` |
| `domain`, `api_prefix` | absent |
| `stream_provision` | see below |

`stream` is named rather than inferred from the subject, because provisioning needs a name and
fetching the stream once at startup turns a typo into an error instead of a `no responders` failure
per batch. `expected_stream=#true` sends `Nats-Expected-Stream`, so a publish that would land in
another stream is refused. `message_id_field` renders a column into `Nats-Msg-Id`, which the
stream's duplicate window deduplicates on.

## stream_provision

| Key | Default |
|---|---|
| `create` | `true` |
| `subjects` | empty, and derived from the half's own subjects |
| `retention` | `"limits"`, or `"interest"` or `"workqueue"` |
| `storage` | `"file"`, or `"memory"` |
| `discard` | `"old"`, or `"new"` |
| `compression` | `"none"`, or `"s2"` |
| `num_replicas` | `1`, within `1..=5` |
| `max_messages`, `max_messages_per_subject`, `max_bytes`, `max_message_size`, `max_consumers` | `-1`, meaning unlimited |
| `max_age_ms` | `0`, so no age limit |
| `duplicate_window_ms` | `0`, keeping the server default |
| `allow_rollup`, `deny_delete`, `deny_purge`, `allow_direct` | `false` |
| `description` | absent |
| `metadata` | empty |

<div class="note">
<span class="note-label">An empty subjects list is derived, not guessed</span>
<p>
A stream is created when it does not exist, like a Kafka topic. Its subject list comes from
<code>subjects</code>, and when that is empty from whatever the half already declares: the sink's
<code>subject</code>, the source's <code>filter_subjects</code>. Those are exactly the subjects
that half writes or reads. With both empty the server's own default applies, which is the stream
name as its sole subject.
</p>
<p>
An existing stream is returned as it stands and never reconfigured, so
<code>create=#true</code> against a stream someone else owns changes nothing. Concurrent PCS
instances race to create the same stream and only one needs to win.
</p>
</div>

<div class="note note-warn">
<span class="note-label">create=#false to catch a typo</span>
<p>
With <code>create=#true</code> a misspelled <code>stream</code> is a new empty stream, and a
source reading it waits forever. Set <code>create=#false</code> to require one that already
exists; a missing stream then reports
<code>NatsSource: cannot resolve stream '{name}': {e}; set mode.stream_provision.create = true to
have PCS create it</code>.
</p>
</div>

## Stream mode and stop_at_end

<div class="note note-warn">
<span class="note-label">Stream mode required</span>
<p>
<code>NatsSource</code> counts as a live source unless <code>stop_at_end=#true</code>, and config
validation refuses a live source outside stream mode:
<code>source type 'NatsSource' never reaches EOF; it requires standalone mode with run_mode
kind="stream"</code>. Stream mode allows any number of sources, pulled round-robin. The opt-out is read off the raw
config table, so it has to be a real boolean: <code>stop_at_end="true"</code> in quotes is ignored
and the source stays live.
</p>
</div>

With `stop_at_end=#true` a JetStream source uses a `no_wait` pull, so an empty result is EOF. A
core source has no end-of-stream signal at all, so one elapsed `poll_timeout_ms` window with
nothing on it is the only EOF a subscription can offer.

## Delivery

JetStream is at-least-once. The source acknowledges the previous batch at the start of the next
`next_batch` call, so a crash between the two redelivers that batch. `double_ack=#true` waits for
the server to confirm each ack, and needs an ack policy: `double_ack` with `ack_policy="none"` is
refused. The sink waits for every publish ack by default, so a returned `write_batch` means the
stream holds the rows.

Core NATS is at-most-once and has no ack at all: a message consumed while the pipeline later fails
is gone. A core sink's `flush_every_batch` waits for the server to acknowledge the whole write,
which is the strongest boundary the protocol offers.

`estimated_rows` reports JetStream's own count of messages still waiting for the consumer. Core
mode has no such number and reports none.

## Formats

A transformer with no message codec is refused on either half:
`NatsSink: format '{format}' has no message codec`, and the source's twin. That rules out
[csv](@/transformers/csv.md) and [parquet](@/transformers/parquet.md).

`subject_field`, `header_fields` and `message_id_field` each need a format that emits one message
per row, so [arrow-ipc](@/transformers/arrow-ipc.md) is refused with
`NatsSink config: 'mode.subject_field' needs a row-per-message format; '{format}' emits one message
per batch`. [ndjson](@/transformers/ndjson.md) and [avro](@/transformers/avro.md) both qualify.

A rendered cell replaces the configured subject outright, so the column holds the whole subject and
`subject` is the fallback for a null cell. Header names and values reach `HeaderName::from_str` and
`HeaderValue::from_str`, so an illegal one from config or from the data is an error rather than a
panic.

## Worked example

`examples/configs/nats.kdl` is a commented config covering both halves:

Runs the same on Linux, macOS and Windows (PowerShell):

<div class="code">
<div class="code-cap"><span>Bash</span><em>validating examples/configs/nats.kdl with the connector-nats feature</em></div>

```bash
cargo run --features connector-nats,wasm --bin pcs-service -- \
    validate --config examples/configs/nats.kdl
```

</div>

`crates/pcs-connector-nats/tests/nats_roundtrip.rs` runs thirteen tests against a real server in a
container, and prints `SKIP:` and passes when no Docker daemon is reachable.
