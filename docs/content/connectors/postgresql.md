+++
title = "PostgreSQL"
description = "Polling, a trigger outbox or pgoutput logical decoding on the way in; COPY binary with an optional staged upsert on the way out."
template = "subpage.html"
weight = 3

[[extra.facts]]
label = "Crate"
value = "<code>pcs-connector-postgresql</code>"
[[extra.facts]]
label = "Feature"
value = "<code>connector-postgresql</code>"
[[extra.facts]]
label = "Config type"
value = "<code>PostgresSource</code>, <code>PostgresSink</code>"
[[extra.facts]]
label = "Transformer key"
value = "None: rows arrive typed, so no transformer is involved"
+++

## What it does

The source reads through incremental polling, a trigger-written outbox table, or `pgoutput` logical
decoding. Every mode returns `Ok(None)` once it is caught up, so the batch runners can drive it as
well as the stream runner.

The sink bulk loads through `COPY FORMAT binary`, with an optional staged upsert.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>both are synchronous and open no connection</em></div>

```rust
use pcs_connector_postgresql::{
    PostgresSink, PostgresSinkConfig, PostgresSource, PostgresSourceConfig,
};

PostgresSource::new(PostgresSourceConfig) -> Result<Self>
PostgresSink::new  (PostgresSinkConfig)   -> Result<Self>
```

</div>

Each one validates its config and then builds a reader or a writer. The DSN is parsed, no socket is
opened, so the first connection happens on the first batch. That is why `pcs-service validate`
needs no database.

## In service config

<div class="code">
<div class="code-cap"><span>KDL</span><em>the whole config table deserialises into the config struct</em></div>

```kdl
source "pg_orders" type="PostgresSource" component="OrderChange" {
    config {
        name "pg_orders"
        batch_rows 8192

        connection dsn="${PCS_PG_DSN}" sslmode="require"

        // polling, cdc_trigger or cdc_logical.
        mode kind="polling" {
            table "public.orders"
            cursor_column "id"
        }

        schema_fields "id" type="int64" nullable=#false
    }
}
```

</div>

The connector only ever sees the `config` node, so `name` is repeated there.

## Source config

### connection

| Key | Default |
|---|---|
| `dsn` | required |
| `user`, `password`, `password_file`, `application_name`, `sslrootcert` | absent |
| `connect_timeout_ms` | `5000` |
| `statement_timeout_ms` | `30000` |
| `sslmode` | `prefer`, one of `disable`, `prefer`, `require` |

A `reconnect` child of `connection` takes `max_attempts` `3`, `base_delay_ms` `100`, `multiplier`
`2.0`, `max_delay_ms` `30000` and `jitter` `0.1`.

### mode

`kind` is `polling`, `cdc_trigger` or `cdc_logical`.

The two cursor modes take `table` and `cursor_column`, both required, plus `tiebreak_column`,
`initial` `"beginning"`, `offset_table` `"pcs_source_offsets"`, `offset_table_autocreate` `true`,
`where_clause`, and `retention` `keep` or `delete_acked`.

`cdc_logical` takes `slot`, `publication` and `table`, all required, plus `slot_autocreate` `true`
and `max_changes_per_cycle` `10000`.

### schema_fields

Each entry takes `name`, `type`, `nullable` defaulting to `true`, and `precision` with `scale` for
`decimal128`. The accepted `type` names are `boolean`, `int16`, `int32`, `int64`, `float32`,
`float64`, `utf8`, `binary`, `date32`, `time64_micros`, `timestamp_micros`,
`timestamp_micros_utc`, `uuid`, `json` and `decimal128`.

That vocabulary is this connector's own and it is not the shared one every other connector uses. It
adds the temporal, `uuid`, `json` and `decimal128` names, and it drops `int8`, the unsigned widths,
`largeutf8` and `date64`.

### The rest

`batch_rows` defaults to `8192` and `max_batches_per_cycle` to `0`, which means no cap. An optional
`notify` node takes a required `channel` and `timeout_ms` `30000`, so a polling source waits on
`LISTEN` instead of a timer.

## Sink config

| Key | Default |
|---|---|
| `table`, `schema_fields` | required |
| `write_mode` | `append`, one of `append`, `upsert`, `ignore_conflicts` |
| `conflict_columns`, `update_columns` | empty |
| `dedupe_order_column` | absent |
| `chunk_rows` | `65536` |
| `flush_rows` | `0` |
| `truncate_before_first_write` | `false` |

`PostgresSink::pending_rows` reports the rows buffered but not yet copied.

## What validation refuses

Every config struct is `#[serde(deny_unknown_fields)]`, so a misspelled key is a startup error
naming it. The cross-field rules are checked in `validate`, before any network call. Each message
below is prefixed with the half it came from, `PostgresSource:` or `PostgresSink:`:

- `notify is not supported with mode kind = "cdc_logical"; the slot interface has no notification channel`
- `retention = "delete_acked" applies to kind = "cdc_trigger" only, not "polling": deleting rows from a live table would destroy data`
- `schema_fields '{name}' uses the reserved '__' prefix, which only kind = "cdc_logical" fills, not "{kind}"`
- `schema_fields '{other}' is not a reserved metadata column; the '__' prefix is reserved, and the known names are {names}`, those names being `__op`, `__lsn`, `__xid`, `__commit_ts` and `__table`
- `reserved column '{name}' must be declared type "{expected}", not "{actual}"`, which is `utf8` for `__op` and `__table`, `int64` for `__lsn` and `__xid`, and `timestamp_micros_utc` for `__commit_ts`
- `write_mode "{mode}" requires a non-empty conflict_columns`
- `update_columns '{column}' is also a conflict column; a conflict key cannot be rewritten by its own upsert`
- `connection.sslmode = "require" needs the 'tls' feature of pcs-connector-postgresql, which is not enabled in this build`

## Constraints

<div class="note">
<span class="note-label">Constraint</span>
<p>
<code>kind="cdc_logical"</code> needs <b>PostgreSQL 14 or newer</b>,
<code>wal_level = logical</code>, and a role with the <code>REPLICATION</code> attribute. None of
the three is probed up front. The server rejects the slot query instead, and the error says which
one is missing: an old server fails on the <code>pgoutput 'binary'</code> option, a missing
privilege adds a note about the <code>REPLICATION</code> attribute, and a wrong
<code>wal_level</code> adds one about <code>wal_level</code> and
<code>max_replication_slots</code>.
</p>
<p>
The crate's own <code>tls</code> feature is on by default and <code>sslmode="require"</code>
depends on it. The <code>connector-postgresql</code> feature on the host adds
<code>tracing</code> and <code>metrics</code> on top.
</p>
</div>

## Worked example

<div class="code">
<div class="code-cap"><span>Bash</span><em>the example exits 0 without a DSN rather than failing</em></div>

```bash
PCS_PG_DSN=postgres://localhost/pcs \
    cargo run -p pcs-connector-postgresql --example postgres_roundtrip
```

</div>

`examples/configs/postgresql.kdl` is a commented config covering both halves.
It sets `run_mode kind="interval"`, because this source reaches EOF once it is caught up.
