//! `pcs-connector-postgresql`: a PostgreSQL [`Source`] and [`Sink`] for PCS.
//!
//! [`Source`]: pcs_core::io::source::Source
//! [`Sink`]: pcs_core::io::sink::Sink
//!
//! Both halves are declarative: the Arrow schema is written in the service
//! configuration, not introspected, because [`SourceFactory::build`] is
//! synchronous and cannot open a connection. The declared schema is checked
//! against the live catalog on the first query and a column the server cannot
//! fill is a loud error rather than a silent coercion.
//!
//! [`SourceFactory::build`]: pcs_connector::factory::SourceFactory::build
//!
//! # Source modes
//!
//! One `mode` node inside the source's `config` picks the read strategy.
//!
//! `kind="polling"` runs an incremental query over a live table, ordered by a
//! cursor column, resuming from a durable offset row. It sees inserts, and it
//! sees updates only when the cursor column is an `updated_at`-style value that
//! the writer bumps. It never sees deletes.
//!
//! `kind="cdc_trigger"` runs the same query shape over an append-only outbox
//! table a trigger writes. It is lossless by construction and can prune rows it
//! has acknowledged with `retention="delete_acked"`.
//!
//! `kind="cdc_logical"` decodes `pgoutput` messages from a logical
//! replication slot through the SQL slot interface
//! (`pg_logical_slot_peek_binary_changes`), so inserts, updates and deletes all
//! arrive with their transaction metadata. It requires **PostgreSQL 14 or
//! newer**, because it asks `pgoutput` for `binary = 'true'`.
//!
//! Every mode returns `Ok(None)` once it is caught up, so the batch runners as
//! well as the stream runner can drive it.
//!
//! ## Server prerequisites for `cdc_logical`
//!
//! - `wal_level = logical`
//! - `max_replication_slots` at least 1
//! - the connecting role has the `REPLICATION` attribute, or is a superuser;
//!   `pg_read_all_data` is not sufficient
//!
//! No walsender process is involved, so `max_wal_senders` is irrelevant, and
//! `pgoutput` needs no allow-listing.
//!
//! A `DELETE`, and the old tuple of an `UPDATE`, carry only replica-identity
//! columns unless the table is `REPLICA IDENTITY FULL`; every other column
//! arrives NULL. Declare those fields `nullable=#true`, or run
//! `ALTER TABLE … REPLICA IDENTITY FULL` when deletes must carry whole rows.
//!
//! ## Reserved metadata columns
//!
//! `cdc_logical` fills five field names from the change stream instead of from
//! the tuple. Declare the ones you want, in any position; any other
//! `__`-prefixed field name is a configuration error.
//!
//! | field | required `type` | value |
//! |---|---|---|
//! | `__op` | `utf8` | `"I"`, `"U"` or `"D"` |
//! | `__lsn` | `int64` | the change's LSN |
//! | `__xid` | `int64` | transaction id, nullable |
//! | `__commit_ts` | `timestamp_micros_utc` | the enclosing transaction's commit time |
//! | `__table` | `utf8` | `"schema.table"` |
//!
//! # Sink write modes
//!
//! Every mode bulk-loads through `COPY … WITH (FORMAT binary)`; one flush is
//! one transaction.
//!
//! `write_mode="append"` copies straight into the target table.
//! `"upsert"` copies into an `ON COMMIT DROP` temp table, then
//! `INSERT … ON CONFLICT (…) DO UPDATE`. `"ignore_conflicts"` is the same with
//! `DO NOTHING`. Both need `conflict_columns`, and a batch that repeats a
//! conflict key needs `dedupe_order_column` to pick the winner.
//!
//! # Delivery semantics
//!
//! `polling` and `cdc_trigger` commit their offset at the start of the *next*
//! drain cycle, and `cdc_logical` advances its slot there, because
//! [`Source`](pcs_core::io::source::Source) has no acknowledgement hook. A
//! crash mid-cycle replays that cycle: delivery is **at-least-once**, matching
//! what the distributed layer already promises. Configure the sink
//! `write_mode="upsert"` or `"ignore_conflicts"` to make replays idempotent.
//!
//! # Configuration
//!
//! ```kdl
//! source "pg_orders" type="PostgresSource" component="OrderChange" {
//!     config name="pg_orders" batch_rows=8192 {
//!         connection dsn="${PCS_PG_DSN}" application_name="pcs-pg_orders" \
//!             connect_timeout_ms=5000 statement_timeout_ms=30000 sslmode="require"
//!         mode kind="cdc_logical" slot="pcs_orders_slot" \
//!             publication="pcs_orders_pub" table="public.orders"
//!         schema_fields "__op" type="utf8" nullable=#false
//!         schema_fields "id" type="int64" nullable=#false
//!     }
//! }
//!
//! sink "pg_enriched" type="PostgresSink" component="OrderChange" {
//!     config name="pg_enriched" table="public.enriched_orders" \
//!         write_mode="upsert" conflict_columns="id" {
//!         connection dsn="${PCS_PG_DSN}" sslmode="require"
//!         schema_fields "id" type="int64" nullable=#false
//!     }
//! }
//! ```
//!
//! The `source` node's declared id is what the service logs; the `name` inside
//! its `config` is what the connector sees, because the factory is handed only
//! the `config` sub-table. Keep them equal for readable output.
//!
//! `${VAR}` and `${VAR:-default}` are substituted by the service before the
//! configuration is parsed, so credentials stay out of the file. Nothing in
//! this crate ever logs a DSN, user or password: every message names the
//! target as `host:port/dbname` only.
//!
//! # Cargo features
//!
//! - `tls` (default): rustls-backed TLS. `sslmode="require"` needs it.
//! - `tracing`: emit `tracing` events.
//! - `metrics`: record into the process-global OpenTelemetry meter `pcs`.

#![deny(missing_docs)]

pub mod config;

pub mod factory;

pub mod sink;
pub mod source;

mod connection;
mod encode;
mod metrics;
mod numeric;
mod offsets;
mod types;
mod values;

pub use factory::{PostgresSinkFactory, PostgresSourceFactory};
pub use sink::PostgresSink;
pub use source::PostgresSource;

pub use config::{
    ConnectionConfig, CursorMode, FieldSpec, LogicalMode, NotifyConfig, PgFieldType,
    PostgresSinkConfig, PostgresSourceConfig, ReconnectConfig, Retention, SourceMode,
    SslModeConfig, WriteMode,
};
