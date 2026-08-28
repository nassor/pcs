//! The `polling` and `cdc_trigger` reader.
//!
//! One [`CursorReader`] serves both modes. They differ only in
//! [`Retention`](crate::config::Retention) and in documented intent: a live
//! table's cursor must be an `updated_at`-style column for updates to be
//! visible, whereas an append-only outbox is lossless by construction.
//!
//! # Query shapes
//!
//! Three statements are prepared lazily and re-executed:
//!
//! ```text
//! -- no committed offset yet
//! SELECT <cols>, <cur>::text FROM <t> [WHERE (<where>)] ORDER BY <cur> LIMIT <n>
//! -- resumed, no tiebreak column
//! ... WHERE <cur> > $1::<cast> [AND (<where>)] ...
//! -- resumed, with a tiebreak column
//! ... WHERE (<cur>, <tie>) > ($1::<c1>, $2::<c2>) [AND (<where>)]
//!     ORDER BY <cur>, <tie> ...
//! ```
//!
//! The row-wise comparison, rather than `cur > $1 OR (cur = $1 AND tie > $2)`,
//! is what keeps a composite index usable.
//!
//! The trailing `::text` columns are how the cursor is read back: PostgreSQL
//! renders the value and parses it again through the same cast, so no date or
//! timestamp formatting happens in this crate. `datestyle` is pinned to
//! `ISO, YMD` on every session so that rendering is deterministic.
//!
//! # Cycle boundary
//!
//! `next_batch` returning `Ok(None)` ends a drain cycle, not the source. The
//! offset is committed at the *start* of the next cycle, which is what makes
//! delivery at-least-once: a crash mid-cycle replays the cycle.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use pcs_core::error::PcsError;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Row, Statement};

use crate::config::{
    CursorMode, FieldSpec, NotifyConfig, PostgresSourceConfig, Retention, SourceMode, find_field,
};
use crate::connection::{
    Connector, PgConnection, pg_detail, quote, quote_columns, quote_qualified,
};
use crate::metrics::Instruments;
use crate::offsets::{Offset, OffsetStore};
use crate::types::{statement_columns, validate_columns};
use crate::values::{ColumnBuilder, RawValue};

/// Alias for the cursor text column, which cannot collide with a declared field
/// because `__`-prefixed names are rejected outside `cdc_logical`.
const CURSOR_ALIAS: &str = "__pcs_cursor";
/// Alias for the tiebreak text column.
const TIEBREAK_ALIAS: &str = "__pcs_tiebreak";

/// A cursor column and everything derived from it.
struct CursorColumn {
    quoted: String,
    name: String,
    cast: &'static str,
}

/// A bound cursor parameter, cast to the column's type.
///
/// The `::text` step is load-bearing: `$1::int8` alone would make PostgreSQL
/// infer the parameter itself as `int8`, and the offset is carried as text.
fn bind(index: usize, cast: &'static str) -> String {
    if cast == "text" {
        format!("${index}::text")
    } else {
        format!("${index}::text::{cast}")
    }
}

impl CursorColumn {
    fn resolve(what: &str, fields: &[FieldSpec], name: &str) -> Result<Self, PcsError> {
        let field = find_field(fields, name).ok_or_else(|| {
            PcsError::configuration(format!(
                "{what}: '{name}' does not name a declared schema_fields entry"
            ))
        })?;
        Ok(Self {
            quoted: quote(name),
            name: name.to_string(),
            cast: field.ty.sql_cast(),
        })
    }
}

/// Incremental reader over a table ordered by a cursor column.
pub(crate) struct CursorReader {
    connector: Connector,
    connection: Option<PgConnection>,
    offsets: OffsetStore,

    /// Quoted `"schema"."table"`.
    table: String,
    /// The unquoted spelling, for error messages.
    table_display: String,
    fields: Vec<FieldSpec>,
    cursor_column: CursorColumn,
    tiebreak_column: Option<CursorColumn>,
    where_clause: Option<String>,
    initial: String,
    retention: Retention,
    batch_rows: usize,
    max_batches_per_cycle: usize,
    notify: Option<NotifyConfig>,

    /// Advances within a cycle as batches are emitted.
    offset: Option<Offset>,
    /// The position that may be persisted: the last row actually handed out.
    committed: Option<Offset>,
    /// Whether the offset table has been read on this connection.
    loaded: bool,
    /// Set when the previous call ended a cycle.
    cycle_start: bool,
    batches_this_cycle: usize,

    /// Prepared statement with no cursor predicate.
    statement_all: Option<Statement>,
    /// Prepared statement comparing the cursor alone.
    statement_cursor: Option<Statement>,
    /// Prepared statement comparing `(cursor, tiebreak)` row-wise.
    statement_pair: Option<Statement>,
    /// Server type OID per declared field, resolved on the first execution.
    column_oids: Vec<u32>,
    listening: bool,

    instruments: Instruments,
}

impl CursorReader {
    /// Build the reader from a validated config. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when the table, offset table, cursor
    /// column or connection cannot be resolved.
    pub(crate) fn new(cfg: &PostgresSourceConfig) -> Result<Self, PcsError> {
        let mode: &CursorMode = match &cfg.mode {
            SourceMode::Polling(mode) | SourceMode::CdcTrigger(mode) => mode,
            SourceMode::CdcLogical(_) => {
                return Err(PcsError::configuration(
                    "PostgresSource: CursorReader cannot serve mode kind = \"cdc_logical\"",
                ));
            }
        };
        let what = "PostgresSource";

        let tiebreak_column = match &mode.tiebreak_column {
            Some(name) => Some(CursorColumn::resolve(what, &cfg.schema_fields, name)?),
            None => None,
        };

        Ok(Self {
            connector: Connector::new(what, &cfg.connection)?,
            connection: None,
            offsets: OffsetStore::new(
                what,
                &mode.offset_table,
                &cfg.name,
                mode.offset_table_autocreate,
            )?,
            table: quote_qualified(what, &mode.table)?,
            table_display: mode.table.clone(),
            fields: cfg.schema_fields.clone(),
            cursor_column: CursorColumn::resolve(what, &cfg.schema_fields, &mode.cursor_column)?,
            tiebreak_column,
            where_clause: mode.where_clause.clone(),
            initial: mode.initial.clone(),
            retention: mode.retention,
            batch_rows: cfg.batch_rows,
            max_batches_per_cycle: cfg.max_batches_per_cycle,
            notify: cfg.notify.clone(),
            offset: None,
            committed: None,
            loaded: false,
            cycle_start: false,
            batches_this_cycle: 0,
            statement_all: None,
            statement_cursor: None,
            statement_pair: None,
            column_oids: Vec::new(),
            listening: false,
            instruments: Instruments::source(&cfg.name, cfg.mode.label()),
        })
    }

    /// Pull the next batch, or `Ok(None)` at the end of a drain cycle.
    pub(crate) async fn next_batch(
        &mut self,
        schema: &Arc<Schema>,
    ) -> Result<Option<RecordBatch>, PcsError> {
        self.ensure_session().await?;

        if self.cycle_start {
            self.commit().await?;
            self.cycle_start = false;
            self.batches_this_cycle = 0;
        }

        if self.max_batches_per_cycle > 0 && self.batches_this_cycle >= self.max_batches_per_cycle {
            self.cycle_start = true;
            return Ok(None);
        }

        loop {
            // A notification queued now refers to data the query below will
            // already see, so consuming it as "new" would only cost a redundant
            // round trip after this batch.
            if self.notify.is_some() {
                let connection = self.connection.as_mut().expect("session established");
                while connection.try_notification().is_some() {}
            }

            let started = Instant::now();
            let rows = self.query().await?;
            self.instruments.observe(started.elapsed().as_secs_f64());

            if !rows.is_empty() {
                let batch = self.build_batch(schema, &rows)?;
                self.batches_this_cycle += 1;
                self.instruments.batch(batch.num_rows() as u64);
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    table = %self.table_display,
                    rows = batch.num_rows(),
                    elapsed_us = started.elapsed().as_micros(),
                    "postgres cursor batch"
                );
                return Ok(Some(batch));
            }

            let Some(notify) = self.notify.clone() else {
                self.cycle_start = true;
                return Ok(None);
            };

            if !self.wait_for_notification(&notify).await? {
                self.cycle_start = true;
                return Ok(None);
            }
        }
    }

    /// Connect if needed, pin the session settings, and load the offset once.
    async fn ensure_session(&mut self) -> Result<(), PcsError> {
        if self
            .connection
            .as_ref()
            .is_some_and(|connection| !connection.is_closed())
        {
            return Ok(());
        }

        if self.connection.is_some() {
            // A reconnect invalidates prepared statements, the LISTEN
            // registration and the offset table check.
            self.statement_all = None;
            self.statement_cursor = None;
            self.statement_pair = None;
            self.column_oids.clear();
            self.listening = false;
            self.loaded = false;
            self.offsets.reset();
        }

        let connection = match self.connector.connect_with_retry().await {
            Ok(connection) => connection,
            Err(e) => {
                self.instruments.error("connect");
                return Err(e);
            }
        };

        // Pin the text rendering the cursor round-trips through, so a server
        // with a non-ISO DateStyle cannot make a date cursor ambiguous.
        connection
            .client()
            .batch_execute("SET datestyle = 'ISO, YMD'")
            .await
            .map_err(|e| {
                self.instruments.error("connect");
                PcsError::generic(format!(
                    "PostgresSource: cannot set datestyle on {}: {}",
                    self.connector.target(),
                    pg_detail(&e)
                ))
            })?;

        self.connection = Some(connection);

        let connection = self.connection.as_ref().expect("just assigned");
        if let Err(e) = self.offsets.ensure(connection.client()).await {
            self.instruments.error("offset");
            return Err(e);
        }

        if !self.loaded {
            self.offset = match self.offsets.load(connection.client()).await {
                Ok(offset) => offset,
                Err(e) => {
                    self.instruments.error("offset");
                    return Err(e);
                }
            };
            if self.offset.is_none() {
                self.offset = self.resolve_initial().await?;
            }
            self.committed = self.offset.clone();
            self.loaded = true;
            #[cfg(feature = "tracing")]
            tracing::info!(
                table = %self.table_display,
                cursor = ?self.offset,
                "postgres cursor source resumed"
            );
        }

        Ok(())
    }

    /// Apply `initial` when the offset table holds nothing for this source.
    async fn resolve_initial(&mut self) -> Result<Option<Offset>, PcsError> {
        match self.initial.as_str() {
            "beginning" => Ok(None),
            "now" => {
                let mut select = format!("SELECT {}::text", self.cursor_column.quoted);
                if let Some(tiebreak) = &self.tiebreak_column {
                    select.push_str(&format!(", {}::text", tiebreak.quoted));
                }
                select.push_str(&format!(" FROM {}", self.table));
                if let Some(predicate) = &self.where_clause {
                    select.push_str(&format!(" WHERE ({predicate})"));
                }
                select.push_str(&format!(" ORDER BY {} DESC", self.cursor_column.quoted));
                if let Some(tiebreak) = &self.tiebreak_column {
                    select.push_str(&format!(", {} DESC", tiebreak.quoted));
                }
                select.push_str(" LIMIT 1");

                let client = self.connection.as_ref().expect("session").client();
                let row = client.query_opt(&select, &[]).await.map_err(|e| {
                    self.instruments.error("query");
                    PcsError::generic(format!(
                        "PostgresSource: cannot read the current cursor maximum of '{}': {}",
                        self.table_display,
                        pg_detail(&e)
                    ))
                })?;
                Ok(row.and_then(|row| {
                    let cursor: Option<String> = row.get(0);
                    cursor.map(|cursor| Offset {
                        cursor,
                        tiebreak: if self.tiebreak_column.is_some() {
                            row.get(1)
                        } else {
                            None
                        },
                    })
                }))
            }
            literal => Ok(Some(Offset {
                cursor: literal.to_string(),
                tiebreak: None,
            })),
        }
    }

    /// Persist the committed offset and, for `delete_acked`, prune the outbox.
    async fn commit(&mut self) -> Result<(), PcsError> {
        let Some(offset) = self.committed.clone() else {
            return Ok(());
        };
        let client = self.connection.as_ref().expect("session").client();

        if let Err(e) = self.offsets.store(client, &offset).await {
            self.instruments.error("offset");
            return Err(e);
        }

        if self.retention == Retention::DeleteAcked {
            let (sql, params) = self.delete_statement(&offset);
            let bound: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            let deleted = client.execute(&sql, &bound).await.map_err(|e| {
                self.instruments.error("query");
                PcsError::generic(format!(
                    "PostgresSource: cannot prune acknowledged rows from '{}': {}",
                    self.table_display,
                    pg_detail(&e)
                ))
            })?;
            #[cfg(feature = "tracing")]
            tracing::debug!(
                table = %self.table_display,
                deleted,
                "postgres outbox rows pruned"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = deleted;
        }

        Ok(())
    }

    /// `DELETE` everything at or before `offset`, matching the read ordering.
    fn delete_statement(&self, offset: &Offset) -> (String, Vec<String>) {
        match (&self.tiebreak_column, &offset.tiebreak) {
            (Some(tiebreak), Some(value)) => (
                format!(
                    "DELETE FROM {} WHERE ({}, {}) <= ({}, {})",
                    self.table,
                    self.cursor_column.quoted,
                    tiebreak.quoted,
                    bind(1, self.cursor_column.cast),
                    bind(2, tiebreak.cast)
                ),
                vec![offset.cursor.clone(), value.clone()],
            ),
            _ => (
                format!(
                    "DELETE FROM {} WHERE {} <= {}",
                    self.table,
                    self.cursor_column.quoted,
                    bind(1, self.cursor_column.cast)
                ),
                vec![offset.cursor.clone()],
            ),
        }
    }

    /// Run whichever prepared statement the current offset selects.
    async fn query(&mut self) -> Result<Vec<Row>, PcsError> {
        let shape = self.shape();
        self.prepare(shape).await?;

        let client = self.connection.as_ref().expect("session").client();
        let statement = match shape {
            Shape::All => self.statement_all.as_ref(),
            Shape::Cursor => self.statement_cursor.as_ref(),
            Shape::Pair => self.statement_pair.as_ref(),
        }
        .expect("prepared above");

        let offset = self.offset.clone();
        let params: Vec<&(dyn ToSql + Sync)> = match (shape, &offset) {
            (Shape::All, _) => Vec::new(),
            (Shape::Cursor, Some(offset)) => vec![&offset.cursor],
            (Shape::Pair, Some(offset)) => {
                vec![
                    &offset.cursor,
                    offset.tiebreak.as_ref().expect("pair shape"),
                ]
            }
            _ => Vec::new(),
        };

        client.query(statement, &params).await.map_err(|e| {
            self.instruments.error("query");
            PcsError::generic(format!(
                "PostgresSource: cannot read '{}': {}",
                self.table_display,
                pg_detail(&e)
            ))
        })
    }

    /// Which statement the current offset needs.
    fn shape(&self) -> Shape {
        match (&self.offset, &self.tiebreak_column) {
            (None, _) => Shape::All,
            (Some(offset), Some(_)) if offset.tiebreak.is_some() => Shape::Pair,
            (Some(_), _) => Shape::Cursor,
        }
    }

    /// Prepare `shape` if it is not prepared yet, then check the declared schema
    /// against the statement's real result columns.
    async fn prepare(&mut self, shape: Shape) -> Result<(), PcsError> {
        let already = match shape {
            Shape::All => self.statement_all.is_some(),
            Shape::Cursor => self.statement_cursor.is_some(),
            Shape::Pair => self.statement_pair.is_some(),
        };
        if already {
            return Ok(());
        }

        let sql = self.select_sql(shape);
        let client = self.connection.as_ref().expect("session").client();
        let statement = client.prepare(&sql).await.map_err(|e| {
            self.instruments.error("query");
            PcsError::generic(format!(
                "PostgresSource: cannot prepare the read of '{}': {}",
                self.table_display,
                pg_detail(&e)
            ))
        })?;

        let columns = statement_columns(&statement);
        validate_columns("PostgresSource", &self.fields, &columns).inspect_err(|_| {
            self.instruments.error("query");
        })?;
        self.column_oids = self
            .fields
            .iter()
            .map(|spec| {
                columns
                    .iter()
                    .find(|(name, _)| *name == spec.name)
                    .map(|(_, oid)| *oid)
                    .expect("validate_columns proved every declared column exists")
            })
            .collect();

        match shape {
            Shape::All => self.statement_all = Some(statement),
            Shape::Cursor => self.statement_cursor = Some(statement),
            Shape::Pair => self.statement_pair = Some(statement),
        }
        Ok(())
    }

    /// Build the `SELECT` for `shape`.
    fn select_sql(&self, shape: Shape) -> String {
        let mut sql = format!(
            "SELECT {}, {}::text AS {}",
            quote_columns(self.fields.iter().map(|spec| spec.name.as_str())),
            self.cursor_column.quoted,
            quote(CURSOR_ALIAS)
        );
        if let Some(tiebreak) = &self.tiebreak_column {
            sql.push_str(&format!(
                ", {}::text AS {}",
                tiebreak.quoted,
                quote(TIEBREAK_ALIAS)
            ));
        }
        sql.push_str(&format!(" FROM {}", self.table));

        let predicate = match shape {
            Shape::All => None,
            Shape::Cursor => Some(format!(
                "{} > {}",
                self.cursor_column.quoted,
                bind(1, self.cursor_column.cast)
            )),
            Shape::Pair => {
                let tiebreak = self
                    .tiebreak_column
                    .as_ref()
                    .expect("pair shape needs a tiebreak column");
                Some(format!(
                    "({}, {}) > ({}, {})",
                    self.cursor_column.quoted,
                    tiebreak.quoted,
                    bind(1, self.cursor_column.cast),
                    bind(2, tiebreak.cast)
                ))
            }
        };

        match (predicate, &self.where_clause) {
            (Some(predicate), Some(extra)) => {
                sql.push_str(&format!(" WHERE {predicate} AND ({extra})"));
            }
            (Some(predicate), None) => sql.push_str(&format!(" WHERE {predicate}")),
            (None, Some(extra)) => sql.push_str(&format!(" WHERE ({extra})")),
            (None, None) => {}
        }

        sql.push_str(&format!(" ORDER BY {}", self.cursor_column.quoted));
        if let Some(tiebreak) = &self.tiebreak_column {
            sql.push_str(&format!(", {}", tiebreak.quoted));
        }
        sql.push_str(&format!(" LIMIT {}", self.batch_rows));
        sql
    }

    /// Decode `rows` into a batch and advance the in-memory cursor.
    fn build_batch(&mut self, schema: &Arc<Schema>, rows: &[Row]) -> Result<RecordBatch, PcsError> {
        let mut builders = self
            .fields
            .iter()
            .zip(&self.column_oids)
            .map(|(spec, oid)| ColumnBuilder::new(spec, rows.len(), *oid))
            .collect::<Result<Vec<_>, _>>()?;

        for row in rows {
            for (index, (spec, builder)) in self.fields.iter().zip(builders.iter_mut()).enumerate()
            {
                let raw: Option<RawValue> = row.try_get(index).map_err(|e| {
                    self.instruments.error("decode");
                    PcsError::generic(format!(
                        "PostgresSource: cannot read column '{}' of '{}': {}",
                        spec.name,
                        self.table_display,
                        pg_detail(&e)
                    ))
                })?;
                builder
                    .push(&spec.name, raw.map(|value| value.0))
                    .inspect_err(|_| self.instruments.error("decode"))?;
            }
        }

        let last = rows.last().expect("caller checked the slice is non-empty");
        let cursor_index = self.fields.len();
        let cursor: Option<String> = last.try_get(cursor_index).map_err(|e| {
            PcsError::generic(format!(
                "PostgresSource: cannot read the cursor column '{}' of '{}': {}",
                self.cursor_column.name,
                self.table_display,
                pg_detail(&e)
            ))
        })?;
        let cursor = cursor.ok_or_else(|| {
            PcsError::generic(format!(
                "PostgresSource: cursor column '{}' of '{}' is NULL; a cursor column must be NOT \
                 NULL for the source to make progress",
                self.cursor_column.name, self.table_display
            ))
        })?;
        let tiebreak = match &self.tiebreak_column {
            Some(_) => last
                .try_get::<_, Option<String>>(cursor_index + 1)
                .map_err(|e| {
                    PcsError::generic(format!(
                        "PostgresSource: cannot read the tiebreak column of '{}': {}",
                        self.table_display,
                        pg_detail(&e)
                    ))
                })?,
            None => None,
        };

        let arrays = builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(schema), arrays).map_err(|e| {
            self.instruments.error("decode");
            PcsError::generic(format!(
                "PostgresSource: cannot assemble a batch from '{}': {e}",
                self.table_display
            ))
        })?;

        let advanced = Offset { cursor, tiebreak };
        self.offset = Some(advanced.clone());
        self.committed = Some(advanced);
        Ok(batch)
    }

    /// `LISTEN` once per connection, then wait for one notification.
    ///
    /// Returns `true` when a notification arrived and the query should be
    /// retried, `false` on timeout or a dead driver task.
    async fn wait_for_notification(&mut self, notify: &NotifyConfig) -> Result<bool, PcsError> {
        if !self.listening {
            let sql = format!("LISTEN {}", quote(&notify.channel));
            let client = self.connection.as_ref().expect("session").client();
            client.batch_execute(&sql).await.map_err(|e| {
                self.instruments.error("query");
                PcsError::generic(format!(
                    "PostgresSource: cannot LISTEN on '{}': {}",
                    notify.channel,
                    pg_detail(&e)
                ))
            })?;
            self.listening = true;
        }

        let connection = self.connection.as_mut().expect("session");
        Ok(matches!(
            connection
                .next_notification(Duration::from_millis(notify.timeout_ms))
                .await,
            Some(Some(_))
        ))
    }
}

/// Which cursor predicate a statement carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// No predicate: the source has no committed offset.
    All,
    /// `cursor > $1`.
    Cursor,
    /// `(cursor, tiebreak) > ($1, $2)`.
    Pair,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pcs_connector::from_kdl_str;
    use serde::Deserialize as _;

    fn config(extra_mode: &str, fields: &str) -> PostgresSourceConfig {
        let text = format!(
            "name \"src\"\nbatch_rows 100\n\n\
             connection dsn=\"postgres://h:5432/d\" sslmode=\"disable\"\n\n\
             mode kind=\"polling\" table=\"sales.orders\" \
             cursor_column=\"updated_at\" {extra_mode}\n{fields}"
        );
        PostgresSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl")).expect("parse")
    }

    const FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"updated_at\" type=\"timestamp_micros_utc\" nullable=#false
";

    fn reader(extra_mode: &str) -> CursorReader {
        CursorReader::new(&config(extra_mode, FIELDS)).expect("reader")
    }

    #[test]
    fn the_first_query_carries_no_cursor_predicate() {
        let reader = reader("");
        let sql = reader.select_sql(Shape::All);
        assert_eq!(
            sql,
            "SELECT \"id\", \"updated_at\", \"updated_at\"::text AS \"__pcs_cursor\" \
             FROM \"sales\".\"orders\" ORDER BY \"updated_at\" LIMIT 100"
        );
    }

    #[test]
    fn a_resumed_query_compares_the_cursor_with_its_declared_cast() {
        let sql = reader("").select_sql(Shape::Cursor);
        assert!(
            sql.contains("WHERE \"updated_at\" > $1::text::timestamptz"),
            "{sql}"
        );
        assert!(sql.ends_with("ORDER BY \"updated_at\" LIMIT 100"), "{sql}");
    }

    #[test]
    fn a_tiebreak_makes_the_comparison_row_wise_and_extends_the_ordering() {
        let sql = reader("tiebreak_column=\"id\"").select_sql(Shape::Pair);
        assert!(
            sql.contains(
                "WHERE (\"updated_at\", \"id\") > ($1::text::timestamptz, $2::text::int8)"
            ),
            "{sql}"
        );
        assert!(sql.contains("ORDER BY \"updated_at\", \"id\""), "{sql}");
        assert!(sql.contains("\"id\"::text AS \"__pcs_tiebreak\""), "{sql}");
    }

    #[test]
    fn a_where_clause_is_anded_into_every_shape() {
        let reader = reader("where_clause=\"status = 'open'\"");
        assert!(
            reader
                .select_sql(Shape::All)
                .contains("WHERE (status = 'open')")
        );
        assert!(
            reader
                .select_sql(Shape::Cursor)
                .contains("WHERE \"updated_at\" > $1::text::timestamptz AND (status = 'open')")
        );
    }

    #[test]
    fn shape_follows_the_offset_and_the_tiebreak_configuration() {
        let mut reader = reader("tiebreak_column=\"id\"");
        assert_eq!(reader.shape(), Shape::All);

        // A literal `initial` has no tiebreak value yet, so the cursor-only
        // predicate is used until the first batch supplies one.
        reader.offset = Some(Offset {
            cursor: "2024-01-01".to_string(),
            tiebreak: None,
        });
        assert_eq!(reader.shape(), Shape::Cursor);

        reader.offset = Some(Offset {
            cursor: "2024-01-01".to_string(),
            tiebreak: Some("7".to_string()),
        });
        assert_eq!(reader.shape(), Shape::Pair);

        let mut plain = reader_without_tiebreak();
        plain.offset = Some(Offset {
            cursor: "1".to_string(),
            tiebreak: Some("ignored".to_string()),
        });
        assert_eq!(plain.shape(), Shape::Cursor);
    }

    fn reader_without_tiebreak() -> CursorReader {
        reader("")
    }

    #[test]
    fn delete_acked_matches_the_read_ordering() {
        let reader = reader("tiebreak_column=\"id\"");
        let (sql, params) = reader.delete_statement(&Offset {
            cursor: "2024-01-01".to_string(),
            tiebreak: Some("7".to_string()),
        });
        assert_eq!(
            sql,
            "DELETE FROM \"sales\".\"orders\" WHERE (\"updated_at\", \"id\") <= \
             ($1::text::timestamptz, $2::text::int8)"
        );
        assert_eq!(params, vec!["2024-01-01".to_string(), "7".to_string()]);

        let reader = reader_without_tiebreak();
        let (sql, params) = reader.delete_statement(&Offset {
            cursor: "2024-01-01".to_string(),
            tiebreak: None,
        });
        assert_eq!(
            sql,
            "DELETE FROM \"sales\".\"orders\" WHERE \"updated_at\" <= $1::text::timestamptz"
        );
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn an_injected_table_name_is_quoted_not_interpolated() {
        let mut cfg = config("", FIELDS);
        if let SourceMode::Polling(mode) = &mut cfg.mode {
            mode.table = "orders\"; DROP TABLE users; --".to_string();
        }
        let reader = CursorReader::new(&cfg).unwrap();
        let sql = reader.select_sql(Shape::All);
        // The embedded quote is doubled, so the whole thing stays one
        // identifier and the statement terminator never escapes it.
        assert!(
            sql.contains("FROM \"public\".\"orders\"\"; DROP TABLE users; --\" ORDER BY"),
            "{sql}"
        );
        assert!(!sql.contains("\"orders\"; DROP"), "{sql}");
    }

    #[test]
    fn cdc_logical_is_not_a_cursor_mode() {
        let cfg = PostgresSourceConfig::deserialize(
            from_kdl_str(
                "name \"s\"\n\nconnection dsn=\"postgres://h/d\" sslmode=\"disable\"\n\n\
                 mode kind=\"cdc_logical\" slot=\"s\" publication=\"p\" \
                 table=\"t\"\n\nschema_fields \"id\" type=\"int64\"\n",
            )
            .unwrap(),
        )
        .unwrap();
        let Err(err) = CursorReader::new(&cfg) else {
            panic!("cdc_logical is not a cursor mode");
        };
        assert_eq!(err.category(), "configuration");
    }
}
