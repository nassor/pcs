//! [`PostgresSink`]: bulk load through `COPY … WITH (FORMAT binary)`.
//!
//! One flush is one transaction, so a pipeline iteration lands atomically
//! downstream. `write_mode = "append"` copies straight into the target;
//! `"upsert"` and `"ignore_conflicts"` copy into an `ON COMMIT DROP` temp table
//! and then merge, which means there is no orphaned-staging-table failure mode.
//!
//! [`new`](PostgresSink::new) opens no connection, mirroring
//! [`PostgresSource::new`](crate::source::PostgresSource::new): the first
//! [`write_batch`](Sink::write_batch) connects, reads the target's real column
//! types from `pg_attribute`, and checks them against the declared schema.
//!
//! `BinaryCopyInWriter` owns the PGCOPY framing — the magic header, the flags,
//! the per-row field count and the length prefixes — so this module only
//! supplies one `ToSql` value per column. See [`crate::encode`].

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use pcs_core::error::PcsError;
use pcs_core::io::sink::Sink;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;

use crate::config::{FieldSpec, PostgresSinkConfig, WriteMode, split_qualified};
use crate::connection::{
    Connector, PgConnection, pg_detail, quote, quote_columns, quote_qualified,
};
use crate::encode::{PgValue, resolve_columns, row_values};
use crate::metrics::Instruments;
use crate::types::validate_columns;

/// A PostgreSQL [`Sink`].
pub struct PostgresSink {
    connector: Connector,
    connection: Option<PgConnection>,

    schema: Arc<Schema>,
    fields: Vec<FieldSpec>,
    /// Quoted `"schema"."table"`.
    table: String,
    /// The unquoted spelling, for error messages.
    table_display: String,
    /// Schema and table parts, for the `pg_attribute` lookup.
    namespace: String,
    relation: String,
    /// Quoted name of the staging table used by the merge modes.
    stage: String,

    write_mode: WriteMode,
    conflict_columns: Vec<String>,
    /// Resolved at construction: `update_columns` when given, every declared
    /// non-conflict column otherwise.
    update_columns: Vec<String>,
    dedupe_order_column: Option<String>,
    chunk_rows: usize,
    flush_rows: usize,
    truncate_before_first_write: bool,

    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    first_write_done: bool,
    /// Target column types in declared order, resolved on the first flush.
    target_types: Vec<Type>,

    instruments: Instruments,
}

impl PostgresSink {
    /// Validate `cfg` and prepare the sink. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] for any violation
    /// [`PostgresSinkConfig::validate`] reports, for a DSN that does not parse,
    /// for an unreadable `password_file`, and for a TLS configuration that
    /// cannot be built.
    pub fn new(cfg: PostgresSinkConfig) -> Result<Self, PcsError> {
        cfg.validate()?;

        let what = "PostgresSink";
        let fields = cfg
            .schema_fields
            .iter()
            .map(|spec| spec.to_arrow_field())
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));
        let (namespace, relation) = split_qualified(what, &cfg.table)?;
        let update_columns = cfg
            .effective_update_columns()
            .into_iter()
            .map(str::to_string)
            .collect();

        let connector = Connector::new(what, &cfg.connection)?;

        #[cfg(feature = "tracing")]
        tracing::info!(
            sink = %cfg.name,
            target_db = %connector.target(),
            table = %cfg.table,
            columns = cfg.schema_fields.len(),
            "postgres sink configured"
        );

        Ok(Self {
            connector,
            connection: None,
            schema,
            fields: cfg.schema_fields.clone(),
            table: quote_qualified(what, &cfg.table)?,
            table_display: cfg.table.clone(),
            namespace,
            relation,
            // `name` is already restricted to [A-Za-z0-9_-] by validation, and
            // '-' is not legal unquoted, so it is folded to '_' before quoting.
            stage: quote(&format!("pcs_stage_{}", cfg.name.replace('-', "_"))),
            write_mode: cfg.write_mode,
            conflict_columns: cfg.conflict_columns.clone(),
            update_columns,
            dedupe_order_column: cfg.dedupe_order_column.clone(),
            chunk_rows: cfg.chunk_rows,
            flush_rows: cfg.flush_rows,
            truncate_before_first_write: cfg.truncate_before_first_write,
            buffer: Vec::new(),
            buffered_rows: 0,
            first_write_done: false,
            target_types: Vec::new(),
            instruments: Instruments::sink(&cfg.name),
        })
    }

    /// Connect if needed and resolve the target's real column types once.
    async fn ensure_session(&mut self) -> Result<(), PcsError> {
        if self
            .connection
            .as_ref()
            .is_some_and(|connection| !connection.is_closed())
        {
            // A failed catalog check leaves `target_types` empty on an open
            // connection. Retry it here, so a missing or invisible table keeps
            // reporting the configuration error that names it instead of failing
            // deeper, as a bare 42P01 from the staging DDL or the COPY.
            if self.target_types.is_empty() {
                self.resolve_target_types().await?;
            }
            return Ok(());
        }

        if self.connection.is_some() {
            // A new session may be a different server, so the catalog check and
            // the one-shot TRUNCATE both apply again.
            self.first_write_done = false;
            self.target_types.clear();
        }

        self.connection = Some(match self.connector.connect_with_retry().await {
            Ok(connection) => connection,
            Err(e) => {
                self.instruments.error("connect");
                return Err(e);
            }
        });

        if self.target_types.is_empty() {
            self.resolve_target_types().await?;
        }
        Ok(())
    }

    /// Read the target's columns from the catalog and check the declared schema.
    async fn resolve_target_types(&mut self) -> Result<(), PcsError> {
        let client = self.connection.as_ref().expect("session").client();
        let rows = client
            .query(
                "SELECT a.attname, a.atttypid \
                 FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 \
                 AND NOT a.attisdropped \
                 ORDER BY a.attnum",
                &[&self.namespace, &self.relation],
            )
            .await
            .map_err(|e| {
                self.instruments.error("query");
                PcsError::generic(format!(
                    "PostgresSink: cannot read the columns of '{}': {}",
                    self.table_display,
                    pg_detail(&e)
                ))
            })?;

        if rows.is_empty() {
            self.instruments.error("query");
            return Err(PcsError::configuration(format!(
                "PostgresSink: table '{}' does not exist on {}, or the connecting role cannot see \
                 it",
                self.table_display,
                self.connector.target()
            )));
        }

        let actual: Vec<(String, u32)> = rows
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, u32>(1)))
            .collect();
        validate_columns("PostgresSink", &self.fields, &actual).inspect_err(|_| {
            self.instruments.error("query");
        })?;

        self.target_types = self
            .fields
            .iter()
            .map(|spec| {
                let oid = actual
                    .iter()
                    .find(|(name, _)| *name == spec.name)
                    .map(|(_, oid)| *oid)
                    .expect("validate_columns proved every declared column exists");
                // BinaryCopyInWriter frames each value against a concrete Type,
                // so a domain, enum or composite column has no encoding here.
                Type::from_oid(oid).ok_or_else(|| {
                    PcsError::configuration(format!(
                        "PostgresSink: column '{}' of '{}' has non-builtin type oid {oid}; \
                         COPY FORMAT binary needs a built-in type, so add a view or a cast",
                        spec.name, self.table_display
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .inspect_err(|_| self.instruments.error("query"))?;

        Ok(())
    }

    /// Write everything buffered in one transaction.
    async fn flush(&mut self) -> Result<(), PcsError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ensure_session().await?;

        let started = Instant::now();
        let staged = self.write_mode != WriteMode::Append;
        let truncate = self.truncate_before_first_write && !self.first_write_done;

        // Everything below runs against `transaction`, which rolls back on drop,
        // so an error anywhere leaves the target untouched.
        let batches = std::mem::take(&mut self.buffer);
        let rows = self.buffered_rows;
        let result = self.flush_in_transaction(&batches, staged, truncate).await;

        match result {
            Ok(()) => {
                self.buffered_rows = 0;
                self.first_write_done = true;
                self.instruments.batch(rows as u64);
                self.instruments.observe(started.elapsed().as_secs_f64());
                self.instruments.gauge(0);
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    table = %self.table_display,
                    rows,
                    batches = batches.len(),
                    elapsed_us = started.elapsed().as_micros(),
                    "postgres sink flushed"
                );
                Ok(())
            }
            Err(e) => {
                // Put the batches back so `finish` can retry after a reconnect
                // rather than silently dropping rows.
                self.buffer = batches;
                self.instruments.error("copy");
                Err(e)
            }
        }
    }

    /// The whole write, inside one transaction that rolls back on drop.
    ///
    /// Every statement is built before the connection is borrowed mutably, and
    /// error mapping goes through free functions, so nothing here re-borrows
    /// `self` while the transaction is alive.
    async fn flush_in_transaction(
        &mut self,
        batches: &[RecordBatch],
        staged: bool,
        truncate: bool,
    ) -> Result<(), PcsError> {
        let types = self.target_types.clone();
        let copy_target = if staged { &self.stage } else { &self.table };
        let copy_sql = self.copy_sql(copy_target);
        let stage_ddl = self.stage_ddl();
        let merge_sql = self.merge_sql();
        let truncate_sql = format!("TRUNCATE {}", self.table);

        // Disjoint field borrows: `connection` is mutable, the rest immutable.
        let table = self.table_display.as_str();
        let fields = self.fields.as_slice();
        let chunk_rows = self.chunk_rows;
        let connection = self.connection.as_mut().expect("session");

        let transaction = connection.client_mut().transaction().await.map_err(|e| {
            PcsError::generic(format!(
                "PostgresSink: cannot open a transaction for '{table}': {}",
                pg_detail(&e)
            ))
        })?;

        if truncate {
            transaction
                .batch_execute(&truncate_sql)
                .await
                .map_err(|e| {
                    PcsError::generic(format!(
                        "PostgresSink: cannot truncate '{table}': {}",
                        pg_detail(&e)
                    ))
                })?;
        }

        if staged {
            transaction.batch_execute(&stage_ddl).await.map_err(|e| {
                PcsError::generic(format!(
                    "PostgresSink: cannot create the staging table for '{table}': {}",
                    pg_detail(&e)
                ))
            })?;
        }

        let mut writer: Option<Pin<Box<BinaryCopyInWriter>>> = None;
        let mut rows_in_copy = 0usize;
        let mut values: Vec<PgValue<'_>> = Vec::with_capacity(fields.len());

        for batch in batches {
            let readers = resolve_columns("PostgresSink", batch, fields)?;
            for row in 0..batch.num_rows() {
                if writer.is_none() {
                    let sink = transaction
                        .copy_in::<_, bytes::Bytes>(&copy_sql)
                        .await
                        .map_err(|e| copy_error(table, e))?;
                    writer = Some(Box::pin(BinaryCopyInWriter::new(sink, &types)));
                }
                row_values(&readers, row, &mut values);
                writer
                    .as_mut()
                    .expect("writer opened above")
                    .as_mut()
                    // `values.iter()`, not `.copied()`: a by-value iterator
                    // would need `PgValue<'_>: Copy` for every lifetime, which
                    // a higher-ranked bound cannot prove. `&T: ToSql` covers it.
                    .write_raw(values.iter())
                    .await
                    .map_err(|e| copy_error(table, e))?;

                rows_in_copy += 1;
                if rows_in_copy >= chunk_rows {
                    let mut full = writer.take().expect("writer opened above");
                    full.as_mut()
                        .finish()
                        .await
                        .map_err(|e| copy_error(table, e))?;
                    rows_in_copy = 0;
                }
            }
        }
        if let Some(mut open) = writer {
            open.as_mut()
                .finish()
                .await
                .map_err(|e| copy_error(table, e))?;
        }

        if let Some(merge_sql) = merge_sql {
            transaction
                .batch_execute(&merge_sql)
                .await
                .map_err(|e| merge_error(table, e))?;
        }

        transaction.commit().await.map_err(|e| {
            PcsError::generic(format!(
                "PostgresSink: cannot commit the write to '{table}': {}",
                pg_detail(&e)
            ))
        })
    }

    /// The declared column list, quoted.
    fn column_list(&self) -> String {
        quote_columns(self.fields.iter().map(|spec| spec.name.as_str()))
    }

    /// `COPY <target> (<cols>) FROM STDIN WITH (FORMAT binary)`.
    fn copy_sql(&self, target: &str) -> String {
        format!(
            "COPY {target} ({}) FROM STDIN WITH (FORMAT binary)",
            self.column_list()
        )
    }

    /// The staging table DDL. `ON COMMIT DROP` is what removes the failure mode
    /// of a leftover table after a crash.
    fn stage_ddl(&self) -> String {
        format!(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP",
            self.stage, self.table
        )
    }

    /// The merge statement, or `None` for `append`.
    fn merge_sql(&self) -> Option<String> {
        if self.write_mode == WriteMode::Append {
            return None;
        }
        let columns = self.column_list();
        let conflict = quote_columns(self.conflict_columns.iter().map(String::as_str));

        let select = match &self.dedupe_order_column {
            Some(order) => format!(
                "SELECT DISTINCT ON ({conflict}) {columns} FROM {} ORDER BY {conflict}, {} DESC",
                self.stage,
                quote(order)
            ),
            None => format!("SELECT {columns} FROM {}", self.stage),
        };

        let action = match self.write_mode {
            WriteMode::IgnoreConflicts => "DO NOTHING".to_string(),
            _ => {
                let assignments = self
                    .update_columns
                    .iter()
                    .map(|column| {
                        let quoted = quote(column);
                        format!("{quoted} = EXCLUDED.{quoted}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("DO UPDATE SET {assignments}")
            }
        };

        Some(format!(
            "INSERT INTO {} ({columns}) {select} ON CONFLICT ({conflict}) {action}",
            self.table
        ))
    }

    /// Reject a batch whose schema is not the declared one.
    fn check_schema(&self, batch: &RecordBatch) -> Result<(), PcsError> {
        let expected = self.schema.fields();
        let actual = batch.schema_ref().fields();
        if expected.len() != actual.len() {
            return Err(PcsError::generic(format!(
                "PostgresSink: batch has {} column(s) but '{}' declares {}",
                actual.len(),
                self.table_display,
                expected.len()
            )));
        }
        for (want, got) in expected.iter().zip(actual.iter()) {
            if want.name() != got.name() {
                return Err(PcsError::generic(format!(
                    "PostgresSink: batch column '{}' does not match the declared column '{}'; \
                     schema_fields order is the batch order",
                    got.name(),
                    want.name()
                )));
            }
            if want.data_type() != got.data_type() {
                return Err(PcsError::generic(format!(
                    "PostgresSink: batch column '{}' is {:?} but '{}' declares {:?}",
                    got.name(),
                    got.data_type(),
                    self.table_display,
                    want.data_type()
                )));
            }
        }
        Ok(())
    }
}

/// A `COPY` failure, naming the target table.
fn copy_error(table: &str, e: tokio_postgres::Error) -> PcsError {
    PcsError::generic(format!(
        "PostgresSink: COPY into '{table}' failed: {}",
        pg_detail(&e)
    ))
}

/// A merge failure. A cardinality violation means one batch repeated a conflict
/// key, which `dedupe_order_column` is what resolves.
fn merge_error(table: &str, e: tokio_postgres::Error) -> PcsError {
    if e.code() == Some(&SqlState::CARDINALITY_VIOLATION) {
        return PcsError::generic(format!(
            "PostgresSink: one batch repeats a conflict key for '{table}', so ON CONFLICT DO \
             UPDATE cannot resolve it; set dedupe_order_column to pick the winning row ({})",
            pg_detail(&e)
        ));
    }
    PcsError::generic(format!(
        "PostgresSink: cannot merge staged rows into '{table}': {}",
        pg_detail(&e)
    ))
}

#[async_trait]
impl Sink for PostgresSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.check_schema(batch)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.buffered_rows += batch.num_rows();
        self.buffer.push(batch.clone());
        self.instruments.gauge(self.buffered_rows as u64);

        if self.flush_rows == 0 || self.buffered_rows >= self.flush_rows {
            self.flush().await?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), PcsError> {
        self.flush().await
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn pending_rows(&self) -> Option<usize> {
        Some(self.buffered_rows)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    use super::*;
    use pcs_connector::from_kdl_str;
    use serde::Deserialize as _;

    const FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"label\" type=\"utf8\"
schema_fields \"seq\" type=\"int64\" nullable=#false
";

    fn sink(header: &str) -> PostgresSink {
        let text = format!(
            "name \"out\"\n{header}\n\n\
             connection dsn=\"postgres://h/d\" sslmode=\"disable\"\n{FIELDS}"
        );
        let cfg = PostgresSinkConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
            .expect("parse");
        PostgresSink::new(cfg).expect("sink")
    }

    #[test]
    fn append_copies_straight_into_the_target_and_has_no_merge() {
        let sink = sink("table \"sales.orders\"");
        assert_eq!(
            sink.copy_sql(&sink.table),
            "COPY \"sales\".\"orders\" (\"id\", \"label\", \"seq\") FROM STDIN \
             WITH (FORMAT binary)"
        );
        assert!(sink.merge_sql().is_none());
    }

    #[test]
    fn upsert_stages_then_merges_every_non_conflict_column() {
        let sink = sink("table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"");
        assert_eq!(
            sink.stage_ddl(),
            "CREATE TEMP TABLE \"pcs_stage_out\" (LIKE \"public\".\"orders\" \
             INCLUDING DEFAULTS) ON COMMIT DROP"
        );
        assert_eq!(
            sink.copy_sql(&sink.stage),
            "COPY \"pcs_stage_out\" (\"id\", \"label\", \"seq\") FROM STDIN WITH (FORMAT binary)"
        );
        assert_eq!(
            sink.merge_sql().unwrap(),
            "INSERT INTO \"public\".\"orders\" (\"id\", \"label\", \"seq\") \
             SELECT \"id\", \"label\", \"seq\" FROM \"pcs_stage_out\" \
             ON CONFLICT (\"id\") DO UPDATE SET \"label\" = EXCLUDED.\"label\", \
             \"seq\" = EXCLUDED.\"seq\""
        );
    }

    #[test]
    fn explicit_update_columns_narrow_the_assignment_list() {
        let sink = sink(
            "table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"\n\
             update_columns \"seq\"",
        );
        assert_eq!(
            sink.merge_sql().unwrap(),
            "INSERT INTO \"public\".\"orders\" (\"id\", \"label\", \"seq\") \
             SELECT \"id\", \"label\", \"seq\" FROM \"pcs_stage_out\" \
             ON CONFLICT (\"id\") DO UPDATE SET \"seq\" = EXCLUDED.\"seq\""
        );
    }

    #[test]
    fn dedupe_order_column_adds_distinct_on_with_a_descending_tiebreak() {
        let sink = sink(
            "table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"\n\
             dedupe_order_column \"seq\"",
        );
        let merge = sink.merge_sql().unwrap();
        assert!(
            merge.contains(
                "SELECT DISTINCT ON (\"id\") \"id\", \"label\", \"seq\" FROM \"pcs_stage_out\" \
                 ORDER BY \"id\", \"seq\" DESC"
            ),
            "{merge}"
        );
    }

    #[test]
    fn ignore_conflicts_does_nothing_on_a_collision() {
        let sink =
            sink("table \"orders\"\nwrite_mode \"ignore_conflicts\"\nconflict_columns \"id\"");
        assert!(
            sink.merge_sql()
                .unwrap()
                .ends_with("ON CONFLICT (\"id\") DO NOTHING")
        );
    }

    #[test]
    fn a_composite_conflict_key_is_quoted_column_by_column() {
        let sink = sink("table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\" \"seq\"");
        let merge = sink.merge_sql().unwrap();
        assert!(merge.contains("ON CONFLICT (\"id\", \"seq\")"), "{merge}");
        assert!(
            merge.contains("DO UPDATE SET \"label\" = EXCLUDED.\"label\""),
            "{merge}"
        );
        assert!(!merge.contains("\"seq\" = EXCLUDED"), "{merge}");
    }

    #[test]
    fn an_injected_table_name_is_quoted_not_interpolated() {
        let sink = sink("table \"my\\\"table\"");
        assert_eq!(sink.table, "\"public\".\"my\"\"table\"");
        let copy = sink.copy_sql(&sink.table);
        assert!(
            copy.starts_with("COPY \"public\".\"my\"\"table\" ("),
            "{copy}"
        );
        assert!(!copy.contains("COPY \"public\".\"my\"table\""), "{copy}");
    }

    #[test]
    fn a_hyphenated_name_becomes_a_legal_staging_identifier() {
        let sink = sink("table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"");
        assert_eq!(sink.stage, "\"pcs_stage_out\"");

        let cfg = PostgresSinkConfig::deserialize(
            from_kdl_str(&format!(
                "name \"out-2\"\ntable \"orders\"\n\n\
                 connection dsn=\"postgres://h/d\" sslmode=\"disable\"\n{FIELDS}"
            ))
            .unwrap(),
        )
        .unwrap();
        let sink = PostgresSink::new(cfg).unwrap();
        assert_eq!(sink.stage, "\"pcs_stage_out_2\"");
    }

    fn batch(fields: Vec<Field>, columns: Vec<arrow_array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[tokio::test]
    async fn a_matching_batch_is_accepted_and_buffered() {
        let mut sink = sink("table \"orders\"\nflush_rows 1000");
        let batch = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
                Field::new("seq", DataType::Int64, false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Int64Array::from(vec![10, 11])),
            ],
        );
        // flush_rows is above the batch size, so no connection is attempted.
        sink.write_batch(&batch).await.expect("buffered");
        assert_eq!(sink.pending_rows(), Some(2));
        assert_eq!(sink.buffer.len(), 1);
    }

    #[tokio::test]
    async fn an_empty_batch_buffers_nothing() {
        let mut sink = sink("table \"orders\"\nflush_rows 1000");
        let batch = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
                Field::new("seq", DataType::Int64, false),
            ],
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        );
        sink.write_batch(&batch).await.expect("accepted");
        assert_eq!(sink.pending_rows(), Some(0));
        assert!(sink.buffer.is_empty());
    }

    #[tokio::test]
    async fn a_renamed_column_is_rejected_by_name() {
        let mut sink = sink("table \"orders\"");
        let batch = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("seq", DataType::Int64, false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        );
        let err = sink.write_batch(&batch).await.unwrap_err();
        assert!(err.message().contains("'name'"), "{}", err.message());
        assert!(err.message().contains("'label'"), "{}", err.message());
    }

    #[tokio::test]
    async fn a_retyped_column_is_rejected_by_type() {
        let mut sink = sink("table \"orders\"");
        let batch = batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
                Field::new("seq", DataType::Int64, false),
            ],
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        );
        let err = sink.write_batch(&batch).await.unwrap_err();
        assert!(err.message().contains("Int32"), "{}", err.message());
        assert!(err.message().contains("Int64"), "{}", err.message());
    }

    #[tokio::test]
    async fn a_column_count_mismatch_is_rejected() {
        let mut sink = sink("table \"orders\"");
        let batch = batch(
            vec![Field::new("id", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1]))],
        );
        let err = sink.write_batch(&batch).await.unwrap_err();
        assert!(err.message().contains("1 column(s)"), "{}", err.message());
        assert!(err.message().contains("declares 3"), "{}", err.message());
    }

    #[test]
    fn the_declared_schema_is_handed_out_unchanged() {
        let sink = sink("table \"orders\"");
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        // Same Arc every call: the trait requires a stable schema.
        assert!(Arc::ptr_eq(&schema, &sink.schema()));
    }
}
