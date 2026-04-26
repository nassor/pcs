//! [`DataFusionSource`] — a [`Source`] backed by a DataFusion SQL query.
//!
//! Executes a SQL statement against a `SessionContext` and streams the
//! resulting [`RecordBatch`]es into a PCS pipeline.
//!
//! ## Feature gate
//!
//! This module requires the `datafusion` feature:
//!
//! ```toml
//! [features]
//! datafusion = ["io", "dep:datafusion"]
//! ```
//!
//! ## Quick start
//!
//! ```rust
//! # #[cfg(feature = "datafusion")]
//! # {
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use pcs_service::io::datafusion_source::DataFusionSource;
//! use pcs_service::io::source::Source;
//! use datafusion::prelude::SessionContext;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let ctx = SessionContext::new();
//! // Register tables and run SQL …
//! // let mut src = DataFusionSource::from_sql(&ctx, "SELECT 1 AS n").await.unwrap();
//! # }
//! # }
//! ```

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::error::PcsError;
use crate::io::source::Source;

/// A [`Source`] that streams [`RecordBatch`]es from a DataFusion SQL query.
///
/// Construct via [`DataFusionSource::from_sql`] (preferred) or
/// [`DataFusionSource::from_stream`] when you already hold a
/// [`SendableRecordBatchStream`].
///
/// The internal stream is protected by a [`Mutex`] so that the struct
/// satisfies `Sync` (required by `#[async_trait]` bounds on `Source`).
///
/// # Notes
///
/// - Batches are pulled lazily — DataFusion executes the query on the
///   executor partition holding the stream.
/// - `estimated_rows` is always `None` unless set via
///   [`DataFusionSource::with_estimated_rows`].
pub struct DataFusionSource {
    stream: Mutex<SendableRecordBatchStream>,
    schema: Arc<Schema>,
    estimated_rows: Option<usize>,
}

impl DataFusionSource {
    /// Execute a SQL query against a DataFusion `SessionContext` and wrap the
    /// resulting batch stream as a [`Source`].
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` if DataFusion fails to parse or plan the
    /// SQL, or if the execution stream cannot be obtained.
    ///
    /// # Example
    ///
    /// ```rust
    /// # #[cfg(feature = "datafusion")]
    /// # {
    /// use std::sync::Arc;
    /// use pcs_service::io::datafusion_source::DataFusionSource;
    /// use datafusion::prelude::SessionContext;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ctx = SessionContext::new();
    /// // let src = DataFusionSource::from_sql(&ctx, "SELECT 1 AS n").await.unwrap();
    /// # }
    /// # }
    /// ```
    pub async fn from_sql(
        ctx: &datafusion::prelude::SessionContext,
        sql: &str,
    ) -> Result<Self, PcsError> {
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| PcsError::generic(format!("DataFusionSource: SQL error: {e}")))?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let stream = df.execute_stream().await.map_err(|e| {
            PcsError::generic(format!("DataFusionSource: execute_stream error: {e}"))
        })?;
        Ok(Self {
            stream: Mutex::new(stream),
            schema,
            estimated_rows: None,
        })
    }

    /// Wrap an existing [`SendableRecordBatchStream`] as a [`Source`].
    ///
    /// Useful when you already have a stream — for example, from a Parquet
    /// scan inside DataFusion, or from a custom execution plan.
    pub fn from_stream(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        Self {
            stream: Mutex::new(stream),
            schema,
            estimated_rows: None,
        }
    }

    /// Set an estimated row count for progress reporting.
    ///
    /// DataFusion does not always expose row counts before execution, so this
    /// is an optional hint. Callers can supply it when they know the approximate
    /// size of the result set.
    pub fn with_estimated_rows(mut self, rows: usize) -> Self {
        self.estimated_rows = Some(rows);
        self
    }
}

#[async_trait]
impl Source for DataFusionSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        let mut stream = self.stream.lock().await;
        match stream.next().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(PcsError::generic(format!(
                "DataFusionSource: stream error: {e}"
            ))),
            None => Ok(None),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "datafusion"))]
mod tests {
    use super::*;
    use crate::io::source::drain_into_dataset;
    use crate::pipeline::Dataset;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_batch(schema: Arc<Schema>, ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn build_ctx_with_table(batch: RecordBatch) -> SessionContext {
        let ctx = SessionContext::new();
        let schema = batch.schema();
        let provider =
            MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable creation failed");
        ctx.register_table("test_table", Arc::new(provider))
            .expect("register_table failed");
        ctx
    }

    // Test 1: SQL query against in-memory table — verify batches and schema.
    #[tokio::test]
    async fn test_sql_query_against_in_memory_table() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[1, 2, 3], &["alice", "bob", "carol"]);
        let ctx = build_ctx_with_table(batch);

        let mut src = DataFusionSource::from_sql(&ctx, "SELECT id, name FROM test_table")
            .await
            .unwrap();

        // Schema must match.
        assert_eq!(src.schema().fields().len(), 2);
        assert_eq!(src.schema().field(0).name(), "id");
        assert_eq!(src.schema().field(1).name(), "name");

        // Drain all batches and count rows.
        let mut total_rows = 0;
        while let Some(batch) = src.next_batch().await.unwrap() {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3);
    }

    // Test 2: Filter + projection — schema matches selection, rows are filtered.
    #[tokio::test]
    async fn test_filter_and_projection() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
        let ctx = build_ctx_with_table(batch);

        let mut src = DataFusionSource::from_sql(&ctx, "SELECT id FROM test_table WHERE id > 2")
            .await
            .unwrap();

        // Projected schema has only "id".
        assert_eq!(src.schema().fields().len(), 1);
        assert_eq!(src.schema().field(0).name(), "id");

        // Rows with id > 2: ids 3, 4, 5 => 3 rows.
        let mut total_rows = 0;
        let mut ids_seen: Vec<i32> = Vec::new();
        while let Some(batch) = src.next_batch().await.unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..col.len() {
                ids_seen.push(col.value(i));
            }
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3);
        // All returned ids must be > 2.
        for id in &ids_seen {
            assert!(*id > 2, "id {id} should be > 2");
        }
    }

    // Test 3: Drain into Pipeline — verify component row count.
    #[tokio::test]
    async fn test_drain_into_pipeline() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[10, 20, 30, 40], &["w", "x", "y", "z"]);
        let ctx = build_ctx_with_table(batch);

        let mut src = DataFusionSource::from_sql(&ctx, "SELECT id, name FROM test_table")
            .await
            .unwrap();

        let src_schema = src.schema();
        let mut dataset = Dataset::new();
        dataset.register_raw_component("records", src_schema);

        let rows: usize = drain_into_dataset(&mut src, &mut dataset, "records")
            .await
            .unwrap();

        assert_eq!(rows, 4);
        assert_eq!(dataset.rows(), 4);
    }

    // Test 4: Empty result — source returns None on first next_batch.
    #[tokio::test]
    async fn test_empty_result_returns_none() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[1, 2], &["a", "b"]);
        let ctx = build_ctx_with_table(batch);

        // WHERE clause that matches nothing.
        let mut src = DataFusionSource::from_sql(&ctx, "SELECT id FROM test_table WHERE id > 9999")
            .await
            .unwrap();

        let first = src.next_batch().await.unwrap();
        assert!(
            first.is_none(),
            "expected None for empty result set, got Some"
        );
    }

    // Test 5: Invalid SQL returns an error from from_sql (not a panic).
    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let ctx = SessionContext::new();
        let result = DataFusionSource::from_sql(&ctx, "SELECT FROM WHERE totally invalid;;;").await;
        assert!(result.is_err(), "expected error for invalid SQL");
        let err = result.err().unwrap();
        let msg = err.message();
        // The error message should come from DataFusion, not be empty.
        assert!(!msg.is_empty(), "error message should be non-empty");
    }

    // Test 6: from_stream wraps a stream correctly.
    #[tokio::test]
    async fn test_from_stream_wraps_correctly() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[100, 200], &["foo", "bar"]);
        let ctx = build_ctx_with_table(batch);

        // Get the stream from DataFusion directly.
        let df = ctx.sql("SELECT id, name FROM test_table").await.unwrap();
        let stream = df.execute_stream().await.unwrap();

        let mut src = DataFusionSource::from_stream(stream);
        assert_eq!(src.schema().fields().len(), 2);

        let mut total = 0;
        while let Some(batch) = src.next_batch().await.unwrap() {
            total += batch.num_rows();
        }
        assert_eq!(total, 2);
    }

    // Test 7: with_estimated_rows propagates.
    #[tokio::test]
    async fn test_estimated_rows_propagates() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[1], &["x"]);
        let ctx = build_ctx_with_table(batch);

        let src = DataFusionSource::from_sql(&ctx, "SELECT id FROM test_table")
            .await
            .unwrap()
            .with_estimated_rows(42);

        assert_eq!(src.estimated_rows(), Some(42));
    }
}
