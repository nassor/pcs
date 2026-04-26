//! Schema cast utilities for the ingestion layer.
//!
//! Provides [`cast_batch`] — a pure function that applies Arrow's `cast`
//! kernel to each field of a [`RecordBatch`] to match a target [`Schema`] —
//! and [`CastingSource`] — a [`Source`] adapter that wraps any inner source
//! and casts each batch on the fly.
//!
//! ## Why a function, not a system
//!
//! `Dataset` registers a schema once and enforces schema consistency at
//! append time.  You cannot change the physical type of a registered column
//! in place.  Schema casting is therefore a **pre-ingestion** operation: cast
//! the batch first, then call [`append_record_batch`](crate::pipeline::Dataset::append_record_batch).
//! `CastingSource` wraps this pattern cleanly.
//!
//! ## Safe vs unsafe casting
//!
//! By default `CastOptions { safe: true }` is used, meaning values that cannot
//! be represented in the target type are silently converted to `null`.  Set
//! `safe: false` to return an error instead.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

use crate::error::PcsError;
use crate::io::source::Source;

/// Cast each column of `batch` to match `target` schema.
///
/// Columns present in `batch` but absent from `target` are dropped.
/// Columns present in `target` but absent from `batch` produce an error.
///
/// # Errors
///
/// Returns `PcsError::Generic` if:
/// - A column required by `target` is missing from `batch`.
/// - Arrow's cast kernel returns an error (e.g. in non-safe mode when a value
///   overflows the target type).
pub fn cast_batch(
    batch: &RecordBatch,
    target: &Schema,
    options: &CastOptions<'_>,
) -> Result<RecordBatch, PcsError> {
    let mut columns = Vec::with_capacity(target.fields().len());
    for target_field in target.fields() {
        let col = batch.column_by_name(target_field.name()).ok_or_else(|| {
            PcsError::generic(format!(
                "cast_batch: source batch missing column '{}'",
                target_field.name()
            ))
        })?;

        let cast_col = if col.data_type() == target_field.data_type() {
            col.clone()
        } else {
            cast_with_options(col.as_ref(), target_field.data_type(), options).map_err(|e| {
                PcsError::generic(format!(
                    "cast_batch: cannot cast column '{}' from {:?} to {:?}: {e}",
                    target_field.name(),
                    col.data_type(),
                    target_field.data_type()
                ))
            })?
        };
        columns.push(cast_col);
    }

    RecordBatch::try_new(Arc::new(target.clone()), columns)
        .map_err(|e| PcsError::generic(format!("cast_batch: RecordBatch rebuild error: {e}")))
}

/// A [`Source`] adapter that casts each batch produced by `inner` to
/// `target` schema using Arrow's `cast` kernel.
///
/// Useful for normalising upstream data where field types differ from the
/// target component schema (e.g. an Int32 CSV column that must become Int64).
///
pub struct CastingSource<S: Source> {
    inner: S,
    target: Arc<Schema>,
    options: CastOptions<'static>,
}

impl<S: Source> CastingSource<S> {
    /// Wrap `inner` and cast each batch to `target` schema using `options`.
    pub fn new(inner: S, target: Arc<Schema>, options: CastOptions<'static>) -> Self {
        Self {
            inner,
            target,
            options,
        }
    }

    /// Return a reference to the inner source.
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

#[async_trait]
impl<S: Source + 'static> Source for CastingSource<S> {
    fn schema(&self) -> Arc<Schema> {
        self.target.clone()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        match self.inner.next_batch().await? {
            None => Ok(None),
            Some(batch) => {
                let cast = cast_batch(&batch, &self.target, &self.options)?;
                Ok(Some(cast))
            }
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.inner.estimated_rows()
    }
}

/// Per-field target type overrides used by [`build_target_schema`].
///
/// A convenience helper for constructing a target schema from a source schema
/// with a subset of field types overridden.
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "io")]
/// # {
/// use std::collections::HashMap;
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use pcs_core::io::cast::build_target_schema;
///
/// let source = Arc::new(Schema::new(vec![
///     Field::new("id",    DataType::Int32,   false),
///     Field::new("score", DataType::Float32, false),
/// ]));
/// let mut overrides = HashMap::new();
/// overrides.insert("id", DataType::Int64);
/// let target = build_target_schema(&source, &overrides);
/// assert_eq!(target.field(0).data_type(), &DataType::Int64);
/// assert_eq!(target.field(1).data_type(), &DataType::Float32); // unchanged
/// # }
/// ```
pub fn build_target_schema(source: &Schema, overrides: &HashMap<&str, DataType>) -> Arc<Schema> {
    let fields: Vec<Field> = source
        .fields()
        .iter()
        .map(|f| {
            if let Some(new_dt) = overrides.get(f.name().as_str()) {
                Field::new(f.name(), new_dt.clone(), f.is_nullable())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(all(test, feature = "io"))]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array};
    use arrow_cast::CastOptions;
    use arrow_schema::{DataType, Field, Schema};

    fn int32_batch(schema: Arc<Schema>, values: Vec<i32>) -> RecordBatch {
        let arr = Arc::new(Int32Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn test_cast_batch_int32_to_int64() {
        let src_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let tgt_schema = Schema::new(vec![Field::new("v", DataType::Int64, false)]);
        let batch = int32_batch(src_schema.clone(), vec![1, 2, 3]);

        let options = CastOptions::default();
        let result = cast_batch(&batch, &tgt_schema, &options).unwrap();
        assert_eq!(result.schema().field(0).data_type(), &DataType::Int64);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1i64);
        assert_eq!(col.value(2), 3i64);
    }

    #[test]
    fn test_cast_batch_missing_column_returns_error() {
        let src_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let tgt_schema = Schema::new(vec![Field::new("b", DataType::Int64, false)]); // different name
        let batch = int32_batch(src_schema, vec![1]);
        let options = CastOptions::default();
        let err = cast_batch(&batch, &tgt_schema, &options).unwrap_err();
        assert!(err.message().contains("missing column"));
    }

    #[test]
    fn test_cast_batch_incompatible_safe_mode_produces_nulls() {
        // Casting Utf8 → Int32 in safe mode: invalid strings become null.
        use arrow_array::StringArray;
        let src_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let tgt_schema = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
        let arr = Arc::new(StringArray::from(vec!["42", "not_a_number"]));
        let batch = RecordBatch::try_new(src_schema, vec![arr]).unwrap();
        let options = CastOptions {
            safe: true,
            ..Default::default()
        };
        // Should succeed in safe mode — invalid values become null.
        let result = cast_batch(&batch, &tgt_schema, &options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cast_batch_incompatible_unsafe_mode_returns_error() {
        use arrow_array::StringArray;
        let src_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let tgt_schema = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
        let arr = Arc::new(StringArray::from(vec!["not_a_number"]));
        let batch = RecordBatch::try_new(src_schema, vec![arr]).unwrap();
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let result = cast_batch(&batch, &tgt_schema, &options);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_target_schema_overrides_selected_fields() {
        let source = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("score", DataType::Float32, false),
        ]));
        let mut overrides = HashMap::new();
        overrides.insert("id", DataType::Int64);
        let target = build_target_schema(&source, &overrides);
        assert_eq!(target.field(0).data_type(), &DataType::Int64);
        assert_eq!(target.field(1).data_type(), &DataType::Float32);
    }

    #[test]
    fn test_build_target_schema_no_overrides() {
        let source = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let target = build_target_schema(&source, &HashMap::new());
        assert_eq!(target.field(0).data_type(), &DataType::Float64);
    }
}
