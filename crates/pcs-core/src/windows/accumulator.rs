//! [`WindowAccumulator`] — durable per-window aggregate state stored as a [`Component`].
//!
//! Each row captures the running aggregate for one `(source_component, window_id, key_hash)` triple.
//! All numeric fields are nullable so future schema versions can add columns without
//! breaking existing checkpoint payloads.
//!
//! ## Schema versioning
//!
//! The `version` field is the discriminator for forward/backward compatibility:
//!
//! | Version | Meaning |
//! |---------|---------|
//! | `None`  | Reserved — treated as v1 by [`migrate_to_current`]. |
//! | `Some(1)` | Current version. All current accumulator fields present. |
//! | `Some(n > 1)` | Future binary — reject with `PcsError::configuration`. |
//!
//! When a new field is added, bump [`CURRENT_ACCUMULATOR_VERSION`] and write a
//! migration function `migrate_v{n}_to_v{n+1}(batch) -> PcsResult<RecordBatch>`.
//! Call it from [`migrate_to_current`] under the appropriate version arm.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::PcsError;
use crate::PcsResult;
use crate::component::Component;

/// The current schema version written by this binary.
pub const CURRENT_ACCUMULATOR_VERSION: u32 = 1;

/// Persistent per-window aggregate state for one `(source_component, window_id, key_hash)` group.
///
/// A [`WindowedSystem`](super::system::WindowedSystem) appends rows to this
/// component at the end of each run and reads them back at the start of the
/// next run to continue accumulation across distributed batch claims.
///
/// All numeric fields are `Option<…>` so that new columns added in later schema
/// versions are represented as Arrow nulls in older checkpoints. The `version`
/// field gates version-dispatch in [`migrate_to_current`].
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct WindowAccumulator {
    /// Schema version written by the producing binary. `None` is treated as v1.
    pub version: Option<u32>,
    /// Name of the source component this accumulator belongs to.
    /// Disambiguates rows when multiple `WindowedSystem`s run on the same pipeline.
    pub source_component: String,
    /// Tumbling/sliding window ID or session ID.
    pub window_id: i64,
    /// Per-row key hash (`0` for global / non-keyed windows).
    pub key_hash: i64,
    /// Row count accumulated so far in this window group.
    pub count: i64,
    /// Running sum (nullable; `None` when the aggregate type does not use sum).
    pub sum_f64: Option<f64>,
    /// Running minimum (nullable).
    pub min_f64: Option<f64>,
    /// Running maximum (nullable).
    pub max_f64: Option<f64>,
    /// Session start timestamp in milliseconds (nullable; only set for session windows).
    pub session_start_ts: Option<i64>,
    /// Session end timestamp in milliseconds (nullable; only set for session windows).
    pub session_end_ts: Option<i64>,
    /// Watermark at which this window was considered finalized.
    /// `None` until a watermark is wired up.
    pub finalized_at_watermark: Option<i64>,
}

impl Component for WindowAccumulator {
    fn name() -> &'static str {
        "WindowAccumulator"
    }

    fn version() -> u32 {
        CURRENT_ACCUMULATOR_VERSION
    }

    fn migrate(from_version: u32, batch: RecordBatch) -> crate::PcsResult<RecordBatch> {
        migrate_to_current_inner(from_version, batch)
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("version", DataType::UInt32, true),
            Field::new("source_component", DataType::Utf8, false),
            Field::new("window_id", DataType::Int64, false),
            Field::new("key_hash", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
            Field::new("sum_f64", DataType::Float64, true),
            Field::new("min_f64", DataType::Float64, true),
            Field::new("max_f64", DataType::Float64, true),
            Field::new("session_start_ts", DataType::Int64, true),
            Field::new("session_end_ts", DataType::Int64, true),
            Field::new("finalized_at_watermark", DataType::Int64, true),
        ]))
    }
}

// ---------------------------------------------------------------------------
// Schema migration
// ---------------------------------------------------------------------------

/// Migrate a `RecordBatch` decoded from a checkpoint to the current schema version.
///
/// Reads the `version` column (or treats its absence as v1) and applies
/// any necessary backfill migrations in order.  Returns the batch unchanged
/// if it is already at [`CURRENT_ACCUMULATOR_VERSION`].
///
/// # Errors
///
/// Returns `PcsError::configuration` if the batch was produced by a newer
/// binary (version > [`CURRENT_ACCUMULATOR_VERSION`]).
pub fn migrate_to_current(batch: RecordBatch) -> PcsResult<RecordBatch> {
    use arrow_array::UInt32Array;

    // Determine batch version from the `version` column.
    let detected_version = if let Ok(idx) = batch.schema().index_of("version") {
        let col = batch.column(idx);
        if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
            // Take the first non-null value; None → treat as v1.
            (0..arr.len()).find_map(|i| {
                if arr.is_valid(i) {
                    Some(arr.value(i))
                } else {
                    None
                }
            })
        } else {
            None
        }
    } else {
        None
    };

    migrate_to_current_inner(detected_version.unwrap_or(1), batch)
}

/// Inner migration dispatcher used by both [`migrate_to_current`] and
/// [`Component::migrate`](crate::component::Component::migrate).
fn migrate_to_current_inner(from_version: u32, batch: RecordBatch) -> PcsResult<RecordBatch> {
    if from_version > CURRENT_ACCUMULATOR_VERSION {
        return Err(PcsError::configuration(format!(
            "WindowAccumulator checkpoint was written by a newer binary (version={from_version}); \
             upgrade pcs to read this checkpoint"
        )));
    }

    // All versions ≤ CURRENT — no migration needed for v1.
    // Future: add `if from_version < 2 { batch = migrate_v1_to_v2(batch)?; }` etc.
    Ok(batch)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray, UInt32Array};

    fn make_accumulator(
        version: Option<u32>,
        source: &str,
        wid: i64,
        kh: i64,
    ) -> WindowAccumulator {
        WindowAccumulator {
            version,
            source_component: source.to_string(),
            window_id: wid,
            key_hash: kh,
            count: 3,
            sum_f64: Some(10.5),
            min_f64: Some(1.0),
            max_f64: Some(9.5),
            session_start_ts: None,
            session_end_ts: None,
            finalized_at_watermark: None,
        }
    }

    #[test]
    fn test_schema_field_count() {
        let schema = WindowAccumulator::schema();
        assert_eq!(schema.fields().len(), 11);
    }

    #[test]
    fn test_schema_nullable_fields() {
        let schema = WindowAccumulator::schema();
        // version is nullable
        assert!(schema.field_with_name("version").unwrap().is_nullable());
        // source_component is not nullable
        assert!(
            !schema
                .field_with_name("source_component")
                .unwrap()
                .is_nullable()
        );
        // sum_f64, min_f64, max_f64, session_start_ts, session_end_ts, finalized_at_watermark are nullable
        for field_name in &[
            "sum_f64",
            "min_f64",
            "max_f64",
            "session_start_ts",
            "session_end_ts",
            "finalized_at_watermark",
        ] {
            assert!(
                schema.field_with_name(field_name).unwrap().is_nullable(),
                "{field_name} should be nullable"
            );
        }
    }

    #[test]
    fn test_round_trip_serde_arrow() {
        let rows = vec![
            make_accumulator(Some(1), "Trade", 0, 100),
            make_accumulator(Some(1), "Trade", 1, 200),
        ];

        let batch = WindowAccumulator::to_record_batch(&rows).expect("serialization failed");
        assert_eq!(batch.num_rows(), 2);

        let recovered =
            WindowAccumulator::from_record_batch(&batch).expect("deserialization failed");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].window_id, 0);
        assert_eq!(recovered[1].key_hash, 200);
        assert_eq!(recovered[0].sum_f64, Some(10.5));
    }

    #[test]
    fn test_round_trip_nullable_fields() {
        let rows = vec![WindowAccumulator {
            version: Some(1),
            source_component: "Orders".to_string(),
            window_id: 42,
            key_hash: 0,
            count: 1,
            sum_f64: None,
            min_f64: None,
            max_f64: None,
            session_start_ts: Some(1_700_000_000_000),
            session_end_ts: Some(1_700_000_030_000),
            finalized_at_watermark: None,
        }];

        let batch = WindowAccumulator::to_record_batch(&rows).unwrap();
        let recovered = WindowAccumulator::from_record_batch(&batch).unwrap();
        assert_eq!(recovered[0].sum_f64, None);
        assert_eq!(recovered[0].session_start_ts, Some(1_700_000_000_000));
        assert_eq!(recovered[0].session_end_ts, Some(1_700_000_030_000));
    }

    #[test]
    fn test_name() {
        assert_eq!(WindowAccumulator::name(), "WindowAccumulator");
    }

    // ── migrate_to_current ───────────────────────────────────────────────────

    #[test]
    fn test_migrate_v1_is_identity() {
        let rows = vec![make_accumulator(Some(1), "A", 0, 0)];
        let batch = WindowAccumulator::to_record_batch(&rows).unwrap();
        let migrated = migrate_to_current(batch.clone()).unwrap();
        assert_eq!(migrated.num_rows(), 1);
    }

    #[test]
    fn test_migrate_none_version_treated_as_v1() {
        let rows = vec![make_accumulator(None, "B", 5, 7)];
        let batch = WindowAccumulator::to_record_batch(&rows).unwrap();
        // The `version` column will be null for None → migrate_to_current should accept it.
        let migrated = migrate_to_current(batch).unwrap();
        assert_eq!(migrated.num_rows(), 1);
    }

    #[test]
    fn test_migrate_future_version_rejected() {
        // Build a batch with version = 999 directly using Arrow arrays.
        let schema = WindowAccumulator::schema();
        let n: usize = 1;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![999u32])) as _,
                Arc::new(StringArray::from(vec!["X"])) as _,
                Arc::new(Int64Array::from(vec![0i64])) as _,
                Arc::new(Int64Array::from(vec![0i64])) as _,
                Arc::new(Int64Array::from(vec![0i64])) as _,
                Arc::new(Float64Array::from(vec![None::<f64>])) as _,
                Arc::new(Float64Array::from(vec![None::<f64>])) as _,
                Arc::new(Float64Array::from(vec![None::<f64>])) as _,
                Arc::new(Int64Array::from(vec![None::<i64>])) as _,
                Arc::new(Int64Array::from(vec![None::<i64>])) as _,
                Arc::new(Int64Array::from(vec![None::<i64>])) as _,
            ],
        )
        .unwrap();
        let _ = n; // suppress unused warning

        let result = migrate_to_current(batch);
        assert!(result.is_err(), "expected config error for future version");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("newer binary"), "error message: {msg}");
    }

    #[test]
    fn test_nullable_preservation_in_batch() {
        // Verify that Option<f64> = None round-trips as a null in the Arrow batch.
        let rows = vec![make_accumulator(Some(1), "T", 0, 0)];
        let batch = WindowAccumulator::to_record_batch(&rows).unwrap();
        let idx = batch.schema().index_of("session_start_ts").unwrap();
        let col = batch.column(idx);
        // session_start_ts is None → should be null at row 0
        assert!(!col.is_valid(0), "expected null for None Option");
    }

    #[test]
    fn test_boolean_alive_column_not_present() {
        // WindowAccumulator schema should NOT have a boolean "alive" field.
        let schema = WindowAccumulator::schema();
        let has_bool = schema
            .fields()
            .iter()
            .any(|f| *f.data_type() == DataType::Boolean);
        assert!(!has_bool);
    }
}
