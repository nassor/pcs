//! Host-side watermark tracking for windowed processor nodes.
//!
//! A processor node whose config declares a `window` block receives the
//! merged rows of every inbound link; the windowing and merging *logic* lives
//! in the processor or plugin itself, but the event-time watermark is a host
//! concern: the host is the only side that sees every stream feeding the node,
//! and the dashboard needs one number per node. [`WindowTracker`] advances a
//! monotonic watermark from the `time_field` column of the node's merged
//! dataset, exposes it to an in-process runtime as a
//! `pcs_core::windows::WindowWatermark` resource, and reports it as the
//! `pcs_window_watermark_seconds` series.
//!
//! The watermark is the maximum event timestamp observed so far across all
//! inbound rows — the same rule `pcs_core::windows::WatermarkState` applies
//! inside a native pipeline, so the host number and a guest's own number
//! cannot drift. Allowed lateness is not subtracted: it is the guest's budget
//! for re-firing, not a completeness threshold.

use pcs_core::PcsResult;
use pcs_core::dataset::Dataset;
use pcs_core::error::PcsError;

use super::config::WindowConfig;

/// Monotonic event-time watermark for one windowed processor node.
///
/// The tracker owns the node's [`WindowConfig`] so a runner can build one per
/// windowed node up front and read the declaration back for the topology or
/// the resource it inserts. The watermark starts at `i64::MIN` — "nothing
/// observed yet" — and only ever moves forward.
#[derive(Debug, Clone)]
pub struct WindowTracker {
    config: WindowConfig,
    watermark_ms: i64,
}

impl WindowTracker {
    /// Create a tracker for `config`, before any data has been observed.
    pub fn new(config: WindowConfig) -> Self {
        Self {
            config,
            watermark_ms: i64::MIN,
        }
    }

    /// The declaration this tracker honours.
    pub fn config(&self) -> &WindowConfig {
        &self.config
    }

    /// The current watermark in milliseconds since the Unix epoch, or
    /// `i64::MIN` when no timestamp has been observed yet.
    pub fn watermark_ms(&self) -> i64 {
        self.watermark_ms
    }

    /// The current watermark as fractional epoch seconds.
    pub fn watermark_seconds(&self) -> f64 {
        self.watermark_ms as f64 / 1000.0
    }

    /// Whether any timestamp has been observed yet.
    pub fn has_watermark(&self) -> bool {
        self.watermark_ms != i64::MIN
    }

    /// Advance the watermark from every row of every component in `dataset`
    /// that carries the configured `time_field`.
    ///
    /// The merged dataset holds one batch per component; a component whose
    /// schema lacks the time field is skipped (load-time validation requires
    /// every *delivered* component to carry it, but a processor may declare
    /// extra components of its own). Null timestamps are skipped, mirroring
    /// `WindowedSystem`'s null-timestamp handling.
    ///
    /// # Errors
    ///
    /// Returns `PcsError::Generic` when a component's time column has a type
    /// the millisecond converter cannot read.
    pub fn advance_from(&mut self, dataset: &Dataset) -> PcsResult<()> {
        let time_field = self.config.time_field.as_str();
        let names: Vec<&'static str> = dataset.schemas().iter().map(|(name, _)| *name).collect();

        for name in names {
            let Some(schema) = dataset.schemas().get(name) else {
                continue;
            };
            if !schema.fields().iter().any(|f| f.name() == time_field) {
                continue;
            }
            let batch = dataset
                .batch_for(name)
                .expect("registered component has a batch");
            let idx = schema
                .index_of(time_field)
                .map_err(|e| PcsError::generic(format!("WindowTracker: time field lookup: {e}")))?;
            let col = batch.column(idx);
            let time_ms = pcs_core::windows::time::to_ms_array(col)?;
            for value in time_ms.iter().flatten() {
                if value > self.watermark_ms {
                    self.watermark_ms = value;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, Int64Array, TimestampMillisecondArray};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use pcs_core::component::Component;
    use pcs_core::windows::WindowSpec;

    use super::*;

    #[derive(Serialize, Deserialize)]
    struct Trade {
        timestamp_ms: i64,
        price: f64,
    }
    impl Component for Trade {
        fn name() -> &'static str {
            "Trade"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("timestamp_ms", DataType::Int64, false),
                Field::new("price", DataType::Float64, false),
            ]))
        }
    }

    fn config() -> WindowConfig {
        WindowConfig {
            spec: WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 0,
            },
            time_field: "timestamp_ms".to_string(),
            key_fields: Vec::new(),
            allowed_lateness_ms: 0,
        }
    }

    #[test]
    fn starts_without_a_watermark() {
        let tracker = WindowTracker::new(config());
        assert!(!tracker.has_watermark());
        assert_eq!(tracker.watermark_ms(), i64::MIN);
    }

    #[test]
    fn advances_from_the_time_column_and_is_monotonic() {
        let mut dataset = Dataset::new();
        dataset.register_component::<Trade>().unwrap();
        dataset
            .append::<Trade>(&[
                Trade {
                    timestamp_ms: 1_000,
                    price: 1.0,
                },
                Trade {
                    timestamp_ms: 3_000,
                    price: 2.0,
                },
            ])
            .unwrap();

        let mut tracker = WindowTracker::new(config());
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 3_000);

        // A later, smaller batch must not move the watermark backwards.
        dataset.clear();
        dataset
            .append::<Trade>(&[Trade {
                timestamp_ms: 500,
                price: 3.0,
            }])
            .unwrap();
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 3_000);

        dataset.clear();
        dataset
            .append::<Trade>(&[Trade {
                timestamp_ms: 4_500,
                price: 4.0,
            }])
            .unwrap();
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 4_500);
        assert!(tracker.has_watermark());
        assert!((tracker.watermark_seconds() - 4.5).abs() < 1e-9);
    }

    #[test]
    fn skips_components_without_the_time_field_and_null_timestamps() {
        let mut dataset = Dataset::new();
        dataset.register_component::<Trade>().unwrap();
        // A component with no time field: must be skipped, not an error.
        dataset.register_raw_component(
            "Audit",
            Arc::new(Schema::new(vec![Field::new("note", DataType::Utf8, false)])),
        );
        // A component whose time column carries nulls: the null's backing bits
        // must not advance the watermark.
        let null_schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ms",
            DataType::Int64,
            true,
        )]));
        dataset.register_raw_component("NullTrade", null_schema);

        dataset
            .append::<Trade>(&[Trade {
                timestamp_ms: 7_000,
                price: 1.0,
            }])
            .unwrap();
        let ts: ArrayRef = Arc::new(Int64Array::from(vec![Some(9_000i64), None]));
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp_ms",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(ts) as ArrayRef],
        )
        .unwrap();
        dataset.append_record_batch("NullTrade", batch).unwrap();

        let mut tracker = WindowTracker::new(config());
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 9_000);
    }

    #[test]
    fn reads_arrow_timestamp_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "at",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            false,
        )]));
        let ts = TimestampMillisecondArray::from(vec![1_000, 2_000]);
        let mut dataset = Dataset::new();
        dataset.register_raw_component("Event", schema.clone());
        dataset
            .append_record_batch(
                "Event",
                arrow_array::RecordBatch::try_new(schema, vec![Arc::new(ts) as ArrayRef]).unwrap(),
            )
            .unwrap();

        let mut tracker = WindowTracker::new(WindowConfig {
            time_field: "at".to_string(),
            ..config()
        });
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 2_000);
    }

    #[test]
    fn rejects_an_unreadable_time_column_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ms",
            DataType::Float64,
            false,
        )]));
        let prices = Float64Array::from(vec![1.5, 2.5]);
        let mut dataset = Dataset::new();
        dataset.register_raw_component("BadTime", schema.clone());
        dataset
            .append_record_batch(
                "BadTime",
                arrow_array::RecordBatch::try_new(schema, vec![Arc::new(prices) as ArrayRef])
                    .unwrap(),
            )
            .unwrap();

        let mut tracker = WindowTracker::new(config());
        let err = tracker.advance_from(&dataset).unwrap_err();
        assert_eq!(err.category(), "generic");
    }
}
