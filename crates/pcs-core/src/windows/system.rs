//! [`WindowedSystem`] — the core windowed aggregation system.
//!
//! Builds on [`WindowSpec`], [`WindowFunction`], and the key-hash helpers to
//! assign window IDs, sort rows into groups, and aggregate each group into a
//! result [`RecordBatch`] stored as a [`WindowResults`] resource.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow_arith::aggregate::{max, min, sum};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt32Array};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;
use async_trait::async_trait;

use crate::error::PcsError;
use crate::pipeline::Dataset;
use crate::system::{System, SystemMeta};

use super::function::{ReduceAggregate, WindowContext, WindowFunction};
use super::hash::{compute_global_hash, compute_key_hash};
use super::result::{DroppedLate, SideOutput, WindowResults};
use super::spec::{WindowSpec, assign_sessions};
use super::time::to_ms_array;
use super::watermark::WatermarkState;

// Maximum expanded rows (rows × k) allowed for sliding windows.
const SLIDING_MAX_EXPANDED_ROWS: u64 = 100_000_000;

// ---------------------------------------------------------------------------
// WindowedSystem
// ---------------------------------------------------------------------------

/// A pipeline system that performs windowed aggregation over a source component.
///
/// On each [`run`](System::run) invocation the system:
///
/// 1. Extracts the time column from the source component and converts it to
///    milliseconds via [`to_ms_array`].
/// 2. Advances the internal [`WatermarkState`] from the observed timestamps.
/// 3. Routes rows beyond the allowed-lateness budget to a
///    [`SideOutput<DroppedLate>`] resource in the pipeline.
/// 4. Assigns a window-bucket ID to every remaining row.
/// 5. Computes a per-row key hash over the configured key fields (or a global
///    all-zero hash for non-keyed windows).
/// 6. Sorts rows by `(window_id, key_hash)` and walks adjacent equal pairs to
///    form groups.
/// 7. Applies the configured [`WindowFunction`] to each group, marking groups
///    as late-firing when the window has already been emitted.
/// 8. Inserts the [`WindowResults`] resource into the pipeline for downstream
///    systems to consume.
///
/// Build via [`WindowedSystemBuilder`].
pub struct WindowedSystem {
    source_component: &'static str,
    time_field: &'static str,
    key_fields: Vec<&'static str>,
    spec: WindowSpec,
    function: WindowFunction,
    meta: SystemMeta,
    /// Watermark state: updated on every run from observed event timestamps.
    ///
    /// `None` when no watermark tracking is configured (legacy batch mode).
    watermark: Option<Mutex<WatermarkState>>,
    /// Set of `(window_id, key_hash)` pairs that have already been emitted at
    /// least once.  Used to detect late re-firings.
    emitted_windows: Mutex<HashSet<(i64, i64)>>,
}

// WindowFunction contains Box<dyn ProcessWindowFn> and Mutex fields — not auto-Debug.
impl std::fmt::Debug for WindowedSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowedSystem")
            .field("source_component", &self.source_component)
            .field("time_field", &self.time_field)
            .field("key_fields", &self.key_fields)
            .field("spec", &self.spec)
            .field("has_watermark", &self.watermark.is_some())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl System for WindowedSystem {
    fn meta(&self) -> SystemMeta {
        self.meta.clone()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), PcsError> {
        let pipeline = data;
        // ------------------------------------------------------------------
        // 1. Get source batch
        // ------------------------------------------------------------------
        let batch = pipeline
            .batch_for(self.source_component)
            .ok_or_else(|| {
                PcsError::generic(format!(
                    "WindowedSystem: component '{}' not registered",
                    self.source_component
                ))
            })?
            .clone();

        let n_rows = batch.num_rows();
        if n_rows == 0 {
            let schema = self.empty_result_schema();
            pipeline.insert_resource(WindowResults::new(schema));
            return Ok(());
        }

        // ------------------------------------------------------------------
        // 2. Extract time column → Int64 milliseconds
        // ------------------------------------------------------------------
        let time_idx = batch.schema().index_of(self.time_field).map_err(|_| {
            PcsError::generic(format!(
                "WindowedSystem: time field '{}' not found in component '{}'",
                self.time_field, self.source_component
            ))
        })?;
        let time_col = batch.column(time_idx).clone();
        let time_ms = to_ms_array(&time_col)?;

        // ------------------------------------------------------------------
        // 2a. Drop rows whose timestamp is null.
        //
        // `to_ms_array` preserves nullability from the source column.  Null
        // timestamps have no meaningful placement in any window — using the raw
        // backing bits (typically 0) would silently land those rows in the
        // epoch window and could advance the watermark to an arbitrary value.
        //
        // We drop them here, before any downstream code reads `.values()` or
        // `.value(i)`, so that every subsequent step can assume a null-free
        // timestamp array.
        // ------------------------------------------------------------------
        let (batch, time_ms) = {
            let null_count = time_ms.null_count();
            if null_count > 0 {
                let n = time_ms.len();
                let keep_indices: Vec<u32> = (0..n)
                    .filter(|&i| !time_ms.is_null(i))
                    .map(|i| i as u32)
                    .collect();

                #[cfg(feature = "tracing")]
                tracing::warn!(
                    component = self.source_component,
                    time_field = self.time_field,
                    null_count,
                    "WindowedSystem: skipping {null_count} row(s) with null timestamp"
                );

                let keep_arr = UInt32Array::from(keep_indices);
                let filtered_cols: Result<Vec<ArrayRef>, PcsError> = batch
                    .columns()
                    .iter()
                    .map(|col| {
                        take(col.as_ref(), &keep_arr, None).map_err(|e| {
                            PcsError::generic(format!(
                                "WindowedSystem: null-timestamp filter take: {e}"
                            ))
                        })
                    })
                    .collect();
                let filtered_batch =
                    RecordBatch::try_new(batch.schema(), filtered_cols?).map_err(|e| {
                        PcsError::generic(format!(
                            "WindowedSystem: null-timestamp filter RecordBatch: {e}"
                        ))
                    })?;
                let filtered_time = take(&time_ms as &dyn Array, &keep_arr, None).map_err(|e| {
                    PcsError::generic(format!(
                        "WindowedSystem: null-timestamp filter take time_ms: {e}"
                    ))
                })?;
                let filtered_time = filtered_time
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        PcsError::generic("WindowedSystem: null-timestamp filter downcast time_ms")
                    })?
                    .clone();

                (filtered_batch, filtered_time)
            } else {
                (batch, time_ms)
            }
        };

        if batch.num_rows() == 0 {
            let schema = self.empty_result_schema();
            pipeline.insert_resource(WindowResults::new(schema));
            return Ok(());
        }

        // ------------------------------------------------------------------
        // 2b. Advance watermark and filter out rows beyond lateness.
        // ------------------------------------------------------------------
        let (batch, time_ms, dropped_side_output) =
            self.apply_watermark_filter(&batch, &time_ms)?;

        if batch.num_rows() == 0 {
            let schema = self.empty_result_schema();
            let mut results = WindowResults::new(schema);
            results.side_output = dropped_side_output;
            pipeline.insert_resource(results);
            return Ok(());
        }

        // ------------------------------------------------------------------
        // 3. Assign window IDs (one per row)
        // ------------------------------------------------------------------
        let n_pre_slide = batch.num_rows();

        // Session windows need key columns for per-key session splitting.
        let session_key_cols: Vec<ArrayRef> = if matches!(&self.spec, WindowSpec::Session { .. }) {
            self.key_fields
                .iter()
                .map(|&field| {
                    let idx = batch.schema().index_of(field).map_err(|_| {
                        PcsError::generic(format!(
                            "WindowedSystem: key field '{field}' not found in component '{}'",
                            self.source_component
                        ))
                    })?;
                    Ok(batch.column(idx).clone())
                })
                .collect::<Result<Vec<_>, PcsError>>()?
        } else {
            vec![]
        };

        // ------------------------------------------------------------------
        // Sliding window: expand batch before the normal aggregation path.
        // ------------------------------------------------------------------
        let (batch, time_ms, sliding_window_ids) = if let WindowSpec::Sliding {
            size_ms,
            slide_ms,
            offset_ms,
        } = &self.spec
        {
            let (exp_batch, exp_time_ms, win_ids) = expand_for_sliding(
                &batch,
                &time_ms,
                n_pre_slide,
                *size_ms,
                *slide_ms,
                *offset_ms,
            )?;
            (exp_batch, exp_time_ms, Some(win_ids))
        } else {
            (batch, time_ms, None)
        };

        let n_rows = batch.num_rows();

        let window_ids: Int64Array = match &self.spec {
            WindowSpec::Tumbling { size_ms, offset_ms } => {
                let ids: Vec<i64> = time_ms
                    .values()
                    .iter()
                    .map(|&ts| WindowSpec::assign_tumbling(ts, *size_ms, *offset_ms))
                    .collect();
                Int64Array::from(ids)
            }
            WindowSpec::Session { gap_ms } => {
                let key_refs: Vec<&ArrayRef> = session_key_cols.iter().collect();
                assign_sessions(&time_ms, &key_refs, *gap_ms)?
            }
            WindowSpec::Sliding { .. } => {
                sliding_window_ids.expect("sliding_window_ids always Some for Sliding spec")
            }
        };

        // ------------------------------------------------------------------
        // 4. Compute key hash
        // ------------------------------------------------------------------
        let key_hash: Int64Array = if self.key_fields.is_empty() {
            compute_global_hash(n_rows)
        } else {
            let key_cols: Vec<ArrayRef> = self
                .key_fields
                .iter()
                .map(|&field| {
                    let idx = batch.schema().index_of(field).map_err(|_| {
                        PcsError::generic(format!(
                            "WindowedSystem: key field '{field}' not found in component '{}'",
                            self.source_component
                        ))
                    })?;
                    Ok(batch.column(idx).clone())
                })
                .collect::<Result<Vec<_>, PcsError>>()?;

            let key_refs: Vec<&ArrayRef> = key_cols.iter().collect();
            compute_key_hash(&key_refs)?
        };

        // ------------------------------------------------------------------
        // 4b. Apply partition filter
        //
        // When a `KeyPartition` resource is present in the pipeline, each runner
        // owns only the rows whose `key_hash % num_instances == instance_ordinal`.
        // Rows assigned to other instances are dropped before aggregation so
        // every runner accumulates a disjoint key slice.
        //
        // Global (non-keyed) windows have key_hash == 0 for every row.
        // Only instance_ordinal == 0 will satisfy `0 % num_instances == 0`
        // when num_instances == 1 (the recommended setting for global windows).
        // ------------------------------------------------------------------
        #[cfg(feature = "distributed")]
        let (window_ids, key_hash, batch, time_ms) = {
            use crate::partition::KeyPartition;
            if let Some(kp) = pipeline.get_resource::<KeyPartition>() {
                let num_instances = kp.num_instances as i64;
                let ordinal = kp.instance_ordinal as i64;
                if num_instances > 1 {
                    use arrow_array::BooleanArray;
                    use arrow_select::filter::filter_record_batch;

                    let keep_mask: BooleanArray = (0..key_hash.len())
                        .map(|i| key_hash.value(i).rem_euclid(num_instances) == ordinal)
                        .collect();

                    let filtered_batch = filter_record_batch(&batch, &keep_mask)
                        .map_err(|e| PcsError::generic(format!("partition filter batch: {e}")))?;
                    let filtered_time_ms = arrow_select::filter::filter(&time_ms, &keep_mask)
                        .map_err(|e| PcsError::generic(format!("partition filter time_ms: {e}")))?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| PcsError::generic("partition filter time_ms downcast"))?
                        .clone();
                    let filtered_key_hash = arrow_select::filter::filter(&key_hash, &keep_mask)
                        .map_err(|e| PcsError::generic(format!("partition filter key_hash: {e}")))?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| PcsError::generic("partition filter key_hash downcast"))?
                        .clone();
                    let filtered_win_ids = arrow_select::filter::filter(&window_ids, &keep_mask)
                        .map_err(|e| {
                            PcsError::generic(format!("partition filter window_ids: {e}"))
                        })?
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| PcsError::generic("partition filter window_ids downcast"))?
                        .clone();

                    if filtered_batch.num_rows() == 0 {
                        let schema = self.empty_result_schema();
                        pipeline.insert_resource(WindowResults::new(schema));
                        pipeline.insert_resource(SideOutput::<DroppedLate>::new());
                        return Ok(());
                    }

                    (
                        filtered_win_ids,
                        filtered_key_hash,
                        filtered_batch,
                        filtered_time_ms,
                    )
                } else {
                    (window_ids, key_hash, batch, time_ms)
                }
            } else {
                (window_ids, key_hash, batch, time_ms)
            }
        };
        #[cfg(not(feature = "distributed"))]
        let (window_ids, key_hash, batch, time_ms) = (window_ids, key_hash, batch, time_ms);

        // Shadow n_rows in case the partition filter reduced the row count.
        let n_rows = batch.num_rows();
        let _ = n_rows; // used below in aggregate_groups via sorted_indices

        // ------------------------------------------------------------------
        // 5. Sort by (window_id, key_hash) → get sorted indices
        // ------------------------------------------------------------------
        let sort_cols = vec![
            SortColumn {
                values: Arc::new(window_ids.clone()) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(key_hash.clone()) as ArrayRef,
                options: None,
            },
        ];

        let sorted_indices: UInt32Array = lexsort_to_indices(&sort_cols, None)
            .map_err(|e| PcsError::generic(format!("WindowedSystem: sort error: {e}")))?;

        let sorted_win_ids = take(&window_ids as &dyn Array, &sorted_indices, None)
            .map_err(|e| PcsError::generic(format!("WindowedSystem: take error: {e}")))?;
        let sorted_key_hash = take(&key_hash as &dyn Array, &sorted_indices, None)
            .map_err(|e| PcsError::generic(format!("WindowedSystem: take error: {e}")))?;
        let sorted_time_ms = take(&time_ms as &dyn Array, &sorted_indices, None)
            .map_err(|e| PcsError::generic(format!("WindowedSystem: take time_ms error: {e}")))?;

        let sorted_win_ids = sorted_win_ids
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| PcsError::generic("WindowedSystem: downcast window_id failed"))?;
        let sorted_key_hash = sorted_key_hash
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| PcsError::generic("WindowedSystem: downcast key_hash failed"))?;
        let sorted_time_ms = sorted_time_ms
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| PcsError::generic("WindowedSystem: downcast time_ms failed"))?;

        // ------------------------------------------------------------------
        // 6. Walk sorted arrays to find group boundaries and aggregate
        // ------------------------------------------------------------------
        let current_wm = self
            .watermark
            .as_ref()
            .map(|m| {
                m.lock()
                    .expect("watermark lock poisoned")
                    .current_watermark()
            })
            .unwrap_or(i64::MIN);

        let (result_schema, on_time_batches, late_batches) = self.aggregate_groups(
            &batch,
            &sorted_indices,
            sorted_win_ids,
            sorted_key_hash,
            sorted_time_ms,
            current_wm,
        )?;

        let mut results = WindowResults::new(result_schema.clone());
        results.batches = on_time_batches.clone();
        results.late_batches = late_batches;
        results.side_output = dropped_side_output;

        // ------------------------------------------------------------------
        // 7. Flush accumulator state
        //
        // If the pipeline has a `WindowAccumulator` component registered, update
        // it with the fresh aggregation results:
        //   a. Mark existing rows for this `source_component` as dead.
        //   b. Append the new aggregate rows.
        //   c. Compact to remove dead rows.
        //
        // This is intentionally gated on the component being registered — not
        // every pipeline needs persistence; the world_factory decides.
        // ------------------------------------------------------------------
        #[cfg(feature = "distributed")]
        {
            use super::accumulator::WindowAccumulator;
            use crate::component::Component as _;

            if pipeline.batch_for(WindowAccumulator::name()).is_some() {
                self.flush_accumulator(pipeline, &on_time_batches)?;
            }
        }

        pipeline.insert_resource(results);

        Ok(())
    }
}

impl WindowedSystem {
    /// Update the pipeline's `WindowAccumulator` component with fresh aggregate results.
    ///
    /// For each result batch (one per `(window_id, key_hash)` group), the
    /// existing accumulator row with matching `source_component`, `window_id`,
    /// and `key_hash` is marked dead, then the new row is appended.
    /// A compaction is performed at the end to remove dead rows.
    #[cfg(feature = "distributed")]
    fn flush_accumulator(
        &self,
        pipeline: &mut Dataset,
        result_batches: &[RecordBatch],
    ) -> Result<(), PcsError> {
        use super::accumulator::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator};
        use crate::component::Component as _;
        use arrow_array::{Array, Int64Array, StringArray};

        if result_batches.is_empty() {
            return Ok(());
        }

        // Build the set of (window_id, key_hash) pairs we are about to write.
        let mut new_groups: std::collections::HashSet<(i64, i64)> =
            std::collections::HashSet::new();
        for rb in result_batches {
            if rb.num_rows() == 0 {
                continue;
            }
            let wid_idx = rb.schema().index_of("window_id").ok();
            let kh_idx = rb.schema().index_of("key_hash").ok();
            if let (Some(wi), Some(ki)) = (wid_idx, kh_idx) {
                let wid_col = rb.column(wi).as_any().downcast_ref::<Int64Array>();
                let kh_col = rb.column(ki).as_any().downcast_ref::<Int64Array>();
                if let (Some(wc), Some(kc)) = (wid_col, kh_col) {
                    for r in 0..rb.num_rows() {
                        new_groups.insert((wc.value(r), kc.value(r)));
                    }
                }
            }
        }

        // Mark superseded accumulator rows as dead.
        if let Some(acc_batch) = pipeline.batch_for(WindowAccumulator::name()) {
            let acc_batch = acc_batch.clone();
            let src_idx = acc_batch.schema().index_of("source_component").ok();
            let wid_idx = acc_batch.schema().index_of("window_id").ok();
            let kh_idx = acc_batch.schema().index_of("key_hash").ok();

            if let (Some(si), Some(wi), Some(ki)) = (src_idx, wid_idx, kh_idx) {
                let src_col = acc_batch.column(si).as_any().downcast_ref::<StringArray>();
                let wid_col = acc_batch.column(wi).as_any().downcast_ref::<Int64Array>();
                let kh_col = acc_batch.column(ki).as_any().downcast_ref::<Int64Array>();

                if let (Some(sc), Some(wc), Some(kc)) = (src_col, wid_col, kh_col) {
                    let row_range = pipeline.row_range();
                    for (row_offset, abs_row) in row_range.enumerate() {
                        if row_offset >= acc_batch.num_rows() {
                            break;
                        }
                        let matches_component = sc.value(row_offset) == self.source_component;
                        let group = (wc.value(row_offset), kc.value(row_offset));
                        if matches_component && new_groups.contains(&group) {
                            pipeline.mark_dead(crate::row::Row::new(abs_row));
                        }
                    }
                }
            }
        }

        // Build new accumulator rows from the aggregation results.
        let new_rows: Vec<WindowAccumulator> = result_batches
            .iter()
            .filter_map(|rb| {
                if rb.num_rows() == 0 {
                    return None;
                }
                let wid_idx = rb.schema().index_of("window_id").ok()?;
                let kh_idx = rb.schema().index_of("key_hash").ok()?;
                let wid_col = rb.column(wid_idx).as_any().downcast_ref::<Int64Array>()?;
                let kh_col = rb.column(kh_idx).as_any().downcast_ref::<Int64Array>()?;

                // Extract session timestamps if present.
                let sts_idx = rb.schema().index_of("session_start_ts").ok();
                let ste_idx = rb.schema().index_of("session_end_ts").ok();
                let sts_val = sts_idx.and_then(|i| {
                    rb.column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|a| {
                            if a.is_valid(0) {
                                Some(a.value(0))
                            } else {
                                None
                            }
                        })
                });
                let ste_val = ste_idx.and_then(|i| {
                    rb.column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|a| {
                            if a.is_valid(0) {
                                Some(a.value(0))
                            } else {
                                None
                            }
                        })
                });

                // Extract aggregate value — look for any Float64 column beyond window_id / key_hash.
                let mut sum_f64 = None;
                let mut count = 0i64;
                let rb_schema = rb.schema();
                for col_idx in 0..rb_schema.fields().len() {
                    let field = rb_schema.field(col_idx);
                    if field.name() == "window_id"
                        || field.name() == "key_hash"
                        || field.name() == "session_start_ts"
                        || field.name() == "session_end_ts"
                    {
                        continue;
                    }
                    if let arrow_schema::DataType::Float64 = field.data_type()
                        && let Some(arr) = rb
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::Float64Array>()
                    {
                        if arr.is_valid(0) {
                            sum_f64 = Some(arr.value(0));
                        }
                        count = 1;
                    }
                    if let arrow_schema::DataType::Int64 = field.data_type()
                        && let Some(arr) = rb.column(col_idx).as_any().downcast_ref::<Int64Array>()
                        && arr.is_valid(0)
                    {
                        count = arr.value(0);
                    }
                }

                Some(WindowAccumulator {
                    version: Some(CURRENT_ACCUMULATOR_VERSION),
                    source_component: self.source_component.to_string(),
                    window_id: wid_col.value(0),
                    key_hash: kh_col.value(0),
                    count,
                    sum_f64,
                    min_f64: None,
                    max_f64: None,
                    session_start_ts: sts_val,
                    session_end_ts: ste_val,
                    finalized_at_watermark: None,
                })
            })
            .collect();

        if !new_rows.is_empty() {
            pipeline
                .append::<WindowAccumulator>(&new_rows)
                .map_err(|e| {
                    PcsError::generic(format!("WindowedSystem: accumulator append error: {e}"))
                })?;
        }

        // Compact to remove the dead rows we just superseded.
        pipeline
            .compact()
            .map_err(|e| PcsError::generic(format!("WindowedSystem: compact error: {e}")))?;

        Ok(())
    }
}

/// Parameters for aggregating a single window group slice.
struct SliceParams<'a> {
    schema: &'a Arc<Schema>,
    /// Sorted input column for `Reduce` aggregates, or the full sorted source
    /// batch for `Process` functions.
    sorted_col: &'a ArrayRef,
    /// Full sorted source batch (used by `WindowFunction::Process`).
    sorted_source_batch: &'a RecordBatch,
    sorted_time_ms: &'a Int64Array,
    win_id: i64,
    key_hash: i64,
    group_start: usize,
    group_end: usize,
    /// Whether this group is a late re-firing of an already-emitted window.
    is_late_firing: bool,
    /// Current watermark at the time of processing.
    watermark: i64,
    /// Window size in milliseconds (used to compute `window_start`/`window_end`).
    window_size_ms: i64,
}

impl WindowedSystem {
    /// Advance the watermark from all timestamps in the batch, then partition
    /// rows into:
    ///
    /// - **retained** (on-time + late-but-acceptable): returned as
    ///   `(filtered_batch, filtered_time_ms)`.
    /// - **dropped** (beyond lateness): collected into `SideOutput<DroppedLate>`.
    ///
    /// When no watermark is configured, all rows are retained unchanged.
    fn apply_watermark_filter(
        &self,
        batch: &RecordBatch,
        time_ms: &Int64Array,
    ) -> Result<(RecordBatch, Int64Array, SideOutput<DroppedLate>), PcsError> {
        let mut side_output = SideOutput::<DroppedLate>::new();

        let wm_lock = match &self.watermark {
            None => {
                // No watermark tracking — pass through unchanged.
                return Ok((batch.clone(), time_ms.clone(), side_output));
            }
            Some(m) => m,
        };

        // Classify rows using the CURRENT (pre-advance) watermark, then advance.
        //
        // The ordering matters: if we advanced first and then classified,
        // on-time rows in the same batch that arrive before the max timestamp
        // would be incorrectly classified as late.  We use the watermark from
        // the end of the *previous* batch to decide lateness for this batch,
        // then update it so the *next* batch sees the higher watermark.
        let wm = wm_lock.lock().expect("watermark lock poisoned");
        let n = time_ms.len();
        let mut keep_indices: Vec<u32> = Vec::with_capacity(n);
        let mut drop_indices: Vec<u32> = Vec::with_capacity(n);

        for i in 0..n {
            // Null timestamps have already been filtered out before this point
            // by the early null-filter in `run`.  Guard defensively in case
            // this method is ever called independently: treat null timestamps
            // as "keep" (they will not advance the watermark).
            if time_ms.is_null(i) {
                keep_indices.push(i as u32);
                continue;
            }
            let ts = time_ms.value(i);
            if wm.is_beyond_lateness(ts) {
                drop_indices.push(i as u32);
            } else {
                keep_indices.push(i as u32);
            }
        }
        drop(wm); // release lock before advancing

        // Now advance the watermark from all timestamps in this batch so that
        // subsequent runs see the updated high-water mark.
        //
        // Use `.iter()` rather than `.values()` to skip null slots — `.values()`
        // returns the raw backing buffer which may contain arbitrary bit patterns
        // for null entries.
        {
            let mut wm = wm_lock.lock().expect("watermark lock poisoned");
            for ts in time_ms.iter().flatten() {
                wm.advance(ts);
            }
        }

        // If nothing is dropped, avoid a copy.
        if drop_indices.is_empty() {
            return Ok((batch.clone(), time_ms.clone(), side_output));
        }

        // Build filtered batch for kept rows.
        let keep_arr = UInt32Array::from(keep_indices);
        let filtered_cols: Result<Vec<ArrayRef>, PcsError> = batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), &keep_arr, None)
                    .map_err(|e| PcsError::generic(format!("watermark filter take: {e}")))
            })
            .collect();
        let filtered_batch = RecordBatch::try_new(batch.schema(), filtered_cols?)
            .map_err(|e| PcsError::generic(format!("watermark filter RecordBatch: {e}")))?;

        let filtered_time = take(time_ms as &dyn Array, &keep_arr, None)
            .map_err(|e| PcsError::generic(format!("watermark filter take time_ms: {e}")))?;
        let filtered_time = filtered_time
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| PcsError::generic("watermark filter downcast time_ms"))?
            .clone();

        // Build side-output batch for dropped rows.
        let drop_arr = UInt32Array::from(drop_indices);
        let drop_cols: Result<Vec<ArrayRef>, PcsError> = batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), &drop_arr, None)
                    .map_err(|e| PcsError::generic(format!("watermark drop take: {e}")))
            })
            .collect();
        let drop_batch = RecordBatch::try_new(batch.schema(), drop_cols?)
            .map_err(|e| PcsError::generic(format!("watermark drop RecordBatch: {e}")))?;
        side_output.push(drop_batch);

        Ok((filtered_batch, filtered_time, side_output))
    }

    /// Walk sorted indices and produce one output `RecordBatch` per group.
    ///
    /// Returns `(schema, on_time_batches, late_batches)`.
    /// Groups whose `(window_id, key_hash)` pair has already been emitted are
    /// classified as late re-firings and placed in `late_batches`.
    #[allow(clippy::type_complexity)]
    fn aggregate_groups(
        &self,
        source_batch: &RecordBatch,
        sorted_indices: &UInt32Array,
        sorted_win_ids: &Int64Array,
        sorted_key_hash: &Int64Array,
        sorted_time_ms: &Int64Array,
        current_wm: i64,
    ) -> Result<(Arc<Schema>, Vec<RecordBatch>, Vec<RecordBatch>), PcsError> {
        let n = sorted_indices.len();
        let (result_schema, sorted_input_col, sorted_source_batch) =
            self.prepare_aggregate_inputs(source_batch, sorted_indices)?;

        // Compute window size for the context (tumbling/sliding only; session
        // windows use 0 since they have variable size).
        let window_size_ms: i64 = match &self.spec {
            WindowSpec::Tumbling { size_ms, .. } => *size_ms,
            WindowSpec::Sliding { size_ms, .. } => *size_ms,
            WindowSpec::Session { .. } => 0,
        };

        let mut on_time_batches = Vec::new();
        let mut late_batches = Vec::new();
        let mut group_start = 0usize;

        while group_start < n {
            let win_id = sorted_win_ids.value(group_start);
            let key_hash = sorted_key_hash.value(group_start);

            // Find the end of this group (same window_id AND key_hash).
            let mut group_end = group_start + 1;
            while group_end < n
                && sorted_win_ids.value(group_end) == win_id
                && sorted_key_hash.value(group_end) == key_hash
            {
                group_end += 1;
            }

            // Determine whether this is a late re-firing by checking the
            // emitted-windows set.
            let group_key = (win_id, key_hash);
            let is_late_firing = {
                let emitted = self
                    .emitted_windows
                    .lock()
                    .expect("emitted_windows poisoned");
                emitted.contains(&group_key)
            };

            let params = SliceParams {
                schema: &result_schema,
                sorted_col: &sorted_input_col,
                sorted_source_batch: &sorted_source_batch,
                sorted_time_ms,
                win_id,
                key_hash,
                group_start,
                group_end,
                is_late_firing,
                watermark: current_wm,
                window_size_ms,
            };
            let output_batch = self.aggregate_slice(params)?;

            // Mark this window as emitted for future late-firing detection.
            {
                let mut emitted = self
                    .emitted_windows
                    .lock()
                    .expect("emitted_windows poisoned");
                emitted.insert(group_key);
            }

            if is_late_firing {
                late_batches.push(output_batch);
            } else {
                on_time_batches.push(output_batch);
            }

            group_start = group_end;
        }

        Ok((result_schema, on_time_batches, late_batches))
    }

    /// Build the result schema and extract the (already-sorted) input column
    /// for the aggregate.
    ///
    /// Returns `(result_schema, sorted_input_col, sorted_source_batch)`.
    ///
    /// For `Reduce` variants, `sorted_input_col` is the target field sorted
    /// according to `sorted_indices`.  For `Process` variants it is a
    /// placeholder (the first column of the source batch) because the full
    /// `sorted_source_batch` is what gets passed to the user function.
    fn prepare_aggregate_inputs(
        &self,
        source_batch: &RecordBatch,
        sorted_indices: &UInt32Array,
    ) -> Result<(Arc<Schema>, ArrayRef, RecordBatch), PcsError> {
        // Build a sorted copy of the entire source batch for Process functions.
        let sorted_cols: Result<Vec<ArrayRef>, PcsError> = source_batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), sorted_indices, None)
                    .map_err(|e| PcsError::generic(format!("WindowedSystem: take error: {e}")))
            })
            .collect();
        let sorted_source_batch = RecordBatch::try_new(source_batch.schema(), sorted_cols?)
            .map_err(|e| PcsError::generic(format!("WindowedSystem: sorted batch error: {e}")))?;

        match &self.function {
            WindowFunction::Reduce {
                input_field,
                aggregate,
            } => {
                let col_idx = source_batch.schema().index_of(input_field).map_err(|_| {
                    PcsError::generic(format!(
                        "WindowedSystem: input field '{input_field}' not found \
                                 in component '{}'",
                        self.source_component
                    ))
                })?;
                let sorted_col = sorted_source_batch.column(col_idx).clone();

                let value_type = sorted_col.data_type().clone();
                let output_name = aggregate_output_name(*aggregate, input_field);
                let is_session = matches!(&self.spec, WindowSpec::Session { .. });
                let mut fields = vec![
                    Field::new("window_id", DataType::Int64, false),
                    Field::new("key_hash", DataType::Int64, false),
                    Field::new(output_name, value_type, true),
                ];
                if is_session {
                    fields.push(Field::new("session_start_ts", DataType::Int64, false));
                    fields.push(Field::new("session_end_ts", DataType::Int64, false));
                }
                let schema = Arc::new(Schema::new(fields));
                Ok((schema, sorted_col, sorted_source_batch))
            }
            WindowFunction::Process(_) => {
                // For Process functions, the schema is determined by the user
                // function's output.  We use an empty placeholder schema here;
                // the actual output schema comes from the returned RecordBatch.
                // `sorted_col` is unused for Process — pass the first column as a
                // placeholder so SliceParams has a valid reference.
                let placeholder_col = sorted_source_batch.column(0).clone();
                let placeholder_schema = Arc::new(Schema::empty());
                Ok((placeholder_schema, placeholder_col, sorted_source_batch))
            }
        }
    }

    /// Aggregate rows `[group_start, group_end)` from the sorted input column.
    fn aggregate_slice(&self, p: SliceParams<'_>) -> Result<RecordBatch, PcsError> {
        // Build WindowContext for both Reduce and Process paths.
        let window_start = p.win_id * p.window_size_ms;
        let window_end = window_start + p.window_size_ms;
        let ctx = WindowContext {
            window_id: p.win_id,
            window_start,
            window_end,
            is_late_firing: p.is_late_firing,
            watermark: p.watermark,
        };

        match &self.function {
            WindowFunction::Reduce { aggregate, .. } => {
                let slice = p
                    .sorted_col
                    .slice(p.group_start, p.group_end - p.group_start);
                let agg_value = apply_reduce_aggregate(*aggregate, &slice)?;

                let win_id_col: ArrayRef = Arc::new(Int64Array::from(vec![p.win_id]));
                let key_hash_col: ArrayRef = Arc::new(Int64Array::from(vec![p.key_hash]));

                let mut columns = vec![win_id_col, key_hash_col, agg_value];

                if matches!(&self.spec, WindowSpec::Session { .. }) {
                    // Compute min/max ts over this group's sorted time slice.
                    let ts_slice = &p.sorted_time_ms.values()[p.group_start..p.group_end];
                    let start_ts = ts_slice.iter().copied().min().unwrap_or(0);
                    let end_ts = ts_slice.iter().copied().max().unwrap_or(0);
                    columns.push(Arc::new(Int64Array::from(vec![start_ts])) as ArrayRef);
                    columns.push(Arc::new(Int64Array::from(vec![end_ts])) as ArrayRef);
                }

                let _ = ctx; // ctx available for future Reduce enhancements
                RecordBatch::try_new(p.schema.clone(), columns).map_err(|e| {
                    PcsError::generic(format!("WindowedSystem: RecordBatch error: {e}"))
                })
            }
            WindowFunction::Process(f) => {
                // Slice the sorted source batch to the group rows.
                let group_len = p.group_end - p.group_start;
                let group_batch = p.sorted_source_batch.slice(p.group_start, group_len);
                f.process(&ctx, &group_batch)
            }
        }
    }

    /// Schema returned when there are no rows to aggregate.
    fn empty_result_schema(&self) -> Arc<Schema> {
        let is_session = matches!(&self.spec, WindowSpec::Session { .. });
        let mut fields = vec![
            Field::new("window_id", DataType::Int64, false),
            Field::new("key_hash", DataType::Int64, false),
            Field::new("sum_value", DataType::Float64, true),
        ];
        if is_session {
            fields.push(Field::new("session_start_ts", DataType::Int64, false));
            fields.push(Field::new("session_end_ts", DataType::Int64, false));
        }
        Arc::new(Schema::new(fields))
    }
}

// ---------------------------------------------------------------------------
// Sliding window expansion
// ---------------------------------------------------------------------------

/// Expand `source_batch` so that each row appears `k = ceil(size_ms/slide_ms)` times.
///
/// Returns `(expanded_batch, expanded_time_ms, window_ids)` where each of the
/// three has `n_rows * k` elements.  The window ID for expanded row `(i, j)`
/// is `assign_sliding(ts[i])[j]`.
///
/// # Errors
///
/// Returns `PcsError::Generic` when `k * n_rows > 100_000_000` (amplification
/// guard) or on any Arrow operation failure.
fn expand_for_sliding(
    source_batch: &RecordBatch,
    time_ms: &Int64Array,
    n_rows: usize,
    size_ms: i64,
    slide_ms: i64,
    offset_ms: i64,
) -> Result<(RecordBatch, Int64Array, Int64Array), PcsError> {
    let k = (size_ms + slide_ms - 1) / slide_ms;

    // Guard: refuse to expand if the result would exceed the memory cap.
    if k as u64 * n_rows as u64 > SLIDING_MAX_EXPANDED_ROWS {
        return Err(PcsError::generic(format!(
            "sliding window amplification too high: k={k} × n_rows={n_rows} \
             exceeds the {SLIDING_MAX_EXPANDED_ROWS} row limit; \
             reduce size_ms/slide_ms ratio or use fewer input rows"
        )));
    }

    // Build the repeating index array: [0,0,..(k times)..,1,1,...,N-1,..]
    let repeat_indices: UInt32Array = (0..n_rows as u32)
        .flat_map(|i| (0..k).map(move |_| i))
        .collect::<Vec<u32>>()
        .into();

    // Expand every column.
    let expanded_batch = {
        let expanded_cols: Result<Vec<ArrayRef>, PcsError> = source_batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), &repeat_indices, None)
                    .map_err(|e| PcsError::generic(format!("expand_for_sliding: take error: {e}")))
            })
            .collect();
        RecordBatch::try_new(source_batch.schema(), expanded_cols?)
            .map_err(|e| PcsError::generic(format!("expand_for_sliding: RecordBatch: {e}")))?
    };

    // Expand time column.
    let expanded_time_ms = take(time_ms as &dyn Array, &repeat_indices, None)
        .map_err(|e| PcsError::generic(format!("expand_for_sliding: take time_ms: {e}")))?;
    let expanded_time_ms = expanded_time_ms
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| PcsError::generic("expand_for_sliding: downcast time_ms failed"))?
        .clone();

    // Compute window IDs for each expanded row.
    // Row i contributes windows: assign_sliding(ts[i])[0..k]
    let window_ids: Int64Array = time_ms
        .values()
        .iter()
        .flat_map(|&ts| WindowSpec::assign_sliding(ts, size_ms, slide_ms, offset_ms))
        .collect::<Vec<i64>>()
        .into();

    Ok((expanded_batch, expanded_time_ms, window_ids))
}

// ---------------------------------------------------------------------------
// Aggregate helpers
// ---------------------------------------------------------------------------

fn aggregate_output_name(aggregate: ReduceAggregate, field: &str) -> String {
    match aggregate {
        ReduceAggregate::Sum => format!("sum_{field}"),
        ReduceAggregate::Min => format!("min_{field}"),
        ReduceAggregate::Max => format!("max_{field}"),
        ReduceAggregate::Count => format!("count_{field}"),
        ReduceAggregate::Mean => format!("mean_{field}"),
    }
}

/// Apply a `ReduceAggregate` to an array slice, returning a single-element `ArrayRef`.
///
/// All aggregates operate on `Float64` columns. Downcast errors return `PcsError::generic`.
fn apply_reduce_aggregate(
    aggregate: ReduceAggregate,
    col: &ArrayRef,
) -> Result<ArrayRef, PcsError> {
    let arr = downcast_float64(col)?;
    let result = match aggregate {
        ReduceAggregate::Sum => sum(arr).unwrap_or(0.0),
        ReduceAggregate::Min => min(arr).unwrap_or(f64::INFINITY),
        ReduceAggregate::Max => max(arr).unwrap_or(f64::NEG_INFINITY),
        ReduceAggregate::Count => arr.len() as f64,
        ReduceAggregate::Mean => {
            // Divide by the number of non-null values so that null entries do
            // not bias the mean downward.  When every value is null the result
            // is NaN (undefined mean).
            let non_null = arr.len() - arr.null_count();
            if non_null == 0 {
                f64::NAN
            } else {
                sum(arr).unwrap_or(0.0) / non_null as f64
            }
        }
    };
    Ok(Arc::new(Float64Array::from(vec![result])) as ArrayRef)
}

fn downcast_float64(col: &ArrayRef) -> Result<&Float64Array, PcsError> {
    match col.data_type() {
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PcsError::generic("WindowedSystem: downcast Float64Array failed")),
        other => Err(PcsError::generic(format!(
            "WindowedSystem: aggregate requires Float64 column, got {other:?}; \
             cast the field to Float64 first"
        ))),
    }
}

// (empty_result_schema moved to WindowedSystem::empty_result_schema)

// ---------------------------------------------------------------------------
// WindowedSystemBuilder
// ---------------------------------------------------------------------------

/// Builder for [`WindowedSystem`].
///
/// All required fields (`source`, `window`, `function`) must be set before
/// calling [`build`](Self::build), which returns a `PcsError::Configuration`
/// for any missing required field.
///
/// # Example
///
/// ```ignore
/// let sys = WindowedSystemBuilder::new()
///     .source("Trade", "timestamp_ms")
///     .keyed_by(&["symbol"])
///     .window(WindowSpec::Tumbling { size_ms: 60_000, offset_ms: 0 })
///     .function(WindowFunction::Reduce {
///         input_field: "price",
///         aggregate: ReduceAggregate::Sum,
///     })
///     .build()
///     .unwrap();
/// ```
pub struct WindowedSystemBuilder {
    source_component: Option<&'static str>,
    time_field: Option<&'static str>,
    key_fields: Vec<&'static str>,
    spec: Option<WindowSpec>,
    function: Option<WindowFunction>,
    /// Allowed lateness in milliseconds. `None` disables watermark tracking.
    allowed_lateness_ms: Option<i64>,
}

impl Default for WindowedSystemBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowedSystemBuilder {
    /// Create a new builder with all fields unset.
    ///
    /// Watermark tracking is disabled by default.  Call
    /// [`allowed_lateness`](Self::allowed_lateness) to enable it.
    pub fn new() -> Self {
        Self {
            source_component: None,
            time_field: None,
            key_fields: vec![],
            spec: None,
            function: None,
            allowed_lateness_ms: None,
        }
    }

    /// Enable watermark tracking with the specified allowed-lateness budget.
    ///
    /// When set, the system advances an internal watermark from observed
    /// event timestamps and classifies each incoming row as:
    ///
    /// - **on-time**: `ts >= watermark` — processed normally.
    /// - **late but acceptable**: `watermark - allowed_lateness ≤ ts < watermark`
    ///   — triggers a late re-firing of the window; result lands in
    ///   [`WindowResults::late_batches`].
    /// - **beyond lateness**: `ts < watermark - allowed_lateness` — routed to
    ///   `WindowResults::side_output` (a [`SideOutput<DroppedLate>`] resource).
    ///
    /// Pass `0` to drop all out-of-order data immediately (no late firings).
    pub fn allowed_lateness(mut self, ms: i64) -> Self {
        self.allowed_lateness_ms = Some(ms);
        self
    }

    /// Set the source component name and the name of its time field.
    ///
    /// The time field must be one of the types accepted by [`to_ms_array`]:
    /// `Int64`, `TimestampMillisecond`, `TimestampSecond`, `TimestampMicrosecond`,
    /// or `TimestampNanosecond`.
    pub fn source(mut self, component: &'static str, time_field: &'static str) -> Self {
        self.source_component = Some(component);
        self.time_field = Some(time_field);
        self
    }

    /// Set the key fields for grouped (keyed) windows.
    ///
    /// Pass an empty slice or omit this call for a non-keyed (global) window
    /// where all rows share the same bucket.
    pub fn keyed_by(mut self, fields: &[&'static str]) -> Self {
        self.key_fields = fields.to_vec();
        self
    }

    /// Set the window specification (geometry).
    pub fn window(mut self, spec: WindowSpec) -> Self {
        self.spec = Some(spec);
        self
    }

    /// Set the window function (aggregate or custom process).
    pub fn function(mut self, f: WindowFunction) -> Self {
        self.function = Some(f);
        self
    }

    /// Build the [`WindowedSystem`], validating that all required fields are set.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] if `source`, `window`, or
    /// `function` have not been set.
    pub fn build(self) -> Result<WindowedSystem, PcsError> {
        let source_component = self.source_component.ok_or_else(|| {
            PcsError::configuration(
                "WindowedSystemBuilder: source component not set; call .source()",
            )
        })?;
        let time_field = self.time_field.ok_or_else(|| {
            PcsError::configuration("WindowedSystemBuilder: time field not set; call .source()")
        })?;
        let spec = self.spec.ok_or_else(|| {
            PcsError::configuration("WindowedSystemBuilder: window spec not set; call .window()")
        })?;
        let function = self.function.ok_or_else(|| {
            PcsError::configuration(
                "WindowedSystemBuilder: window function not set; call .function()",
            )
        })?;

        let meta = SystemMeta::new("windowed")
            .read_component(source_component)
            .write_resource::<WindowResults>();

        let watermark = self
            .allowed_lateness_ms
            .map(|ms| Mutex::new(WatermarkState::new(ms)));

        Ok(WindowedSystem {
            source_component,
            time_field,
            key_fields: self.key_fields,
            spec,
            function,
            meta,
            watermark,
            emitted_windows: Mutex::new(HashSet::new()),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::pipeline::Dataset;

    use super::super::function::{ReduceAggregate, WindowFunction};
    use super::super::result::WindowResults;
    use super::super::spec::WindowSpec;
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

    // -----------------------------------------------------------------------
    // Builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_builder_missing_source_returns_error() {
        let result = WindowedSystemBuilder::new()
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "configuration");
    }

    #[test]
    fn test_builder_missing_window_returns_error() {
        let result = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "configuration");
    }

    #[test]
    fn test_builder_missing_function_returns_error() {
        let result = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "configuration");
    }

    #[test]
    fn test_builder_success_sets_meta_name() {
        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();
        assert_eq!(sys.meta().name, "windowed");
    }

    #[test]
    fn test_builder_meta_reads_component_and_writes_resource() {
        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();
        let meta = sys.meta();
        assert!(meta.reads_components.contains(&"Trade"));
        assert!(!meta.writes_resources.is_empty());
    }

    // -----------------------------------------------------------------------
    // Non-keyed tumbling sum (core integration test)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_non_keyed_tumbling_sum() {
        // 6 trades across two 1-second windows:
        //   window 0 (ts 0..1000ms):  prices 10.0, 20.0, 30.0  → sum = 60.0
        //   window 1 (ts 1000..2000): prices 5.0,  15.0, 25.0  → sum = 45.0
        let trades = vec![
            Trade {
                timestamp_ms: 100,
                price: 10.0,
            },
            Trade {
                timestamp_ms: 200,
                price: 20.0,
            },
            Trade {
                timestamp_ms: 300,
                price: 30.0,
            },
            Trade {
                timestamp_ms: 1100,
                price: 5.0,
            },
            Trade {
                timestamp_ms: 1200,
                price: 15.0,
            },
            Trade {
                timestamp_ms: 1300,
                price: 25.0,
            },
        ];

        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 2, "expected two window groups");

        // Results are sorted by window_id ascending.
        let first = &results.batches[0];
        let second = &results.batches[1];

        let win0_id = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let win1_id = second
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        assert_eq!(win0_id, 0);
        assert_eq!(win1_id, 1);

        let sum0 = first
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let sum1 = second
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);

        assert!(
            (sum0 - 60.0).abs() < 1e-9,
            "window 0 sum: expected 60.0, got {sum0}"
        );
        assert!(
            (sum1 - 45.0).abs() < 1e-9,
            "window 1 sum: expected 45.0, got {sum1}"
        );
    }

    // -----------------------------------------------------------------------
    // Empty pipeline produces empty results
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_empty_component_produces_empty_results() {
        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert!(results.batches.is_empty());
    }

    // -----------------------------------------------------------------------
    // Unregistered component returns error
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_missing_component_returns_error() {
        let mut pipeline = Dataset::new();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        let result = sys.run(&mut pipeline).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().category(), "generic");
    }

    // -----------------------------------------------------------------------
    // Single row → sum equals that row's value
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_single_window_single_row_sum() {
        let trades = vec![Trade {
            timestamp_ms: 500,
            price: 42.0,
        }];

        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 1);
        let sum_val = results.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((sum_val - 42.0).abs() < 1e-9);
    }

    // -----------------------------------------------------------------------
    // Helper: build a pipeline with a single Trade window containing prices
    // -----------------------------------------------------------------------

    async fn world_with_prices(prices: &[f64]) -> Dataset {
        let base_ts = 100i64;
        let trades: Vec<Trade> = prices
            .iter()
            .enumerate()
            .map(|(i, &price)| Trade {
                timestamp_ms: base_ts + i as i64 * 10,
                price,
            })
            .collect();

        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();
        pipeline
    }

    async fn run_aggregate(pipeline: &mut Dataset, aggregate: ReduceAggregate) -> f64 {
        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 10_000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate,
            })
            .build()
            .unwrap();

        sys.run(pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 1, "expected single window group");
        results.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    // -----------------------------------------------------------------------
    // Min aggregate
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_min_aggregate_returns_smallest_value() {
        let mut pipeline = world_with_prices(&[30.0, 10.0, 20.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Min).await;
        assert!(
            (result - 10.0).abs() < 1e-9,
            "min: expected 10.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_min_aggregate_single_row() {
        let mut pipeline = world_with_prices(&[99.5]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Min).await;
        assert!(
            (result - 99.5).abs() < 1e-9,
            "min single row: expected 99.5, got {result}"
        );
    }

    // -----------------------------------------------------------------------
    // Max aggregate
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_max_aggregate_returns_largest_value() {
        let mut pipeline = world_with_prices(&[30.0, 10.0, 20.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Max).await;
        assert!(
            (result - 30.0).abs() < 1e-9,
            "max: expected 30.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_max_aggregate_single_row() {
        let mut pipeline = world_with_prices(&[7.25]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Max).await;
        assert!(
            (result - 7.25).abs() < 1e-9,
            "max single row: expected 7.25, got {result}"
        );
    }

    // -----------------------------------------------------------------------
    // Count aggregate
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_count_aggregate_returns_row_count() {
        let mut pipeline = world_with_prices(&[1.0, 2.0, 3.0, 4.0, 5.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Count).await;
        assert!(
            (result - 5.0).abs() < 1e-9,
            "count: expected 5.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_count_aggregate_single_row() {
        let mut pipeline = world_with_prices(&[42.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Count).await;
        assert!(
            (result - 1.0).abs() < 1e-9,
            "count single row: expected 1.0, got {result}"
        );
    }

    // -----------------------------------------------------------------------
    // Mean aggregate
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_mean_aggregate_returns_arithmetic_mean() {
        // prices: 10, 20, 30 → mean = 20
        let mut pipeline = world_with_prices(&[10.0, 20.0, 30.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Mean).await;
        assert!(
            (result - 20.0).abs() < 1e-9,
            "mean: expected 20.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_mean_aggregate_single_row() {
        let mut pipeline = world_with_prices(&[55.0]).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Mean).await;
        assert!(
            (result - 55.0).abs() < 1e-9,
            "mean single row: expected 55.0, got {result}"
        );
    }

    #[tokio::test]
    async fn test_mean_aggregate_non_uniform_distribution() {
        // prices: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 → mean = 5.5
        let prices: Vec<f64> = (1..=10).map(|i| i as f64).collect();
        let mut pipeline = world_with_prices(&prices).await;
        let result = run_aggregate(&mut pipeline, ReduceAggregate::Mean).await;
        assert!(
            (result - 5.5).abs() < 1e-9,
            "mean: expected 5.5, got {result}"
        );
    }

    // -----------------------------------------------------------------------
    // Sliding window unit tests
    // -----------------------------------------------------------------------

    /// Two rows each belonging to k=2 windows.
    /// size=2000ms, slide=1000ms → k=2.
    ///
    /// ts=500  → windows: floor(500/1000)=0, floor(-500/1000)=-1  → ids [0, -1]
    /// ts=1500 → windows: floor(1500/1000)=1, floor(500/1000)=0   → ids [1, 0]
    ///
    /// Aggregated by window_id:
    ///   window -1: row with ts=500  → price 10.0 → sum 10.0
    ///   window  0: row ts=500+ts=1500 → prices 10.0, 20.0 → sum 30.0
    ///   window  1: row with ts=1500 → price 20.0 → sum 20.0
    #[tokio::test]
    async fn test_sliding_window_k2_correct_window_ids_and_sums() {
        let trades = vec![
            Trade {
                timestamp_ms: 500,
                price: 10.0,
            },
            Trade {
                timestamp_ms: 1500,
                price: 20.0,
            },
        ];

        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Sliding {
                size_ms: 2000,
                slide_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        // 3 distinct window_ids: -1, 0, 1
        assert_eq!(results.batches.len(), 3, "expected 3 window groups");

        let mut pairs: Vec<(i64, f64)> = results
            .batches
            .iter()
            .map(|b| {
                let win_id = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                let sum = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0);
                (win_id, sum)
            })
            .collect();
        pairs.sort_by_key(|&(w, _)| w);

        assert_eq!(pairs[0].0, -1);
        assert!(
            (pairs[0].1 - 10.0).abs() < 1e-9,
            "window -1 sum: {}",
            pairs[0].1
        );

        assert_eq!(pairs[1].0, 0);
        assert!(
            (pairs[1].1 - 30.0).abs() < 1e-9,
            "window 0 sum: {}",
            pairs[1].1
        );

        assert_eq!(pairs[2].0, 1);
        assert!(
            (pairs[2].1 - 20.0).abs() < 1e-9,
            "window 1 sum: {}",
            pairs[2].1
        );
    }

    /// When k=1 (size == slide), sliding behaves identically to tumbling.
    #[tokio::test]
    async fn test_sliding_equals_tumbling_when_size_eq_slide() {
        let trades = vec![
            Trade {
                timestamp_ms: 100,
                price: 10.0,
            },
            Trade {
                timestamp_ms: 1100,
                price: 20.0,
            },
        ];

        let mut world_sliding = Dataset::new();
        world_sliding.register_component::<Trade>().unwrap();
        world_sliding.append::<Trade>(&trades).unwrap();

        let mut world_tumbling = Dataset::new();
        world_tumbling.register_component::<Trade>().unwrap();
        world_tumbling.append::<Trade>(&trades).unwrap();

        let sys_sliding = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Sliding {
                size_ms: 1000,
                slide_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        let sys_tumbling = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys_sliding.run(&mut world_sliding).await.unwrap();
        sys_tumbling.run(&mut world_tumbling).await.unwrap();

        let r_sliding = world_sliding.get_resource::<WindowResults>().unwrap();
        let r_tumbling = world_tumbling.get_resource::<WindowResults>().unwrap();
        assert_eq!(r_sliding.batches.len(), r_tumbling.batches.len());
        assert_eq!(r_sliding.total_rows(), r_tumbling.total_rows());
    }

    /// Amplification limit: k*N > 100_000_000 returns an error.
    #[tokio::test]
    async fn test_sliding_amplification_limit_returns_error() {
        // 1 row; size=10_000_000_000, slide=1 → k=10^10 exceeds limit.
        let trades = vec![Trade {
            timestamp_ms: 0,
            price: 1.0,
        }];

        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();
        pipeline.append::<Trade>(&trades).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Sliding {
                size_ms: 10_000_000_000,
                slide_ms: 1,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        let result = sys.run(&mut pipeline).await;
        assert!(result.is_err(), "expected amplification error, got Ok");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("amplification"),
            "error should mention amplification, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Issue 2: null timestamps must be silently skipped
    // -----------------------------------------------------------------------

    /// Rows with a null timestamp must be dropped before window assignment.
    /// The remaining non-null rows must still produce correct aggregate output.
    #[tokio::test]
    async fn test_null_timestamps_are_skipped() {
        use arrow_array::builder::{Float64Builder, Int64Builder};

        // Build a RecordBatch manually so we can inject a null timestamp.
        // Schema allows nullable timestamp.
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, true),
            Field::new("price", DataType::Float64, false),
        ]));

        let mut ts_builder = Int64Builder::new();
        ts_builder.append_value(100); // window 0
        ts_builder.append_null(); // should be dropped
        ts_builder.append_value(200); // window 0
        let ts_array = ts_builder.finish();

        let mut price_builder = Float64Builder::new();
        price_builder.append_value(10.0);
        price_builder.append_value(99.0); // this row is dropped
        price_builder.append_value(20.0);
        let price_array = price_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts_array) as ArrayRef,
                Arc::new(price_array) as ArrayRef,
            ],
        )
        .unwrap();

        let mut pipeline = Dataset::new();
        pipeline.register_raw_component("NullTrade", schema);
        pipeline.append_record_batch("NullTrade", batch).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("NullTrade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        // Only the two non-null rows (price 10.0 + 20.0 = 30.0) should appear.
        assert_eq!(results.batches.len(), 1, "expected one window group");
        let sum = results.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(
            (sum - 30.0).abs() < 1e-9,
            "expected sum 30.0 (null row skipped), got {sum}"
        );
    }

    // -----------------------------------------------------------------------
    // Issue 3: mean must ignore nulls in the denominator
    // -----------------------------------------------------------------------

    /// Mean of [10.0, null, 20.0] must be (10.0 + 20.0) / 2 = 15.0, not
    /// (10.0 + 20.0) / 3 ≈ 10.0.
    #[tokio::test]
    async fn test_mean_excludes_null_values_from_denominator() {
        use arrow_array::builder::{Float64Builder, Int64Builder};

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("price", DataType::Float64, true), // nullable price
        ]));

        let mut ts_builder = Int64Builder::new();
        ts_builder.append_value(100); // all in window 0
        ts_builder.append_value(200);
        ts_builder.append_value(300);
        let ts_array = ts_builder.finish();

        let mut price_builder = Float64Builder::new();
        price_builder.append_value(10.0);
        price_builder.append_null(); // must not count in denominator
        price_builder.append_value(20.0);
        let price_array = price_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts_array) as ArrayRef,
                Arc::new(price_array) as ArrayRef,
            ],
        )
        .unwrap();

        let mut pipeline = Dataset::new();
        pipeline.register_raw_component("NullPriceTrade", schema);
        pipeline
            .append_record_batch("NullPriceTrade", batch)
            .unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("NullPriceTrade", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Mean,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 1, "expected one window group");
        let mean = results.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(
            (mean - 15.0).abs() < 1e-9,
            "expected mean 15.0 (null excluded), got {mean}"
        );
    }

    /// Mean of all-null values must be NaN (not a divide-by-zero panic).
    #[tokio::test]
    async fn test_mean_all_nulls_returns_nan() {
        use arrow_array::builder::{Float64Builder, Int64Builder};

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("price", DataType::Float64, true),
        ]));

        let mut ts_builder = Int64Builder::new();
        ts_builder.append_value(100);
        ts_builder.append_value(200);
        let ts_array = ts_builder.finish();

        let mut price_builder = Float64Builder::new();
        price_builder.append_null();
        price_builder.append_null();
        let price_array = price_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ts_array) as ArrayRef,
                Arc::new(price_array) as ArrayRef,
            ],
        )
        .unwrap();

        let mut pipeline = Dataset::new();
        pipeline.register_raw_component("AllNullPrice", schema);
        pipeline.append_record_batch("AllNullPrice", batch).unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("AllNullPrice", "timestamp_ms")
            .window(WindowSpec::Tumbling {
                size_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Mean,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert_eq!(results.batches.len(), 1, "expected one window group");
        let mean = results.batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(
            mean.is_nan(),
            "expected NaN when all values are null, got {mean}"
        );
    }

    /// Empty pipeline with Sliding spec produces empty results (no panic).
    #[tokio::test]
    async fn test_sliding_empty_world_no_panic() {
        let mut pipeline = Dataset::new();
        pipeline.register_component::<Trade>().unwrap();

        let sys = WindowedSystemBuilder::new()
            .source("Trade", "timestamp_ms")
            .window(WindowSpec::Sliding {
                size_ms: 2000,
                slide_ms: 1000,
                offset_ms: 0,
            })
            .function(WindowFunction::Reduce {
                input_field: "price",
                aggregate: ReduceAggregate::Sum,
            })
            .build()
            .unwrap();

        sys.run(&mut pipeline).await.unwrap();

        let results = pipeline.get_resource::<WindowResults>().unwrap();
        assert!(results.batches.is_empty());
    }
}
