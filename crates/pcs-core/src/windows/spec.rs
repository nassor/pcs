//! Window specifications: tumbling, sliding, session, and (future) global windows.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_select::take::take;

use crate::error::PcsError;

use super::hash::compute_key_hash;

/// A window specification defining the geometry and boundaries of windows.
#[derive(Debug, Clone)]
pub enum WindowSpec {
    /// Fixed-size, non-overlapping windows.
    /// Each record belongs to exactly one window.
    /// Window boundaries: `floor_div(ts - offset, size)`
    Tumbling {
        /// Window size in milliseconds.
        size_ms: i64,
        /// Alignment offset in milliseconds (default 0).
        offset_ms: i64,
    },
    /// Gap-based session windows.
    ///
    /// A new session begins whenever the gap between consecutive events
    /// (within the same key) exceeds `gap_ms` milliseconds.  All events
    /// within the same unbroken run belong to the same session.
    Session {
        /// Inactivity gap in milliseconds that delimits session boundaries.
        gap_ms: i64,
    },
    /// Overlapping fixed-size windows that advance by a slide interval.
    ///
    /// Each record belongs to `k = ceil(size_ms / slide_ms)` windows.
    /// Window boundaries follow the same floor-division alignment as
    /// tumbling windows but with step size `slide_ms` instead of `size_ms`.
    Sliding {
        /// Window size in milliseconds.
        size_ms: i64,
        /// Slide (advance) interval in milliseconds. Must be ≤ `size_ms`.
        slide_ms: i64,
        /// Alignment offset in milliseconds (default 0).
        offset_ms: i64,
    },
    // Future:
    // Global,
}

impl WindowSpec {
    /// Assign a window_id to a timestamp (milliseconds since epoch).
    ///
    /// For Tumbling: `floor_div(ts - offset, size)`.
    ///
    /// # Correctness Note
    ///
    /// Standard division truncates toward zero. For negative dividends,
    /// this differs from floor division. E.g., `-5 / 3 = -1` (truncate)
    /// but `floor(-5 / 3) = -2`. This function implements true floor division.
    pub fn assign_tumbling(ts: i64, size_ms: i64, offset_ms: i64) -> i64 {
        let ts = ts - offset_ms;
        // Floor division: (a / b) - (1 if (a % b) != 0 && sign(a) != sign(b) else 0)
        // Simplified for i64: use (a / b) - (if (a % b) < 0 { 1 } else { 0 })
        let q = ts / size_ms;
        let r = ts % size_ms;
        if r < 0 { q - 1 } else { q }
    }

    /// Compute the `k = ceil(size_ms / slide_ms)` window IDs that contain `ts`.
    ///
    /// Each window in a sliding specification is identified by the tumbling
    /// window ID computed with step size `slide_ms`:
    ///
    /// ```text
    /// window_id[j] = assign_tumbling(ts - j * slide_ms, slide_ms, offset_ms)
    ///                for j in 0..k
    /// ```
    ///
    /// The returned `Vec` has exactly `k` elements (may contain duplicates at
    /// boundaries when `size_ms` is not a multiple of `slide_ms`).
    ///
    /// # Panics
    ///
    /// Does not panic; `slide_ms` must be > 0 (the caller is responsible for
    /// validation before calling this function).
    pub fn assign_sliding(ts: i64, size_ms: i64, slide_ms: i64, offset_ms: i64) -> Vec<i64> {
        // k = ceil(size_ms / slide_ms)
        let k = (size_ms + slide_ms - 1) / slide_ms;
        (0..k)
            .map(|j| Self::assign_tumbling(ts - j * slide_ms, slide_ms, offset_ms))
            .collect()
    }
}

/// Assign session IDs to every row in `batch`.
///
/// Algorithm:
/// 1. Compute a per-row key hash from `key_cols` (empty slice = global, all rows same key).
/// 2. Sort rows by `(key_hash, ts_ms)` using Arrow's lexicographic sort.
/// 3. Scan the sorted order: start a new session whenever the key changes
///    or the gap to the previous event exceeds `gap_ms`.
/// 4. Map sorted session IDs back to the original row order.
///
/// Returns an `Int64Array` of length `batch.num_rows()` where `result[i]` is
/// the session ID for original row `i`.  Session IDs are zero-based and
/// monotonically increasing in the sort order.
///
/// # Errors
///
/// Returns `PcsError::Generic` if key-hashing, sorting, or index-mapping fails.
pub fn assign_sessions(
    ts_ms: &Int64Array,
    key_cols: &[&ArrayRef],
    gap_ms: i64,
) -> Result<Int64Array, PcsError> {
    let n = ts_ms.len();
    if n == 0 {
        return Ok(Int64Array::from(Vec::<i64>::new()));
    }

    // ------------------------------------------------------------------
    // 1. Compute key hash
    // ------------------------------------------------------------------
    let key_hash = if key_cols.is_empty() {
        Int64Array::from(vec![0i64; n])
    } else {
        compute_key_hash(key_cols)?
    };

    // ------------------------------------------------------------------
    // 2. Sort by (key_hash, ts_ms)
    // ------------------------------------------------------------------
    let sort_cols = vec![
        SortColumn {
            values: Arc::new(key_hash.clone()) as ArrayRef,
            options: None,
        },
        SortColumn {
            values: Arc::new(ts_ms.clone()) as ArrayRef,
            options: None,
        },
    ];
    let sorted_indices = lexsort_to_indices(&sort_cols, None)
        .map_err(|e| PcsError::generic(format!("assign_sessions: sort error: {e}")))?;

    // Reorder key_hash and ts_ms to the sorted order.
    let sorted_key_hash = take(&key_hash as &dyn Array, &sorted_indices, None)
        .map_err(|e| PcsError::generic(format!("assign_sessions: take key_hash: {e}")))?;
    let sorted_ts = take(ts_ms as &dyn Array, &sorted_indices, None)
        .map_err(|e| PcsError::generic(format!("assign_sessions: take ts: {e}")))?;

    let sorted_key_hash = sorted_key_hash
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| PcsError::generic("assign_sessions: downcast key_hash failed"))?;
    let sorted_ts = sorted_ts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| PcsError::generic("assign_sessions: downcast ts failed"))?;

    // ------------------------------------------------------------------
    // 3. Scan sorted rows and assign session IDs
    // ------------------------------------------------------------------
    let mut sorted_session_ids = vec![0i64; n];
    let mut session_id = 0i64;

    // Iterate over consecutive (prev, curr) index pairs starting at 1.
    // We need raw indices to call `.value(i)` on the Arrow arrays, so we
    // zip an offset-by-one range rather than indexing `sorted_session_ids`
    // directly — the write target is accessed via a separate mutable ref.
    let keys = sorted_key_hash.values();
    let timestamps = sorted_ts.values();
    for (prev_idx, slot) in sorted_session_ids[1..].iter_mut().enumerate() {
        let curr_idx = prev_idx + 1;
        if keys[curr_idx] != keys[prev_idx]
            || (timestamps[curr_idx] - timestamps[prev_idx]) > gap_ms
        {
            session_id += 1;
        }
        *slot = session_id;
    }

    // ------------------------------------------------------------------
    // 4. Map sorted session IDs back to original row order
    // ------------------------------------------------------------------
    // sorted_indices[sort_pos] = original_row_idx
    // We need result[original_row_idx] = sorted_session_ids[sort_pos]
    let mut result = vec![0i64; n];
    for (sort_pos, &orig_row) in sorted_indices.values().iter().enumerate() {
        result[orig_row as usize] = sorted_session_ids[sort_pos];
    }

    Ok(Int64Array::from(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};

    use super::*;

    // -----------------------------------------------------------------------
    // assign_sessions tests
    // -----------------------------------------------------------------------

    fn str_col(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    /// Single key, two clear sessions separated by a large gap.
    #[test]
    fn test_assign_sessions_single_key_two_sessions() {
        // gap_ms = 1000; events at 0, 100, 200 then 5000, 5100 → 2 sessions
        let ts = Int64Array::from(vec![0i64, 100, 200, 5000, 5100]);
        let ids = assign_sessions(&ts, &[], 1000).unwrap();
        assert_eq!(ids.len(), 5);
        // All first three in session 0, last two in session 1
        assert_eq!(ids.value(0), ids.value(1));
        assert_eq!(ids.value(1), ids.value(2));
        assert_ne!(ids.value(2), ids.value(3));
        assert_eq!(ids.value(3), ids.value(4));
    }

    /// Single key, all events close together → one session.
    #[test]
    fn test_assign_sessions_single_key_one_session() {
        let ts = Int64Array::from(vec![0i64, 500, 999]);
        let ids = assign_sessions(&ts, &[], 1000).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids.value(0), ids.value(1));
        assert_eq!(ids.value(1), ids.value(2));
    }

    /// Exact gap boundary: gap == gap_ms is NOT a new session; gap > gap_ms IS.
    #[test]
    fn test_assign_sessions_exact_gap_boundary() {
        // gap_ms = 1000; ts diff of 1000 is not > 1000 so same session
        let ts = Int64Array::from(vec![0i64, 1000, 2001]);
        let ids = assign_sessions(&ts, &[], 1000).unwrap();
        assert_eq!(ids.value(0), ids.value(1), "gap==gap_ms: same session");
        assert_ne!(ids.value(1), ids.value(2), "gap>gap_ms: new session");
    }

    /// Multiple keys: each key gets its own independent session numbering.
    #[test]
    fn test_assign_sessions_multi_key() {
        // key A: ts 0, 100, 5000   (gap after 100 → new session for A)
        // key B: ts 200, 300       (one session for B)
        // Interleaved in original order: A0, B0, A1, B1, A2
        let ts = Int64Array::from(vec![0i64, 200, 100, 300, 5000]);
        let key_col = str_col(&["A", "B", "A", "B", "A"]);
        let key_ref: ArrayRef = key_col;
        let ids = assign_sessions(&ts, &[&key_ref], 1000).unwrap();
        assert_eq!(ids.len(), 5);

        // A events are at original indices 0, 2, 4
        let a0 = ids.value(0); // ts=0
        let a1 = ids.value(2); // ts=100 (same session as ts=0, gap=100 <= 1000)
        let a2 = ids.value(4); // ts=5000 (new session, gap=4900 > 1000)
        assert_eq!(a0, a1, "A: ts=0 and ts=100 same session");
        assert_ne!(a1, a2, "A: ts=5000 is a new session");

        // B events are at original indices 1, 3
        let b0 = ids.value(1); // ts=200
        let b1 = ids.value(3); // ts=300 (gap=100 <= 1000, same session)
        assert_eq!(b0, b1, "B: ts=200 and ts=300 same session");
    }

    /// Single event → always session 0.
    #[test]
    fn test_assign_sessions_single_event() {
        let ts = Int64Array::from(vec![42i64]);
        let ids = assign_sessions(&ts, &[], 500).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.value(0), 0);
    }

    /// Empty input → empty output.
    #[test]
    fn test_assign_sessions_empty() {
        let ts = Int64Array::from(Vec::<i64>::new());
        let ids = assign_sessions(&ts, &[], 1000).unwrap();
        assert_eq!(ids.len(), 0);
    }

    // -----------------------------------------------------------------------
    // assign_tumbling tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_tumbling_assign_positive_ts() {
        // Timestamp 1500 ms, window size 1000 ms, no offset
        // floor(1500 / 1000) = 1
        assert_eq!(WindowSpec::assign_tumbling(1500, 1000, 0), 1);
    }

    #[test]
    fn test_tumbling_assign_with_offset() {
        // Timestamp 1500 ms, window size 1000 ms, offset 500 ms
        // floor((1500 - 500) / 1000) = floor(1000 / 1000) = 1
        assert_eq!(WindowSpec::assign_tumbling(1500, 1000, 500), 1);

        // Timestamp 1200 ms, window size 1000 ms, offset 500 ms
        // floor((1200 - 500) / 1000) = floor(700 / 1000) = 0
        assert_eq!(WindowSpec::assign_tumbling(1200, 1000, 500), 0);
    }

    #[test]
    fn test_tumbling_assign_negative_ts() {
        // Negative timestamp: -1500 ms, window size 1000 ms, no offset
        // floor(-1500 / 1000) = floor(-1.5) = -2
        assert_eq!(WindowSpec::assign_tumbling(-1500, 1000, 0), -2);

        // Timestamp -500 ms, window size 1000 ms, no offset
        // floor(-500 / 1000) = floor(-0.5) = -1
        assert_eq!(WindowSpec::assign_tumbling(-500, 1000, 0), -1);
    }

    #[test]
    fn test_tumbling_boundary_alignment() {
        // Window boundaries at multiples of size
        // [0, 1000), [1000, 2000), [2000, 3000), etc.
        assert_eq!(WindowSpec::assign_tumbling(0, 1000, 0), 0);
        assert_eq!(WindowSpec::assign_tumbling(999, 1000, 0), 0);
        assert_eq!(WindowSpec::assign_tumbling(1000, 1000, 0), 1);
        assert_eq!(WindowSpec::assign_tumbling(1999, 1000, 0), 1);
        assert_eq!(WindowSpec::assign_tumbling(2000, 1000, 0), 2);
    }

    // -----------------------------------------------------------------------
    // assign_sliding tests
    // -----------------------------------------------------------------------

    /// size=10, slide=5 → k=2; each record belongs to exactly 2 windows.
    #[test]
    fn test_assign_sliding_k2() {
        // ts=7, size=10, slide=5, offset=0
        // j=0: assign_tumbling(7, 5, 0) = floor(7/5) = 1
        // j=1: assign_tumbling(7 - 5, 5, 0) = assign_tumbling(2, 5, 0) = floor(2/5) = 0
        let ids = WindowSpec::assign_sliding(7, 10, 5, 0);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 1);
        assert_eq!(ids[1], 0);
    }

    /// size=15, slide=5 → k=3; each record belongs to 3 windows.
    #[test]
    fn test_assign_sliding_k3() {
        // ts=12, size=15, slide=5, offset=0
        // j=0: floor(12/5) = 2
        // j=1: floor(7/5)  = 1
        // j=2: floor(2/5)  = 0
        let ids = WindowSpec::assign_sliding(12, 15, 5, 0);
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], 2);
        assert_eq!(ids[1], 1);
        assert_eq!(ids[2], 0);
    }

    /// size == slide (degenerate case: same as tumbling, k=1).
    #[test]
    fn test_assign_sliding_equals_tumbling_when_size_eq_slide() {
        let ids = WindowSpec::assign_sliding(1500, 1000, 1000, 0);
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], WindowSpec::assign_tumbling(1500, 1000, 0));
    }

    /// With offset: same floor-division shift as tumbling.
    #[test]
    fn test_assign_sliding_with_offset() {
        // ts=600, size=1000, slide=500, offset=100
        // k = ceil(1000/500) = 2
        // j=0: assign_tumbling(600, 500, 100) = floor((600-100)/500) = floor(500/500) = 1
        // j=1: assign_tumbling(100, 500, 100) = floor((100-100)/500) = floor(0/500) = 0
        let ids = WindowSpec::assign_sliding(600, 1000, 500, 100);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 1);
        assert_eq!(ids[1], 0);
    }

    /// Negative timestamps use floor division, same as tumbling.
    #[test]
    fn test_assign_sliding_negative_ts() {
        // ts=-3, size=10, slide=5, offset=0
        // k=2
        // j=0: assign_tumbling(-3, 5, 0) = floor(-3/5) = -1
        // j=1: assign_tumbling(-8, 5, 0) = floor(-8/5) = -2
        let ids = WindowSpec::assign_sliding(-3, 10, 5, 0);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], -1);
        assert_eq!(ids[1], -2);
    }

    /// Non-divisible size/slide: k rounds up correctly.
    #[test]
    fn test_assign_sliding_non_divisible_ceil() {
        // size=7, slide=3 → k = ceil(7/3) = 3
        let ids = WindowSpec::assign_sliding(5, 7, 3, 0);
        assert_eq!(ids.len(), 3);
    }
}
