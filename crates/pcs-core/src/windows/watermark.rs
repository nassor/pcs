//! Watermark tracking for streaming semantics.
//!
//! A watermark is a monotonically increasing timestamp that signals "all events
//! with timestamps earlier than this value have been observed". Systems that
//! produce output based on event-time windows use watermarks to decide when a
//! window is complete and can be safely emitted.
//!
//! # Design
//!
//! [`WatermarkState`] maintains the current watermark (high-water mark across
//! all observed event timestamps) and an `allowed_lateness` tolerance window.
//! Late data — rows whose event timestamp falls before
//! `current_watermark - allowed_lateness` — is considered beyond the lateness
//! budget and gets routed to a side-output rather than re-processed.
//!
//! The watermark does **not** advance automatically; callers drive it via
//! [`WatermarkState::advance`].

/// Watermark tracking state for event-time streaming windows.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "windows")]
/// # {
/// use pcs_core::windows::watermark::WatermarkState;
///
/// let mut wm = WatermarkState::new(500); // 500 ms of allowed lateness
/// wm.advance(1_000);
/// assert_eq!(wm.current_watermark(), 1_000);
/// assert!(!wm.is_beyond_lateness(600)); // 1000 - 600 = 400 <= 500 → still late but accepted
/// assert!(wm.is_beyond_lateness(400));  // 1000 - 400 = 600 > 500 → dropped
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct WatermarkState {
    /// Current watermark: the highest event-timestamp seen so far (ms since epoch).
    ///
    /// Initialised to `i64::MIN` so that the first real timestamp always advances it.
    current_watermark: i64,
    /// Maximum allowed lateness in milliseconds.
    ///
    /// A row whose event-timestamp `ts` satisfies
    /// `ts >= current_watermark - allowed_lateness` is still eligible for
    /// late-data re-firing.  Rows with `ts < current_watermark - allowed_lateness`
    /// are beyond the lateness budget.
    allowed_lateness: i64,
}

impl WatermarkState {
    /// Create a new [`WatermarkState`] with the given allowed lateness.
    ///
    /// The initial watermark is `i64::MIN` (no data seen yet).
    ///
    /// # Arguments
    ///
    /// * `allowed_lateness` — maximum milliseconds a row's timestamp may fall
    ///   below the current watermark and still be eligible for late firing.
    ///   Pass `0` to drop all out-of-order data immediately.
    pub fn new(allowed_lateness: i64) -> Self {
        Self {
            current_watermark: i64::MIN,
            allowed_lateness,
        }
    }

    /// Advance the watermark to `ts` if `ts` is greater than the current value.
    ///
    /// Watermarks are monotonically non-decreasing; calling this with a value
    /// smaller than the current watermark has no effect.
    pub fn advance(&mut self, ts: i64) {
        if ts > self.current_watermark {
            self.current_watermark = ts;
        }
    }

    /// The current watermark (highest event-timestamp observed so far).
    pub fn current_watermark(&self) -> i64 {
        self.current_watermark
    }

    /// The configured allowed-lateness tolerance (milliseconds).
    pub fn allowed_lateness(&self) -> i64 {
        self.allowed_lateness
    }

    /// Returns `true` when `ts` is strictly before the lateness threshold.
    ///
    /// The threshold is `current_watermark - allowed_lateness`.  A row with
    /// timestamp `ts` is "beyond lateness" when
    /// `ts < current_watermark - allowed_lateness`, meaning it should be
    /// routed to a side-output or dropped.
    ///
    /// Returns `false` when the watermark has not yet been set (`i64::MIN`).
    pub fn is_beyond_lateness(&self, ts: i64) -> bool {
        if self.current_watermark == i64::MIN {
            return false;
        }
        // If allowed_lateness >= current_watermark, the threshold becomes <= 0,
        // which effectively means "infinite tolerance" — nothing is beyond lateness.
        if self.allowed_lateness >= self.current_watermark {
            return false;
        }
        // Subtraction is safe because allowed_lateness < current_watermark.
        let threshold = self.current_watermark - self.allowed_lateness;
        ts < threshold
    }

    /// Returns `true` when `ts` is late (below watermark) but still within the
    /// allowed-lateness window — i.e. eligible for late-data re-firing.
    ///
    /// ```text
    /// current_watermark - allowed_lateness  ≤  ts  <  current_watermark
    /// ```
    pub fn is_late_but_acceptable(&self, ts: i64) -> bool {
        if self.current_watermark == i64::MIN {
            return false;
        }
        ts < self.current_watermark && !self.is_beyond_lateness(ts)
    }

    /// Returns `true` when `ts` arrived on-time (≥ current watermark).
    pub fn is_on_time(&self, ts: i64) -> bool {
        self.current_watermark == i64::MIN || ts >= self.current_watermark
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_watermark_is_min() {
        let wm = WatermarkState::new(1000);
        assert_eq!(wm.current_watermark(), i64::MIN);
    }

    #[test]
    fn test_advance_increases_watermark() {
        let mut wm = WatermarkState::new(0);
        wm.advance(500);
        assert_eq!(wm.current_watermark(), 500);
        wm.advance(300); // backward — no effect
        assert_eq!(wm.current_watermark(), 500);
        wm.advance(1000);
        assert_eq!(wm.current_watermark(), 1000);
    }

    #[test]
    fn test_is_beyond_lateness_no_watermark_set() {
        let wm = WatermarkState::new(100);
        // No watermark yet → nothing is beyond lateness.
        assert!(!wm.is_beyond_lateness(-99999));
        assert!(!wm.is_beyond_lateness(0));
    }

    #[test]
    fn test_is_beyond_lateness_zero_allowed() {
        let mut wm = WatermarkState::new(0);
        wm.advance(1000);
        // threshold = 1000 - 0 = 1000; ts=999 < 1000 → beyond
        assert!(wm.is_beyond_lateness(999));
        // ts=1000 is not beyond (< 1000 is false)
        assert!(!wm.is_beyond_lateness(1000));
    }

    #[test]
    fn test_is_beyond_lateness_with_allowance() {
        let mut wm = WatermarkState::new(500);
        wm.advance(1000);
        // threshold = 1000 - 500 = 500
        assert!(!wm.is_beyond_lateness(500)); // exactly at threshold → not beyond
        assert!(wm.is_beyond_lateness(499));
        assert!(!wm.is_beyond_lateness(600)); // late but within window
        assert!(!wm.is_beyond_lateness(1000)); // on-time
    }

    #[test]
    fn test_is_late_but_acceptable() {
        let mut wm = WatermarkState::new(500);
        wm.advance(1000);
        // late but acceptable: 500 <= ts < 1000
        assert!(wm.is_late_but_acceptable(500));
        assert!(wm.is_late_but_acceptable(999));
        assert!(!wm.is_late_but_acceptable(499)); // beyond lateness
        assert!(!wm.is_late_but_acceptable(1000)); // on time, not late
    }

    #[test]
    fn test_is_on_time() {
        let mut wm = WatermarkState::new(500);
        // Before any advance, everything is on-time.
        assert!(wm.is_on_time(-1000));
        wm.advance(1000);
        assert!(wm.is_on_time(1000));
        assert!(wm.is_on_time(2000));
        assert!(!wm.is_on_time(999)); // late
    }

    #[test]
    fn test_watermark_large_allowed_lateness_does_not_panic() {
        // When allowed_lateness >= current_watermark, no data is beyond lateness
        // (effectively infinite tolerance).

        // Case 1: watermark=100, allowed_lateness=i64::MAX (>> 100).
        let mut wm = WatermarkState::new(i64::MAX);
        wm.advance(100);
        assert!(!wm.is_beyond_lateness(i64::MIN)); // allowed_lateness >= watermark → false
        assert!(!wm.is_beyond_lateness(0));
        assert!(!wm.is_beyond_lateness(100));

        // Case 2: watermark=i64::MAX, allowed_lateness=i64::MAX.
        let mut wm2 = WatermarkState::new(i64::MAX);
        wm2.advance(i64::MAX);
        assert!(!wm2.is_beyond_lateness(-1)); // allowed_lateness >= watermark → false
        assert!(!wm2.is_beyond_lateness(0));
        assert!(!wm2.is_beyond_lateness(i64::MAX));

        // Case 3: Normal case with reasonable values.
        // watermark=1000, allowed_lateness=500 → threshold=500.
        let mut wm3 = WatermarkState::new(500);
        wm3.advance(1000);
        assert!(wm3.is_beyond_lateness(499)); // 499 < 500 → beyond
        assert!(!wm3.is_beyond_lateness(500)); // 500 not < 500 → not beyond
        assert!(!wm3.is_beyond_lateness(1000)); // on-time
    }
}
