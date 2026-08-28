//! Retry wrappers for [`Source`] and [`Sink`].
//!
//! [`RetryingSource`] and [`RetryingSink`] wrap any [`Source`] or [`Sink`]
//! and run every fallible operation with the retry policy of a
//! [`SystemConfig`], so a transient connector failure is retried with
//! exponential backoff instead of failing the whole run. A source's
//! `Ok(None)` (EOF) is not an error and is returned as-is.
//!
//! The wrappers retry every error: `PcsError` carries no retryable
//! classification, and connectors validate their configuration at build time,
//! so a configuration error never surfaces at run time. The cost of retrying a
//! permanent error is a bounded backoff window; a [`SystemConfig`] with
//! [`RetryMode::None`] (one attempt) disables retrying.
//!
//! The host's `ServiceBuilder` applies these wrappers to every config-driven
//! source and sink in `pcs-service`. An embedder that calls a factory directly
//! gets the wrapper only if it constructs one itself.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::error::PcsError;
use crate::io::sink::Sink;
use crate::io::source::Source;
use crate::retry::SystemConfig;
#[cfg(feature = "tracing")]
use tracing::Instrument as _;

/// A [`Source`] that retries failed [`next_batch`](Source::next_batch) calls
/// with exponential backoff.
///
/// Each `next_batch` is retried up to `config.retry_mode.max_attempts()`
/// times and either recovers or surfaces `PcsError::RetryExhausted`. A
/// recovered call logs a warning carrying the retry count under the `tracing`
/// feature. `Ok(None)` (EOF) passes through without retrying.
pub struct RetryingSource<S: Source> {
    inner: S,
    config: SystemConfig,
    what: String,
}

impl<S: Source> RetryingSource<S> {
    /// Wrap `inner` with `config`'s retry policy.
    ///
    /// `what` names the node in log lines, typically the node id.
    pub fn new(inner: S, config: SystemConfig, what: &str) -> Self {
        Self {
            inner,
            config,
            what: what.to_string(),
        }
    }
}

#[async_trait]
impl<S: Source + 'static> Source for RetryingSource<S> {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        let config = self.config;
        let (batch, retries) = retry_source_next_batch(&config, &mut self.inner).await?;
        log_recovered("source", &self.what, retries);
        Ok(batch)
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.inner.estimated_rows()
    }
}

/// A [`Sink`] that retries failed [`write_batch`](Sink::write_batch) and
/// [`finish`](Sink::finish) calls with exponential backoff.
///
/// Each call is retried up to `config.retry_mode.max_attempts()` times and
/// either recovers or surfaces `PcsError::RetryExhausted`. A recovered call
/// logs a warning carrying the retry count under the `tracing` feature. A
/// failed `finish` after a partial flush retries the flush, preserving the
/// at-least-once semantics connectors already provide.
pub struct RetryingSink<S: Sink> {
    inner: S,
    config: SystemConfig,
    what: String,
}

impl<S: Sink> RetryingSink<S> {
    /// Wrap `inner` with `config`'s retry policy.
    ///
    /// `what` names the node in log lines, typically the node id.
    pub fn new(inner: S, config: SystemConfig, what: &str) -> Self {
        Self {
            inner,
            config,
            what: what.to_string(),
        }
    }
}

#[async_trait]
impl<S: Sink + 'static> Sink for RetryingSink<S> {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        let config = self.config;
        let retries = retry_sink_write_batch(&config, &mut self.inner, batch).await?;
        log_recovered("sink", &self.what, retries);
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), PcsError> {
        let config = self.config;
        let retries = retry_sink_finish(&config, &mut self.inner).await?;
        log_recovered("sink", &self.what, retries);
        Ok(())
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn pending_rows(&self) -> Option<usize> {
        self.inner.pending_rows()
    }
}

/// One `next_batch` pass through the retry policy, returning the batch and the
/// number of retries consumed.
///
/// This loops directly over [`RetryMode`](crate::retry::RetryMode) instead of
/// calling [`run_with_retries`] because the wrappers' `#[async_trait]` impls
/// must prove their futures `Send` at the trait's higher-ranked lifetime, and
/// rustc cannot prove that for an async closure capturing `&mut` state.
/// Semantics match `run_with_retries` exactly: same attempt counting, same
/// `delay_for_attempt` delays, same `PcsError::RetryExhausted` wrapping.
async fn retry_source_next_batch(
    config: &SystemConfig,
    inner: &mut dyn Source,
) -> Result<(Option<RecordBatch>, u32), PcsError> {
    let max_attempts = config.retry_mode.max_attempts();
    let mut attempt = 0usize;
    loop {
        let result = {
            #[cfg(feature = "tracing")]
            let span = tracing::info_span!("task_attempt", attempt = attempt + 1, max_attempts);
            #[cfg(feature = "tracing")]
            let result = inner.next_batch().instrument(span).await;
            #[cfg(not(feature = "tracing"))]
            let result = inner.next_batch().await;
            result
        };
        match result {
            Ok(batch) => return Ok((batch, attempt as u32)),
            Err(e) => {
                attempt += 1;
                if attempt >= max_attempts {
                    #[cfg(feature = "tracing")]
                    tracing::error!(error = %e, attempts = attempt, "retry exhausted, giving up");
                    return Err(PcsError::retry_exhausted(e, attempt));
                }
                if let Some(delay) = config.retry_mode.delay_for_attempt(attempt - 1) {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        error = %e,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        delay_ms = delay.as_millis(),
                        "attempt failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

/// One `write_batch` pass through the retry policy, returning the number of
/// retries consumed. See [`retry_source_next_batch`] for why this loops
/// directly instead of using [`run_with_retries`].
async fn retry_sink_write_batch(
    config: &SystemConfig,
    inner: &mut dyn Sink,
    batch: &RecordBatch,
) -> Result<u32, PcsError> {
    let max_attempts = config.retry_mode.max_attempts();
    let mut attempt = 0usize;
    loop {
        let result = {
            #[cfg(feature = "tracing")]
            let span = tracing::info_span!("task_attempt", attempt = attempt + 1, max_attempts);
            #[cfg(feature = "tracing")]
            let result = inner.write_batch(batch).instrument(span).await;
            #[cfg(not(feature = "tracing"))]
            let result = inner.write_batch(batch).await;
            result
        };
        match result {
            Ok(()) => return Ok(attempt as u32),
            Err(e) => {
                attempt += 1;
                if attempt >= max_attempts {
                    #[cfg(feature = "tracing")]
                    tracing::error!(error = %e, attempts = attempt, "retry exhausted, giving up");
                    return Err(PcsError::retry_exhausted(e, attempt));
                }
                if let Some(delay) = config.retry_mode.delay_for_attempt(attempt - 1) {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        error = %e,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        delay_ms = delay.as_millis(),
                        "attempt failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

/// One `finish` pass through the retry policy, returning the number of retries
/// consumed. See [`retry_source_next_batch`] for why this loops directly
/// instead of using [`run_with_retries`].
async fn retry_sink_finish(config: &SystemConfig, inner: &mut dyn Sink) -> Result<u32, PcsError> {
    let max_attempts = config.retry_mode.max_attempts();
    let mut attempt = 0usize;
    loop {
        let result = {
            #[cfg(feature = "tracing")]
            let span = tracing::info_span!("task_attempt", attempt = attempt + 1, max_attempts);
            #[cfg(feature = "tracing")]
            let result = inner.finish().instrument(span).await;
            #[cfg(not(feature = "tracing"))]
            let result = inner.finish().await;
            result
        };
        match result {
            Ok(()) => return Ok(attempt as u32),
            Err(e) => {
                attempt += 1;
                if attempt >= max_attempts {
                    #[cfg(feature = "tracing")]
                    tracing::error!(error = %e, attempts = attempt, "retry exhausted, giving up");
                    return Err(PcsError::retry_exhausted(e, attempt));
                }
                if let Some(delay) = config.retry_mode.delay_for_attempt(attempt - 1) {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        error = %e,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        delay_ms = delay.as_millis(),
                        "attempt failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

/// Log a warning when an operation recovered after one or more retries.
fn log_recovered(kind: &str, what: &str, retries: u32) {
    if retries > 0 {
        #[cfg(feature = "tracing")]
        tracing::warn!("{kind} '{what}' recovered after {retries} retries");
        #[cfg(not(feature = "tracing"))]
        let _ = (kind, what, retries);
    }
}

#[cfg(all(test, feature = "io"))]
mod tests {
    use super::*;
    use crate::error::PcsError;
    use crate::retry::RetryMode;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap()
    }

    fn fast_backoff(max_retries: usize) -> SystemConfig {
        SystemConfig {
            retry_mode: RetryMode::exponential_custom(
                max_retries,
                Duration::from_millis(1),
                2.0,
                Duration::from_millis(10),
                0.0,
            ),
        }
    }

    struct FlakySource {
        failures_left: usize,
        calls: AtomicUsize,
    }

    impl FlakySource {
        fn failing(failures_left: usize) -> Self {
            Self {
                failures_left,
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Source for FlakySource {
        fn schema(&self) -> Arc<Schema> {
            schema()
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.failures_left > 0 {
                self.failures_left -= 1;
                Err(PcsError::generic("flaky source failure"))
            } else {
                Ok(Some(batch()))
            }
        }
    }

    struct EofSource {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Source for EofSource {
        fn schema(&self) -> Arc<Schema> {
            schema()
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }

    struct FlakySink {
        write_failures_left: usize,
        finish_failures_left: usize,
        write_calls: AtomicUsize,
        finish_calls: AtomicUsize,
    }

    impl FlakySink {
        fn new(write_failures_left: usize, finish_failures_left: usize) -> Self {
            Self {
                write_failures_left,
                finish_failures_left,
                write_calls: AtomicUsize::new(0),
                finish_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Sink for FlakySink {
        async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<(), PcsError> {
            self.write_calls.fetch_add(1, Ordering::SeqCst);
            if self.write_failures_left > 0 {
                self.write_failures_left -= 1;
                Err(PcsError::generic("flaky sink write failure"))
            } else {
                Ok(())
            }
        }

        async fn finish(&mut self) -> Result<(), PcsError> {
            self.finish_calls.fetch_add(1, Ordering::SeqCst);
            if self.finish_failures_left > 0 {
                self.finish_failures_left -= 1;
                Err(PcsError::generic("flaky sink finish failure"))
            } else {
                Ok(())
            }
        }

        fn schema(&self) -> Arc<Schema> {
            schema()
        }
    }

    #[tokio::test]
    async fn a_source_that_fails_then_succeeds_is_recovered() {
        let mut source =
            RetryingSource::new(FlakySource::failing(2), fast_backoff(3), "test-source");
        let out = source.next_batch().await.unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(source.inner.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhaustion_returns_the_last_error_wrapped() {
        let mut source = RetryingSource::new(
            FlakySource::failing(usize::MAX),
            fast_backoff(2),
            "test-source",
        );
        let err = source.next_batch().await.unwrap_err();
        let PcsError::RetryExhausted {
            source: last_error,
            attempts,
        } = err
        else {
            panic!("expected RetryExhausted");
        };
        assert_eq!(attempts, 3);
        assert!(matches!(*last_error, PcsError::Generic(_)));
        assert_eq!(source.inner.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_minimal_config_surfaces_the_first_error_immediately() {
        let mut source = RetryingSource::new(
            FlakySource::failing(usize::MAX),
            SystemConfig::minimal(),
            "test-source",
        );
        assert!(source.next_batch().await.is_err());
        assert_eq!(source.inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_eof_is_returned_as_is() {
        let mut source = RetryingSource::new(
            EofSource {
                calls: AtomicUsize::new(0),
            },
            fast_backoff(3),
            "test-source",
        );
        assert!(source.next_batch().await.unwrap().is_none());
        assert_eq!(source.inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_sink_that_fails_then_succeeds_is_recovered() {
        let mut sink = RetryingSink::new(FlakySink::new(1, 0), fast_backoff(3), "test-sink");
        sink.write_batch(&batch()).await.unwrap();
        assert_eq!(sink.inner.write_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn finish_is_retried_too() {
        let mut sink = RetryingSink::new(FlakySink::new(0, 1), fast_backoff(3), "test-sink");
        sink.finish().await.unwrap();
        assert_eq!(sink.inner.finish_calls.load(Ordering::SeqCst), 2);
    }
}
