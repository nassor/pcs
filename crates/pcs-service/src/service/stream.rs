//! Stream runner for [`BuiltService`].
//!
//! [`run_stream`] drives a [`BuiltService`] one source batch at a time: each
//! arriving [`RecordBatch`](arrow_array::RecordBatch) is appended to a cleared
//! dataset, pushed through the runtime, and drained to every sink before the
//! next item is pulled. There is no inter-item sleep: latency is bounded by the
//! pipeline itself, not by a pacing timer.
//!
//! Selected by `run_mode` `kind = "stream"` in standalone mode;
//! [`run_standalone`](super::standalone::run_standalone) dispatches here.
//!
//! ## Semantics
//!
//! - **Exactly one source.** TOML configs are checked by
//!   [`ServiceConfig::validate`](super::config::ServiceConfig::validate);
//!   hand-built services are checked here.
//! - **State carry.** The blob returned by `run_on_with_state` is fed back as
//!   `prior` on the next item, so guest state survives across items even
//!   though the WASM store does not. The blob lives in loop memory only: it is
//!   never checkpointed and is lost on restart.
//! - **At-most-once.** An item whose runtime call fails is dropped (logged and
//!   counted); `prior` is left untouched so the next item resumes from the last
//!   good state.
//! - **Sink finalisation.** Sinks are drained per item but `finish()` is called
//!   once, at exit (source EOF or cancellation).

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::error::PcsError;
use crate::io::sink::drain_dataset;

use super::builder::{BuiltService, BuiltSink};
use super::standalone::{StandaloneStats, leak_str};

/// Minimum spacing between `live_stats` publishes.
///
/// A per-item `RwLock` write would dominate a sub-millisecond item budget, so
/// the shared snapshot is refreshed at most this often (plus once at exit).
const PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

/// Backoff after a source error, so a permanently failing source cannot spin
/// the loop at full speed.
const SOURCE_ERROR_BACKOFF: Duration = Duration::from_millis(10);

/// Drive a [`BuiltService`] in stream mode: one pipeline invocation per
/// arriving source batch.
///
/// Returns [`StandaloneStats`] on success. Cancellation and source EOF are both
/// clean exits. Returns `Err` only for configuration violations detected at
/// entry.
///
/// `stats.iterations` counts items, `stats.total_busy_micros` sums per-item
/// processing time, and `stats.max_item_micros` records the slowest item.
///
/// ## Error policy
///
/// Mirrors [`run_standalone`](super::standalone::run_standalone): log, count in
/// `iteration_errors`, continue with the next item. Source errors additionally
/// back off for 10 ms (cancellable) to avoid a hot error loop.
pub async fn run_stream(
    built: BuiltService,
    cancel: CancellationToken,
    live_stats: Option<Arc<RwLock<StandaloneStats>>>,
) -> Result<StandaloneStats, PcsError> {
    if built.sources.len() != 1 {
        return Err(PcsError::configuration(format!(
            "stream mode requires exactly one source ({} configured)",
            built.sources.len()
        )));
    }

    let BuiltService {
        runtime,
        mut sources,
        mut sinks,
        registry: _,
    } = built;

    let mut dataset = runtime.template_dataset();

    // Promote component name strings to `&'static str` once at startup.
    let source_component = leak_str(sources[0].target_component.clone());
    let sink_component_names: Vec<&'static str> = sinks
        .iter()
        .map(|s| leak_str(s.source_component.clone()))
        .collect();

    let mut stats = StandaloneStats::default();
    let start = Instant::now();
    let mut last_publish = Instant::now();
    // Runtime-internal state carried from item to item (WIT checkpoint blob).
    let mut prior: Option<Vec<u8>> = None;

    #[cfg(feature = "tracing")]
    tracing::info!(source = %sources[0].name, "stream runner starting");

    loop {
        let next = tokio::select! {
            r = sources[0].source.next_batch() => r,
            _ = cancel.cancelled() => {
                #[cfg(feature = "tracing")]
                tracing::info!("stream runner cancelled while waiting for input");
                break;
            }
        };

        let batch = match next {
            Ok(None) => {
                #[cfg(feature = "tracing")]
                tracing::info!("stream source reached EOF");
                break;
            }
            Ok(Some(batch)) => batch,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(source_name = %sources[0].name, error = %_e, "stream source error (continuing)");
                stats.iteration_errors += 1;
                tokio::select! {
                    _ = tokio::time::sleep(SOURCE_ERROR_BACKOFF) => {}
                    _ = cancel.cancelled() => break,
                }
                continue;
            }
        };

        let item_start = Instant::now();
        let rows = batch.num_rows() as u64;

        dataset.clear();
        if let Err(_e) = dataset.append_record_batch(source_component, batch) {
            #[cfg(feature = "tracing")]
            tracing::warn!(component = source_component, error = %_e, "stream item append failed (dropping item)");
            stats.iteration_errors += 1;
            continue;
        }

        let run_result = tokio::select! {
            r = runtime.run_on_with_state(&mut dataset, prior.as_deref()) => r,
            _ = cancel.cancelled() => {
                #[cfg(feature = "tracing")]
                tracing::info!("stream runner cancelled during runtime run");
                finish_sinks(&mut sinks, &mut stats).await;
                stats.total_duration_ms = start.elapsed().as_millis() as u64;
                publish(&live_stats, &stats).await;
                return Ok(stats);
            }
        };

        match run_result {
            // The checkpoint is persisted verbatim: `None` means the runtime
            // carries no state, so it must clear `prior` too.
            Ok(state) => prior = state,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(error = %_e, "stream runtime error (dropping item)");
                stats.iteration_errors += 1;
                continue;
            }
        }

        for (i, built_sink) in sinks.iter_mut().enumerate() {
            match drain_dataset(&dataset, sink_component_names[i], built_sink.sink.as_mut()).await {
                Ok(n) if n > 0 => stats.sink_batches_written += 1,
                Ok(_) => { /* empty, no-op */ }
                Err(_e) => {
                    #[cfg(feature = "tracing")]
                    tracing::error!(sink_name = %built_sink.name, error = %_e, "stream sink drain error (continuing)");
                    stats.iteration_errors += 1;
                }
            }
        }

        let item_micros = item_start.elapsed().as_micros() as u64;
        stats.iterations += 1;
        stats.rows_processed += rows;
        stats.total_busy_micros += item_micros;
        stats.max_item_micros = stats.max_item_micros.max(item_micros);

        if live_stats.is_some() && last_publish.elapsed() >= PUBLISH_INTERVAL {
            publish(&live_stats, &stats).await;
            last_publish = Instant::now();
        }
    }

    finish_sinks(&mut sinks, &mut stats).await;
    stats.total_duration_ms = start.elapsed().as_millis() as u64;
    publish(&live_stats, &stats).await;

    #[cfg(feature = "tracing")]
    tracing::info!(
        items = stats.iterations,
        rows_processed = stats.rows_processed,
        iteration_errors = stats.iteration_errors,
        total_busy_micros = stats.total_busy_micros,
        max_item_micros = stats.max_item_micros,
        total_duration_ms = stats.total_duration_ms,
        "stream runner clean shutdown"
    );

    Ok(stats)
}

/// Finalise every sink. The dataset was already drained per item, so only
/// `finish` remains.
async fn finish_sinks(sinks: &mut [BuiltSink], stats: &mut StandaloneStats) {
    for built_sink in sinks.iter_mut() {
        if let Err(_e) = built_sink.sink.finish().await {
            #[cfg(feature = "tracing")]
            tracing::error!(sink_name = %built_sink.name, error = %_e, "stream sink finish error");
            stats.iteration_errors += 1;
        }
    }
}

async fn publish(shared: &Option<Arc<RwLock<StandaloneStats>>>, stats: &StandaloneStats) {
    if let Some(shared) = shared {
        *shared.write().await = stats.clone();
    }
}
