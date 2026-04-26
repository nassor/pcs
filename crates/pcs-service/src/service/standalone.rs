//! Standalone runner for [`BuiltService`].
//!
//! [`run_standalone`] drives a [`BuiltService`] through repeated processing
//! iterations in a single process with no distributed coordination. It handles
//! cancellation, transient source/sink errors, and run-mode pacing
//! (one-shot, continuous, or interval-based).
//!
//! ## Store-per-call semantics (WASM runtimes)
//!
//! When the runtime is a `WasmPipelineRuntime`, each call to `run_on` creates a
//! fresh wasmtime `Store`, resetting all guest linear memory. Any in-guest state
//! that must survive across iterations (accumulators, window buffers, per-pipeline
//! caches) must be round-tripped through the host via `snapshot`/`restore`. Guests
//! that rely on in-memory state across `run_on` calls will silently lose it.
//!
//! ## Example
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use tokio_util::sync::CancellationToken;
//! use pcs_service::service::standalone::{run_standalone, StandaloneStats};
//! // Build a BuiltService (via ServiceBuilder::build) then:
//! // let stats = run_standalone(built, &config, cancel).await?;
//! # }
//! ```

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::error::PcsError;
use crate::io::sink::drain_dataset;
use crate::io::source::drain_into_dataset;
use crate::pipeline::Dataset;

use super::builder::{BuiltService, BuiltSink};
use super::config::{RunMode, ServiceConfig, ServiceMode};

// ── StandaloneStats ───────────────────────────────────────────────────────────

/// Diagnostic counters accumulated over a [`run_standalone`] call.
///
/// Fields are public so the HTTP control plane can expose them via `/metrics`
/// by reading from a shared `Arc<RwLock<StandaloneStats>>`.
#[derive(Debug, Default, Clone)]
pub struct StandaloneStats {
    /// Total number of completed scheduler iterations.
    pub iterations: u64,
    /// Total number of source drain calls that returned at least one row.
    pub source_batches_drained: u64,
    /// Total rows loaded from sources across all iterations.
    pub rows_processed: u64,
    /// Total number of sink drain calls that wrote at least one row.
    pub sink_batches_written: u64,
    /// Count of non-fatal errors (source, scheduler, or sink failures).
    pub iteration_errors: u64,
    /// Wall-clock time from the first iteration to the last, in milliseconds.
    pub total_duration_ms: u64,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Promote a `String` to a `&'static str` via `Box::leak`.
///
/// Appropriate for service-lifetime strings (component names). The leaked
/// allocation lives until the process exits, which is fine for a service runner.
fn leak_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

// ── run_standalone ────────────────────────────────────────────────────────────

/// Drive a [`BuiltService`] through repeated processing iterations.
///
/// Returns [`StandaloneStats`] on success (including on cancellation, which is
/// treated as a clean exit).  Returns `Err` only for genuinely unrecoverable
/// conditions (e.g. internal invariant violations).
///
/// ## Live stats publishing
///
/// If `live_stats` is `Some`, the shared stats are updated after every
/// completed iteration so that `GET /status` reflects current progress.
/// The lock is held only briefly (one write per iteration).
///
/// ## Loop per iteration
///
/// 1. Check cancellation.
/// 2. Drain each source into the dataset.
/// 3. Run the runtime via `runtime.run_on(&mut dataset)`.
/// 4. Drain the dataset into each sink.
/// 5. Publish stats to `live_stats` (if provided).
/// 6. Clear the dataset for the next iteration.
/// 7. Sleep / exit based on [`RunMode`].
///
/// ## Error policy
///
/// - Source errors: log WARN, increment `iteration_errors`, continue.
/// - Runtime errors: log ERROR, increment `iteration_errors`, still flush sinks.
/// - Sink errors: log ERROR, increment `iteration_errors`, continue.
pub async fn run_standalone(
    built: BuiltService,
    config: &ServiceConfig,
    cancel: CancellationToken,
    live_stats: Option<Arc<RwLock<StandaloneStats>>>,
) -> Result<StandaloneStats, PcsError> {
    let run_mode = match &config.mode {
        ServiceMode::Standalone { config: sc } => sc.run_mode.clone(),
        ServiceMode::Cluster { .. } => {
            return Err(PcsError::configuration(
                "run_standalone called with a cluster-mode config; use the cluster runner instead",
            ));
        }
    };

    // Destructure BuiltService so we can hold runtime, sources, and sinks
    // without fighting the borrow checker. runtime is immutable (&self for
    // run_on), dataset is mutably borrowed for drains and run_on.
    let BuiltService {
        runtime,
        mut sources,
        mut sinks,
        registry: _,
    } = built;

    // Seed the working dataset from the runtime's component schema template.
    let mut dataset = runtime.template_dataset();

    // Promote component name strings to `&'static str` once at startup.
    let source_component_names: Vec<&'static str> = sources
        .iter()
        .map(|s| leak_str(s.target_component.clone()))
        .collect();

    let sink_component_names: Vec<&'static str> = sinks
        .iter()
        .map(|s| leak_str(s.source_component.clone()))
        .collect();

    let mut stats = StandaloneStats::default();
    let start = Instant::now();

    #[cfg(feature = "tracing")]
    tracing::info!(mode = ?run_mode, "standalone runner starting");

    loop {
        // ── 1. Cancellation check ────────────────────────────────────────────
        if cancel.is_cancelled() {
            #[cfg(feature = "tracing")]
            tracing::info!("standalone runner cancelled, draining in-flight work");
            flush_sinks(
                &dataset,
                &mut sinks,
                &sink_component_names,
                &mut stats,
                true,
            )
            .await;
            break;
        }

        let iter_start = Instant::now();

        #[cfg(feature = "tracing")]
        tracing::info!(iteration = stats.iterations + 1, mode = ?run_mode, "iteration starting");

        // ── 2. Drain sources ─────────────────────────────────────────────────
        let mut source_error = false;
        let mut cancelled_during_drain = false;
        for i in 0..sources.len() {
            let component_name = source_component_names[i];
            let result = tokio::select! {
                r = drain_into_dataset(sources[i].source.as_mut(), &mut dataset, component_name) => Some(r),
                _ = cancel.cancelled() => None,
            };
            let result = match result {
                None => {
                    #[cfg(feature = "tracing")]
                    tracing::info!("standalone runner cancelled during source drain");
                    cancelled_during_drain = true;
                    break;
                }
                Some(r) => r,
            };

            match result {
                Ok(rows) if rows > 0 => {
                    stats.source_batches_drained += 1;
                    stats.rows_processed += rows as u64;
                }
                Ok(_) => { /* empty drain, no-op */ }
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(source_name = %sources[i].name, error = %e, "source drain error (continuing)");
                    #[cfg(not(feature = "tracing"))]
                    let _ = &e;
                    stats.iteration_errors += 1;
                    source_error = true;
                }
            }
        }

        if cancelled_during_drain {
            flush_sinks(
                &dataset,
                &mut sinks,
                &sink_component_names,
                &mut stats,
                true,
            )
            .await;
            stats.total_duration_ms = start.elapsed().as_millis() as u64;
            return Ok(stats);
        }

        let _ = source_error; // kept for future max_consecutive_errors policy

        // ── 3. Run runtime ───────────────────────────────────────────────────
        let run_result = tokio::select! {
            r = runtime.run_on(&mut dataset) => r,
            _ = cancel.cancelled() => {
                #[cfg(feature = "tracing")]
                tracing::info!("standalone runner cancelled during runtime run");
                flush_sinks(&dataset, &mut sinks, &sink_component_names, &mut stats, true).await;
                stats.total_duration_ms = start.elapsed().as_millis() as u64;
                return Ok(stats);
            }
        };

        if let Err(e) = run_result {
            #[cfg(feature = "tracing")]
            tracing::error!(error = %e, "runtime error (continuing, attempting sink drain)");
            #[cfg(not(feature = "tracing"))]
            let _ = &e;
            stats.iteration_errors += 1;
            // Fall through to sink drain intentionally — don't skip it.
        }

        // ── 4. Drain sinks ───────────────────────────────────────────────────
        let is_oneshot_final = run_mode == RunMode::OneShot;
        let cancelled_before_sink = cancel.is_cancelled();
        flush_sinks(
            &dataset,
            &mut sinks,
            &sink_component_names,
            &mut stats,
            is_oneshot_final || cancelled_before_sink,
        )
        .await;

        if cancelled_before_sink {
            #[cfg(feature = "tracing")]
            tracing::info!("standalone runner cancelled after runtime, clean exit");
            break;
        }

        // ── 5. Increment counter, emit tracing ───────────────────────────────
        stats.iterations += 1;
        let iter_ms = iter_start.elapsed().as_millis() as u64;

        // ── 5a. Publish live stats so /status reflects current progress ──────
        if let Some(ref shared) = live_stats {
            *shared.write().await = stats.clone();
        }

        // ── 6. Clear dataset ─────────────────────────────────────────────────
        // Dataset::clear() resets row data but keeps component schemas registered.
        dataset.clear();

        #[cfg(feature = "tracing")]
        tracing::info!(
            iteration = stats.iterations,
            rows_processed = stats.rows_processed,
            duration_ms = iter_ms,
            "iteration complete"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = iter_ms;

        // ── 7. Sleep / exit based on RunMode ─────────────────────────────────
        match &run_mode {
            RunMode::OneShot => {
                #[cfg(feature = "tracing")]
                tracing::info!("one-shot mode: exiting after first iteration");
                break;
            }

            RunMode::Continuous => {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {}
                    _ = cancel.cancelled() => {
                        #[cfg(feature = "tracing")]
                        tracing::info!("standalone runner cancelled during continuous pause");
                        break;
                    }
                }
            }

            RunMode::Interval { interval_ms } => {
                let interval = tokio::time::Duration::from_millis(*interval_ms);
                let deadline = tokio::time::Instant::now() + interval;

                loop {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let slice = remaining.min(tokio::time::Duration::from_millis(100));
                    tokio::select! {
                        _ = tokio::time::sleep(slice) => {}
                        _ = cancel.cancelled() => {
                            #[cfg(feature = "tracing")]
                            tracing::info!("standalone runner cancelled during interval sleep");
                            stats.total_duration_ms = start.elapsed().as_millis() as u64;
                            return Ok(stats);
                        }
                    }
                }
            }
        }
    }

    // ── Clean shutdown ────────────────────────────────────────────────────────
    stats.total_duration_ms = start.elapsed().as_millis() as u64;

    #[cfg(feature = "tracing")]
    tracing::info!(
        iterations = stats.iterations,
        rows_processed = stats.rows_processed,
        iteration_errors = stats.iteration_errors,
        total_duration_ms = stats.total_duration_ms,
        "standalone runner clean shutdown"
    );

    Ok(stats)
}

async fn flush_sinks(
    dataset: &Dataset,
    sinks: &mut [BuiltSink],
    sink_component_names: &[&'static str],
    stats: &mut StandaloneStats,
    finish: bool,
) {
    for (i, built_sink) in sinks.iter_mut().enumerate() {
        let component_name = sink_component_names[i];

        match drain_dataset(dataset, component_name, built_sink.sink.as_mut()).await {
            Ok(rows) if rows > 0 => {
                stats.sink_batches_written += 1;
            }
            Ok(_) => { /* empty, no-op */ }
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(sink_name = %built_sink.name, error = %e, "sink drain error (continuing)");
                #[cfg(not(feature = "tracing"))]
                let _ = &e;
                stats.iteration_errors += 1;
            }
        }

        if finish && let Err(e) = built_sink.sink.finish().await {
            #[cfg(feature = "tracing")]
            tracing::error!(sink_name = %built_sink.name, error = %e, "sink finish error");
            #[cfg(not(feature = "tracing"))]
            let _ = &e;
            stats.iteration_errors += 1;
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "service"))]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use arrow_array::RecordBatch;
    use arrow_array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::error::PcsError;
    use crate::io::channel_sink::ChannelSink;
    use crate::io::channel_source::ChannelSource;
    use crate::io::sink::Sink;
    use crate::io::source::Source;
    use crate::pipeline::{Dataset, Pipeline};
    use crate::service::builder::{BuiltService, BuiltSink, BuiltSource};
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, PipelineSpec, RunMode, ServiceConfig,
        ServiceMode, StandaloneConfig,
    };
    use crate::service::registry::Registry;
    use crate::system::{System, SystemMeta};

    use super::run_standalone;

    // ── Schema / batch helpers ────────────────────────────────────────────────

    const COMP: &str = "values";

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn make_batch(schema: Arc<Schema>, values: &[i32]) -> RecordBatch {
        let arr: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    // ── NoopSystem ────────────────────────────────────────────────────────────

    struct NoopSystem;
    #[async_trait]
    impl System for NoopSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("noop")
        }
        async fn run(&self, _data: &mut Dataset) -> Result<(), PcsError> {
            Ok(())
        }
    }

    // ── FailingSystem ─────────────────────────────────────────────────────────

    struct FailingSystem;
    #[async_trait]
    impl System for FailingSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("failing")
        }
        async fn run(&self, _data: &mut Dataset) -> Result<(), PcsError> {
            Err(PcsError::generic("intentional test failure"))
        }
    }

    // ── FallibleSource ────────────────────────────────────────────────────────

    struct FallibleSource {
        schema: Arc<Schema>,
        call_count: u32,
        batches: Vec<RecordBatch>,
        pos: usize,
    }

    impl FallibleSource {
        fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                call_count: 0,
                batches,
                pos: 0,
            }
        }
    }

    #[async_trait]
    impl Source for FallibleSource {
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
            self.call_count += 1;
            if self.call_count.is_multiple_of(2) {
                return Err(PcsError::generic("FallibleSource: alternating error"));
            }
            if self.pos < self.batches.len() {
                let batch = self.batches[self.pos].clone();
                self.pos += 1;
                Ok(Some(batch))
            } else {
                Ok(None)
            }
        }
    }

    // ── FinishTrackingSink ────────────────────────────────────────────────────

    struct FinishTrackingSink {
        inner: ChannelSink,
        finish_called: Arc<Mutex<bool>>,
    }

    impl FinishTrackingSink {
        fn new(
            schema: Arc<Schema>,
            finish_called: Arc<Mutex<bool>>,
        ) -> (Self, mpsc::Receiver<RecordBatch>) {
            let (inner, rx) = ChannelSink::new(schema, 8);
            (
                Self {
                    inner,
                    finish_called,
                },
                rx,
            )
        }
    }

    #[async_trait]
    impl Sink for FinishTrackingSink {
        fn schema(&self) -> Arc<Schema> {
            self.inner.schema()
        }
        async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
            self.inner.write_batch(batch).await
        }
        async fn finish(&mut self) -> Result<(), PcsError> {
            *self.finish_called.lock().unwrap() = true;
            self.inner.finish().await
        }
    }

    // ── build_service helper ──────────────────────────────────────────────────

    fn build_service(
        system: Box<dyn System>,
    ) -> (
        BuiltService,
        mpsc::Sender<RecordBatch>,
        mpsc::Receiver<RecordBatch>,
    ) {
        let schema = test_schema();

        let (tx, src) = ChannelSource::new(schema.clone(), 16);
        let built_source = BuiltSource {
            name: "test_source".to_string(),
            target_component: COMP.to_string(),
            source: Box::new(src),
        };

        let (sink, rx) = ChannelSink::new(schema.clone(), 16);
        let built_sink = BuiltSink {
            name: "test_sink".to_string(),
            source_component: COMP.to_string(),
            sink: Box::new(sink),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline.data_mut().register_raw_component(COMP, schema);
        pipeline.add_system_boxed(system);

        let service = BuiltService::from_runtime(
            Box::new(pipeline),
            vec![built_source],
            vec![built_sink],
            Registry::new(),
        );

        (service, tx, rx)
    }

    fn make_config(run_mode: RunMode) -> ServiceConfig {
        ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: std::path::PathBuf::from("/tmp/pcs-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig { run_mode },
            },
            pipeline: PipelineSpec {
                systems: vec![],
                components: vec![],
                #[cfg(feature = "wasm")]
                wasm: None,
            },
            sources: vec![],
            sinks: vec![],
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }

    // ── Test 1: OneShot runs exactly one iteration ────────────────────────────

    #[tokio::test]
    async fn test_oneshot_runs_one_iteration_and_exits() {
        let schema = test_schema();
        let (service, tx, mut rx) = build_service(Box::new(NoopSystem));
        let config = make_config(RunMode::OneShot);

        for _ in 0..3 {
            tx.send(make_batch(schema.clone(), &[1, 2, 3]))
                .await
                .unwrap();
        }
        drop(tx);

        let cancel = CancellationToken::new();
        let stats = run_standalone(service, &config, cancel, None)
            .await
            .unwrap();

        assert_eq!(stats.iterations, 1, "should complete exactly one iteration");
        assert_eq!(stats.rows_processed, 9, "3 batches × 3 rows");
        assert_eq!(stats.iteration_errors, 0);

        let mut total_sink_rows = 0usize;
        while let Ok(batch) = rx.try_recv() {
            total_sink_rows += batch.num_rows();
        }
        assert_eq!(total_sink_rows, 9, "sink should receive all 9 rows");
    }

    // ── Test 2: Continuous mode processes multiple iterations ─────────────────

    #[tokio::test]
    async fn test_continuous_multiple_iterations() {
        let schema = test_schema();
        let (service, tx, _rx) = build_service(Box::new(NoopSystem));
        let config = make_config(RunMode::Continuous);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let local = tokio::task::LocalSet::new();
        let config_owned = config.clone();
        let handle = local.spawn_local(async move {
            run_standalone(service, &config_owned, cancel_clone, None).await
        });

        let schema_clone = schema.clone();
        let feeder = local.spawn_local(async move {
            for _ in 0..5 {
                let _ = tx.send(make_batch(schema_clone.clone(), &[1])).await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        local
            .run_until(async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                cancel.cancel();
            })
            .await;

        let stats = local
            .run_until(async { handle.await.unwrap().unwrap() })
            .await;
        feeder.abort();

        assert!(
            stats.iterations >= 2,
            "expected ≥2 iterations in continuous mode, got {}",
            stats.iterations
        );
        assert_eq!(stats.iteration_errors, 0);
    }

    // ── Test 3: Interval mode honors the sleep ────────────────────────────────

    #[tokio::test]
    async fn test_interval_mode_honors_sleep() {
        let mut pipeline = Pipeline::new("test");
        pipeline.add_system_boxed(Box::new(NoopSystem));
        let service =
            BuiltService::from_runtime(Box::new(pipeline), vec![], vec![], Registry::new());

        let config = make_config(RunMode::Interval { interval_ms: 200 });
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let wall_start = std::time::Instant::now();
        let config_owned = config.clone();
        let local = tokio::task::LocalSet::new();
        let handle = local.spawn_local(async move {
            run_standalone(service, &config_owned, cancel_clone, None).await
        });

        local
            .run_until(async {
                tokio::time::sleep(Duration::from_millis(750)).await;
                cancel.cancel();
            })
            .await;

        let stats = local
            .run_until(async { handle.await.unwrap().unwrap() })
            .await;
        let elapsed = wall_start.elapsed();

        assert!(
            stats.iterations >= 3,
            "expected ≥3 iterations in interval mode (200ms), got {}; elapsed={elapsed:?}",
            stats.iterations
        );
        assert!(
            elapsed >= Duration::from_millis(600),
            "expected ≥600ms elapsed for 3 intervals, got {elapsed:?}"
        );
    }

    // ── Test 4: Cancellation exits cleanly ────────────────────────────────────

    #[tokio::test]
    async fn test_cancellation_exits_cleanly() {
        let (service, _tx, _rx) = build_service(Box::new(NoopSystem));
        let config = make_config(RunMode::Continuous);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let config_owned = config.clone();
        let local = tokio::task::LocalSet::new();
        let handle = local.spawn_local(async move {
            run_standalone(service, &config_owned, cancel_clone, None).await
        });

        local
            .run_until(async {
                tokio::time::sleep(Duration::from_millis(150)).await;
                cancel.cancel();
            })
            .await;

        let result = local.run_until(async { handle.await.unwrap() }).await;
        assert!(
            result.is_ok(),
            "cancellation should return Ok, got: {:?}",
            result
        );
        let stats = result.unwrap();
        assert_eq!(stats.iteration_errors, 0);
    }

    // ── Test 5: Source error doesn't kill the service ─────────────────────────

    #[tokio::test]
    async fn test_source_error_does_not_kill_service() {
        let schema = test_schema();
        let batches = vec![make_batch(schema.clone(), &[1, 2, 3])];
        let fallible = FallibleSource::new(schema.clone(), batches);

        let (sink, _rx) = ChannelSink::new(schema.clone(), 8);
        let built_sink = BuiltSink {
            name: "test_sink".to_string(),
            source_component: COMP.to_string(),
            sink: Box::new(sink),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline.data_mut().register_raw_component(COMP, schema);
        pipeline.add_system_boxed(Box::new(NoopSystem));

        let service = BuiltService::from_runtime(
            Box::new(pipeline),
            vec![BuiltSource {
                name: "fallible".to_string(),
                target_component: COMP.to_string(),
                source: Box::new(fallible),
            }],
            vec![built_sink],
            Registry::new(),
        );

        let config = make_config(RunMode::OneShot);
        let cancel = CancellationToken::new();
        let stats = run_standalone(service, &config, cancel, None)
            .await
            .unwrap();

        assert_eq!(stats.iterations, 1);
        assert_eq!(
            stats.iteration_errors, 1,
            "expected 1 source error, got {}",
            stats.iteration_errors
        );
    }

    // ── Test 6: Runtime error doesn't skip sink drain ────────────────────────

    #[tokio::test]
    async fn test_pipeline_error_still_drains_sink() {
        let schema = test_schema();
        let (service, tx, mut rx) = build_service(Box::new(FailingSystem));
        let config = make_config(RunMode::OneShot);

        tx.send(make_batch(schema.clone(), &[10, 20]))
            .await
            .unwrap();
        drop(tx);

        let cancel = CancellationToken::new();
        let stats = run_standalone(service, &config, cancel, None)
            .await
            .unwrap();

        assert!(
            stats.iteration_errors >= 1,
            "expected at least 1 error from failing runtime"
        );
        assert_eq!(stats.iterations, 1);

        let mut sink_rows = 0;
        while let Ok(b) = rx.try_recv() {
            sink_rows += b.num_rows();
        }
        assert_eq!(
            sink_rows, 2,
            "sink should receive the 2 rows from the dataset despite runtime error"
        );
    }

    // ── Test 7: Empty sources — runtime still runs ────────────────────────────

    #[tokio::test]
    async fn test_no_sources_runtime_still_runs() {
        let mut pipeline = Pipeline::new("test");
        pipeline.add_system_boxed(Box::new(NoopSystem));

        let service =
            BuiltService::from_runtime(Box::new(pipeline), vec![], vec![], Registry::new());

        let config = make_config(RunMode::OneShot);
        let cancel = CancellationToken::new();
        let stats = run_standalone(service, &config, cancel, None)
            .await
            .unwrap();

        assert_eq!(
            stats.iterations, 1,
            "runtime should run one iteration even with no sources"
        );
        assert_eq!(stats.rows_processed, 0);
        assert_eq!(stats.iteration_errors, 0);
    }

    // ── Test 8: Sink finish is called on clean exit ────────────────────────────

    #[tokio::test]
    async fn test_sink_finish_called_on_oneshot_exit() {
        let schema = test_schema();
        let finish_called = Arc::new(Mutex::new(false));
        let (tracking_sink, _rx) = FinishTrackingSink::new(schema.clone(), finish_called.clone());

        let (tx, src) = ChannelSource::new(schema.clone(), 8);
        let built_source = BuiltSource {
            name: "src".to_string(),
            target_component: COMP.to_string(),
            source: Box::new(src),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline
            .data_mut()
            .register_raw_component(COMP, schema.clone());
        pipeline.add_system_boxed(Box::new(NoopSystem));

        let service = BuiltService::from_runtime(
            Box::new(pipeline),
            vec![built_source],
            vec![BuiltSink {
                name: "tracking".to_string(),
                source_component: COMP.to_string(),
                sink: Box::new(tracking_sink),
            }],
            Registry::new(),
        );

        tx.send(make_batch(schema.clone(), &[1])).await.unwrap();
        drop(tx);

        let config = make_config(RunMode::OneShot);
        let cancel = CancellationToken::new();
        run_standalone(service, &config, cancel, None)
            .await
            .unwrap();

        assert!(
            *finish_called.lock().unwrap(),
            "finish() should have been called on the sink during OneShot exit"
        );
    }

    // ── BurstSource ──────────────────────────────────────────────────────────

    struct BurstSource {
        schema: Arc<Schema>,
        queue: Arc<Mutex<Vec<RecordBatch>>>,
    }

    impl BurstSource {
        fn new(schema: Arc<Schema>) -> (Arc<Mutex<Vec<RecordBatch>>>, Self) {
            let queue = Arc::new(Mutex::new(Vec::new()));
            let src = Self {
                schema,
                queue: queue.clone(),
            };
            (queue, src)
        }
    }

    #[async_trait]
    impl Source for BurstSource {
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
            let batch = self.queue.lock().unwrap().pop();
            Ok(batch)
        }
    }

    // ── Test 9: Dataset clear between iterations ──────────────────────────────

    #[tokio::test]
    async fn test_world_clear_between_iterations() {
        let row_counts = Arc::new(Mutex::new(Vec::<usize>::new()));

        struct RecordingSystem {
            counts: Arc<Mutex<Vec<usize>>>,
        }

        #[async_trait]
        impl System for RecordingSystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("recording")
            }
            async fn run(&self, data: &mut Dataset) -> Result<(), PcsError> {
                self.counts.lock().unwrap().push(data.rows());
                Ok(())
            }
        }

        let schema = test_schema();
        let (queue, burst_src) = BurstSource::new(schema.clone());

        let (sink, _rx) = ChannelSink::new(schema.clone(), 8);
        let built_sink = BuiltSink {
            name: "sink".to_string(),
            source_component: COMP.to_string(),
            sink: Box::new(sink),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline
            .data_mut()
            .register_raw_component(COMP, schema.clone());
        pipeline.add_system_boxed(Box::new(RecordingSystem {
            counts: row_counts.clone(),
        }));

        let service = BuiltService::from_runtime(
            Box::new(pipeline),
            vec![BuiltSource {
                name: "burst".to_string(),
                target_component: COMP.to_string(),
                source: Box::new(burst_src),
            }],
            vec![built_sink],
            Registry::new(),
        );

        for _ in 0..10 {
            queue.lock().unwrap().push(make_batch(schema.clone(), &[1]));
        }

        let config = make_config(RunMode::Interval { interval_ms: 30 });
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let config_owned = config.clone();
        let queue_clone = queue.clone();
        let local = tokio::task::LocalSet::new();
        let handle = local.spawn_local(async move {
            run_standalone(service, &config_owned, cancel_clone, None).await
        });

        let schema_clone = schema.clone();
        local
            .run_until(async move {
                tokio::time::sleep(Duration::from_millis(80)).await;
                for _ in 0..5 {
                    queue_clone
                        .lock()
                        .unwrap()
                        .push(make_batch(schema_clone.clone(), &[2]));
                }
                tokio::time::sleep(Duration::from_millis(80)).await;
                cancel.cancel();
            })
            .await;
        local
            .run_until(async { handle.await.unwrap().unwrap() })
            .await;

        let counts = row_counts.lock().unwrap().clone();
        assert!(
            counts.len() >= 2,
            "expected ≥2 recorded row counts, got {counts:?}"
        );

        let max_rows = counts.iter().copied().max().unwrap_or(0);
        assert!(
            max_rows <= 10,
            "dataset should be cleared between iterations; max rows seen = {max_rows}, counts = {counts:?}"
        );
    }

    // ── Test: Cluster config returns configuration error ──────────────────────

    #[tokio::test]
    async fn test_cluster_config_returns_error() {
        use crate::service::config::{ClusterConfig, PeerSpec};

        let cluster_config = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: std::path::PathBuf::from("/tmp/pcs"),
            },
            mode: ServiceMode::Cluster {
                config: ClusterConfig {
                    peers: vec![PeerSpec {
                        id: 1,
                        addr: "127.0.0.1:9000".to_string(),
                    }],
                    bootstrap: true,
                    lease_ttl_ms: 30_000,
                    election_timeout_ms: 1_500,
                    heartbeat_interval_ms: 300,
                    snapshot_log_interval: 10_000,
                },
            },
            pipeline: PipelineSpec {
                systems: vec![],
                components: vec![],
                #[cfg(feature = "wasm")]
                wasm: None,
            },
            sources: vec![],
            sinks: vec![],
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
        };

        let pipeline = Pipeline::new("test");
        let service =
            BuiltService::from_runtime(Box::new(pipeline), vec![], vec![], Registry::new());

        let cancel = CancellationToken::new();
        let result = run_standalone(service, &cluster_config, cancel, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.category(), "configuration");
    }

    // ── Test: live_stats is updated during runner execution ───────────────────

    #[tokio::test]
    async fn test_live_stats_updated_during_execution() {
        use crate::service::StandaloneStats;
        use tokio::sync::RwLock;

        let schema = test_schema();
        let (queue, burst_src) = BurstSource::new(schema.clone());

        for _ in 0..3 {
            queue
                .lock()
                .unwrap()
                .push(make_batch(schema.clone(), &[1, 2, 3]));
        }

        let (sink, _rx) = ChannelSink::new(schema.clone(), 16);
        let built_sink = BuiltSink {
            name: "test_sink".to_string(),
            source_component: COMP.to_string(),
            sink: Box::new(sink),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline
            .data_mut()
            .register_raw_component(COMP, schema.clone());
        pipeline.add_system_boxed(Box::new(NoopSystem));

        let service = BuiltService::from_runtime(
            Box::new(pipeline),
            vec![BuiltSource {
                name: "burst".to_string(),
                target_component: COMP.to_string(),
                source: Box::new(burst_src),
            }],
            vec![built_sink],
            Registry::new(),
        );

        let config = make_config(RunMode::Continuous);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let live = Arc::new(RwLock::new(StandaloneStats::default()));
        let live_clone = live.clone();

        let local = tokio::task::LocalSet::new();
        let config_owned = config.clone();
        let handle = local.spawn_local(async move {
            run_standalone(service, &config_owned, cancel_clone, Some(live_clone)).await
        });

        local
            .run_until(async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                cancel.cancel();
                handle.await.unwrap().unwrap();
            })
            .await;

        let snapshot = live.read().await;
        assert!(
            snapshot.iterations > 0,
            "live stats should be updated after at least one iteration, got iterations={}",
            snapshot.iterations
        );
        assert!(
            snapshot.rows_processed > 0,
            "live stats should show rows processed, got rows_processed={}",
            snapshot.rows_processed
        );
    }
}
