//! `pcs-service serve`: start the PCS service.
//!
//! Loads config, initialises logging and OpenTelemetry, builds the service from
//! registered factories, wires the HTTP control plane and watchdog, then
//! dispatches to the standalone or cluster runner. Waits for SIGINT or SIGTERM
//! before draining all tasks within the 30-second shutdown budget.
//!
//! The `ready` flag flips as soon as the runner is spawned, not after the first
//! successful pipeline iteration. In cluster mode `cluster_probe` is `None`, so
//! `/status` reports `"cluster": null`.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use opentelemetry_prometheus::exporter;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tokio::sync::RwLock;

use pcs_service::PcsError;
use pcs_service::service::config::{LogFormat, ServiceConfig, ServiceMode};
use pcs_service::service::factories::register_builtin_factories;
use pcs_service::service::http::{ServiceModeLabel, ServiceState};
use pcs_service::service::standalone::StandaloneStats;
use pcs_service::service::{
    ServiceBuilder, ShutdownCoordinator, register_standard_metrics, serve_http, spawn_watchdog,
    validate_io_coverage,
};

use crate::cli::{GlobalOpts, LogFormatArg, ServeArgs};

/// Entry point for the `serve` subcommand.
pub async fn run(global: &GlobalOpts, args: &ServeArgs) -> Result<(), PcsError> {
    let config_path = global
        .config
        .as_ref()
        .ok_or_else(|| PcsError::configuration("--config is required for serve"))?;
    let mut config = ServiceConfig::load(config_path)?;

    if let Some(node_id) = args.node_id {
        config.node.id = node_id;
    }
    if let Some(level) = &global.log_level {
        config.observability.log_level = level.clone();
    }
    if let Some(format) = &global.log_format {
        config.observability.log_format = match format {
            LogFormatArg::Pretty => LogFormat::Pretty,
            LogFormatArg::Json => LogFormat::Json,
        };
    }
    if let Some(port) = args.port {
        // Replace only the port portion of the existing bind address.
        let existing = &config.http.bind;
        let host = existing
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or("127.0.0.1");
        config.http.bind = format!("{host}:{port}");
    }

    // Logging must be initialised before any tracing call.
    pcs_service::service::init_logging(&config.observability)?;
    tracing::info!(node_id = config.node.id, "pcs-service starting");

    let prometheus_registry = Arc::new(prometheus::Registry::new());
    let otel_exporter = exporter()
        .with_registry((*prometheus_registry).clone())
        .build()
        .map_err(|e| PcsError::generic(format!("failed to build OTel exporter: {e}")))?;
    let provider = SdkMeterProvider::builder()
        .with_reader(otel_exporter)
        .build();
    opentelemetry::global::set_meter_provider(provider);
    register_standard_metrics();

    // Forks of this binary register their own runtime and IO factories here:
    // register_builtin_factories(ServiceBuilder::new()).with_runtime(...).
    let builder = register_builtin_factories(ServiceBuilder::new());
    #[cfg(feature = "wasm")]
    let has_wasm = config.pipeline.wasm.is_some();
    #[cfg(not(feature = "wasm"))]
    let has_wasm = false;
    let builder = if !has_wasm {
        builder.with_runtime(Box::new(pcs_service::pipeline::Pipeline::new(
            "pcs-service",
        )))
    } else {
        builder
    };
    let built = builder.build(&config)?;

    // Gate 3: IO targets must be covered by the runtime's declared components.
    {
        let declared = built.runtime.declared_components();
        validate_io_coverage(&declared, &config).map_err(|e| {
            PcsError::configuration(format!("semantic validation failed: {}", e.message()))
        })?;
    }

    let coord = ShutdownCoordinator::new(Duration::from_secs(30));

    let liveness = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let mode_label = match &config.mode {
        ServiceMode::Standalone { .. } => ServiceModeLabel::Standalone,
        ServiceMode::Cluster { .. } => ServiceModeLabel::Cluster,
    };
    let standalone_stats: Option<Arc<RwLock<StandaloneStats>>> = match &config.mode {
        ServiceMode::Standalone { .. } => Some(Arc::new(RwLock::new(StandaloneStats::default()))),
        ServiceMode::Cluster { .. } => None,
    };

    let state = ServiceState {
        node_id: config.node.id,
        node_name: config.node.name.clone(),
        mode: mode_label,
        started_at: Instant::now(),
        prometheus_registry,
        liveness: liveness.clone(),
        ready: ready.clone(),
        // No cluster probe: in cluster mode /status reports "cluster": null.
        cluster_probe: None,
        standalone_stats: standalone_stats.clone(),
    };

    let watchdog_handle = spawn_watchdog(state.clone(), coord.child());

    // Resolve the bind address and print it so test harnesses can read the
    // port. For port 0 we pre-bind to learn the OS-assigned port, drop the
    // temporary listener, and let serve_http rebind. The gap is safe because
    // the OS does not hand out ephemeral ports in LIFO order.
    let http_bind_addr: std::net::SocketAddr = config.http.bind.parse().map_err(|e| {
        PcsError::configuration(format!(
            "invalid HTTP bind address '{}': {e}",
            config.http.bind
        ))
    })?;
    let resolved_addr = if http_bind_addr.port() == 0 {
        let tmp = tokio::net::TcpListener::bind(http_bind_addr)
            .await
            .map_err(|e| {
                PcsError::generic(format!(
                    "failed to probe HTTP bind address {http_bind_addr}: {e}"
                ))
            })?;
        let addr = tmp
            .local_addr()
            .map_err(|e| PcsError::generic(format!("failed to read local address: {e}")))?;
        drop(tmp);
        // Update config so serve_http binds the same concrete port.
        config.http.bind = addr.to_string();
        addr
    } else {
        http_bind_addr
    };
    println!("pcs-service listening on {resolved_addr}");

    let http_config = config.http.clone();
    let http_state = state.clone();
    let http_cancel = coord.child();
    let http_handle = tokio::spawn(async move {
        if let Err(e) = serve_http(&http_config, http_state, http_cancel).await {
            tracing::error!(error = %e, "http server failed");
        }
    });

    // Ready flips once the runner is spawned, not after the first pipeline
    // iteration completes.
    ready.store(true, Ordering::Relaxed);

    // The runner runs inline rather than under tokio::spawn: BuiltService holds
    // a Box<dyn Sink> that is Send but not Sync, and running inline avoids the
    // Future: Send bound.
    let runner_cancel = coord.child();
    let runner_config = config.clone();
    let runner_stats = standalone_stats.clone();

    let runner_fut = async move {
        match runner_config.mode {
            ServiceMode::Standalone { .. } => pcs_service::service::run_standalone(
                built,
                &runner_config,
                runner_cancel,
                runner_stats,
            )
            .await
            .map(|_| ()),
            #[cfg(feature = "service-cluster")]
            ServiceMode::Cluster { .. } => {
                pcs_service::service::run_cluster(built, &runner_config, runner_cancel)
                    .await
                    .map(|_| ())
            }
            // `service`-only build: the TOML parses so operators get a clear
            // message instead of a cryptic parse error, but cluster mode needs
            // the Raft stack.
            #[cfg(not(feature = "service-cluster"))]
            ServiceMode::Cluster { .. } => {
                // Drop the runner-only inputs explicitly so the async move
                // closure still captures them.
                drop(built);
                drop(runner_cancel);
                drop(runner_stats);
                Err(PcsError::configuration(
                    "config requests `mode: cluster`, but this binary was built \
                     without the `service-cluster` feature — rebuild with \
                     `--features service-cluster` to enable cluster mode",
                ))
            }
        }
    };

    // `wait_for_signal` consumes `coord` and cancels the root token itself;
    // clone the token first so the runner-exits-first path can do the same.
    let shutdown_token = coord.root();

    let runner_result = tokio::select! {
        _ = coord.wait_for_signal() => {
            tracing::info!("shutdown signal received");
            Ok(())
        }
        result = runner_fut => {
            tracing::info!("runner exited before shutdown signal; initiating shutdown");
            result
        }
    };

    // Idempotent: cancels the HTTP and watchdog child tokens on both paths.
    shutdown_token.cancel();

    if let Err(ref e) = runner_result {
        tracing::error!(error = %e, "runner exited with error");
    }

    let drain_coord = ShutdownCoordinator::new(Duration::from_secs(30));
    let clean = drain_coord.drain(vec![http_handle, watchdog_handle]).await;
    if !clean {
        tracing::error!("shutdown budget exceeded; forcing exit");
        std::process::exit(1);
    }
    tracing::info!("pcs-service stopped cleanly");

    // Cancellation from a clean shutdown signal is not an error.
    runner_result
}
