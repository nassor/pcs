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
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tokio::sync::RwLock;

use pcs_service::PcsError;
#[cfg(feature = "tikv-store")]
use pcs_service::distributed::TikvStoreConfig;
use pcs_service::service::TikvStateClient;
use pcs_service::service::config::{LogFormat, ServiceConfig, ServiceMode};
use pcs_service::service::factories::register_builtin_factories;
use pcs_service::service::http::{ServiceModeLabel, ServiceState};
use pcs_service::service::standalone::StandaloneStats;
use pcs_service::service::{ServiceBuilder, ShutdownCoordinator, serve_http, spawn_watchdog};

use crate::cli::{GlobalOpts, LogFormatArg, ServeArgs};

/// Entry point for the `serve` subcommand.
pub async fn run(global: &GlobalOpts, args: &ServeArgs) -> Result<(), PcsError> {
    let config_path = &global.config;
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
    if let Some(endpoint) = &global.otlp_endpoint {
        config.observability.otlp_endpoint = Some(endpoint.clone());
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

    // Logging must be initialised before any tracing call. The inspector is
    // built here too, because its capture layer joins the same subscriber.
    let (telemetry, inspector) =
        pcs_service::service::init_logging(&config.observability, config.node.id)?;
    tracing::info!(node_id = config.node.id, "pcs-service starting");
    // With a `store` block configured, persist the raw config file (pre
    // env-substitution, so `${VAR}` secrets stay as references) before the
    // pipeline builds; an unreachable store fails startup here rather than
    // mid-run. `validate` and `cluster init` do not write.
    let tikv: Option<Arc<TikvStateClient>> = match &config.store {
        #[cfg(feature = "tikv-store")]
        Some(_) => {
            let tcfg = TikvStoreConfig::try_from(config.store.as_ref().expect("matched Some"))?;
            let client = Arc::new(TikvStateClient::connect(&tcfg).await?);
            let raw = std::fs::read(config_path).map_err(|e| {
                PcsError::configuration(format!(
                    "reading config file {}: {e}",
                    config_path.display()
                ))
            })?;
            let name = config_path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "pcs.kdl".to_string());
            client.put_config(&name, &raw).await?;
            tracing::info!(config = %name, "pipeline config persisted to TiKV");
            Some(client)
        }
        #[cfg(not(feature = "tikv-store"))]
        Some(_) => unreachable!("validate rejects `store` without tikv-store"),
        None => None,
    };

    let prometheus_registry = Arc::new(prometheus::Registry::new());
    let otel_exporter = exporter()
        // Instrument names already carry the `_total` convention, so the
        // exporter must not append a second one.
        .without_counter_suffixes()
        .with_registry((*prometheus_registry).clone())
        .build()
        .map_err(|e| PcsError::generic(format!("failed to build OTel exporter: {e}")))?;
    // Both readers live on one provider: `with_reader` is additive and each
    // reader gets its own aggregation pipeline, so the inspector's cumulative
    // temporality cannot disturb the Prometheus reader's.
    let mut provider_builder = SdkMeterProvider::builder().with_reader(otel_exporter);
    if let Some(inspector) = &inspector {
        provider_builder = provider_builder.with_reader(
            PeriodicReader::builder(inspector.metric_exporter())
                .with_interval(config.observability.inspector.sample_interval())
                .build(),
        );
    }
    // `metrics::init()` forces the instrument LazyLock, so it must follow the
    // provider install: instruments bind to whichever provider is current when
    // they are first built.
    opentelemetry::global::set_meter_provider(provider_builder.build());
    pcs_service::metrics::init();

    // Forks of this binary register their own runtime and IO factories here:
    // register_builtin_factories(ServiceBuilder::new()).with_runtime(id, ...).
    let builder = register_builtin_factories(ServiceBuilder::new());
    // The builder publishes the topology into the inspector once it knows the
    // runtime and the source/sink sets.
    let builder = match inspector.clone() {
        Some(inspector) => builder.with_inspector(inspector),
        None => builder,
    };
    // `ServiceBuilder::build_all` validates each workflow's graph (rule:
    // matching components and field-for-field identical Arrow schemas end to
    // end) before returning, so a config/runtime mismatch fails here rather
    // than on the first pipeline iteration.
    let built = builder.build_all(&config)?;

    let coord = ShutdownCoordinator::new(Duration::from_secs(30));

    let liveness = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let mode_label = match &config.mode {
        ServiceMode::Standalone { .. } => ServiceModeLabel::Standalone,
        ServiceMode::Cluster { .. } => ServiceModeLabel::Cluster,
    };
    let standalone_stats: Option<Vec<(String, Arc<RwLock<StandaloneStats>>)>> = match &config.mode {
        ServiceMode::Standalone { .. } => Some(
            built
                .iter()
                .map(|b| {
                    (
                        b.workflow_id.clone(),
                        Arc::new(RwLock::new(StandaloneStats::default())),
                    )
                })
                .collect(),
        ),
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
        inspector,
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
    if state.inspector.as_ref().is_some_and(|i| i.ui_enabled()) {
        println!("dashboard at http://{resolved_addr}/ui");
    } else {
        println!("endpoints at http://{resolved_addr}/");
    }

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
    //
    // One cancel-child token per built workflow, taken before `coord` itself
    // is consumed by `wait_for_signal()` below. This works uniformly for
    // standalone mode (N workflows) and cluster mode (exactly one, guaranteed
    // by `ServiceConfig::validate`).
    let items: Vec<_> = built
        .into_iter()
        .map(|b| {
            let cancel_child = coord.child();
            (b, cancel_child)
        })
        .collect();
    let runner_config = config.clone();
    let runner_stats = standalone_stats.clone();

    let runner_fut = async move {
        match runner_config.mode {
            ServiceMode::Standalone { .. } => {
                let runner_futs = items.into_iter().map(|(b, cancel_child)| {
                    let stats = runner_stats.as_ref().and_then(|entries| {
                        entries
                            .iter()
                            .find(|(id, _)| *id == b.workflow_id)
                            .map(|(_, lock)| lock.clone())
                    });
                    let cfg = runner_config.clone();
                    let tikv = tikv.clone();
                    async move {
                        pcs_service::service::run_standalone(b, &cfg, cancel_child, stats, tikv)
                            .await
                            .map(|_| ())
                    }
                });
                let results = futures::future::join_all(runner_futs).await;
                results.into_iter().find(Result::is_err).unwrap_or(Ok(()))
            }
            #[cfg(feature = "service-cluster")]
            ServiceMode::Cluster { .. } => {
                let (b, cancel_child) = items
                    .into_iter()
                    .next()
                    .expect("cluster mode config validation guarantees exactly one workflow");
                pcs_service::service::run_cluster(b, &runner_config, cancel_child)
                    .await
                    .map(|_| ())
            }
            // `service`-only build: the config parses so operators get a clear
            // message instead of a cryptic parse error, but cluster mode needs
            // the Raft stack.
            #[cfg(not(feature = "service-cluster"))]
            ServiceMode::Cluster { .. } => {
                // Drop the runner-only inputs explicitly so the async move
                // closure still captures them.
                drop(items);
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
    // Flush spans while no task is still emitting, then log the last line.
    telemetry.shutdown().await;
    tracing::info!("pcs-service stopped cleanly");

    // Cancellation from a clean shutdown signal is not an error.
    runner_result
}
