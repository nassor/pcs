//! Logging and span-export initialisation for the PCS service.
//!
//! [`init_logging`] must be called once at service startup, before any
//! `tracing` instrumentation fires.  It selects a log format (Pretty or JSON)
//! based on [`ObservabilityConfig`], wires an [`EnvFilter`] so operators can
//! override the level at runtime via the `RUST_LOG` environment variable,
//! installs the [`SpanMetricsLayer`] that feeds `pcs_stage_duration_seconds`,
//! adds the [`Inspector`]'s capture layer unless it is disabled in config, and
//! turns on OTLP/HTTP span export when `otlp_endpoint` is set.
//!
//! ## Format selection
//!
//! | `log_format` | Output |
//! |---|---|
//! | `Pretty` | Human-readable, ANSI colour when stdout is a TTY |
//! | `Json`   | One JSON object per log record for log aggregators |
//!
//! ## `RUST_LOG` override
//!
//! The `RUST_LOG` environment variable takes precedence over `config.log_level`.
//! When `RUST_LOG` is not set the default filter is:
//! `pcs=<log_level>,tower_http=info,warn`
//!
//! ## Span export
//!
//! OTLP carries spans only. Metrics stay on the Prometheus pull endpoint the
//! HTTP control plane serves; no OTLP metrics exporter is installed. The
//! returned [`TelemetryGuard`] owns the tracer provider, and
//! [`TelemetryGuard::shutdown`] flushes the batch processor before the process
//! exits.

use crate::error::{PcsError, PcsResult};
use crate::inspector::Inspector;
use crate::service::config::{LogFormat, ObservabilityConfig};
use crate::service::span_metrics::SpanMetricsLayer;

use std::io::IsTerminal as _;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

/// The subscriber the format layer is built against: the bare registry plus the
/// `EnvFilter`, which is the same for both log formats.
type FilteredRegistry = tracing_subscriber::layer::Layered<EnvFilter, tracing_subscriber::Registry>;

/// Holds the tracer provider so a final flush is possible.
///
/// Empty when OTLP export is off. Dropping it is not enough once
/// `opentelemetry::global::set_tracer_provider` has stored a clone in a
/// process-lifetime static, so [`shutdown`](Self::shutdown) is load-bearing.
#[derive(Debug)]
pub struct TelemetryGuard {
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl TelemetryGuard {
    /// Flush and stop span export.
    ///
    /// Logs a failure rather than returning it: the process is already exiting,
    /// and a failed flush must not mask the runner's own exit status.
    pub async fn shutdown(self) {
        let Some(provider) = self.tracer_provider else {
            return;
        };
        // `SdkTracerProvider::shutdown` joins the batch processor's OS thread,
        // so it must not run on a runtime worker.
        let joined = tokio::task::spawn_blocking(move || provider.shutdown()).await;
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "otlp span exporter shutdown failed"),
            Err(e) => tracing::error!(error = %e, "otlp span exporter shutdown task failed"),
        }
    }
}

/// Initialise the global `tracing` subscriber from `config`, and build the
/// in-process inspector when `observability.inspector.enabled` is set.
///
/// Call once per process. A second call returns [`PcsError::Configuration`]
/// because the global subscriber is already installed.
///
/// `node_id` is attached to exported spans as the `pcs.node_id` resource
/// attribute, so a collector can tell cluster members apart.
///
/// The returned [`Inspector`] is `None` when capture is disabled, in which case
/// no inspector layer is installed at all and the cost is one `Option` branch.
/// The caller owns the handle: it is what the HTTP router and
/// [`ServiceBuilder`](crate::service::builder::ServiceBuilder) need.
///
/// The `EnvFilter` this installs is subscriber-wide, so a `RUST_LOG` that
/// suppresses `pcs_service` also empties the inspector's span buffer — the same
/// caveat `pcs_stage_duration_seconds` carries.
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] if:
/// - `config.trace_sample_ratio` is outside `0.0..=1.0`.
/// - The OTLP span exporter cannot be built from `config.otlp_endpoint`.
/// - A global subscriber has already been installed.
/// - `RUST_LOG` contains an invalid filter directive (the error is described
///   in the message).
///
/// # Examples
///
/// ```rust,no_run
/// # #[cfg(feature = "service")]
/// # {
/// use pcs_service::service::config::ObservabilityConfig;
/// use pcs_service::service::logging::init_logging;
///
/// let cfg = ObservabilityConfig::default(); // Pretty format, info level, no OTLP
/// let (telemetry, inspector) = init_logging(&cfg, 1).expect("logging init");
/// assert!(inspector.is_some()); // the inspector is on by default
/// # }
/// ```
pub fn init_logging(
    config: &ObservabilityConfig,
    node_id: u64,
) -> PcsResult<(TelemetryGuard, Option<Inspector>)> {
    // Reject a bad ratio before building anything.
    if !(0.0..=1.0).contains(&config.trace_sample_ratio) {
        return Err(PcsError::configuration(
            "observability.trace_sample_ratio must be between 0.0 and 1.0",
        ));
    }

    let default_directive = format!("pcs={},tower_http=info,warn", config.log_level);

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&default_directive));

    let (tracer_provider, otel_layer) = match &config.otlp_endpoint {
        None => (None, None),
        Some(endpoint) => {
            use opentelemetry::trace::TracerProvider as _;
            use opentelemetry_otlp::WithExportConfig as _;

            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .with_endpoint(traces_endpoint(endpoint))
                .build()
                .map_err(|e| PcsError::configuration(format!("otlp span exporter: {e}")))?;
            let resource = opentelemetry_sdk::Resource::builder()
                .with_service_name("pcs")
                .with_attribute(opentelemetry::KeyValue::new(
                    "pcs.node_id",
                    node_id.to_string(),
                ))
                .build();
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource)
                .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
                    opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(config.trace_sample_ratio),
                )))
                .build();
            opentelemetry::global::set_tracer_provider(provider.clone());
            let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("pcs"));
            (Some(provider), Some(layer))
        }
    };

    // The json and pretty format layers are different types, and
    // `OpenTelemetryLayer<S, T>` is generic over the subscriber it sits on, so
    // a per-format chain would need a second, differently-typed otel layer.
    // Boxing the format layer keeps one chain and one `try_init`.
    let fmt_layer: Box<dyn tracing_subscriber::Layer<FilteredRegistry> + Send + Sync> =
        match config.log_format {
            LogFormat::Json => Box::new(fmt::layer().json()),
            LogFormat::Pretty => {
                let use_ansi = std::io::stdout().is_terminal();
                Box::new(fmt::layer().with_ansi(use_ansi))
            }
        };

    // The inspector is one more layer on this same registry, deliberately: a
    // second span pipeline would double-instrument every span `pcs-core` opens.
    let inspector = if config.inspector.enabled {
        Some(Inspector::new(&config.inspector))
    } else {
        None
    };

    // `tracing_subscriber` implements `Layer` for `Option<L>`, so the same
    // chain covers both the export-on and export-off cases, and both the
    // inspector-on and inspector-off ones.
    let result = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(SpanMetricsLayer)
        .with(inspector.as_ref().map(Inspector::layer))
        .with(otel_layer)
        .try_init();

    result.map_err(|e| {
        PcsError::configuration(format!("failed to install tracing subscriber: {e}"))
    })?;

    Ok((TelemetryGuard { tracer_provider }, inspector))
}

/// Turn a collector base URL into the OTLP/HTTP traces URL.
///
/// `opentelemetry-otlp` uses a programmatically supplied endpoint verbatim, so
/// a bare collector root would POST to `/` and every collector would reject it.
/// This applies the same rule the OTLP spec gives for a base endpoint: append
/// `/v1/traces`. An operator who already wrote the full URL gets it unchanged.
fn traces_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    if trimmed.ends_with("/v1/traces") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/traces")
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::service::config::{LogFormat, ObservabilityConfig};

    fn pretty_config() -> ObservabilityConfig {
        ObservabilityConfig {
            log_format: LogFormat::Pretty,
            log_level: "info".to_string(),
            ..ObservabilityConfig::default()
        }
    }

    fn json_config() -> ObservabilityConfig {
        ObservabilityConfig {
            log_format: LogFormat::Json,
            log_level: "debug".to_string(),
            ..ObservabilityConfig::default()
        }
    }

    /// Calling `init_logging` twice returns a Configuration error on the second
    /// call, because the global subscriber is already installed.
    ///
    /// Ignored by default: it races with any other test that installs a
    /// subscriber. Run with `cargo test -- --ignored` to exercise it.
    #[test]
    #[ignore = "installs a global subscriber; must run in isolation"]
    fn test_second_init_returns_error() {
        let cfg = pretty_config();
        // The first call may lose the race to another test's subscriber.
        let _ = init_logging(&cfg, 1);
        let err = init_logging(&cfg, 1).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("subscriber"),
            "error should mention subscriber: {err}"
        );
    }

    /// A ratio outside `0.0..=1.0` is rejected before any provider is built, so
    /// this is safe to run alongside other tests.
    #[test]
    fn test_out_of_range_sample_ratio_is_rejected() {
        let mut cfg = pretty_config();
        cfg.trace_sample_ratio = 1.5;
        let err = init_logging(&cfg, 1).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("trace_sample_ratio"),
            "error should name the key: {err}"
        );
    }

    #[test]
    fn test_pretty_config_construction() {
        let cfg = pretty_config();
        assert_eq!(cfg.log_format, LogFormat::Pretty);
        assert_eq!(cfg.log_level, "info");
    }

    #[test]
    fn test_json_config_construction() {
        let cfg = json_config();
        assert_eq!(cfg.log_format, LogFormat::Json);
        assert_eq!(cfg.log_level, "debug");
    }

    #[test]
    fn test_default_observability_config_is_pretty_info() {
        let cfg = ObservabilityConfig::default();
        assert_eq!(cfg.log_format, LogFormat::Pretty);
        assert_eq!(cfg.log_level, "info");
        assert!(cfg.otlp_endpoint.is_none());
        assert_eq!(cfg.trace_sample_ratio, 1.0);
    }

    /// A base URL gains the traces path; a full URL is left alone. Getting this
    /// wrong makes the exporter POST to `/`, which collectors reject.
    #[test]
    fn test_traces_endpoint_appends_the_signal_path() {
        assert_eq!(
            traces_endpoint("http://127.0.0.1:4318"),
            "http://127.0.0.1:4318/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://127.0.0.1:4318/"),
            "http://127.0.0.1:4318/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://collector:4318/otlp"),
            "http://collector:4318/otlp/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://127.0.0.1:4318/v1/traces"),
            "http://127.0.0.1:4318/v1/traces"
        );
        assert_eq!(
            traces_endpoint("http://127.0.0.1:4318/v1/traces/"),
            "http://127.0.0.1:4318/v1/traces"
        );
    }
}
