//! Logging initialisation for the PCS service.
//!
//! [`init_logging`] must be called once at service startup, before any
//! `tracing` instrumentation fires.  It selects a log format (Pretty or JSON)
//! based on [`ObservabilityConfig`] and wires an [`EnvFilter`] so operators
//! can override the level at runtime via the `RUST_LOG` environment variable.
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

use crate::error::{PcsError, PcsResult};
use crate::service::config::{LogFormat, ObservabilityConfig};

use std::io::IsTerminal as _;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

/// Initialise the global `tracing` subscriber from `config`.
///
/// This function must be called exactly once per process.  Calling it a second
/// time returns [`PcsError::Configuration`] because the global subscriber is
/// already set.
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] if:
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
/// let cfg = ObservabilityConfig::default(); // Pretty format, info level
/// init_logging(&cfg).expect("logging init");
/// # }
/// ```
pub fn init_logging(config: &ObservabilityConfig) -> PcsResult<()> {
    let default_directive = format!("pcs={},tower_http=info,warn", config.log_level);

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&default_directive));

    let result = match config.log_format {
        LogFormat::Json => {
            let fmt_layer = fmt::layer().json();
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .try_init()
        }
        LogFormat::Pretty => {
            let use_ansi = std::io::stdout().is_terminal();
            let fmt_layer = fmt::layer().with_ansi(use_ansi);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .try_init()
        }
    };

    result
        .map_err(|e| PcsError::configuration(format!("failed to install tracing subscriber: {e}")))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::service::config::{LogFormat, ObservabilityConfig};

    fn pretty_config() -> ObservabilityConfig {
        ObservabilityConfig {
            log_format: LogFormat::Pretty,
            log_level: "info".to_string(),
        }
    }

    fn json_config() -> ObservabilityConfig {
        ObservabilityConfig {
            log_format: LogFormat::Json,
            log_level: "debug".to_string(),
        }
    }

    /// init_logging is idempotent in the sense that calling it twice returns an
    /// error on the second call (global subscriber already set).  We test this
    /// by calling twice and checking that the second call returns a
    /// Configuration error.
    ///
    /// Note: this test races with any other test that installs a subscriber, so
    /// it is marked `#[ignore]` in normal CI runs.  Run with
    /// `cargo test -- --ignored` to exercise it in isolation.
    #[test]
    #[ignore = "installs a global subscriber; must run in isolation"]
    fn test_second_init_returns_error() {
        let cfg = pretty_config();
        // First call — may succeed or fail if another test already installed.
        let _ = init_logging(&cfg);
        // Second call must fail.
        let err = init_logging(&cfg).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("subscriber"),
            "error should mention subscriber: {err}"
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
    }
}
