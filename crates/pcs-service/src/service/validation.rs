//! Load-time semantic validation for assembled services.
//!
//! Provides [`validate_io_coverage`] which checks that every source
//! `target_component` and sink `source_component` declared in the TOML config
//! is covered by the runtime's declared component list.  The check runs after
//! the runtime is loaded so it catches config↔runtime mismatches before the
//! first pipeline iteration.

use pcs_core::PcsResult;
use pcs_core::error::PcsError;

use super::config::ServiceConfig;

/// Verify that every IO endpoint declared in the config targets a component
/// the runtime actually handles.
///
/// `declared` is the slice returned by [`PipelineRuntime::declared_components`].
/// When `declared` is empty the function returns `Ok(())` immediately — the
/// runtime opts out of the coverage check (e.g. WASM runtimes that describe
/// their components lazily, or test pipelines with no registered components).
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] with a single message listing every
/// unresolved source/sink reference when at least one is missing.
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use pcs_service::service::validation::validate_io_coverage;
/// use pcs_service::service::config::{
///     ServiceConfig, ServiceMode, StandaloneConfig, NodeConfig, PipelineSpec,
///     HttpConfig, ObservabilityConfig, SourceSpec, SinkSpec,
/// };
/// use std::path::PathBuf;
///
/// let config = ServiceConfig {
///     node: NodeConfig { id: 1, name: None, data_dir: PathBuf::from("/tmp") },
///     mode: ServiceMode::Standalone { config: StandaloneConfig::default() },
///     pipeline: PipelineSpec { systems: vec![], components: vec![],
///         #[cfg(feature = "wasm")] wasm: None },
///     sources: vec![],
///     sinks: vec![],
///     http: HttpConfig::default(),
///     observability: ObservabilityConfig::default(),
/// };
/// // Passes: no sources/sinks to check.
/// validate_io_coverage(&["Orders", "Prices"], &config).unwrap();
/// # }
/// ```
pub fn validate_io_coverage(declared: &[&str], config: &ServiceConfig) -> PcsResult<()> {
    // Empty declared list ⇒ runtime opts out; skip check.
    if declared.is_empty() {
        return Ok(());
    }

    let declared_set: std::collections::HashSet<&str> = declared.iter().copied().collect();
    let mut missing: Vec<String> = Vec::new();

    for src in &config.sources {
        if !declared_set.contains(src.target_component.as_str()) {
            missing.push(format!(
                "source '{}' targets component '{}' which is not declared by the runtime",
                src.name, src.target_component
            ));
        }
    }

    for sink in &config.sinks {
        if !declared_set.contains(sink.source_component.as_str()) {
            missing.push(format!(
                "sink '{}' reads component '{}' which is not declared by the runtime",
                sink.name, sink.source_component
            ));
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(PcsError::configuration(format!(
            "IO coverage mismatch — {} unresolved reference(s):\n  {}",
            missing.len(),
            missing.join("\n  ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, PipelineSpec, ServiceConfig, ServiceMode,
        SinkSpec, SourceSpec, StandaloneConfig,
    };
    use std::path::PathBuf;

    fn make_config(sources: Vec<SourceSpec>, sinks: Vec<SinkSpec>) -> ServiceConfig {
        ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-cov-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            pipeline: PipelineSpec {
                systems: vec![],
                components: vec![],
                #[cfg(feature = "wasm")]
                wasm: None,
            },
            sources,
            sinks,
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }

    fn src(name: &str, target: &str) -> SourceSpec {
        SourceSpec {
            name: name.to_string(),
            type_name: "Test".to_string(),
            target_component: target.to_string(),
            config: toml::Value::Table(toml::Table::new()),
        }
    }

    fn sink_spec(name: &str, source_component: &str) -> SinkSpec {
        SinkSpec {
            name: name.to_string(),
            type_name: "Test".to_string(),
            source_component: source_component.to_string(),
            config: toml::Value::Table(toml::Table::new()),
        }
    }

    #[test]
    fn test_empty_declared_skips_check() {
        let config = make_config(vec![src("s1", "Orders")], vec![sink_spec("k1", "Invoices")]);
        // Empty declared list = runtime opts out; always passes.
        validate_io_coverage(&[], &config).unwrap();
    }

    #[test]
    fn test_all_covered_passes() {
        let config = make_config(
            vec![src("s1", "Orders"), src("s2", "Prices")],
            vec![sink_spec("k1", "Orders")],
        );
        validate_io_coverage(&["Orders", "Prices"], &config).unwrap();
    }

    #[test]
    fn test_missing_source_target_fails() {
        let config = make_config(vec![src("s1", "Missing")], vec![]);
        let err = validate_io_coverage(&["Orders"], &config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.to_string().contains("'Missing'"),
            "error should name the missing component: {err}"
        );
        assert!(
            err.to_string().contains("source 's1'"),
            "error should name the source: {err}"
        );
    }

    #[test]
    fn test_missing_sink_source_fails() {
        let config = make_config(vec![], vec![sink_spec("k1", "Ghost")]);
        let err = validate_io_coverage(&["Orders"], &config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.to_string().contains("'Ghost'"),
            "error should name the missing component: {err}"
        );
        assert!(
            err.to_string().contains("sink 'k1'"),
            "error should name the sink: {err}"
        );
    }

    #[test]
    fn test_multiple_missing_reported_together() {
        let config = make_config(
            vec![src("s1", "A"), src("s2", "B")],
            vec![sink_spec("k1", "C")],
        );
        let err = validate_io_coverage(&["Orders"], &config).unwrap_err();
        // All three unresolved references reported in one error.
        assert!(err.to_string().contains("3 unresolved"), "{err}");
    }

    #[test]
    fn test_no_sources_or_sinks_passes() {
        let config = make_config(vec![], vec![]);
        validate_io_coverage(&["Orders"], &config).unwrap();
    }
}
