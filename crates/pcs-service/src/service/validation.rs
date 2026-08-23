//! Load-time semantic validation for assembled services.
//!
//! Two checks run after the runtime is loaded, catching config/runtime and
//! runtime/persisted-state mismatches before the first pipeline iteration:
//!
//! - [`validate_io_coverage`]: every source `target_component` and sink
//!   `source_component` in the TOML config is covered by the runtime's
//!   declared component list.
//! - [`validate_schema_fingerprint`]: the runtime's Arrow schema fingerprint
//!   matches the one recorded by the node's persisted checkpoints, so a
//!   redeployed pipeline cannot resume against incompatible state.

use pcs_core::PcsResult;
use pcs_core::error::PcsError;

use super::config::ServiceConfig;

/// Verify that every IO endpoint declared in the config targets a component
/// the runtime actually handles.
///
/// `declared` is the slice returned by [`PipelineRuntime::declared_components`].
/// When `declared` is empty the function returns `Ok(())`: the runtime has
/// opted out of the coverage check, as WASM runtimes that describe their
/// components lazily and test pipelines with no components do.
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
///     pipeline: PipelineSpec::default(),
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

/// Verify that the runtime's Arrow schema fingerprint matches the one recorded
/// by this node's persisted checkpoints.
///
/// `runtime` is `runtime.template_dataset().schemas().fingerprint()`, the same
/// `u32` a WASM guest reports as `pipeline-descriptor.schema-fingerprint` (the
/// guest formats it as 8-char hex; the value is identical). `persisted` is
/// [`RedbSharedStore::persisted_schema_id`](crate::distributed::consensus::store::RedbSharedStore::persisted_schema_id),
/// which is `None` on a node with no state yet.
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] when both are present and differ: the
/// persisted checkpoints describe a different schema shape than the pipeline
/// about to resume from them, so resuming would silently mix layouts.
pub fn validate_schema_fingerprint(runtime: u32, persisted: Option<u32>) -> PcsResult<()> {
    match persisted {
        None => Ok(()),
        Some(stored) if stored == runtime => Ok(()),
        Some(stored) => Err(PcsError::configuration(format!(
            "schema fingerprint mismatch: the pipeline declares {runtime:08x} but this \
             node's persisted checkpoints were written with {stored:08x}. The deployed \
             pipeline's component schemas changed. Either restore the previous pipeline \
             or clear node.data_dir before starting with the new schema."
        ))),
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
                #[cfg(feature = "wasm")]
                wasm: None,
                #[cfg(feature = "plugin")]
                plugin: None,
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
        assert!(err.to_string().contains("3 unresolved"), "{err}");
    }

    #[test]
    fn test_no_sources_or_sinks_passes() {
        let config = make_config(vec![], vec![]);
        validate_io_coverage(&["Orders"], &config).unwrap();
    }

    #[test]
    fn test_fingerprint_passes_on_a_fresh_node() {
        validate_schema_fingerprint(0xdead_beef, None)
            .expect("a node with no persisted state has nothing to conflict with");
    }

    #[test]
    fn test_fingerprint_passes_when_it_matches() {
        validate_schema_fingerprint(0xdead_beef, Some(0xdead_beef)).expect("same shape resumes");
    }

    #[test]
    fn test_fingerprint_mismatch_is_rejected() {
        let err = validate_schema_fingerprint(0x0000_0001, Some(0x0000_0002))
            .expect_err("a redeployed pipeline must not resume against foreign state");
        assert_eq!(err.category(), "configuration");
        let msg = err.to_string();
        assert!(
            msg.contains("00000001"),
            "must name the pipeline's own: {msg}"
        );
        assert!(
            msg.contains("00000002"),
            "must name the persisted one: {msg}"
        );
        assert!(
            msg.contains("data_dir"),
            "must tell the operator what to do: {msg}"
        );
    }
}
