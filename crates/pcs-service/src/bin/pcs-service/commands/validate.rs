//! `pcs-service validate` — validate a config file without starting the service.
//!
//! Runs three validation gates:
//!
//! **Gate 1 — structural**: Parses the TOML and verifies every field is valid.
//! Any TOML typo, missing required field, or key the service cannot honour
//! (`pipeline.systems`, `pipeline.wasm.watch`, …) fails here.
//!
//! **Gate 2 — world match**: Attempts to build the service using the built-in
//! factory registry, which compiles any WASM module and verifies the WIT world
//! via wasmtime's instantiation (automatic — wasmtime checks imports/exports).
//!
//! **Gate 3 — semantic**: After the runtime is loaded, verifies that every
//! source `target_component` and sink `source_component` declared in the config
//! refers to a component actually handled by the runtime.
//!
//! The schema-fingerprint gate
//! ([`validate_schema_fingerprint`](pcs_service::service::validation::validate_schema_fingerprint))
//! is **not** run here: it compares the pipeline against the persisted state in
//! `node.data_dir`, which only exists once the cluster runner has opened it.
//! `pcs-service serve` applies it at cluster startup.
//!
//! ## Unknown type handling
//!
//! User-defined factory types (sources, sinks) are not in the built-in
//! registry.  Unknown types are reported as **warnings** by default so that
//! configs referencing user types still pass `validate` without a full custom
//! binary.  Use `--strict` to promote unknown types to errors.
//!
//! ## Exit codes
//!
//! | Condition | Exit code |
//! |-----------|-----------|
//! | Config is structurally valid and all built-in types resolve | 0 |
//! | Config is structurally valid but some types are unknown (default mode) | 0 (warnings printed to stderr) |
//! | Unknown types present and `--strict` is set | 1 |
//! | Config fails structural validation | 1 |
//! | Gate 3 semantic mismatch (source/sink targets missing from runtime) | 1 |

use pcs_service::PcsError;
use pcs_service::service::ServiceBuilder;
use pcs_service::service::config::{ServiceConfig, ServiceMode};
use pcs_service::service::factories::register_builtin_factories;
use pcs_service::service::validate_io_coverage;

use crate::cli::{GlobalOpts, ValidateArgs};

/// Entry point for the `validate` subcommand.
pub async fn run(global: &GlobalOpts, args: &ValidateArgs) -> Result<(), PcsError> {
    let config_path = global
        .config
        .as_ref()
        .ok_or_else(|| PcsError::configuration("--config is required for validate"))?;

    // Step 1: structural parse + semantic validation (data_dir, peers, etc.)
    let config = ServiceConfig::load(config_path)?;

    // Step 2 (gates 1+2): attempt to build with the built-in factory registry.
    // For WASM configs this also compiles the module and verifies the WIT world
    // (gate 1: structural compile, gate 2: world match via wasmtime instantiation).
    // Unknown type names produce PcsError::Configuration with a message that
    // names the missing factory.
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
    let build_result = builder.build(&config);

    // Collect all unknown-type errors; other errors (e.g. schema mismatches)
    // are always fatal regardless of --strict.
    let (unknown_warnings, built_service) = match build_result {
        Ok(built) => (vec![], Some(built)),
        Err(ref e) if is_unknown_factory_error(e) => (vec![e.message().to_string()], None),
        Err(e) => {
            // A non-unknown-factory error is always fatal.
            return Err(PcsError::configuration(format!(
                "factory build failed: {}",
                e.message()
            )));
        }
    };

    // Step 3 (gate 3 — semantic): check that all source/sink targets are covered
    // by the runtime's declared component list.  Only runs when build succeeded.
    if let Some(ref built) = built_service {
        let declared = built.runtime.declared_components();
        validate_io_coverage(&declared, &config).map_err(|e| {
            PcsError::configuration(format!("semantic validation failed: {}", e.message()))
        })?;
        println!("OK: all IO targets covered by runtime declared components");
    }

    // Report structural summary.
    println!("OK: config is structurally valid");
    println!("  node.id:  {}", config.node.id);
    if let Some(name) = &config.node.name {
        println!("  node.name: {name}");
    }
    println!(
        "  mode:     {}",
        match config.mode {
            ServiceMode::Standalone { .. } => "standalone",
            ServiceMode::Cluster { .. } => "cluster",
        }
    );
    #[cfg(feature = "wasm")]
    println!(
        "  pipeline: {}",
        match &config.pipeline.wasm {
            Some(spec) => spec.module.as_str(),
            None => "(runtime supplied programmatically)",
        }
    );
    println!("  sources:  {}", config.sources.len());
    println!("  sinks:    {}", config.sinks.len());
    println!("  http.bind: {}", config.http.bind);
    println!("  log_level: {}", config.observability.log_level);

    // Report unknown factories.
    if unknown_warnings.is_empty() {
        println!("OK: all declared types resolved in built-in registry");
    } else {
        for warn in &unknown_warnings {
            eprintln!("WARNING: {warn}");
        }
        eprintln!(
            "NOTE: {} unknown type(s) above are not in the built-in registry. \
             They may be user-defined types registered at serve time. \
             Use --strict to treat these as errors.",
            unknown_warnings.len()
        );

        if args.strict {
            return Err(PcsError::configuration(format!(
                "{} unknown factory type(s) found (--strict mode). \
                 Register the factory or fix the type name in the config.",
                unknown_warnings.len()
            )));
        }
    }

    Ok(())
}

/// Returns `true` if the error is specifically a missing factory registration
/// (as opposed to a factory build failure or schema error).
fn is_unknown_factory_error(e: &PcsError) -> bool {
    // ServiceBuilder::build formats missing-factory errors as:
    //   "no source/sink factory registered for type '...'"
    e.category() == "configuration"
        && (e.message().contains("no source factory registered")
            || e.message().contains("no sink factory registered"))
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use pcs_service::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, PipelineSpec, ServiceConfig, ServiceMode,
        SinkSpec, SourceSpec, StandaloneConfig,
    };
    use std::path::PathBuf;

    fn make_config(sources: Vec<SourceSpec>, sinks: Vec<SinkSpec>) -> ServiceConfig {
        ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            pipeline: PipelineSpec::default(),
            sources,
            sinks,
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }

    // ── Test 1: config with only built-in factories validates cleanly ─────────

    #[test]
    fn test_builtin_only_config_validates_cleanly() {
        // An empty config (no sources, no sinks) should always pass.
        let config = make_config(vec![], vec![]);
        let pipeline = pcs_service::pipeline::Pipeline::new("test");
        let builder =
            register_builtin_factories(ServiceBuilder::new()).with_runtime(Box::new(pipeline));
        let result = builder.build(&config);
        assert!(
            result.is_ok(),
            "empty config should build cleanly: {:?}",
            result.unwrap_err()
        );
    }
    // ── Test 2: unknown sink type is classified as unknown factory error ─────

    #[test]
    fn test_unknown_sink_type_is_unknown_factory_error() {
        let config = make_config(
            vec![],
            vec![SinkSpec {
                name: "sink1".to_string(),
                type_name: "PostgresSink".to_string(), // not built-in
                source_component: "orders".to_string(),
                config: toml::Value::Table(toml::Table::new()),
            }],
        );
        let pipeline = pcs_service::pipeline::Pipeline::new("test");
        let builder =
            register_builtin_factories(ServiceBuilder::new()).with_runtime(Box::new(pipeline));
        let err = builder.build(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            is_unknown_factory_error(&err),
            "PostgresSink should be classified as unknown factory: {err}"
        );
    }

    // ── Test 3: unknown source type is classified as unknown factory error ─────

    #[test]
    fn test_unknown_source_type_is_unknown_factory_error() {
        let config = make_config(
            vec![SourceSpec {
                name: "src1".to_string(),
                type_name: "KafkaSource".to_string(), // not built-in
                target_component: "orders".to_string(),
                config: toml::Value::Table(toml::Table::new()),
            }],
            vec![],
        );
        let pipeline = pcs_service::pipeline::Pipeline::new("test");
        let builder =
            register_builtin_factories(ServiceBuilder::new()).with_runtime(Box::new(pipeline));
        let err = builder.build(&config).unwrap_err();
        assert!(
            is_unknown_factory_error(&err),
            "KafkaSource should be classified as unknown factory: {err}"
        );
    }

    // ── Test 4: is_unknown_factory_error is false for other PcsErrors ────────

    #[test]
    fn test_non_factory_errors_not_classified_as_unknown() {
        let schema_err = PcsError::configuration("schema mismatch");
        assert!(
            !is_unknown_factory_error(&schema_err),
            "generic config error should not be classified as unknown factory"
        );
    }
}
