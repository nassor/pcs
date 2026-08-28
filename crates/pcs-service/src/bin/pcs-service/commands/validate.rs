//! `pcs-service validate`: validate a config file without starting the service.
//!
//! Runs two validation gates:
//!
//! **Gate 1 (structural)**: parses the KDL and verifies every field. Syntax
//! errors, missing required fields, and keys the service cannot honour
//! (`workflow.systems`, a `watch` property on `wasm`) fail here.
//!
//! **Gate 2 (build + graph)**: builds the service with the built-in factory
//! registry, compiling any WASM module and checking its WIT world through
//! wasmtime instantiation, then validates every declared `link` end to end:
//! matching components and field-for-field identical Arrow schemas
//! ([`validate_workflow_graph`](pcs_service::service::validation::validate_workflow_graph)).
//!
//! The schema-fingerprint gate
//! ([`validate_schema_fingerprint`](pcs_service::service::validation::validate_schema_fingerprint))
//! is not run here. It compares the workflow against persisted state in
//! `node.data_dir`, which exists only once the cluster runner has opened it, so
//! `pcs-service serve` applies it at cluster startup.
//!
//! ## Unknown type handling
//!
//! User-defined factory types (sources, sinks) are not in the built-in
//! registry. Unknown types are reported as warnings by default so that configs
//! referencing user types still pass without a full custom binary. Use
//! `--strict` to promote unknown types to errors.
//!
//! ## Exit codes
//!
//! | Condition | Exit code |
//! |-----------|-----------|
//! | Config is structurally valid and all built-in types resolve | 0 |
//! | Config is structurally valid but some types are unknown (default mode) | 0 (warnings printed to stderr) |
//! | Unknown types present and `--strict` is set | 1 |
//! | Config fails structural validation | 1 |
//! | Workflow graph mismatch (link component/schema disagreement) | 1 |

use pcs_service::PcsError;
use pcs_service::service::ServiceBuilder;
use pcs_service::service::config::{ServiceConfig, ServiceMode};
use pcs_service::service::factories::register_builtin_factories;

use crate::cli::{GlobalOpts, ValidateArgs};

/// Entry point for the `validate` subcommand.
pub async fn run(global: &GlobalOpts, args: &ValidateArgs) -> Result<(), PcsError> {
    let config = ServiceConfig::load(&global.config)?;

    // Building with the built-in registry also compiles any WASM module,
    // verifies its WIT world, and validates the workflow graph. Unknown type
    // names surface as configuration errors naming the missing factory.
    let builder = register_builtin_factories(ServiceBuilder::new());
    let build_result = builder.build_all(&config);

    // Unknown-type errors become warnings; every other error is fatal
    // regardless of --strict.
    let (unknown_warnings, built) = match build_result {
        Ok(built) => (vec![], Some(built)),
        Err(ref e) if is_unknown_factory_error(e) => (vec![e.message().to_string()], None),
        Err(e) => {
            return Err(PcsError::configuration(format!(
                "factory build failed: {}",
                e.message()
            )));
        }
    };

    if built.is_some() {
        println!("OK: workflow graph validated (components and schemas agree end to end)");
    }

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
    for workflow in &config.workflows {
        println!("  workflow: {}", workflow.id);
        #[cfg(feature = "wasm")]
        if !workflow.wasm.is_empty() {
            println!(
                "  processors: {}",
                workflow
                    .wasm
                    .iter()
                    .map(|spec| spec
                        .module
                        .as_deref()
                        .unwrap_or("(runtime supplied programmatically)"))
                    .collect::<Vec<_>>()
                    .join(" -> ")
            );
        }
        println!("  sources:  {}", workflow.sources.len());
        println!("  sinks:    {}", workflow.sinks.len());
    }
    println!("  http.bind: {}", config.http.bind);
    println!("  log_level: {}", config.observability.log_level);
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
    // ServiceBuilder::build_all formats missing-factory errors as
    // "no source/sink factory registered for type '...'".
    e.category() == "configuration"
        && (e.message().contains("no source factory registered")
            || e.message().contains("no sink factory registered"))
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use pcs_connector::{ConfigMap, ConfigValue};
    use pcs_service::service::config::{
        HttpConfig, LinkSpec, NodeConfig, ObservabilityConfig, ServiceConfig, ServiceMode,
        SinkSpec, SourceSpec, StandaloneConfig, WorkflowSpec,
    };
    use std::path::PathBuf;

    fn make_workflow(sources: Vec<SourceSpec>, sinks: Vec<SinkSpec>) -> WorkflowSpec {
        let links = sources
            .iter()
            .flat_map(|s| {
                sinks.iter().map(move |k| LinkSpec {
                    from: s.id.clone(),
                    to: k.id.clone(),
                    branch: None,
                })
            })
            .collect();
        WorkflowSpec {
            id: "test".to_string(),
            name: None,
            transformers: Vec::new(),
            sources,
            #[cfg(feature = "wasm")]
            wasm: Vec::new(),
            #[cfg(feature = "plugin")]
            plugin: Vec::new(),
            sinks,
            links,
        }
    }

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
            workflows: vec![make_workflow(sources, sinks)],
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }

    #[test]
    fn test_builtin_only_config_validates_cleanly() {
        let config = make_config(vec![], vec![]);
        let builder = register_builtin_factories(ServiceBuilder::new());
        let result = builder.build_all(&config);
        assert!(
            result.is_ok(),
            "empty config should build cleanly: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_unknown_sink_type_is_unknown_factory_error() {
        let config = make_config(
            vec![],
            vec![SinkSpec {
                id: "sink1".to_string(),
                name: None,
                type_name: "ClickHouseSink".to_string(), // not built-in
                transformer: None,
                component: "orders".to_string(),
                config: ConfigValue::Object(ConfigMap::new()),
            }],
        );
        let builder = register_builtin_factories(ServiceBuilder::new());
        let err = builder.build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            is_unknown_factory_error(&err),
            "ClickHouseSink should be classified as unknown factory: {err}"
        );
    }

    #[test]
    fn test_unknown_source_type_is_unknown_factory_error() {
        let config = make_config(
            vec![SourceSpec {
                id: "src1".to_string(),
                name: None,
                type_name: "MongoSource".to_string(), // not built-in
                transformer: None,
                component: "orders".to_string(),
                config: ConfigValue::Object(ConfigMap::new()),
            }],
            vec![],
        );
        let builder = register_builtin_factories(ServiceBuilder::new());
        let err = builder.build_all(&config).unwrap_err();
        assert!(
            is_unknown_factory_error(&err),
            "MongoSource should be classified as unknown factory: {err}"
        );
    }

    #[test]
    fn test_non_factory_errors_not_classified_as_unknown() {
        let schema_err = PcsError::configuration("schema mismatch");
        assert!(
            !is_unknown_factory_error(&schema_err),
            "generic config error should not be classified as unknown factory"
        );
    }
}
