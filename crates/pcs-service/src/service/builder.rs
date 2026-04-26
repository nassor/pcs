//! [`ServiceBuilder`] — assembles a [`BuiltService`] from config + registry.
//!
//! `ServiceBuilder` is the integration point between TOML configuration and
//! the PCS runtime. It holds a [`Registry`] of user-provided IO factories and,
//! when given a [`ServiceConfig`], instantiates every declared source and sink.
//!
//! The runtime is provided either directly via [`with_runtime`] or loaded from
//! a WASM module (when `config.pipeline.wasm` is set and the `wasm` feature is
//! enabled). System and component factories were removed.
//!
//! ## Usage
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use pcs_service::service::builder::ServiceBuilder;
//! use pcs_service::pipeline::Pipeline;
//!
//! let pipeline = Pipeline::new("my_pipeline");
//! let _builder = ServiceBuilder::new().with_runtime(Box::new(pipeline));
//! # }
//! ```

use crate::error::PcsError;
use crate::io::sink::Sink;
use crate::io::source::Source;
use pcs_core::runtime::PipelineRuntime;

use super::config::ServiceConfig;
#[cfg(feature = "wasm")]
use super::loader::{LocalModuleResolver, PipelineRuntimeLoader};
use super::registry::{Registry, SinkFactory, SourceFactory};
#[cfg(feature = "wasm")]
use crate::wasm::WasmEngine;

// ---------------------------------------------------------------------------
// BuiltSource / BuiltSink — assembled IO endpoints
// ---------------------------------------------------------------------------

/// An assembled source ready to drain into the pipeline.
pub struct BuiltSource {
    /// Instance name from the config (diagnostic use only).
    pub name: String,
    /// Name of the component this source writes rows into.
    pub target_component: String,
    /// The constructed source object.
    pub source: Box<dyn Source>,
}

/// An assembled sink ready to drain from the pipeline.
pub struct BuiltSink {
    /// Instance name from the config (diagnostic use only).
    pub name: String,
    /// Name of the component this sink reads rows from.
    pub source_component: String,
    /// The constructed sink object.
    pub sink: Box<dyn Sink>,
}

// ---------------------------------------------------------------------------
// BuiltService — the assembled runtime artifacts
// ---------------------------------------------------------------------------

/// All runtime artifacts produced by [`ServiceBuilder::build`].
///
/// The caller owns these and is responsible for driving them via the runner
/// functions (`run_standalone`, `run_cluster`). The `runtime` field is
/// authoritative; both standalone and cluster runners use it uniformly via
/// the [`PipelineRuntime`] trait.
///
/// The `registry` field is retained so factory-allocated resources that the
/// source/sink instances hold references back to remain alive for the service
/// lifetime.
///
/// ## Debug
///
/// `BuiltService` intentionally provides a minimal `Debug` implementation
/// (counts only) because the source/sink trait objects are not `Debug`.
pub struct BuiltService {
    /// The execution backend — native `Pipeline` or WASM guest.
    pub runtime: Box<dyn PipelineRuntime>,
    /// Constructed source instances in config order.
    pub sources: Vec<BuiltSource>,
    /// Constructed sink instances in config order.
    pub sinks: Vec<BuiltSink>,
    /// The registry that built this service — retained for lifetime management.
    pub registry: Registry,
}

impl BuiltService {
    /// Consume `self` and return a `Box<dyn PipelineRuntime>`.
    ///
    /// Used by the cluster runner. Prefer accessing `self.runtime` directly for
    /// standalone use.
    pub fn into_runtime(self) -> Box<dyn PipelineRuntime> {
        self.runtime
    }

    /// Crate-internal convenience used by tests that need to build a
    /// `BuiltService` without going through [`ServiceBuilder`].
    #[cfg(test)]
    pub(crate) fn from_runtime(
        runtime: Box<dyn PipelineRuntime>,
        sources: Vec<BuiltSource>,
        sinks: Vec<BuiltSink>,
        registry: Registry,
    ) -> Self {
        Self {
            runtime,
            sources,
            sinks,
            registry,
        }
    }
}

impl std::fmt::Debug for BuiltService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltService")
            .field("sources_count", &self.sources.len())
            .field("sinks_count", &self.sinks.len())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// ServiceBuilder
// ---------------------------------------------------------------------------

/// Assembles a [`BuiltService`] from a [`ServiceConfig`] and a populated
/// [`Registry`].
///
/// Register IO factories first, optionally set the runtime via
/// [`with_runtime`](Self::with_runtime), then call [`build`](Self::build) with
/// a loaded config.
///
/// ## Build flow
///
/// 1. Determine the runtime:
///    - (wasm feature) If `config.pipeline.wasm` is set, load via
///      [`PipelineRuntimeLoader`].
///    - Else if [`with_runtime`](Self::with_runtime) was called, use that.
///    - Else return [`PcsError::Configuration`].
/// 2. Emit a deprecation warning if `config.pipeline.systems` is non-empty
///    (legacy path).
/// 3. For each `SourceSpec`: look up factory, build source, wrap in
///    [`BuiltSource`].
/// 4. For each `SinkSpec`: look up factory, build sink, wrap in
///    [`BuiltSink`].
///
/// ## Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use pcs_service::service::builder::ServiceBuilder;
/// use pcs_service::pipeline::Pipeline;
///
/// let pipeline = Pipeline::new("my_pipeline");
/// let _builder = ServiceBuilder::new().with_runtime(Box::new(pipeline));
/// // builder.build(&config) would return Ok(BuiltService { ... })
/// # }
/// ```
pub struct ServiceBuilder {
    registry: Registry,
    runtime: Option<Box<dyn PipelineRuntime>>,
    #[cfg(feature = "wasm")]
    wasm_engine: Option<WasmEngine>,
}

impl ServiceBuilder {
    /// Create a new builder with an empty registry and no runtime.
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            runtime: None,
            #[cfg(feature = "wasm")]
            wasm_engine: None,
        }
    }

    /// Set the runtime directly.
    ///
    /// Any `Box<dyn PipelineRuntime>` is accepted — typically `Box::new(pipeline)`
    /// for a native [`Pipeline`](crate::pipeline::Pipeline).
    pub fn with_runtime(mut self, runtime: Box<dyn PipelineRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Set the [`WasmEngine`] used when the config includes a `pipeline.wasm`
    /// block. If not set and the config contains a wasm spec, `build` creates
    /// a default engine automatically.
    #[cfg(feature = "wasm")]
    pub fn with_wasm_engine(mut self, engine: WasmEngine) -> Self {
        self.wasm_engine = Some(engine);
        self
    }

    /// Register a source factory (builder-style chaining).
    pub fn register_source<F: SourceFactory>(mut self, factory: F) -> Self {
        self.registry.register_source(factory);
        self
    }

    /// Register a sink factory (builder-style chaining).
    pub fn register_sink<F: SinkFactory>(mut self, factory: F) -> Self {
        self.registry.register_sink(factory);
        self
    }

    /// Mutably register a source factory.
    pub fn register_source_mut<F: SourceFactory>(&mut self, factory: F) -> &mut Self {
        self.registry.register_source(factory);
        self
    }

    /// Mutably register a sink factory.
    pub fn register_sink_mut<F: SinkFactory>(&mut self, factory: F) -> &mut Self {
        self.registry.register_sink(factory);
        self
    }

    /// Access the inner registry (for inspection or passing to helpers).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Assemble a [`BuiltService`] from the loaded `config`.
    ///
    /// When `config.pipeline.wasm` is set (requires the `wasm` feature), the
    /// WASM module is loaded and compiled. Otherwise the runtime set via
    /// [`with_runtime`](Self::with_runtime) is used.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] if:
    /// - No runtime was provided and no wasm spec is present in the config.
    /// - A source or sink factory is missing or returns an error.
    /// - (wasm) The module cannot be resolved, compiled, or described.
    pub fn build(self, config: &ServiceConfig) -> Result<BuiltService, PcsError> {
        // Deprecation warnings for legacy `pipeline.systems` and `pipeline.components` config.
        if !config.pipeline.systems.is_empty() {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "pipeline.systems is deprecated and will be removed in a future release. \
                 Provide a runtime via ServiceBuilder::with_runtime() instead."
            );
            #[cfg(not(feature = "tracing"))]
            eprintln!(
                "[pcs-service] WARN: pipeline.systems is deprecated and will be removed \
                  in a future release. Provide a runtime via ServiceBuilder::with_runtime() instead."
            );
        }
        if !config.pipeline.components.is_empty() {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "pipeline.components is deprecated and will be removed in a future release. \
                 Components are now registered by the runtime at construction time."
            );
            #[cfg(not(feature = "tracing"))]
            eprintln!(
                "[pcs-service] WARN: pipeline.components is deprecated and will be removed \
                  in a future release. Components are now registered by the runtime at construction time."
            );
        }

        // ── Determine runtime ─────────────────────────────────────────────────
        let runtime: Box<dyn PipelineRuntime> = {
            // WASM path takes priority when the config declares a wasm spec.
            #[cfg(feature = "wasm")]
            if let Some(ref wasm_spec) = config.pipeline.wasm {
                let engine = self
                    .wasm_engine
                    .unwrap_or_else(|| WasmEngine::new().expect("wasmtime Engine creation failed"));
                let loader = PipelineRuntimeLoader::new(engine, LocalModuleResolver::new());
                Box::new(loader.load("service", wasm_spec)?)
            } else if let Some(rt) = self.runtime {
                rt
            } else {
                return Err(PcsError::configuration(
                    "no runtime provided: call ServiceBuilder::with_runtime() or set \
                     pipeline.wasm in the config",
                ));
            }

            #[cfg(not(feature = "wasm"))]
            if let Some(rt) = self.runtime {
                rt
            } else {
                return Err(PcsError::configuration(
                    "no runtime provided: call ServiceBuilder::with_runtime()",
                ));
            }
        };

        // ── Sources + Sinks ───────────────────────────────────────────────────
        let (sources, sinks) = build_io(&self.registry, config)?;

        Ok(BuiltService {
            runtime,
            sources,
            sinks,
            registry: self.registry,
        })
    }
}

impl Default for ServiceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// IO build helpers
// ---------------------------------------------------------------------------

pub(crate) fn build_io(
    registry: &Registry,
    config: &ServiceConfig,
) -> Result<(Vec<BuiltSource>, Vec<BuiltSink>), PcsError> {
    let mut sources = Vec::with_capacity(config.sources.len());
    for spec in &config.sources {
        let factory = registry.source(&spec.type_name).ok_or_else(|| {
            PcsError::configuration(format!(
                "no source factory registered for type '{}' \
                 (required by source '{}')",
                spec.type_name, spec.name
            ))
        })?;
        let source = factory.build(&spec.config)?;
        sources.push(BuiltSource {
            name: spec.name.clone(),
            target_component: spec.target_component.clone(),
            source,
        });
    }

    let mut sinks = Vec::with_capacity(config.sinks.len());
    for spec in &config.sinks {
        let factory = registry.sink(&spec.type_name).ok_or_else(|| {
            PcsError::configuration(format!(
                "no sink factory registered for type '{}' \
                 (required by sink '{}')",
                spec.type_name, spec.name
            ))
        })?;
        let sink = factory.build(&spec.config)?;
        sinks.push(BuiltSink {
            name: spec.name.clone(),
            source_component: spec.source_component.clone(),
            sink,
        });
    }

    Ok((sources, sinks))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::pipeline::{Dataset, Pipeline};
    use crate::service::config::{
        NodeConfig, PipelineSpec, ServiceConfig, ServiceMode, SinkSpec, SourceSpec,
        StandaloneConfig,
    };
    use crate::service::registry::{SinkFactory, SourceFactory};
    use crate::system::{System, SystemMeta};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::path::PathBuf;
    use std::sync::Arc;

    // ── Test helpers ──────────────────────────────────────────────────────────

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

    struct NoopSourceFactory;
    impl SourceFactory for NoopSourceFactory {
        fn type_name(&self) -> &'static str {
            "NoopSource"
        }
        fn build(&self, _config: &toml::Value) -> Result<Box<dyn Source>, PcsError> {
            use crate::io::channel_source::ChannelSource;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (_tx, src) = ChannelSource::new(schema, 1);
            Ok(Box::new(src))
        }
    }

    struct NoopSinkFactory;
    impl SinkFactory for NoopSinkFactory {
        fn type_name(&self) -> &'static str {
            "NoopSink"
        }
        fn build(&self, _config: &toml::Value) -> Result<Box<dyn Sink>, PcsError> {
            use crate::io::channel_sink::ChannelSink;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (sink, _rx) = ChannelSink::new(schema, 1);
            Ok(Box::new(sink))
        }
    }

    fn minimal_config() -> ServiceConfig {
        ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-test"),
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
            sources: vec![],
            sinks: vec![],
            http: crate::service::config::HttpConfig::default(),
            observability: crate::service::config::ObservabilityConfig::default(),
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_no_runtime_returns_error() {
        let config = minimal_config();
        let result = ServiceBuilder::new().build(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("no runtime"), "got: {err}");
    }

    #[test]
    fn test_with_runtime_builds_successfully() {
        let config = minimal_config();
        let pipeline = Pipeline::new("test");
        let result = ServiceBuilder::new()
            .with_runtime(Box::new(pipeline))
            .build(&config);
        assert!(result.is_ok(), "build failed: {:?}", result.unwrap_err());
        let service = result.unwrap();
        assert!(service.sources.is_empty());
        assert!(service.sinks.is_empty());
    }

    #[test]
    fn test_sources_and_sinks_built_from_config() {
        let config = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-test"),
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
            sources: vec![SourceSpec {
                name: "src1".to_string(),
                type_name: "NoopSource".to_string(),
                target_component: "comp1".to_string(),
                config: toml::Value::Table(toml::Table::new()),
            }],
            sinks: vec![SinkSpec {
                name: "sink1".to_string(),
                type_name: "NoopSink".to_string(),
                source_component: "comp1".to_string(),
                config: toml::Value::Table(toml::Table::new()),
            }],
            http: crate::service::config::HttpConfig::default(),
            observability: crate::service::config::ObservabilityConfig::default(),
        };

        let pipeline = Pipeline::new("test");
        let service = ServiceBuilder::new()
            .with_runtime(Box::new(pipeline))
            .register_source(NoopSourceFactory)
            .register_sink(NoopSinkFactory)
            .build(&config)
            .unwrap();

        assert_eq!(service.sources.len(), 1);
        assert_eq!(service.sinks.len(), 1);
        assert_eq!(service.sources[0].name, "src1");
        assert_eq!(service.sinks[0].name, "sink1");
        assert_eq!(service.sources[0].target_component, "comp1");
        assert_eq!(service.sinks[0].source_component, "comp1");
    }

    #[test]
    fn test_unknown_source_factory_returns_error() {
        let config = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-test"),
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
            sources: vec![SourceSpec {
                name: "bad_src".to_string(),
                type_name: "GhostSource".to_string(),
                target_component: "comp".to_string(),
                config: toml::Value::Table(toml::Table::new()),
            }],
            sinks: vec![],
            http: crate::service::config::HttpConfig::default(),
            observability: crate::service::config::ObservabilityConfig::default(),
        };

        let pipeline = Pipeline::new("test");
        let err = ServiceBuilder::new()
            .with_runtime(Box::new(pipeline))
            .build(&config)
            .unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("GhostSource"));
    }

    #[test]
    fn test_unknown_sink_factory_returns_error() {
        let config = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-test"),
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
            sources: vec![],
            sinks: vec![SinkSpec {
                name: "bad_sink".to_string(),
                type_name: "GhostSink".to_string(),
                source_component: "comp".to_string(),
                config: toml::Value::Table(toml::Table::new()),
            }],
            http: crate::service::config::HttpConfig::default(),
            observability: crate::service::config::ObservabilityConfig::default(),
        };

        let pipeline = Pipeline::new("test");
        let err = ServiceBuilder::new()
            .with_runtime(Box::new(pipeline))
            .build(&config)
            .unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("GhostSink"));
    }

    #[test]
    fn test_into_runtime_returns_runtime() {
        let config = minimal_config();
        let pipeline = Pipeline::new("test-rt");
        let service = ServiceBuilder::new()
            .with_runtime(Box::new(pipeline))
            .build(&config)
            .unwrap();
        let rt = service.into_runtime();
        assert_eq!(rt.name(), "test-rt");
    }

    #[test]
    fn test_boxed_system_runs_on_runtime() {
        let mut pipeline = Pipeline::new("test");
        pipeline.add_system_boxed(Box::new(NoopSystem));
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async { pipeline.run().await }).unwrap();
    }

    #[test]
    fn test_from_runtime_test_helper() {
        let pipeline = Pipeline::new("helper");
        let service =
            BuiltService::from_runtime(Box::new(pipeline), vec![], vec![], Registry::new());
        assert_eq!(service.runtime.name(), "helper");
        assert!(service.sources.is_empty());
    }
}
