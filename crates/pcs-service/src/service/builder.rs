//! [`ServiceBuilder`]: assembles a [`BuiltService`] per workflow from config
//! and registry.
//!
//! `ServiceBuilder` is the integration point between the configuration file and
//! the PCS runtime. It holds a [`Registry`] of user-provided IO factories and,
//! given a [`ServiceConfig`], instantiates every declared node of every
//! declared `workflow`: sources, sinks, transformers, and `wasm`/`plugin`
//! processors.
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
//! let _builder = ServiceBuilder::new().with_runtime("my-processor", Box::new(pipeline));
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::{PcsError, PcsResult};
use pcs_core::io::retry::{RetryingSink, RetryingSource};
use pcs_core::io::sink::Sink;
use pcs_core::io::source::Source;
use pcs_core::runtime::PipelineRuntime;

use super::config::{NodeKind, RunMode, ServiceConfig, ServiceMode, TransformerSpec, WorkflowSpec};
#[cfg(feature = "wasm")]
use super::loader::{LocalModuleResolver, PipelineRuntimeLoader};
#[cfg(feature = "plugin")]
use super::plugin_loader::load_plugin_runtime;
use super::registry::{Registry, SinkFactory, SourceFactory, TransformerFactory};
use crate::inspector::Inspector;
#[cfg(feature = "wasm")]
use crate::wasm::WasmEngine;
use pcs_connector::{ChannelBridge, ConnectorContext};
use pcs_transformer::Transformer;

use super::topology::build_topology;

// The "no runtime" help text names how a processor node can be given a
// runtime, so it has one arm per `wasm` and `plugin` feature combination.
#[cfg(all(feature = "wasm", not(feature = "plugin")))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::with_runtime(id, ..) or set 'module' on the wasm node";
#[cfg(all(feature = "plugin", not(feature = "wasm")))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::with_runtime(id, ..) \
     or set 'library' on the plugin node";
#[cfg(all(feature = "wasm", feature = "plugin"))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::with_runtime(id, ..) \
     or set 'module'/'library' on the node";
#[cfg(not(any(feature = "wasm", feature = "plugin")))]
const RUNTIME_SOURCE_HELP: &str = "no runtime provided: call ServiceBuilder::with_runtime(id, ..)";

/// Whether a config-driven source is handed to the runner wrapped in
/// [`RetryingSource`].
///
/// Sinks always take their wrapper: a `write_batch` the runner cannot re-drive
/// is lost rows. A source's answer depends on who owns its error policy.
///
/// - Batch modes (`one_shot`, `interval`, `continuous`) and cluster mode drain
///   a source through `drain_into_dataset`, where the first error abandons the
///   whole iteration. The wrapper is what turns a transient failure into a
///   retried read instead of a lost iteration.
/// - `run_mode kind="stream"` is itself the retry loop: `run_stream` polls the
///   source once per item, and on an error logs it, counts it in
///   `iteration_errors` and re-polls after a cancellable
///   `SOURCE_ERROR_BACKOFF`. Wrapping there nests a second retry loop inside
///   the first, on the item path, with a first backoff an order of magnitude
///   longer than the runner's own — so a stream source is handed over
///   unwrapped and the runner's documented policy is the only one that runs.
fn sources_take_retry_wrapper(config: &ServiceConfig) -> bool {
    !matches!(
        &config.mode,
        ServiceMode::Standalone { config: sc } if sc.run_mode == RunMode::Stream
    )
}

/// Intern `name` to a process-lifetime `&'static str`, deduplicating by
/// content so the same component name declared on several nodes leaks once.
///
/// `Dataset::append_record_batch`/`Dataset::batch_for` key components by
/// `&'static str`, matching every compile-time `Component::name()` in the
/// codebase; this is the one adapter from a KDL-declared, therefore runtime,
/// component name to that contract. Growth is bounded by the number of
/// distinct component names the config declares, which is fixed for the life
/// of the process — this mirrors `pcs_core::dataset`'s own (crate-private)
/// component-name interner for the identical reason.
fn intern_component_name(name: &str) -> &'static str {
    static INTERNED: OnceLock<Mutex<std::collections::HashSet<&'static str>>> = OnceLock::new();
    let set = INTERNED.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
    let mut set = set
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(&existing) = set.get(name) {
        return existing;
    }
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    set.insert(leaked);
    leaked
}

/// What kind of execution backend a [`BuiltNode::kind`] holds.
pub enum BuiltNodeKind {
    /// A constructed IO source.
    Source(Box<dyn Source>),
    /// A constructed processor runtime.
    Processor {
        /// The execution backend.
        runtime: Box<dyn PipelineRuntime>,
        /// `"wasm"`, `"plugin"` or `"native"`.
        kind: &'static str,
    },
    /// A constructed IO sink.
    Sink(Box<dyn Sink>),
}

/// One declared outbound edge of a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltEdge {
    /// Index into [`BuiltService::nodes`]. Always greater than the owning
    /// node's index, because `nodes` is in topological order.
    pub node: usize,
    /// Branch name this edge carries; `None` for an unlabelled link.
    pub branch: Option<String>,
}

/// Whether a routing decision selects an edge. `None` routes (legacy) select
/// everything; otherwise only edges whose branch is named are selected.
pub(crate) fn edge_selected(routes: &Option<Vec<String>>, branch: &Option<String>) -> bool {
    match routes {
        None => true,
        Some(routes) => match branch {
            Some(name) => routes.iter().any(|r| r == name),
            None => false,
        },
    }
}

/// One assembled node of the workflow graph.
pub struct BuiltNode {
    /// The declared id.
    pub id: String,
    /// Declared name, absent when the config named none.
    pub name: Option<String>,
    /// Connector `type` for a source or sink; the runtime kind
    /// (`"wasm"`/`"plugin"`/`"native"`) for a processor.
    pub type_name: String,
    /// Component a source writes or a sink reads. `None` for a processor.
    ///
    /// Leaked to `'static` once at build time: it is a span field and a
    /// `Dataset` key on every iteration.
    pub component: Option<&'static str>,
    /// The constructed execution backend.
    pub kind: BuiltNodeKind,
    /// Outbound edges into [`BuiltService::nodes`]. Always greater than this
    /// node's own index, because `nodes` is in topological order.
    pub downstream: Vec<BuiltEdge>,
    /// Artifact path for a `wasm`/`plugin` processor, for the topology
    /// detail. `None` for a source, a sink, or a processor whose runtime was
    /// supplied through [`ServiceBuilder::with_runtime`].
    pub artifact: Option<String>,
    /// The node's windowing declaration, when its config declared a `window`
    /// block. `None` for a non-windowed processor and for every source or
    /// sink. The runners use it to track the node's watermark; the topology
    /// uses it to describe the node.
    #[cfg(feature = "windows")]
    pub window: Option<super::config::WindowConfig>,
}

/// All runtime artifacts produced by [`ServiceBuilder::build_all`] for one
/// workflow.
///
/// The caller owns these and drives them with a runner function
/// (`run_standalone`, `run_cluster`).
///
/// `registry` is shared (via `Arc`) across every workflow one `build_all` call
/// produces, so that factory-allocated resources the sources and sinks point
/// back to stay alive for the service lifetime.
///
/// The `Debug` implementation reports counts only, because the node trait
/// objects are not `Debug`.
pub struct BuiltService {
    /// The workflow's declared id.
    pub workflow_id: String,
    /// The workflow's declared name, absent when the config named none.
    pub workflow_name: Option<String>,
    /// Every declared node, in topological order: a node always follows every
    /// node that links into it.
    pub nodes: Vec<BuiltNode>,
    /// The registry that built this service, shared across every workflow
    /// `build_all` produced and retained for lifetime management.
    pub registry: Arc<Registry>,
    /// The inspector this build published its topology into, when enabled.
    /// Runners hand it to the HTTP layer; nothing on the execution path reads
    /// it.
    pub inspector: Option<Inspector>,
}

impl std::fmt::Debug for BuiltService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltService")
            .field("workflow_id", &self.workflow_id)
            .field("nodes_count", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

/// Assembles one [`BuiltService`] per declared workflow from a
/// [`ServiceConfig`] and a populated [`Registry`].
///
/// Register IO factories first, optionally supply native runtimes for
/// artifact-less processor nodes via [`with_runtime`](Self::with_runtime),
/// then call [`build_all`](Self::build_all) with a loaded config.
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
/// let _builder = ServiceBuilder::new().with_runtime("my-processor", Box::new(pipeline));
/// // builder.build_all(&config) would return Ok(vec![BuiltService { ... }])
/// # }
/// ```
pub struct ServiceBuilder {
    registry: Registry,
    /// Native runtimes keyed by the processor node id they back. Consumed by
    /// [`build_all`](Self::build_all) for a `wasm`/`plugin` node declared with
    /// no artifact.
    runtimes: HashMap<String, Box<dyn PipelineRuntime>>,
    #[cfg(feature = "wasm")]
    wasm_engine: Option<WasmEngine>,
    inspector: Option<Inspector>,
    channels: Option<Arc<dyn ChannelBridge>>,
}

impl ServiceBuilder {
    /// Create a new builder with an empty registry and no runtimes.
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            runtimes: HashMap::new(),
            #[cfg(feature = "wasm")]
            wasm_engine: None,
            inspector: None,
            channels: None,
        }
    }

    /// Supply the runtime for the processor node declared with id
    /// `processor_id` and no `module`/`library` key.
    ///
    /// Any `Box<dyn PipelineRuntime>` is accepted, typically `Box::new(pipeline)`
    /// for a native [`Pipeline`](crate::pipeline::Pipeline).
    pub fn with_runtime(
        mut self,
        processor_id: impl Into<String>,
        runtime: Box<dyn PipelineRuntime>,
    ) -> Self {
        self.runtimes.insert(processor_id.into(), runtime);
        self
    }

    /// Publish the built topology into `inspector` during
    /// [`build_all`](Self::build_all).
    ///
    /// The builder is the only place that knows both the concrete runtime kind
    /// (before the `Box<dyn PipelineRuntime>` erases it) and the configured
    /// source and sink sets, which is exactly what the topology is.
    pub fn with_inspector(mut self, inspector: Inspector) -> Self {
        self.inspector = Some(inspector);
        self
    }

    /// Register the shared channel bridge every `ChannelSource`/`ChannelSink`
    /// node resolves its named half through.
    ///
    /// `register_builtin_factories` attaches a default
    /// `pcs_connector_channel::ChannelRegistry` automatically when
    /// `connector-channel` is enabled; call this to supply a different
    /// instance (for example, to share one registry across two independently
    /// built services).
    pub fn with_channel_bridge(mut self, channels: Arc<dyn ChannelBridge>) -> Self {
        self.channels = Some(channels);
        self
    }

    /// Set the [`WasmEngine`] used to load every `wasm` node's module. If not
    /// set and the workflow declares one, `build_all` creates a default engine
    /// automatically and shares it across every `wasm` node.
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

    /// Register a transformer factory (builder-style chaining).
    pub fn register_transformer<F: TransformerFactory>(mut self, factory: F) -> Self {
        self.registry.register_transformer(factory);
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

    /// Mutably register a transformer factory.
    pub fn register_transformer_mut<F: TransformerFactory>(&mut self, factory: F) -> &mut Self {
        self.registry.register_transformer(factory);
        self
    }

    /// Access the inner registry (for inspection or passing to helpers).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Assemble one [`BuiltService`] per workflow declared in `config`, in
    /// declaration order, sharing one [`Registry`] and, when enabled, one
    /// [`Inspector`] topology across all of them.
    ///
    /// Every sink is wrapped in a [`RetryingSink`]. Sources are wrapped in a
    /// [`RetryingSource`] too, except under `run_mode kind="stream"`, where
    /// the stream runner's own re-poll is already the retry loop.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] if any workflow fails to build; see
    /// [`build_one`](Self::build_one) for the full list of failure modes.
    pub fn build_all(mut self, config: &ServiceConfig) -> Result<Vec<BuiltService>, PcsError> {
        let registry = Arc::new(std::mem::replace(&mut self.registry, Registry::new()));
        let retry_sources = sources_take_retry_wrapper(config);

        let mut out = Vec::with_capacity(config.workflows.len());
        for workflow in &config.workflows {
            out.push(self.build_one(&registry, workflow, retry_sources)?);
        }

        if let Some(inspector) = &self.inspector {
            let node_slices: Vec<&[BuiltNode]> = out.iter().map(|b| b.nodes.as_slice()).collect();
            inspector.set_topology(build_topology(
                config,
                &node_slices,
                inspector.topology().version + 1,
            ));
        }

        Ok(out)
    }

    /// Assemble a [`BuiltService`] for one declared `workflow`.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] if:
    /// - A transformer names an unregistered format.
    /// - A source or sink names an unregistered connector `type`.
    /// - A source or sink factory returns an error.
    /// - A `wasm`/`plugin` node names a module/library that cannot be
    ///   resolved, compiled, or described.
    /// - A `wasm`/`plugin` node declares neither an artifact nor a runtime
    ///   registered for its id through [`with_runtime`](Self::with_runtime).
    /// - The links do not carry matching components and schemas end to end
    ///   (see [`validate_workflow_graph`](super::validation::validate_workflow_graph)).
    fn build_one(
        &mut self,
        registry: &Arc<Registry>,
        workflow: &WorkflowSpec,
        retry_sources: bool,
    ) -> Result<BuiltService, PcsError> {
        let transformers = build_transformers(registry, &workflow.transformers)?;
        let order = workflow.topological_order()?;
        let nodes_meta = workflow.nodes();

        // Natural (declaration) index -> topological position, so `downstream`
        // can be filled by looking up a link's endpoints.
        let id_to_topo_index: HashMap<&str, usize> = order
            .iter()
            .enumerate()
            .map(|(topo_idx, &nat_idx)| (nodes_meta[nat_idx].0, topo_idx))
            .collect();

        let mut nodes: Vec<BuiltNode> = Vec::with_capacity(order.len());
        for &nat_idx in &order {
            let (id, kind) = nodes_meta[nat_idx];
            let built = match kind {
                NodeKind::Source => {
                    self.build_source_node(id, workflow, registry, &transformers, retry_sources)?
                }
                NodeKind::Sink => self.build_sink_node(id, workflow, registry, &transformers)?,
                NodeKind::Processor => self.build_processor_node(id, workflow)?,
            };
            nodes.push(built);
        }

        for link in &workflow.links {
            let &from = id_to_topo_index.get(link.from.as_str()).ok_or_else(|| {
                PcsError::configuration(format!(
                    "workflow '{}': link names undeclared node '{}'",
                    workflow.id, link.from
                ))
            })?;
            let &to = id_to_topo_index.get(link.to.as_str()).ok_or_else(|| {
                PcsError::configuration(format!(
                    "workflow '{}': link names undeclared node '{}'",
                    workflow.id, link.to
                ))
            })?;
            nodes[from].downstream.push(BuiltEdge {
                node: to,
                branch: link.branch.clone(),
            });
        }

        super::validation::validate_workflow_graph(&workflow.id, &nodes)?;

        Ok(BuiltService {
            workflow_id: workflow.id.clone(),
            workflow_name: workflow.name.clone(),
            nodes,
            registry: registry.clone(),
            inspector: self.inspector.clone(),
        })
    }

    fn build_source_node(
        &self,
        id: &str,
        workflow: &WorkflowSpec,
        registry: &Registry,
        transformers: &HashMap<String, Arc<dyn Transformer>>,
        retry_sources: bool,
    ) -> PcsResult<BuiltNode> {
        let spec = workflow
            .sources
            .iter()
            .find(|s| s.id == id)
            .expect("nodes() id must resolve to a declared source");
        let factory = registry.source(&spec.type_name).ok_or_else(|| {
            PcsError::configuration(format!(
                "no source factory registered for type '{}' \
                 (required by source '{}')",
                spec.type_name, spec.id
            ))
        })?;
        let bound = spec
            .transformer
            .as_deref()
            .map(|tid| transformers[tid].clone());
        let mut ctx = ConnectorContext::new(bound);
        if let Some(channels) = &self.channels {
            ctx = ctx.with_channels(channels.clone());
        }
        let built = factory.build(&spec.config, &ctx)?;
        let source: Box<dyn Source> = if retry_sources {
            Box::new(RetryingSource::new(
                built,
                spec.retry.to_system_config(),
                &spec.id,
            ))
        } else {
            built
        };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            type_name: spec.type_name.clone(),
            component: Some(intern_component_name(&spec.component)),
            kind: BuiltNodeKind::Source(source),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
        })
    }

    fn build_sink_node(
        &self,
        id: &str,
        workflow: &WorkflowSpec,
        registry: &Registry,
        transformers: &HashMap<String, Arc<dyn Transformer>>,
    ) -> PcsResult<BuiltNode> {
        let spec = workflow
            .sinks
            .iter()
            .find(|s| s.id == id)
            .expect("nodes() id must resolve to a declared sink");
        let factory = registry.sink(&spec.type_name).ok_or_else(|| {
            PcsError::configuration(format!(
                "no sink factory registered for type '{}' \
                 (required by sink '{}')",
                spec.type_name, spec.id
            ))
        })?;
        let bound = spec
            .transformer
            .as_deref()
            .map(|tid| transformers[tid].clone());
        let mut ctx = ConnectorContext::new(bound);
        if let Some(channels) = &self.channels {
            ctx = ctx.with_channels(channels.clone());
        }
        let sink = Box::new(RetryingSink::new(
            factory.build(&spec.config, &ctx)?,
            spec.retry.to_system_config(),
            &spec.id,
        ));
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            type_name: spec.type_name.clone(),
            component: Some(intern_component_name(&spec.component)),
            kind: BuiltNodeKind::Sink(sink),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
        })
    }

    /// Dispatch to whichever of `workflow.wasm` / `workflow.plugin` declared
    /// `id`. A [`NodeKind::Processor`] id always comes from exactly one of
    /// the two (or the corresponding feature would make it
    /// unrepresentable), so exactly one `#[cfg]` arm below matches at runtime.
    #[allow(unused_variables, unused_mut)]
    fn build_processor_node(&mut self, id: &str, workflow: &WorkflowSpec) -> PcsResult<BuiltNode> {
        #[cfg(feature = "wasm")]
        if let Some(spec) = workflow.wasm.iter().find(|w| w.id == id) {
            return self.build_wasm_node(spec, &workflow.id);
        }
        #[cfg(feature = "plugin")]
        if let Some(spec) = workflow.plugin.iter().find(|p| p.id == id) {
            return self.build_plugin_node(spec, &workflow.id);
        }
        Err(PcsError::configuration(format!(
            "workflow '{}': processor '{id}' names neither a wasm nor a plugin node",
            workflow.id
        )))
    }

    #[cfg(feature = "wasm")]
    fn build_wasm_node(
        &mut self,
        spec: &super::config::WasmSpec,
        workflow_id: &str,
    ) -> PcsResult<BuiltNode> {
        // The window geometry is injected into the config table as `window.*`
        // keys, so the guest's `get-config` answers one source of truth: the
        // block the operator wrote, not a copy in the `config` node.
        let spec = config_with_window(spec.clone());
        let (runtime, kind, artifact): (Box<dyn PipelineRuntime>, &'static str, Option<String>) =
            match &spec.module {
                Some(module) => {
                    let engine = self
                        .wasm_engine
                        .get_or_insert_with(|| {
                            WasmEngine::new().expect("wasmtime Engine creation failed")
                        })
                        .clone();
                    let loader = PipelineRuntimeLoader::new(engine, LocalModuleResolver::new());
                    let runtime = loader
                        .load(&spec.id, &spec)?
                        .with_identity(workflow_id.to_string(), spec.id.clone());
                    (Box::new(runtime), "wasm", Some(module.clone()))
                }
                None => {
                    let runtime = self.runtimes.remove(&spec.id).ok_or_else(|| {
                        PcsError::configuration(format!(
                            "processor '{}': {RUNTIME_SOURCE_HELP}",
                            spec.id
                        ))
                    })?;
                    (runtime, "native", None)
                }
            };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            type_name: kind.to_string(),
            component: None,
            kind: BuiltNodeKind::Processor { runtime, kind },
            downstream: Vec::new(),
            artifact,
            #[cfg(feature = "windows")]
            window: spec.window.clone(),
        })
    }

    #[cfg(feature = "plugin")]
    fn build_plugin_node(
        &mut self,
        spec: &super::config::PluginSpec,
        workflow_id: &str,
    ) -> PcsResult<BuiltNode> {
        // Same `window.*` config injection as the wasm path: the plugin's
        // `get_config` callback answers the geometry the operator declared.
        let spec = config_with_window(spec.clone());
        let (runtime, kind, artifact): (Box<dyn PipelineRuntime>, &'static str, Option<String>) =
            match &spec.library {
                Some(library) => {
                    let runtime = load_plugin_runtime(&spec, None)?
                        .with_identity(workflow_id.to_string(), spec.id.clone());
                    (Box::new(runtime), "plugin", Some(library.clone()))
                }
                None => {
                    let runtime = self.runtimes.remove(&spec.id).ok_or_else(|| {
                        PcsError::configuration(format!(
                            "processor '{}': {RUNTIME_SOURCE_HELP}",
                            spec.id
                        ))
                    })?;
                    (runtime, "native", None)
                }
            };
        Ok(BuiltNode {
            id: spec.id.clone(),
            name: spec.name.clone(),
            type_name: kind.to_string(),
            component: None,
            kind: BuiltNodeKind::Processor { runtime, kind },
            downstream: Vec::new(),
            artifact,
            #[cfg(feature = "windows")]
            window: spec.window.clone(),
        })
    }
}

/// Inject a node's `window` block into its `config` table as `window.*` keys.
///
/// The block and the table are two KDL nodes but one contract: the host tracks
/// the watermark from the block, and the processor or plugin reads the same
/// geometry back through `get-config`. Building the enriched spec keeps every
/// call site (loader, plugin host) reading `spec.config` unchanged.
#[cfg(feature = "windows")]
fn config_with_window<T>(mut spec: T) -> T
where
    T: WindowConfigCarrier,
{
    if let Some(window) = spec.window_config() {
        for (key, value) in window.config_pairs() {
            spec.config_mut().entry(key).or_insert(value);
        }
    }
    spec
}

#[cfg(not(feature = "windows"))]
fn config_with_window<T>(spec: T) -> T {
    spec
}

/// The two processor node specs, unified for [`config_with_window`].
#[cfg(feature = "windows")]
trait WindowConfigCarrier {
    fn window_config(&self) -> Option<&super::config::WindowConfig>;
    fn config_mut(&mut self) -> &mut HashMap<String, String>;
}

#[cfg(all(feature = "windows", feature = "wasm"))]
impl WindowConfigCarrier for super::config::WasmSpec {
    fn window_config(&self) -> Option<&super::config::WindowConfig> {
        self.window.as_ref()
    }
    fn config_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.config
    }
}

#[cfg(all(feature = "windows", feature = "plugin"))]
impl WindowConfigCarrier for super::config::PluginSpec {
    fn window_config(&self) -> Option<&super::config::WindowConfig> {
        self.window.as_ref()
    }
    fn config_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.config
    }
}

impl Default for ServiceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve every declared `transformer` node against `registry`, building one
/// instance per declaration.
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] when a transformer names an
/// unregistered format, or when its factory rejects its `options`.
fn build_transformers(
    registry: &Registry,
    specs: &[TransformerSpec],
) -> PcsResult<HashMap<String, Arc<dyn Transformer>>> {
    let mut out = HashMap::with_capacity(specs.len());
    for spec in specs {
        let factory = registry.transformers().get(&spec.format).ok_or_else(|| {
            let registered = registry.transformers().formats();
            let list = if registered.is_empty() {
                "none".to_string()
            } else {
                registered.join(", ")
            };
            PcsError::configuration(format!(
                "transformer '{}' names format '{}', which no transformer is registered for \
                 (registered: {list})",
                spec.id, spec.format
            ))
        })?;
        out.insert(spec.id.clone(), factory.build(&spec.options)?);
    }
    Ok(out)
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::dataset::Dataset;
    use crate::pipeline::Pipeline;
    use crate::service::config::{
        HttpConfig, NodeConfig, ObservabilityConfig, RetryConfig, ServiceMode, SinkSpec,
        SourceSpec, StandaloneConfig, WorkflowSpec,
    };
    use crate::service::registry::{SinkFactory, SourceFactory};
    use crate::system::{System, SystemMeta};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use pcs_connector::{ConfigMap, ConfigValue};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, PcsError> {
            use pcs_connector_channel::ChannelSource;
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
        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Sink>, PcsError> {
            use pcs_connector_channel::ChannelSink;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (sink, _rx) = ChannelSink::new(schema, 1);
            Ok(Box::new(sink))
        }
    }

    fn base_config(workflow: WorkflowSpec) -> ServiceConfig {
        ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![workflow],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
        }
    }

    fn empty_workflow(id: &str) -> WorkflowSpec {
        WorkflowSpec {
            id: id.to_string(),
            name: None,
            transformers: Vec::new(),
            sources: Vec::new(),
            #[cfg(feature = "wasm")]
            wasm: Vec::new(),
            #[cfg(feature = "plugin")]
            plugin: Vec::new(),
            sinks: Vec::new(),
            links: Vec::new(),
        }
    }

    #[test]
    fn test_source_straight_to_sink_builds_with_no_processor() {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            id: "src1".to_string(),
            name: None,
            type_name: "NoopSource".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.sinks.push(SinkSpec {
            id: "sink1".to_string(),
            name: None,
            type_name: "NoopSink".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.links.push(super::super::config::LinkSpec {
            from: "src1".to_string(),
            to: "sink1".to_string(),
            branch: None,
        });
        let config = base_config(workflow);

        let service = ServiceBuilder::new()
            .register_source(NoopSourceFactory)
            .register_sink(NoopSinkFactory)
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        assert_eq!(service.workflow_id, "w");
        assert_eq!(service.nodes.len(), 2);
        assert_eq!(service.nodes[0].id, "src1");
        assert_eq!(service.nodes[0].component, Some("comp1"));
        assert!(matches!(service.nodes[0].kind, BuiltNodeKind::Source(_)));
        assert_eq!(service.nodes[1].id, "sink1");
        assert!(matches!(service.nodes[1].kind, BuiltNodeKind::Sink(_)));
        assert_eq!(
            service.nodes[0].downstream,
            vec![BuiltEdge {
                node: 1,
                branch: None
            }]
        );
    }

    #[test]
    fn test_unknown_source_factory_returns_error() {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            id: "bad_src".to_string(),
            name: None,
            type_name: "GhostSource".to_string(),
            transformer: None,
            component: "comp".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("GhostSource"));
    }

    #[test]
    fn test_unknown_sink_factory_returns_error() {
        let mut workflow = empty_workflow("w");
        workflow.sinks.push(SinkSpec {
            id: "bad_sink".to_string(),
            name: None,
            type_name: "GhostSink".to_string(),
            transformer: None,
            component: "comp".to_string(),
            retry: RetryConfig::default(),
            config: ConfigValue::Object(ConfigMap::new()),
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("GhostSink"));
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_node_with_no_module_and_no_registered_runtime_is_an_error() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "p".to_string(),
            name: None,
            module: None,
            sha3_256: None,
            config: std::collections::HashMap::new(),
            #[cfg(feature = "windows")]
            window: None,
        });
        let config = base_config(workflow);

        let err = ServiceBuilder::new().build_all(&config).unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("processor 'p'"), "got: {err}");
        assert!(
            err.message().contains("ServiceBuilder::with_runtime"),
            "got: {err}"
        );
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_node_with_no_module_uses_the_registered_native_runtime() {
        let mut workflow = empty_workflow("w");
        workflow.wasm.push(super::super::config::WasmSpec {
            id: "p".to_string(),
            name: None,
            module: None,
            sha3_256: None,
            config: std::collections::HashMap::new(),
            #[cfg(feature = "windows")]
            window: None,
        });
        let config = base_config(workflow);

        let service = ServiceBuilder::new()
            .with_runtime("p", Box::new(Pipeline::new("native-p")))
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        assert_eq!(service.nodes.len(), 1);
        match &service.nodes[0].kind {
            BuiltNodeKind::Processor { runtime, kind } => {
                assert_eq!(*kind, "native");
                assert_eq!(runtime.name(), "native-p");
            }
            _ => panic!("expected a processor node"),
        }
        assert_eq!(service.nodes[0].artifact, None);
    }

    #[test]
    fn test_boxed_system_runs_on_runtime() {
        let mut pipeline = Pipeline::new("test");
        pipeline.add_system_boxed(Box::new(NoopSystem));
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async { pipeline.run().await }).unwrap();
    }

    #[test]
    fn test_build_all_returns_one_built_service_per_workflow() {
        let config = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs-test"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![empty_workflow("a"), empty_workflow("b")],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
        };

        let built = ServiceBuilder::new()
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"));

        assert_eq!(built.len(), 2);
        assert_eq!(built[0].workflow_id, "a");
        assert_eq!(built[1].workflow_id, "b");
    }

    // ── Retry wrappers ────────────────────────────────────────────────────────

    /// A source that fails the first `failures` `next_batch` calls, then
    /// yields one 1-row batch. `calls` counts every `next_batch` invocation.
    struct FlakySource {
        failures_left: usize,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Source for FlakySource {
        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.failures_left > 0 {
                self.failures_left -= 1;
                Err(PcsError::generic("flaky source failure"))
            } else {
                let batch = RecordBatch::try_new(
                    self.schema(),
                    vec![Arc::new(Int32Array::from(vec![1_i32]))],
                )
                .expect("schema should build a batch");
                Ok(Some(batch))
            }
        }
    }

    struct FlakySourceFactory {
        failures: usize,
        calls: Arc<AtomicUsize>,
    }

    impl SourceFactory for FlakySourceFactory {
        fn type_name(&self) -> &'static str {
            "FlakySource"
        }

        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, PcsError> {
            Ok(Box::new(FlakySource {
                failures_left: self.failures,
                calls: Arc::clone(&self.calls),
            }))
        }
    }

    /// A sink whose `write_batch` fails `failures` times, then succeeds.
    struct FlakySink {
        write_failures_left: usize,
        write_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Sink for FlakySink {
        async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<(), PcsError> {
            self.write_calls.fetch_add(1, Ordering::SeqCst);
            if self.write_failures_left > 0 {
                self.write_failures_left -= 1;
                Err(PcsError::generic("flaky sink write failure"))
            } else {
                Ok(())
            }
        }

        async fn finish(&mut self) -> Result<(), PcsError> {
            Ok(())
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
        }
    }

    struct FlakySinkFactory {
        write_failures: usize,
        write_calls: Arc<AtomicUsize>,
    }

    impl SinkFactory for FlakySinkFactory {
        fn type_name(&self) -> &'static str {
            "FlakySink"
        }

        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Sink>, PcsError> {
            Ok(Box::new(FlakySink {
                write_failures_left: self.write_failures,
                write_calls: Arc::clone(&self.write_calls),
            }))
        }
    }

    /// A one-link workflow whose flaky source feeds a flaky sink, so
    /// `build_all` produces a valid graph with both nodes retry-wrapped.
    fn flaky_workflow(source_retry: RetryConfig, sink_retry: RetryConfig) -> WorkflowSpec {
        let mut workflow = empty_workflow("w");
        workflow.sources.push(SourceSpec {
            id: "src1".to_string(),
            name: None,
            type_name: "FlakySource".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: source_retry,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.sinks.push(SinkSpec {
            id: "sink1".to_string(),
            name: None,
            type_name: "FlakySink".to_string(),
            transformer: None,
            component: "comp1".to_string(),
            retry: sink_retry,
            config: ConfigValue::Object(ConfigMap::new()),
        });
        workflow.links.push(super::super::config::LinkSpec {
            from: "src1".to_string(),
            to: "sink1".to_string(),
            branch: None,
        });
        workflow
    }

    #[tokio::test]
    async fn a_flaky_source_is_retried_until_it_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(
            RetryConfig {
                base_delay_ms: 1,
                ..Default::default()
            },
            RetryConfig::default(),
        );
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 2,
                calls: Arc::clone(&calls),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build_all(&base_config(workflow))
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(0);
        let BuiltNodeKind::Source(mut source) = node.kind else {
            panic!("expected a source node");
        };
        let out = source.next_batch().await.expect("retry should recover");
        assert_eq!(out.unwrap().num_rows(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two failures then success");
    }

    #[tokio::test]
    async fn a_source_with_retry_disabled_surfaces_the_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(
            RetryConfig {
                max_attempts: 1,
                ..Default::default()
            },
            RetryConfig::default(),
        );
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 2,
                calls: Arc::clone(&calls),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build_all(&base_config(workflow))
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(0);
        let BuiltNodeKind::Source(mut source) = node.kind else {
            panic!("expected a source node");
        };
        let err = source.next_batch().await.unwrap_err();
        let PcsError::RetryExhausted { attempts, .. } = err else {
            panic!("expected the first error wrapped as RetryExhausted");
        };
        assert_eq!(attempts, 1, "single attempt");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "single attempt");
    }

    /// `run_mode kind="stream"` hands the source over unwrapped: `run_stream`
    /// is itself the retry loop, so a second one inside `next_batch` would put
    /// its 100 ms first backoff on the item path in front of the runner's own
    /// 10 ms re-poll. The error must reach the runner as the connector raised
    /// it, on the first attempt, not wrapped in `RetryExhausted`.
    #[tokio::test]
    async fn a_stream_mode_source_is_not_retry_wrapped() {
        let calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(RetryConfig::default(), RetryConfig::default());
        let mut config = base_config(workflow);
        config.mode = ServiceMode::Standalone {
            config: StandaloneConfig {
                run_mode: RunMode::Stream,
            },
        };
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 2,
                calls: Arc::clone(&calls),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 0,
                write_calls: Arc::new(AtomicUsize::new(0)),
            })
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(0);
        let BuiltNodeKind::Source(mut source) = node.kind else {
            panic!("expected a source node");
        };
        let err = source.next_batch().await.unwrap_err();
        assert!(
            !matches!(err, PcsError::RetryExhausted { .. }),
            "a stream source's error reaches the runner unwrapped, got {err}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no retry inside next_batch"
        );
    }

    /// The sink keeps its wrapper in stream mode: nothing re-drives a dropped
    /// `write_batch`.
    #[tokio::test]
    async fn a_stream_mode_sink_keeps_its_retry_wrapper() {
        let write_calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(RetryConfig::default(), RetryConfig::default());
        let mut config = base_config(workflow);
        config.mode = ServiceMode::Standalone {
            config: StandaloneConfig {
                run_mode: RunMode::Stream,
            },
        };
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 0,
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 1,
                write_calls: Arc::clone(&write_calls),
            })
            .build_all(&config)
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(1);
        let BuiltNodeKind::Sink(mut sink) = node.kind else {
            panic!("expected a sink node");
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .expect("schema should build a batch");
        sink.write_batch(&batch)
            .await
            .expect("retry should recover");
        assert_eq!(
            write_calls.load(Ordering::SeqCst),
            2,
            "one failure then success"
        );
    }

    #[tokio::test]
    async fn a_flaky_sink_is_retried_until_it_succeeds() {
        let write_calls = Arc::new(AtomicUsize::new(0));
        let workflow = flaky_workflow(
            RetryConfig::default(),
            RetryConfig {
                base_delay_ms: 1,
                ..Default::default()
            },
        );
        let mut service = ServiceBuilder::new()
            .register_source(FlakySourceFactory {
                failures: 0,
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .register_sink(FlakySinkFactory {
                write_failures: 1,
                write_calls: Arc::clone(&write_calls),
            })
            .build_all(&base_config(workflow))
            .unwrap_or_else(|e| panic!("build failed: {e}"))
            .remove(0);

        let node = service.nodes.remove(1);
        let BuiltNodeKind::Sink(mut sink) = node.kind else {
            panic!("expected a sink node");
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .expect("schema should build a batch");
        sink.write_batch(&batch)
            .await
            .expect("retry should recover");
        assert_eq!(
            write_calls.load(Ordering::SeqCst),
            2,
            "one failure then success"
        );
    }
}
