//! # Service Runner
//!
//! Top-level configuration schema, factory registry, service builder, and the
//! standalone and cluster runners.
//!
//! ## Feature layering
//!
//! The service layer is split across two features so operators who only need
//! standalone mode do not pay for the Raft stack:
//!
//! - `service`: standalone-only control plane. HTTP API, TOML config, logging,
//!   Prometheus, the `pcs-service` binary, and the core
//!   [`config::ServiceConfig`] schema, which can describe a cluster config even
//!   though running one fails at startup.
//! - `service-cluster`: adds the cluster runner ([`cluster::run_cluster`]) and
//!   enables `distributed-raft`, compiling in the full openraft, redb, and TCP
//!   transport stack.
//!
//! ## Quick start
//!
//! 1. Build your [`Pipeline`](pcs_core::Pipeline) or `Box<dyn PipelineRuntime>`.
//! 2. Create a [`builder::ServiceBuilder`], call `with_runtime(...)`, and register IO factories.
//! 3. Load a [`config::ServiceConfig`] from TOML.
//! 4. Call [`builder::ServiceBuilder::build`] to get a [`builder::BuiltService`].

#[cfg(feature = "service")]
pub mod builder;
#[cfg(feature = "service-cluster")]
pub mod cluster;
#[cfg(feature = "service")]
pub mod config;
#[cfg(all(feature = "service", any(feature = "wasm", feature = "plugin")))]
pub(crate) mod digest;
#[cfg(feature = "service")]
pub mod factories;
#[cfg(feature = "service")]
pub mod http;
#[cfg(all(feature = "service", feature = "wasm"))]
pub mod loader;
#[cfg(feature = "service")]
pub mod logging;
#[cfg(all(feature = "service", feature = "plugin"))]
pub mod plugin_loader;
#[cfg(feature = "service")]
pub mod registry;
#[cfg(feature = "service")]
pub mod shutdown;
#[cfg(feature = "service")]
pub mod standalone;
#[cfg(feature = "service")]
pub mod stream;
#[cfg(feature = "service")]
pub mod validation;

#[cfg(feature = "service")]
pub use builder::{BuiltService, BuiltSink, BuiltSource, ServiceBuilder};
#[cfg(feature = "service-cluster")]
pub use cluster::{ClusterStats, run_cluster};
#[cfg(all(feature = "service", feature = "plugin"))]
pub use config::PluginSpec;
#[cfg(all(feature = "service", feature = "wasm"))]
pub use config::WasmSpec;
#[cfg(feature = "service")]
pub use config::{
    ClusterConfig, HttpConfig, LogFormat, NodeConfig, ObservabilityConfig, PeerSpec, PipelineSpec,
    RunMode, ServiceConfig, ServiceMode, SinkSpec, SourceSpec, StandaloneConfig,
};
#[cfg(feature = "service")]
pub use factories::register_builtin_factories;
#[cfg(feature = "service")]
pub use http::{
    ClusterProbe, ClusterProbeSnapshot, ServiceModeLabel, ServiceState, build_router,
    register_standard_metrics, serve_http, spawn_watchdog,
};
#[cfg(all(feature = "service", feature = "wasm"))]
pub use loader::{LocalModuleResolver, ModuleResolver, PipelineRuntimeLoader};
#[cfg(feature = "service")]
pub use logging::init_logging;
#[cfg(all(feature = "service", feature = "plugin"))]
pub use plugin_loader::load_plugin_runtime;
#[cfg(feature = "service")]
pub use registry::{Registry, SinkFactory, SourceFactory};
#[cfg(feature = "service")]
pub use shutdown::ShutdownCoordinator;
#[cfg(feature = "service")]
pub use standalone::{StandaloneStats, run_standalone};
#[cfg(feature = "service")]
pub use stream::run_stream;
#[cfg(feature = "service")]
pub use validation::validate_io_coverage;
