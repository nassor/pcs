//! # Service Configuration Schema
//!
//! This module defines the TOML configuration schema for the PCS service runner.
//! Operators write a single TOML file; the service parses, validates, and boots
//! from it.
//!
//! ## Example (standalone)
//!
//! ```toml
//! mode = "standalone"
//!
//! [node]
//! id = 1
//! data_dir = "/var/lib/pcs"
//!
//! [run_mode]
//! kind = "interval"
//! interval_ms = 5000
//!
//! [[pipeline.systems]]
//! name = "validate"
//! type = "ValidateOrder"
//!
//! [[pipeline.systems]]
//! name = "enrich"
//! type = "EnrichOrder"
//! ```
//!
//! ## Example (cluster)
//!
//! ```toml
//! mode = "cluster"
//! bootstrap = true
//!
//! [node]
//! id = 1
//! data_dir = "/var/lib/pcs"
//!
//! [[peers]]
//! id = 1
//! addr = "10.0.0.1:9000"
//!
//! [[peers]]
//! id = 2
//! addr = "10.0.0.2:9000"
//!
//! [[pipeline.systems]]
//! name = "process"
//! type = "ProcessBatch"
//! ```
//!
//! ## Env var substitution
//!
//! Any `${VAR}` placeholder in the TOML is replaced with the matching env var.
//! `${VAR:-default}` falls back to `default` if `VAR` is unset.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{PcsError, PcsResult};

/// Default opaque per-instance config: an empty TOML table.
fn default_table() -> toml::Value {
    toml::Value::Table(toml::Table::new())
}

// Flexible deserializers: accept either the native TOML type, or a string that
// parses to it. Lets env-var substitution work for non-string fields without
// breaking strict TOML validity in unsubstituted templates (`id = "${PCS_NODE_ID}"`).

fn de_u64_flexible<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum U64OrStr {
        Int(u64),
        Str(String),
    }
    match U64OrStr::deserialize(d)? {
        U64OrStr::Int(n) => Ok(n),
        U64OrStr::Str(s) => s.trim().parse().map_err(serde::de::Error::custom),
    }
}

fn de_bool_flexible<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrStr {
        Bool(bool),
        Str(String),
    }
    match BoolOrStr::deserialize(d)? {
        BoolOrStr::Bool(b) => Ok(b),
        BoolOrStr::Str(s) => s.trim().parse().map_err(serde::de::Error::custom),
    }
}

// ── NodeConfig ───────────────────────────────────────────────────────────────

/// Identity and storage configuration for a single service node.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeConfig {
    /// Raft node ID — must be unique in the cluster and stable across restarts.
    ///
    /// Accepts either an unquoted integer or a string parseable as `u64`. The
    /// string form lets env-var substitution work in TOML templates
    /// (`id = "${PCS_NODE_ID}"` stays strict-TOML-valid pre-substitution).
    #[serde(deserialize_with = "de_u64_flexible")]
    pub id: u64,
    /// Human-readable label used in logs and metrics. Optional.
    #[serde(default)]
    pub name: Option<String>,
    /// Filesystem path used for redb data files and WAL.
    pub data_dir: PathBuf,
}

// ── RunMode ───────────────────────────────────────────────────────────────────

/// How the standalone service drives pipeline execution.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunMode {
    /// Run continuously, re-entering the pipeline as the source produces work.
    #[default]
    Continuous,
    /// Run the pipeline exactly once, then exit.
    OneShot,
    /// Re-run the pipeline every `interval_ms` milliseconds.
    Interval {
        /// Milliseconds between successive pipeline runs.
        interval_ms: u64,
    },
}

// ── StandaloneConfig ──────────────────────────────────────────────────────────

/// Configuration for standalone (single-node) mode.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StandaloneConfig {
    /// Determines how the service drives pipeline runs.
    #[serde(default)]
    pub run_mode: RunMode,
}

// ── PeerSpec ──────────────────────────────────────────────────────────────────

/// A peer in the Raft cluster.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PeerSpec {
    /// Raft node ID of this peer.
    pub id: u64,
    /// Network address in `host:port` format.
    pub addr: String,
}

// ── ClusterConfig ─────────────────────────────────────────────────────────────

fn default_lease_ttl() -> u64 {
    30_000
}
fn default_election_timeout() -> u64 {
    1_500
}
fn default_heartbeat_interval() -> u64 {
    300
}
fn default_snapshot_log_interval() -> u64 {
    10_000
}

/// Configuration for cluster (multi-node Raft) mode.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    /// All peers in the cluster, including this node.
    pub peers: Vec<PeerSpec>,
    /// Bootstrap a fresh cluster when `data_dir` is empty.
    /// Set to `false` for nodes that join an existing cluster.
    ///
    /// Accepts either an unquoted bool or a string parseable as `bool`
    /// (`"true"` / `"false"`). The string form lets env-var substitution work
    /// in TOML templates without violating strict TOML pre-substitution.
    #[serde(default, deserialize_with = "de_bool_flexible")]
    pub bootstrap: bool,
    /// Batch-lease TTL in milliseconds. Must be >= 3 × `election_timeout_ms`.
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_ms: u64,
    /// Raft election timeout in milliseconds.
    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,
    /// Raft heartbeat interval in milliseconds.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,
    /// Take a log snapshot every N committed log entries.
    #[serde(default = "default_snapshot_log_interval")]
    pub snapshot_log_interval: u64,
}

// ── ServiceMode ───────────────────────────────────────────────────────────────

/// Which runtime mode the service runs in.
///
/// TOML representation uses an inline `mode` tag at the top of the document:
///
/// ```toml
/// mode = "standalone"
/// # — or —
/// mode = "cluster"
/// # plus [[peers]] entries
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ServiceMode {
    /// Single-node operation. No consensus required.
    Standalone {
        /// Standalone-specific options flattened into the top-level document.
        #[serde(flatten, default)]
        config: StandaloneConfig,
    },
    /// Multi-node Raft cluster.
    Cluster {
        /// Cluster-specific options flattened into the top-level document.
        #[serde(flatten)]
        config: ClusterConfig,
    },
}

// ── SystemInstance / ComponentInstance ────────────────────────────────────────

/// A named system instance to be constructed via the factory registry (S4).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SystemInstance {
    /// Unique name for this instance within the pipeline.
    pub name: String,
    /// Factory lookup key, e.g. `"ValidateOrder"`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Opaque per-system configuration passed to the factory.
    #[serde(default = "default_table")]
    pub config: toml::Value,
}

/// A named component instance to be constructed via the factory registry (S4).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ComponentInstance {
    /// Unique name for this instance.
    pub name: String,
    /// Factory lookup key.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Schema version for this component. Defaults to 1 when absent.
    #[serde(default)]
    pub version: Option<u32>,
    /// Opaque per-component configuration passed to the factory.
    #[serde(default = "default_table")]
    pub config: toml::Value,
}

// ── WasmSpec / PipelineSpec ───────────────────────────────────────────────────

/// WASM guest pipeline specification (requires the `wasm` feature at runtime).
///
/// ```toml
/// [pipeline.wasm]
/// module = "pipelines/transform.wasm"
/// sha3_256 = "abc123..."
/// watch = false
///
/// [pipeline.wasm.config]
/// batch_size = "1000"
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WasmSpec {
    /// Path to the `.wasm` component file (relative or absolute).
    pub module: String,
    /// Optional expected SHA3-256 hex digest of the module bytes. Validation
    /// fails at load time if the digest does not match. The value may carry
    /// an optional `sha3-256:` prefix.
    #[serde(default)]
    pub sha3_256: Option<String>,
    /// Re-load the module on disk change (hot-reload). Defaults to `false`.
    #[serde(default)]
    pub watch: bool,
    /// Opaque key-value config passed to the guest's `init` function.
    #[serde(default)]
    pub config: HashMap<String, String>,
}

/// Top-level workload spec — describes the systems the `Scheduler` runs and
/// the components the `Pipeline` holds.
///
/// Exactly one of `systems` or `wasm` must be non-empty/present.
///
/// Note: `PipelineSpec` and the TOML `[pipeline]` table refer to the abstract
/// workload definition (systems + components), not the `Pipeline` Rust type
/// (the columnar data container).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PipelineSpec {
    /// Systems in registration order. DAG scheduling applies.
    /// Mutually exclusive with `wasm`. Must be non-empty when `wasm` is absent.
    #[serde(default)]
    pub systems: Vec<SystemInstance>,
    /// Components to register before the first pipeline run.
    /// For WASM pipelines the component list is derived from the guest's
    /// `describe()` response and this field is ignored.
    #[serde(default)]
    pub components: Vec<ComponentInstance>,
    /// WASM guest module to run instead of native systems.
    /// Mutually exclusive with `systems`.
    #[cfg(feature = "wasm")]
    #[serde(default)]
    pub wasm: Option<WasmSpec>,
}

// ── SourceSpec / SinkSpec ─────────────────────────────────────────────────────

/// Declares an IO source that feeds rows into a component column.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceSpec {
    /// Unique name.
    pub name: String,
    /// Factory lookup key.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Name of the `ComponentInstance` this source writes into.
    pub target_component: String,
    /// Opaque per-source configuration.
    #[serde(default = "default_table")]
    pub config: toml::Value,
}

/// Declares an IO sink that drains rows from a component column.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SinkSpec {
    /// Unique name.
    pub name: String,
    /// Factory lookup key.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Name of the `ComponentInstance` this sink reads from.
    pub source_component: String,
    /// Opaque per-sink configuration.
    #[serde(default = "default_table")]
    pub config: toml::Value,
}

// ── HttpConfig ────────────────────────────────────────────────────────────────

fn default_http_bind() -> String {
    "0.0.0.0:8080".to_string()
}

/// HTTP control-plane configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HttpConfig {
    /// Socket address to bind the HTTP server on.
    #[serde(default = "default_http_bind")]
    pub bind: String,
    /// Disable the HTTP control plane entirely.
    #[serde(default)]
    pub disabled: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: default_http_bind(),
            disabled: false,
        }
    }
}

// ── ObservabilityConfig ───────────────────────────────────────────────────────

/// Log output format.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Human-readable output for TTY / development.
    #[default]
    Pretty,
    /// Structured JSON for production log aggregators.
    Json,
}

fn default_log_level() -> String {
    "info".to_string()
}

/// Observability (logging) configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObservabilityConfig {
    /// Log output format.
    #[serde(default)]
    pub log_format: LogFormat,
    /// Tracing level filter string (`"info"`, `"debug"`, etc.).
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Pretty,
            log_level: default_log_level(),
        }
    }
}

// ── ServiceConfig ─────────────────────────────────────────────────────────────

/// Top-level service configuration.
///
/// Load from a TOML file with [`ServiceConfig::load`]:
///
/// ```no_run
/// # #[cfg(feature = "service")]
/// # {
/// use pcs_service::service::config::ServiceConfig;
/// let cfg = ServiceConfig::load("config.toml").unwrap();
/// # }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceConfig {
    /// Node identity and storage.
    pub node: NodeConfig,
    /// Runtime mode (standalone or cluster), flattened into the document.
    #[serde(flatten)]
    pub mode: ServiceMode,
    /// Pipeline systems and components to instantiate.
    pub pipeline: PipelineSpec,
    /// Data sources feeding the pipeline.
    #[serde(default)]
    pub sources: Vec<SourceSpec>,
    /// Data sinks draining the pipeline.
    #[serde(default)]
    pub sinks: Vec<SinkSpec>,
    /// HTTP control-plane options.
    #[serde(default)]
    pub http: HttpConfig,
    /// Logging / observability options.
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

impl ServiceConfig {
    /// Load a [`ServiceConfig`] from a TOML file at `path`.
    ///
    /// The file is read, env-var placeholders are substituted, the TOML is
    /// parsed, and semantic validation is applied before returning.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] for IO failures, TOML parse errors,
    /// or any validation constraint violation.
    pub fn load(path: impl AsRef<std::path::Path>) -> PcsResult<Self> {
        let raw = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            PcsError::configuration(format!(
                "reading config file {}: {e}",
                path.as_ref().display()
            ))
        })?;
        let substituted = substitute_env_vars(&raw)?;
        let config: ServiceConfig = toml::from_str(&substituted)
            .map_err(|e| PcsError::configuration(format!("parsing TOML: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Validate semantic constraints that serde cannot enforce.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] describing the first constraint
    /// violation found.
    pub fn validate(&self) -> PcsResult<()> {
        // 1. data_dir must be non-empty.
        if self.node.data_dir.as_os_str().is_empty() {
            return Err(PcsError::configuration("node.data_dir must not be empty"));
        }

        // 2. Cluster-mode constraints.
        if let ServiceMode::Cluster { config } = &self.mode {
            if config.peers.is_empty() {
                return Err(PcsError::configuration(
                    "cluster mode requires at least one peer",
                ));
            }

            // Peer IDs must be unique.
            let mut seen_ids: HashSet<u64> = HashSet::new();
            for peer in &config.peers {
                if !seen_ids.insert(peer.id) {
                    return Err(PcsError::configuration(format!(
                        "cluster peers contain duplicate id: {}",
                        peer.id
                    )));
                }
            }

            // This node's ID must appear in the peer list.
            let node_id = self.node.id;
            if !config.peers.iter().any(|p| p.id == node_id) {
                return Err(PcsError::configuration(format!(
                    "node id {node_id} is not listed in cluster.peers"
                )));
            }

            // lease_ttl_ms must be >= 3 * election_timeout_ms.
            let min_lease = config.election_timeout_ms.saturating_mul(3);
            if config.lease_ttl_ms < min_lease {
                return Err(PcsError::configuration(format!(
                    "lease_ttl_ms ({}) must be >= 3 × election_timeout_ms ({}) = {}",
                    config.lease_ttl_ms, config.election_timeout_ms, min_lease,
                )));
            }

            // Cluster mode cannot use declared sources: the cluster runner ingests
            // via PartitionSource (a distributed pull mechanism), not the Source
            // trait used by standalone sources.  Declared sources would be silently
            // ignored, leaving the pipeline idle.  Fail early with a clear message.
            if !self.sources.is_empty() {
                return Err(PcsError::configuration(format!(
                    "cluster mode does not support declared 'sources:' entries \
                     ({} source(s) declared). \
                     Cluster mode ingests via PartitionSource — batches must be \
                     pre-registered via register_master_batch or a separate producer \
                     service. Remove the 'sources:' section or switch to standalone mode.",
                    self.sources.len()
                )));
            }
        }

        // 3. Wasm / native mutual exclusivity.
        #[cfg(feature = "wasm")]
        let is_wasm = self.pipeline.wasm.is_some();
        #[cfg(not(feature = "wasm"))]
        let is_wasm = false;

        if is_wasm && !self.pipeline.systems.is_empty() {
            return Err(PcsError::configuration(
                "pipeline.wasm and pipeline.systems are mutually exclusive — \
                 remove one or the other",
            ));
        }
        if !is_wasm && self.pipeline.systems.is_empty() {
            return Err(PcsError::configuration(
                "pipeline must have at least one system when pipeline.wasm is absent",
            ));
        }

        // 4. Pipeline system names must be unique (native path only).
        if !is_wasm {
            let mut seen_sys: HashSet<&str> = HashSet::new();
            for sys in &self.pipeline.systems {
                if !seen_sys.insert(sys.name.as_str()) {
                    return Err(PcsError::configuration(format!(
                        "pipeline has duplicate system name: {}",
                        sys.name
                    )));
                }
            }

            // Build component name set for cross-reference checks.
            // Skip for wasm: the guest's describe() provides the component list
            // at load time, not at config-parse time.
            let component_names: HashSet<&str> = self
                .pipeline
                .components
                .iter()
                .map(|c| c.name.as_str())
                .collect();

            // 5. Every SourceSpec.target_component must reference a known component.
            for src in &self.sources {
                if !component_names.contains(src.target_component.as_str()) {
                    return Err(PcsError::configuration(format!(
                        "source '{}' references unknown component '{}'",
                        src.name, src.target_component
                    )));
                }
            }

            // 6. Every SinkSpec.source_component must reference a known component.
            for sink in &self.sinks {
                if !component_names.contains(sink.source_component.as_str()) {
                    return Err(PcsError::configuration(format!(
                        "sink '{}' references unknown component '{}'",
                        sink.name, sink.source_component
                    )));
                }
            }
        }

        // 7. HTTP bind address must parse as a SocketAddr (unless disabled).
        if !self.http.disabled {
            SocketAddr::from_str(&self.http.bind).map_err(|e| {
                PcsError::configuration(format!(
                    "http.bind '{}' is not a valid socket address: {e}",
                    self.http.bind
                ))
            })?;
        }

        Ok(())
    }
}

// ── Env-var substitution ──────────────────────────────────────────────────────

/// Substitute `${VAR}` and `${VAR:-default}` placeholders in `raw`.
///
/// - `${VAR}` — replaced with the value of env var `VAR`. Returns
///   [`PcsError::Configuration`] if `VAR` is not set.
/// - `${VAR:-default}` — replaced with the value of `VAR`, or `default` if
///   `VAR` is not set.
pub fn substitute_env_vars(raw: &str) -> PcsResult<String> {
    let mut result = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            // Consume the `{`.
            chars.next();

            // Read until `}`.
            let mut placeholder = String::new();
            let mut closed = false;
            for inner in chars.by_ref() {
                if inner == '}' {
                    closed = true;
                    break;
                }
                placeholder.push(inner);
            }

            if !closed {
                return Err(PcsError::configuration(
                    "unclosed '${' in config — missing '}'",
                ));
            }

            // Split on `:-` for default-value syntax.
            let (var_name, fallback) = if let Some(pos) = placeholder.find(":-") {
                (&placeholder[..pos], Some(&placeholder[pos + 2..]))
            } else {
                (placeholder.as_str(), None)
            };

            match std::env::var(var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => match fallback {
                    Some(default) => result.push_str(default),
                    None => {
                        return Err(PcsError::configuration(format!(
                            "env var '${{{var_name}}}' is not set and has no default"
                        )));
                    }
                },
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn minimal_standalone_toml() -> &'static str {
        r#"
mode = "standalone"

[node]
id = 1
data_dir = "/tmp/pcs-test"

[[pipeline.systems]]
name = "process"
type = "ProcessBatch"
"#
    }

    fn minimal_cluster_toml() -> &'static str {
        r#"
mode = "cluster"
bootstrap = true

[node]
id = 1
data_dir = "/tmp/pcs-cluster"

[[peers]]
id = 1
addr = "127.0.0.1:9000"

[[peers]]
id = 2
addr = "127.0.0.2:9000"

[[pipeline.systems]]
name = "process"
type = "ProcessBatch"
"#
    }

    fn full_config() -> ServiceConfig {
        ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: Some("node-1".to_string()),
                data_dir: PathBuf::from("/tmp/pcs"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig {
                    run_mode: RunMode::Interval { interval_ms: 5_000 },
                },
            },
            pipeline: PipelineSpec {
                systems: vec![SystemInstance {
                    name: "validate".to_string(),
                    type_name: "ValidateOrder".to_string(),
                    config: default_table(),
                }],
                components: vec![ComponentInstance {
                    name: "orders".to_string(),
                    type_name: "OrderComponent".to_string(),
                    version: None,
                    config: default_table(),
                }],
                #[cfg(feature = "wasm")]
                wasm: None,
            },
            sources: vec![SourceSpec {
                name: "kafka_in".to_string(),
                type_name: "KafkaSource".to_string(),
                target_component: "orders".to_string(),
                config: default_table(),
            }],
            sinks: vec![SinkSpec {
                name: "pg_out".to_string(),
                type_name: "PostgresSink".to_string(),
                source_component: "orders".to_string(),
                config: default_table(),
            }],
            http: HttpConfig {
                bind: "0.0.0.0:8080".to_string(),
                disabled: false,
            },
            observability: ObservabilityConfig {
                log_format: LogFormat::Json,
                log_level: "debug".to_string(),
            },
        }
    }

    // ── Test 1: Round-trip via TOML literal ───────────────────────────────────
    //
    // Production path is load-only (`toml::from_str`). We verify the canonical
    // TOML literal deserializes into the same shape `full_config()` constructs
    // by hand. We don't attempt `toml::to_string` round-tripping because the
    // serializer's interaction with `#[serde(flatten)]` + internally-tagged
    // enums can produce table-before-bare-key invalid TOML for cluster mode.

    #[test]
    fn test_full_standalone_toml_deserialises() {
        let raw = r#"
mode = "standalone"

[node]
id = 1
name = "node-1"
data_dir = "/tmp/pcs"

[run_mode]
kind = "interval"
interval_ms = 5000

[[pipeline.systems]]
name = "validate"
type = "ValidateOrder"

[[pipeline.components]]
name = "orders"
type = "OrderComponent"

[[sources]]
name = "kafka_in"
type = "KafkaSource"
target_component = "orders"

[[sinks]]
name = "pg_out"
type = "PostgresSink"
source_component = "orders"

[http]
bind = "0.0.0.0:8080"
disabled = false

[observability]
log_format = "json"
log_level = "debug"
"#;
        let restored: ServiceConfig = toml::from_str(raw).expect("deserialize");
        let original = full_config();

        assert_eq!(restored.node.id, original.node.id);
        assert_eq!(restored.node.name, original.node.name);
        assert_eq!(restored.node.data_dir, original.node.data_dir);
        assert_eq!(restored.pipeline.systems.len(), 1);
        assert_eq!(restored.pipeline.systems[0].name, "validate");
        assert_eq!(restored.sources.len(), 1);
        assert_eq!(restored.sources[0].target_component, "orders");
        assert_eq!(restored.sinks.len(), 1);
        assert_eq!(restored.sinks[0].source_component, "orders");
        assert_eq!(restored.http.bind, "0.0.0.0:8080");
        assert!(!restored.http.disabled);
        assert_eq!(restored.observability.log_level, "debug");
        assert_eq!(restored.observability.log_format, LogFormat::Json);
        match restored.mode {
            ServiceMode::Standalone { config } => {
                assert_eq!(config.run_mode, RunMode::Interval { interval_ms: 5_000 });
            }
            _ => panic!("expected standalone"),
        }
    }

    // ── Test 2: Minimal standalone with defaults ──────────────────────────────

    #[test]
    fn test_minimal_standalone_parses_with_defaults() {
        let cfg: ServiceConfig = toml::from_str(minimal_standalone_toml()).expect("parse");

        assert_eq!(cfg.node.id, 1);
        assert!(cfg.node.name.is_none());
        assert_eq!(cfg.node.data_dir, PathBuf::from("/tmp/pcs-test"));

        // Mode is standalone.
        assert!(matches!(cfg.mode, ServiceMode::Standalone { .. }));

        // Defaults applied.
        assert!(cfg.sources.is_empty());
        assert!(cfg.sinks.is_empty());
        assert_eq!(cfg.http.bind, "0.0.0.0:8080");
        assert!(!cfg.http.disabled);
        assert_eq!(cfg.observability.log_level, "info");
        assert_eq!(cfg.observability.log_format, LogFormat::Pretty);
        assert_eq!(cfg.pipeline.systems[0].name, "process");
    }

    // ── Test 3: Minimal cluster config parses ─────────────────────────────────

    #[test]
    fn test_minimal_cluster_parses() {
        let cfg: ServiceConfig = toml::from_str(minimal_cluster_toml()).expect("parse");

        match &cfg.mode {
            ServiceMode::Cluster { config } => {
                assert_eq!(config.peers.len(), 2);
                assert!(config.bootstrap);
                assert_eq!(config.lease_ttl_ms, default_lease_ttl());
                assert_eq!(config.election_timeout_ms, default_election_timeout());
                assert_eq!(config.heartbeat_interval_ms, default_heartbeat_interval());
                assert_eq!(
                    config.snapshot_log_interval,
                    default_snapshot_log_interval()
                );
            }
            _ => panic!("expected cluster mode"),
        }
    }

    // ── Test 4: Missing required field produces clear error ───────────────────

    #[test]
    fn test_missing_node_id_produces_error() {
        let raw = r#"
mode = "standalone"

[node]
data_dir = "/tmp/pcs"

[[pipeline.systems]]
name = "proc"
type = "Proc"
"#;
        let result: Result<ServiceConfig, _> = toml::from_str(raw);
        assert!(result.is_err(), "expected parse error for missing node.id");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("id") || err.contains("missing field"),
            "error should mention missing field: {err}"
        );
    }

    // ── Test 5: Invalid mode produces clear error ──────────────────────────────

    #[test]
    fn test_invalid_mode_produces_error() {
        let raw = r#"
mode = "turbo_mode"

[node]
id = 1
data_dir = "/tmp/pcs"

[[pipeline.systems]]
name = "proc"
type = "Proc"
"#;
        let result: Result<ServiceConfig, _> = toml::from_str(raw);
        assert!(result.is_err(), "expected parse error for unknown mode");
    }

    // ── Test 6: Cluster without this node's id in peer list rejected ──────────

    #[test]
    fn test_cluster_node_not_in_peers_rejected() {
        let raw = r#"
mode = "cluster"

[node]
id = 99
data_dir = "/tmp/pcs"

[[peers]]
id = 1
addr = "127.0.0.1:9000"

[[peers]]
id = 2
addr = "127.0.0.2:9000"

[[pipeline.systems]]
name = "proc"
type = "Proc"
"#;
        let cfg: ServiceConfig = toml::from_str(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("99"),
            "error should mention node id 99: {err}"
        );
    }

    // ── Test 7: lease_ttl_ms < 3 * election_timeout_ms rejected ──────────────

    #[test]
    fn test_cluster_insufficient_lease_ttl_rejected() {
        let raw = r#"
mode = "cluster"
lease_ttl_ms = 1000
election_timeout_ms = 1000

[node]
id = 1
data_dir = "/tmp/pcs"

[[peers]]
id = 1
addr = "127.0.0.1:9000"

[[pipeline.systems]]
name = "proc"
type = "Proc"
"#;
        let cfg: ServiceConfig = toml::from_str(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("lease_ttl_ms"),
            "error should mention lease_ttl_ms: {err}"
        );
    }

    // ── Test 8: Source references unknown component rejected ──────────────────

    #[test]
    fn test_source_unknown_component_rejected() {
        let raw = r#"
mode = "standalone"

[node]
id = 1
data_dir = "/tmp/pcs"

[[pipeline.systems]]
name = "proc"
type = "Proc"

[[sources]]
name = "my_source"
type = "KafkaSource"
target_component = "nonexistent_component"
"#;
        let cfg: ServiceConfig = toml::from_str(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("nonexistent_component"),
            "error should name the unknown component: {err}"
        );
    }

    // ── Test 9: Env var substitution — ${HOME} replaced ───────────────────────

    #[test]
    fn test_env_var_substitution_set_var() {
        // HOME is virtually always set; use a known test var as a fallback.
        // SAFETY: single-threaded test; no other thread reads this var.
        unsafe {
            std::env::set_var("PCS_TEST_DATA_DIR_9", "/tmp/envtest");
        }
        let raw = "data_dir: ${PCS_TEST_DATA_DIR_9}";
        let out = substitute_env_vars(raw).expect("substitution");
        assert_eq!(out, "data_dir: /tmp/envtest");
        // SAFETY: single-threaded test.
        unsafe {
            std::env::remove_var("PCS_TEST_DATA_DIR_9");
        }
    }

    // ── Test 10: Env var with default fallback ────────────────────────────────

    #[test]
    fn test_env_var_substitution_default_fallback() {
        // Ensure the var is unset.
        // SAFETY: single-threaded test; no other thread reads this var.
        unsafe {
            std::env::remove_var("PCS_DEFINITELY_NOT_SET_10");
        }
        let raw = "data_dir: ${PCS_DEFINITELY_NOT_SET_10:-/fallback/path}";
        let out = substitute_env_vars(raw).expect("substitution with fallback");
        assert_eq!(out, "data_dir: /fallback/path");
    }

    // ── Test 11: Load from on-disk TOML file ──────────────────────────────────

    #[test]
    fn test_load_from_disk_toml() {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(minimal_standalone_toml().as_bytes())
            .expect("write");
        let path = file.path().to_path_buf();

        let cfg = ServiceConfig::load(&path).expect("load");
        assert_eq!(cfg.node.id, 1);
        assert!(matches!(cfg.mode, ServiceMode::Standalone { .. }));
    }

    // ── Extra: duplicate system names rejected ────────────────────────────────

    #[test]
    fn test_duplicate_system_names_rejected() {
        let raw = r#"
mode = "standalone"

[node]
id = 1
data_dir = "/tmp/pcs"

[[pipeline.systems]]
name = "dup"
type = "Foo"

[[pipeline.systems]]
name = "dup"
type = "Bar"
"#;
        let cfg: ServiceConfig = toml::from_str(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("dup"),
            "error should name the duplicate: {err}"
        );
    }

    // ── Extra: RunMode interval round-trips ───────────────────────────────────

    #[test]
    fn test_run_mode_interval_round_trip() {
        let raw = r#"
kind = "interval"
interval_ms = 3000
"#;
        let restored: RunMode = toml::from_str(raw).expect("deserialize");
        assert_eq!(restored, RunMode::Interval { interval_ms: 3_000 });
    }

    // ── Extra: HttpConfig disabled skips bind parse ───────────────────────────

    #[test]
    fn test_http_disabled_skips_bind_validation() {
        let raw = r#"
mode = "standalone"

[node]
id = 1
data_dir = "/tmp/pcs"

[[pipeline.systems]]
name = "proc"
type = "Proc"

[http]
bind = "not-a-socket-addr"
disabled = true
"#;
        let cfg: ServiceConfig = toml::from_str(raw).expect("parse");
        // validate should succeed because disabled=true bypasses bind check.
        cfg.validate()
            .expect("disabled http should not validate bind");
    }

    // ── Extra: unclosed ${ returns error ─────────────────────────────────────

    #[test]
    fn test_unclosed_placeholder_returns_error() {
        let raw = "data_dir: ${UNCLOSED";
        let err = substitute_env_vars(raw).unwrap_err();
        assert!(
            err.to_string().contains("unclosed"),
            "error should mention unclosed: {err}"
        );
    }

    // ── Cluster mode with declared sources is rejected at validate ──

    #[test]
    fn test_cluster_mode_with_sources_rejected_at_validate() {
        let raw = r#"
mode = "cluster"

[node]
id = 1
data_dir = "/tmp/pcs"

[[peers]]
id = 1
addr = "127.0.0.1:9000"

[[peers]]
id = 2
addr = "127.0.0.2:9000"

[[pipeline.systems]]
name = "proc"
type = "Proc"

[[pipeline.components]]
name = "orders"
type = "OrderComp"

[[sources]]
name = "kafka_in"
type = "KafkaSource"
target_component = "orders"
"#;
        let cfg: ServiceConfig = toml::from_str(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert_eq!(
            err.category(),
            "configuration",
            "expected configuration error: {err}"
        );
        assert!(
            err.to_string().contains("cluster mode"),
            "error should mention cluster mode: {err}"
        );
        assert!(
            err.to_string().contains("source"),
            "error should mention sources: {err}"
        );
    }

    // ── Empty systems + no wasm rejected ─────────────────────────────────────

    #[test]
    fn test_empty_systems_without_wasm_rejected() {
        let raw = r#"
mode = "standalone"

[node]
id = 1
data_dir = "/tmp/pcs"

[pipeline]
systems = []
"#;
        let cfg: ServiceConfig = toml::from_str(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("at least one system"),
            "error should mention systems requirement: {err}"
        );
    }

    // ── Wasm + systems mutual exclusivity ────────────────────────────────────

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_and_systems_mutually_exclusive() {
        use std::collections::HashMap;
        let cfg = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            pipeline: PipelineSpec {
                systems: vec![SystemInstance {
                    name: "proc".to_string(),
                    type_name: "Proc".to_string(),
                    config: default_table(),
                }],
                components: vec![],
                wasm: Some(WasmSpec {
                    module: "pipeline.wasm".to_string(),
                    sha3_256: None,
                    watch: false,
                    config: HashMap::new(),
                }),
            },
            sources: vec![],
            sinks: vec![],
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("mutually exclusive"),
            "error should mention mutual exclusivity: {err}"
        );
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_only_pipeline_validates() {
        use std::collections::HashMap;
        let cfg = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            pipeline: PipelineSpec {
                systems: vec![],
                components: vec![],
                wasm: Some(WasmSpec {
                    module: "pipeline.wasm".to_string(),
                    sha3_256: None,
                    watch: false,
                    config: HashMap::new(),
                }),
            },
            sources: vec![],
            sinks: vec![],
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
        };
        cfg.validate().expect("wasm-only pipeline should be valid");
    }

    // Standalone mode with sources is still valid.
    #[test]
    fn test_standalone_mode_with_sources_allowed() {
        let raw = r#"
mode = "standalone"

[node]
id = 1
data_dir = "/tmp/pcs"

[[pipeline.systems]]
name = "proc"
type = "Proc"

[[pipeline.components]]
name = "orders"
type = "OrderComp"

[[sources]]
name = "kafka_in"
type = "KafkaSource"
target_component = "orders"
"#;
        let cfg: ServiceConfig = toml::from_str(raw).expect("parse should succeed");
        // Validate should succeed — source cross-reference check only cares that
        // target_component is in pipeline.components, which it is.
        cfg.validate()
            .expect("standalone mode with sources should be valid");
    }
}
