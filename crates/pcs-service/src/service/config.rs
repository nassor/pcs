//! # Service Configuration Schema
//!
//! KDL configuration schema for the PCS service runner. Operators write a
//! single file; the service parses, validates, and boots from it.
//!
//! ## Example (standalone)
//!
//! ```kdl
//! mode "standalone"
//!
//! node id=1 data_dir="/var/lib/pcs"
//!
//! run_mode kind="interval" interval_ms=5000
//!
//! workflow "etl" name="ETL" {
//!     wasm "transform" module="pipelines/transform.wasm" {
//!         config batch_size="1000"
//!     }
//! }
//! ```
//!
//! One `workflow` declares the whole DAG: sources, `wasm`/`plugin` processors,
//! sinks and transformers, each with a mandatory id and an optional name,
//! connected by explicit `link from="..." to="..."` declarations. See
//! [`WorkflowSpec`].
//!
//! Cluster mode instead sets `mode "cluster"`, `bootstrap` on the first node,
//! and one `peer` node (`id`, `addr`) per cluster member. A cluster-mode
//! workflow declares exactly one `wasm` or `plugin` node and no source, sink
//! or link: the distributed runner ingests through `PartitionSource` and
//! drives one runtime per node.
//!
//! ## Env var substitution
//!
//! Any `${VAR}` placeholder in the file is replaced with the matching env var.
//! `${VAR:-default}` falls back to `default` if `VAR` is unset. Substitution
//! runs over the raw text before the parser, so a template stays valid KDL
//! either side of it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use pcs_config::one_or_many;
use serde::{Deserialize, Serialize};

use crate::error::{PcsError, PcsResult};

/// The configuration language, re-exported so an embedder that reads
/// [`SourceSpec::config`] or writes a factory names one path.
pub use pcs_config::{ConfigMap, ConfigValue, from_kdl_str, substitute_env_vars};

/// Default opaque per-instance config: an empty table.
///
/// Serde's own `Default` for the value type is null, which every factory
/// rejects.
fn default_config() -> ConfigValue {
    ConfigValue::Object(ConfigMap::new())
}

// Flexible deserializers: accept either the native type, or a string that
// parses to it. Lets env-var substitution work for non-string fields
// (`id="${PCS_NODE_ID}"`).

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

/// Identity and storage configuration for a single service node.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeConfig {
    /// Raft node ID: unique in the cluster and stable across restarts.
    ///
    /// Accepts either a bare integer or a string parseable as `u64`. The
    /// string form lets env-var substitution work in templates
    /// (`id="${PCS_NODE_ID}"` stays valid KDL before substitution).
    #[serde(deserialize_with = "de_u64_flexible")]
    pub id: u64,
    /// Human-readable label used in logs and metrics. Optional.
    #[serde(default)]
    pub name: Option<String>,
    /// Filesystem path used for redb data files and WAL.
    pub data_dir: PathBuf,
}

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
    /// Process each source batch individually as it arrives (streaming).
    ///
    /// Requires at least one declared source and standalone mode. Sources
    /// are pulled round-robin: each arriving batch, from whichever source
    /// produced it, is one item. No inter-item sleep: latency is bounded by
    /// the pipeline itself.
    Stream,
}

/// Configuration for standalone (single-node) mode.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StandaloneConfig {
    /// Determines how the service drives pipeline runs.
    #[serde(default)]
    pub run_mode: RunMode,
}

/// A peer in the Raft cluster.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PeerSpec {
    /// Raft node ID of this peer.
    pub id: u64,
    /// Network address in `host:port` format.
    pub addr: String,
}

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
    /// All peers in the cluster, including this node. One `peer` node each.
    #[serde(rename = "peer", deserialize_with = "one_or_many")]
    pub peers: Vec<PeerSpec>,
    /// Bootstrap a fresh cluster when `data_dir` is empty.
    /// Set to `false` for nodes that join an existing cluster.
    ///
    /// Accepts either a bare bool or a string parseable as `bool`
    /// (`"true"` / `"false"`). The string form lets env-var substitution work
    /// in templates.
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

/// Which runtime mode the service runs in.
///
/// The `mode` tag sits at the top of the document:
///
/// ```kdl
/// mode "standalone"
/// // or
/// mode "cluster"
/// // plus one `peer` node per cluster member
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

/// A windowing declaration on a processor node: which windows the host
/// assumes the processor's merging logic works in, and what it tracks for
/// them.
///
/// The geometry lives on the node itself, tagged by `kind`, because a KDL
/// property table is a flat map and the spec's own `kind` tag is one key of
/// it. Key fields are child nodes: a KDL property cannot repeat, and a child
/// with one argument is a scalar while one with several is an array, which is
/// exactly the one-or-many shape a key list needs.
///
/// ```kdl
/// wasm "windowed" module="pipelines/windowed.wasm" {
///     window kind="tumbling" size_ms=30000 offset_ms=0
///            time_field="timestamp_ms" allowed_lateness_ms=5000 {
///         key_field "category"
///     }
/// }
/// ```
///
/// The host uses the declaration to validate the node's inbound streams
/// (every delivered component must carry `time_field`), to track the
/// node's event-time watermark across batches, and to describe the node in
/// the dashboard. The processor itself implements the merging logic and
/// reads the same geometry back through `get-config` under the `window.*`
/// keys, which the builder injects into the node's config table; the two
/// sides of the contract cannot drift because there is only one source of
/// truth.
#[cfg(feature = "windows")]
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WindowConfig {
    /// The window geometry (tumbling, sliding or session).
    #[serde(flatten)]
    pub spec: pcs_core::windows::WindowSpec,
    /// The event-time column every component delivered to this processor
    /// must carry. Supports `Int64` milliseconds and the Arrow timestamp
    /// types.
    pub time_field: String,
    /// Grouping key columns; empty for a global window.
    #[serde(rename = "key_field")]
    pub key_fields: Vec<String>,
    /// How many milliseconds past the watermark a late row is still accepted
    /// by the processor's windowing logic. Defaults to 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub allowed_lateness_ms: i64,
}

#[cfg(feature = "windows")]
fn is_zero(value: &i64) -> bool {
    *value == 0
}

#[cfg(feature = "windows")]
impl WindowConfig {
    /// Whether the declaration is geometrically sane and names a non-empty
    /// time field with a non-negative lateness budget.
    pub fn validate(&self) -> Result<(), String> {
        self.spec.validate()?;
        if self.time_field.is_empty() {
            return Err("window time_field must not be empty".to_string());
        }
        if self.allowed_lateness_ms < 0 {
            return Err(format!(
                "window allowed_lateness_ms must be >= 0, got {}",
                self.allowed_lateness_ms
            ));
        }
        Ok(())
    }

    /// The declaration as `window.*` config keys, for injection into the
    /// processor node's `config` table so `get-config` answers them.
    pub fn config_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::with_capacity(6);
        let (kind, size, slide, offset, gap) = match &self.spec {
            pcs_core::windows::WindowSpec::Tumbling { size_ms, offset_ms } => {
                ("tumbling", Some(*size_ms), None, Some(*offset_ms), None)
            }
            pcs_core::windows::WindowSpec::Sliding {
                size_ms,
                slide_ms,
                offset_ms,
            } => (
                "sliding",
                Some(*size_ms),
                Some(*slide_ms),
                Some(*offset_ms),
                None,
            ),
            pcs_core::windows::WindowSpec::Session { gap_ms } => {
                ("session", None, None, None, Some(*gap_ms))
            }
        };
        pairs.push(("window.kind".to_string(), kind.to_string()));
        if let Some(size) = size {
            pairs.push(("window.size_ms".to_string(), size.to_string()));
        }
        if let Some(slide) = slide {
            pairs.push(("window.slide_ms".to_string(), slide.to_string()));
        }
        if let Some(offset) = offset {
            pairs.push(("window.offset_ms".to_string(), offset.to_string()));
        }
        if let Some(gap) = gap {
            pairs.push(("window.gap_ms".to_string(), gap.to_string()));
        }
        pairs.push(("window.time_field".to_string(), self.time_field.clone()));
        pairs.push(("window.key_fields".to_string(), self.key_fields.join(",")));
        pairs.push((
            "window.allowed_lateness_ms".to_string(),
            self.allowed_lateness_ms.to_string(),
        ));
        pairs
    }
}

#[cfg(feature = "windows")]
impl<'de> Deserialize<'de> for WindowConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error as _, MapAccess, Visitor};

        struct WindowConfigVisitor;

        impl<'de> Visitor<'de> for WindowConfigVisitor {
            type Value = WindowConfig;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a window node: kind, geometry, time_field, optional key_field(s) and \
                     allowed_lateness_ms",
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                const KNOWN: &[&str] = &[
                    "kind",
                    "size_ms",
                    "slide_ms",
                    "offset_ms",
                    "gap_ms",
                    "time_field",
                    "allowed_lateness_ms",
                    "key_field",
                ];
                let mut kind: Option<String> = None;
                let mut size_ms: Option<i64> = None;
                let mut slide_ms: Option<i64> = None;
                let mut offset_ms: Option<i64> = None;
                let mut gap_ms: Option<i64> = None;
                let mut time_field: Option<String> = None;
                let mut allowed_lateness_ms: Option<i64> = None;
                let mut key_fields: Vec<String> = Vec::new();

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "kind" => kind = Some(map.next_value()?),
                        "size_ms" => size_ms = Some(map.next_value()?),
                        "slide_ms" => slide_ms = Some(map.next_value()?),
                        "offset_ms" => offset_ms = Some(map.next_value()?),
                        "gap_ms" => gap_ms = Some(map.next_value()?),
                        "time_field" => time_field = Some(map.next_value()?),
                        "allowed_lateness_ms" => allowed_lateness_ms = Some(map.next_value()?),
                        "key_field" => {
                            #[derive(Deserialize)]
                            #[serde(untagged)]
                            enum OneOrMany {
                                One(String),
                                Many(Vec<String>),
                            }
                            key_fields = match map.next_value::<OneOrMany>()? {
                                OneOrMany::One(one) => vec![one],
                                OneOrMany::Many(many) => many,
                            };
                        }
                        other => return Err(A::Error::unknown_field(other, KNOWN)),
                    }
                }

                let kind = kind.ok_or_else(|| A::Error::missing_field("kind"))?;
                let spec = match kind.as_str() {
                    "tumbling" => {
                        if slide_ms.is_some() {
                            return Err(A::Error::custom(
                                "slide_ms is only valid on a sliding window",
                            ));
                        }
                        if gap_ms.is_some() {
                            return Err(A::Error::custom(
                                "gap_ms is only valid on a session window",
                            ));
                        }
                        pcs_core::windows::WindowSpec::Tumbling {
                            size_ms: size_ms.ok_or_else(|| A::Error::missing_field("size_ms"))?,
                            offset_ms: offset_ms.unwrap_or(0),
                        }
                    }
                    "sliding" => {
                        if gap_ms.is_some() {
                            return Err(A::Error::custom(
                                "gap_ms is only valid on a session window",
                            ));
                        }
                        pcs_core::windows::WindowSpec::Sliding {
                            size_ms: size_ms.ok_or_else(|| A::Error::missing_field("size_ms"))?,
                            slide_ms: slide_ms
                                .ok_or_else(|| A::Error::missing_field("slide_ms"))?,
                            offset_ms: offset_ms.unwrap_or(0),
                        }
                    }
                    "session" => {
                        if size_ms.is_some() {
                            return Err(A::Error::custom(
                                "size_ms is only valid on a tumbling or sliding window",
                            ));
                        }
                        if slide_ms.is_some() {
                            return Err(A::Error::custom(
                                "slide_ms is only valid on a sliding window",
                            ));
                        }
                        if offset_ms.is_some() {
                            return Err(A::Error::custom(
                                "offset_ms is only valid on a tumbling or sliding window",
                            ));
                        }
                        pcs_core::windows::WindowSpec::Session {
                            gap_ms: gap_ms.ok_or_else(|| A::Error::missing_field("gap_ms"))?,
                        }
                    }
                    other => {
                        return Err(A::Error::unknown_variant(
                            other,
                            &["tumbling", "sliding", "session"],
                        ));
                    }
                };

                Ok(WindowConfig {
                    spec,
                    time_field: time_field.ok_or_else(|| A::Error::missing_field("time_field"))?,
                    key_fields,
                    allowed_lateness_ms: allowed_lateness_ms.unwrap_or(0),
                })
            }
        }

        deserializer.deserialize_map(WindowConfigVisitor)
    }
}

/// WASM processor node (requires the `wasm` feature at runtime).
///
/// ```kdl
/// workflow "w" {
///     wasm "transform" module="pipelines/transform.wasm" sha3_256="abc123..." {
///         config batch_size="1000"
///     }
/// }
/// ```
///
/// `module` is optional: a node declared with no `module` key is a processor
/// whose runtime is supplied programmatically through
/// [`ServiceBuilder::with_runtime`](crate::service::builder::ServiceBuilder::with_runtime),
/// looked up by this node's `id`. Building a workflow where such a node has
/// neither a `module` nor a matching registered runtime is a build-time
/// error.
///
/// Unknown keys are rejected: a key the service cannot honour is a
/// configuration error, not something to ignore silently.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct WasmSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Path to the `.wasm` component file (relative or absolute). Absent for
    /// a node whose runtime is supplied through `with_runtime`.
    #[serde(default)]
    pub module: Option<String>,
    /// Optional expected SHA3-256 hex digest of the module bytes. Validation
    /// fails at load time if the digest does not match. The value may carry
    /// an optional `sha3-256:` prefix.
    #[serde(default)]
    pub sha3_256: Option<String>,
    /// Opaque key-value config the processor reads through the
    /// `pcs:pipeline/host-io` `get-config` import.
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Windowing declaration: how the host merges this node's inbound
    /// streams and tracks its event-time watermark. The geometry is injected
    /// into `config` as `window.*` keys, so the processor's merging logic
    /// reads one source of truth.
    #[cfg(feature = "windows")]
    #[serde(default)]
    pub window: Option<WindowConfig>,
}

/// Native plugin processor node (requires the `plugin` feature at runtime).
///
/// ```kdl
/// workflow "w" {
///     plugin "audit" library="pipelines/libtransform.so" sha3_256="abc123..." {
///         config batch_size="1000"
///     }
/// }
/// ```
///
/// The key is `library`, not `module`, because the artifact is a shared
/// library the host loads with `dlopen`. It is optional for the same reason
/// [`WasmSpec::module`] is: a node with no `library` relies on a runtime
/// registered under this node's `id` through
/// [`ServiceBuilder::with_runtime`](crate::service::builder::ServiceBuilder::with_runtime).
///
/// Unknown keys are rejected: a key the service cannot honour is a
/// configuration error, not something to ignore silently.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct PluginSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Path to the shared library file (relative or absolute). Absent for a
    /// node whose runtime is supplied through `with_runtime`.
    #[serde(default)]
    pub library: Option<String>,
    /// Optional expected SHA3-256 hex digest of the library file bytes.
    /// Validation fails at load time if the digest does not match. The value
    /// may carry an optional `sha3-256:` prefix.
    #[serde(default)]
    pub sha3_256: Option<String>,
    /// Opaque key-value config the plugin reads through the `get_config`
    /// callback on the host vtable.
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Windowing declaration: how the host merges this node's inbound
    /// streams and tracks its event-time watermark. The geometry is injected
    /// into `config` as `window.*` keys, so the plugin's merging logic reads
    /// one source of truth.
    #[cfg(feature = "windows")]
    #[serde(default)]
    pub window: Option<WindowConfig>,
}

/// A declared byte-format instance: a name a source or sink's `transformer`
/// key can reference.
///
/// ```kdl
/// workflow "w" {
///     transformer "orders-json" name="Orders NDJSON" format="ndjson" {
///         options infer_max=1000
///     }
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TransformerSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Registered format name, resolved against the `TransformerRegistry`.
    pub format: String,
    /// Handed to that format's factory. Empty table when absent.
    #[serde(default = "default_config")]
    pub options: ConfigValue,
}

/// Declares an IO source that feeds rows into a component column.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Factory lookup key.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Id of the `transformer` node that decodes this source's bytes. Absent
    /// for a connector that produces `RecordBatch`es directly.
    #[serde(default)]
    pub transformer: Option<String>,
    /// Name of the component this source writes into. Checked against the
    /// runtime's `declared_components()` at load time.
    pub component: String,
    /// Opaque per-source configuration.
    #[serde(default = "default_config")]
    pub config: ConfigValue,
}

/// Declares an IO sink that drains rows from a component column.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SinkSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Factory lookup key.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Id of the `transformer` node that encodes this sink's bytes. Absent
    /// for a connector that consumes `RecordBatch`es directly.
    #[serde(default)]
    pub transformer: Option<String>,
    /// Name of the component this sink reads from. Checked against the
    /// runtime's `declared_components()` at load time.
    pub component: String,
    /// Opaque per-sink configuration.
    #[serde(default = "default_config")]
    pub config: ConfigValue,
}

/// An explicit directed edge between two declared node ids.
///
/// `from` and `to` name a `source`, `wasm`, `plugin` or `sink` id — never a
/// `transformer` id, which is not a graph node.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinkSpec {
    /// The upstream node id.
    pub from: String,
    /// The downstream node id.
    pub to: String,
    /// Branch name this link carries. A processor's `run-result.routes`
    /// selects the links its output is delivered to by this name. Absent =
    /// unlabelled: never selected by a routing decision, delivered only under
    /// legacy multicast.
    #[serde(default)]
    pub branch: Option<String>,
}

/// Which of the three graph roles a declared node plays.
///
/// Distinct from a node's KDL kind (`source`/`wasm`/`plugin`/`sink`): both
/// `wasm` and `plugin` nodes are [`NodeKind::Processor`], since a link treats
/// them identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    Source,
    Processor,
    Sink,
}

/// One `workflow`: the whole DAG of sources, processors, sinks and
/// transformers, wired together by explicit [`LinkSpec`] declarations.
///
/// Every declared transformer, source, `wasm`, `plugin` and sink id shares one
/// namespace and must be unique; see [`WorkflowSpec::validate`] for the full
/// set of load-time graph rules.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSpec {
    /// Mandatory id, from the node's leading argument.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Declared byte formats, one `transformer` node each.
    #[serde(rename = "transformer", default, deserialize_with = "one_or_many")]
    pub transformers: Vec<TransformerSpec>,
    /// Data sources feeding the workflow. One `source` node each.
    #[serde(rename = "source", default, deserialize_with = "one_or_many")]
    pub sources: Vec<SourceSpec>,
    /// WASM processor nodes, one `wasm` node each.
    #[cfg(feature = "wasm")]
    #[serde(default, deserialize_with = "one_or_many")]
    pub wasm: Vec<WasmSpec>,
    /// Native plugin processor nodes, one `plugin` node each.
    #[cfg(feature = "plugin")]
    #[serde(default, deserialize_with = "one_or_many")]
    pub plugin: Vec<PluginSpec>,
    /// Data sinks draining the workflow. One `sink` node each.
    #[serde(rename = "sink", default, deserialize_with = "one_or_many")]
    pub sinks: Vec<SinkSpec>,
    /// Explicit edges between declared nodes. One `link` node each.
    #[serde(rename = "link", default, deserialize_with = "one_or_many")]
    pub links: Vec<LinkSpec>,
}

/// `^[A-Za-z0-9][A-Za-z0-9_-]*$`, at most 64 bytes.
///
/// Ids are used verbatim as OpenTelemetry attribute values and as topology
/// node ids, so the charset is closed rather than escaped.
fn is_valid_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 64 {
        return false;
    }
    let mut chars = id.chars();
    let first_is_alphanumeric = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
    first_is_alphanumeric
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl WorkflowSpec {
    /// Every declared node's id and kind, in declaration order: sources, then
    /// wasm processors, then plugin processors, then sinks.
    pub(crate) fn nodes(&self) -> Vec<(&str, NodeKind)> {
        let mut out = Vec::new();
        for s in &self.sources {
            out.push((s.id.as_str(), NodeKind::Source));
        }
        #[cfg(feature = "wasm")]
        for w in &self.wasm {
            out.push((w.id.as_str(), NodeKind::Processor));
        }
        #[cfg(feature = "plugin")]
        for p in &self.plugin {
            out.push((p.id.as_str(), NodeKind::Processor));
        }
        for s in &self.sinks {
            out.push((s.id.as_str(), NodeKind::Sink));
        }
        out
    }

    /// Every declared id outside `nodes()` too (transformers), each paired
    /// with the KDL node kind that declared it, for id-validation messages.
    fn declared_ids(&self) -> Vec<(&str, &'static str)> {
        let mut out = Vec::new();
        for t in &self.transformers {
            out.push((t.id.as_str(), "transformer"));
        }
        for (id, kind) in self.nodes() {
            let label = match kind {
                NodeKind::Source => "source",
                NodeKind::Processor => "processor",
                NodeKind::Sink => "sink",
            };
            out.push((id, label));
        }
        out
    }

    /// Node indices in topological order, so a node always follows every node
    /// that links into it.
    ///
    /// # Errors
    ///
    /// `PcsError::Configuration` when the links contain a cycle.
    pub(crate) fn topological_order(&self) -> PcsResult<Vec<usize>> {
        let nodes = self.nodes();
        let n = nodes.len();
        let id_to_idx: HashMap<&str, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, (id, _))| (*id, i))
            .collect();

        // adjacency[i] = list of node indices that depend on i (i.e. i → dependent).
        let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut in_degree: Vec<usize> = vec![0; n];
        for link in &self.links {
            let (Some(&from), Some(&to)) = (
                id_to_idx.get(link.from.as_str()),
                id_to_idx.get(link.to.as_str()),
            ) else {
                // Unknown ids are rejected by `validate`'s rule 5; this
                // function is also called from within `validate` itself
                // (rule 8), before rule 5 in a hand-built (non-KDL) spec, so
                // skip rather than panic.
                continue;
            };
            adjacency[from].push(to);
            in_degree[to] += 1;
        }

        // Kahn's algorithm, draining the whole frontier each round so the
        // result is flattened frontier-by-frontier, keeping declaration order
        // inside each depth.
        let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
        let mut order: Vec<usize> = Vec::with_capacity(n);
        let mut visited = 0usize;

        while !queue.is_empty() {
            let frontier: Vec<usize> = queue.drain(..).collect();
            visited += frontier.len();

            let mut next_queue: VecDeque<usize> = VecDeque::new();
            for &node in &frontier {
                order.push(node);
                for &dep in &adjacency[node] {
                    in_degree[dep] -= 1;
                    if in_degree[dep] == 0 {
                        next_queue.push_back(dep);
                    }
                }
            }
            queue = next_queue;
        }

        if visited != n {
            return Err(PcsError::configuration(format!(
                "workflow '{}': links contain a cycle",
                self.id
            )));
        }

        Ok(order)
    }

    /// Validate every load-time graph rule.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] describing the first violation
    /// found, in the order documented on the type.
    pub(crate) fn validate(&self, mode: &ServiceMode) -> PcsResult<()> {
        let wf = &self.id;

        // 1. The workflow declares at least one node.
        let nodes = self.nodes();
        if nodes.is_empty() {
            return Err(PcsError::configuration(format!(
                "workflow '{wf}': declares no source, processor or sink node"
            )));
        }

        // 2. Every id matches the closed charset and length bound.
        if !is_valid_id(wf) {
            return Err(PcsError::configuration(format!(
                "workflow: id '{wf}' is invalid; ids must match \
                 ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes"
            )));
        }
        let declared = self.declared_ids();
        for (id, kind) in &declared {
            if !is_valid_id(id) {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': {kind} id '{id}' is invalid; ids must match \
                     ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes"
                )));
            }
        }

        // 3. No id is declared twice anywhere in the workflow.
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for (id, kind) in &declared {
            if let Some(&prev_kind) = seen.get(id) {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': id '{id}' is declared twice, as {prev_kind} and as {kind}"
                )));
            }
            seen.insert(id, kind);
        }

        // 4. Every source.transformer / sink.transformer names a declared
        //    transformer id.
        let transformer_ids: HashSet<&str> =
            self.transformers.iter().map(|t| t.id.as_str()).collect();
        for s in &self.sources {
            if let Some(t) = &s.transformer
                && !transformer_ids.contains(t.as_str())
            {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': source '{}' names transformer '{t}', \
                     which is not declared",
                    s.id
                )));
            }
        }
        for s in &self.sinks {
            if let Some(t) = &s.transformer
                && !transformer_ids.contains(t.as_str())
            {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': sink '{}' names transformer '{t}', which is not declared",
                    s.id
                )));
            }
        }

        // 5. Every link.from / link.to names a declared source, wasm, plugin
        //    or sink id, and from != to. A transformer id is not a node, so
        //    it is rejected here too.
        let node_kind: HashMap<&str, NodeKind> =
            nodes.iter().map(|&(id, kind)| (id, kind)).collect();
        for link in &self.links {
            if link.from == link.to {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' links a node to itself",
                    link.from, link.to
                )));
            }
            if !node_kind.contains_key(link.from.as_str()) {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': link names undeclared node '{}'",
                    link.from
                )));
            }
            if !node_kind.contains_key(link.to.as_str()) {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': link names undeclared node '{}'",
                    link.to
                )));
            }
        }

        // 6. No (from, to) pair is declared twice.
        let mut seen_edges: HashSet<(&str, &str)> = HashSet::new();
        for link in &self.links {
            if !seen_edges.insert((link.from.as_str(), link.to.as_str())) {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' is declared twice",
                    link.from, link.to
                )));
            }
        }

        // 7. The edge-kind matrix: a source has no input, a sink has no
        //    output.
        for link in &self.links {
            if node_kind[link.to.as_str()] == NodeKind::Source {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' targets source '{}'; \
                     a source has no input",
                    link.from, link.to, link.to
                )));
            }
            if node_kind[link.from.as_str()] == NodeKind::Sink {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' starts at sink '{}'; \
                     a sink has no output",
                    link.from, link.to, link.from
                )));
            }
        }

        // 8. The graph is acyclic.
        self.topological_order()?;

        // 9. Every source has at least one outbound link, and every sink at
        //    least one inbound link. A processor needs neither.
        for s in &self.sources {
            if !self.links.iter().any(|l| l.from == s.id) {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': source '{}' has no outbound link",
                    s.id
                )));
            }
        }
        for s in &self.sinks {
            if !self.links.iter().any(|l| l.to == s.id) {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': sink '{}' has no inbound link",
                    s.id
                )));
            }
        }

        // 10. Cluster mode: exactly one processor node, zero sources, zero
        //     sinks and zero links.
        if matches!(mode, ServiceMode::Cluster { .. }) {
            let processor_count = nodes
                .iter()
                .filter(|&&(_, kind)| kind == NodeKind::Processor)
                .count();
            if processor_count != 1
                || !self.sources.is_empty()
                || !self.sinks.is_empty()
                || !self.links.is_empty()
            {
                return Err(PcsError::configuration(format!(
                    "cluster mode runs exactly one 'wasm' or 'plugin' node with no source, \
                     sink or link ({} node(s), {} link(s) declared)",
                    nodes.len(),
                    self.links.len()
                )));
            }
            // The service-level window block tracks a watermark from the
            // node's inbound links, and cluster mode has none: the distributed
            // runner drives the one processor straight from partition claims.
            // Reject the block rather than silently not tracking anything.
            #[cfg(feature = "windows")]
            {
                #[cfg(feature = "wasm")]
                if let Some(w) = self.wasm.iter().find(|w| w.window.is_some()) {
                    return Err(PcsError::configuration(format!(
                        "workflow '{wf}': cluster mode cannot honour wasm node '{}' window \
                         block; cluster-mode windowing lives in the processor's own pipeline \
                         (WindowedSystem + WindowAccumulator)",
                        w.id
                    )));
                }
                #[cfg(feature = "plugin")]
                if let Some(p) = self.plugin.iter().find(|p| p.window.is_some()) {
                    return Err(PcsError::configuration(format!(
                        "workflow '{wf}': cluster mode cannot honour plugin node '{}' window \
                         block; cluster-mode windowing lives in the processor's own pipeline \
                         (WindowedSystem + WindowAccumulator)",
                        p.id
                    )));
                }
            }
        }

        // 11. `run_mode kind="stream"`: at least one source in the whole
        //     workflow.
        let stream_mode = matches!(
            mode,
            ServiceMode::Standalone { config } if config.run_mode == RunMode::Stream
        );
        if stream_mode && self.sources.is_empty() {
            return Err(PcsError::configuration(format!(
                "stream run mode requires at least one 'source' node ({}) declared",
                self.sources.len()
            )));
        }

        // 13. Outside stream mode, no source is live.
        if !stream_mode && let Some(live) = self.sources.iter().find(|s| is_live_source(s)) {
            return Err(PcsError::configuration(format!(
                "source type '{}' never reaches EOF; it requires standalone mode \
                 with run_mode kind=\"stream\"",
                live.type_name
            )));
        }

        // 14. Every present link branch is a valid id.
        for link in &self.links {
            if let Some(branch) = &link.branch
                && !is_valid_id(branch)
            {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' branch '{branch}' is invalid; \
                     branches must match ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes",
                    link.from, link.to
                )));
            }
        }

        // 15. A labelled link must start at a processor.
        for link in &self.links {
            if let Some(branch) = &link.branch
                && node_kind[link.from.as_str()] != NodeKind::Processor
            {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' carries branch '{branch}' but '{}' is \
                     a source; only a processor can route",
                    link.from, link.to, link.from
                )));
            }
        }

        // 16. Per node, either every outbound link carries a branch or none
        //     do.
        for &(id, _) in &nodes {
            let mut labelled = false;
            let mut unlabelled = false;
            for link in &self.links {
                if link.from != id {
                    continue;
                }
                if link.branch.is_some() {
                    labelled = true;
                } else {
                    unlabelled = true;
                }
            }
            if labelled && unlabelled {
                return Err(PcsError::configuration(format!(
                    "workflow '{wf}': node '{id}' mixes labelled and unlabelled outbound \
                     links; label every link or none"
                )));
            }
        }

        // 17. A declared `window` block must be geometrically sane.
        #[cfg(feature = "windows")]
        {
            #[cfg(feature = "wasm")]
            for w in &self.wasm {
                validate_window_block(wf, "wasm", &w.id, w.window.as_ref())?;
            }
            #[cfg(feature = "plugin")]
            for p in &self.plugin {
                validate_window_block(wf, "plugin", &p.id, p.window.as_ref())?;
            }
        }

        Ok(())
    }
}

/// Run a declared [`WindowConfig`]'s sanity checks, naming the workflow, node
/// kind and id in the error.
#[cfg(feature = "windows")]
fn validate_window_block(
    workflow_id: &str,
    kind: &str,
    id: &str,
    window: Option<&WindowConfig>,
) -> PcsResult<()> {
    let Some(window) = window else {
        return Ok(());
    };
    window.validate().map_err(|msg| {
        PcsError::configuration(format!(
            "workflow '{workflow_id}': {kind} node '{id}' window is invalid: {msg}"
        ))
    })
}

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

fn default_trace_sample_ratio() -> f64 {
    1.0
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
    /// OTLP/HTTP collector base URL, for example
    /// `http://127.0.0.1:4318`. `None` disables span export.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Parent-based trace-id-ratio sampling ratio, 0.0 to 1.0.
    #[serde(default = "default_trace_sample_ratio")]
    pub trace_sample_ratio: f64,
    /// In-process telemetry capture, its JSON API and the `/ui` dashboard.
    #[serde(default)]
    pub inspector: crate::inspector::InspectorConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Pretty,
            log_level: default_log_level(),
            otlp_endpoint: None,
            trace_sample_ratio: default_trace_sample_ratio(),
            inspector: crate::inspector::InspectorConfig::default(),
        }
    }
}

/// Top-level service configuration.
///
/// Load from a KDL file with [`ServiceConfig::load`]:
///
/// ```no_run
/// # #[cfg(feature = "service")]
/// # {
/// use pcs_service::service::config::ServiceConfig;
/// let cfg = ServiceConfig::load("pcs.kdl").unwrap();
/// # }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceConfig {
    /// Node identity and storage.
    pub node: NodeConfig,
    /// Runtime mode (standalone or cluster), flattened into the document.
    #[serde(flatten)]
    pub mode: ServiceMode,
    /// The workflows this process runs. One or more `workflow` blocks; a
    /// single block deserializes as a one-element list.
    #[serde(rename = "workflow", deserialize_with = "one_or_many")]
    pub workflows: Vec<WorkflowSpec>,
    /// HTTP control-plane options.
    #[serde(default)]
    pub http: HttpConfig,
    /// Logging / observability options.
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

impl ServiceConfig {
    /// Load a [`ServiceConfig`] from a KDL file at `path`.
    ///
    /// The file is read, env-var placeholders are substituted, the KDL is
    /// parsed, and semantic validation is applied before returning.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] for IO failures, parse errors, or
    /// any validation constraint violation.
    pub fn load(path: impl AsRef<std::path::Path>) -> PcsResult<Self> {
        let raw = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            PcsError::configuration(format!(
                "reading config file {}: {e}",
                path.as_ref().display()
            ))
        })?;
        let substituted = pcs_config::substitute_env_vars(&raw)?;
        let value = pcs_config::from_kdl_str(&substituted)?;
        let config = ServiceConfig::deserialize(value)
            .map_err(|e| PcsError::configuration(format!("parsing config: {e}")))?;
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
        if self.node.data_dir.as_os_str().is_empty() {
            return Err(PcsError::configuration("node.data_dir must not be empty"));
        }

        if let ServiceMode::Cluster { config } = &self.mode {
            if config.peers.is_empty() {
                return Err(PcsError::configuration(
                    "cluster mode requires at least one peer",
                ));
            }

            let mut seen_ids: HashSet<u64> = HashSet::new();
            for peer in &config.peers {
                if !seen_ids.insert(peer.id) {
                    return Err(PcsError::configuration(format!(
                        "cluster peers contain duplicate id: {}",
                        peer.id
                    )));
                }
            }

            let node_id = self.node.id;
            if !config.peers.iter().any(|p| p.id == node_id) {
                return Err(PcsError::configuration(format!(
                    "node id {node_id} is not listed in cluster.peers"
                )));
            }

            let min_lease = config.election_timeout_ms.saturating_mul(3);
            if config.lease_ttl_ms < min_lease {
                return Err(PcsError::configuration(format!(
                    "lease_ttl_ms ({}) must be >= 3 × election_timeout_ms ({}) = {}",
                    config.lease_ttl_ms, config.election_timeout_ms, min_lease,
                )));
            }
        }

        for wf in &self.workflows {
            wf.validate(&self.mode)?;
        }

        // Cross-workflow constraints, enforced after every workflow passes
        // its own graph rules.
        //
        // Cluster mode runs one distributed group per process: exactly one
        // workflow may be declared.
        if matches!(self.mode, ServiceMode::Cluster { .. }) && self.workflows.len() != 1 {
            return Err(PcsError::configuration(format!(
                "cluster mode requires exactly one workflow; found {}",
                self.workflows.len()
            )));
        }

        // Workflow ids are process-unique so topology and logs can name a
        // workflow without ambiguity.
        let mut seen_workflows: HashSet<&str> = HashSet::new();
        for wf in &self.workflows {
            if !seen_workflows.insert(wf.id.as_str()) {
                return Err(PcsError::configuration(format!(
                    "workflow id '{}' is declared twice",
                    wf.id
                )));
            }
        }

        // Node ids are process-unique across workflows: the OTel attribution
        // keys (`source=`/`processor=`/`sink=`) are bare node ids, so an
        // overlap would double-count metrics.
        let mut node_owners: HashMap<&str, (&str, &str)> = HashMap::new();
        for wf in &self.workflows {
            for (id, kind) in wf.declared_ids() {
                if let Some(&(prev_kind, prev_wf)) = node_owners.get(id) {
                    return Err(PcsError::configuration(format!(
                        "node id '{id}' is declared in workflow '{prev_wf}' as {prev_kind} \
                         and in workflow '{}' as {kind}; node ids must be unique across \
                         all workflows",
                        wf.id
                    )));
                }
                node_owners.insert(id, (kind, wf.id.as_str()));
            }
        }

        // Channel names pair exactly one ChannelSink with one ChannelSource.
        // A dangling half hangs: a source whose registry-held sender never
        // drops, or a sink whose registry-held receiver is never drained.
        let mut channels: HashMap<&str, (Option<&str>, Option<&str>)> = HashMap::new();
        for wf in &self.workflows {
            for s in &wf.sources {
                if s.type_name == "ChannelSource"
                    && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
                {
                    let entry = channels.entry(name).or_default();
                    if entry.0.is_some() {
                        return Err(PcsError::configuration(format!(
                            "channel '{name}': more than one ChannelSource declared"
                        )));
                    }
                    entry.0 = Some(s.id.as_str());
                }
            }
            for s in &wf.sinks {
                if s.type_name == "ChannelSink"
                    && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
                {
                    let entry = channels.entry(name).or_default();
                    if entry.1.is_some() {
                        return Err(PcsError::configuration(format!(
                            "channel '{name}': more than one ChannelSink declared"
                        )));
                    }
                    entry.1 = Some(s.id.as_str());
                }
            }
        }
        for (name, (source, sink)) in &channels {
            match (source, sink) {
                (Some(_), None) => {
                    return Err(PcsError::configuration(format!(
                        "channel '{name}': declares a ChannelSource but no ChannelSink"
                    )));
                }
                (None, Some(_)) => {
                    return Err(PcsError::configuration(format!(
                        "channel '{name}': declares a ChannelSink but no ChannelSource"
                    )));
                }
                (Some(_), Some(_)) => {}
                (None, None) => unreachable!("an entry exists only when a half was seen"),
            }
        }

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

/// A source that never reports EOF, so only the stream runner may drive it.
fn is_live_source(spec: &SourceSpec) -> bool {
    match spec.type_name.as_str() {
        "tcp" => true,
        "KafkaSource" | "NatsSource" => !spec
            .config
            .get("stop_at_end")
            .and_then(ConfigValue::as_bool)
            .unwrap_or(false),
        _ => false,
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Parse a fixture the way [`ServiceConfig::load`] does, minus the file
    /// read. The error is flattened to a `String` so a test can assert on the
    /// text without naming the value tree's error type.
    fn parse_as<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, String> {
        let value = pcs_config::from_kdl_str(raw).map_err(|e| e.to_string())?;
        T::deserialize(value).map_err(|e| e.to_string())
    }

    fn parse(raw: &str) -> Result<ServiceConfig, String> {
        parse_as(raw)
    }

    /// A trivial, feature-independent one-link workflow: a source straight to
    /// a sink. Valid under every WorkflowSpec::validate rule regardless of
    /// which of `wasm`/`plugin` are compiled in.
    const TRIVIAL_WORKFLOW: &str = r#"
workflow "w" {
    source "in" type="NoopSource" component="X"
    sink "out" type="NoopSink" component="X"
    link from="in" to="out"
}
"#;

    fn minimal_standalone_kdl() -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs-test"
{TRIVIAL_WORKFLOW}
"#
        )
    }

    fn minimal_cluster_kdl() -> String {
        format!(
            r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/pcs-cluster"

peer id=1 addr="127.0.0.1:9000"
peer id=2 addr="127.0.0.2:9000"
{TRIVIAL_WORKFLOW}
"#
        )
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
            workflows: vec![WorkflowSpec {
                id: "payments".to_string(),
                name: None,
                transformers: Vec::new(),
                sources: vec![SourceSpec {
                    id: "kafka_in".to_string(),
                    name: None,
                    type_name: "MongoSource".to_string(),
                    transformer: None,
                    component: "orders".to_string(),
                    config: default_config(),
                }],
                #[cfg(feature = "wasm")]
                wasm: Vec::new(),
                #[cfg(feature = "plugin")]
                plugin: Vec::new(),
                sinks: vec![SinkSpec {
                    id: "pg_out".to_string(),
                    name: None,
                    type_name: "PostgresSink".to_string(),
                    transformer: None,
                    component: "orders".to_string(),
                    config: default_config(),
                }],
                links: Vec::new(),
            }],
            http: HttpConfig {
                bind: "0.0.0.0:8080".to_string(),
                disabled: false,
            },
            observability: ObservabilityConfig {
                log_format: LogFormat::Json,
                log_level: "debug".to_string(),
                otlp_endpoint: None,
                trace_sample_ratio: 1.0,
                inspector: crate::inspector::InspectorConfig::default(),
            },
        }
    }

    // The canonical config literal must deserialise into the same shape
    // `full_config()` builds by hand. Serialization is not exercised: nothing
    // writes a config file, and `#[serde(flatten)]` plus internally tagged
    // enums make the `Serialize` direction meaningless here. This config is
    // never `.validate()`d: the source and sink are deliberately unlinked, to
    // pin down the parse shape without also asserting the graph rules.
    #[test]
    fn test_full_standalone_config_deserialises() {
        let raw = r#"
mode "standalone"

node id=1 name="node-1" data_dir="/tmp/pcs"

run_mode kind="interval" interval_ms=5000

workflow "payments" {
    source "kafka_in" type="MongoSource" component="orders"

    sink "pg_out" type="PostgresSink" component="orders"
}

http bind="0.0.0.0:8080" disabled=#false

observability log_format="json" log_level="debug"
"#;
        let restored = parse(raw).expect("deserialize");
        let original = full_config();

        assert_eq!(restored.node.id, original.node.id);
        assert_eq!(restored.node.name, original.node.name);
        assert_eq!(restored.node.data_dir, original.node.data_dir);
        assert_eq!(restored.workflows[0].id, "payments");
        #[cfg(feature = "wasm")]
        assert!(
            restored.workflows[0].wasm.is_empty(),
            "no wasm node means no wasm processors"
        );
        assert_eq!(restored.workflows[0].sources.len(), 1);
        assert_eq!(restored.workflows[0].sources[0].component, "orders");
        assert_eq!(restored.workflows[0].sinks.len(), 1);
        assert_eq!(restored.workflows[0].sinks[0].component, "orders");
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

    #[test]
    fn test_minimal_standalone_parses_and_validates() {
        let cfg = parse(&minimal_standalone_kdl()).expect("parse");

        assert_eq!(cfg.node.id, 1);
        assert!(cfg.node.name.is_none());
        assert_eq!(cfg.node.data_dir, PathBuf::from("/tmp/pcs-test"));

        assert!(matches!(cfg.mode, ServiceMode::Standalone { .. }));

        assert_eq!(cfg.workflows[0].id, "w");
        assert_eq!(cfg.workflows[0].sources.len(), 1);
        assert_eq!(cfg.workflows[0].sinks.len(), 1);
        assert_eq!(cfg.http.bind, "0.0.0.0:8080");
        assert!(!cfg.http.disabled);
        assert_eq!(cfg.observability.log_level, "info");
        assert_eq!(cfg.observability.log_format, LogFormat::Pretty);
        cfg.validate().expect("trivial source-to-sink workflow");
    }

    #[test]
    fn test_missing_workflow_node_is_a_parse_error() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"
"#;
        let err = parse(raw).expect_err("a config with no workflow node must not parse");
        assert!(
            err.contains("workflow"),
            "error should name the missing field: {err}"
        );
    }

    #[test]
    fn test_minimal_cluster_parses() {
        let cfg = parse(&minimal_cluster_kdl()).expect("parse");

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

    /// One `peer` node is a single table, not a list, so `ClusterConfig.peers`
    /// carries `one_or_many`.
    #[test]
    fn test_single_peer_node_parses_as_a_one_element_list() {
        let raw = format!(
            r#"
mode "cluster"

node id=1 data_dir="/tmp/pcs"

peer id=1 addr="127.0.0.1:9000"
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        match &cfg.mode {
            ServiceMode::Cluster { config } => {
                assert_eq!(config.peers.len(), 1);
                assert_eq!(config.peers[0].addr, "127.0.0.1:9000");
            }
            _ => panic!("expected cluster mode"),
        }
    }

    #[test]
    fn test_missing_node_id_produces_error() {
        let raw = r#"
mode "standalone"

node data_dir="/tmp/pcs"
"#;
        let err = parse(raw).expect_err("expected parse error for missing node.id");
        assert!(
            err.contains("id") || err.contains("missing field"),
            "error should mention missing field: {err}"
        );
    }

    #[test]
    fn test_invalid_mode_produces_error() {
        let raw = r#"
mode "turbo_mode"

node id=1 data_dir="/tmp/pcs"
"#;
        assert!(parse(raw).is_err(), "expected parse error for unknown mode");
    }

    #[test]
    fn test_cluster_node_not_in_peers_rejected() {
        let raw = format!(
            r#"
mode "cluster"

node id=99 data_dir="/tmp/pcs"

peer id=1 addr="127.0.0.1:9000"
peer id=2 addr="127.0.0.2:9000"
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("99"),
            "error should mention node id 99: {err}"
        );
    }

    #[test]
    fn test_cluster_insufficient_lease_ttl_rejected() {
        let raw = format!(
            r#"
mode "cluster"
lease_ttl_ms 1000
election_timeout_ms 1000

node id=1 data_dir="/tmp/pcs"

peer id=1 addr="127.0.0.1:9000"
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("lease_ttl_ms"),
            "error should mention lease_ttl_ms: {err}"
        );
    }

    #[test]
    fn test_load_from_disk() {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(minimal_standalone_kdl().as_bytes())
            .expect("write");
        let path = file.path().to_path_buf();

        let cfg = ServiceConfig::load(&path).expect("load");
        assert_eq!(cfg.node.id, 1);
        assert!(matches!(cfg.mode, ServiceMode::Standalone { .. }));
    }

    #[test]
    fn test_load_rejects_a_malformed_document_naming_the_position() {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(b"mode \"standalone\nnode id=1\n")
            .expect("write");

        let err = ServiceConfig::load(file.path()).expect_err("unterminated string");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().starts_with("parsing KDL: 1:"),
            "error should name the position: {err}"
        );
    }

    #[test]
    fn test_run_mode_interval_round_trip() {
        let raw = r#"
kind "interval"
interval_ms 3000
"#;
        let restored: RunMode = parse_as(raw).expect("deserialize");
        assert_eq!(restored, RunMode::Interval { interval_ms: 3_000 });
    }

    #[test]
    fn test_http_disabled_skips_bind_validation() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

http bind="not-a-socket-addr" disabled=#true
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        cfg.validate()
            .expect("disabled http should not validate bind");
    }

    #[test]
    fn test_cluster_mode_with_a_source_rejected_at_validate() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/pcs"

peer id=1 addr="127.0.0.1:9000"
peer id=2 addr="127.0.0.2:9000"

workflow "w" {
    source "kafka_in" type="MongoSource" component="orders"
    sink "out" type="NoopSink" component="orders"
    link from="kafka_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert_eq!(
            err.category(),
            "configuration",
            "expected configuration error: {err}"
        );
        assert!(
            err.to_string()
                .contains("cluster mode runs exactly one 'wasm' or 'plugin' node"),
            "error should name the cluster rule: {err}"
        );
    }

    #[test]
    fn test_run_mode_stream_parses() {
        let restored: RunMode = parse_as("kind \"stream\"\n").expect("deserialize");
        assert_eq!(restored, RunMode::Stream);
    }

    fn stream_kdl(sources: &str, sinks: &str, links: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

run_mode kind="stream"

workflow "w" {{
{sources}
{sinks}
{links}
}}
"#
        )
    }

    #[test]
    fn test_stream_mode_with_one_source_validates() {
        let raw = stream_kdl(
            r#"source "ticks" type="tcp" component="Tick""#,
            r#"sink "out" type="NoopSink" component="Tick""#,
            r#"link from="ticks" to="out""#,
        );
        let cfg = parse(&raw).expect("parse");
        match &cfg.mode {
            ServiceMode::Standalone { config } => assert_eq!(config.run_mode, RunMode::Stream),
            _ => panic!("expected standalone"),
        }
        cfg.validate().expect("one source + stream mode is valid");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_stream_mode_requires_at_least_one_source() {
        // A processor-only workflow passes every earlier rule, so rule 11 is
        // what fires: stream mode needs at least one source to pull from.
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

run_mode kind="stream"

workflow "w" {
    wasm "p" module="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string()
                .contains("stream run mode requires at least one 'source' node"),
            "got: {err}"
        );
    }

    #[test]
    fn test_stream_mode_with_two_sources_is_valid() {
        // Two live sources feeding the same sink: the stream runner pulls
        // them round-robin, one batch per item.
        let raw = stream_kdl(
            r#"
source "a" type="tcp" component="Tick"
source "b" type="tcp" component="Tick"
"#,
            r#"sink "out" type="NoopSink" component="Tick""#,
            r#"
link from="a" to="out"
link from="b" to="out"
"#,
        );
        let cfg = parse(&raw).expect("parse");
        cfg.validate().expect("two sources + stream mode is valid");
    }

    #[test]
    fn test_tcp_source_rejected_outside_stream_mode() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    source "ticks" type="tcp" component="Tick"
    sink "out" type="NoopSink" component="Tick"
    link from="ticks" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("never reaches EOF"), "got: {err}");
    }

    #[test]
    fn test_kafka_source_rejected_outside_stream_mode_unless_stop_at_end() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    source "orders_in" type="KafkaSource" component="Order"
    sink "out" type="NoopSink" component="Order"
    link from="orders_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("never reaches EOF"), "got: {err}");

        let raw_stop_at_end = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    source "orders_in" type="KafkaSource" component="Order" {
        config stop_at_end=#true
    }
    sink "out" type="NoopSink" component="Order"
    link from="orders_in" to="out"
}
"#;
        let cfg = parse(raw_stop_at_end).expect("parse");
        cfg.validate()
            .expect("stop_at_end=#true makes a KafkaSource usable outside stream mode");
    }

    #[test]
    fn test_workflow_systems_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    systems "proc" type="Proc"
}
"#;
        let err = parse(raw).expect_err("a systems node must be rejected, not silently dropped");
        assert!(
            err.contains("systems"),
            "error should name the offending key: {err}"
        );
    }

    #[test]
    fn test_workflow_components_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    components "orders" type="GenericComponent"
}
"#;
        let err = parse(raw).expect_err("a components node must be rejected, not silently dropped");
        assert!(err.contains("components"), "{err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_watch_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    wasm "p" module="pipeline.wasm" watch=#true
}
"#;
        let err = parse(raw).expect_err("watch was never implemented; it must not parse");
        assert!(err.contains("watch"), "{err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_only_workflow_is_a_valid_entry_point() {
        let cfg = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/pcs"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![WorkflowSpec {
                id: "w".to_string(),
                name: None,
                transformers: Vec::new(),
                sources: Vec::new(),
                wasm: vec![WasmSpec {
                    id: "p".to_string(),
                    name: None,
                    module: Some("pipeline.wasm".to_string()),
                    sha3_256: None,
                    config: HashMap::new(),
                    #[cfg(feature = "windows")]
                    window: None,
                }],
                #[cfg(feature = "plugin")]
                plugin: Vec::new(),
                sinks: Vec::new(),
                links: Vec::new(),
            }],
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
        };
        cfg.validate()
            .expect("a lone processor entry point should be valid");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_two_wasm_nodes_are_independent_processors_linked_explicitly() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    wasm "validate" module="validate.wasm" {
        config min_amount="0.50"
    }
    wasm "settle" module="settle.wasm" {
        config fee_bps="290"
    }
    link from="validate" to="settle"
}
"#;
        let cfg = parse(raw).expect("parse");
        assert_eq!(cfg.workflows[0].wasm.len(), 2);
        assert_eq!(cfg.workflows[0].wasm[0].id, "validate");
        assert_eq!(
            cfg.workflows[0].wasm[0].module.as_deref(),
            Some("validate.wasm")
        );
        assert_eq!(
            cfg.workflows[0].wasm[0]
                .config
                .get("min_amount")
                .map(String::as_str),
            Some("0.50")
        );
        assert_eq!(cfg.workflows[0].wasm[1].id, "settle");
        assert_eq!(
            cfg.workflows[0].wasm[1]
                .config
                .get("fee_bps")
                .map(String::as_str),
            Some("290")
        );
        assert_eq!(
            cfg.workflows[0].links,
            vec![LinkSpec {
                from: "validate".to_string(),
                to: "settle".to_string(),
                branch: None,
            }]
        );
        cfg.validate().expect("two linked processors are valid");
    }

    /// `one_or_many` goes through the value tree rather than an untagged enum
    /// specifically so a `deny_unknown_fields` violation inside one entry
    /// still names the offending key instead of collapsing into a generic
    /// "data did not match any variant" error.
    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_unknown_key_is_rejected_with_field_name() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    wasm "validate" module="validate.wasm"
    wasm "settle" module="settle.wasm" watch=#true
}
"#;
        let err = parse(raw).expect_err("watch was never implemented; it must not parse");
        assert!(err.contains("watch"), "{err}");
    }

    #[cfg(feature = "plugin")]
    #[test]
    fn test_plugin_workflow_round_trips() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    plugin "audit" library="pipelines/libtransform.so" sha3_256="sha3-256:abc123" {
        config "smoketest.multiplier"="10"
    }
}
"#;
        let cfg = parse(raw).expect("parse");
        let spec = cfg.workflows[0]
            .plugin
            .first()
            .expect("the plugin node should parse");
        assert_eq!(spec.id, "audit");
        assert_eq!(spec.library.as_deref(), Some("pipelines/libtransform.so"));
        assert_eq!(spec.sha3_256.as_deref(), Some("sha3-256:abc123"));
        assert_eq!(
            spec.config.get("smoketest.multiplier").map(String::as_str),
            Some("10")
        );
        cfg.validate()
            .expect("plugin-only workflow should be valid");
    }

    #[cfg(feature = "plugin")]
    #[test]
    fn test_plugin_unknown_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    plugin "p" library="libtransform.so" module="libtransform.so"
}
"#;
        let err = parse(raw).expect_err("a key the loader cannot honour must not parse");
        assert!(err.contains("module"), "{err}");
    }

    #[cfg(feature = "plugin")]
    #[test]
    fn test_plugin_digest_and_config_default_to_empty() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    plugin "p" library="libtransform.so"
}
"#;
        let cfg = parse(raw).expect("parse");
        let spec = cfg
            .workflows
            .into_iter()
            .next()
            .expect("a workflow")
            .plugin
            .into_iter()
            .next()
            .expect("the plugin node should parse");
        assert!(spec.name.is_none(), "name is optional");
        assert!(spec.sha3_256.is_none(), "digest is optional");
        assert!(spec.config.is_empty(), "config defaults to an empty table");
    }

    #[cfg(feature = "plugin")]
    #[test]
    fn test_plugin_with_no_library_relies_on_a_registered_native_runtime() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    plugin "p" name="P"
}
"#;
        let cfg = parse(raw).expect("parse");
        let spec = &cfg.workflows[0].plugin[0];
        assert!(spec.library.is_none());
        cfg.validate()
            .expect("an artifact-less processor entry point is structurally valid");
    }

    #[test]
    fn test_standalone_mode_with_a_linked_source_and_sink_is_valid() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {
    source "kafka_in" type="MongoSource" component="orders"
    sink "out" type="NoopSink" component="orders"
    link from="kafka_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        cfg.validate()
            .expect("standalone mode with a linked source and sink should be valid");
    }

    // ── WorkflowSpec::validate: graph rules ─────────────────────────────────

    fn workflow_kdl(body: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "w" {{
{body}
}}
"#
        )
    }

    fn workflow_err(body: &str) -> String {
        let cfg = parse(&workflow_kdl(body)).expect("parse should succeed");
        cfg.validate()
            .expect_err("expected a graph validation error")
            .to_string()
    }

    #[test]
    fn rule_id_charset_is_enforced() {
        let err = workflow_err(
            r#"
source "bad/id" type="NoopSource" component="X"
sink "out" type="NoopSink" component="X"
link from="bad/id" to="out"
"#,
        );
        assert!(err.contains("is invalid"), "got: {err}");
        assert!(err.contains("bad/id"), "got: {err}");
    }

    #[test]
    fn rule_duplicate_id_across_kinds_is_rejected() {
        let err = workflow_err(
            r#"
source "shared" type="NoopSource" component="X"
sink "shared" type="NoopSink" component="X"
link from="shared" to="shared"
"#,
        );
        assert!(err.contains("declared twice"), "got: {err}");
    }

    #[test]
    fn rule_source_transformer_must_be_declared() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" transformer="missing" component="X"
sink "out" type="NoopSink" component="X"
link from="in" to="out"
"#,
        );
        assert!(err.contains("names transformer 'missing'"), "got: {err}");
    }

    #[test]
    fn rule_link_to_undeclared_node_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "out" type="NoopSink" component="X"
link from="in" to="ghost"
"#,
        );
        assert!(err.contains("undeclared node 'ghost'"), "got: {err}");
    }

    #[test]
    fn rule_self_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
link from="in" to="in"
"#,
        );
        assert!(err.contains("links a node to itself"), "got: {err}");
    }

    #[test]
    fn rule_duplicate_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "out" type="NoopSink" component="X"
link from="in" to="out"
link from="in" to="out"
"#,
        );
        assert!(err.contains("is declared twice"), "got: {err}");
    }

    #[test]
    fn rule_link_into_a_source_is_rejected() {
        let err = workflow_err(
            r#"
source "a" type="NoopSource" component="X"
source "b" type="NoopSource" component="X"
link from="a" to="b"
"#,
        );
        assert!(err.contains("a source has no input"), "got: {err}");
    }

    #[test]
    fn rule_link_out_of_a_sink_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "a" type="NoopSink" component="X"
sink "b" type="NoopSink" component="X"
link from="in" to="a"
link from="a" to="b"
"#,
        );
        assert!(err.contains("a sink has no output"), "got: {err}");
    }

    #[test]
    fn rule_two_link_cycle_is_rejected() {
        #[cfg(feature = "wasm")]
        {
            let err = workflow_err(
                r#"
wasm "p1" module="p1.wasm"
wasm "p2" module="p2.wasm"
link from="p1" to="p2"
link from="p2" to="p1"
"#,
            );
            assert!(err.contains("links contain a cycle"), "got: {err}");
        }
    }

    #[test]
    fn rule_source_with_no_outbound_link_is_rejected() {
        let err = workflow_err(r#"source "in" type="NoopSource" component="X""#);
        assert!(
            err.contains("source 'in' has no outbound link"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_sink_with_no_inbound_link_is_rejected() {
        let err = workflow_err(r#"sink "out" type="NoopSink" component="X""#);
        assert!(err.contains("sink 'out' has no inbound link"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_branch_charset_is_enforced() {
        let err = workflow_err(
            r#"
wasm "p" module="p.wasm"
sink "a" type="NoopSink" component="X"
sink "b" type="NoopSink" component="X"
link from="p" to="a" branch="bad/branch"
link from="p" to="b" branch="high"
"#,
        );
        assert!(err.contains("is invalid"), "got: {err}");
        assert!(err.contains("bad/branch"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_branch_on_a_source_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "out" type="NoopSink" component="X"
link from="in" to="out" branch="high"
"#,
        );
        assert!(err.contains("only a processor can route"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_node_mixes_labelled_and_unlabelled_links_is_rejected() {
        let err = workflow_err(
            r#"
wasm "p" module="p.wasm"
sink "a" type="NoopSink" component="X"
sink "b" type="NoopSink" component="X"
link from="p" to="a" branch="high"
link from="p" to="b"
"#,
        );
        assert!(err.contains("mixes labelled and unlabelled"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_two_labelled_links_validate() {
        let cfg = parse(&workflow_kdl(
            r#"
wasm "p" module="p.wasm"
sink "a" type="NoopSink" component="X"
sink "b" type="NoopSink" component="X"
link from="p" to="a" branch="high"
link from="p" to="b" branch="low"
"#,
        ))
        .expect("parse should succeed");
        cfg.validate().expect("two labelled links are valid");
    }

    // ── Cross-workflow rules: ServiceConfig::validate ────────────────────────

    #[test]
    fn test_two_workflow_blocks_parse_into_two_workflows() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "a" {
    source "in_a" type="NoopSource" component="X"
    sink "out_a" type="NoopSink" component="X"
    link from="in_a" to="out_a"
}

workflow "b" {
    source "in_b" type="NoopSource" component="Y"
    sink "out_b" type="NoopSink" component="Y"
    link from="in_b" to="out_b"
}
"#;
        let cfg = parse(raw).expect("parse");
        assert_eq!(cfg.workflows.len(), 2);
        assert_eq!(cfg.workflows[0].id, "a");
        assert_eq!(cfg.workflows[1].id, "b");
        cfg.validate()
            .expect("two independent workflows with disjoint ids validate");
    }

    #[test]
    fn rule_duplicate_node_id_across_workflows_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "a" {
    source "shared" type="NoopSource" component="X"
    sink "out_a" type="NoopSink" component="X"
    link from="shared" to="out_a"
}

workflow "b" {
    source "shared" type="NoopSource" component="Y"
    sink "out_b" type="NoopSink" component="Y"
    link from="shared" to="out_b"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("node id 'shared'"), "got: {err}");
        assert!(err.contains("workflow 'a'"), "got: {err}");
        assert!(err.contains("workflow 'b'"), "got: {err}");
    }

    #[test]
    fn rule_duplicate_workflow_id_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "dup" {
    source "in_a" type="NoopSource" component="X"
    sink "out_a" type="NoopSink" component="X"
    link from="in_a" to="out_a"
}

workflow "dup" {
    source "in_b" type="NoopSource" component="Y"
    sink "out_b" type="NoopSink" component="Y"
    link from="in_b" to="out_b"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("workflow id 'dup' is declared twice"),
            "got: {err}"
        );
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_cluster_mode_requires_exactly_one_workflow() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/pcs"

peer id=1 addr="127.0.0.1:9000"

workflow "a" {
    wasm "p1" module="p1.wasm"
}

workflow "b" {
    wasm "p2" module="p2.wasm"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("cluster mode requires exactly one workflow; found 2"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_channel_source_with_no_paired_sink_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="ChannelSource" component="X" {
    config name="orphan"
}
sink "out" type="NoopSink" component="X"
link from="in" to="out"
"#,
        );
        assert!(
            err.contains("channel 'orphan': declares a ChannelSource but no ChannelSink"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_channel_sink_with_no_paired_source_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "out" type="ChannelSink" component="X" {
    config name="orphan"
}
link from="in" to="out"
"#,
        );
        assert!(
            err.contains("channel 'orphan': declares a ChannelSink but no ChannelSource"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_duplicate_channel_source_name_is_rejected() {
        let err = workflow_err(
            r#"
source "in1" type="ChannelSource" component="X" {
    config name="dup"
}
source "in2" type="ChannelSource" component="X" {
    config name="dup"
}
sink "out1" type="NoopSink" component="X"
sink "out2" type="NoopSink" component="X"
link from="in1" to="out1"
link from="in2" to="out2"
"#,
        );
        assert!(
            err.contains("channel 'dup': more than one ChannelSource declared"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_duplicate_channel_sink_name_is_rejected() {
        let err = workflow_err(
            r#"
source "in1" type="NoopSource" component="X"
source "in2" type="NoopSource" component="X"
sink "out1" type="ChannelSink" component="X" {
    config name="dup"
}
sink "out2" type="ChannelSink" component="X" {
    config name="dup"
}
link from="in1" to="out1"
link from="in2" to="out2"
"#,
        );
        assert!(
            err.contains("channel 'dup': more than one ChannelSink declared"),
            "got: {err}"
        );
    }

    #[test]
    fn two_workflows_bridged_by_a_named_channel_validate() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/pcs"

workflow "producer" {
    source "in" type="NoopSource" component="X"
    sink "bridge_out" type="ChannelSink" component="X" {
        config name="bridge"
    }
    link from="in" to="bridge_out"
}

workflow "consumer" {
    source "bridge_in" type="ChannelSource" component="X" {
        config name="bridge"
    }
    sink "out" type="NoopSink" component="X"
    link from="bridge_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        cfg.validate()
            .expect("a sink in one workflow paired with a source in another is valid");
    }

    // ── Windowing: the `window` block on processor nodes ────────────────────

    /// The old rule 10 rejected a processor fed by both a source and another
    /// processor. Fan-in merging is exactly what a windowed processor is for:
    /// one node receives rows from several streams and merges them, so the
    /// rule is gone and this shape must validate.
    #[cfg(feature = "wasm")]
    #[test]
    fn mixed_source_and_processor_fan_in_is_valid() {
        let cfg = parse(&workflow_kdl(
            r#"
source "s" type="NoopSource" component="X"
wasm "up" module="up.wasm"
wasm "down" module="down.wasm"
link from="s" to="down"
link from="up" to="down"
"#,
        ))
        .expect("parse should succeed");
        cfg.validate()
            .expect("a processor may be fed by sources and processors at once");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn window_block_parses_with_one_and_many_key_fields() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="tumbling" size_ms=30000 offset_ms=500 time_field="timestamp_ms" allowed_lateness_ms=5000 {
        key_field "category"
        key_field "region"
    }
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let window = cfg.workflows[0].wasm[0]
            .window
            .as_ref()
            .expect("window block");
        assert_eq!(
            window.spec,
            pcs_core::windows::WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 500,
            }
        );
        assert_eq!(window.time_field, "timestamp_ms");
        assert_eq!(window.key_fields, vec!["category", "region"]);
        assert_eq!(window.allowed_lateness_ms, 5_000);
        cfg.validate().expect("a sane window block validates");

        // A single key_field child is a scalar, not an array: the one-or-many
        // deserializer must accept both shapes.
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="session" gap_ms=10000 time_field="ts" {
        key_field "category"
    }
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let window = cfg.workflows[0].wasm[0]
            .window
            .as_ref()
            .expect("window block");
        assert_eq!(
            window.spec,
            pcs_core::windows::WindowSpec::Session { gap_ms: 10_000 }
        );
        assert_eq!(window.key_fields, vec!["category"]);
        assert_eq!(window.allowed_lateness_ms, 0, "lateness defaults to zero");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn window_unknown_key_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="tumbling" size_ms=1000 time_field="ts" bogus=1
}
"#,
        );
        let err = parse(&raw).expect_err("an unknown window key must not parse");
        assert!(err.contains("bogus"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn window_unknown_kind_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="hourly" size_ms=1000 time_field="ts"
}
"#,
        );
        let err = parse(&raw).expect_err("an unknown window kind must not parse");
        assert!(err.contains("hourly"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn window_missing_time_field_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="tumbling" size_ms=1000
}
"#,
        );
        let err = parse(&raw).expect_err("time_field is mandatory");
        assert!(err.contains("time_field"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn window_geometry_key_wrong_for_kind_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="session" gap_ms=1000 size_ms=500 time_field="ts"
}
"#,
        );
        let err = parse(&raw).expect_err("size_ms is not a session geometry");
        assert!(err.contains("size_ms"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn window_invalid_geometry_is_rejected_at_validate() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="sliding" size_ms=1000 slide_ms=2000 time_field="ts"
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let err = cfg
            .validate()
            .expect_err("slide > size is nonsense and must be rejected")
            .to_string();
        assert!(err.contains("window is invalid"), "got: {err}");
        assert!(err.contains("slide_ms"), "got: {err}");
    }

    #[cfg(feature = "windows")]
    #[test]
    fn window_config_pairs_cover_the_geometry() {
        let window = WindowConfig {
            spec: pcs_core::windows::WindowSpec::Sliding {
                size_ms: 60_000,
                slide_ms: 10_000,
                offset_ms: 500,
            },
            time_field: "timestamp_ms".to_string(),
            key_fields: vec!["category".to_string(), "region".to_string()],
            allowed_lateness_ms: 5_000,
        };
        let pairs = window.config_pairs();
        let get = |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("missing {key}"))
        };
        assert_eq!(get("window.kind"), "sliding");
        assert_eq!(get("window.size_ms"), "60000");
        assert_eq!(get("window.slide_ms"), "10000");
        assert_eq!(get("window.offset_ms"), "500");
        assert_eq!(get("window.time_field"), "timestamp_ms");
        assert_eq!(get("window.key_fields"), "category,region");
        assert_eq!(get("window.allowed_lateness_ms"), "5000");

        // The injected keys must not clobber a key the operator already wrote.
        let mut spec = std::collections::HashMap::new();
        spec.insert("window.kind".to_string(), "custom".to_string());
        for (key, value) in pairs {
            spec.entry(key).or_insert(value);
        }
        assert_eq!(spec.get("window.kind").map(String::as_str), Some("custom"));
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn window_block_is_rejected_in_cluster_mode() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/pcs"

peer id=1 addr="127.0.0.1:9000"

workflow "w" {
    wasm "p" module="p.wasm" {
        window kind="tumbling" size_ms=1000 time_field="ts"
    }
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg
            .validate()
            .expect_err("cluster mode cannot honour a window block");
        assert!(err.to_string().contains("window"), "got: {err}");
    }
}
