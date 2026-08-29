//! Cluster runner for the `service` feature.
//!
//! [`run_cluster`] validates the data-directory layout (`bootstrap.lock`,
//! `node-id`), starts an [`ArrowRaftDriver`] with the configured peers and
//! timings, bootstraps a fresh cluster when configured, waits for Raft to
//! settle, runs the [`DistributedRunner`] loop until cancellation, then shuts
//! down gracefully.
//!
//! ## Sources and sinks
//!
//! Sources are rejected in cluster mode by
//! [`ServiceConfig::validate`](super::config::ServiceConfig::validate), so the
//! cluster path consumes batches registered through the shared store. Sinks run
//! locally in the runner's scheduler after each batch, so output is spread
//! across nodes and operators must aggregate it externally.
//!
//! ## Store backend
//!
//! The shared store is always TiKV: `ServiceConfig::validate` requires a
//! `store "tikv" { … }` block in cluster mode. Application mutations —
//! partitions, claims, checkpoints — go to TiKV's own raft, and the PCS raft
//! node (tikv/raft-rs) runs only for membership and leadership.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::PcsError;
use crate::PcsResult;
use crate::distributed::SharedStore;
use crate::distributed::checkpoint::CheckpointStore;
use crate::distributed::consensus::driver::{
    ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle,
};
use crate::distributed::runner::{DistributedRunner, RunnerConfig};
use crate::distributed::strategy::CheckpointStrategy;
#[cfg(feature = "tikv-store")]
use crate::distributed::{TikvSharedStore, TikvStoreConfig};
use crate::service::builder::BuiltService;
use crate::service::config::{ClusterConfig, ServiceConfig, ServiceMode};

// ── File names ────────────────────────────────────────────────────────────────

const BOOTSTRAP_LOCK_FILE: &str = "bootstrap.lock";
const RAFT_LOG_DB_FILE: &str = "raft-log.redb";
const NODE_ID_FILE: &str = "node-id";

// ── Timing constants ─────────────────────────────────────────────────────────

/// How long to wait for Raft to exit Candidate state on startup.
const RAFT_SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll interval while waiting for Raft metrics to settle.
const RAFT_SETTLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Budget for leader-transfer attempt during graceful shutdown.
const LEADER_TRANSFER_BUDGET: Duration = Duration::from_secs(5);

/// Aggregate statistics returned by [`run_cluster`].
#[derive(Debug, Default, Clone)]
pub struct ClusterStats {
    /// Batches successfully processed (acked) during this run.
    pub batches_processed: u64,
    /// Batches that encountered a processing error.
    pub batches_failed: u64,
    /// Claim errors (could not claim a batch).
    pub claim_errors: u64,
    /// Checkpoints written during this run.
    pub checkpoints_written: u64,
    /// Node ID of the last known leader, if available.
    pub last_leader_id: Option<u64>,
    /// Raft term at exit, if available.
    pub last_raft_term: Option<u64>,
    /// Total wall-clock milliseconds the runner was active.
    pub total_duration_ms: u64,
}

/// Run the cluster scheduler until `cancel` is signalled.
///
/// # Errors
///
/// Returns [`PcsError::Configuration`] if:
/// - `config.mode` is not `ServiceMode::Cluster`.
/// - The data directory contains `raft-log.redb` without `bootstrap.lock`
///   (indicates an unclean shutdown before bootstrap completed).
/// - The stored `node-id` file disagrees with `config.node.id`.
/// - The Raft driver cannot be started.
///
/// Returns [`PcsError::Generic`] if the Raft cluster does not settle within
/// the configured settle timeout.
pub async fn run_cluster(
    built: BuiltService,
    config: &ServiceConfig,
    cancel: CancellationToken,
) -> PcsResult<ClusterStats> {
    let start = std::time::Instant::now();
    let mut stats = ClusterStats::default();

    let cluster = match &config.mode {
        ServiceMode::Cluster { config: c } => c,
        ServiceMode::Standalone { .. } => {
            return Err(PcsError::configuration(
                "run_cluster called with standalone config — use run_standalone instead",
            ));
        }
    };

    let data_dir = &config.node.data_dir;

    validate_data_dir(data_dir, config.node.id)?;

    let this_peer = cluster
        .peers
        .iter()
        .find(|p| p.id == config.node.id)
        .ok_or_else(|| {
            PcsError::configuration(format!(
                "node id {} not found in cluster peers",
                config.node.id
            ))
        })?;

    let listen_addr: SocketAddr = this_peer.addr.parse().map_err(|e| {
        PcsError::configuration(format!("invalid peer addr '{}': {e}", this_peer.addr))
    })?;

    // Peer addresses are stored as strings; the transport layer resolves them
    // via DNS lazily at connection time (supports Docker service hostnames).
    let peers: HashMap<u64, String> = cluster
        .peers
        .iter()
        .filter(|p| p.id != config.node.id)
        .map(|p| (p.id, p.addr.clone()))
        .collect();

    let driver_config = ArrowRaftDriverConfig {
        node_id: config.node.id,
        listen_addr,
        peers,
        heartbeat_interval_ms: cluster.heartbeat_interval_ms,
        election_timeout_ms: cluster.election_timeout_ms,
        snapshot_log_interval: cluster.snapshot_log_interval,
    };

    let log_db_path = data_dir.join(RAFT_LOG_DB_FILE);

    let (handle, _driver_task) = ArrowRaftDriver::start(driver_config, &log_db_path)
        .await
        .map_err(|e| PcsError::configuration(format!("ArrowRaftDriver::start failed: {e}")))?;

    let bootstrap_lock = data_dir.join(BOOTSTRAP_LOCK_FILE);
    if cluster.bootstrap && !bootstrap_lock.exists() {
        bootstrap_cluster(
            &handle,
            cluster,
            config.node.id,
            &this_peer.addr,
            &bootstrap_lock,
        )
        .await?;
        // Record the node identity so a later start can catch an accidental
        // node-id change, such as a wrong PCS_NODE_ID.
        write_node_id_file(data_dir, config.node.id)?;
    }

    wait_for_raft_settled(&handle, cluster.election_timeout_ms).await?;

    // Bound to the returned handle's lifetime: `AbortOnDropHandle` stops the
    // recorder on every exit path from `run_cluster`, including the fallible
    // ones between here and `handle.shutdown()`.
    let _raft_gauges = tokio_util::task::AbortOnDropHandle::new(spawn_raft_gauges(
        handle.clone(),
        cancel.child_token(),
    ));

    let metrics = handle.metrics();
    stats.last_raft_term = Some(metrics.term);
    stats.last_leader_id = metrics.leader_id;

    // TiKV is the only cluster application-data store. `validate` guarantees a
    // `store "tikv"` block is present and that this binary carries the
    // `tikv-store` feature, so both error paths here are defence in depth for a
    // caller that built a `ServiceConfig` in code and skipped validation.
    let store_config = config.store.as_ref().ok_or_else(|| {
        PcsError::configuration(
            "mode \"cluster\" requires a `store \"tikv\"` block: TiKV is the only cluster \
             application-data store",
        )
    })?;
    let store = connect_store(store_config).await?;

    let producer_cancel = cancel.child_token();
    // ServiceConfig::validate rejects sources in cluster mode, so built.nodes
    // contains no BuiltNodeKind::Source by the time run_cluster runs.
    debug_assert!(
        built
            .nodes
            .iter()
            .all(|n| !matches!(n.kind, super::builder::BuiltNodeKind::Source(_))),
        "cluster runner received a source node — validate() should have rejected this config"
    );
    let source_task: Option<tokio::task::JoinHandle<()>> = None;
    let _ = producer_cancel; // unused until sources are wired

    // The clone-empty template gives each partition a fresh, schema-registered
    // Dataset with no row data. Config validation (rule 11) guarantees the
    // workflow declared exactly one node and that it is a processor.
    let mut nodes = built.nodes;
    let node = nodes
        .pop()
        .expect("cluster mode config validation guarantees exactly one node");
    let runtime = match node.kind {
        super::builder::BuiltNodeKind::Processor { runtime, .. } => runtime,
        _ => unreachable!("cluster mode config validation guarantees the one node is a processor"),
    };
    let dataset_template = runtime.template_dataset();

    // Gate 3b: the persisted checkpoints on this node must belong to the same
    // schema shape the pipeline declares, or resuming would mix layouts.
    crate::service::validation::validate_schema_fingerprint(
        dataset_template.schemas().fingerprint(),
        store.persisted_schema_id().await?,
    )?;

    let runner_config = RunnerConfig {
        checkpoint_strategy: CheckpointStrategy::EveryStage,
        // Cluster mode is guaranteed exactly one workflow by
        // `ServiceConfig::validate`.
        workflow_id: config.workflows[0].id.clone(),
        processor_id: node.id.clone(),
        ..Default::default()
    };

    let runner = DistributedRunner::new(store, runtime, runner_config);

    let runner_cancel = cancel.child_token();

    // `DistributedRunner::run` loops until no more batches or `max_batches`.
    // We race it against the cancellation token so the cluster runner exits
    // cleanly when the service receives a shutdown signal.
    //
    // At-least-once guarantee: if the cancellation arm wins, the in-flight
    // `runner.run()` future is dropped and the current batch is NOT acked via
    // `PartitionSource::ack_claim`. On the next run, the `PartitionSource`
    // redelivers it (via claim lease expiry or unacked claim). Scheduler
    // systems must therefore be idempotent.
    let processed = tokio::select! {
        result = runner.run(move || dataset_template.clone_empty()) => result,
        _ = runner_cancel.cancelled() => Ok(0),
    };

    match processed {
        Ok(n) => {
            stats.batches_processed = n as u64;
        }
        Err(e) => {
            stats.batches_failed += 1;
            // Log but do not abort; shutdown proceeds regardless.
            eprintln!("[pcs cluster] runner error: {e}");
        }
    }

    eprintln!("[pcs cluster] cluster runner cancelled, initiating shutdown");

    // Cancel the source producer (if it ever runs).
    if let Some(task) = source_task {
        task.abort();
    }

    // Shutdown skips leader transfer. The cost is one election cycle of
    // unavailability (election_timeout_ms * 2).
    let _ = LEADER_TRANSFER_BUDGET; // budget reserved

    handle.shutdown().await;

    stats.total_duration_ms = start.elapsed().as_millis() as u64;

    Ok(stats)
}

/// Connect the cluster shared store described by `store_config`.
///
/// Dual impl, like `crate::metrics::Instruments`: without `tikv-store` there is
/// no backend to connect, so the call is a configuration error rather than a
/// `#[cfg]` at the call site.
#[cfg(feature = "tikv-store")]
async fn connect_store(
    store_config: &crate::service::config::StoreConfig,
) -> PcsResult<Box<dyn SharedStore>> {
    let tcfg = TikvStoreConfig::try_from(store_config)?;
    Ok(Box::new(TikvSharedStore::connect(&tcfg).await?))
}

#[cfg(not(feature = "tikv-store"))]
async fn connect_store(
    _store_config: &crate::service::config::StoreConfig,
) -> PcsResult<Box<dyn SharedStore>> {
    Err(PcsError::configuration(
        "mode \"cluster\" requires the `tikv-store` feature: TiKV is the only cluster \
         application-data store — rebuild with `--features service-cluster,tikv-store`",
    ))
}

/// Record `pcs_raft_commit_index`, `pcs_raft_term` and `pcs_raft_leader_id`
/// once a second until `cancel` fires.
///
/// [`ArrowRaftDriverHandle::metrics`] is synchronous, so this needs no other
/// plumbing. It exists because `/status` still reports `"cluster": null`: the
/// gauges come from the driver handle, not from a populated `cluster_probe`.
fn spawn_raft_gauges(
    handle: ArrowRaftDriverHandle,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let m = handle.metrics();
                    crate::metrics::instruments().raft(
                        m.applied_index,
                        m.term,
                        m.leader_id,
                    );
                }
                _ = cancel.cancelled() => break,
            }
        }
    })
}

// ── Validation helpers ────────────────────────────────────────────────────────

/// Validate the data-directory layout before starting the cluster.
///
/// Rules enforced:
/// - If `raft-log.redb` exists without `bootstrap.lock` → error (unclean state).
/// - If `node-id` file exists and disagrees with `node_id` → error (misconfiguration).
fn validate_data_dir(data_dir: &Path, node_id: u64) -> PcsResult<()> {
    let raft_log = data_dir.join(RAFT_LOG_DB_FILE);
    let bootstrap_lock = data_dir.join(BOOTSTRAP_LOCK_FILE);
    let node_id_file = data_dir.join(NODE_ID_FILE);

    // Rule 1: raft-log.redb without bootstrap.lock → refuse to start.
    if raft_log.exists() && !bootstrap_lock.exists() {
        return Err(PcsError::configuration(format!(
            "data_dir {:?} contains '{}' but no '{}'. \
             This indicates an unclean shutdown before bootstrap completed. \
             Restore from backup or delete the data directory to reinitialise.",
            data_dir, RAFT_LOG_DB_FILE, BOOTSTRAP_LOCK_FILE
        )));
    }

    // Rule 2: node-id file must match config.
    if node_id_file.exists() {
        let stored = std::fs::read_to_string(&node_id_file)
            .map_err(|e| PcsError::store(format!("read node-id file: {e}")))?;
        let stored_id: u64 = stored.trim().parse().map_err(|_| {
            PcsError::configuration(format!(
                "node-id file contains non-numeric content: {:?}",
                stored.trim()
            ))
        })?;
        if stored_id != node_id {
            return Err(PcsError::configuration(format!(
                "node-id file contains {stored_id} but config has node.id={node_id}. \
                 Data directory belongs to a different node. \
                 Use the correct data_dir or update node.id."
            )));
        }
    }

    Ok(())
}

/// Write the `node-id` file (idempotent if already correct).
fn write_node_id_file(data_dir: &Path, node_id: u64) -> PcsResult<()> {
    let path = data_dir.join(NODE_ID_FILE);
    if path.exists() {
        return Ok(()); // already written and validated by validate_data_dir
    }
    std::fs::create_dir_all(data_dir)
        .map_err(|e| PcsError::store(format!("create data_dir {data_dir:?}: {e}")))?;
    std::fs::write(&path, node_id.to_string())
        .map_err(|e| PcsError::store(format!("write node-id file: {e}")))?;
    Ok(())
}

async fn bootstrap_cluster(
    _handle: &crate::distributed::consensus::driver::ArrowRaftDriverHandle,
    cluster: &ClusterConfig,
    node_id: u64,
    this_addr: &str,
    bootstrap_lock: &Path,
) -> PcsResult<()> {
    // Membership is static: the driver seeds the conf state from the
    // configured peers on first boot, so there is no `initialize` call.
    // Bootstrap bookkeeping below stays: it marks that the operator
    // explicitly created this cluster.
    let _ = (cluster, node_id, this_addr);

    let lock_dir = bootstrap_lock.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(lock_dir)
        .map_err(|e| PcsError::store(format!("create data_dir {lock_dir:?}: {e}")))?;
    std::fs::write(bootstrap_lock, "bootstrapped")
        .map_err(|e| PcsError::store(format!("write bootstrap.lock: {e}")))?;

    eprintln!("[pcs cluster] cluster bootstrapped, bootstrap.lock written");
    Ok(())
}

/// Wait until the Raft node settles into a stable role.
///
/// Polls [`ArrowRaftDriverHandle::metrics`] every [`RAFT_SETTLE_POLL_INTERVAL`]
/// until the node reports `Leader`, `Follower`, or `Learner` state, or until
/// [`RAFT_SETTLE_TIMEOUT`] expires.
async fn wait_for_raft_settled(
    handle: &crate::distributed::consensus::driver::ArrowRaftDriverHandle,
    election_timeout_ms: u64,
) -> PcsResult<()> {
    let deadline = tokio::time::Instant::now() + RAFT_SETTLE_TIMEOUT;

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(PcsError::generic(format!(
                "Raft did not settle within {}s. \
                 Check peer connectivity and election_timeout_ms ({election_timeout_ms}ms).",
                RAFT_SETTLE_TIMEOUT.as_secs()
            )));
        }

        let metrics = handle.metrics();
        {
            use crate::distributed::consensus::driver::RaftNodeState;
            match metrics.state {
                RaftNodeState::Leader | RaftNodeState::Follower => {
                    return Ok(());
                }
                RaftNodeState::Candidate | RaftNodeState::PreCandidate => {
                    // Still electing; keep waiting.
                }
            }
        }

        tokio::time::sleep(RAFT_SETTLE_POLL_INTERVAL).await;
    }
}

#[cfg(all(test, feature = "service-cluster"))]
mod tests {
    use super::*;
    use crate::distributed::consensus::driver::{ArrowRaftDriver, ArrowRaftDriverConfig};
    use crate::service::config::{
        ClusterConfig, HttpConfig, NodeConfig, ObservabilityConfig, PeerSpec, ServiceConfig,
        ServiceMode, StandaloneConfig, WorkflowSpec,
    };
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    fn free_addr() -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    }

    /// Read the value of a single-sample Prometheus gauge line by series name.
    ///
    /// The exporter attaches an `otel_scope_name` label, so the name is followed
    /// by `{...}` rather than a space.
    #[cfg(feature = "metrics")]
    fn gauge_value(text: &str, series: &str) -> Option<f64> {
        text.lines()
            .filter(|l| !l.starts_with('#'))
            .find_map(|l| l.strip_prefix(series))
            .and_then(|rest| rest.rsplit(' ').next())
            .and_then(|v| v.parse::<f64>().ok())
    }

    fn single_node_driver_config(addr: SocketAddr) -> ArrowRaftDriverConfig {
        ArrowRaftDriverConfig {
            node_id: 1,
            listen_addr: addr,
            peers: HashMap::new(),
            heartbeat_interval_ms: 30,
            election_timeout_ms: 200,
            snapshot_log_interval: 1000,
        }
    }

    fn single_peer_cluster(addr: SocketAddr) -> ClusterConfig {
        ClusterConfig {
            peers: vec![PeerSpec {
                id: 1,
                addr: addr.to_string(),
            }],
            bootstrap: true,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_log_interval: 1000,
        }
    }

    /// A one-node cluster config with no `store` block, which `validate`
    /// rejects and `run_cluster` refuses.
    fn storeless_cluster_config(addr: SocketAddr, data_dir: PathBuf) -> ServiceConfig {
        ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir,
            },
            mode: ServiceMode::Cluster {
                config: single_peer_cluster(addr),
            },
            workflows: vec![WorkflowSpec {
                id: "cluster-test".to_string(),
                name: None,
                transformers: Vec::new(),
                sources: Vec::new(),
                #[cfg(feature = "wasm")]
                wasm: Vec::new(),
                #[cfg(feature = "plugin")]
                plugin: Vec::new(),
                sinks: Vec::new(),
                links: Vec::new(),
            }],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: HashMap::new(),
        }
    }

    fn empty_built_service() -> crate::service::builder::BuiltService {
        use crate::service::builder::{BuiltNode, BuiltNodeKind, BuiltService};
        BuiltService {
            workflow_id: "cluster-test".to_string(),
            workflow_name: None,
            nodes: vec![BuiltNode {
                id: "p".to_string(),
                name: None,
                type_name: "native".to_string(),
                component: None,
                kind: BuiltNodeKind::Processor {
                    runtime: Box::new(crate::pipeline::Pipeline::new("test")),
                    kind: "native",
                },
                downstream: Vec::new(),
                artifact: None,
                #[cfg(feature = "windows")]
                window: None,
            }],
            registry: std::sync::Arc::new(crate::service::registry::Registry::new()),
            inspector: None,
        }
    }

    #[test]
    fn test_inconsistent_data_dir_returns_error() {
        let dir = TempDir::new().unwrap();
        // Write raft-log.redb but NOT bootstrap.lock.
        std::fs::write(dir.path().join(RAFT_LOG_DB_FILE), b"fake").unwrap();

        let result = validate_data_dir(dir.path(), 1);
        assert!(result.is_err(), "expected error for missing bootstrap.lock");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains(BOOTSTRAP_LOCK_FILE),
            "error should mention bootstrap.lock: {msg}"
        );
    }

    #[test]
    fn test_consistent_data_dir_accepted() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(RAFT_LOG_DB_FILE), b"fake").unwrap();
        std::fs::write(dir.path().join(BOOTSTRAP_LOCK_FILE), b"bootstrapped").unwrap();
        validate_data_dir(dir.path(), 1).expect("consistent dir should be accepted");
    }

    #[test]
    fn test_empty_data_dir_accepted() {
        let dir = TempDir::new().unwrap();
        validate_data_dir(dir.path(), 1).expect("empty dir should be accepted");
    }

    #[test]
    fn test_node_id_mismatch_returns_error() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(NODE_ID_FILE), b"42").unwrap();

        let result = validate_data_dir(dir.path(), 1);
        assert!(result.is_err(), "expected error for node-id mismatch");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("42"),
            "error should mention stored id 42: {msg}"
        );
        assert!(msg.contains('1'), "error should mention config id 1: {msg}");
    }

    #[test]
    fn test_node_id_match_accepted() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(NODE_ID_FILE), b"7").unwrap();
        validate_data_dir(dir.path(), 7).expect("matching node-id should be accepted");
    }

    #[test]
    fn test_write_node_id_file_creates_file_on_first_run() {
        let dir = TempDir::new().unwrap();
        assert!(!dir.path().join(NODE_ID_FILE).exists());

        write_node_id_file(dir.path(), 42).expect("first write should succeed");

        let path = dir.path().join(NODE_ID_FILE);
        assert!(path.exists(), "node-id file should exist after first write");
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            contents.trim(),
            "42",
            "node-id file should contain the node id"
        );
    }

    #[test]
    fn test_write_node_id_file_is_idempotent() {
        let dir = TempDir::new().unwrap();
        write_node_id_file(dir.path(), 7).unwrap();
        // Second call must not overwrite or error.
        write_node_id_file(dir.path(), 7).expect("second write should be idempotent");

        let contents = std::fs::read_to_string(dir.path().join(NODE_ID_FILE)).unwrap();
        assert_eq!(contents.trim(), "7");
    }

    #[test]
    fn test_validate_data_dir_detects_mismatch_after_write() {
        let dir = TempDir::new().unwrap();
        // Simulate a successful bootstrap: write node-id for node 1.
        write_node_id_file(dir.path(), 1).unwrap();

        // A second run with a *different* node.id should fail.
        let err = validate_data_dir(dir.path(), 99).unwrap_err();
        assert!(
            err.to_string().contains("99"),
            "error should mention the new id 99: {err}"
        );
        assert!(
            err.to_string().contains('1'),
            "error should mention the stored id 1: {err}"
        );
    }

    /// The node directory a bootstrapped node leaves behind is exactly
    /// `bootstrap.lock`, `raft-log.redb` and `node-id`: no application store
    /// lives on local disk.
    #[tokio::test]
    async fn test_bootstrap_creates_lock_file() {
        let dir = TempDir::new().unwrap();
        let addr = free_addr();

        let (handle, _task) = ArrowRaftDriver::start(
            single_node_driver_config(addr),
            dir.path().join(RAFT_LOG_DB_FILE),
        )
        .await
        .unwrap();

        let bootstrap_lock = dir.path().join(BOOTSTRAP_LOCK_FILE);
        let cluster = single_peer_cluster(addr);

        bootstrap_cluster(&handle, &cluster, 1, &addr.to_string(), &bootstrap_lock)
            .await
            .unwrap();
        write_node_id_file(dir.path(), 1).unwrap();

        assert!(
            bootstrap_lock.exists(),
            "bootstrap.lock should have been created"
        );

        handle.shutdown().await;

        let mut entries: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        entries.sort();
        assert_eq!(
            entries,
            vec![
                BOOTSTRAP_LOCK_FILE.to_string(),
                NODE_ID_FILE.to_string(),
                RAFT_LOG_DB_FILE.to_string(),
            ],
            "the node directory holds only raft state and identity"
        );
    }

    /// The one-second recorder must write all three Raft gauges from the driver
    /// handle, with the bootstrapped node as the leader.
    ///
    /// `pcs_raft_leader_id` is `-1` for an unknown leader, so asserting `1`
    /// proves the recorder read a settled `RaftMetrics` rather than a default.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn test_spawn_raft_gauges_records_all_three() {
        use prometheus::TextEncoder;

        let dir = TempDir::new().unwrap();
        let addr = free_addr();

        let (handle, _task) = ArrowRaftDriver::start(
            single_node_driver_config(addr),
            dir.path().join(RAFT_LOG_DB_FILE),
        )
        .await
        .unwrap();

        let cluster = single_peer_cluster(addr);
        bootstrap_cluster(
            &handle,
            &cluster,
            1,
            &addr.to_string(),
            &dir.path().join(BOOTSTRAP_LOCK_FILE),
        )
        .await
        .unwrap();
        wait_for_raft_settled(&handle, cluster.election_timeout_ms)
            .await
            .unwrap();

        let cancel = CancellationToken::new();
        let gauges = spawn_raft_gauges(handle.clone(), cancel.child_token());

        // A freshly bootstrapped node passes `wait_for_raft_settled` as a
        // follower, before it wins its first election, so the first tick
        // legitimately sees no leader and records -1. Polling proves the
        // recorder keeps publishing rather than firing once.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut text = String::new();
        let mut leader = None;
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
            text = TextEncoder::new()
                .encode_to_string(&crate::metrics::test_registry().gather())
                .expect("encode prometheus text");
            leader = gauge_value(&text, "pcs_raft_leader_id");
            if leader == Some(1.0) {
                break;
            }
        }
        cancel.cancel();
        let _ = gauges.await;

        for series in [
            "pcs_raft_commit_index",
            "pcs_raft_term",
            "pcs_raft_leader_id",
        ] {
            assert!(
                text.contains(series),
                "{series} should have been recorded:\n{text}"
            );
        }
        assert_eq!(
            leader,
            Some(1.0),
            "the single bootstrapped node must become the recorded leader:\n{text}"
        );
        assert!(
            gauge_value(&text, "pcs_raft_term").is_some_and(|t| t >= 1.0),
            "a leader implies a term of at least 1:\n{text}"
        );

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_standalone_mode_rejected() {
        let dir = TempDir::new().unwrap();
        let config = ServiceConfig {
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: dir.path().to_path_buf(),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![WorkflowSpec {
                id: "w".to_string(),
                name: None,
                transformers: Vec::new(),
                sources: Vec::new(),
                #[cfg(feature = "wasm")]
                wasm: Vec::new(),
                #[cfg(feature = "plugin")]
                plugin: Vec::new(),
                sinks: Vec::new(),
                links: Vec::new(),
            }],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: HashMap::new(),
        };

        let cancel = CancellationToken::new();
        let result = run_cluster(empty_built_service(), &config, cancel).await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("standalone"),
            "error should mention standalone: {msg}"
        );
    }

    /// A cluster config with no `store` block must be refused rather than fall
    /// back to a local store. `validate` catches it at load time; this covers
    /// the runner's own guard for a config built in code.
    #[tokio::test]
    async fn test_cluster_without_store_is_refused() {
        let dir = TempDir::new().unwrap();
        let config = storeless_cluster_config(free_addr(), dir.path().to_path_buf());

        config
            .validate()
            .expect_err("validate must reject cluster mode without a store");

        let result = run_cluster(empty_built_service(), &config, CancellationToken::new()).await;
        let err = result.expect_err("run_cluster must refuse a storeless cluster config");
        let msg = err.to_string();
        assert!(
            msg.contains("tikv"),
            "error should name the required TiKV store: {msg}"
        );
    }
}
