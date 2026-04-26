//! Arrow-IPC Raft node driver.
//!
//! [`ArrowRaftDriver`] manages an openraft node parameterised with
//! [`PcsTypeConfig`](crate::distributed::consensus::types::PcsTypeConfig). It:
//!
//! 1. Initialises the Raft node (single-node cluster or provided peers).
//! 2. Receives [`ConsensusCommand`](crate::distributed::consensus::ConsensusCommand) proposals.
//! 3. Calls `Raft::client_write` directly with the command — no intermediate
//!    string encoding, because `D = ConsensusCommand` for `PcsTypeConfig`.
//! 4. Returns the [`ConsensusResponse`](crate::distributed::consensus::ConsensusResponse) via a oneshot reply channel.

#[cfg(feature = "distributed-raft")]
pub(crate) mod raft_impl {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;

    use openraft::BasicNode;
    use openraft::Config as RaftConfig;
    use openraft::Raft;
    use openraft::error::InitializeError;
    use tokio::sync::{mpsc, oneshot};

    use crate::PcsError;
    use crate::PcsResult;
    use crate::distributed::consensus::storage::raft_impl::{
        ArrowRedbLogStore, ArrowRedbStateMachine,
    };
    use crate::distributed::consensus::transport::TcpNetworkFactory;
    use crate::distributed::consensus::types::{
        ConsensusCommand as SmConsensusCommand, ConsensusResponse, PcsTypeConfig,
    };

    pub type ArrowPCSRaft = Raft<PcsTypeConfig, ArrowRedbStateMachine>;

    // ── Configuration ─────────────────────────────────────────────────────────

    /// Configuration for [`ArrowRaftDriver`].
    #[derive(Debug, Clone)]
    pub struct ArrowRaftDriverConfig {
        pub node_id: u64,
        pub listen_addr: SocketAddr,
        /// Peer addresses as `"host:port"` strings — may be IP literals or
        /// hostnames resolved lazily at connection time (e.g. Docker service
        /// names).  An empty map triggers single-node auto-initialisation.
        pub peers: HashMap<u64, String>,
        pub heartbeat_interval_ms: u64,
        pub election_timeout_min_ms: u64,
        pub election_timeout_max_ms: u64,
    }

    impl Default for ArrowRaftDriverConfig {
        fn default() -> Self {
            Self {
                node_id: 1,
                listen_addr: "127.0.0.1:7101".parse().unwrap(),
                peers: HashMap::new(),
                heartbeat_interval_ms: 50,
                election_timeout_min_ms: 150,
                election_timeout_max_ms: 300,
            }
        }
    }

    // ── Handle ────────────────────────────────────────────────────────────────

    /// Handle for submitting proposals and requesting shutdown.
    #[derive(Clone)]
    pub struct ArrowRaftDriverHandle {
        proposal_tx: mpsc::Sender<(
            SmConsensusCommand,
            oneshot::Sender<PcsResult<ConsensusResponse>>,
        )>,
        shutdown_tx: Arc<tokio::sync::Mutex<Option<oneshot::Sender<()>>>>,
        /// The underlying Raft instance — exposes `metrics()` and `initialize()`.
        raft: ArrowPCSRaft,
        /// The redb database shared with the Raft state machine.
        ///
        /// Used by `RedbSharedStore::multi_node` to open read-only queries
        /// against the same file that the state machine writes to.
        app_db: Arc<std::sync::Mutex<redb::Database>>,
        /// Peer addresses as `"host:port"` strings for leader forwarding.
        #[allow(dead_code)]
        peers: HashMap<u64, String>,
    }

    impl ArrowRaftDriverHandle {
        pub async fn propose(&self, cmd: SmConsensusCommand) -> PcsResult<ConsensusResponse> {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.proposal_tx
                .send((cmd, reply_tx))
                .await
                .map_err(|_| PcsError::generic("ArrowRaftDriver: proposal channel closed"))?;
            reply_rx
                .await
                .map_err(|_| PcsError::generic("ArrowRaftDriver: reply channel closed"))?
        }

        pub async fn shutdown(&self) {
            let mut guard = self.shutdown_tx.lock().await;
            if let Some(tx) = guard.take() {
                let _ = tx.send(());
            }
        }

        /// Return the redb database shared with the Raft state machine.
        pub fn app_db(&self) -> &Arc<std::sync::Mutex<redb::Database>> {
            &self.app_db
        }

        /// Return the latest Raft metrics snapshot.
        pub fn metrics(&self) -> openraft::RaftMetrics<PcsTypeConfig> {
            use openraft::async_runtime::WatchReceiver;
            self.raft.metrics().borrow_watched().clone()
        }

        /// Spawn the TCP server that accepts inbound Raft RPCs from cluster peers.
        ///
        /// Call this once per node after `ArrowRaftDriver::start` in multi-node
        /// mode. Without it, other nodes cannot deliver heartbeats, votes, or
        /// log entries to this node — the cluster will fail to elect a leader.
        ///
        /// Returns a `JoinHandle` for the server task. The server runs until
        /// the process exits.
        pub fn spawn_tcp_server(
            &self,
            listen_addr: std::net::SocketAddr,
        ) -> tokio::task::JoinHandle<std::io::Result<()>> {
            use crate::distributed::consensus::transport::RaftTcpServer;
            RaftTcpServer::new(self.raft.clone(), listen_addr).spawn()
        }

        /// Bootstrap the Raft cluster with the given initial membership.
        ///
        /// This is a no-op if the cluster is already initialised
        /// (`NotAllowed` error is swallowed).
        pub async fn initialize(
            &self,
            members: std::collections::BTreeMap<u64, BasicNode>,
        ) -> PcsResult<()> {
            match self.raft.initialize(members).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    if matches!(
                        e,
                        openraft::error::RaftError::APIError(InitializeError::NotAllowed(_))
                    ) {
                        Ok(()) // already initialised
                    } else {
                        Err(PcsError::configuration(format!("raft initialize: {e}")))
                    }
                }
            }
        }
    }

    // ── Driver ────────────────────────────────────────────────────────────────

    pub struct ArrowRaftDriver;

    impl ArrowRaftDriver {
        pub async fn start(
            config: ArrowRaftDriverConfig,
            log_db_path: impl Into<PathBuf>,
            app_db_path: impl Into<PathBuf>,
        ) -> PcsResult<(
            ArrowRaftDriverHandle,
            tokio::task::JoinHandle<PcsResult<()>>,
        )> {
            let log_db_path = log_db_path.into();
            let app_db_path = app_db_path.into();

            let raft_config = Arc::new(
                RaftConfig {
                    cluster_name: "arrow-pcs-cluster".into(),
                    heartbeat_interval: config.heartbeat_interval_ms,
                    election_timeout_min: config.election_timeout_min_ms,
                    election_timeout_max: config.election_timeout_max_ms,
                    ..Default::default()
                }
                .validate()
                .map_err(|e| PcsError::configuration(format!("openraft config: {e}")))?,
            );

            let log_store = ArrowRedbLogStore::open(&log_db_path)?;
            let app_db = Arc::new(std::sync::Mutex::new(
                redb::Database::create(&app_db_path)
                    .map_err(|e| PcsError::store(format!("open app_db: {e}")))?,
            ));
            let state_machine = ArrowRedbStateMachine::open(app_db.clone())
                .map_err(|e| PcsError::store(format!("open state machine: {e}")))?;

            let peers_basic: HashMap<u64, BasicNode> = config
                .peers
                .iter()
                .map(|(id, addr)| (*id, BasicNode { addr: addr.clone() }))
                .collect();
            let network = TcpNetworkFactory::from_basic_nodes(&peers_basic);

            let raft: ArrowPCSRaft = Raft::new(
                config.node_id,
                raft_config,
                network,
                log_store,
                state_machine,
            )
            .await
            .map_err(|e| PcsError::configuration(format!("Raft::new: {e}")))?;

            if config.peers.is_empty() {
                let mut members = std::collections::BTreeMap::new();
                members.insert(
                    config.node_id,
                    BasicNode {
                        addr: config.listen_addr.to_string(),
                    },
                );
                match raft.initialize(members).await {
                    Ok(()) => {}
                    Err(e) => {
                        if !matches!(
                            e,
                            openraft::error::RaftError::APIError(InitializeError::NotAllowed(_))
                        ) {
                            return Err(PcsError::configuration(format!("raft initialize: {e}")));
                        }
                    }
                }
            }

            // Extract peers after all config.peers borrows are done.
            let peers = config.peers;

            let (proposal_tx, proposal_rx) = mpsc::channel::<(
                SmConsensusCommand,
                oneshot::Sender<PcsResult<ConsensusResponse>>,
            )>(128);
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

            let handle = ArrowRaftDriverHandle {
                proposal_tx,
                shutdown_tx: Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx))),
                raft: raft.clone(),
                app_db,
                peers: peers.clone(),
            };

            let raft_clone = raft.clone();
            let join =
                tokio::spawn(
                    async move { run_loop(raft_clone, peers, proposal_rx, shutdown_rx).await },
                );

            Ok((handle, join))
        }
    }

    async fn run_loop(
        raft: ArrowPCSRaft,
        peers: HashMap<u64, String>,
        mut proposal_rx: mpsc::Receiver<(
            SmConsensusCommand,
            oneshot::Sender<PcsResult<ConsensusResponse>>,
        )>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> PcsResult<()> {
        loop {
            tokio::select! {
                proposal_opt = proposal_rx.recv() => {
                    match proposal_opt {
                        Some((cmd, reply_tx)) => {
                            let result = write_command(&raft, &peers, cmd).await;
                            let _ = reply_tx.send(result);
                        }
                        None => break,
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
        raft.shutdown()
            .await
            .map_err(|e| PcsError::generic(format!("raft shutdown: {e}")))?;
        Ok(())
    }

    async fn write_command(
        raft: &ArrowPCSRaft,
        peers: &HashMap<u64, String>,
        cmd: SmConsensusCommand,
    ) -> PcsResult<ConsensusResponse> {
        // `D = ConsensusCommand`, `R = ConsensusResponse` on `PcsTypeConfig`.
        // The state machine returns a `ConsensusResponse` via the responder.
        use openraft::error::{ClientWriteError, RaftError};
        match raft.client_write(cmd.clone()).await {
            Ok(r) => Ok(r.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                // This node is a follower; locate the leader and forward.
                // Prefer the address from our peers map (configured at startup)
                // since the ForwardToLeader node info may be incomplete.
                let leader_addr: Option<String> = fwd
                    .leader_id
                    .and_then(|id| peers.get(&id).cloned())
                    .or_else(|| fwd.leader_node.map(|n| n.addr.clone()));

                match leader_addr {
                    Some(addr) => {
                        use crate::distributed::consensus::transport::forward_proposal;
                        forward_proposal(&addr, cmd).await
                    }
                    None => {
                        // No leader elected yet — let the caller back off and retry.
                        Ok(ConsensusResponse::NoBatchAvailable)
                    }
                }
            }
            Err(e) => Err(PcsError::generic(format!("client_write: {e}"))),
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::time::Duration;
        use tempfile::TempDir;

        fn free_addr() -> SocketAddr {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        }

        #[tokio::test]
        async fn test_arrow_driver_starts_and_shuts_down() {
            let dir = TempDir::new().unwrap();
            let addr = free_addr();
            let config = ArrowRaftDriverConfig {
                node_id: 1,
                listen_addr: addr,
                peers: HashMap::new(),
                heartbeat_interval_ms: 30,
                election_timeout_min_ms: 100,
                election_timeout_max_ms: 200,
            };

            let (handle, task) = ArrowRaftDriver::start(
                config,
                dir.path().join("arrow_log.redb"),
                dir.path().join("arrow_app.redb"),
            )
            .await
            .unwrap();

            tokio::time::sleep(Duration::from_millis(300)).await;
            handle.shutdown().await;
            let result = tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .expect("driver should stop within 3s");
            assert!(result.is_ok());
        }
    }
}

#[cfg(feature = "distributed-raft")]
pub use raft_impl::{ArrowPCSRaft, ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle};
