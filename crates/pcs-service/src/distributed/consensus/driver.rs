//! Arrow-IPC Raft node driver on tikv/raft-rs.
//!
//! [`ArrowRaftDriver`] manages a [`RawNode`](raft::RawNode) backed by
//! [`RaftRedbLogStore`]. It:
//!
//! 1. Seeds the static membership (conf state) into the log store on first boot.
//! 2. Receives [`ConsensusCommand`](crate::distributed::consensus::ConsensusCommand)
//!    proposals, proposing them when leader and forwarding them to the leader
//!    otherwise.
//! 3. Drives the Ready cycle (tick, step, persist, apply, advance) exactly like
//!    raft's canonical `five_mem_node` example.
//! 4. Returns the [`ConsensusResponse`](crate::distributed::consensus::ConsensusResponse)
//!    via a oneshot reply channel.
//!
//! Membership is static: the peer list is seeded once and never changes, so
//! there are no conf-change proposals and no `initialize` step.

#[cfg(feature = "distributed-raft")]
pub(crate) mod raft_impl {
    use std::collections::{HashMap, VecDeque};
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    use raft::eraftpb::{Message, MessageType, Snapshot};
    use raft::{Config as RaftConfig, RawNode, SnapshotStatus, StateRole};
    use tokio::sync::{mpsc, oneshot};

    use crate::PcsError;
    use crate::PcsResult;
    use crate::distributed::consensus::snapshot::raft_impl::{
        build_snapshot_bytes, install_snapshot_bytes,
    };
    use crate::distributed::consensus::storage::raft_impl::{
        AppStateMachine, RaftRedbLogStore, validate_store_consistency,
    };
    use crate::distributed::consensus::transport::{RaftTcpServer, TransportHub};
    use crate::distributed::consensus::types::{ConsensusCommand, ConsensusResponse};

    /// Tick interval for the raft drive loop, in milliseconds.
    const TICK_INTERVAL_MS: u64 = 100;

    /// The raft node: a `RawNode` over the redb log store.
    pub type ArrowPCSRaft = RawNode<RaftRedbLogStore>;

    /// Mirror of [`StateRole`] for the metrics snapshot.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RaftNodeState {
        Follower,
        Candidate,
        Leader,
        PreCandidate,
    }

    impl From<StateRole> for RaftNodeState {
        fn from(r: StateRole) -> Self {
            match r {
                StateRole::Follower => RaftNodeState::Follower,
                StateRole::Candidate => RaftNodeState::Candidate,
                StateRole::Leader => RaftNodeState::Leader,
                StateRole::PreCandidate => RaftNodeState::PreCandidate,
            }
        }
    }

    /// A cheap, sync-readable snapshot of the node's consensus state.
    #[derive(Debug, Clone)]
    pub struct ArrowRaftMetrics {
        pub state: RaftNodeState,
        pub term: u64,
        pub leader_id: Option<u64>,
        pub applied_index: u64,
        pub last_log_index: u64,
    }

    /// Configuration for [`ArrowRaftDriver`].
    #[derive(Debug, Clone)]
    pub struct ArrowRaftDriverConfig {
        pub node_id: u64,
        pub listen_addr: SocketAddr,
        /// Peer addresses as `"host:port"` strings. These may be IP literals or
        /// hostnames resolved lazily at connection time, such as Docker service names.
        pub peers: HashMap<u64, String>,
        pub heartbeat_interval_ms: u64,
        pub election_timeout_ms: u64,
        /// Build and compact a snapshot every this many applied entries.
        pub snapshot_log_interval: u64,
    }

    impl Default for ArrowRaftDriverConfig {
        fn default() -> Self {
            Self {
                node_id: 1,
                listen_addr: "127.0.0.1:7101".parse().unwrap(),
                peers: HashMap::new(),
                heartbeat_interval_ms: 50,
                election_timeout_ms: 300,
                snapshot_log_interval: 10_000,
            }
        }
    }

    /// Handle for submitting proposals and requesting shutdown.
    #[derive(Clone)]
    pub struct ArrowRaftDriverHandle {
        proposal_tx: mpsc::Sender<(
            ConsensusCommand,
            oneshot::Sender<PcsResult<ConsensusResponse>>,
        )>,
        shutdown_tx: Arc<tokio::sync::Mutex<Option<oneshot::Sender<()>>>>,
        transport_tx: mpsc::Sender<Message>,
        metrics: Arc<RwLock<ArrowRaftMetrics>>,
        /// The redb database shared with the state machine.
        ///
        /// Used by `RedbSharedStore::multi_node` to open read-only queries
        /// against the same file that the state machine writes to.
        app_db: Arc<Mutex<redb::Database>>,
    }

    impl ArrowRaftDriverHandle {
        pub async fn propose(&self, cmd: ConsensusCommand) -> PcsResult<ConsensusResponse> {
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
        pub fn app_db(&self) -> &Arc<Mutex<redb::Database>> {
            &self.app_db
        }

        /// Return the latest Raft metrics snapshot.
        pub fn metrics(&self) -> ArrowRaftMetrics {
            self.metrics.read().expect("metrics lock poisoned").clone()
        }

        /// Spawn the TCP server that accepts inbound Raft RPCs from cluster peers.
        ///
        /// Call this once per node after `ArrowRaftDriver::start` in multi-node mode.
        /// Without it, other nodes cannot deliver heartbeats, votes, or log entries
        /// here, and the cluster fails to elect a leader.
        ///
        /// Returns a `JoinHandle` for the server task. The server runs until
        /// the process exits.
        pub fn spawn_tcp_server(
            &self,
            listen_addr: std::net::SocketAddr,
        ) -> tokio::task::JoinHandle<std::io::Result<()>> {
            RaftTcpServer::new(
                self.transport_tx.clone(),
                self.proposal_tx.clone(),
                listen_addr,
            )
            .spawn()
        }
    }

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

            let log_store = RaftRedbLogStore::open(&log_db_path)?;
            let app_db = Arc::new(Mutex::new(
                redb::Database::create(&app_db_path)
                    .map_err(|e| PcsError::store(format!("open app_db: {e}")))?,
            ));
            let app_sm = AppStateMachine::open(Arc::clone(&app_db))
                .map_err(|e| PcsError::store(format!("open state machine: {e}")))?;

            // Both halves of the node directory are open; refuse to start if the
            // state machine is behind what the log store already purged. Without
            // this a node restored from mismatched backups would silently
            // diverge instead of failing loudly.
            validate_store_consistency(
                log_store.first_index()?,
                app_sm.last_applied().map(|(_, i)| i),
            )?;

            // Static membership: seed the conf state on first boot. The
            // operational model already requires editing the peers list and
            // restarting every node, so membership never changes at runtime.
            // The node itself is always a voter even when the configured peer
            // list omits it (e.g. a test harness building per-node maps).
            let conf_state = log_store.read_conf_state()?;
            if conf_state.voters.is_empty() {
                let mut voters: Vec<u64> = config.peers.keys().copied().collect();
                if !voters.contains(&config.node_id) {
                    voters.push(config.node_id);
                }
                voters.sort_unstable();
                log_store.persist_conf_state(&raft::eraftpb::ConfState {
                    voters,
                    learners: Vec::new(),
                    ..Default::default()
                })?;
            }

            let ticks_from_ms = |ms: u64| (ms / TICK_INTERVAL_MS).max(1) as usize;
            let election_tick = ticks_from_ms(config.election_timeout_ms);
            let heartbeat_tick = ticks_from_ms(config.heartbeat_interval_ms);
            let raft_config = RaftConfig {
                id: config.node_id,
                election_tick,
                heartbeat_tick,
                // Deterministic elections: the randomization window is one tick.
                min_election_tick: election_tick,
                max_election_tick: election_tick + 1,
                applied: app_sm.last_applied().map(|(_, i)| i).unwrap_or(0),
                ..Default::default()
            };
            raft_config
                .validate()
                .map_err(|e| PcsError::configuration(format!("raft config: {e}")))?;

            let raw_node = RawNode::with_default_logger(&raft_config, log_store.clone())
                .map_err(|e| PcsError::configuration(format!("RawNode::new: {e}")))?;
            let raw_node = Arc::new(Mutex::new(raw_node));

            let (proposal_tx, proposal_rx) = mpsc::channel::<(
                ConsensusCommand,
                oneshot::Sender<PcsResult<ConsensusResponse>>,
            )>(128);
            let (transport_tx, transport_rx) = mpsc::channel::<Message>(1024);
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

            let metrics = Arc::new(RwLock::new(ArrowRaftMetrics {
                state: RaftNodeState::Follower,
                term: 0,
                leader_id: None,
                applied_index: app_sm.last_applied().map(|(_, i)| i).unwrap_or(0),
                last_log_index: log_store.last_index()?,
            }));

            let handle = ArrowRaftDriverHandle {
                proposal_tx: proposal_tx.clone(),
                shutdown_tx: Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx))),
                transport_tx,
                metrics: Arc::clone(&metrics),
                app_db,
            };

            let hub = TransportHub::new(&config.peers);
            let run = RunState {
                raw_node,
                log_store,
                app_sm,
                hub,
                pending: Arc::new(Mutex::new(VecDeque::new())),
                metrics,
                peers: config.peers,
                node_id: config.node_id,
                snapshot_log_interval: config.snapshot_log_interval,
                last_snapshot_index: Arc::new(AtomicU64::new(0)),
            };

            let join =
                tokio::spawn(
                    async move { run.drive(proposal_rx, transport_rx, shutdown_rx).await },
                );

            Ok((handle, join))
        }
    }

    struct RunState {
        raw_node: Arc<Mutex<ArrowPCSRaft>>,
        log_store: RaftRedbLogStore,
        app_sm: AppStateMachine,
        hub: TransportHub,
        pending: Arc<Mutex<VecDeque<oneshot::Sender<PcsResult<ConsensusResponse>>>>>,
        metrics: Arc<RwLock<ArrowRaftMetrics>>,
        peers: HashMap<u64, String>,
        node_id: u64,
        snapshot_log_interval: u64,
        last_snapshot_index: Arc<AtomicU64>,
    }

    impl RunState {
        async fn drive(
            mut self,
            mut proposal_rx: mpsc::Receiver<(
                ConsensusCommand,
                oneshot::Sender<PcsResult<ConsensusResponse>>,
            )>,
            mut transport_rx: mpsc::Receiver<Message>,
            mut shutdown_rx: oneshot::Receiver<()>,
        ) -> PcsResult<()> {
            let mut ticker = tokio::time::interval(Duration::from_millis(TICK_INTERVAL_MS));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick fires immediately; skip it so a fresh node starts
            // with a full tick interval before any election activity.
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let run = self.clone_shared();
                        tokio::task::spawn_blocking(move || run.tick_once()).await
                            .map_err(|e| PcsError::generic(format!("tick task panicked: {e}")))??;
                    }
                    msg_opt = transport_rx.recv() => {
                        let Some(msg) = msg_opt else { break; };
                        let run = self.clone_shared();
                        tokio::task::spawn_blocking(move || run.step(msg)).await
                            .map_err(|e| PcsError::generic(format!("step task panicked: {e}")))??;
                    }
                    proposal_opt = proposal_rx.recv() => {
                        match proposal_opt {
                            Some((cmd, reply_tx)) => {
                                self.handle_proposal(cmd, reply_tx).await;
                            }
                            None => break,
                        }
                    }
                    _ = &mut shutdown_rx => break,
                }
            }

            // Let raft flush any final ready state before the storage closes.
            {
                let run = self.clone_shared();
                tokio::task::spawn_blocking(move || run.drain_ready())
                    .await
                    .map_err(|e| PcsError::generic(format!("drain task panicked: {e}")))??;
            }
            Ok(())
        }

        fn clone_shared(&self) -> SharedRun {
            SharedRun {
                raw_node: Arc::clone(&self.raw_node),
                log_store: self.log_store.clone(),
                app_sm: Arc::new(self.app_sm.clone()),
                hub: self.hub.clone(),
                pending: Arc::clone(&self.pending),
                metrics: Arc::clone(&self.metrics),
                snapshot_log_interval: self.snapshot_log_interval,
                last_snapshot_index: Arc::clone(&self.last_snapshot_index),
            }
        }

        async fn handle_proposal(
            &mut self,
            cmd: ConsensusCommand,
            reply_tx: oneshot::Sender<PcsResult<ConsensusResponse>>,
        ) {
            // Propose synchronously (cheap: it only appends to the in-memory
            // log); the response arrives later through the committed-entries
            // path when this node is the leader.
            let data = match postcard::to_allocvec(&cmd) {
                Ok(d) => d,
                Err(e) => {
                    let _ = reply_tx.send(Err(PcsError::generic(format!("encode proposal: {e}"))));
                    return;
                }
            };
            let leader_id = {
                let mut rn = self.raw_node.lock().expect("raw_node lock poisoned");
                let leader = rn.status().ss.leader_id;
                match rn.propose(Vec::new(), data) {
                    Ok(()) => {
                        self.pending
                            .lock()
                            .expect("pending lock poisoned")
                            .push_back(reply_tx);
                        return;
                    }
                    Err(_) => leader,
                }
            };

            // Not the leader: forward to the current leader if known.
            let leader_addr = if leader_id != 0 && leader_id != self.node_id {
                self.peers.get(&leader_id).cloned()
            } else {
                None
            };
            let result = match leader_addr {
                Some(addr) => {
                    use crate::distributed::consensus::transport::forward_proposal;
                    forward_proposal(&addr, cmd).await
                }
                None => {
                    // No leader elected yet; let the caller back off and retry.
                    Ok(ConsensusResponse::NoBatchAvailable)
                }
            };
            let _ = reply_tx.send(result);
        }
    }

    /// Everything `drain_ready` needs, cloneable into `spawn_blocking`.
    #[derive(Clone)]
    struct SharedRun {
        raw_node: Arc<Mutex<ArrowPCSRaft>>,
        log_store: RaftRedbLogStore,
        app_sm: Arc<AppStateMachine>,
        hub: TransportHub,
        pending: Arc<Mutex<VecDeque<oneshot::Sender<PcsResult<ConsensusResponse>>>>>,
        metrics: Arc<RwLock<ArrowRaftMetrics>>,
        snapshot_log_interval: u64,
        last_snapshot_index: Arc<AtomicU64>,
    }

    impl SharedRun {
        fn tick_once(&self) -> PcsResult<()> {
            let mut rn = self.raw_node.lock().expect("raw_node lock poisoned");
            rn.tick();
            self.drain_ready_after(&mut rn)
        }

        /// One final Ready drain at shutdown.
        fn drain_ready(&self) -> PcsResult<()> {
            let mut rn = self.raw_node.lock().expect("raw_node lock poisoned");
            self.drain_ready_after(&mut rn)
        }

        fn step(&self, msg: Message) -> PcsResult<()> {
            let mut rn = self.raw_node.lock().expect("raw_node lock poisoned");
            if let Err(e) = rn.step(msg) {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %e, "raft step failed");
                #[cfg(not(feature = "tracing"))]
                let _ = e;
            }
            self.drain_ready_after(&mut rn)
        }

        /// One full Ready cycle, mirroring `five_mem_node`: ship messages,
        /// apply snapshots, apply committed entries, persist entries and hard
        /// state, then advance. The message sends block the blocking thread on
        /// the async transport (acceptable: raft traffic is low-rate).
        fn drain_ready_after(&self, rn: &mut ArrowPCSRaft) -> PcsResult<()> {
            while rn.has_ready() {
                let mut rd = rn.ready();

                // 1. Ship outbound messages.
                self.ship_messages(rn, rd.take_messages());

                // 2. Persist and install a snapshot this node received.
                if *rd.snapshot() != Snapshot::default() {
                    let snap = rd.snapshot().clone();
                    self.install_snapshot(&snap)?;
                }

                // 3. Apply committed entries.
                let committed = rd.take_committed_entries();
                if !committed.is_empty() {
                    self.apply_committed(&committed)?;
                }

                // 4. Persist entries and hard state.
                if !rd.entries().is_empty() {
                    self.log_store.append_entries(rd.entries())?;
                }
                if let Some(hs) = rd.hs() {
                    self.log_store.persist_hard_state(hs)?;
                }

                // 5. Ship persisted messages.
                self.ship_messages(rn, rd.take_persisted_messages());

                // 6. Advance, then fold in the light ready.
                let mut light = rn.advance(rd);
                self.ship_messages(rn, light.take_messages());
                let light_committed = light.take_committed_entries();
                if !light_committed.is_empty() {
                    self.apply_committed(&light_committed)?;
                }
                let applied = self.app_sm.last_applied().map(|(_, i)| i).unwrap_or(0);
                rn.advance_apply_to(applied);
            }

            // Periodic snapshot cadence: compact the log every
            // `snapshot_log_interval` applied entries.
            self.maybe_snapshot();
            self.update_metrics(rn);
            Ok(())
        }

        fn ship_messages(&self, rn: &mut ArrowPCSRaft, messages: Vec<Message>) {
            let handle = tokio::runtime::Handle::current();
            for msg in messages {
                let to = msg.to;
                let is_snapshot = msg.get_msg_type() == MessageType::MsgSnapshot;
                let sent = handle.block_on(self.hub.send_message(to, &msg));
                match sent {
                    Ok(()) => {
                        if is_snapshot {
                            rn.report_snapshot(to, SnapshotStatus::Finish);
                        }
                    }
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            peer = to,
                            error = %_e,
                            "raft message send failed; reporting peer unreachable"
                        );
                        rn.report_unreachable(to);
                        if is_snapshot {
                            rn.report_snapshot(to, SnapshotStatus::Failure);
                        }
                    }
                }
            }
        }

        /// Persist a received snapshot into the log store (one fsync) and
        /// install its data into the app database, advancing the watermark.
        fn install_snapshot(&self, snap: &Snapshot) -> PcsResult<()> {
            let meta = snap.metadata.clone().unwrap_or_default();
            self.log_store.compact_to(meta.index, snap)?;
            let applied_bytes = postcard::to_allocvec(&Some((meta.term, meta.index)))
                .map_err(|e| PcsError::store(format!("encode applied watermark: {e}")))?;
            let membership_bytes = postcard::to_allocvec(&Vec::<u64>::new())
                .map_err(|e| PcsError::store(format!("encode membership placeholder: {e}")))?;
            let db = self.app_sm.db.clone();
            let db = db
                .lock()
                .map_err(|_| PcsError::store("app db mutex poisoned"))?;
            install_snapshot_bytes(&db, &snap.data, Some((&applied_bytes, &membership_bytes)))
                .map_err(|e| PcsError::store(format!("install snapshot: {e}")))?;
            self.app_sm.set_last_applied(meta.term, meta.index);
            Ok(())
        }

        /// Apply committed entries, persist the watermark, and complete any
        /// pending proposals FIFO (data entries only; blank election entries
        /// carry no client).
        fn apply_committed(&self, entries: &[raft::eraftpb::Entry]) -> PcsResult<()> {
            let responses = self.app_sm.apply_batch(entries)?;
            self.app_sm.persist_applied()?;
            let mut pending = self.pending.lock().expect("pending lock poisoned");
            for (_index, response) in responses {
                if let Some(tx) = pending.pop_front() {
                    let _ = tx.send(Ok(response));
                }
            }
            Ok(())
        }

        fn maybe_snapshot(&self) {
            let Some(applied) = self.app_sm.last_applied() else {
                return;
            };
            let (term, index) = applied;
            if index.saturating_sub(self.last_snapshot_index.load(Ordering::Relaxed))
                < self.snapshot_log_interval
            {
                return;
            }
            let db = self.app_sm.db.clone();
            let db = match db.lock() {
                Ok(db) => db,
                Err(_) => return,
            };
            let payload = match build_snapshot_bytes(&db) {
                Ok(p) => p,
                Err(_e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %_e, "snapshot build failed");
                    return;
                }
            };
            drop(db);
            let conf = self.log_store.read_conf_state();
            let conf = conf.map(|c| raft::eraftpb::ConfState {
                voters: c.voters,
                learners: c.learners,
                ..Default::default()
            });
            let conf_state = conf.unwrap_or_default();
            let snap = Snapshot {
                data: payload,
                metadata: Some(raft::eraftpb::SnapshotMetadata {
                    index,
                    term,
                    conf_state: Some(conf_state),
                }),
            };
            if let Err(e) = self.log_store.compact_to(index, &snap) {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %e, "snapshot compaction failed");
                return;
            }
            self.last_snapshot_index.store(index, Ordering::Relaxed);
        }

        fn update_metrics(&self, rn: &ArrowPCSRaft) {
            let st = rn.status();
            let last_log = self.log_store.last_index().unwrap_or(0);
            let mut m = self.metrics.write().expect("metrics lock poisoned");
            m.state = st.ss.raft_state.into();
            m.term = st.hs.term;
            m.leader_id = if st.ss.leader_id == 0 {
                None
            } else {
                Some(st.ss.leader_id)
            };
            m.applied_index = st.applied;
            m.last_log_index = last_log;
        }
    }

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
                election_timeout_ms: 200,
                snapshot_log_interval: 10_000,
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
            assert!(result.is_ok(), "driver task should exit cleanly");
        }

        #[tokio::test]
        async fn test_single_node_propose_round_trip() {
            let dir = TempDir::new().unwrap();
            let addr = free_addr();
            let config = ArrowRaftDriverConfig {
                node_id: 1,
                listen_addr: addr,
                peers: HashMap::new(),
                heartbeat_interval_ms: 30,
                election_timeout_ms: 200,
                snapshot_log_interval: 10_000,
            };

            let (handle, task) = ArrowRaftDriver::start(
                config,
                dir.path().join("arrow_log.redb"),
                dir.path().join("arrow_app.redb"),
            )
            .await
            .unwrap();

            // Wait for the single-node cluster to elect itself leader.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let m = handle.metrics();
                if m.state == RaftNodeState::Leader {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "single node should become leader: {:?}",
                    m
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // Registering a master batch round-trips through raft.
            let response = handle
                .propose(ConsensusCommand::RegisterMasterBatch {
                    batch_id: 1,
                    component: "test".to_string(),
                    schema_id: 1,
                    ipc_bytes: vec![0u8; 32],
                    total_rows: 10,
                    now_at_propose: 0,
                })
                .await
                .expect("proposal succeeds");
            assert!(matches!(
                response,
                ConsensusResponse::MasterBatchRegistered { batch_id: 1 }
            ));

            handle.shutdown().await;
            let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
        }
    }
}

#[cfg(feature = "distributed-raft")]
pub use raft_impl::{
    ArrowPCSRaft, ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle, ArrowRaftMetrics,
    RaftNodeState,
};
