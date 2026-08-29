//! Raft node driver on tikv/raft-rs.
//!
//! [`ArrowRaftDriver`] manages a [`RawNode`](raft::RawNode) backed by
//! [`RaftRedbLogStore`]. It:
//!
//! 1. Seeds the static membership (conf state) into the log store on first boot.
//! 2. Drives the Ready cycle (tick, step, persist, advance) exactly like
//!    raft's canonical `five_mem_node` example.
//! 3. Compacts the log on a `snapshot_log_interval` cadence.
//! 4. Publishes a sync-readable [`ArrowRaftMetrics`] snapshot.
//!
//! Membership is static: the peer list is seeded once and never changes, so
//! there are no conf-change proposals and no `initialize` step.
//!
//! The driver holds no application state. Cluster application data lives in
//! TiKV, so nothing is ever proposed into this log: its entries are raft's own
//! per-term no-ops, committed entries are acknowledged and discarded, and a
//! snapshot carries raft metadata with an empty payload.

#[cfg(feature = "distributed-raft")]
pub(crate) mod raft_impl {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    use raft::eraftpb::{Message, MessageType, Snapshot};
    use raft::{Config as RaftConfig, RawNode, SnapshotStatus, StateRole, Storage as _};
    use tokio::sync::{mpsc, oneshot};

    use crate::PcsError;
    use crate::PcsResult;
    use crate::distributed::consensus::storage::raft_impl::RaftRedbLogStore;
    use crate::distributed::consensus::transport::{RaftTcpServer, TransportHub};

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
        /// Compact the log every this many applied entries.
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

    /// Handle for reading consensus metrics and requesting shutdown.
    #[derive(Clone)]
    pub struct ArrowRaftDriverHandle {
        shutdown_tx: Arc<tokio::sync::Mutex<Option<oneshot::Sender<()>>>>,
        transport_tx: mpsc::Sender<Message>,
        metrics: Arc<RwLock<ArrowRaftMetrics>>,
    }

    impl ArrowRaftDriverHandle {
        /// Signal the drive loop to stop. Idempotent: later calls are no-ops.
        pub async fn shutdown(&self) {
            let mut guard = self.shutdown_tx.lock().await;
            if let Some(tx) = guard.take() {
                let _ = tx.send(());
            }
        }

        /// Return the latest Raft metrics snapshot.
        pub fn metrics(&self) -> ArrowRaftMetrics {
            self.metrics.read().expect("metrics lock poisoned").clone()
        }

        /// Spawn the TCP server that accepts inbound Raft messages from cluster peers.
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
            RaftTcpServer::new(self.transport_tx.clone(), listen_addr).spawn()
        }
    }

    pub struct ArrowRaftDriver;

    impl ArrowRaftDriver {
        /// Open the log store at `log_db_path`, seed static membership, and spawn
        /// the drive loop.
        ///
        /// # Errors
        ///
        /// Returns [`PcsError::Store`] when the log-only redb file cannot be
        /// opened, and [`PcsError::Configuration`] when the derived raft config
        /// or the `RawNode` construction is rejected.
        pub async fn start(
            config: ArrowRaftDriverConfig,
            log_db_path: impl Into<PathBuf>,
        ) -> PcsResult<(
            ArrowRaftDriverHandle,
            tokio::task::JoinHandle<PcsResult<()>>,
        )> {
            let log_db_path = log_db_path.into();
            let log_store = RaftRedbLogStore::open(&log_db_path)?;

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
                // Nothing is applied out of band: raft starts applied at the
                // log's first index minus one and the drive loop advances it.
                applied: 0,
                ..Default::default()
            };
            raft_config
                .validate()
                .map_err(|e| PcsError::configuration(format!("raft config: {e}")))?;

            let raw_node = RawNode::with_default_logger(&raft_config, log_store.clone())
                .map_err(|e| PcsError::configuration(format!("RawNode::new: {e}")))?;
            let raw_node = Arc::new(Mutex::new(raw_node));

            let (transport_tx, transport_rx) = mpsc::channel::<Message>(1024);
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

            let last_log_index = log_store.last_index()?;
            // The log's existing base: a restart must not immediately re-compact
            // to the snapshot index it already holds.
            let compaction_base = log_store.first_index()?.saturating_sub(1);
            let metrics = Arc::new(RwLock::new(ArrowRaftMetrics {
                state: RaftNodeState::Follower,
                term: 0,
                leader_id: None,
                applied_index: 0,
                last_log_index,
            }));

            let handle = ArrowRaftDriverHandle {
                shutdown_tx: Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx))),
                transport_tx,
                metrics: Arc::clone(&metrics),
            };

            let run = SharedRun {
                raw_node,
                log_store,
                hub: TransportHub::new(&config.peers),
                metrics,
                snapshot_log_interval: config.snapshot_log_interval,
                last_snapshot_index: Arc::new(AtomicU64::new(compaction_base)),
            };

            let join = tokio::spawn(async move { run.drive(transport_rx, shutdown_rx).await });

            Ok((handle, join))
        }
    }

    /// Everything the drive loop needs, cloneable into `spawn_blocking`.
    #[derive(Clone)]
    struct SharedRun {
        raw_node: Arc<Mutex<ArrowPCSRaft>>,
        log_store: RaftRedbLogStore,
        hub: TransportHub,
        metrics: Arc<RwLock<ArrowRaftMetrics>>,
        snapshot_log_interval: u64,
        last_snapshot_index: Arc<AtomicU64>,
    }

    impl SharedRun {
        async fn drive(
            self,
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
                        let run = self.clone();
                        tokio::task::spawn_blocking(move || run.tick_once()).await
                            .map_err(|e| PcsError::generic(format!("tick task panicked: {e}")))??;
                    }
                    msg_opt = transport_rx.recv() => {
                        let Some(msg) = msg_opt else { break; };
                        let run = self.clone();
                        tokio::task::spawn_blocking(move || run.step(msg)).await
                            .map_err(|e| PcsError::generic(format!("step task panicked: {e}")))??;
                    }
                    _ = &mut shutdown_rx => break,
                }
            }

            // Let raft flush any final ready state before the storage closes.
            let run = self.clone();
            tokio::task::spawn_blocking(move || run.drain_ready())
                .await
                .map_err(|e| PcsError::generic(format!("drain task panicked: {e}")))??;
            Ok(())
        }

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
        /// persist a received snapshot, persist entries and hard state, then
        /// advance. Committed entries carry no application payload, so
        /// acknowledging them is `advance_apply`. The message sends block the
        /// blocking thread on the async transport (acceptable: raft traffic is
        /// low-rate).
        fn drain_ready_after(&self, rn: &mut ArrowPCSRaft) -> PcsResult<()> {
            while rn.has_ready() {
                let mut rd = rn.ready();

                // 1. Ship outbound messages.
                self.ship_messages(rn, rd.take_messages());

                // 2. Persist a snapshot this node received, compacting the log
                //    to its index. The payload is empty: there is no
                //    application state to install.
                if *rd.snapshot() != Snapshot::default() {
                    let snap = rd.snapshot().clone();
                    let index = snap.metadata.as_ref().map_or(0, |m| m.index);
                    self.log_store.compact_to(index, &snap)?;
                    self.last_snapshot_index.store(index, Ordering::Relaxed);
                }

                // 3. Persist entries and hard state.
                if !rd.entries().is_empty() {
                    self.log_store.append_entries(rd.entries())?;
                }
                if let Some(hs) = rd.hs() {
                    self.log_store.persist_hard_state(hs)?;
                }

                // 4. Ship persisted messages.
                self.ship_messages(rn, rd.take_persisted_messages());

                // 5. Advance, then fold in the light ready.
                let mut light = rn.advance(rd);
                self.ship_messages(rn, light.take_messages());
                rn.advance_apply();
            }

            // Periodic cadence: compact the log every `snapshot_log_interval`
            // applied entries so it does not grow without bound.
            self.maybe_compact(rn);
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

        /// Compact the log once `snapshot_log_interval` entries have been
        /// applied since the last compaction, writing a metadata-only snapshot
        /// as the new log base.
        fn maybe_compact(&self, rn: &ArrowPCSRaft) {
            let applied = rn.status().applied;
            if applied == 0
                || applied.saturating_sub(self.last_snapshot_index.load(Ordering::Relaxed))
                    < self.snapshot_log_interval
            {
                return;
            }
            let term = match self.log_store.term(applied) {
                Ok(t) => t,
                Err(_e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(index = applied, error = %_e, "log compaction: term lookup failed");
                    return;
                }
            };
            let conf_state = self.log_store.read_conf_state().unwrap_or_default();
            let snap = Snapshot {
                data: Vec::new(),
                metadata: Some(raft::eraftpb::SnapshotMetadata {
                    index: applied,
                    term,
                    conf_state: Some(conf_state),
                }),
            };
            if let Err(_e) = self.log_store.compact_to(applied, &snap) {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "log compaction failed");
                return;
            }
            self.last_snapshot_index.store(applied, Ordering::Relaxed);
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

        fn single_node_config(listen_addr: SocketAddr) -> ArrowRaftDriverConfig {
            ArrowRaftDriverConfig {
                node_id: 1,
                listen_addr,
                peers: HashMap::new(),
                heartbeat_interval_ms: 30,
                election_timeout_ms: 200,
                snapshot_log_interval: 10_000,
            }
        }

        /// Wait until `handle` reports the leader role, or fail the test.
        async fn await_leader(handle: &ArrowRaftDriverHandle) -> ArrowRaftMetrics {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let m = handle.metrics();
                if m.state == RaftNodeState::Leader {
                    return m;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "single node should become leader: {m:?}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        #[tokio::test]
        async fn test_arrow_driver_starts_and_shuts_down() {
            let dir = TempDir::new().unwrap();
            let config = single_node_config(free_addr());

            let (handle, task) = ArrowRaftDriver::start(config, dir.path().join("raft-log.redb"))
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(300)).await;
            handle.shutdown().await;
            let result = tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .expect("driver should stop within 3s");
            assert!(result.is_ok(), "driver task should exit cleanly");
        }

        /// A single-node cluster elects itself and commits raft's own per-term
        /// entry, so both the term and the applied index advance past zero
        /// without anything being proposed.
        #[tokio::test]
        async fn test_single_node_elects_and_applies_own_entry() {
            let dir = TempDir::new().unwrap();
            let config = single_node_config(free_addr());

            let (handle, task) = ArrowRaftDriver::start(config, dir.path().join("raft-log.redb"))
                .await
                .unwrap();

            let metrics = await_leader(&handle).await;
            assert_eq!(
                metrics.leader_id,
                Some(1),
                "the sole voter must report itself as leader: {metrics:?}"
            );
            assert!(
                metrics.term >= 1,
                "a leader implies a term of at least 1: {metrics:?}"
            );

            // raft appends one empty entry when it becomes leader; the drive
            // loop must acknowledge it.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let m = handle.metrics();
                if m.applied_index >= 1 {
                    assert!(
                        m.last_log_index >= m.applied_index,
                        "log must hold every applied entry: {m:?}"
                    );
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "applied index must advance past 0: {m:?}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            handle.shutdown().await;
            let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
        }

        /// Membership seeded on first boot survives a restart: the second
        /// `start` reads the persisted conf state instead of re-seeding, and
        /// the node resumes with the same voter set.
        #[tokio::test]
        async fn test_membership_persists_across_restart() {
            let dir = TempDir::new().unwrap();
            let log_path = dir.path().join("raft-log.redb");

            let mut config = single_node_config(free_addr());
            config.peers.insert(2, "127.0.0.1:1".to_string());
            config.peers.insert(3, "127.0.0.1:2".to_string());

            let (handle, task) = ArrowRaftDriver::start(config, &log_path).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            handle.shutdown().await;
            let _ = tokio::time::timeout(Duration::from_secs(3), task).await;

            let store = RaftRedbLogStore::open(&log_path).unwrap();
            assert_eq!(
                store.read_conf_state().unwrap().voters,
                vec![1, 2, 3],
                "the seeded voter set must be persisted"
            );
            drop(store);

            // A restart whose config lists no peers must not shrink the
            // persisted membership.
            let (handle, task) = ArrowRaftDriver::start(single_node_config(free_addr()), &log_path)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            handle.shutdown().await;
            let _ = tokio::time::timeout(Duration::from_secs(3), task).await;

            let store = RaftRedbLogStore::open(&log_path).unwrap();
            assert_eq!(
                store.read_conf_state().unwrap().voters,
                vec![1, 2, 3],
                "a restart must reuse the persisted voter set, not re-seed it"
            );
        }

        /// With a one-entry cadence the drive loop compacts the log as soon as
        /// the leader's own entry applies, leaving a metadata-only snapshot as
        /// the new log base.
        #[tokio::test]
        async fn test_log_compaction_writes_metadata_only_snapshot() {
            let dir = TempDir::new().unwrap();
            let log_path = dir.path().join("raft-log.redb");
            let mut config = single_node_config(free_addr());
            config.snapshot_log_interval = 1;

            let (handle, task) = ArrowRaftDriver::start(config, &log_path).await.unwrap();
            await_leader(&handle).await;

            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                if handle.metrics().applied_index >= 1 {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "applied index must advance before compaction can run"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // One more Ready cycle so `maybe_compact` observes the new applied index.
            tokio::time::sleep(Duration::from_millis(300)).await;

            handle.shutdown().await;
            let _ = tokio::time::timeout(Duration::from_secs(3), task).await;

            let store = RaftRedbLogStore::open(&log_path).unwrap();
            let snap = store
                .read_snapshot()
                .unwrap()
                .expect("compaction must persist a snapshot");
            let meta = snap.metadata.as_ref().expect("snapshot metadata");
            assert!(meta.index >= 1, "snapshot index must name a real entry");
            assert!(
                snap.data.is_empty(),
                "the PCS raft holds no application state, so the payload stays empty"
            );
            assert_eq!(
                meta.conf_state.as_ref().expect("conf state").voters,
                vec![1],
                "the snapshot must carry the current membership"
            );
            assert_eq!(
                store.first_index().unwrap(),
                meta.index + 1,
                "entries up to the snapshot index must be purged"
            );
        }
    }
}

#[cfg(feature = "distributed-raft")]
pub use raft_impl::{
    ArrowPCSRaft, ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle, ArrowRaftMetrics,
    RaftNodeState,
};
