//! Shared test harness for multi-node Raft chaos tests.
//!
//! ## Components
//!
//! - [`ToxiproxyContainer`] — wraps a Toxiproxy Docker container.
//! - [`ToxiproxyClient`] — thin wrapper over `toxiproxy_rust::client::Client`
//!   with helpers for `reset_peer` and `delete_toxic`.
//! - [`RaftClusterHarness`] — spins up N PCS Raft nodes with TCP links routed
//!   through per-edge Toxiproxy proxies.
//!
//! ## Usage
//!
//! ```rust,ignore
//! let harness = RaftClusterHarness::start(3).await.unwrap();
//! let leader = harness.await_leader().await.unwrap();
//! harness.propose_noop(leader).await.unwrap();
//! ```

#![cfg(feature = "distributed-raft")]
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use openraft::BasicNode;
use pcs_service::distributed::consensus::driver::{
    ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle,
};
use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use toxiproxy_rust::client::Client as ToxiClient;
use toxiproxy_rust::proxy::ProxyPack;

// ── Toxiproxy container ───────────────────────────────────────────────────────

/// Toxiproxy container with the HTTP API port (8474) and proxy ports 20001–20020 exposed.
pub struct ToxiproxyContainer {
    pub container: ContainerAsync<GenericImage>,
    pub api_port: u16,
    /// Host ports mapped from container proxy ports 20001–20020.
    pub proxy_host_ports: Vec<u16>,
}

impl ToxiproxyContainer {
    pub async fn start() -> anyhow::Result<Self> {
        let mut image = GenericImage::new("ghcr.io/shopify/toxiproxy", "2.9.0")
            .with_wait_for(WaitFor::message_on_stdout("API server started"))
            .with_exposed_port(8474_u16.tcp());

        for port in 20001_u16..=20020 {
            image = image.with_exposed_port(port.tcp());
        }

        let container = image.start().await?;
        let api_port = container.get_host_port_ipv4(8474_u16.tcp()).await?;

        let mut proxy_host_ports = Vec::with_capacity(20);
        for port in 20001_u16..=20020 {
            proxy_host_ports.push(container.get_host_port_ipv4(port.tcp()).await?);
        }

        Ok(Self {
            container,
            api_port,
            proxy_host_ports,
        })
    }

    /// Start a container, but return `None` (with a printed warning) if Docker
    /// is unavailable. Tests use this to soft-skip instead of hard-failing on
    /// developer machines / CI without Docker.
    pub async fn try_start() -> Option<Self> {
        match Self::start().await {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("SKIP: toxiproxy container unavailable: {e}");
                None
            }
        }
    }

    pub fn client(&self) -> ToxiproxyClient {
        ToxiproxyClient::new(self.api_port)
    }
}

// ── Toxiproxy client wrapper ──────────────────────────────────────────────────

/// Wraps `toxiproxy_rust::client::Client`, adding `reset_peer` and `delete_toxic`
/// which the upstream crate doesn't expose directly.
pub struct ToxiproxyClient {
    inner: ToxiClient,
    pub api_port: u16,
}

impl ToxiproxyClient {
    pub fn new(api_port: u16) -> Self {
        Self {
            inner: ToxiClient::new(format!("127.0.0.1:{api_port}")),
            api_port,
        }
    }

    /// Create a proxy that listens on `listen_port` (container-internal) and
    /// forwards to `upstream` (`host:port` string).
    pub fn create_proxy(&self, name: &str, upstream: &str, listen_port: u16) -> anyhow::Result<()> {
        let listen = format!("0.0.0.0:{listen_port}");
        self.inner
            .populate(vec![ProxyPack::new(name.into(), listen, upstream.into())])
            .map_err(|e| anyhow::anyhow!("create_proxy: {e}"))?;
        Ok(())
    }

    /// Delete a named proxy.
    pub fn delete_proxy(&self, name: &str) -> anyhow::Result<()> {
        self.inner
            .find_proxy(name)
            .map_err(|e| anyhow::anyhow!("find_proxy: {e}"))?
            .delete()
            .map_err(|e| anyhow::anyhow!("delete_proxy: {e}"))
    }

    /// Add a latency toxic (milliseconds).
    pub fn add_latency(&self, proxy: &str, ms: u64) -> anyhow::Result<()> {
        self.inner
            .find_proxy(proxy)
            .map_err(|e| anyhow::anyhow!("find_proxy: {e}"))?
            .with_latency("upstream".into(), ms as u32, 0, 1.0);
        Ok(())
    }

    /// Add a bandwidth toxic (kbps).
    pub fn add_bandwidth(&self, proxy: &str, kbps: u64) -> anyhow::Result<()> {
        self.inner
            .find_proxy(proxy)
            .map_err(|e| anyhow::anyhow!("find_proxy: {e}"))?
            .with_bandwidth("upstream".into(), kbps as u32, 1.0);
        Ok(())
    }

    /// Add a timeout toxic — closes connection after `timeout_ms` with no data.
    pub fn add_timeout(&self, proxy: &str, timeout_ms: u64) -> anyhow::Result<()> {
        self.inner
            .find_proxy(proxy)
            .map_err(|e| anyhow::anyhow!("find_proxy: {e}"))?
            .with_timeout("upstream".into(), timeout_ms as u32, 1.0);
        Ok(())
    }

    /// Add a reset_peer toxic — sends TCP RST after `timeout_ms` ms.
    ///
    /// `toxiproxy_rust` doesn't expose reset_peer; falls back to a blocking
    /// HTTP call against the Toxiproxy REST API.
    pub fn add_reset_peer(&self, proxy: &str, timeout_ms: u64) -> anyhow::Result<()> {
        let url = format!("http://127.0.0.1:{}/proxies/{proxy}/toxics", self.api_port);
        let body = format!(
            r#"{{"name":"reset_peer","type":"reset_peer","stream":"upstream","toxicity":1.0,"attributes":{{"timeout":{timeout_ms}}}}}"#
        );
        reqwest_blocking::blocking::Client::new()
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|e| anyhow::anyhow!("add_reset_peer: {e}"))?
            .error_for_status()
            .map_err(|e| anyhow::anyhow!("add_reset_peer status: {e}"))?;
        Ok(())
    }

    /// Add a limit_data toxic — closes connection after `bytes` bytes.
    pub fn add_limit_data(&self, proxy: &str, bytes: u64) -> anyhow::Result<()> {
        self.inner
            .find_proxy(proxy)
            .map_err(|e| anyhow::anyhow!("find_proxy: {e}"))?
            .with_limit_data("upstream".into(), bytes as u32, 1.0);
        Ok(())
    }

    /// Disable a proxy (all connections fail immediately).
    pub fn disable_proxy(&self, name: &str) -> anyhow::Result<()> {
        self.inner
            .find_proxy(name)
            .map_err(|e| anyhow::anyhow!("find_proxy: {e}"))?
            .disable()
            .map_err(|e| anyhow::anyhow!("disable_proxy: {e}"))
    }

    /// Re-enable a disabled proxy.
    pub fn enable_proxy(&self, name: &str) -> anyhow::Result<()> {
        self.inner
            .find_proxy(name)
            .map_err(|e| anyhow::anyhow!("find_proxy: {e}"))?
            .enable()
            .map_err(|e| anyhow::anyhow!("enable_proxy: {e}"))
    }

    /// Delete a named toxic from a proxy.
    pub fn delete_toxic(&self, proxy: &str, toxic_name: &str) -> anyhow::Result<()> {
        let url = format!(
            "http://127.0.0.1:{}/proxies/{proxy}/toxics/{toxic_name}",
            self.api_port
        );
        reqwest_blocking::blocking::Client::new()
            .delete(&url)
            .send()
            .map_err(|e| anyhow::anyhow!("delete_toxic: {e}"))?
            .error_for_status()
            .map_err(|e| anyhow::anyhow!("delete_toxic status: {e}"))?;
        Ok(())
    }

    /// Reset all proxies (enable all, remove all toxics).
    pub fn reset(&self) -> anyhow::Result<()> {
        self.inner
            .reset()
            .map_err(|e| anyhow::anyhow!("reset: {e}"))
    }
}

// ── Per-node state ────────────────────────────────────────────────────────────

struct NodeState {
    handle: ArrowRaftDriverHandle,
    _dir: TempDir,
    listen_addr: SocketAddr,
    _driver_task: tokio::task::JoinHandle<pcs_service::PcsResult<()>>,
}

// ── Raft cluster harness ──────────────────────────────────────────────────────

/// Multi-node PCS Raft cluster with all TCP links routed through Toxiproxy.
///
/// Proxy names follow the pattern `"n{src}_to_{dst}"` (0-indexed). Each
/// directed edge has one proxy so chaos toxics can be applied per-edge.
pub struct RaftClusterHarness {
    nodes: Vec<NodeState>,
    toxi: ToxiproxyClient,
    _container: ToxiproxyContainer,
}

impl RaftClusterHarness {
    /// Spawn an N-node cluster, or return `None` (with a printed warning) if
    /// Docker is unavailable. Use this in tests that should soft-skip without
    /// Docker.
    pub async fn try_start(n: u32) -> Option<Self> {
        match Self::start(n).await {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("SKIP: raft cluster harness unavailable: {e}");
                None
            }
        }
    }

    /// Spawn an N-node cluster where every directed TCP edge routes through its
    /// own Toxiproxy proxy.
    pub async fn start(n: u32) -> anyhow::Result<Self> {
        assert!(n >= 1, "need at least 1 node");
        let n = n as usize;

        // Bind N listen addrs (port 0 → OS assigns), then drop so nodes can bind.
        let listen_addrs: Vec<SocketAddr> = (0..n)
            .map(|_| {
                let l = std::net::TcpListener::bind("127.0.0.1:0")?;
                let addr = l.local_addr()?;
                drop(l);
                Ok(addr)
            })
            .collect::<std::io::Result<_>>()?;

        // Start Toxiproxy container.
        let container: ToxiproxyContainer = ToxiproxyContainer::start().await?;
        let toxi = container.client();

        let edge_count = n * (n - 1);
        assert!(
            edge_count <= container.proxy_host_ports.len(),
            "need {edge_count} proxy ports but only {} pre-exposed",
            container.proxy_host_ports.len()
        );

        // On macOS containers reach the host via host.docker.internal.
        #[cfg(target_os = "macos")]
        let host_alias = "host.docker.internal";
        #[cfg(not(target_os = "macos"))]
        let host_alias = "172.17.0.1";

        // edge_host_ports[(src, dst)] = host port of the proxy for src→dst traffic.
        let mut edge_host_ports = std::collections::HashMap::<(usize, usize), u16>::new();
        let mut port_idx = 0usize;

        // Two independent index dimensions (src × dst), not a single collection iteration.
        #[allow(clippy::needless_range_loop)]
        for src in 0..n {
            for dst in 0..n {
                if src == dst {
                    continue;
                }
                let host_port = container.proxy_host_ports[port_idx];
                let container_port = 20001 + port_idx as u16;
                edge_host_ports.insert((src, dst), host_port);

                let upstream = format!("{host_alias}:{}", listen_addrs[dst].port());
                toxi.create_proxy(&format!("n{src}_to_{dst}"), &upstream, container_port)?;
                port_idx += 1;
            }
        }

        // Build peer maps: node src connects to node dst via the proxy for edge (src→dst).
        let mut peer_maps: Vec<std::collections::HashMap<u64, String>> =
            vec![std::collections::HashMap::new(); n];
        for src in 0..n {
            for dst in 0..n {
                if src == dst {
                    continue;
                }
                let host_port = edge_host_ports[&(src, dst)];
                peer_maps[src].insert(dst as u64 + 1, format!("127.0.0.1:{host_port}"));
            }
        }

        // Start all Raft nodes (non-empty peers map → skip auto-init).
        let mut nodes: Vec<NodeState> = Vec::with_capacity(n);
        for i in 0..n {
            let node_id = i as u64 + 1;
            let listen_addr = listen_addrs[i];
            let dir = TempDir::new()?;
            let config = ArrowRaftDriverConfig {
                node_id,
                listen_addr,
                peers: peer_maps[i].clone(),
                heartbeat_interval_ms: 50,
                election_timeout_min_ms: 300,
                election_timeout_max_ms: 500,
            };
            let (handle, task) = ArrowRaftDriver::start(
                config,
                dir.path().join("log.redb"),
                dir.path().join("app.redb"),
            )
            .await?;
            handle.spawn_tcp_server(listen_addr);
            nodes.push(NodeState {
                handle,
                _dir: dir,
                listen_addr,
                _driver_task: task,
            });
        }

        // Initialize cluster from node 0 with all members.
        if n > 1 {
            let members: BTreeMap<u64, BasicNode> = listen_addrs
                .iter()
                .enumerate()
                .map(|(i, addr)| {
                    (
                        i as u64 + 1,
                        BasicNode {
                            addr: addr.to_string(),
                        },
                    )
                })
                .collect();
            nodes[0].handle.initialize(members).await?;
        }

        Ok(Self {
            nodes,
            toxi,
            _container: container,
        })
    }

    /// Poll until any node reports a leader, or error after 10 seconds.
    pub async fn await_leader(&self) -> anyhow::Result<u64> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            for node in &self.nodes {
                if let Some(leader_id) = node.handle.metrics().current_leader {
                    return Ok(leader_id);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for Raft leader election");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Propose a lightweight no-op via `Heartbeat` (no dedicated Noop variant).
    pub async fn propose_noop(&self, leader_id: u64) -> anyhow::Result<()> {
        use pcs_service::distributed::consensus::types::ConsensusCommand;
        use uuid::Uuid;

        let idx = (leader_id - 1) as usize;
        let cmd = ConsensusCommand::Heartbeat {
            instance_id: Uuid::nil(),
            at: 0,
        };
        self.nodes[idx]
            .handle
            .propose(cmd)
            .await
            .map_err(|e| anyhow::anyhow!("propose_noop: {e}"))?;
        Ok(())
    }

    /// Last-applied log index for a node (None if nothing applied yet).
    pub fn last_applied(&self, node_id: u64) -> Option<u64> {
        self.nodes
            .get((node_id - 1) as usize)?
            .handle
            .metrics()
            .last_applied
            .map(|lid| lid.index)
    }

    /// Access the Toxiproxy client for injecting faults.
    pub fn toxiproxy(&self) -> &ToxiproxyClient {
        &self.toxi
    }

    /// Proxy name for the directed edge src_node → dst_node (0-indexed).
    pub fn proxy_name(src: usize, dst: usize) -> String {
        format!("n{src}_to_{dst}")
    }

    /// Return the maximum Raft term seen across all nodes.
    ///
    /// Used by chaos tests to detect leader elections: if the term advanced
    /// between two calls, at least one election occurred.
    pub fn max_term(&self) -> u64 {
        self.nodes
            .iter()
            .map(|n| n.handle.metrics().current_term)
            .max()
            .unwrap_or(0)
    }

    /// Return a `RedbSharedStore` wired to node `node_id` (1-indexed).
    ///
    /// The store routes all mutations through the live Raft handle; read-only
    /// queries go directly to the node's shared app-db.  Used by chaos tests
    /// to hand a `DistributedRunner` a cluster-connected store.
    pub fn store_for_node(
        &self,
        node_id: u64,
    ) -> pcs_service::distributed::consensus::store::RedbSharedStore {
        let node = &self.nodes[(node_id - 1) as usize];
        pcs_service::distributed::consensus::store::RedbSharedStore::multi_node(
            std::sync::Arc::clone(node.handle.app_db()),
            node.handle.clone(),
        )
    }

    /// Count `Completed` claims on node `node_id` (1-indexed).
    pub fn count_completed_claims(&self, node_id: u64) -> usize {
        let node = &self.nodes[(node_id - 1) as usize];
        let db = node.handle.app_db().lock().unwrap();
        pcs_service::distributed::consensus::state_machine::count_completed_claims(&db)
    }

    /// Gracefully shut down all nodes.
    pub async fn shutdown(self) {
        for node in &self.nodes {
            node.handle.shutdown().await;
        }
    }
}
