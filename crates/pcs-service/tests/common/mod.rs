//! Shared test harness for multi-node Raft chaos tests.
//!
//! - [`ToxiproxyContainer`]: wraps a Toxiproxy Docker container.
//! - [`ToxiproxyClient`]: minimal client for the Toxiproxy HTTP API.
//! - [`RaftClusterHarness`]: N PCS Raft nodes with TCP links routed through
//!   per-edge Toxiproxy proxies.
//!
//! The PCS raft carries membership and leadership only, so the harness has no
//! application store and nothing to propose. Progress is observed through
//! [`RaftClusterHarness::await_leader`], [`RaftClusterHarness::last_applied`]
//! and [`RaftClusterHarness::max_term`]: a leader commits raft's own entry on
//! taking office, so both the term and the applied index advance per election.
//!
//! ```rust,ignore
//! let harness = RaftClusterHarness::start(3).await.unwrap();
//! let leader = harness.await_leader().await.unwrap();
//! harness.await_convergence(Duration::from_secs(10)).await.unwrap();
//! ```

#![cfg(feature = "distributed-raft")]
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use pcs_service::distributed::consensus::driver::{
    ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle,
};
use serde_json::json;
use tempfile::TempDir;
#[cfg(not(any(target_os = "macos", target_os = "windows")))]
use testcontainers::ImageExt;
#[cfg(not(any(target_os = "macos", target_os = "windows")))]
use testcontainers::core::Host;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

/// First container port a proxy listens on; one port per directed edge follows.
const PROXY_PORT_BASE: u16 = 20001;

/// Host alias every proxy uses to dial back to the host-side raft listeners.
const HOST_ALIAS: &str = "host.docker.internal";

/// Toxiproxy container exposing the HTTP API port (8474) plus one proxy port
/// per directed edge of the cluster under test.
pub struct ToxiproxyContainer {
    /// Held so the container outlives the harness.
    _container: ContainerAsync<GenericImage>,
    pub api_port: u16,
    /// Host ports mapped from container proxy ports, in edge order.
    pub proxy_host_ports: Vec<u16>,
}

impl ToxiproxyContainer {
    /// Start Toxiproxy with exactly `edge_count` proxy ports published.
    ///
    /// Publishing only the ports the cluster uses keeps Docker from mapping
    /// ports nothing listens on, which is what made port resolution race
    /// proxy creation.
    pub async fn start(edge_count: usize) -> anyhow::Result<Self> {
        let mut image = GenericImage::new("ghcr.io/shopify/toxiproxy", "2.9.0")
            // Toxiproxy 2.x logs JSON on stdout, so this matches the text
            // inside the `message` field. A predicate that never matches makes
            // every test here soft-skip on a machine that does have Docker.
            .with_wait_for(WaitFor::message_on_stdout("Starting Toxiproxy HTTP server"))
            .with_exposed_port(8474_u16.tcp());
        for offset in 0..edge_count as u16 {
            image = image.with_exposed_port((PROXY_PORT_BASE + offset).tcp());
        }

        // Docker Desktop (Windows, macOS) publishes `HOST_ALIAS` itself.
        // Native Linux Docker does not, so it gets an explicit mapping.
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        let container = image.start().await?;
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        let container = image
            .with_host(HOST_ALIAS, Host::HostGateway)
            .start()
            .await?;

        let api_port = resolve_port(&container, 8474).await?;
        let mut proxy_host_ports = Vec::with_capacity(edge_count);
        for offset in 0..edge_count as u16 {
            proxy_host_ports.push(resolve_port(&container, PROXY_PORT_BASE + offset).await?);
        }

        Ok(Self {
            _container: container,
            api_port,
            proxy_host_ports,
        })
    }

    pub fn client(&self) -> ToxiproxyClient {
        ToxiproxyClient::new(self.api_port)
    }
}

/// Wait until something accepts TCP on `addr`, so a node that lost its
/// reserved port is reported at the point of failure.
async fn await_listening(addr: SocketAddr) -> anyhow::Result<()> {
    const ATTEMPTS: u32 = 40;
    let target = std::net::SocketAddr::new([127, 0, 0, 1].into(), addr.port());
    let mut last_err = None;
    for _ in 0..ATTEMPTS {
        match tokio::net::TcpStream::connect(target).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "{}",
        last_err.expect("at least one attempt failed")
    ))
}

/// Read the host port mapped to `container_port`, retrying briefly.
///
/// Docker reports published ports through `inspect`, and under load that can
/// lag the container becoming ready, so a single lookup intermittently fails
/// with "does not expose port".
async fn resolve_port(
    container: &ContainerAsync<GenericImage>,
    container_port: u16,
) -> anyhow::Result<u16> {
    const ATTEMPTS: u32 = 30;
    let mut last_err = None;
    for _ in 0..ATTEMPTS {
        match container.get_host_port_ipv4(container_port.tcp()).await {
            Ok(port) => return Ok(port),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "container port {container_port} never resolved to a host port: {}",
        last_err.expect("at least one attempt failed")
    ))
}

/// Minimal client for the Toxiproxy HTTP API (see
/// <https://github.com/Shopify/toxiproxy#http-api>).
///
/// Uses `reqwest::blocking` directly instead of the `toxiproxy_rust` crate: that
/// crate pins `reqwest` 0.11, which pulls a vulnerable `h2` (RUSTSEC-2026-0258)
/// with no fix available upstream.
///
/// Every method is sync, because the chaos tests drive toxics from sync
/// closures. A `reqwest::blocking::Client` owns a private tokio runtime, and
/// building or dropping one inside an async context panics ("Cannot drop a
/// runtime in a context where blocking is not allowed"), so each request is
/// built, sent and dropped on its own short-lived OS thread.
pub struct ToxiproxyClient {
    pub api_port: u16,
}

/// The two HTTP shapes the Toxiproxy API needs.
enum Req {
    Post(Option<serde_json::Value>),
    Delete,
}

impl ToxiproxyClient {
    pub fn new(api_port: u16) -> Self {
        #[cfg(feature = "service")]
        pcs_service::service::install_ring_provider();
        Self { api_port }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}/{path}", self.api_port)
    }

    /// Issue one request on a dedicated thread and wait for it.
    fn request(&self, what: &str, path: &str, req: Req) -> anyhow::Result<()> {
        let url = self.url(path);
        let joined = std::thread::spawn(move || -> anyhow::Result<()> {
            let http = reqwest::blocking::Client::new();
            let builder = match req {
                Req::Post(body) => {
                    let post = http.post(url);
                    match body {
                        Some(json) => post.json(&json),
                        None => post,
                    }
                }
                Req::Delete => http.delete(url),
            };
            builder.send()?.error_for_status()?;
            Ok(())
        })
        .join();
        match joined {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(anyhow::anyhow!("{what}: {e}")),
            Err(_) => Err(anyhow::anyhow!("{what}: request thread panicked")),
        }
    }

    /// Create a proxy that listens on `listen_port` (container-internal) and
    /// forwards to `upstream` (`host:port` string).
    pub fn create_proxy(&self, name: &str, upstream: &str, listen_port: u16) -> anyhow::Result<()> {
        self.request(
            "create_proxy",
            "proxies",
            Req::Post(Some(json!({
                "name": name,
                "listen": format!("0.0.0.0:{listen_port}"),
                "upstream": upstream,
                "enabled": true,
            }))),
        )
    }

    /// Delete a named proxy.
    pub fn delete_proxy(&self, name: &str) -> anyhow::Result<()> {
        self.request("delete_proxy", &format!("proxies/{name}"), Req::Delete)
    }

    /// Register a toxic on `proxy`.
    fn add_toxic(
        &self,
        proxy: &str,
        name: &str,
        toxic_type: &str,
        stream: &str,
        attributes: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.request(
            &format!("add_toxic({toxic_type})"),
            &format!("proxies/{proxy}/toxics"),
            Req::Post(Some(json!({
                "name": name,
                "type": toxic_type,
                "stream": stream,
                "toxicity": 1.0,
                "attributes": attributes,
            }))),
        )
    }

    /// Add a latency toxic (milliseconds).
    pub fn add_latency(&self, proxy: &str, ms: u64) -> anyhow::Result<()> {
        self.add_toxic(
            proxy,
            "latency_upstream",
            "latency",
            "upstream",
            json!({ "latency": ms, "jitter": 0 }),
        )
    }

    /// Add a bandwidth toxic (kbps).
    pub fn add_bandwidth(&self, proxy: &str, kbps: u64) -> anyhow::Result<()> {
        self.add_toxic(
            proxy,
            "bandwidth_upstream",
            "bandwidth",
            "upstream",
            json!({ "rate": kbps }),
        )
    }

    /// Add a timeout toxic: closes the connection after `timeout_ms` with no data.
    pub fn add_timeout(&self, proxy: &str, timeout_ms: u64) -> anyhow::Result<()> {
        self.add_toxic(
            proxy,
            "timeout_upstream",
            "timeout",
            "upstream",
            json!({ "timeout": timeout_ms }),
        )
    }

    /// Add a reset_peer toxic: sends a TCP RST after `timeout_ms` ms.
    pub fn add_reset_peer(&self, proxy: &str, timeout_ms: u64) -> anyhow::Result<()> {
        self.add_toxic(
            proxy,
            "reset_peer",
            "reset_peer",
            "upstream",
            json!({ "timeout": timeout_ms }),
        )
    }

    /// Add a limit_data toxic: closes the connection after `bytes` bytes.
    pub fn add_limit_data(&self, proxy: &str, bytes: u64) -> anyhow::Result<()> {
        self.add_toxic(
            proxy,
            "limit_data_upstream",
            "limit_data",
            "upstream",
            json!({ "bytes": bytes }),
        )
    }

    /// Disable a proxy (all connections fail immediately).
    pub fn disable_proxy(&self, name: &str) -> anyhow::Result<()> {
        self.set_proxy_enabled(name, false)
    }

    /// Re-enable a disabled proxy.
    pub fn enable_proxy(&self, name: &str) -> anyhow::Result<()> {
        self.set_proxy_enabled(name, true)
    }

    fn set_proxy_enabled(&self, name: &str, enabled: bool) -> anyhow::Result<()> {
        self.request(
            "set_proxy_enabled",
            &format!("proxies/{name}"),
            Req::Post(Some(json!({ "enabled": enabled }))),
        )
    }

    /// Delete a named toxic from a proxy.
    pub fn delete_toxic(&self, proxy: &str, toxic_name: &str) -> anyhow::Result<()> {
        self.request(
            "delete_toxic",
            &format!("proxies/{proxy}/toxics/{toxic_name}"),
            Req::Delete,
        )
    }

    /// Reset all proxies (enable all, remove all toxics).
    pub fn reset(&self) -> anyhow::Result<()> {
        self.request("reset", "reset", Req::Post(None))
    }
}

struct NodeState {
    handle: ArrowRaftDriverHandle,
    _dir: TempDir,
    listen_addr: SocketAddr,
    _driver_task: tokio::task::JoinHandle<pcs_service::PcsResult<()>>,
}

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
    /// Spawns an N-node cluster, or returns `None` when the Docker daemon
    /// cannot give us a Toxiproxy container.
    ///
    /// Only the container step soft-skips. Once the container is up, every
    /// later failure (port resolution, proxy creation, node startup) is a hard
    /// panic, mirroring `tests/common/tikv.rs`: "containers up but the store
    /// unreachable -> a real error". That is what keeps a green run from hiding
    /// a broken harness.
    pub async fn try_start(n: u32) -> Option<Self> {
        assert!(n >= 1, "need at least 1 node");
        let edge_count = (n as usize) * (n as usize - 1);
        let container = match ToxiproxyContainer::start(edge_count).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("SKIP: raft cluster harness unavailable: {e}");
                return None;
            }
        };
        Some(
            Self::wire_up(container, n as usize)
                .await
                .expect("Toxiproxy is up, so cluster wiring must succeed"),
        )
    }

    /// Spawn an N-node cluster where every directed TCP edge routes through its
    /// own Toxiproxy proxy.
    async fn wire_up(container: ToxiproxyContainer, n: usize) -> anyhow::Result<Self> {
        let toxi = container.client();

        // Reserve the node ports only now that Docker has already published
        // the container's proxy ports. Both come from the host's ephemeral
        // range, so reserving first leaves a window in which Docker publishes
        // onto a port we just released; losing two of three nodes that way
        // costs quorum and the cluster campaigns forever without electing.
        // Binding 0.0.0.0 rather than loopback is what lets the container
        // reach these listeners through `HOST_ALIAS`.
        let listen_addrs: Vec<SocketAddr> = (0..n)
            .map(|_| {
                let l = std::net::TcpListener::bind("0.0.0.0:0")?;
                let addr = l.local_addr()?;
                drop(l);
                Ok(addr)
            })
            .collect::<std::io::Result<_>>()?;

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
                let container_port = PROXY_PORT_BASE + port_idx as u16;
                edge_host_ports.insert((src, dst), host_port);

                let upstream = format!("{HOST_ALIAS}:{}", listen_addrs[dst].port());
                toxi.create_proxy(&format!("n{src}_to_{dst}"), &upstream, container_port)?;
                port_idx += 1;
            }
        }

        // Node src reaches node dst through the proxy for edge (src, dst).
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
                election_timeout_ms: 400,
                snapshot_log_interval: 1000,
            };
            let (handle, task) =
                ArrowRaftDriver::start(config, dir.path().join("raft-log.redb")).await?;
            handle.spawn_tcp_server(listen_addr);
            // A reserved port can still be lost to another process between
            // the reservation and this bind. Without this check the node is
            // simply unreachable and the whole cluster fails later with an
            // unexplained election timeout.
            await_listening(listen_addr).await.map_err(|e| {
                anyhow::anyhow!("node {node_id} never accepted on {listen_addr}: {e}")
            })?;
            nodes.push(NodeState {
                handle,
                _dir: dir,
                listen_addr,
                _driver_task: task,
            });
        }

        // Membership is static: each driver seeds its conf state from its
        // configured peers on first boot, so no initialize call is needed.

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
                if let Some(leader_id) = node.handle.metrics().leader_id {
                    return Ok(leader_id);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for Raft leader election; per-node state: [{}]",
                    self.diagnostics()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll until a node reports a leader other than `excluded`, or error
    /// after 10 seconds.
    ///
    /// `await_leader` returns whichever node answers first in list order,
    /// which an isolated leader can satisfy forever: with raft-rs defaults
    /// (`check_quorum = false`) a leader cut off from its quorum never steps
    /// down and keeps reporting itself. Excluding its stale self-report makes
    /// post-isolation waits meaningful.
    pub async fn await_leader_excluding(&self, excluded: u64) -> anyhow::Result<u64> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            for node in &self.nodes {
                if let Some(leader_id) = node.handle.metrics().leader_id
                    && leader_id != excluded
                {
                    return Ok(leader_id);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for a leader other than node {excluded}; per-node state: [{}]",
                    self.diagnostics()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait until every node reports the same applied index, then return it.
    ///
    /// Raft appends one entry per term when a node takes office, so a settled
    /// cluster converges on a common index without any client traffic. Errors
    /// on timeout, reporting the per-node indices.
    pub async fn await_convergence(&self, within: Duration) -> anyhow::Result<u64> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let indices: Vec<Option<u64>> = (1..=self.nodes.len() as u64)
                .map(|id| self.last_applied(id))
                .collect();
            let settled = indices.iter().all(|i| i.is_some())
                && indices.iter().flatten().min() == indices.iter().flatten().max();
            if settled {
                return Ok(indices.iter().flatten().copied().max().unwrap_or(0));
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "applied indices did not converge: {indices:?}; per-node state: [{}]",
                    self.diagnostics()
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait until `node_id` reports an applied index of at least `target`.
    ///
    /// Errors on timeout, reporting the index actually reached.
    pub async fn await_applied_at_least(
        &self,
        node_id: u64,
        target: u64,
        within: Duration,
    ) -> anyhow::Result<u64> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let applied = self.last_applied(node_id).unwrap_or(0);
            if applied >= target {
                return Ok(applied);
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "node {node_id} reached applied index {applied}, expected at least {target}"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Last-applied log index for a node (None if nothing applied yet).
    pub fn last_applied(&self, node_id: u64) -> Option<u64> {
        let applied = self
            .nodes
            .get((node_id - 1) as usize)?
            .handle
            .metrics()
            .applied_index;
        (applied > 0).then_some(applied)
    }

    /// Access the Toxiproxy client for injecting faults.
    pub fn toxiproxy(&self) -> &ToxiproxyClient {
        &self.toxi
    }

    /// Proxy name for the directed edge src_node → dst_node (0-indexed).
    pub fn proxy_name(src: usize, dst: usize) -> String {
        format!("n{src}_to_{dst}")
    }

    /// One line per node: id, role, term, applied index, listen address.
    ///
    /// All-Follower with a climbing term everywhere means votes are not being
    /// delivered (no container-to-host connectivity); a stable term with no
    /// leader is a genuine consensus problem.
    pub fn diagnostics(&self) -> String {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, node)| {
                let m = node.handle.metrics();
                format!(
                    "node{}: {:?} term={} applied={} last_log={} leader={:?} listen={}",
                    i + 1,
                    m.state,
                    m.term,
                    m.applied_index,
                    m.last_log_index,
                    m.leader_id,
                    node.listen_addr
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Return the maximum Raft term seen across all nodes.
    ///
    /// Used by chaos tests to detect leader elections: if the term advanced
    /// between two calls, at least one election occurred.
    pub fn max_term(&self) -> u64 {
        self.nodes
            .iter()
            .map(|n| n.handle.metrics().term)
            .max()
            .unwrap_or(0)
    }

    /// Number of nodes in the cluster.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Gracefully shut down all nodes.
    pub async fn shutdown(self) {
        for node in &self.nodes {
            node.handle.shutdown().await;
        }
    }
}
