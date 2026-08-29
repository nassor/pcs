//! Outbound half of the transport: per-peer connection pooling, the circuit
//! breaker, and raft message sending for the driver's [`TransportHub`].

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Instant;

use raft::eraftpb::Message;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::{PcsError, PcsResult};

use super::wire::{encode_raft_message, write_frame};
use super::{
    CIRCUIT_OPEN_DURATION, CIRCUIT_OPEN_THRESHOLD, CONNECT_TIMEOUT, POOL_CAPACITY, POOL_MAX_IDLE,
    RPC_WRITE_TIMEOUT, TransportError,
};

// ── Per-peer connection pool ───────────────────────────────────────────────────

/// A pooled stream tagged with the time it was returned to the pool.
struct PooledStream {
    stream: TcpStream,
    returned_at: Instant,
}

/// Per-peer circuit-breaker state.
///
/// Transitions:
/// - `Closed { consecutive_failures }` → `Open { opened_at }` when
///   `consecutive_failures` reaches [`CIRCUIT_OPEN_THRESHOLD`].
/// - `Open { opened_at }` → `Closed { consecutive_failures: 0 }` after
///   [`CIRCUIT_OPEN_DURATION`] has elapsed.
///
/// Any successful send resets the counter to zero.
#[derive(Debug, Clone)]
enum CircuitState {
    Closed { consecutive_failures: u32 },
    Open { opened_at: Instant },
}

impl CircuitState {
    fn new() -> Self {
        CircuitState::Closed {
            consecutive_failures: 0,
        }
    }

    /// Returns `true` if the circuit is currently open (blocking sends).
    fn is_open(&self) -> bool {
        matches!(self, CircuitState::Open { opened_at } if opened_at.elapsed() < CIRCUIT_OPEN_DURATION)
    }

    /// Record a successful send; resets the failure counter.
    fn record_success(&mut self) {
        *self = CircuitState::Closed {
            consecutive_failures: 0,
        };
    }

    /// Record a failed send; may transition to Open.
    fn record_failure(&mut self) {
        match self {
            CircuitState::Closed {
                consecutive_failures,
            } => {
                *consecutive_failures += 1;
                if *consecutive_failures >= CIRCUIT_OPEN_THRESHOLD {
                    *self = CircuitState::Open {
                        opened_at: Instant::now(),
                    };
                }
            }
            CircuitState::Open { opened_at } => {
                if opened_at.elapsed() >= CIRCUIT_OPEN_DURATION {
                    // Timeout elapsed, so half-open: allow one attempt with the
                    // counter reset to a single failure.
                    *self = CircuitState::Closed {
                        consecutive_failures: 1,
                    };
                }
                // else: still open, stay open.
            }
        }
    }
}

/// A bounded pool of idle [`TcpStream`]s for one remote peer.
///
/// Acquire a stream with [`PeerPool::acquire`]; after use, return it with
/// [`PeerPool::release`] on success or simply drop it (do not call `release`)
/// on error so a broken stream is never returned to the pool.
struct PeerPool {
    /// Peer address as `"host:port"`, either a hostname or an IP literal. Resolved to
    /// [`SocketAddr`](std::net::SocketAddr) lazily at connection time so Docker service
    /// names such as `"node2:9002"` work without a pre-boot DNS lookup.
    addr: String,
    idle: Mutex<VecDeque<PooledStream>>,
    /// Per-peer circuit breaker; keeps `acquire` from hammering a dead peer.
    circuit: Mutex<CircuitState>,
}

impl PeerPool {
    fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            idle: Mutex::new(VecDeque::with_capacity(POOL_CAPACITY)),
            circuit: Mutex::new(CircuitState::new()),
        }
    }

    /// Acquire a stream: pops idle connections, dropping any that have exceeded
    /// [`POOL_MAX_IDLE`], until a fresh-enough one is found or the pool is empty, then
    /// opens a new TCP connection with a [`CONNECT_TIMEOUT`] deadline.
    ///
    /// Returns `Err(TransportError::Other)` immediately if the per-peer circuit
    /// is open, avoiding unnecessary connect attempts to a known-dead peer.
    ///
    /// The peer address is resolved via DNS on each new-connection attempt, so
    /// hostnames such as Docker Compose service names work.
    async fn acquire(&self) -> Result<TcpStream, TransportError> {
        // Circuit breaker: fast-fail if too many recent failures.
        {
            let circuit = self.circuit.lock().await;
            if circuit.is_open() {
                return Err(TransportError::Other(io::Error::other(format!(
                    "circuit open for peer {} — too many consecutive failures",
                    self.addr
                ))));
            }
        }

        {
            let mut guard = self.idle.lock().await;
            while let Some(pooled) = guard.pop_front() {
                if pooled.returned_at.elapsed() <= POOL_MAX_IDLE {
                    return Ok(pooled.stream);
                }
                // Stale connection: drop it and try the next one.
            }
        }
        // Resolve the hostname lazily so Docker service-name peers work.
        let addr = tokio::net::lookup_host(&self.addr)
            .await
            .map_err(|e| TransportError::ConnectFailed(io::Error::other(format!("DNS: {e}"))))?
            .next()
            .ok_or_else(|| {
                TransportError::ConnectFailed(io::Error::other(format!(
                    "no addresses for {}",
                    self.addr
                )))
            })?;
        tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| TransportError::ConnectFailed(io::Error::other("connect timeout")))?
            .map_err(TransportError::ConnectFailed)
    }

    /// Return a healthy stream to the pool. If the pool is full, the stream is dropped.
    async fn release(&self, stream: TcpStream) {
        let mut guard = self.idle.lock().await;
        if guard.len() < POOL_CAPACITY {
            guard.push_back(PooledStream {
                stream,
                returned_at: Instant::now(),
            });
        }
        // A full pool drops the stream here, by design.
    }

    /// Record a successful send against this peer; resets the circuit.
    async fn record_success(&self) {
        self.circuit.lock().await.record_success();
    }

    /// Record a failed send against this peer; may open the circuit.
    async fn record_failure(&self) {
        self.circuit.lock().await.record_failure();
    }
}

/// Client-side network handle for one remote Raft peer: a bounded connection
/// pool plus circuit breaker.
#[derive(Clone)]
pub struct TcpNetwork {
    pub target: u64,
    pool: Arc<PeerPool>,
}

impl TcpNetwork {
    /// Create a network channel to `target`.
    ///
    /// `addr` is a `"host:port"` string, either an IP literal or a hostname resolved
    /// lazily at connect time.
    pub(crate) fn new(target: u64, addr: impl Into<String>) -> Self {
        Self {
            target,
            pool: Arc::new(PeerPool::new(addr)),
        }
    }

    /// Send one raft protocol message.
    ///
    /// Raft traffic is fire-and-forget and the server writes no reply frame, so
    /// nothing is read back here. A failed send is surfaced to the caller
    /// (which reports the peer unreachable to raft so it re-probes).
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Generic`] when the connect, the write, or the
    /// circuit breaker refuses the send.
    pub async fn send_message(&self, msg: &Message) -> PcsResult<()> {
        use prost::Message as _;
        let bytes = encode_raft_message(&msg.encode_to_vec());
        let mut stream = match self.pool.acquire().await {
            Ok(s) => s,
            Err(e) => {
                return Err(PcsError::generic(format!(
                    "send raft message to peer {} failed: {e}",
                    self.target
                )));
            }
        };
        let write_result =
            tokio::time::timeout(RPC_WRITE_TIMEOUT, write_frame(&mut stream, &bytes))
                .await
                .map_err(|_| TransportError::WriteTimeout)
                .and_then(|r| r.map_err(TransportError::WriteFailed));
        match write_result {
            Ok(()) => {
                self.pool.release(stream).await;
                self.pool.record_success().await;
                Ok(())
            }
            Err(e) => {
                self.pool.record_failure().await;
                Err(PcsError::generic(format!(
                    "send raft message to peer {} failed: {e}",
                    self.target
                )))
            }
        }
    }
}

/// The driver's outbound hub: one pooled connection per cluster peer.
#[derive(Clone)]
pub struct TransportHub {
    networks: HashMap<u64, TcpNetwork>,
}

impl TransportHub {
    /// Build one [`TcpNetwork`] per configured peer.
    pub fn new(peers: &HashMap<u64, String>) -> Self {
        let networks = peers
            .iter()
            .map(|(id, addr)| (*id, TcpNetwork::new(*id, addr.clone())))
            .collect();
        Self { networks }
    }

    /// Send a raft protocol message to `target`.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Generic`] when the peer is unknown or the send
    /// fails; the drive loop reports the peer unreachable so raft re-probes.
    pub async fn send_message(&self, target: u64, msg: &Message) -> PcsResult<()> {
        match self.networks.get(&target) {
            Some(network) => network.send_message(msg).await,
            None => Err(PcsError::generic(format!(
                "TransportHub: no network for peer {target}"
            ))),
        }
    }
}
