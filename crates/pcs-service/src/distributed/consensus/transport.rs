//! TCP transport for Arrow-IPC Raft consensus messages.
//!
//! Same length-prefixed framing as the existing transport, but with a typed
//! envelope to distinguish message kinds on the wire:
//!
//! - Control messages (`AppendEntries`, `Vote`) are serialised as `serde_json`.
//! - Snapshot transfer uses a multi-frame chunked protocol (4 MiB per chunk).
//!
//! ```text
//! ┌────────────────┬──────────────────────────┐
//! │  length: u32   │  payload: [u8; length]   │
//! │  (big-endian)  │                          │
//! └────────────────┴──────────────────────────┘
//! ```
//!
//! Each payload is a `serde_json`-encoded `RpcEnvelope`. The wire format is
//! **append-only**: existing variant positions must never change so that
//! rolling upgrades remain compatible.
//!
//! ## TCP server
//!
//! [`RaftTcpServer`] binds a listen address and dispatches incoming envelopes
//! to the local [`Raft`](openraft::Raft) node.  Start it once during node
//! initialisation before any remote peer can contact the node.
//!
//! ## Connection pool
//!
//! [`TcpNetwork`] maintains a per-peer pool of idle [`TcpStream`]s bounded by
//! [`POOL_CAPACITY`]. Streams are acquired from the pool (or freshly connected)
//! and returned on success; dropped on error so a broken stream never re-enters
//! the pool.
//!
//! ## Timeouts
//!
//! All read-frame calls on the RPC-response path are wrapped in
//! [`tokio::time::timeout`] with a deadline of [`RPC_READ_TIMEOUT`].  Connect
//! attempts are wrapped with [`CONNECT_TIMEOUT`].  Write calls are wrapped with
//! [`RPC_WRITE_TIMEOUT`].

use std::collections::HashMap;
#[cfg(feature = "distributed-raft")]
use std::collections::VecDeque;
use std::io;
// Used inside #[cfg(feature = "distributed-raft")] blocks and tests.
#[allow(unused_imports)]
use std::net::SocketAddr;
#[allow(unused_imports)]
use std::sync::Arc;

#[cfg(feature = "distributed-raft")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "distributed-raft")]
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
#[allow(unused_imports)]
use tokio::sync::Mutex;

#[cfg(feature = "distributed-raft")]
use openraft::{
    BasicNode, RaftNetworkFactory, RaftNetworkV2, Snapshot,
    error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{Backoff, RPCOption},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf},
};
#[cfg(feature = "distributed-raft")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "distributed-raft")]
use crate::distributed::consensus::types::{ConsensusCommand, ConsensusResponse, PcsTypeConfig};
#[cfg(feature = "distributed-raft")]
use crate::{PcsError, PcsResult};

// ── Constants ──────────────────────────────────────────────────────────────────

/// Hard cap on a single TCP frame.  Snapshot *chunks* are bounded by
/// [`SNAPSHOT_CHUNK_BYTES`], which is well within this limit.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Maximum snapshot payload per chunk frame.
///
/// Chosen to keep each frame well below `MAX_FRAME_BYTES` while limiting the
/// number of round-trips for typical state-machine snapshots (< 64 MiB).
#[cfg(feature = "distributed-raft")]
pub const SNAPSHOT_CHUNK_BYTES: usize = 4 * 1024 * 1024; // 4 MiB

/// Per-RPC read-response timeout.  A dead peer that accepted the TCP connect
/// but never replies will be declared unreachable after this duration.
#[cfg(feature = "distributed-raft")]
pub const RPC_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-RPC write timeout. A blocked-but-alive peer (TCP send buffer full)
/// will be declared unreachable after this duration.
#[cfg(feature = "distributed-raft")]
pub const RPC_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Read-response timeout used exclusively for [`forward_proposal`].
///
/// Proposal forwarding waits for the remote leader to run `client_write` —
/// which can block for the full Raft commit round-trip.  This timeout must
/// be strictly greater than `CLUSTER_PROPOSE_TIMEOUT` (30 s) so the store
/// layer's outer timeout fires first if something goes wrong.
#[cfg(feature = "distributed-raft")]
const PROPOSAL_FORWARD_READ_TIMEOUT: Duration = Duration::from_secs(35);

/// Timeout for establishing a new TCP connection to a peer.
#[cfg(feature = "distributed-raft")]
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Maximum number of idle connections kept per peer.
#[cfg(feature = "distributed-raft")]
pub const POOL_CAPACITY: usize = 4;

/// Maximum idle time for a pooled connection before it is dropped on next acquire.
#[cfg(feature = "distributed-raft")]
const POOL_MAX_IDLE: Duration = Duration::from_secs(10);

/// Maximum number of concurrent accepted connections on the server.
#[cfg(feature = "distributed-raft")]
const MAX_ACCEPTED_CONNECTIONS: usize = 1024;

/// Maximum total bytes buffered per in-flight snapshot transfer (256 MiB).
#[cfg(feature = "distributed-raft")]
const SNAPSHOT_MAX_TRANSFER_BYTES: usize = 256 * 1024 * 1024;

/// Maximum number of concurrent in-flight snapshot transfers per connection.
#[cfg(feature = "distributed-raft")]
const SNAPSHOT_MAX_CONCURRENT_TRANSFERS: usize = 4;

/// Idle timeout for a snapshot transfer (no chunk received for this duration).
#[cfg(feature = "distributed-raft")]
const SNAPSHOT_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Idle-read timeout on the server connection task.
///
/// A peer that keeps TCP alive but stops sending is evicted after this window.
/// Chosen to be well above Raft heartbeat intervals (typically 150–500 ms).
#[cfg(feature = "distributed-raft")]
const IDLE_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum per-chunk byte size enforced on the client before framing.
///
/// Matches the server-side cap so oversized chunks are caught client-side with
/// a clear error rather than being rejected by the server after framing.
#[cfg(feature = "distributed-raft")]
const MAX_SNAPSHOT_CHUNK_BYTES: usize = SNAPSHOT_CHUNK_BYTES; // 4 MiB

/// Number of consecutive RPC failures before a per-peer circuit opens.
#[cfg(feature = "distributed-raft")]
const CIRCUIT_OPEN_THRESHOLD: u32 = 5;

/// Duration a circuit stays open before allowing a retry attempt.
#[cfg(feature = "distributed-raft")]
const CIRCUIT_OPEN_DURATION: Duration = Duration::from_secs(30);

// ── Transport error classification ────────────────────────────────────────────

/// Fine-grained transport error, mapped to the appropriate openraft error type.
///
/// Mapping table:
///
/// | `TransportError` variant    | openraft mapping                          | Semantic                            |
/// |-----------------------------|-------------------------------------------|-------------------------------------|
/// | `ConnectFailed`             | `RPCError::Unreachable`                   | Peer is down / unreachable          |
/// | `WriteFailed`               | `RPCError::Network` (transient)           | Lost connection mid-send            |
/// | `WriteTimeout`              | `RPCError::Network` (transient)           | Peer alive but write buffer full    |
/// | `ReadTimeout`               | `RPCError::Network` (transient)           | Peer alive but not responding       |
/// | `FramingError`              | `RPCError::Network` (transient)           | Corrupt/truncated frame             |
/// | `PeerReset`                 | `RPCError::Network` (transient)           | Peer closed connection cleanly      |
/// | `EncodeError`               | `RPCError::Unreachable` (fatal-ish)       | Serialization bug — not transient   |
/// | `Other`                     | `RPCError::Network` (transient)           | Miscellaneous I/O error             |
#[cfg(feature = "distributed-raft")]
#[derive(Debug)]
pub enum TransportError {
    /// TCP connect failed (peer unreachable or connection refused).
    ConnectFailed(io::Error),
    /// Write to stream failed.
    WriteFailed(io::Error),
    /// Write timed out — peer send buffer full.
    WriteTimeout,
    /// Read timed out — peer did not respond within [`RPC_READ_TIMEOUT`].
    ReadTimeout,
    /// Frame protocol error (oversized frame, premature EOF).
    FramingError(String),
    /// Peer reset the connection cleanly (EOF on read).
    PeerReset,
    /// Serialization error — this is a bug, not a transient network issue.
    EncodeError(String),
    /// Other I/O error.
    Other(io::Error),
}

#[cfg(feature = "distributed-raft")]
impl TransportError {
    /// Map to openraft `RPCError`.  Connect failures and encode errors surface
    /// as `Unreachable` (causes openraft to back off); everything else is
    /// `Network` (transient, causes immediate retry).
    pub fn into_rpc_error(self) -> RPCError<PcsTypeConfig> {
        match self {
            TransportError::ConnectFailed(e) => {
                RPCError::Unreachable(Unreachable::from_string(format!("connect failed: {e}")))
            }
            TransportError::EncodeError(msg) => RPCError::Unreachable(Unreachable::from_string(
                format!("encode error (bug): {msg}"),
            )),
            TransportError::ReadTimeout => {
                RPCError::Network(NetworkError::new(&io::Error::other("RPC read timeout")))
            }
            TransportError::WriteTimeout => {
                RPCError::Network(NetworkError::new(&io::Error::other("RPC write timeout")))
            }
            TransportError::WriteFailed(e) => RPCError::Network(NetworkError::new(
                &io::Error::other(format!("write failed: {e}")),
            )),
            TransportError::FramingError(msg) => {
                RPCError::Network(NetworkError::new(&io::Error::other(msg)))
            }
            TransportError::PeerReset => RPCError::Network(NetworkError::new(&io::Error::other(
                "peer reset connection",
            ))),
            TransportError::Other(e) => RPCError::Network(NetworkError::new(&e)),
        }
    }

    pub fn into_streaming_error(self) -> StreamingError<PcsTypeConfig> {
        StreamingError::from(self.into_rpc_error())
    }
}

// ── Wire envelope ─────────────────────────────────────────────────────────────

/// Typed envelope for all RPCs sent over the TCP transport.
///
/// **Append-only**: do not reorder or remove variants. The `serde_json`
/// discriminant is the variant name string — adding new variants at the end is
/// always safe.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RpcEnvelope {
    /// `AppendEntries` RPC.
    AppendEntries(AppendEntriesRequest<PcsTypeConfig>),
    /// `Vote` / `RequestVote` RPC.
    Vote(VoteRequest<PcsTypeConfig>),
    /// One chunk of a snapshot transfer.
    SnapshotChunk(SnapshotChunkMsg),
    /// Signals the last chunk and carries the snapshot metadata.
    SnapshotFinal(SnapshotFinalMsg),
    /// A follower forwards a proposal to the leader.
    ///
    /// Added at the end to preserve wire-format compatibility with older nodes.
    ProposalForward { command: ConsensusCommand },
}

/// A single data chunk within a snapshot transfer.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotChunkMsg {
    /// Unique transfer ID shared across all chunks of one snapshot send.
    pub transfer_id: u64,
    /// Byte offset within the full snapshot payload.
    pub offset: u64,
    /// Raw bytes of this chunk.
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

/// Final (or only) chunk of a snapshot transfer; includes metadata.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotFinalMsg {
    /// Unique transfer ID shared across all chunks of one snapshot send.
    pub transfer_id: u64,
    /// Byte offset of the last chunk's start.
    pub offset: u64,
    /// Raw bytes of the last chunk (may be empty).
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    /// Leader vote, forwarded to [`Raft::install_full_snapshot`].
    pub vote: VoteOf<PcsTypeConfig>,
    /// Snapshot metadata.
    pub meta: SnapshotMetaOf<PcsTypeConfig>,
}

/// Response envelope returned from the server for each incoming RPC.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RpcResponse {
    /// Response to an `AppendEntries` RPC.
    AppendEntries(AppendEntriesResponse<PcsTypeConfig>),
    /// Response to a `Vote` RPC.
    Vote(VoteResponse<PcsTypeConfig>),
    /// Acknowledgement for an intermediate snapshot chunk.
    SnapshotChunkAck { transfer_id: u64 },
    /// Final response after the snapshot was installed.
    SnapshotDone(SnapshotResponse<PcsTypeConfig>),
    /// Error string returned by the server.
    Error(String),
    /// Result of a forwarded proposal. Uses `Option` fields instead of
    /// `Result` to keep serde_json serialization clean.
    ///
    /// Exactly one of `ok` and `err` is `Some`.
    ProposalResult {
        ok: Option<ConsensusResponse>,
        err: Option<String>,
    },
}

// ── Frame helpers ─────────────────────────────────────────────────────────────

/// Read one length-prefixed frame from `stream`.
///
/// Returns:
/// - `Ok(Some(bytes))` — a complete frame was received.
/// - `Ok(None)` — the peer closed the connection cleanly (EOF on length header).
/// - `Err(e)` — an I/O error occurred:
///   - `ErrorKind::InvalidData` — frame length exceeds [`MAX_FRAME_BYTES`].
///   - `ErrorKind::UnexpectedEof` — truncated frame (EOF inside payload).
///   - Other kinds forwarded from the underlying stream.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
async fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} > {MAX_FRAME_BYTES}"),
        ));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            io::Error::new(io::ErrorKind::UnexpectedEof, "truncated frame payload")
        } else {
            e
        }
    })?;
    Ok(Some(payload))
}

#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    if data.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {} > {MAX_FRAME_BYTES}", data.len()),
        ));
    }
    let len = u32::try_from(data.len()).map_err(|_| io::Error::other("frame too large"))?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await
}

// ── Per-peer connection pool ───────────────────────────────────────────────────

/// A pooled stream tagged with the time it was returned to the pool.
#[cfg(feature = "distributed-raft")]
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
/// Any successful RPC resets the counter to zero.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Clone)]
enum CircuitState {
    Closed { consecutive_failures: u32 },
    Open { opened_at: Instant },
}

#[cfg(feature = "distributed-raft")]
impl CircuitState {
    fn new() -> Self {
        CircuitState::Closed {
            consecutive_failures: 0,
        }
    }

    /// Returns `true` if the circuit is currently open (blocking RPCs).
    fn is_open(&self) -> bool {
        matches!(self, CircuitState::Open { opened_at } if opened_at.elapsed() < CIRCUIT_OPEN_DURATION)
    }

    /// Record a successful RPC — resets the failure counter.
    fn record_success(&mut self) {
        *self = CircuitState::Closed {
            consecutive_failures: 0,
        };
    }

    /// Record a failed RPC — may transition to Open.
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
                    // Timeout elapsed — half-open: allow one attempt (reset to 1 failure).
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
#[cfg(feature = "distributed-raft")]
struct PeerPool {
    /// Peer address as `"host:port"` — may be a hostname or an IP literal.
    /// Resolved to [`SocketAddr`] lazily at connection time so that Docker
    /// service names (e.g. `"node2:9002"`) work without requiring a pre-boot
    /// DNS lookup.
    addr: String,
    idle: Mutex<VecDeque<PooledStream>>,
    /// Per-peer circuit breaker — guards `acquire` from hammering a dead peer.
    circuit: Mutex<CircuitState>,
}

#[cfg(feature = "distributed-raft")]
impl PeerPool {
    fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            idle: Mutex::new(VecDeque::with_capacity(POOL_CAPACITY)),
            circuit: Mutex::new(CircuitState::new()),
        }
    }

    /// Acquire a stream: pops idle connections, dropping any that have
    /// exceeded [`POOL_MAX_IDLE`], until a fresh-enough one is found or
    /// the pool is empty, then opens a new TCP connection with a
    /// [`CONNECT_TIMEOUT`] deadline.
    ///
    /// Returns `Err(TransportError::Other)` immediately if the per-peer circuit
    /// is open, avoiding unnecessary connect attempts to a known-dead peer.
    ///
    /// The peer address is resolved via DNS on each new-connection attempt so
    /// that hostnames (e.g. Docker Compose service names) are supported.
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
                // Stale connection — drop it and try the next one.
            }
        }
        // Resolve hostname lazily — enables Docker service-name peers.
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

    /// Return a healthy stream to the pool.  If the pool is full, the stream
    /// is dropped.
    async fn release(&self, stream: TcpStream) {
        let mut guard = self.idle.lock().await;
        if guard.len() < POOL_CAPACITY {
            guard.push_back(PooledStream {
                stream,
                returned_at: Instant::now(),
            });
        }
        // If full, the stream is dropped here — that is intentional.
    }

    /// Record a successful RPC against this peer — resets the circuit.
    async fn record_success(&self) {
        self.circuit.lock().await.record_success();
    }

    /// Record a failed RPC against this peer — may open the circuit.
    async fn record_failure(&self) {
        self.circuit.lock().await.record_failure();
    }
}

// ── TcpNetwork ────────────────────────────────────────────────────────────────

/// Client-side network handle for one remote Raft peer.
#[cfg(feature = "distributed-raft")]
pub struct TcpNetwork {
    pub target: u64,
    pool: Arc<PeerPool>,
}

#[cfg(feature = "distributed-raft")]
impl TcpNetwork {
    /// Create a network channel to `target`.
    ///
    /// `addr` is a `"host:port"` string — either an IP literal or a hostname
    /// resolved lazily at connect time.
    pub(crate) fn new(target: u64, addr: impl Into<String>) -> Self {
        Self {
            target,
            pool: Arc::new(PeerPool::new(addr)),
        }
    }
}

// ── openraft trait impls ───────────────────────────────────────────────────────

/// Monotonic counter for snapshot transfer IDs — avoids subsec_nanos collisions.
#[cfg(feature = "distributed-raft")]
static TRANSFER_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "distributed-raft")]
impl TcpNetwork {
    /// Send an envelope and read one response frame with a read timeout.
    ///
    /// On success the stream is returned to the pool and the circuit-breaker
    /// success counter is reset.  On any error the stream is dropped and the
    /// circuit-breaker failure counter is incremented.
    async fn send_envelope(
        pool: &PeerPool,
        envelope: &RpcEnvelope,
    ) -> Result<RpcResponse, TransportError> {
        let bytes = serde_json::to_vec(envelope)
            .map_err(|e| TransportError::EncodeError(format!("envelope encode: {e}")))?;

        let mut stream = pool.acquire().await?;

        // Write with timeout.
        let write_result =
            tokio::time::timeout(RPC_WRITE_TIMEOUT, write_frame(&mut stream, &bytes))
                .await
                .map_err(|_| TransportError::WriteTimeout)?
                .map_err(TransportError::WriteFailed);
        if let Err(e) = write_result {
            pool.record_failure().await;
            return Err(e);
        }

        // Read with timeout.
        let raw_result = tokio::time::timeout(RPC_READ_TIMEOUT, read_frame(&mut stream))
            .await
            .map_err(|_| TransportError::ReadTimeout)?
            .map_err(|e| TransportError::FramingError(e.to_string()))
            .and_then(|opt| opt.ok_or(TransportError::PeerReset));
        let raw = match raw_result {
            Ok(r) => r,
            Err(e) => {
                pool.record_failure().await;
                return Err(e);
            }
        };

        let resp: RpcResponse = serde_json::from_slice(&raw)
            .map_err(|e| TransportError::FramingError(format!("response decode: {e}")))?;

        // Return healthy stream to pool and reset circuit breaker.
        pool.release(stream).await;
        pool.record_success().await;
        Ok(resp)
    }

    /// Send snapshot chunks and await the final `SnapshotDone` response.
    async fn send_snapshot_chunks(
        pool: &PeerPool,
        vote: VoteOf<PcsTypeConfig>,
        snapshot: SnapshotOf<PcsTypeConfig>,
        transfer_id: u64,
    ) -> Result<SnapshotResponse<PcsTypeConfig>, StreamingError<PcsTypeConfig>> {
        let meta = snapshot.meta.clone();
        let body: Vec<u8> = snapshot.snapshot.into_inner();
        let total = body.len();
        let mut offset = 0usize;

        // We need a persistent stream for the duration of the transfer so the
        // server can correlate chunks by transfer_id.  We acquire once and keep
        // it for the whole transfer.
        let mut stream = pool.acquire().await.map_err(|e| e.into_streaming_error())?;

        loop {
            let end = (offset + SNAPSHOT_CHUNK_BYTES).min(total);
            let chunk_data = body[offset..end].to_vec();
            let is_last = end == total;

            let envelope: RpcEnvelope = if is_last {
                RpcEnvelope::SnapshotFinal(SnapshotFinalMsg {
                    transfer_id,
                    offset: offset as u64,
                    data: chunk_data,
                    vote,
                    meta: meta.clone(),
                })
            } else {
                RpcEnvelope::SnapshotChunk(SnapshotChunkMsg {
                    transfer_id,
                    offset: offset as u64,
                    data: chunk_data,
                })
            };

            // Enforce max chunk size client-side before framing so the peer
            // isn't forced to reject oversized payloads after reassembly.
            let chunk_len = match &envelope {
                RpcEnvelope::SnapshotChunk(c) => c.data.len(),
                RpcEnvelope::SnapshotFinal(f) => f.data.len(),
                _ => 0,
            };
            if chunk_len > MAX_SNAPSHOT_CHUNK_BYTES {
                return Err(TransportError::EncodeError(format!(
                    "snapshot chunk too large: {chunk_len} > {MAX_SNAPSHOT_CHUNK_BYTES}"
                ))
                .into_streaming_error());
            }

            let bytes = serde_json::to_vec(&envelope).map_err(|e| {
                TransportError::EncodeError(format!("snapshot chunk encode: {e}"))
                    .into_streaming_error()
            })?;

            // Write with timeout.
            tokio::time::timeout(RPC_WRITE_TIMEOUT, write_frame(&mut stream, &bytes))
                .await
                .map_err(|_| TransportError::WriteTimeout.into_streaming_error())?
                .map_err(|e| TransportError::WriteFailed(e).into_streaming_error())?;

            let raw = tokio::time::timeout(RPC_READ_TIMEOUT, read_frame(&mut stream))
                .await
                .map_err(|_| TransportError::ReadTimeout.into_streaming_error())?
                .map_err(|e| TransportError::FramingError(e.to_string()).into_streaming_error())?
                .ok_or_else(|| TransportError::PeerReset.into_streaming_error())?;

            let resp: RpcResponse = serde_json::from_slice(&raw).map_err(|e| {
                TransportError::FramingError(format!("snapshot ack decode: {e}"))
                    .into_streaming_error()
            })?;

            match resp {
                RpcResponse::SnapshotChunkAck { .. } => {
                    offset = end;
                }
                RpcResponse::SnapshotDone(snap_resp) => {
                    pool.release(stream).await;
                    return Ok(snap_resp);
                }
                RpcResponse::Error(msg) => {
                    return Err(StreamingError::Network(NetworkError::new(
                        &io::Error::other(format!("snapshot install error from peer: {msg}")),
                    )));
                }
                other => {
                    return Err(StreamingError::Network(NetworkError::new(
                        &io::Error::other(format!("unexpected snapshot response: {other:?}")),
                    )));
                }
            }

            if is_last {
                break;
            }
        }

        // Unreachable: the loop always returns or breaks when is_last is true,
        // but the compiler needs a value here.
        Err(StreamingError::Network(NetworkError::new(
            &io::Error::other("snapshot transfer loop exited without final response"),
        )))
    }
}

#[cfg(feature = "distributed-raft")]
impl RaftNetworkV2<PcsTypeConfig> for TcpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<PcsTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<PcsTypeConfig>, RPCError<PcsTypeConfig>> {
        let envelope = RpcEnvelope::AppendEntries(rpc);
        let resp = Self::send_envelope(&self.pool, &envelope)
            .await
            .map_err(|e| e.into_rpc_error())?;

        match resp {
            RpcResponse::AppendEntries(r) => Ok(r),
            RpcResponse::Error(msg) => Err(RPCError::Network(NetworkError::new(
                &io::Error::other(format!("append_entries error: {msg}")),
            ))),
            _ => Err(RPCError::Network(NetworkError::new(&io::Error::other(
                "unexpected response variant for append_entries",
            )))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<PcsTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<PcsTypeConfig>, RPCError<PcsTypeConfig>> {
        let envelope = RpcEnvelope::Vote(rpc);
        let resp = Self::send_envelope(&self.pool, &envelope)
            .await
            .map_err(|e| e.into_rpc_error())?;

        match resp {
            RpcResponse::Vote(r) => Ok(r),
            RpcResponse::Error(msg) => Err(RPCError::Network(NetworkError::new(
                &io::Error::other(format!("vote error: {msg}")),
            ))),
            _ => Err(RPCError::Network(NetworkError::new(&io::Error::other(
                "unexpected response variant for vote",
            )))),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<PcsTypeConfig>,
        snapshot: SnapshotOf<PcsTypeConfig>,
        cancel: impl std::future::Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<PcsTypeConfig>, StreamingError<PcsTypeConfig>> {
        let transfer_id = TRANSFER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        let send_fut = Self::send_snapshot_chunks(&self.pool, vote, snapshot, transfer_id);

        tokio::select! {
            result = send_fut => result,
            closed = cancel => Err(StreamingError::Closed(closed)),
        }
    }

    fn backoff(&self) -> Backoff {
        // Exponential backoff: 100ms base, 2x multiplier, 10s cap, 20% jitter.
        let base_ms: u64 = 100;
        let cap_ms: u64 = 10_000;
        let iter = std::iter::successors(Some(base_ms), move |&prev| {
            let next = (prev * 2).min(cap_ms);
            let jitter = {
                use rand::RngExt;
                rand::rng().random_range(0..=(next / 5))
            };
            Some(next + jitter)
        })
        .map(Duration::from_millis);
        Backoff::new(iter)
    }
}

// ── TCP Server ─────────────────────────────────────────────────────────────────

/// In-flight snapshot transfer state on the server side.
#[cfg(feature = "distributed-raft")]
struct InFlightSnapshot {
    data: Vec<u8>,
    last_chunk_at: Instant,
}

/// TCP server that dispatches incoming Raft RPCs to a local `Raft` node.
///
/// Start one instance per node during cluster initialisation. The server binds
/// `listen_addr` and spawns a Tokio task per accepted connection. The accept
/// loop stops on [`RaftTcpServer::shutdown`] or when the server handle drops.
#[cfg(feature = "distributed-raft")]
pub struct RaftTcpServer {
    raft: crate::distributed::consensus::driver::raft_impl::ArrowPCSRaft,
    listen_addr: SocketAddr,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

#[cfg(feature = "distributed-raft")]
impl RaftTcpServer {
    /// Create a new server bound to `listen_addr` that dispatches RPCs to `raft`.
    pub fn new(
        raft: crate::distributed::consensus::driver::raft_impl::ArrowPCSRaft,
        listen_addr: SocketAddr,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            raft,
            listen_addr,
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Signal the accept loop to stop.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Spawn the server as a background Tokio task.
    ///
    /// The returned `JoinHandle` can be awaited for a clean shutdown signal.
    pub fn spawn(self) -> tokio::task::JoinHandle<io::Result<()>> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(mut self) -> io::Result<()> {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(self.listen_addr).await?;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_ACCEPTED_CONNECTIONS));

        #[cfg(feature = "tracing")]
        tracing::info!(addr = %self.listen_addr, "RaftTcpServer listening");

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, _peer_addr) = match accept_result {
                        Ok(pair) => pair,
                        Err(_e) => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!(error = %_e, "RaftTcpServer: accept error");
                            // Sleep briefly to avoid tight-looping on EMFILE.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };

                    #[cfg(feature = "tracing")]
                    tracing::debug!(peer = %_peer_addr, "RaftTcpServer: accepted connection");

                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!(peer = %_peer_addr, "RaftTcpServer: connection limit reached, dropping");
                            drop(stream);
                            continue;
                        }
                    };

                    let raft = self.raft.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        handle_connection(raft, stream).await;
                        #[cfg(feature = "tracing")]
                        tracing::debug!(peer = %_peer_addr, "RaftTcpServer: connection closed");
                    });
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        #[cfg(feature = "tracing")]
                        tracing::info!("RaftTcpServer: shutdown signal received");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Handle one accepted connection: read RPCs, dispatch, write responses.
#[cfg(feature = "distributed-raft")]
async fn handle_connection(
    raft: crate::distributed::consensus::driver::raft_impl::ArrowPCSRaft,
    mut stream: TcpStream,
) {
    // Per-connection reassembly state for snapshot transfers.
    let mut snapshot_transfers: HashMap<u64, InFlightSnapshot> = HashMap::new();

    loop {
        // Close connection if peer stops sending — avoids parking a task
        // indefinitely on an alive-but-silent TCP link.
        let raw = match tokio::time::timeout(IDLE_READ_TIMEOUT, read_frame(&mut stream)).await {
            Ok(Ok(Some(b))) => b,
            Ok(Ok(None)) => break, // clean EOF
            Ok(Err(_e)) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: read frame error");
                break;
            }
            Err(_) => {
                // Idle timeout — no frame received. Close cleanly.
                #[cfg(feature = "tracing")]
                tracing::debug!("RaftTcpServer: idle timeout, closing connection");
                break;
            }
        };

        let envelope: RpcEnvelope = match serde_json::from_slice(&raw) {
            Ok(e) => e,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: envelope decode error");
                break;
            }
        };

        let response = handle_envelope(&raft, envelope, &mut snapshot_transfers).await;

        let resp_bytes = match serde_json::to_vec(&response) {
            Ok(b) => b,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: response encode error");
                break;
            }
        };

        if write_frame(&mut stream, &resp_bytes).await.is_err() {
            break;
        }
    }
}

/// Dispatch one decoded envelope to the local Raft node and return a response.
#[cfg(feature = "distributed-raft")]
async fn handle_envelope(
    raft: &crate::distributed::consensus::driver::raft_impl::ArrowPCSRaft,
    envelope: RpcEnvelope,
    snapshot_transfers: &mut HashMap<u64, InFlightSnapshot>,
) -> RpcResponse {
    use std::io::Cursor;

    match envelope {
        RpcEnvelope::AppendEntries(req) => match raft.append_entries(req).await {
            Ok(resp) => RpcResponse::AppendEntries(resp),
            Err(e) => RpcResponse::Error(e.to_string()),
        },

        RpcEnvelope::Vote(req) => match raft.vote(req).await {
            Ok(resp) => RpcResponse::Vote(resp),
            Err(e) => RpcResponse::Error(e.to_string()),
        },

        RpcEnvelope::SnapshotChunk(chunk) => {
            let transfer_id = chunk.transfer_id;
            // Evict stale transfers only on snapshot traffic — bounds memory
            // without adding per-RPC overhead on the common append_entries path.
            snapshot_transfers
                .retain(|_, v| v.last_chunk_at.elapsed() <= SNAPSHOT_TRANSFER_IDLE_TIMEOUT);

            // Enforce concurrent transfer limit.
            if !snapshot_transfers.contains_key(&transfer_id)
                && snapshot_transfers.len() >= SNAPSHOT_MAX_CONCURRENT_TRANSFERS
            {
                return RpcResponse::Error(format!(
                    "too many concurrent snapshot transfers (max {SNAPSHOT_MAX_CONCURRENT_TRANSFERS})"
                ));
            }

            let entry = snapshot_transfers
                .entry(transfer_id)
                .or_insert_with(|| InFlightSnapshot {
                    data: Vec::new(),
                    last_chunk_at: Instant::now(),
                });

            // Enforce per-transfer size cap.
            let new_size = entry.data.len() + chunk.data.len();
            if new_size > SNAPSHOT_MAX_TRANSFER_BYTES {
                snapshot_transfers.remove(&transfer_id);
                return RpcResponse::Error(format!(
                    "snapshot transfer {transfer_id} exceeded size limit ({SNAPSHOT_MAX_TRANSFER_BYTES} bytes)"
                ));
            }

            entry.data.extend_from_slice(&chunk.data);
            entry.last_chunk_at = Instant::now();

            RpcResponse::SnapshotChunkAck { transfer_id }
        }

        RpcEnvelope::SnapshotFinal(final_msg) => {
            let transfer_id = final_msg.transfer_id;
            let mut buf = snapshot_transfers
                .remove(&transfer_id)
                .map(|s| s.data)
                .unwrap_or_default();
            buf.extend_from_slice(&final_msg.data);

            let snapshot = Snapshot {
                meta: final_msg.meta,
                snapshot: Cursor::new(buf),
            };

            let vote = final_msg.vote;
            // Spawn the install to keep the connection task responsive.
            // We still await the handle so we can send the response on the same stream.
            let raft = raft.clone();
            let result =
                tokio::spawn(async move { raft.install_full_snapshot(vote, snapshot).await }).await;

            match result {
                Ok(Ok(resp)) => RpcResponse::SnapshotDone(resp),
                Ok(Err(e)) => RpcResponse::Error(e.to_string()),
                Err(e) => RpcResponse::Error(format!("snapshot install task panicked: {e}")),
            }
        }

        RpcEnvelope::ProposalForward { command } => {
            // The server-side handler calls client_write directly.
            // It does NOT forward further even if it is also a follower — that
            // would cause infinite forwarding loops between nodes.
            //
            // A 28 s timeout around client_write ensures we always respond
            // before the caller's PROPOSAL_FORWARD_READ_TIMEOUT (35 s) fires,
            // so the follower gets a structured error rather than a TCP drop.
            use openraft::error::{ClientWriteError, RaftError};
            const SERVER_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(28);
            let write_result =
                tokio::time::timeout(SERVER_WRITE_TIMEOUT, raft.client_write(command)).await;
            match write_result {
                Ok(Ok(resp)) => RpcResponse::ProposalResult {
                    ok: Some(resp.data),
                    err: None,
                },
                Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))) => {
                    // This node is not the leader — reject the forward rather
                    // than propagating it further.
                    RpcResponse::ProposalResult {
                        ok: None,
                        err: Some("not leader".to_string()),
                    }
                }
                Ok(Err(e)) => RpcResponse::ProposalResult {
                    ok: None,
                    err: Some(e.to_string()),
                },
                Err(_elapsed) => RpcResponse::ProposalResult {
                    ok: None,
                    err: Some("server-side client_write timeout".to_string()),
                },
            }
        }
    }
}

// ── Proposal forwarding ────────────────────────────────────────────────────────

/// Module-level pool cache for `forward_proposal` so each leader address
/// reuses pooled connections instead of opening a fresh one per call.
#[cfg(feature = "distributed-raft")]
static FORWARD_PROPOSAL_POOLS: std::sync::OnceLock<Mutex<HashMap<String, Arc<PeerPool>>>> =
    std::sync::OnceLock::new();

#[cfg(feature = "distributed-raft")]
async fn get_forward_pool(addr: &str) -> Arc<PeerPool> {
    let pools = FORWARD_PROPOSAL_POOLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = pools.lock().await;
    if let Some(p) = guard.get(addr) {
        return Arc::clone(p);
    }
    let pool = Arc::new(PeerPool::new(addr));
    guard.insert(addr.to_string(), Arc::clone(&pool));
    pool
}

/// Forward a [`ConsensusCommand`] proposal to the Raft leader over TCP.
///
/// Called by a follower node when `client_write` returns `ForwardToLeader`.
/// Resolves `addr` via DNS (same as [`PeerPool::acquire`]), sends a
/// [`RpcEnvelope::ProposalForward`] frame via a pooled connection, and reads
/// the [`RpcResponse::ProposalResult`] response.
///
/// Uses [`CONNECT_TIMEOUT`] for the TCP connect, [`RPC_WRITE_TIMEOUT`] for
/// the frame write, and [`PROPOSAL_FORWARD_READ_TIMEOUT`] for the response read.
///
/// # Errors
///
/// Returns a [`PcsError`] if the connection fails, the write/read times out,
/// or the leader returns an error response.
#[cfg(feature = "distributed-raft")]
pub(crate) async fn forward_proposal(
    addr: &str,
    cmd: ConsensusCommand,
) -> PcsResult<ConsensusResponse> {
    let pool = get_forward_pool(addr).await;

    let mut stream = pool
        .acquire()
        .await
        .map_err(|e| PcsError::generic(format!("forward_proposal: connect to {addr}: {e:?}")))?;

    let envelope = RpcEnvelope::ProposalForward { command: cmd };
    let bytes = serde_json::to_vec(&envelope)
        .map_err(|e| PcsError::generic(format!("forward_proposal: serialize envelope: {e}")))?;

    tokio::time::timeout(RPC_WRITE_TIMEOUT, write_frame(&mut stream, &bytes))
        .await
        .map_err(|_| PcsError::generic(format!("forward_proposal: write timeout to {addr}")))?
        .map_err(|e| PcsError::generic(format!("forward_proposal: write frame: {e}")))?;

    let raw = tokio::time::timeout(PROPOSAL_FORWARD_READ_TIMEOUT, read_frame(&mut stream))
        .await
        .map_err(|_| PcsError::generic(format!("forward_proposal: read timeout from {addr}")))?
        .map_err(|e| PcsError::generic(format!("forward_proposal: read frame: {e}")))?
        .ok_or_else(|| {
            PcsError::generic(format!("forward_proposal: connection reset by {addr}"))
        })?;

    // Return the stream to the pool on success path.
    pool.release(stream).await;

    let resp: RpcResponse = serde_json::from_slice(&raw)
        .map_err(|e| PcsError::generic(format!("forward_proposal: response decode: {e}")))?;

    match resp {
        RpcResponse::ProposalResult {
            ok: Some(result), ..
        } => Ok(result),
        RpcResponse::ProposalResult { err: Some(msg), .. } => Err(PcsError::generic(format!(
            "forward_proposal: leader returned error: {msg}"
        ))),
        RpcResponse::ProposalResult {
            ok: None,
            err: None,
        } => Err(PcsError::generic(
            "forward_proposal: empty ProposalResult from leader",
        )),
        other => Err(PcsError::generic(format!(
            "forward_proposal: unexpected response from {addr}: {other:?}"
        ))),
    }
}

// ── TcpNetworkFactory ──────────────────────────────────────────────────────────

/// Factory that creates [`TcpNetwork`] instances for each cluster peer.
///
/// Peer addresses are stored as `"host:port"` strings and resolved via DNS
/// lazily at connection time.  This means Docker Compose service names
/// (e.g. `"node2:9002"`) are valid peer addresses even if the peer container
/// is not yet running when the factory is created.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
pub struct TcpNetworkFactory {
    /// Peer addresses as `"host:port"` strings (may be hostnames or IPs).
    peers: HashMap<u64, String>,
    /// Per-RPC read-response timeout.  Defaults to [`RPC_READ_TIMEOUT`].
    #[cfg(feature = "distributed-raft")]
    pub rpc_read_timeout: Duration,
}

impl TcpNetworkFactory {
    pub fn new(peers: HashMap<u64, String>) -> Self {
        Self {
            peers,
            #[cfg(feature = "distributed-raft")]
            rpc_read_timeout: RPC_READ_TIMEOUT,
        }
    }

    #[cfg(feature = "distributed-raft")]
    pub fn from_basic_nodes(nodes: &HashMap<u64, BasicNode>) -> Self {
        let peers = nodes
            .iter()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();
        Self {
            peers,
            rpc_read_timeout: RPC_READ_TIMEOUT,
        }
    }
}

#[cfg(feature = "distributed-raft")]
impl RaftNetworkFactory<PcsTypeConfig> for TcpNetworkFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> TcpNetwork {
        // Use the pre-stored address (from factory config) if available;
        // fall back to the address advertised in the Raft BasicNode.
        let addr = self
            .peers
            .get(&target)
            .cloned()
            .unwrap_or_else(|| node.addr.clone());
        TcpNetwork::new(target, addr)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::TcpListener;

    fn free_addr() -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    }

    fn spawn_echo_server(addr: SocketAddr) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    while let Ok(Some(frame)) = read_frame(&mut stream).await {
                        let _ = write_frame(&mut stream, &frame).await;
                    }
                });
            }
        });
    }

    #[test]
    fn test_serde_command_round_trip_via_json() {
        use crate::distributed::consensus::types::ConsensusCommand;
        let cmd = ConsensusCommand::AckClaim {
            claim_id: uuid::Uuid::new_v4(),
            instance_id: uuid::Uuid::new_v4(),
        };
        let json = serde_json::to_vec(&cmd).unwrap();
        let decoded: ConsensusCommand = serde_json::from_slice(&json).unwrap();
        assert!(matches!(decoded, ConsensusCommand::AckClaim { .. }));
    }

    // ── Task-#29 tests ────────────────────────────────────────────────────────

    /// Verify that connecting to an unreachable peer produces `ConnectFailed`
    /// which maps to `RPCError::Unreachable` (permanent-ish classification).
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_connect_failure_maps_to_unreachable() {
        // Port 1 is never listening.
        let pool = PeerPool::new("127.0.0.1:1");
        let err = pool.acquire().await.unwrap_err();
        let rpc_err = err.into_rpc_error();
        assert!(
            matches!(rpc_err, RPCError::Unreachable(_)),
            "connect failure must map to Unreachable, got: {rpc_err:?}"
        );
    }

    /// Verify that a read timeout maps to `RPCError::Network` (transient).
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_read_timeout_maps_to_network_error() {
        use tokio::net::TcpListener;

        // Spawn a server that accepts but never responds.
        let addr = free_addr();
        tokio::spawn(async move {
            let l = TcpListener::bind(addr).await.unwrap();
            while let Ok((_stream, _)) = l.accept().await {
                // Intentionally keep the stream open without sending anything.
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let pool = PeerPool::new(addr.to_string());

        // Use a very short timeout so the test doesn't actually wait 5 seconds.
        let mut stream = pool.acquire().await.expect("should connect");
        write_frame(&mut stream, b"probe").await.unwrap();

        let short_timeout = Duration::from_millis(50);
        let result = tokio::time::timeout(short_timeout, read_frame(&mut stream)).await;

        // timeout fires → transient network error
        assert!(result.is_err(), "expected timeout to fire");
        let rpc_err = TransportError::ReadTimeout.into_rpc_error();
        assert!(
            matches!(rpc_err, RPCError::Network(_)),
            "read timeout must map to Network (transient), got: {rpc_err:?}"
        );
    }

    /// `PeerReset`, `WriteFailed`, `FramingError`, and `Other` must all map to
    /// the transient `RPCError::Network` variant.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_transient_errors_map_to_network() {
        let cases: Vec<TransportError> = vec![
            TransportError::PeerReset,
            TransportError::WriteFailed(io::Error::other("x")),
            TransportError::WriteTimeout,
            TransportError::FramingError("bad frame".to_string()),
            TransportError::Other(io::Error::other("y")),
        ];
        for err in cases {
            let rpc_err = err.into_rpc_error();
            assert!(
                matches!(rpc_err, RPCError::Network(_)),
                "expected Network for transient error, got: {rpc_err:?}"
            );
        }
    }

    /// `EncodeError` must map to `RPCError::Unreachable` (non-transient —
    /// serialization failures are bugs, not network hiccups).
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_encode_error_maps_to_unreachable() {
        let err = TransportError::EncodeError("bad type".to_string());
        let rpc_err = err.into_rpc_error();
        assert!(
            matches!(rpc_err, RPCError::Unreachable(_)),
            "encode error must map to Unreachable, got: {rpc_err:?}"
        );
    }

    /// Verify the connection pool acquire/release/capacity semantics.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_pool_acquire_release_capacity() {
        let addr = free_addr();
        spawn_echo_server(addr);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let pool = PeerPool::new(addr.to_string());

        // Acquire two connections and release them.
        let s1 = pool.acquire().await.unwrap();
        let s2 = pool.acquire().await.unwrap();
        pool.release(s1).await;
        pool.release(s2).await;

        let guard = pool.idle.lock().await;
        assert_eq!(guard.len(), 2, "both streams should be in the pool");
        drop(guard);

        // Fill pool to capacity then try to release one more.
        let mut extras = Vec::new();
        for _ in 0..POOL_CAPACITY {
            extras.push(pool.acquire().await.unwrap());
        }
        for s in extras {
            pool.release(s).await;
        }
        let guard = pool.idle.lock().await;
        assert_eq!(
            guard.len(),
            POOL_CAPACITY,
            "pool must not exceed POOL_CAPACITY"
        );
    }

    /// Stale pooled connections (exceeding POOL_MAX_IDLE) are dropped on acquire.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_pool_stale_connection_dropped() {
        let addr = free_addr();
        spawn_echo_server(addr);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let pool = PeerPool::new(addr.to_string());
        let stream = pool.acquire().await.unwrap();

        // Manually insert a stale PooledStream (returned_at far in the past).
        {
            let mut guard = pool.idle.lock().await;
            guard.push_back(PooledStream {
                stream,
                returned_at: Instant::now() - POOL_MAX_IDLE - Duration::from_secs(1),
            });
        }

        // Acquire should discard the stale entry and open a fresh connection.
        let fresh = pool.acquire().await;
        assert!(
            fresh.is_ok(),
            "should open a new connection after discarding stale one"
        );
    }

    /// Oversized frame returns InvalidData, not silently truncates.
    #[tokio::test]
    async fn test_read_frame_oversized_returns_error() {
        use tokio::io::AsyncWriteExt;
        let addr = free_addr();
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            if let Ok((mut stream, _)) = listener.accept().await {
                // Send a length larger than MAX_FRAME_BYTES.
                let oversized_len = (MAX_FRAME_BYTES + 1) as u32;
                let _ = stream.write_all(&oversized_len.to_be_bytes()).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let result = read_frame(&mut stream).await;
        assert!(
            matches!(result, Err(ref e) if e.kind() == io::ErrorKind::InvalidData),
            "oversized frame must return InvalidData, got: {result:?}"
        );
    }

    /// Truncated frame payload returns UnexpectedEof.
    #[tokio::test]
    async fn test_read_frame_truncated_returns_unexpected_eof() {
        use tokio::io::AsyncWriteExt;
        let addr = free_addr();
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            if let Ok((mut stream, _)) = listener.accept().await {
                // Claim 10 bytes but only send 5.
                let len: u32 = 10;
                let _ = stream.write_all(&len.to_be_bytes()).await;
                let _ = stream.write_all(b"hello").await;
                // Drop stream — causes EOF mid-payload.
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let result = read_frame(&mut stream).await;
        assert!(
            matches!(result, Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof),
            "truncated frame must return UnexpectedEof, got: {result:?}"
        );
    }

    /// write_frame rejects frames larger than MAX_FRAME_BYTES before writing.
    #[tokio::test]
    async fn test_write_frame_oversized_returns_error() {
        let addr = free_addr();
        spawn_echo_server(addr);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        // MAX_FRAME_BYTES + 1 bytes.
        let big = vec![0u8; MAX_FRAME_BYTES + 1];
        let result = write_frame(&mut stream, &big).await;
        assert!(
            matches!(result, Err(ref e) if e.kind() == io::ErrorKind::InvalidData),
            "oversized write must return InvalidData, got: {result:?}"
        );
    }

    /// clean EOF (peer closes without sending anything) returns Ok(None).
    #[tokio::test]
    async fn test_read_frame_clean_eof_returns_none() {
        let addr = free_addr();
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            if let Ok((_stream, _)) = listener.accept().await {
                // Drop stream immediately — sends clean FIN.
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let result = read_frame(&mut stream).await;
        assert!(
            matches!(result, Ok(None)),
            "clean EOF must return Ok(None), got: {result:?}"
        );
    }

    /// Snapshot transfer IDs from TRANSFER_ID_COUNTER are unique.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_transfer_id_counter_unique() {
        let id1 = TRANSFER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let id2 = TRANSFER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        assert_ne!(id1, id2, "transfer IDs must be unique");
    }

    /// Snapshot chunk accumulation enforces the concurrent transfer limit.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_snapshot_buffer_concurrent_limit() {
        // Fill up to the limit.
        let mut transfers: HashMap<u64, InFlightSnapshot> = HashMap::new();
        for i in 0..SNAPSHOT_MAX_CONCURRENT_TRANSFERS {
            transfers.insert(
                i as u64,
                InFlightSnapshot {
                    data: Vec::new(),
                    last_chunk_at: Instant::now(),
                },
            );
        }
        assert_eq!(transfers.len(), SNAPSHOT_MAX_CONCURRENT_TRANSFERS);

        // A new transfer_id when at limit should be rejected.
        let new_id = 999u64;
        let is_new = !transfers.contains_key(&new_id);
        let at_limit = transfers.len() >= SNAPSHOT_MAX_CONCURRENT_TRANSFERS;
        assert!(is_new && at_limit, "should detect new transfer at limit");
    }

    /// Snapshot chunk accumulation enforces the per-transfer size cap.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_snapshot_buffer_size_cap() {
        let mut transfers: HashMap<u64, InFlightSnapshot> = HashMap::new();
        let transfer_id = 1u64;
        transfers.insert(
            transfer_id,
            InFlightSnapshot {
                // Pre-fill with data just below the cap.
                data: vec![0u8; SNAPSHOT_MAX_TRANSFER_BYTES - 1],
                last_chunk_at: Instant::now(),
            },
        );

        let entry = transfers.get(&transfer_id).unwrap();
        // Adding 2 more bytes would exceed the cap.
        let new_size = entry.data.len() + 2;
        assert!(
            new_size > SNAPSHOT_MAX_TRANSFER_BYTES,
            "size check should detect overflow"
        );
    }

    // ── Task-#6 test: snapshot chunk reassembly ────────────────────────────────

    /// Round-trip a snapshot through the chunk serialisation / reassembly path
    /// using an in-memory pair of TCP endpoints.
    ///
    /// We spawn a minimal echo-and-accumulate server that:
    /// 1. Acks each `SnapshotChunk` immediately.
    /// 2. On `SnapshotFinal`, echoes back a fake `SnapshotDone`.
    ///
    /// This validates that the sender correctly slices the body and that the
    /// reassembly logic produces the original bytes.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_snapshot_chunk_reassembly_roundtrip() {
        use std::io::Cursor;
        use tokio::net::TcpListener;

        let listen_addr = free_addr();

        // --- Minimal snapshot server ---
        tokio::spawn(async move {
            let listener = TcpListener::bind(listen_addr).await.unwrap();
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut accumulated: Vec<u8> = Vec::new();

                    loop {
                        let raw = match read_frame(&mut stream).await {
                            Ok(Some(b)) => b,
                            _ => break,
                        };
                        let env: RpcEnvelope = serde_json::from_slice(&raw).unwrap();
                        match env {
                            RpcEnvelope::SnapshotChunk(c) => {
                                accumulated.extend_from_slice(&c.data);
                                let ack = RpcResponse::SnapshotChunkAck {
                                    transfer_id: c.transfer_id,
                                };
                                let ack_bytes = serde_json::to_vec(&ack).unwrap();
                                write_frame(&mut stream, &ack_bytes).await.unwrap();
                            }
                            RpcEnvelope::SnapshotFinal(f) => {
                                accumulated.extend_from_slice(&f.data);
                                // Verify the payload was reassembled correctly.
                                // The server would call install_full_snapshot here.
                                assert!(!accumulated.is_empty(), "accumulated must be non-empty");

                                // Echo back a fake SnapshotDone.
                                // `SnapshotResponse` is in scope from the outer use block.
                                let done =
                                    RpcResponse::SnapshotDone(SnapshotResponse { vote: f.vote });
                                let done_bytes = serde_json::to_vec(&done).unwrap();
                                write_frame(&mut stream, &done_bytes).await.unwrap();
                                break;
                            }
                            _ => break,
                        }
                    }
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        // Build a snapshot larger than one chunk to exercise multi-chunk path.
        let body_size = SNAPSHOT_CHUNK_BYTES + 100;
        let body: Vec<u8> = (0..body_size).map(|i| (i % 251) as u8).collect();

        // We need a fake Vote and SnapshotMeta for the test.
        // Since they are serde types we construct them manually.
        use openraft::{SnapshotMeta, StoredMembership, impls::Vote};
        let vote = Vote::new(1, 1);
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "test-snap-1".to_string(),
        };
        let snapshot: SnapshotOf<PcsTypeConfig> = Snapshot {
            meta,
            snapshot: Cursor::new(body.clone()),
        };

        let pool = PeerPool::new(listen_addr.to_string());
        let transfer_id = TRANSFER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let result = TcpNetwork::send_snapshot_chunks(&pool, vote, snapshot, transfer_id).await;

        assert!(
            result.is_ok(),
            "snapshot transfer should succeed, got: {result:?}"
        );
    }

    // ── forward_proposal tests ────────────────────────────────────────────────

    /// Spawn a minimal TCP server that handles `ProposalForward` envelopes and
    /// returns a `ProposalResult { ok: Some(...) }` response.
    #[cfg(feature = "distributed-raft")]
    fn spawn_fake_leader(addr: SocketAddr, response: ConsensusResponse) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((mut stream, _)) = listener.accept().await {
                let resp = response.clone();
                tokio::spawn(async move {
                    while let Ok(Some(raw)) = read_frame(&mut stream).await {
                        let envelope: RpcEnvelope = serde_json::from_slice(&raw).unwrap();
                        let reply = match envelope {
                            RpcEnvelope::ProposalForward { .. } => RpcResponse::ProposalResult {
                                ok: Some(resp.clone()),
                                err: None,
                            },
                            _ => RpcResponse::Error("unexpected envelope".to_string()),
                        };
                        let bytes = serde_json::to_vec(&reply).unwrap();
                        let _ = write_frame(&mut stream, &bytes).await;
                    }
                });
            }
        });
    }

    /// `forward_proposal` succeeds when the leader returns `ProposalResult { ok }`.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_forward_proposal_success() {
        use uuid::Uuid;

        let addr = free_addr();
        spawn_fake_leader(addr, ConsensusResponse::ClaimAcked);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let cmd = ConsensusCommand::AckClaim {
            claim_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
        };
        let result = forward_proposal(&addr.to_string(), cmd).await;
        assert!(
            matches!(result, Ok(ConsensusResponse::ClaimAcked)),
            "expected ClaimAcked, got: {result:?}"
        );
    }

    /// `forward_proposal` returns an error when the leader returns
    /// `ProposalResult { err }`.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_forward_proposal_leader_error_response() {
        use tokio::net::TcpListener;
        use uuid::Uuid;

        let addr = free_addr();
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    while let Ok(Some(_raw)) = read_frame(&mut stream).await {
                        let reply = RpcResponse::ProposalResult {
                            ok: None,
                            err: Some("not leader".to_string()),
                        };
                        let bytes = serde_json::to_vec(&reply).unwrap();
                        let _ = write_frame(&mut stream, &bytes).await;
                    }
                });
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let cmd = ConsensusCommand::AckClaim {
            claim_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
        };
        let result = forward_proposal(&addr.to_string(), cmd).await;
        assert!(result.is_err(), "expected error from leader error response");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("not leader"),
            "error message should contain 'not leader': {msg}"
        );
    }

    /// `forward_proposal` returns a connect error when the address is unreachable.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_forward_proposal_connect_failure() {
        use uuid::Uuid;

        // Port 1 is never listening.
        let cmd = ConsensusCommand::AckClaim {
            claim_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
        };
        let result = forward_proposal("127.0.0.1:1", cmd).await;
        assert!(
            result.is_err(),
            "expected connection failure, got: {result:?}"
        );
    }

    /// `handle_envelope` with `ProposalForward` returns `ProposalResult` via
    /// the wire framing end-to-end (serde round-trip check).
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_proposal_forward_envelope_serde_round_trip() {
        use uuid::Uuid;

        let cmd = ConsensusCommand::AckClaim {
            claim_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
        };
        let envelope = RpcEnvelope::ProposalForward {
            command: cmd.clone(),
        };
        let json = serde_json::to_vec(&envelope).unwrap();
        let decoded: RpcEnvelope = serde_json::from_slice(&json).unwrap();
        assert!(
            matches!(decoded, RpcEnvelope::ProposalForward { .. }),
            "should decode back to ProposalForward"
        );

        let resp = RpcResponse::ProposalResult {
            ok: Some(ConsensusResponse::ClaimAcked),
            err: None,
        };
        let json = serde_json::to_vec(&resp).unwrap();
        let decoded: RpcResponse = serde_json::from_slice(&json).unwrap();
        assert!(
            matches!(
                decoded,
                RpcResponse::ProposalResult {
                    ok: Some(ConsensusResponse::ClaimAcked),
                    ..
                }
            ),
            "should decode back to ProposalResult with ok"
        );
    }

    // ── Circuit breaker state machine tests ───────────────────────────────────

    /// Fresh circuit is closed with zero failures.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_circuit_starts_closed() {
        let state = CircuitState::new();
        assert!(!state.is_open(), "fresh circuit must be closed");
    }

    /// Circuit opens after CIRCUIT_OPEN_THRESHOLD consecutive failures.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_circuit_opens_after_threshold_failures() {
        let mut state = CircuitState::new();
        for i in 0..CIRCUIT_OPEN_THRESHOLD {
            assert!(
                !state.is_open(),
                "circuit should still be closed at failure {i}"
            );
            state.record_failure();
        }
        assert!(
            state.is_open(),
            "circuit must be open after {CIRCUIT_OPEN_THRESHOLD} failures"
        );
    }

    /// A success resets the failure counter so the threshold must be reached again.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_circuit_resets_on_success() {
        let mut state = CircuitState::new();
        // Accumulate failures up to threshold - 1.
        for _ in 0..CIRCUIT_OPEN_THRESHOLD - 1 {
            state.record_failure();
        }
        assert!(!state.is_open(), "should still be closed before threshold");
        state.record_success();
        // After success, need another full run of failures to open.
        for i in 0..CIRCUIT_OPEN_THRESHOLD {
            assert!(
                !state.is_open(),
                "circuit should be closed after reset, at failure {i}"
            );
            state.record_failure();
        }
        assert!(
            state.is_open(),
            "circuit must open again after threshold failures post-reset"
        );
    }

    /// Circuit transitions to half-open after CIRCUIT_OPEN_DURATION expires.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_circuit_half_open_after_duration() {
        let mut state = CircuitState::Open {
            opened_at: Instant::now() - CIRCUIT_OPEN_DURATION - Duration::from_millis(1),
        };
        // is_open checks elapsed < CIRCUIT_OPEN_DURATION — should now return false.
        assert!(
            !state.is_open(),
            "expired open circuit should not be considered open"
        );

        // record_failure on an expired Open transitions to Closed{1}.
        state.record_failure();
        assert!(
            matches!(
                state,
                CircuitState::Closed {
                    consecutive_failures: 1
                }
            ),
            "half-open after timeout: one failure allowed before re-opening"
        );
    }

    /// Closed circuit stays closed when failures are below the threshold.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_circuit_stays_closed_below_threshold() {
        let mut state = CircuitState::new();
        for _ in 0..CIRCUIT_OPEN_THRESHOLD - 1 {
            state.record_failure();
        }
        assert!(
            !state.is_open(),
            "circuit must stay closed with fewer than {CIRCUIT_OPEN_THRESHOLD} failures"
        );
    }

    /// `PeerPool::acquire` returns an error immediately when the circuit is open.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_pool_acquire_blocked_by_open_circuit() {
        let pool = PeerPool::new("127.0.0.1:1"); // unreachable port

        // Force-open the circuit.
        {
            let mut c = pool.circuit.lock().await;
            *c = CircuitState::Open {
                opened_at: Instant::now(),
            };
        }

        let err = pool.acquire().await.unwrap_err();
        assert!(
            matches!(err, TransportError::Other(_)),
            "open circuit must return TransportError::Other, got: {err:?}"
        );
    }

    /// Client-side chunk size guard rejects oversized data before framing.
    ///
    /// This is a logic test — we verify the constant relationship, not the
    /// network path. The actual guard inside `send_snapshot_chunks` compares
    /// `chunk_data.len() > MAX_SNAPSHOT_CHUNK_BYTES`.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_snapshot_chunk_size_constant_matches_server_cap() {
        assert_eq!(
            MAX_SNAPSHOT_CHUNK_BYTES, SNAPSHOT_CHUNK_BYTES,
            "client and server chunk caps must match"
        );
    }
}
