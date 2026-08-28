//! TCP transport for Arrow-IPC Raft consensus messages.
//!
//! Length-prefixed framing with a typed envelope that distinguishes message
//! kinds on the wire:
//!
//! - Raft protocol messages travel as raw prost-encoded `eraftpb::Message`
//!   bytes (the raft `prost-codec` wire format), fire-and-forget.
//! - Forwarded proposals ride a separate tag, answered on the same connection.
//!
//! Each payload is `1 tag byte + body` (see `wire`) inside a
//! length-prefixed frame. The wire format is **append-only**: existing tag
//! values must never change, so rolling upgrades stay compatible.
//!
//! ## Layout
//!
//! This file owns the shared frame and circuit constants plus the
//! [`TransportError`] classification. `wire` holds the on-wire message types
//! and the frame codec, `client` the outbound half ([`TransportHub`], the
//! per-peer pool, proposal forwarding), and `server` the inbound half
//! ([`RaftTcpServer`]).
//!
//! [`RaftTcpServer`] binds a listen address and dispatches incoming envelopes
//! to the local driver's inbox. Start it once during node initialisation,
//! before any remote peer can contact the node.
//!
//! [`TransportHub`] keeps a per-peer pool of idle
//! [`TcpStream`](tokio::net::TcpStream)s bounded by [`POOL_CAPACITY`]. Streams
//! are acquired from the pool or freshly connected, returned on success, and
//! dropped on error so a broken stream never re-enters the pool.
//!
//! Read-frame calls on the RPC-response path use [`tokio::time::timeout`] with
//! a deadline of [`RPC_READ_TIMEOUT`], connect attempts use
//! [`CONNECT_TIMEOUT`], and write calls use [`RPC_WRITE_TIMEOUT`].

mod client;
mod server;
mod wire;

#[cfg(feature = "distributed-raft")]
use std::io;
#[cfg(feature = "distributed-raft")]
use std::time::Duration;

#[cfg(feature = "distributed-raft")]
pub use client::TransportHub;
#[cfg(feature = "distributed-raft")]
pub(crate) use client::forward_proposal;
#[cfg(feature = "distributed-raft")]
pub use server::RaftTcpServer;

/// Hard cap on a single TCP frame.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Per-RPC read-response timeout. A peer that accepted the TCP connect but never
/// replies is declared unreachable.
#[cfg(feature = "distributed-raft")]
pub const RPC_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-RPC write timeout. A blocked-but-alive peer, with a full TCP send buffer, is
/// declared unreachable.
#[cfg(feature = "distributed-raft")]
pub const RPC_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Read-response timeout used exclusively for [`forward_proposal`].
///
/// Proposal forwarding waits for the remote leader to commit the proposal,
/// which can block for a full Raft commit round-trip. Must be strictly greater
/// than `CLUSTER_PROPOSE_TIMEOUT` (30 s) so the store layer's outer timeout
/// fires first.
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

/// Idle-read timeout on the server connection task.
///
/// A peer that keeps TCP alive but stops sending is evicted. Well above Raft
/// heartbeat intervals, typically 150-500 ms.
#[cfg(feature = "distributed-raft")]
const IDLE_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Number of consecutive RPC failures before a per-peer circuit opens.
#[cfg(feature = "distributed-raft")]
const CIRCUIT_OPEN_THRESHOLD: u32 = 5;

/// Duration a circuit stays open before allowing a retry attempt.
#[cfg(feature = "distributed-raft")]
const CIRCUIT_OPEN_DURATION: Duration = Duration::from_secs(30);

/// Fine-grained transport error surfaced by the connection pool.
///
/// The raft driver treats every variant as a retryable send failure and
/// reports the peer unreachable, so raft re-probes on the next tick.
#[cfg(feature = "distributed-raft")]
#[derive(Debug)]
pub enum TransportError {
    /// TCP connect failed (peer unreachable or connection refused).
    ConnectFailed(io::Error),
    /// Write to stream failed.
    WriteFailed(io::Error),
    /// Write timed out; the peer send buffer is full.
    WriteTimeout,
    /// Read timed out: no response within [`RPC_READ_TIMEOUT`].
    ReadTimeout,
    /// Frame protocol error (oversized frame, premature EOF).
    FramingError(String),
    /// Peer reset the connection cleanly (EOF on read).
    PeerReset,
    /// Serialization error: a bug, not a transient network issue.
    EncodeError(String),
    /// Other I/O error.
    Other(io::Error),
}

#[cfg(feature = "distributed-raft")]
impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::ConnectFailed(e) => write!(f, "connect failed: {e}"),
            TransportError::WriteFailed(e) => write!(f, "write failed: {e}"),
            TransportError::WriteTimeout => write!(f, "write timeout"),
            TransportError::ReadTimeout => write!(f, "read timeout"),
            TransportError::FramingError(msg) => write!(f, "framing error: {msg}"),
            TransportError::PeerReset => write!(f, "peer reset connection"),
            TransportError::EncodeError(msg) => write!(f, "encode error: {msg}"),
            TransportError::Other(e) => write!(f, "transport error: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::wire::{RpcEnvelope, read_frame};

    #[cfg(feature = "distributed-raft")]
    use crate::distributed::consensus::types::ConsensusCommand;

    /// The frame codec round-trips an envelope through an in-memory pair.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_frame_round_trip_envelope() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

        let (mut a, mut b) = duplex(64 * 1024);
        let envelope = RpcEnvelope::ProposalForward {
            command: ConsensusCommand::AckClaim {
                claim_id: uuid::Uuid::nil(),
                instance_id: uuid::Uuid::nil(),
            },
        };
        let bytes = envelope.encode();
        let len = (bytes.len() as u32).to_be_bytes();
        a.write_all(&len).await.unwrap();
        a.write_all(&bytes).await.unwrap();
        a.flush().await.unwrap();
        drop(a);

        let mut len_buf = [0u8; 4];
        b.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        b.read_exact(&mut payload).await.unwrap();
        let decoded = RpcEnvelope::decode(&payload).unwrap();
        assert!(matches!(decoded, RpcEnvelope::ProposalForward { .. }));
    }

    /// The frame codec rejects an oversized frame with `InvalidData`.
    #[tokio::test]
    async fn test_frame_rejects_oversized() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream
                .write_all(&(MAX_FRAME_BYTES as u32 + 1).to_be_bytes())
                .await
                .unwrap();
            stream
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        let _server_stream = server.await.unwrap();
        let err = read_frame(&mut client)
            .await
            .expect_err("oversized frame must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
