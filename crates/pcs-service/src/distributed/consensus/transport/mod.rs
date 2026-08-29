//! TCP transport for Raft consensus messages.
//!
//! Length-prefixed framing carrying one message kind: raft protocol messages
//! travel as raw prost-encoded `eraftpb::Message` bytes (the raft
//! `prost-codec` wire format), fire-and-forget, with no reply frame. Nothing
//! is proposed into the PCS raft, so there is no request/response half.
//!
//! Each payload is `1 tag byte + body` (see `wire`) inside a
//! length-prefixed frame. The wire format is **append-only**: existing tag
//! values must never change, so rolling upgrades stay compatible.
//!
//! ## Layout
//!
//! This file owns the shared frame and circuit constants plus the
//! [`TransportError`] classification. `wire` holds the frame codec and the
//! tagged body helpers, `client` the outbound half ([`TransportHub`] and its
//! per-peer pool), and `server` the inbound half ([`RaftTcpServer`]).
//!
//! [`RaftTcpServer`] binds a listen address and dispatches incoming messages
//! to the local driver's inbox. Start it once during node initialisation,
//! before any remote peer can contact the node.
//!
//! [`TransportHub`] keeps a per-peer pool of idle
//! [`TcpStream`](tokio::net::TcpStream)s bounded by [`POOL_CAPACITY`]. Streams
//! are acquired from the pool or freshly connected, returned on success, and
//! dropped on error so a broken stream never re-enters the pool.
//!
//! Connect attempts use [`CONNECT_TIMEOUT`] and write calls use
//! [`RPC_WRITE_TIMEOUT`].

mod client;
mod server;
mod wire;

use std::io;
use std::time::Duration;

pub use client::TransportHub;
pub use server::RaftTcpServer;

/// Hard cap on a single TCP frame.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Per-message write timeout. A blocked-but-alive peer, with a full TCP send
/// buffer, is declared unreachable.
pub const RPC_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for establishing a new TCP connection to a peer.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Maximum number of idle connections kept per peer.
pub const POOL_CAPACITY: usize = 4;

/// Maximum idle time for a pooled connection before it is dropped on next acquire.
const POOL_MAX_IDLE: Duration = Duration::from_secs(10);

/// Maximum number of concurrent accepted connections on the server.
const MAX_ACCEPTED_CONNECTIONS: usize = 1024;

/// Idle-read timeout on the server connection task.
///
/// A peer that keeps TCP alive but stops sending is evicted. Well above Raft
/// heartbeat intervals, typically 150-500 ms.
const IDLE_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Number of consecutive send failures before a per-peer circuit opens.
const CIRCUIT_OPEN_THRESHOLD: u32 = 5;

/// Duration a circuit stays open before allowing a retry attempt.
const CIRCUIT_OPEN_DURATION: Duration = Duration::from_secs(30);

/// Fine-grained transport error surfaced by the connection pool.
///
/// The raft driver treats every variant as a retryable send failure and
/// reports the peer unreachable, so raft re-probes on the next tick.
#[derive(Debug)]
pub enum TransportError {
    /// TCP connect failed (peer unreachable or connection refused).
    ConnectFailed(io::Error),
    /// Write to stream failed.
    WriteFailed(io::Error),
    /// Write timed out; the peer send buffer is full.
    WriteTimeout,
    /// Other I/O error.
    Other(io::Error),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::ConnectFailed(e) => write!(f, "connect failed: {e}"),
            TransportError::WriteFailed(e) => write!(f, "write failed: {e}"),
            TransportError::WriteTimeout => write!(f, "write timeout"),
            TransportError::Other(e) => write!(f, "transport error: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::wire::{encode_raft_message, read_frame, write_frame};

    /// The frame codec round-trips a tagged raft-message body over a real
    /// socket pair.
    #[tokio::test]
    async fn test_frame_round_trip_raft_message() {
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = encode_raft_message(&[0x01, 0x02, 0x03]);
        let expected = body.clone();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            write_frame(&mut stream, &body).await.unwrap();
            stream
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let _server_stream = server.await.unwrap();
        let frame = read_frame(&mut client)
            .await
            .unwrap()
            .expect("a complete frame");
        assert_eq!(frame, expected);
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
