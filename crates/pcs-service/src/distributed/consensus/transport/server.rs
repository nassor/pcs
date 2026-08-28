//! Inbound half of the transport: the TCP accept loop and RPC dispatch.
//!
//! Owns [`RaftTcpServer`] and the per-connection read/dispatch/write loop.
//! Inbound raft messages are forwarded to the driver's transport inbox
//! (which steps them into the local `RawNode`); forwarded proposals are
//! routed to the proposal channel and their responses written back on the
//! same connection.

#[cfg(feature = "distributed-raft")]
use std::io;
#[cfg(feature = "distributed-raft")]
use std::net::SocketAddr;
#[cfg(feature = "distributed-raft")]
use std::sync::Arc;
#[cfg(feature = "distributed-raft")]
use std::time::Duration;

#[cfg(feature = "distributed-raft")]
use prost::Message as _;
#[cfg(feature = "distributed-raft")]
use raft::eraftpb::Message;
#[cfg(feature = "distributed-raft")]
use tokio::net::TcpStream;
#[cfg(feature = "distributed-raft")]
use tokio::sync::{mpsc, oneshot};

#[cfg(feature = "distributed-raft")]
use super::MAX_ACCEPTED_CONNECTIONS;
#[cfg(feature = "distributed-raft")]
use super::wire::{RpcEnvelope, RpcResponse, read_frame, write_frame};
#[cfg(feature = "distributed-raft")]
use crate::PcsResult;
#[cfg(feature = "distributed-raft")]
use crate::distributed::consensus::types::ConsensusCommand;

/// TCP server that dispatches incoming Raft RPCs to the local driver.
///
/// Start one instance per node during cluster initialisation. The server binds
/// `listen_addr` and spawns a Tokio task per accepted connection. The accept
/// loop stops on [`RaftTcpServer::shutdown`] or when the server handle drops.
#[cfg(feature = "distributed-raft")]
pub struct RaftTcpServer {
    transport_tx: mpsc::Sender<Message>,
    proposal_tx: mpsc::Sender<(
        ConsensusCommand,
        oneshot::Sender<PcsResult<ConsensusResponse>>,
    )>,
    listen_addr: SocketAddr,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

#[cfg(feature = "distributed-raft")]
impl RaftTcpServer {
    /// Create a new server bound to `listen_addr` that forwards inbound
    /// messages to the driver's channels.
    #[allow(clippy::type_complexity)]
    pub fn new(
        transport_tx: mpsc::Sender<Message>,
        proposal_tx: mpsc::Sender<(
            ConsensusCommand,
            oneshot::Sender<PcsResult<ConsensusResponse>>,
        )>,
        listen_addr: SocketAddr,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            transport_tx,
            proposal_tx,
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

                    let transport_tx = self.transport_tx.clone();
                    let proposal_tx = self.proposal_tx.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        handle_connection(transport_tx, proposal_tx, stream).await;
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

#[cfg(feature = "distributed-raft")]
use crate::distributed::consensus::types::ConsensusResponse;

/// Handle one accepted connection: read RPCs, dispatch, write responses.
#[cfg(feature = "distributed-raft")]
async fn handle_connection(
    transport_tx: mpsc::Sender<Message>,
    proposal_tx: mpsc::Sender<(
        ConsensusCommand,
        oneshot::Sender<PcsResult<ConsensusResponse>>,
    )>,
    mut stream: TcpStream,
) {
    loop {
        // Close the connection if the peer stops sending, so an alive-but-silent TCP
        // link does not park a task indefinitely.
        let raw =
            match tokio::time::timeout(super::IDLE_READ_TIMEOUT, read_frame(&mut stream)).await {
                Ok(Ok(Some(b))) => b,
                Ok(Ok(None)) => break, // clean EOF
                Ok(Err(_e)) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %_e, "RaftTcpServer: read frame error");
                    break;
                }
                Err(_) => {
                    // Idle timeout with no frame received; close cleanly.
                    #[cfg(feature = "tracing")]
                    tracing::debug!("RaftTcpServer: idle timeout, closing connection");
                    break;
                }
            };

        let envelope: RpcEnvelope = match RpcEnvelope::decode(&raw) {
            Ok(e) => e,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: envelope decode error");
                break;
            }
        };

        let response = handle_envelope(&transport_tx, &proposal_tx, envelope).await;

        // Raft messages are fire-and-forget: no reply frame is written, so no
        // stale frames accumulate on pooled connections.
        let Some(response) = response else {
            continue;
        };
        let resp_bytes = response.encode();
        if write_frame(&mut stream, &resp_bytes).await.is_err() {
            break;
        }
    }
}

/// Dispatch one decoded envelope: raft messages to the driver's inbox
/// (response-less), forwarded proposals to the proposal channel with the
/// response written back.
#[cfg(feature = "distributed-raft")]
async fn handle_envelope(
    transport_tx: &mpsc::Sender<Message>,
    proposal_tx: &mpsc::Sender<(
        ConsensusCommand,
        oneshot::Sender<PcsResult<ConsensusResponse>>,
    )>,
    envelope: RpcEnvelope,
) -> Option<RpcResponse> {
    match envelope {
        RpcEnvelope::RaftMessage(bytes) => {
            let msg = match Message::decode(bytes.as_slice()) {
                Ok(m) => m,
                Err(_e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %_e, "RaftTcpServer: raft message decode error");
                    return None;
                }
            };
            // Overflow drops the message; raft re-sends (heartbeats, probes).
            let _ = transport_tx.try_send(msg);
            None
        }
        RpcEnvelope::ProposalForward { command } => {
            let (reply_tx, reply_rx) = oneshot::channel();
            if proposal_tx.send((command, reply_tx)).await.is_err() {
                return Some(RpcResponse::Error("proposal channel closed".to_string()));
            }
            match reply_rx.await {
                Ok(Ok(response)) => Some(RpcResponse::Applied { index: 0, response }),
                Ok(Err(e)) => Some(RpcResponse::Error(e.to_string())),
                Err(_) => Some(RpcResponse::Error(
                    "proposal reply channel closed".to_string(),
                )),
            }
        }
    }
}
