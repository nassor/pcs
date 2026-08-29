//! Inbound half of the transport: the TCP accept loop and message dispatch.
//!
//! Owns [`RaftTcpServer`] and the per-connection read/dispatch loop. Inbound
//! raft messages are forwarded to the driver's transport inbox, which steps
//! them into the local `RawNode`. Nothing is answered on the connection: raft
//! traffic is fire-and-forget.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use raft::eraftpb::Message;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use super::MAX_ACCEPTED_CONNECTIONS;
use super::wire::{decode_raft_message, read_frame};

/// TCP server that dispatches incoming Raft messages to the local driver.
///
/// Start one instance per node during cluster initialisation. The server binds
/// `listen_addr` and spawns a Tokio task per accepted connection. The accept
/// loop stops on [`RaftTcpServer::shutdown`] or when the server handle drops.
pub struct RaftTcpServer {
    transport_tx: mpsc::Sender<Message>,
    listen_addr: SocketAddr,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl RaftTcpServer {
    /// Create a new server bound to `listen_addr` that forwards inbound
    /// messages to the driver's transport inbox.
    pub fn new(transport_tx: mpsc::Sender<Message>, listen_addr: SocketAddr) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            transport_tx,
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
                    tokio::spawn(async move {
                        let _permit = permit;
                        handle_connection(transport_tx, stream).await;
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

/// Handle one accepted connection: read frames and step each raft message into
/// the driver.
async fn handle_connection(transport_tx: mpsc::Sender<Message>, mut stream: TcpStream) {
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

        let body = match decode_raft_message(&raw) {
            Ok(b) => b,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: frame tag decode error");
                break;
            }
        };
        let msg = match Message::decode(body) {
            Ok(m) => m,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: raft message decode error");
                break;
            }
        };
        // Overflow drops the message; raft re-sends (heartbeats, probes).
        let _ = transport_tx.try_send(msg);
    }
}
