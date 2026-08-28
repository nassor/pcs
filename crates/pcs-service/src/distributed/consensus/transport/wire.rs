//! On-wire message types and the length-prefixed frame codec.
//!
//! Holds the [`RpcEnvelope`] / [`RpcResponse`] pair exchanged by every RPC and
//! the [`read_frame`] / [`write_frame`] helpers both directions share.
//!
//! Each payload is `1 tag byte + body` inside the existing length-prefixed
//! frame: raft messages travel as raw prost-encoded `eraftpb::Message` bytes
//! (the raft `prost-codec` wire format), everything else as postcard.

use std::io;

use tokio::net::TcpStream;

#[cfg(feature = "distributed-raft")]
use crate::distributed::consensus::types::{ConsensusCommand, ConsensusResponse};

use super::MAX_FRAME_BYTES;

/// Tag byte for a raft message frame body.
#[cfg(feature = "distributed-raft")]
const TAG_RAFT_MESSAGE: u8 = 0;
/// Tag byte for a proposal-forward frame body.
#[cfg(feature = "distributed-raft")]
const TAG_PROPOSAL_FORWARD: u8 = 1;
/// Tag byte for an applied response body.
#[cfg(feature = "distributed-raft")]
const TAG_RESPONSE_APPLIED: u8 = 0;
/// Tag byte for an error response body.
#[cfg(feature = "distributed-raft")]
const TAG_RESPONSE_ERROR: u8 = 1;

/// Typed envelope for all RPCs sent over the TCP transport.
///
/// Serialized as `1 tag byte + body` inside the length-prefixed frame:
/// [`RaftMessage`](Self::RaftMessage) bodies are raw prost-encoded
/// `eraftpb::Message` bytes, [`ProposalForward`](Self::ProposalForward)
/// bodies are postcard-encoded [`ConsensusCommand`].
#[cfg(feature = "distributed-raft")]
#[derive(Debug)]
pub(crate) enum RpcEnvelope {
    /// A raft protocol message: prost-encoded `eraftpb::Message` bytes.
    RaftMessage(Vec<u8>),
    /// A follower forwards a proposal to the leader.
    ProposalForward { command: ConsensusCommand },
}

#[cfg(feature = "distributed-raft")]
impl RpcEnvelope {
    /// Encode as `1 tag byte + body`.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RpcEnvelope::RaftMessage(bytes) => {
                let mut out = Vec::with_capacity(1 + bytes.len());
                out.push(TAG_RAFT_MESSAGE);
                out.extend_from_slice(bytes);
                out
            }
            RpcEnvelope::ProposalForward { command } => {
                let body = postcard::to_allocvec(command)
                    .expect("postcard encode of ConsensusCommand is infallible");
                let mut out = Vec::with_capacity(1 + body.len());
                out.push(TAG_PROPOSAL_FORWARD);
                out.extend_from_slice(&body);
                out
            }
        }
    }

    /// Decode from `1 tag byte + body`.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let Some((tag, body)) = bytes.split_first() else {
            return Err(io::Error::other("empty envelope frame"));
        };
        match *tag {
            TAG_RAFT_MESSAGE => Ok(RpcEnvelope::RaftMessage(body.to_vec())),
            TAG_PROPOSAL_FORWARD => {
                let command = postcard::from_bytes(body)
                    .map_err(|e| io::Error::other(format!("proposal decode: {e}")))?;
                Ok(RpcEnvelope::ProposalForward { command })
            }
            other => Err(io::Error::other(format!("unknown envelope tag {other}"))),
        }
    }
}

/// Response envelope returned from the server for each incoming RPC.
#[cfg(feature = "distributed-raft")]
#[derive(Debug)]
pub(crate) enum RpcResponse {
    /// A forwarded proposal was applied at `index`.
    Applied {
        index: u64,
        response: ConsensusResponse,
    },
    /// Error string returned by the server.
    Error(String),
}

#[cfg(feature = "distributed-raft")]
impl RpcResponse {
    /// Encode as `1 tag byte + body`.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RpcResponse::Applied { index, response } => {
                let body = postcard::to_allocvec(&(*index, response))
                    .expect("postcard encode of the applied pair is infallible");
                let mut out = Vec::with_capacity(1 + body.len());
                out.push(TAG_RESPONSE_APPLIED);
                out.extend_from_slice(&body);
                out
            }
            RpcResponse::Error(message) => {
                let body = postcard::to_allocvec(message)
                    .expect("postcard encode of String is infallible");
                let mut out = Vec::with_capacity(1 + body.len());
                out.push(TAG_RESPONSE_ERROR);
                out.extend_from_slice(&body);
                out
            }
        }
    }

    /// Decode from `1 tag byte + body`.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let Some((tag, body)) = bytes.split_first() else {
            return Err(io::Error::other("empty response frame"));
        };
        match *tag {
            TAG_RESPONSE_APPLIED => {
                let (index, response) = postcard::from_bytes(body)
                    .map_err(|e| io::Error::other(format!("applied decode: {e}")))?;
                Ok(RpcResponse::Applied { index, response })
            }
            TAG_RESPONSE_ERROR => {
                let message = postcard::from_bytes(body)
                    .map_err(|e| io::Error::other(format!("error decode: {e}")))?;
                Ok(RpcResponse::Error(message))
            }
            other => Err(io::Error::other(format!("unknown response tag {other}"))),
        }
    }
}

/// Read one length-prefixed frame from `stream`.
///
/// Returns:
/// - `Ok(Some(bytes))` when a complete frame was received.
/// - `Ok(None)` when the peer closed the connection cleanly (EOF on length header).
/// - `Err(e)` on I/O error:
///   - `ErrorKind::InvalidData` when the frame length exceeds [`MAX_FRAME_BYTES`].
///   - `ErrorKind::UnexpectedEof` on a truncated frame (EOF inside payload).
///   - Other kinds forwarded from the underlying stream.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
pub(super) async fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    use tokio::io::AsyncReadExt;

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
    stream.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

/// Write one length-prefixed frame to `stream`.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
pub(super) async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::consensus::types::ConsensusCommand;

    #[test]
    fn test_envelope_round_trip_raft_message() {
        let envelope = RpcEnvelope::RaftMessage(vec![0xAA, 0xBB, 0xCC]);
        let bytes = envelope.encode();
        assert_eq!(bytes[0], TAG_RAFT_MESSAGE);
        let back = RpcEnvelope::decode(&bytes).unwrap();
        match back {
            RpcEnvelope::RaftMessage(b) => assert_eq!(b, vec![0xAA, 0xBB, 0xCC]),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_envelope_round_trip_proposal() {
        let cmd = ConsensusCommand::AckClaim {
            claim_id: uuid::Uuid::now_v7(),
            instance_id: uuid::Uuid::now_v7(),
        };
        let envelope = RpcEnvelope::ProposalForward {
            command: cmd.clone(),
        };
        let bytes = envelope.encode();
        let back = RpcEnvelope::decode(&bytes).unwrap();
        match back {
            RpcEnvelope::ProposalForward { command } => assert_eq!(command, cmd),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_response_round_trip_applied() {
        let resp = RpcResponse::Applied {
            index: 7,
            response: ConsensusResponse::ClaimAcked,
        };
        let bytes = resp.encode();
        assert_eq!(bytes[0], TAG_RESPONSE_APPLIED);
        match RpcResponse::decode(&bytes).unwrap() {
            RpcResponse::Applied { index, response } => {
                assert_eq!(index, 7);
                assert!(matches!(response, ConsensusResponse::ClaimAcked));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_response_round_trip_error() {
        let resp = RpcResponse::Error("boom".to_string());
        let bytes = resp.encode();
        match RpcResponse::decode(&bytes).unwrap() {
            RpcResponse::Error(m) => assert_eq!(m, "boom"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_decode_rejects_unknown_tags() {
        assert!(RpcEnvelope::decode(&[0xFF, 0x00]).is_err());
        assert!(RpcResponse::decode(&[0xFF, 0x00]).is_err());
        assert!(RpcEnvelope::decode(&[]).is_err());
    }
}
