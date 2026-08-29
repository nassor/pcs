//! On-wire frame body codec and the length-prefixed frame helpers.
//!
//! One message kind travels over this transport: a raft protocol message,
//! carried as raw prost-encoded `eraftpb::Message` bytes (the raft
//! `prost-codec` wire format) behind a one-byte tag inside a length-prefixed
//! frame.
//!
//! The tag exists so the format stays extensible. It is **append-only**:
//! [`TAG_RAFT_MESSAGE`] must keep its value, and a future message kind takes
//! the next free tag, so rolling upgrades stay compatible.

use std::io;

use tokio::net::TcpStream;

use super::MAX_FRAME_BYTES;

/// Tag byte for a raft message frame body.
const TAG_RAFT_MESSAGE: u8 = 0;

/// Encode prost-encoded raft message bytes as a tagged frame body.
pub(super) fn encode_raft_message(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + bytes.len());
    out.push(TAG_RAFT_MESSAGE);
    out.extend_from_slice(bytes);
    out
}

/// Strip the tag byte from a frame body, returning the prost-encoded raft
/// message bytes.
///
/// # Errors
///
/// Returns an error for an empty body or an unrecognised tag.
pub(super) fn decode_raft_message(body: &[u8]) -> io::Result<&[u8]> {
    let Some((tag, rest)) = body.split_first() else {
        return Err(io::Error::other("empty transport frame"));
    };
    match *tag {
        TAG_RAFT_MESSAGE => Ok(rest),
        other => Err(io::Error::other(format!("unknown transport tag {other}"))),
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

    #[test]
    fn test_raft_message_body_round_trip() {
        let body = encode_raft_message(&[0xAA, 0xBB, 0xCC]);
        assert_eq!(body[0], TAG_RAFT_MESSAGE);
        assert_eq!(decode_raft_message(&body).unwrap(), &[0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn test_empty_raft_message_body_round_trips() {
        let body = encode_raft_message(&[]);
        assert_eq!(body, vec![TAG_RAFT_MESSAGE]);
        assert!(decode_raft_message(&body).unwrap().is_empty());
    }

    #[test]
    fn test_decode_rejects_empty_and_unknown_tags() {
        assert!(decode_raft_message(&[]).is_err());
        let err = decode_raft_message(&[0xFF, 0x00]).unwrap_err();
        assert!(
            err.to_string().contains("unknown transport tag 255"),
            "an unknown tag must name itself: {err}"
        );
    }
}
