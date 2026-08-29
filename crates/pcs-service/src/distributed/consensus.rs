//! Raft consensus layer for PCS's cluster mode.
//!
//! The PCS raft runs for membership and leadership only: application data
//! (partitions, checkpoints, claims) lives in TiKV, so nothing is proposed
//! into this log and its entries are raft's own per-term no-ops.
//!
//! - `driver`: the [`ArrowRaftDriver`] `RawNode` drive loop, its metrics
//!   snapshot, and the shutdown handle.
//! - `storage`: [`RaftRedbLogStore`], the `raft::Storage` implementation over
//!   a log-only redb file.
//! - `transport`: the length-prefixed TCP transport carrying prost-encoded
//!   `eraftpb::Message` frames between peers.
//!
//! # Feature gates
//!
//! Every module here needs `distributed-raft`; without it this module is empty.

#[cfg(feature = "distributed-raft")]
pub mod driver;
#[cfg(feature = "distributed-raft")]
pub mod storage;
#[cfg(feature = "distributed-raft")]
pub mod transport;

#[cfg(feature = "distributed-raft")]
pub use driver::{ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle};
#[cfg(feature = "distributed-raft")]
pub use storage::RaftRedbLogStore;
