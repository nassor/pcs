//! Persistent Raft log storage in a dedicated redb file.
//!
//! [`RaftRedbLogStore`] (in `log_store`) is the only store the driver keeps:
//! raft metadata (hard state, conf state), log entries and the latest
//! snapshot. No application data lives here — cluster application data is in
//! TiKV.
//!
//! ## Blocking-I/O discipline
//!
//! All redb read/write transactions are wrapped in
//! [`tokio::task::spawn_blocking`]. redb's commit path issues `fsync`, which
//! must not run on a tokio worker thread: blocking a worker for fsync latency
//! stalls every other task on that runtime. The log store holds an
//! `Arc<Database>` and no [`std::sync::Mutex`], because redb's
//! `Database::begin_write` / `begin_read` both take `&self` and coordinate
//! internally, so external serialization would only add lock contention.
//!
//! ## Entry encoding
//!
//! Log entries are prost-encoded `eraftpb::Entry` (the raft `prost-codec` wire
//! format); metadata rows (hard state, conf state, snapshot) are `postcard`.
//! Postcard is canonical by construction: the same input always produces the
//! same bytes, and it has no JSON map-ordering ambiguity if a nested type grows
//! a map field.

#[cfg(feature = "distributed-raft")]
#[path = "."]
pub(crate) mod raft_impl {
    mod log_store;

    pub use log_store::RaftRedbLogStore;
}

#[cfg(feature = "distributed-raft")]
pub use raft_impl::RaftRedbLogStore;
