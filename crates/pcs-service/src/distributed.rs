//! Arrow-IPC distributed execution layer for PCS.
//!
//! This module implements distributed batch processing natively on Apache
//! Arrow IPC: partitions are claimed as row ranges of a replicated master
//! `RecordBatch`, and every persisted artefact — checkpoints, window
//! accumulators, runtime-state blobs — crosses the wire as IPC bytes.
//!
//! # Feature gates
//!
//! - `distributed` — enables this module and the core traits
//! - `distributed-raft` — additionally enables openraft consensus
//!
//! # Architecture
//!
//! ```text
//! PartitionSource  ─────────────────────────────►  claim row-ranges
//! CheckpointStore  ─────────────────────────────►  persist IPC snapshots
//! DistributedRunner ─ claim → run_on_with_state → checkpoint → ack
//! RedbSharedStore  ─ single-node or multi-node (via Raft channel)
//! consensus/            ─ state machine, log store, snapshot, driver, transport
//! ```
//!
//! # Log entry size constraint
//!
//! All Arrow IPC payloads embedded in Raft log entries are bounded at 1 MiB
//! ([`MAX_LOG_ENTRY_BYTES`]). Larger payloads
//! are rejected at the propose boundary. The snapshot path (openraft
//! `build_snapshot` / `install_snapshot`) handles arbitrarily large state.

pub mod accumulator_store;
pub mod checkpoint;
pub mod consensus;
pub mod guest_state_store;
pub mod partition;
pub mod runner;
pub mod strategy;

pub use checkpoint::{
    ACCUMULATOR_STAGE_SENTINEL, Checkpoint, CheckpointStore, GUEST_STATE_STAGE_SENTINEL,
};
pub use consensus::RedbSharedStore;
pub use partition::{BatchClaim, MAX_LOG_ENTRY_BYTES, PartitionSource};
pub use runner::{DistributedRunner, KeyPartition, RunnerConfig};
pub use strategy::CheckpointStrategy;

// Parquet-based archival checkpoint store — requires both `distributed` and `io`.
#[cfg(feature = "io")]
pub mod parquet_checkpoint;
#[cfg(feature = "io")]
pub use parquet_checkpoint::ParquetCheckpointStore;
