//! Arrow-IPC distributed execution layer for PCS.
//!
//! Distributed batch processing runs natively on Apache Arrow IPC. Partitions are
//! claimed as row ranges of a replicated master `RecordBatch`, and every persisted
//! artefact (checkpoints, window accumulators, runtime-state blobs) crosses the wire
//! as IPC bytes.
//!
//! # Feature gates
//!
//! - `distributed`: this module, the core traits, and the runner.
//! - `tikv-store`: adds the TiKV-backed shared store and state client, the
//!   only application-data backend.
//! - `distributed-raft`: adds the raft-rs consensus driver in `consensus/`,
//!   which carries cluster membership and leadership only.
//!
//! # Architecture
//!
//! ```text
//! PartitionSource   ────────────────────────────►  claim row-ranges
//! CheckpointStore   ────────────────────────────►  persist IPC snapshots
//! DistributedRunner ─ claim → run_on_with_state → checkpoint → ack
//! TikvSharedStore   ─ both traits over a raw TiKV client (claims via CAS)
//! consensus/        ─ raft log store, driver, TCP transport
//! ```
//!
//! # Payload size constraint
//!
//! Arrow IPC payloads are bounded at 1 MiB ([`MAX_LOG_ENTRY_BYTES`]) for a
//! registered master batch and, by default, for a checkpoint; the TiKV store
//! raises the checkpoint half to `TIKV_MAX_CHECKPOINT_BYTES` (4 MiB).

pub mod accumulator_store;
pub mod checkpoint;
pub mod consensus;
pub mod partition;
pub mod processor_state_store;
pub mod runner;
pub mod strategy;
#[cfg(feature = "tikv-store")]
pub mod tikv_store;
#[cfg(feature = "tikv-store")]
pub use tikv_store::{TikvSharedStore, TikvStoreConfig};

use async_trait::async_trait;
pub use checkpoint::{
    ACCUMULATOR_STAGE_SENTINEL, Checkpoint, CheckpointStore, PROCESSOR_STATE_STAGE_SENTINEL,
};
pub use partition::{BatchClaim, MAX_LOG_ENTRY_BYTES, PartitionSource};
pub use runner::{DistributedRunner, KeyPartition, RunnerConfig};
pub use strategy::CheckpointStrategy;
use uuid::Uuid;

use crate::PcsResult;
/// A [`PartitionSource`] that is also a [`CheckpointStore`] — the full shared
/// store surface [`DistributedRunner`] needs.
///
/// A blanket impl covers every concrete store, so a backend can be boxed as a
/// single-trait object (`Box<dyn SharedStore>`) and still satisfy the runner's
/// `S: PartitionSource + CheckpointStore` bounds via the delegating impls
/// below. Rust forbids `Box<dyn PartitionSource + CheckpointStore>` (two
/// non-auto traits in one object), which is what makes this marker necessary.
#[async_trait]
pub trait SharedStore: PartitionSource + CheckpointStore {}
impl<T: PartitionSource + CheckpointStore> SharedStore for T {}

#[async_trait]
impl PartitionSource for Box<dyn SharedStore> {
    async fn claim_next_batch(&self, instance_id: Uuid) -> PcsResult<Option<BatchClaim>> {
        self.as_ref().claim_next_batch(instance_id).await
    }
    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<u64> {
        self.as_ref().renew_claim(claim_id, instance_id).await
    }
    async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<()> {
        self.as_ref().ack_claim(claim_id, instance_id).await
    }
    async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<()> {
        self.as_ref().release_claim(claim_id, instance_id).await
    }
    async fn reclaim_expired(&self, now_millis: u64) -> PcsResult<u32> {
        self.as_ref().reclaim_expired(now_millis).await
    }
}

#[async_trait]
impl CheckpointStore for Box<dyn SharedStore> {
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> PcsResult<()> {
        self.as_ref()
            .save_checkpoint(claim_id, stage_idx, ipc_bytes, schema_id)
            .await
    }
    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> PcsResult<Option<Checkpoint>> {
        self.as_ref().load_checkpoint(claim_id, stage_idx).await
    }
    async fn persisted_schema_id(&self) -> PcsResult<Option<u32>> {
        self.as_ref().persisted_schema_id().await
    }
    fn max_checkpoint_bytes(&self) -> usize {
        self.as_ref().max_checkpoint_bytes()
    }
}

// Parquet archival checkpoint store; requires both `distributed` and
// `parquet-checkpoint`.
#[cfg(feature = "parquet-checkpoint")]
pub mod parquet_checkpoint;
#[cfg(feature = "parquet-checkpoint")]
pub use parquet_checkpoint::ParquetCheckpointStore;
