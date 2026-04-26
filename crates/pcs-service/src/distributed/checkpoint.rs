//! Arrow-IPC-native checkpoint store traits and types.
//!
//! [`CheckpointStore`] persists intermediate pipeline state as Arrow IPC bytes
//! so that a runner can resume from the last completed stage after a crash or
//! lease expiry.

use async_trait::async_trait;
use uuid::Uuid;

use crate::PcsResult;

/// Sentinel `stage_idx` value used to store the window accumulator checkpoint.
///
/// Regular pipeline stages are numbered starting from 0. Using `u32::MAX`
/// as a dedicated key avoids any overlap with real stage indices (the practical
/// limit is in the dozens). The state machine has no upper-bound check on
/// `stage_idx`, so this value is safe to use directly.
pub const ACCUMULATOR_STAGE_SENTINEL: u32 = u32::MAX;

// ── Checkpoint ───────────────────────────────────────────────────────────

/// A persisted intermediate snapshot of pipeline state for one claim stage.
///
/// The `payload` field holds Arrow IPC bytes serialised from the
/// [`Dataset`](crate::Dataset) at the checkpoint boundary.
/// It is empty (`vec![]`) when the checkpoint strategy is
/// [`CheckpointStrategy::None`](crate::distributed::strategy::CheckpointStrategy::None).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// Stable identifier for the master batch this checkpoint belongs to.
    pub batch_id: u64,
    /// Dataset stage index after which this checkpoint was taken.
    pub stage_idx: u32,
    /// Arrow IPC bytes for the intermediate pipeline state; empty if no snapshot.
    pub payload: Vec<u8>,
    /// Schema version of the Arrow data in `payload`.
    pub schema_id: u32,
    /// Unix milliseconds when this checkpoint was created.
    pub created_at: u64,
}

// ── CheckpointStore ──────────────────────────────────────────────────────

/// Persistent storage for Arrow-IPC pipeline checkpoints.
///
/// In multi-node mode each mutation is committed through Raft before returning.
/// In single-node mode mutations are applied directly to the local database.
///
/// Reads always go directly to the local replica (eventual consistency is
/// acceptable for checkpoint data — the worst case is re-processing one stage).
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Save a checkpoint for `claim_id` at `stage_idx`.
    ///
    /// `ipc_bytes` must be less than
    /// [`MAX_LOG_ENTRY_BYTES`](crate::distributed::partition::MAX_LOG_ENTRY_BYTES).
    /// The caller is responsible for splitting across multiple checkpoints if
    /// the pipeline state is larger.
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> PcsResult<()>;

    /// Load the latest checkpoint for `claim_id` at `stage_idx`, if any.
    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> PcsResult<Option<Checkpoint>>;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_checkpoint_fields() {
        let cp = Checkpoint {
            batch_id: 42,
            stage_idx: 3,
            payload: vec![0xAA, 0xBB],
            schema_id: 1,
            created_at: 1_700_000_000_000,
        };
        assert_eq!(cp.batch_id, 42);
        assert_eq!(cp.stage_idx, 3);
        assert_eq!(cp.payload, vec![0xAA, 0xBB]);
        assert_eq!(cp.schema_id, 1);
        assert_eq!(cp.created_at, 1_700_000_000_000);
    }

    #[test]
    fn test_arrow_checkpoint_empty_payload() {
        let cp = Checkpoint {
            batch_id: 1,
            stage_idx: 0,
            payload: vec![],
            schema_id: 0,
            created_at: 0,
        };
        assert!(cp.payload.is_empty());
    }

    #[test]
    fn test_arrow_checkpoint_clone_eq() {
        let cp = Checkpoint {
            batch_id: 7,
            stage_idx: 2,
            payload: vec![1, 2, 3],
            schema_id: 5,
            created_at: 999,
        };
        assert_eq!(cp.clone(), cp);
    }
}
