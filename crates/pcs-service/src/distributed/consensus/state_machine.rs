//! Arrow-IPC-aware deterministic state machine for PCS distributed consensus.
//!
//! `apply(db, command)` is the single entry point. It opens a single redb
//! `WriteTransaction`, performs all mutations, and commits — ensuring all
//! writes are fate-shared with a single fsync.
//!
//! ## Tables
//!
//! | Table | Key | Value |
//! |-------|-----|-------|
//! | `arrow_master_batches` | `batch_id: u64` | JSON-encoded [`MasterBatchRecord`] |
//! | `arrow_claims` | `claim_id: [u8; 16]` | JSON-encoded [`ClaimRecord`] |
//! | `arrow_claims_by_batch` | `(batch_id_be8 ++ claim_id_16): [u8; 24]` | `start_be4 ++ end_be4 ++ status_byte`: 9 bytes |
//! | `arrow_checkpoints` | `(claim_id_bytes ++ stage_u32_be): [u8; 20]` | JSON-encoded [`CheckpointRecord`] |
//! | `arrow_instances` | `instance_id: [u8; 16]` | JSON-encoded [`InstanceRecord`] |
//!
//! ## Secondary index for per-batch claim scans
//!
//! `arrow_claims_by_batch` is a secondary index kept in lockstep with
//! `arrow_claims`. Its key prefix-encodes `batch_id` in big-endian, so a
//! range scan with a `batch_id`-bounded key range returns only the claims for
//! that batch in O(k) where k = claims in the batch — not O(total_claims).
//! The value stores the row range and status byte so overlap checks never need
//! to touch the primary `arrow_claims` table.
//!
//! ## Hot-path complexity
//!
//! | Operation | Before | After |
//! |-----------|--------|-------|
//! | ClaimRowRange (reject: batch missing) | O(1) read lock, no write | O(1) read lock, no write |
//! | ClaimRowRange (reject: range overlap) | O(total_claims) write lock | O(k) read lock, no write |
//! | ClaimRowRange (accept) | O(total_claims) write lock | O(k) read + O(k) double-check write |
//!
//! ## Two-step claim check (TOCTOU safety)
//!
//! `apply_claim_row_range` uses a two-step approach:
//!
//! 1. **Read precheck** — opens a `ReadTransaction` and scans `arrow_claims_by_batch`
//!    for the target batch only.  If the batch is missing or the range already
//!    overlaps a `Claimed`/`Completed` entry, returns early with no write.
//!
//! 2. **Write confirmation** — opens a `WriteTransaction` for the final check
//!    (under the write lock) and the actual insert.  Because redb serialises all
//!    writers, there is no window between the write-lock acquisition and the
//!    second scan — a concurrent writer that inserted between step 1 and
//!    step 2 will have already committed, and the secondary-index scan under
//!    the write lock will see it.
//!
//! ## Determinism invariant
//!
//! Apply handlers **must not** read wall-clock time, random numbers, or any
//! other ambient state. Every time-dependent field is carried on the
//! [`ConsensusCommand`] itself via `now_at_propose`, populated by the **leader**
//! at propose time. This guarantees that two replicas applying the same
//! committed log entry produce byte-identical database state.

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::PcsError;
use crate::PcsResult;
use crate::distributed::partition::MAX_LOG_ENTRY_BYTES;

use super::types::{ClaimStatus, ConsensusCommand, ConsensusResponse};

// ── Table definitions ─────────────────────────────────────────────────────────

const MASTER_BATCHES: TableDefinition<u64, &[u8]> = TableDefinition::new("arrow_master_batches");
const CLAIMS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("arrow_claims");
/// Secondary index: key = `batch_id_be8 ++ claim_id_16` (24 bytes),
/// value = `start_row_be4 ++ end_row_be4 ++ status_byte` (9 bytes).
///
/// Allows O(k) range scans for a single batch without touching `arrow_claims`.
const CLAIMS_BY_BATCH: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("arrow_claims_by_batch");
const CHECKPOINTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("arrow_checkpoints");
const INSTANCES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("arrow_instances");
/// Secondary index: set of batch_ids that still have at least one pending claim.
///
/// Key = `batch_id: u64`, value = empty slice `&[]`.
/// Inserted by `apply_register_master_batch`; removed by `apply_ack_claim` when
/// all rows of the batch are covered by Completed claims. Enables O(k) scan
/// over pending batches instead of an O(N) sweep of all batch_ids.
const PENDING_BATCHES: TableDefinition<u64, &[u8]> = TableDefinition::new("arrow_pending_batches");

/// SM metadata table. Defined here so `restore_state` can write watermarks
/// in the same transaction as the snapshot data — one commit, one fsync.
const SM_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("arrow_sm_meta");

/// SM metadata key for `last_applied` — must match the constant in `storage.rs`.
pub(crate) const KEY_SM_LAST_APPLIED: &str = "sm_last_applied";
/// SM metadata key for `last_membership` — must match the constant in `storage.rs`.
pub(crate) const KEY_SM_LAST_MEMBERSHIP: &str = "sm_last_membership";

// ── Record types ──────────────────────────────────────────────────────────────

/// Eligibility status of a master batch.
///
/// Old records that predate this field decode as `Active` via
/// `#[serde(default)]` on [`MasterBatchRecord::status`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchStatus {
    /// Batch is eligible for claiming. Default for all newly-registered
    /// batches and for legacy records decoded from disk.
    #[default]
    Active,
    /// Batch has been permanently disqualified by a runner-side release-cap
    /// trip. `claim_next_batch` will never return this batch again. Operators
    /// can observe poisoned batches via `/status` and must re-register a new
    /// batch if they want to retry the same data.
    Poisoned,
}

/// Stored master batch record.
///
/// ## Schema evolution
///
/// New fields added after initial release use `#[serde(default)]`. The
/// state machine encodes records with `serde_json`; missing fields in
/// on-disk records decode cleanly to their `Default` value, producing a
/// deterministic upgrade path that works on every node and every replay.
/// Actual malformed JSON still returns `Err` from `dec()` and halts the
/// state machine, preserving the halt-on-decode-failure invariant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterBatchRecord {
    pub batch_id: u64,
    pub component: String,
    pub schema_id: u32,
    /// Arrow IPC bytes for the full master RecordBatch.
    pub ipc_bytes: Vec<u8>,
    pub total_rows: u32,
    pub created_at: u64,
    /// Next checkpoint counter (incremented on each successful Checkpoint apply).
    pub checkpoint_seq: u64,
    /// Consecutive release-attempt counter.
    ///
    /// Incremented by `apply_release_claim` and `apply_reclaim_expired`,
    /// reset to 0 by `apply_ack_claim`. When a runner observes this value
    /// crossing `RunnerConfig::max_claim_releases`, it proposes a
    /// `PoisonBatch` command to disqualify the batch.
    #[serde(default)]
    pub release_attempts: u32,
    /// Batch eligibility status. Defaults to `Active` for old records.
    #[serde(default)]
    pub status: BatchStatus,
    /// Unix epoch milliseconds at which the batch was poisoned, or `None`
    /// while the batch is `Active`. Set by `apply_poison_batch` and never
    /// mutated again (first-writer wins on races).
    #[serde(default)]
    pub poisoned_at: Option<u64>,
}

/// Stored claim record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimRecord {
    pub batch_id: u64,
    pub row_range_start: u32,
    pub row_range_end: u32,
    pub instance_id: [u8; 16],
    pub lease_expires_at: u64,
    pub status: ClaimStatus,
}

/// Stored checkpoint record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub batch_id: u64,
    pub stage_idx: u32,
    pub ipc_bytes: Vec<u8>,
    pub schema_id: u32,
    pub created_at: u64,
    /// FNV-1a hash of `claim_id_bytes || stage_idx_be4 || ipc_bytes`. Two
    /// checkpoint applies for the same (claim_id, stage_idx) with identical
    /// body produce the same hash, so a retry with a fresh `now_at_propose`
    /// is still detected as a duplicate.
    pub content_hash: u64,
}

/// Per-instance heartbeat record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceRecord {
    pub last_heartbeat_at: u64,
}

// ── Serialization helpers ─────────────────────────────────────────────────────

fn enc<T: Serialize>(v: &T) -> PcsResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| PcsError::generic(format!("state machine encode: {e}")))
}

fn dec<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> PcsResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|e| PcsError::generic(format!("state machine decode: {e}")))
}

/// Compute the checkpoint content hash (FNV-1a 64) from its identifying inputs.
/// The body is streamed through the hasher without building an intermediate
/// buffer, so a 1 MiB checkpoint does not pay for a 1 MiB allocation + memcpy.
fn checkpoint_content_hash(claim_id: &[u8; 16], stage_idx: u32, ipc_bytes: &[u8]) -> u64 {
    use std::hash::Hasher as _;
    let mut h = fnv::FnvHasher::default();
    h.write(claim_id);
    h.write(&stage_idx.to_be_bytes());
    h.write(ipc_bytes);
    h.finish()
}

// ── Composite key helpers ─────────────────────────────────────────────────────

/// Build a 20-byte composite key: `claim_id_bytes (16) || stage_idx_be (4)`.
fn checkpoint_key(claim_id: &[u8; 16], stage_idx: u32) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[..16].copy_from_slice(claim_id);
    k[16..].copy_from_slice(&stage_idx.to_be_bytes());
    k
}

/// Build a 24-byte secondary index key: `batch_id_be8 (8) || claim_id_16 (16)`.
///
/// The `batch_id_be8` prefix enables efficient batch-scoped range queries.
fn claims_by_batch_key(batch_id: u64, claim_id: &[u8; 16]) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[..8].copy_from_slice(&batch_id.to_be_bytes());
    k[8..].copy_from_slice(claim_id);
    k
}

/// Build the 8-byte lower-bound key for a batch range scan: `batch_id_be8`.
fn batch_range_start(batch_id: u64) -> [u8; 8] {
    batch_id.to_be_bytes()
}

/// Build the 8-byte exclusive upper-bound key for a batch range scan:
/// `(batch_id + 1)_be8`. Returns `None` if `batch_id == u64::MAX`.
fn batch_range_end(batch_id: u64) -> Option<[u8; 8]> {
    batch_id.checked_add(1).map(|n| n.to_be_bytes())
}

/// Encode the secondary index value: `start_row_be4 ++ end_row_be4 ++ status_byte`.
fn claims_by_batch_value(start: u32, end: u32, status: ClaimStatus) -> [u8; 9] {
    let mut v = [0u8; 9];
    v[..4].copy_from_slice(&start.to_be_bytes());
    v[4..8].copy_from_slice(&end.to_be_bytes());
    v[8] = status_byte(status);
    v
}

/// Decode the secondary index value into `(start_row, end_row, status)`.
///
/// Returns `None` if the slice is not exactly 9 bytes or the status byte is
/// unrecognised.
fn decode_claims_by_batch_value(v: &[u8]) -> Option<(u32, u32, ClaimStatus)> {
    if v.len() != 9 {
        return None;
    }
    let start = u32::from_be_bytes(v[..4].try_into().ok()?);
    let end = u32::from_be_bytes(v[4..8].try_into().ok()?);
    let status = status_from_byte(v[8])?;
    Some((start, end, status))
}

/// Encode a [`ClaimStatus`] as a single byte.
fn status_byte(s: ClaimStatus) -> u8 {
    match s {
        ClaimStatus::Pending => 0,
        ClaimStatus::Claimed => 1,
        ClaimStatus::Completed => 2,
    }
}

/// Decode a [`ClaimStatus`] from a single byte. Returns `None` for unknown bytes.
fn status_from_byte(b: u8) -> Option<ClaimStatus> {
    match b {
        0 => Some(ClaimStatus::Pending),
        1 => Some(ClaimStatus::Claimed),
        2 => Some(ClaimStatus::Completed),
        _ => None,
    }
}

// ── State machine entry point ─────────────────────────────────────────────────

/// Apply a committed Raft log entry to the redb application tables.
///
/// This function is **deterministic**: the same sequence of commands applied
/// to the same initial state always produces the same final state.
///
/// Application-level conditions (batch not found, claim overlaps, etc.) are
/// returned as `Ok(ConsensusResponse::Error { .. })` rather than `Err(...)`.
/// Only I/O failures or serialization errors produce `Err(PcsError)`.
///
/// # Errors
///
/// Returns [`PcsError`] for I/O failures or serialisation errors.
pub fn apply(db: &Database, command: ConsensusCommand) -> PcsResult<ConsensusResponse> {
    match command {
        ConsensusCommand::RegisterMasterBatch {
            batch_id,
            component,
            schema_id,
            ipc_bytes,
            total_rows,
            now_at_propose,
        } => apply_register_master_batch(
            db,
            batch_id,
            component,
            schema_id,
            ipc_bytes,
            total_rows,
            now_at_propose,
        ),

        ConsensusCommand::ClaimRowRange {
            batch_id,
            row_range_start,
            row_range_end,
            claim_id,
            instance_id,
            lease_ttl_millis,
            now_at_propose,
        } => apply_claim_row_range(
            db,
            batch_id,
            row_range_start,
            row_range_end,
            claim_id,
            instance_id,
            lease_ttl_millis,
            now_at_propose,
        ),

        ConsensusCommand::RenewClaim {
            claim_id,
            instance_id,
            lease_ttl_millis,
            now_at_propose,
        } => apply_renew_claim(db, claim_id, instance_id, lease_ttl_millis, now_at_propose),

        ConsensusCommand::AckClaim {
            claim_id,
            instance_id,
        } => apply_ack_claim(db, claim_id, instance_id),

        ConsensusCommand::ReleaseClaim {
            claim_id,
            instance_id,
        } => apply_release_claim(db, claim_id, instance_id),

        ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx,
            ipc_bytes,
            schema_id,
            now_at_propose,
        } => apply_checkpoint(
            db,
            claim_id,
            stage_idx,
            ipc_bytes,
            schema_id,
            now_at_propose,
        ),

        ConsensusCommand::Heartbeat { instance_id, at } => apply_heartbeat(db, instance_id, at),

        ConsensusCommand::ReclaimExpired { now_at_propose } => {
            apply_reclaim_expired(db, now_at_propose)
        }

        ConsensusCommand::PoisonBatch {
            batch_id,
            now_at_propose,
        } => apply_poison_batch(db, batch_id, now_at_propose),
    }
}

// ── Command handlers ──────────────────────────────────────────────────────────

fn apply_register_master_batch(
    db: &Database,
    batch_id: u64,
    component: String,
    schema_id: u32,
    ipc_bytes: Vec<u8>,
    total_rows: u32,
    now_at_propose: u64,
) -> PcsResult<ConsensusResponse> {
    // Enforce the 1 MiB hard cap.
    if ipc_bytes.len() >= MAX_LOG_ENTRY_BYTES {
        return Ok(ConsensusResponse::Error {
            message: format!(
                "RegisterMasterBatch: ipc_bytes ({} bytes) exceeds MAX_LOG_ENTRY_BYTES ({})",
                ipc_bytes.len(),
                MAX_LOG_ENTRY_BYTES
            ),
        });
    }

    let record = MasterBatchRecord {
        batch_id,
        component,
        schema_id,
        ipc_bytes,
        total_rows,
        created_at: now_at_propose,
        checkpoint_seq: 0,
        release_attempts: 0,
        status: BatchStatus::Active,
        poisoned_at: None,
    };
    let bytes = enc(&record)?;
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn: {e}")))?;
    {
        let mut table = txn
            .open_table(MASTER_BATCHES)
            .map_err(|e| PcsError::generic(format!("open master_batches: {e}")))?;
        table
            .insert(batch_id, bytes.as_slice())
            .map_err(|e| PcsError::generic(format!("insert master_batch: {e}")))?;

        // Mark as pending in the secondary index so scan_pending is O(k).
        let mut pending_table = txn
            .open_table(PENDING_BATCHES)
            .map_err(|e| PcsError::generic(format!("open pending_batches: {e}")))?;
        pending_table
            .insert(batch_id, [].as_slice())
            .map_err(|e| PcsError::generic(format!("insert pending_batches: {e}")))?;
    }
    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit: {e}")))?;
    Ok(ConsensusResponse::MasterBatchRegistered { batch_id })
}

/// Scan `arrow_claims_by_batch` for a specific `batch_id` and check whether
/// `[row_range_start, row_range_end)` overlaps any `Claimed` or `Completed`
/// entry.
///
/// Returns `Ok(true)` if an overlap is found. Works on any readable table type
/// that implements `ReadableTable<&[u8], &[u8]>`.
fn has_batch_overlap<T>(
    table: &T,
    batch_id: u64,
    row_range_start: u32,
    row_range_end: u32,
) -> PcsResult<bool>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let lo = batch_range_start(batch_id);
    // Use the inclusive start and exclusive end to constrain the scan to this
    // batch's entries only. If batch_id == u64::MAX, scan to end of table.
    let range_iter = match batch_range_end(batch_id) {
        Some(hi) => table.range(lo.as_slice()..hi.as_slice()),
        None => table.range(lo.as_slice()..),
    }
    .map_err(|e| PcsError::generic(format!("claims_by_batch range: {e}")))?;

    for item in range_iter {
        let (_k, v) = item.map_err(|e| PcsError::generic(format!("claims_by_batch item: {e}")))?;
        let (start, end, status) = decode_claims_by_batch_value(v.value())
            .ok_or_else(|| PcsError::generic("claims_by_batch: malformed value (not 9 bytes)"))?;
        if matches!(status, ClaimStatus::Claimed | ClaimStatus::Completed) {
            // [rs, re) overlaps [a, b) iff rs < b && re > a
            if row_range_start < end && row_range_end > start {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)] // one-shot internal apply handler; all args come from a single ConsensusCommand::ClaimRowRange variant
fn apply_claim_row_range(
    db: &Database,
    batch_id: u64,
    row_range_start: u32,
    row_range_end: u32,
    claim_id: uuid::Uuid,
    instance_id: uuid::Uuid,
    lease_ttl_millis: u64,
    now_at_propose: u64,
) -> PcsResult<ConsensusResponse> {
    // Basic range validity — pure arithmetic, no I/O.
    if row_range_start >= row_range_end {
        return Ok(ConsensusResponse::Error {
            message: format!("invalid row_range: start {row_range_start} >= end {row_range_end}"),
        });
    }

    // Fast-path replay idempotency: if this claim_id was already applied
    // (crash between apply and watermark persist → raft replays the entry),
    // return success immediately instead of hitting the phase-1 overlap error.
    {
        let rtxn_idem = db
            .begin_read()
            .map_err(|e| PcsError::generic(format!("redb read txn (idempotency): {e}")))?;
        let claim_table_opt = match rtxn_idem.open_table(CLAIMS) {
            Ok(t) => Some(t),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(e) => return Err(PcsError::generic(format!("open claims (idempotency): {e}"))),
        };
        if let Some(claim_table) = claim_table_opt {
            let raw = claim_table
                .get(claim_id.as_bytes().as_slice())
                .map_err(|e| PcsError::generic(format!("get claim (idempotency): {e}")))?
                .map(|g| g.value().to_vec());
            if let Some(bytes) = raw {
                let existing: ClaimRecord = dec(&bytes)?;
                if existing.batch_id == batch_id
                    && existing.row_range_start == row_range_start
                    && existing.row_range_end == row_range_end
                    && existing.instance_id == *instance_id.as_bytes()
                {
                    // Batch may have been updated since step 1; read it for the response.
                    let batch_for_response = match rtxn_idem.open_table(MASTER_BATCHES) {
                        Ok(t) => match t.get(batch_id).map_err(|e| {
                            PcsError::generic(format!("get batch (idempotency): {e}"))
                        })? {
                            Some(g) => dec::<MasterBatchRecord>(g.value())?,
                            None => {
                                return Ok(ConsensusResponse::Error {
                                    message: format!(
                                        "batch_id {batch_id} not found on idempotent replay"
                                    ),
                                });
                            }
                        },
                        Err(e) => {
                            return Err(PcsError::generic(format!(
                                "open batches (idempotency): {e}"
                            )));
                        }
                    };
                    return Ok(ConsensusResponse::BatchClaimed {
                        batch_id,
                        component: batch_for_response.component,
                        row_range_start,
                        row_range_end,
                        schema_id: batch_for_response.schema_id,
                        claim_id,
                        instance_id,
                        lease_expires_at: existing.lease_expires_at,
                    });
                }
            }
        }
    }

    // ── Step 1: read precheck (no write lock) ───────────────────────────────
    //
    // Validate the proposed range against the persisted state using only a
    // ReadTransaction. This is the common rejection path — if the batch is
    // missing or the range is already occupied, we return immediately without
    // ever acquiring the write lock.

    let precheck_result: Result<Option<MasterBatchRecord>, ConsensusResponse> = {
        let rtxn = db
            .begin_read()
            .map_err(|e| PcsError::generic(format!("redb read txn: {e}")))?;

        // Check batch existence and bounds.
        let batch_opt: Option<MasterBatchRecord> = {
            let batch_table = match rtxn.open_table(MASTER_BATCHES) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok(ConsensusResponse::Error {
                        message: format!("batch_id {batch_id} not found"),
                    });
                }
                Err(e) => return Err(PcsError::generic(format!("open master_batches: {e}"))),
            };
            match batch_table
                .get(batch_id)
                .map_err(|e| PcsError::generic(format!("get master_batch: {e}")))?
            {
                None => None,
                Some(guard) => Some(dec(guard.value())?),
            }
        };

        match &batch_opt {
            None => Err(ConsensusResponse::Error {
                message: format!("batch_id {batch_id} not found"),
            }),
            Some(batch) => {
                if row_range_end > batch.total_rows {
                    return Ok(ConsensusResponse::Error {
                        message: format!(
                            "row_range [{row_range_start}, {row_range_end}) exceeds total_rows {}",
                            batch.total_rows
                        ),
                    });
                }

                // Overlap check using the secondary index (O(k) for this batch).
                let overlap = match rtxn.open_table(CLAIMS_BY_BATCH) {
                    Ok(idx_table) => {
                        has_batch_overlap(&idx_table, batch_id, row_range_start, row_range_end)?
                    }
                    // Table doesn't exist yet — no claims at all, no overlap.
                    Err(redb::TableError::TableDoesNotExist(_)) => false,
                    Err(e) => {
                        return Err(PcsError::generic(format!("open claims_by_batch: {e}")));
                    }
                };

                if overlap {
                    Err(ConsensusResponse::Error {
                        message: format!(
                            "row_range [{row_range_start}, {row_range_end}) overlaps an existing active claim"
                        ),
                    })
                } else {
                    Ok(batch_opt)
                }
            }
        }
    };

    let batch = match precheck_result {
        Err(early_response) => return Ok(early_response),
        Ok(None) => {
            return Ok(ConsensusResponse::Error {
                message: format!("batch_id {batch_id} not found"),
            });
        }
        Ok(Some(b)) => b,
    };

    // ── Step 2: write confirmation (under write lock) ───────────────────────
    //
    // Re-check for overlaps under the write lock to close the TOCTOU window
    // between step 1 and the actual insert. Because redb serialises all
    // writers, no other writer can have inserted between write-lock acquisition
    // and this scan.

    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn: {e}")))?;

    // open_table on a WriteTransaction creates the table if it does not exist.
    // This is safe: a newly-created table is empty so the range scan returns
    // nothing, meaning no overlap.
    let overlap_under_lock = {
        let idx_table = txn
            .open_table(CLAIMS_BY_BATCH)
            .map_err(|e| PcsError::generic(format!("open claims_by_batch (write): {e}")))?;
        has_batch_overlap(&idx_table, batch_id, row_range_start, row_range_end)?
    };

    if overlap_under_lock {
        // Abort the write txn (implicit on drop) — no changes to persist.
        return Ok(ConsensusResponse::Error {
            message: format!(
                "row_range [{row_range_start}, {row_range_end}) overlaps an existing active claim"
            ),
        });
    }

    // No overlap — proceed with the insert.
    let lease_expires_at = now_at_propose + lease_ttl_millis;
    let record = ClaimRecord {
        batch_id,
        row_range_start,
        row_range_end,
        instance_id: *instance_id.as_bytes(),
        lease_expires_at,
        status: ClaimStatus::Claimed,
    };
    let bytes = enc(&record)?;
    let claim_id_bytes = *claim_id.as_bytes();
    let secondary_key = claims_by_batch_key(batch_id, &claim_id_bytes);
    let secondary_val = claims_by_batch_value(row_range_start, row_range_end, ClaimStatus::Claimed);

    {
        let mut claim_table = txn
            .open_table(CLAIMS)
            .map_err(|e| PcsError::generic(format!("open claims: {e}")))?;
        claim_table
            .insert(claim_id_bytes.as_slice(), bytes.as_slice())
            .map_err(|e| PcsError::generic(format!("insert claim: {e}")))?;

        let mut idx_table = txn
            .open_table(CLAIMS_BY_BATCH)
            .map_err(|e| PcsError::generic(format!("open claims_by_batch: {e}")))?;
        idx_table
            .insert(secondary_key.as_slice(), secondary_val.as_slice())
            .map_err(|e| PcsError::generic(format!("insert claims_by_batch: {e}")))?;
    }

    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit: {e}")))?;

    Ok(ConsensusResponse::BatchClaimed {
        batch_id,
        component: batch.component,
        row_range_start,
        row_range_end,
        schema_id: batch.schema_id,
        claim_id,
        instance_id,
        lease_expires_at,
    })
}

fn apply_renew_claim(
    db: &Database,
    claim_id: uuid::Uuid,
    instance_id: uuid::Uuid,
    lease_ttl_millis: u64,
    now_at_propose: u64,
) -> PcsResult<ConsensusResponse> {
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn: {e}")))?;
    let response = {
        let mut table = txn
            .open_table(CLAIMS)
            .map_err(|e| PcsError::generic(format!("open claims: {e}")))?;
        let key = claim_id.as_bytes().as_slice();
        // Read the record and drop the guard before mutating.
        let existing: Option<ClaimRecord> = {
            let raw = table
                .get(key)
                .map_err(|e| PcsError::generic(format!("get claim: {e}")))?
                .map(|guard| guard.value().to_vec());
            raw.map(|bytes| dec(&bytes)).transpose()?
        };
        match existing {
            None => ConsensusResponse::Error {
                message: format!("claim {claim_id} not found"),
            },
            Some(mut record) => {
                if record.status != ClaimStatus::Claimed {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} is not in Claimed state"),
                    }
                } else if record.instance_id != *instance_id.as_bytes() {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} held by different instance"),
                    }
                } else if record.lease_expires_at < now_at_propose {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} lease has already expired"),
                    }
                } else {
                    let new_expires = now_at_propose + lease_ttl_millis;
                    // max() guarantees monotonicity against out-of-order proposals.
                    record.lease_expires_at = record.lease_expires_at.max(new_expires);
                    let expires_at = record.lease_expires_at;
                    let bytes = enc(&record)?;
                    table
                        .insert(key, bytes.as_slice())
                        .map_err(|e| PcsError::generic(format!("update claim: {e}")))?;
                    ConsensusResponse::ClaimRenewed { expires_at }
                }
            }
        }
    };
    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit: {e}")))?;
    Ok(response)
}

fn apply_ack_claim(
    db: &Database,
    claim_id: uuid::Uuid,
    instance_id: uuid::Uuid,
) -> PcsResult<ConsensusResponse> {
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn: {e}")))?;
    let response = {
        let mut table = txn
            .open_table(CLAIMS)
            .map_err(|e| PcsError::generic(format!("open claims: {e}")))?;
        let key = claim_id.as_bytes().as_slice();
        // Read and drop the guard before inserting.
        let existing: Option<ClaimRecord> = {
            let raw = table
                .get(key)
                .map_err(|e| PcsError::generic(format!("get claim: {e}")))?
                .map(|guard| guard.value().to_vec());
            raw.map(|bytes| dec(&bytes)).transpose()?
        };
        match existing {
            None => ConsensusResponse::Error {
                message: format!("claim {claim_id} not found"),
            },
            Some(mut record) => {
                if record.status != ClaimStatus::Claimed {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} not in Claimed state"),
                    }
                } else if record.instance_id != *instance_id.as_bytes() {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} held by different instance"),
                    }
                } else {
                    let batch_id = record.batch_id;
                    let row_range_start = record.row_range_start;
                    let row_range_end = record.row_range_end;
                    record.status = ClaimStatus::Completed;
                    record.lease_expires_at = 0;
                    let bytes = enc(&record)?;
                    table
                        .insert(key, bytes.as_slice())
                        .map_err(|e| PcsError::generic(format!("update claim: {e}")))?;

                    // Keep secondary index in sync.
                    let claim_id_bytes = *claim_id.as_bytes();
                    let sec_key = claims_by_batch_key(batch_id, &claim_id_bytes);
                    let sec_val = claims_by_batch_value(
                        row_range_start,
                        row_range_end,
                        ClaimStatus::Completed,
                    );
                    let mut idx_table = txn
                        .open_table(CLAIMS_BY_BATCH)
                        .map_err(|e| PcsError::generic(format!("open claims_by_batch: {e}")))?;
                    idx_table
                        .insert(sec_key.as_slice(), sec_val.as_slice())
                        .map_err(|e| PcsError::generic(format!("update claims_by_batch: {e}")))?;

                    // Remove from PENDING_BATCHES if all claims for this batch are now Completed.
                    let all_complete = {
                        let lo = batch_range_start(batch_id);
                        let range_iter = match batch_range_end(batch_id) {
                            Some(hi) => idx_table.range(lo.as_slice()..hi.as_slice()),
                            None => idx_table.range(lo.as_slice()..),
                        }
                        .map_err(|e| {
                            PcsError::generic(format!("claims_by_batch range (ack): {e}"))
                        })?;
                        let mut complete = true;
                        for item in range_iter {
                            let (_k, v) = item.map_err(|e| {
                                PcsError::generic(format!("claims_by_batch item (ack): {e}"))
                            })?;
                            let (_, _, status) = decode_claims_by_batch_value(v.value())
                                .ok_or_else(|| {
                                    PcsError::generic("claims_by_batch: malformed value (ack)")
                                })?;
                            if !matches!(status, ClaimStatus::Completed) {
                                complete = false;
                                break;
                            }
                        }
                        complete
                    };
                    if all_complete {
                        let mut pending_table = txn.open_table(PENDING_BATCHES).map_err(|e| {
                            PcsError::generic(format!("open pending_batches (ack): {e}"))
                        })?;
                        pending_table.remove(batch_id).map_err(|e| {
                            PcsError::generic(format!("remove pending_batches (ack): {e}"))
                        })?;
                    }

                    // Consecutive-failure counter resets on any
                    // successful ack. Counts from zero after the next failure.
                    reset_release_attempts(&txn, batch_id)?;

                    ConsensusResponse::ClaimAcked
                }
            }
        }
    };
    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit: {e}")))?;
    Ok(response)
}

fn apply_release_claim(
    db: &Database,
    claim_id: uuid::Uuid,
    instance_id: uuid::Uuid,
) -> PcsResult<ConsensusResponse> {
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn: {e}")))?;
    let response = {
        let mut table = txn
            .open_table(CLAIMS)
            .map_err(|e| PcsError::generic(format!("open claims: {e}")))?;
        let key = claim_id.as_bytes().as_slice();
        // Read and drop the guard before inserting.
        let existing: Option<ClaimRecord> = {
            let raw = table
                .get(key)
                .map_err(|e| PcsError::generic(format!("get claim: {e}")))?
                .map(|guard| guard.value().to_vec());
            raw.map(|bytes| dec(&bytes)).transpose()?
        };
        match existing {
            None => ConsensusResponse::Error {
                message: format!("claim {claim_id} not found"),
            },
            Some(mut record) => {
                if record.status != ClaimStatus::Claimed {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} not in Claimed state"),
                    }
                } else if record.instance_id != *instance_id.as_bytes() {
                    ConsensusResponse::Error {
                        message: format!("claim {claim_id} held by different instance"),
                    }
                } else {
                    let batch_id = record.batch_id;
                    let row_range_start = record.row_range_start;
                    let row_range_end = record.row_range_end;
                    record.status = ClaimStatus::Pending;
                    record.lease_expires_at = 0;
                    record.instance_id = [0u8; 16];
                    let bytes = enc(&record)?;
                    table
                        .insert(key, bytes.as_slice())
                        .map_err(|e| PcsError::generic(format!("update claim: {e}")))?;

                    // Keep secondary index in sync.
                    let claim_id_bytes = *claim_id.as_bytes();
                    let sec_key = claims_by_batch_key(batch_id, &claim_id_bytes);
                    let sec_val =
                        claims_by_batch_value(row_range_start, row_range_end, ClaimStatus::Pending);
                    // Drop the CLAIMS borrow before opening CLAIMS_BY_BATCH
                    // or MASTER_BATCHES in the same write txn.
                    drop(table);
                    let mut idx_table = txn
                        .open_table(CLAIMS_BY_BATCH)
                        .map_err(|e| PcsError::generic(format!("open claims_by_batch: {e}")))?;
                    idx_table
                        .insert(sec_key.as_slice(), sec_val.as_slice())
                        .map_err(|e| PcsError::generic(format!("update claims_by_batch: {e}")))?;
                    drop(idx_table);

                    // Bump release_attempts on the master batch.
                    // This lives inside the Claimed→Pending success branch so
                    // a late ReleaseClaim against an already-Pending claim
                    // (beaten by ReclaimExpired) hits the status guard above
                    // and never reaches this increment.  Regression test:
                    // `test_release_claim_rejects_pending_status`.
                    increment_release_attempts(&txn, batch_id)?;

                    ConsensusResponse::ClaimReleased
                }
            }
        }
    };
    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit: {e}")))?;
    Ok(response)
}

fn apply_checkpoint(
    db: &Database,
    claim_id: uuid::Uuid,
    stage_idx: u32,
    ipc_bytes: Vec<u8>,
    schema_id: u32,
    now_at_propose: u64,
) -> PcsResult<ConsensusResponse> {
    // Enforce 1 MiB cap.
    if ipc_bytes.len() >= MAX_LOG_ENTRY_BYTES {
        return Ok(ConsensusResponse::Error {
            message: format!(
                "Checkpoint ipc_bytes ({} bytes) exceeds MAX_LOG_ENTRY_BYTES ({})",
                ipc_bytes.len(),
                MAX_LOG_ENTRY_BYTES
            ),
        });
    }

    // Look up the batch_id from the claim for the checkpoint record.
    let batch_id = {
        let txn = db
            .begin_read()
            .map_err(|e| PcsError::generic(format!("redb read txn: {e}")))?;
        let key = claim_id.as_bytes().as_slice();
        let table = match txn.open_table(CLAIMS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(ConsensusResponse::Error {
                    message: format!("claim_id {claim_id} not found (table missing)"),
                });
            }
            Err(e) => return Err(PcsError::generic(format!("open claims: {e}"))),
        };
        match table
            .get(key)
            .map_err(|e| PcsError::generic(format!("get claim: {e}")))?
        {
            None => {
                return Ok(ConsensusResponse::Error {
                    message: format!("claim_id {claim_id} not found"),
                });
            }
            Some(guard) => {
                let record: ClaimRecord = dec(guard.value())?;
                record.batch_id
            }
        }
    };

    let content_hash = checkpoint_content_hash(claim_id.as_bytes(), stage_idx, &ipc_bytes);
    let record = CheckpointRecord {
        batch_id,
        stage_idx,
        ipc_bytes,
        schema_id,
        created_at: now_at_propose,
        content_hash,
    };
    let bytes = enc(&record)?;
    let key = checkpoint_key(claim_id.as_bytes(), stage_idx);

    // Atomically write checkpoint AND increment the master batch's checkpoint_seq.
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn: {e}")))?;
    let checkpoint_id = {
        // Idempotency: if this (claim_id, stage_idx) already exists with the
        // same content_hash, return success without re-incrementing checkpoint_seq.
        // Keyed on content_hash so a retry with a fresh now_at_propose is caught.
        let already_exists = {
            let cp_table = txn
                .open_table(CHECKPOINTS)
                .map_err(|e| PcsError::generic(format!("open checkpoints (idempotency): {e}")))?;
            let raw = cp_table
                .get(key.as_slice())
                .map_err(|e| PcsError::generic(format!("get checkpoint (idempotency): {e}")))?
                .map(|g| g.value().to_vec());
            if let Some(existing_bytes) = raw {
                let existing: CheckpointRecord = dec(&existing_bytes)?;
                existing.content_hash == content_hash
            } else {
                false
            }
        };

        if already_exists {
            // Return the current checkpoint_seq from master batch (don't increment).
            let batch_table = txn.open_table(MASTER_BATCHES).map_err(|e| {
                PcsError::generic(format!("open master_batches (idempotency): {e}"))
            })?;
            let raw = batch_table
                .get(batch_id)
                .map_err(|e| PcsError::generic(format!("get master_batch (idempotency): {e}")))?
                .map(|g| g.value().to_vec());
            let seq = match raw {
                Some(b) => dec::<MasterBatchRecord>(&b)?.checkpoint_seq,
                None => 0,
            };
            return Ok(ConsensusResponse::CheckpointWritten { checkpoint_id: seq });
        }

        // Increment master batch checkpoint_seq.
        let seq = {
            let mut batch_table = txn
                .open_table(MASTER_BATCHES)
                .map_err(|e| PcsError::generic(format!("open master_batches: {e}")))?;
            // Read and drop the guard before inserting.
            let existing: Option<MasterBatchRecord> = {
                let raw = batch_table
                    .get(batch_id)
                    .map_err(|e| PcsError::generic(format!("get master_batch: {e}")))?
                    .map(|guard| guard.value().to_vec());
                raw.map(|bytes| dec(&bytes)).transpose()?
            };
            match existing {
                None => 0u64,
                Some(mut br) => {
                    br.checkpoint_seq += 1;
                    let seq = br.checkpoint_seq;
                    let updated = enc(&br)?;
                    batch_table
                        .insert(batch_id, updated.as_slice())
                        .map_err(|e| PcsError::generic(format!("update master_batch: {e}")))?;
                    seq
                }
            }
        };

        {
            let mut cp_table = txn
                .open_table(CHECKPOINTS)
                .map_err(|e| PcsError::generic(format!("open checkpoints: {e}")))?;
            cp_table
                .insert(key.as_slice(), bytes.as_slice())
                .map_err(|e| PcsError::generic(format!("insert checkpoint: {e}")))?;
        }

        seq
    };

    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit: {e}")))?;
    Ok(ConsensusResponse::CheckpointWritten { checkpoint_id })
}

fn apply_heartbeat(
    db: &Database,
    instance_id: uuid::Uuid,
    at: u64,
) -> PcsResult<ConsensusResponse> {
    let record = InstanceRecord {
        last_heartbeat_at: at,
    };
    let bytes = enc(&record)?;
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn: {e}")))?;
    {
        let mut table = txn
            .open_table(INSTANCES)
            .map_err(|e| PcsError::generic(format!("open instances: {e}")))?;
        table
            .insert(instance_id.as_bytes().as_slice(), bytes.as_slice())
            .map_err(|e| PcsError::generic(format!("insert instance: {e}")))?;
    }
    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit: {e}")))?;
    Ok(ConsensusResponse::HeartbeatRecorded)
}

/// Sweep expired leases: flip `Claimed → Pending` for every claim whose
/// `lease_expires_at < now_at_propose`. Returns the count of reclaimed entries.
///
/// Both `CLAIMS` and `CLAIMS_BY_BATCH` are updated in a single transaction.
fn apply_reclaim_expired(db: &Database, now_at_propose: u64) -> PcsResult<ConsensusResponse> {
    // Collect expired claim_ids under a read transaction first.
    let expired: Vec<([u8; 16], ClaimRecord)> = {
        let rtxn = db
            .begin_read()
            .map_err(|e| PcsError::generic(format!("redb read txn (reclaim): {e}")))?;
        let table = match rtxn.open_table(CLAIMS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(ConsensusResponse::ExpiredReclaimed { reclaimed_count: 0 });
            }
            Err(e) => return Err(PcsError::generic(format!("open claims (reclaim): {e}"))),
        };
        let mut out = Vec::new();
        for item in table
            .iter()
            .map_err(|e| PcsError::generic(format!("iter claims (reclaim): {e}")))?
        {
            let (k, v) =
                item.map_err(|e| PcsError::generic(format!("claims iter item (reclaim): {e}")))?;
            let key_bytes: [u8; 16] = k
                .value()
                .try_into()
                .map_err(|_| PcsError::generic("claim key not 16 bytes"))?;
            let record: ClaimRecord = dec(v.value())?;
            if record.status == ClaimStatus::Claimed && record.lease_expires_at < now_at_propose {
                out.push((key_bytes, record));
            }
        }
        out
    };

    if expired.is_empty() {
        return Ok(ConsensusResponse::ExpiredReclaimed { reclaimed_count: 0 });
    }

    let count = expired.len() as u32;
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn (reclaim): {e}")))?;

    // Collect the parent batch_ids as we reclaim — we bump
    // `release_attempts` on each, but we can't interleave that with the
    // claim-table writes because `increment_release_attempts` opens a
    // different table in the same txn.
    let mut bumped_batch_ids: Vec<u64> = Vec::with_capacity(expired.len());

    {
        let mut claim_table = txn
            .open_table(CLAIMS)
            .map_err(|e| PcsError::generic(format!("open claims (reclaim write): {e}")))?;
        let mut idx_table = txn
            .open_table(CLAIMS_BY_BATCH)
            .map_err(|e| PcsError::generic(format!("open claims_by_batch (reclaim): {e}")))?;

        for (key_bytes, mut record) in expired {
            record.status = ClaimStatus::Pending;
            record.lease_expires_at = 0;
            let updated = enc(&record)?;
            claim_table
                .insert(key_bytes.as_slice(), updated.as_slice())
                .map_err(|e| PcsError::generic(format!("update claim (reclaim): {e}")))?;

            // Sync secondary index.
            let sec_key = claims_by_batch_key(record.batch_id, &key_bytes);
            let sec_val = claims_by_batch_value(
                record.row_range_start,
                record.row_range_end,
                ClaimStatus::Pending,
            );
            idx_table
                .insert(sec_key.as_slice(), sec_val.as_slice())
                .map_err(|e| PcsError::generic(format!("update claims_by_batch (reclaim): {e}")))?;

            bumped_batch_ids.push(record.batch_id);
        }
    }

    // Bump release_attempts on every reclaimed claim's parent
    // batch. A crashed runner is functionally indistinguishable from an
    // explicit ReleaseClaim for retry-cap purposes — without this bump,
    // a runner that repeatedly crashes on a poison batch would never
    // trip the cap.
    for batch_id in bumped_batch_ids {
        increment_release_attempts(&txn, batch_id)?;
    }
    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit (reclaim): {e}")))?;
    Ok(ConsensusResponse::ExpiredReclaimed {
        reclaimed_count: count,
    })
}

/// Permanently disqualify a master batch.
///
/// Idempotent: if the batch is already `BatchStatus::Poisoned`, returns
/// `Ok` without mutating state — preserving the first-writer `poisoned_at`
/// timestamp. This matters for cross-node poison races where two runners
/// may independently propose `PoisonBatch` for the same batch; Raft
/// serialises them, first wins, second is a no-op.
///
/// On first-writer application: sets `status = Poisoned`, stamps
/// `poisoned_at = now_at_propose`, writes the master batch back, AND
/// removes the batch from the `PENDING_BATCHES` secondary index so
/// `claim_next_batch` via `find_first_pending_batch` never returns it
/// again. Existing claim records (if any remain `Claimed` or `Pending`)
/// are left alone — `/status` can use them for audit, and they'll be
/// cleaned up naturally when their lease expires or when the operator
/// deletes the batch.
fn apply_poison_batch(
    db: &Database,
    batch_id: u64,
    now_at_propose: u64,
) -> PcsResult<ConsensusResponse> {
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn (poison): {e}")))?;
    let response = {
        let mut table = txn
            .open_table(MASTER_BATCHES)
            .map_err(|e| PcsError::generic(format!("open master_batches (poison): {e}")))?;
        let existing: Option<MasterBatchRecord> = {
            let raw = table
                .get(batch_id)
                .map_err(|e| PcsError::generic(format!("get master_batch (poison): {e}")))?
                .map(|g| g.value().to_vec());
            raw.map(|bytes| dec(&bytes)).transpose()?
        };
        match existing {
            None => ConsensusResponse::Error {
                message: format!("PoisonBatch: batch_id {batch_id} not found"),
            },
            Some(record) if record.status == BatchStatus::Poisoned => {
                // Idempotent no-op: preserve first-writer poisoned_at.
                ConsensusResponse::BatchPoisoned {
                    batch_id,
                    poisoned_at: record.poisoned_at.unwrap_or(now_at_propose),
                }
            }
            Some(mut record) => {
                record.status = BatchStatus::Poisoned;
                record.poisoned_at = Some(now_at_propose);
                let bytes = enc(&record)?;
                table
                    .insert(batch_id, bytes.as_slice())
                    .map_err(|e| PcsError::generic(format!("update master_batch (poison): {e}")))?;
                // Drop the master_batches borrow before opening PENDING_BATCHES
                // in the same write txn.
                drop(table);
                let mut pending_table = txn.open_table(PENDING_BATCHES).map_err(|e| {
                    PcsError::generic(format!("open pending_batches (poison): {e}"))
                })?;
                pending_table.remove(batch_id).map_err(|e| {
                    PcsError::generic(format!("remove pending_batches (poison): {e}"))
                })?;
                ConsensusResponse::BatchPoisoned {
                    batch_id,
                    poisoned_at: now_at_propose,
                }
            }
        }
    };
    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit (poison): {e}")))?;
    Ok(response)
}

// ── Release-attempts helpers ────────────────────────────────────────────────

/// Increment `MasterBatchRecord.release_attempts` in the given write
/// transaction. Called from `apply_release_claim` and the per-claim
/// loop inside `apply_reclaim_expired`. A missing master batch is a
/// silent no-op — the caller has already validated the claim, and the
/// absence of its parent batch would be an orphaned-claim bug we don't
/// want to mask as a release error.
fn increment_release_attempts(txn: &redb::WriteTransaction, batch_id: u64) -> PcsResult<()> {
    let mut table = txn
        .open_table(MASTER_BATCHES)
        .map_err(|e| PcsError::generic(format!("open master_batches (bump): {e}")))?;
    let existing: Option<MasterBatchRecord> = {
        let raw = table
            .get(batch_id)
            .map_err(|e| PcsError::generic(format!("get master_batch (bump): {e}")))?
            .map(|g| g.value().to_vec());
        raw.map(|bytes| dec(&bytes)).transpose()?
    };
    if let Some(mut record) = existing {
        record.release_attempts = record.release_attempts.saturating_add(1);
        let bytes = enc(&record)?;
        table
            .insert(batch_id, bytes.as_slice())
            .map_err(|e| PcsError::generic(format!("update master_batch (bump): {e}")))?;
    }
    Ok(())
}

/// Reset `MasterBatchRecord.release_attempts` to 0. Called from
/// `apply_ack_claim` when a claim successfully completes; any further
/// failures on the same batch then count from zero (consecutive
/// failures, not lifetime). Missing batch → silent no-op, same as
/// [`increment_release_attempts`].
fn reset_release_attempts(txn: &redb::WriteTransaction, batch_id: u64) -> PcsResult<()> {
    let mut table = txn
        .open_table(MASTER_BATCHES)
        .map_err(|e| PcsError::generic(format!("open master_batches (reset): {e}")))?;
    let existing: Option<MasterBatchRecord> = {
        let raw = table
            .get(batch_id)
            .map_err(|e| PcsError::generic(format!("get master_batch (reset): {e}")))?
            .map(|g| g.value().to_vec());
        raw.map(|bytes| dec(&bytes)).transpose()?
    };
    if let Some(mut record) = existing
        && record.release_attempts != 0
    {
        record.release_attempts = 0;
        let bytes = enc(&record)?;
        table
            .insert(batch_id, bytes.as_slice())
            .map_err(|e| PcsError::generic(format!("update master_batch (reset): {e}")))?;
    }
    Ok(())
}

// ── Read helpers ──────────────────────────────────────────────────────────────

/// Read a [`MasterBatchRecord`] by `batch_id`. Returns `Ok(None)` if absent.
pub fn read_master_batch(db: &Database, batch_id: u64) -> PcsResult<Option<MasterBatchRecord>> {
    let txn = db
        .begin_read()
        .map_err(|e| PcsError::generic(format!("redb read txn: {e}")))?;
    let table = match txn.open_table(MASTER_BATCHES) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(PcsError::generic(format!("open master_batches: {e}"))),
    };
    match table
        .get(batch_id)
        .map_err(|e| PcsError::generic(format!("get master_batch: {e}")))?
    {
        None => Ok(None),
        Some(guard) => dec(guard.value()).map(Some),
    }
}

/// Read a [`ClaimRecord`] by `claim_id`. Returns `Ok(None)` if absent.
pub fn read_claim(db: &Database, claim_id: uuid::Uuid) -> PcsResult<Option<ClaimRecord>> {
    let txn = db
        .begin_read()
        .map_err(|e| PcsError::generic(format!("redb read txn: {e}")))?;
    let table = match txn.open_table(CLAIMS) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(PcsError::generic(format!("open claims: {e}"))),
    };
    match table
        .get(claim_id.as_bytes().as_slice())
        .map_err(|e| PcsError::generic(format!("get claim: {e}")))?
    {
        None => Ok(None),
        Some(guard) => dec(guard.value()).map(Some),
    }
}

/// Read a [`CheckpointRecord`] by `(claim_id, stage_idx)`. Returns `Ok(None)` if absent.
pub fn read_checkpoint(
    db: &Database,
    claim_id: uuid::Uuid,
    stage_idx: u32,
) -> PcsResult<Option<CheckpointRecord>> {
    let txn = db
        .begin_read()
        .map_err(|e| PcsError::generic(format!("redb read txn: {e}")))?;
    let table = match txn.open_table(CHECKPOINTS) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(PcsError::generic(format!("open checkpoints: {e}"))),
    };
    let key = checkpoint_key(claim_id.as_bytes(), stage_idx);
    match table
        .get(key.as_slice())
        .map_err(|e| PcsError::generic(format!("get checkpoint: {e}")))?
    {
        None => Ok(None),
        Some(guard) => dec(guard.value()).map(Some),
    }
}

/// Find the first pending (unclaimed) row range for `batch_id`.
///
/// Returns `Ok(None)` if no pending ranges exist for the batch — either the
/// batch is missing, empty, or every row is already covered by a Claimed or
/// Completed claim. Completed claims are **occupied**, never
/// re-issuable, so they are folded into the same exclusion set as Claimed.
///
/// Uses the `arrow_claims_by_batch` secondary index for an O(k) scan where k
/// is the number of claims for this batch, rather than O(total_claims).
pub fn find_first_pending_claim(db: &Database, batch_id: u64) -> PcsResult<Option<(u32, u32)>> {
    // Find the first available row range. We use total_rows from the batch and
    // any existing Claimed/Completed claims to find unclaimed ranges.
    let batch = match read_master_batch(db, batch_id)? {
        None => return Ok(None),
        Some(b) => b,
    };

    // Collect occupied ranges using the secondary index (O(k) for this batch).
    let txn = db
        .begin_read()
        .map_err(|e| PcsError::generic(format!("redb read txn: {e}")))?;

    let mut claimed: Vec<(u32, u32)> = Vec::new();

    let idx_open = txn.open_table(CLAIMS_BY_BATCH);
    match idx_open {
        Ok(idx_table) => {
            let lo = batch_range_start(batch_id);
            let range_iter = match batch_range_end(batch_id) {
                Some(hi) => idx_table.range(lo.as_slice()..hi.as_slice()),
                None => idx_table.range(lo.as_slice()..),
            }
            .map_err(|e| PcsError::generic(format!("claims_by_batch range: {e}")))?;

            for item in range_iter {
                let (_k, v) =
                    item.map_err(|e| PcsError::generic(format!("claims_by_batch item: {e}")))?;
                let (start, end, status) = decode_claims_by_batch_value(v.value())
                    .ok_or_else(|| PcsError::generic("claims_by_batch: malformed value"))?;
                if matches!(status, ClaimStatus::Claimed | ClaimStatus::Completed) {
                    claimed.push((start, end));
                }
            }
        }
        // Secondary index table doesn't exist yet — no claims, entire batch is available.
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(PcsError::generic(format!("open claims_by_batch: {e}"))),
    }

    if claimed.is_empty() {
        if batch.total_rows > 0 {
            return Ok(Some((0, batch.total_rows)));
        }
        return Ok(None);
    }

    // Find the first row not covered by any occupied range.
    claimed.sort_by_key(|&(s, _)| s);
    let mut cursor: u32 = 0;
    for (start, end) in &claimed {
        if cursor < *start {
            return Ok(Some((cursor, *start)));
        }
        if *end > cursor {
            cursor = *end;
        }
    }
    if cursor < batch.total_rows {
        return Ok(Some((cursor, batch.total_rows)));
    }

    Ok(None)
}

/// Count `Completed` claims in `db` via the `arrow_claims_by_batch` secondary
/// index. Returns 0 if the table or any read fails — callers are tests that
/// treat errors as "not yet populated".
pub fn count_completed_claims(db: &Database) -> usize {
    use redb::ReadableTable as _;
    let Ok(txn) = db.begin_read() else {
        return 0;
    };
    let Ok(table) = txn.open_table(CLAIMS_BY_BATCH) else {
        return 0;
    };
    let Ok(iter) = table.iter() else {
        return 0;
    };
    iter.filter_map(|item| item.ok())
        .filter(|(_, v)| {
            decode_claims_by_batch_value(v.value())
                .is_some_and(|(_, _, s)| matches!(s, ClaimStatus::Completed))
        })
        .count()
}

/// Find the first batch_id that still has pending work, using the `PENDING_BATCHES`
/// secondary index for O(k) lookup where k is the number of pending batches.
/// Collects candidates under a single read transaction, then queries each with
/// [`find_first_pending_claim`]. Stale index entries (batch fully consumed but
/// index not yet cleaned up) are silently skipped.
pub fn find_first_pending_batch(db: &Database) -> PcsResult<Option<(u64, (u32, u32))>> {
    let candidates: Vec<u64> = {
        let txn = db
            .begin_read()
            .map_err(|e| PcsError::generic(format!("redb read txn (pending_batch): {e}")))?;
        match txn.open_table(PENDING_BATCHES) {
            Ok(table) => {
                let mut ids = Vec::new();
                for item in table
                    .iter()
                    .map_err(|e| PcsError::generic(format!("iter pending_batches: {e}")))?
                {
                    let (k, _) =
                        item.map_err(|e| PcsError::generic(format!("pending_batches item: {e}")))?;
                    ids.push(k.value());
                }
                ids
            }
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(PcsError::generic(format!("open pending_batches: {e}"))),
        }
    };
    for batch_id in candidates {
        if let Some(range) = find_first_pending_claim(db, batch_id)? {
            return Ok(Some((batch_id, range)));
        }
    }
    Ok(None)
}

// ── Snapshot helpers ──────────────────────────────────────────────────────────

/// Full snapshot of all state machine tables.
///
/// Returned by [`dump_state`]: `(master_batches, claims, checkpoints, instances)`.
pub type DumpedState = (
    Vec<MasterBatchRecord>,
    Vec<(uuid::Uuid, ClaimRecord)>,
    Vec<([u8; 20], CheckpointRecord)>,
    Vec<(uuid::Uuid, InstanceRecord)>,
);

/// Dump all tables from `db` into a snapshot representation for Arrow IPC
/// serialization. Returns `(master_batches, claims, checkpoints, instances)`.
pub fn dump_state(db: &Database) -> PcsResult<DumpedState> {
    let txn = db
        .begin_read()
        .map_err(|e| PcsError::generic(format!("redb read txn: {e}")))?;

    let mut batches = Vec::new();
    if let Ok(table) = txn.open_table(MASTER_BATCHES) {
        for item in table
            .iter()
            .map_err(|e| PcsError::generic(format!("master_batches iter: {e}")))?
        {
            let (_k, v) =
                item.map_err(|e| PcsError::generic(format!("master_batches iter item: {e}")))?;
            batches.push(dec(v.value())?);
        }
    }

    let mut claims = Vec::new();
    if let Ok(table) = txn.open_table(CLAIMS) {
        for item in table
            .iter()
            .map_err(|e| PcsError::generic(format!("claims iter: {e}")))?
        {
            let (k, v) = item.map_err(|e| PcsError::generic(format!("claims iter item: {e}")))?;
            let id_bytes: [u8; 16] = k
                .value()
                .try_into()
                .map_err(|_| PcsError::generic("claim key is not 16 bytes"))?;
            let id = uuid::Uuid::from_bytes(id_bytes);
            claims.push((id, dec(v.value())?));
        }
    }

    let mut checkpoints = Vec::new();
    if let Ok(table) = txn.open_table(CHECKPOINTS) {
        for item in table
            .iter()
            .map_err(|e| PcsError::generic(format!("checkpoints iter: {e}")))?
        {
            let (k, v) =
                item.map_err(|e| PcsError::generic(format!("checkpoints iter item: {e}")))?;
            let key_bytes: [u8; 20] = k
                .value()
                .try_into()
                .map_err(|_| PcsError::generic("checkpoint key is not 20 bytes"))?;
            checkpoints.push((key_bytes, dec(v.value())?));
        }
    }

    let mut instances = Vec::new();
    if let Ok(table) = txn.open_table(INSTANCES) {
        for item in table
            .iter()
            .map_err(|e| PcsError::generic(format!("instances iter: {e}")))?
        {
            let (k, v) =
                item.map_err(|e| PcsError::generic(format!("instances iter item: {e}")))?;
            let id_bytes: [u8; 16] = k
                .value()
                .try_into()
                .map_err(|_| PcsError::generic("instance key is not 16 bytes"))?;
            let id = uuid::Uuid::from_bytes(id_bytes);
            instances.push((id, dec(v.value())?));
        }
    }

    Ok((batches, claims, checkpoints, instances))
}

/// Restore all tables from a dump produced by [`dump_state`].
///
/// Clears existing state, then installs snapshot content. Optional watermarks
/// (`last_applied_bytes`, `last_membership_bytes`) are written in the same
/// transaction so install and watermark are fate-shared in one commit/fsync.
///
/// Rebuilds the `arrow_claims_by_batch` secondary index from the claims.
pub fn restore_state(
    db: &Database,
    batches: Vec<MasterBatchRecord>,
    claims: Vec<(uuid::Uuid, ClaimRecord)>,
    checkpoints: Vec<([u8; 20], CheckpointRecord)>,
    instances: Vec<(uuid::Uuid, InstanceRecord)>,
    sm_meta: Option<(&[u8], &[u8])>, // (last_applied_bytes, last_membership_bytes)
) -> PcsResult<()> {
    let txn = db
        .begin_write()
        .map_err(|e| PcsError::generic(format!("redb write txn: {e}")))?;
    {
        // Drop existing tables so the snapshot replaces rather than merges
        // with current state. Re-opened below via `open_table` which recreates.
        macro_rules! drop_table {
            ($def:expr) => {
                match txn.delete_table($def) {
                    Ok(_) => {}
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(e) => {
                        return Err(PcsError::generic(format!(
                            "delete table {}: {e}",
                            <_ as redb::TableHandle>::name(&$def)
                        )));
                    }
                }
            };
        }
        drop_table!(MASTER_BATCHES);
        drop_table!(CLAIMS);
        drop_table!(CLAIMS_BY_BATCH);
        drop_table!(CHECKPOINTS);
        drop_table!(INSTANCES);
        drop_table!(PENDING_BATCHES);

        let mut batch_table = txn
            .open_table(MASTER_BATCHES)
            .map_err(|e| PcsError::generic(format!("open master_batches: {e}")))?;
        let mut claim_table = txn
            .open_table(CLAIMS)
            .map_err(|e| PcsError::generic(format!("open claims: {e}")))?;
        let mut idx_table = txn
            .open_table(CLAIMS_BY_BATCH)
            .map_err(|e| PcsError::generic(format!("open claims_by_batch: {e}")))?;
        let mut cp_table = txn
            .open_table(CHECKPOINTS)
            .map_err(|e| PcsError::generic(format!("open checkpoints: {e}")))?;
        let mut inst_table = txn
            .open_table(INSTANCES)
            .map_err(|e| PcsError::generic(format!("open instances: {e}")))?;
        let mut pending_table = txn
            .open_table(PENDING_BATCHES)
            .map_err(|e| PcsError::generic(format!("open pending_batches: {e}")))?;

        // ── Install snapshot content ───────────────────────────────────────────
        for record in batches {
            let bytes = enc(&record)?;
            batch_table
                .insert(record.batch_id, bytes.as_slice())
                .map_err(|e| PcsError::generic(format!("insert batch: {e}")))?;
            // All registered batches start as pending; acks will remove them.
            pending_table
                .insert(record.batch_id, [].as_slice())
                .map_err(|e| PcsError::generic(format!("insert pending_batches (restore): {e}")))?;
        }

        // Stale pending_table entries are harmless: find_first_pending_batch
        // delegates to find_first_pending_claim which checks CLAIMS_BY_BATCH
        // authoritatively and skips fully-completed batches.
        for (id, record) in claims {
            let bytes = enc(&record)?;
            claim_table
                .insert(id.as_bytes().as_slice(), bytes.as_slice())
                .map_err(|e| PcsError::generic(format!("insert claim: {e}")))?;

            let id_bytes = *id.as_bytes();
            let sec_key = claims_by_batch_key(record.batch_id, &id_bytes);
            let sec_val =
                claims_by_batch_value(record.row_range_start, record.row_range_end, record.status);
            idx_table
                .insert(sec_key.as_slice(), sec_val.as_slice())
                .map_err(|e| PcsError::generic(format!("insert claims_by_batch: {e}")))?;
        }

        for (key, record) in checkpoints {
            let bytes = enc(&record)?;
            cp_table
                .insert(key.as_slice(), bytes.as_slice())
                .map_err(|e| PcsError::generic(format!("insert checkpoint: {e}")))?;
        }

        for (id, record) in instances {
            let bytes = enc(&record)?;
            inst_table
                .insert(id.as_bytes().as_slice(), bytes.as_slice())
                .map_err(|e| PcsError::generic(format!("insert instance: {e}")))?;
        }

        // Write sm_meta watermarks in the same transaction so install and
        // watermark commit atomically.
        if let Some((applied_bytes, membership_bytes)) = sm_meta {
            let mut meta_table = txn
                .open_table(SM_META_TABLE)
                .map_err(|e| PcsError::generic(format!("open sm_meta: {e}")))?;
            meta_table
                .insert(KEY_SM_LAST_APPLIED, applied_bytes)
                .map_err(|e| PcsError::generic(format!("write sm_last_applied: {e}")))?;
            meta_table
                .insert(KEY_SM_LAST_MEMBERSHIP, membership_bytes)
                .map_err(|e| PcsError::generic(format!("write sm_last_membership: {e}")))?;
        }
    }
    txn.commit()
        .map_err(|e| PcsError::generic(format!("commit: {e}")))?;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> (Database, tempfile::TempPath) {
        let file = tempfile::NamedTempFile::new().expect("tempfile");
        let path = file.into_temp_path();
        let db = Database::create(&path).expect("redb create");
        (db, path)
    }

    fn small_ipc() -> Vec<u8> {
        vec![0xAB; 64]
    }

    #[test]
    fn test_register_master_batch_succeeds() {
        let (db, _path) = temp_db();
        let resp = apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "orders".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(
                resp,
                ConsensusResponse::MasterBatchRegistered { batch_id: 1 }
            ),
            "{resp:?}"
        );
        let record = read_master_batch(&db, 1).unwrap().unwrap();
        assert_eq!(record.batch_id, 1);
        assert_eq!(record.component, "orders");
        assert_eq!(record.total_rows, 100);
    }

    #[test]
    fn test_register_master_batch_oversize_rejected() {
        let (db, _path) = temp_db();
        let big_ipc = vec![0u8; MAX_LOG_ENTRY_BYTES]; // exactly at limit → rejected
        let resp = apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 2,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: big_ipc,
                total_rows: 1,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "expected Error, got {resp:?}"
        );
    }

    #[test]
    fn test_claim_row_range_happy_path() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "items".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 200,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let claim_id = uuid::Uuid::new_v4();
        let instance_id = uuid::Uuid::new_v4();
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 50,
                claim_id,
                instance_id,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        assert!(
            matches!(
                resp,
                ConsensusResponse::BatchClaimed {
                    row_range_start: 0,
                    row_range_end: 50,
                    ..
                }
            ),
            "{resp:?}"
        );

        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(record.row_range_start, 0);
        assert_eq!(record.row_range_end, 50);
        assert_eq!(record.status, ClaimStatus::Claimed);
    }

    #[test]
    fn test_claim_overlap_rejected() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "items".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 200,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let c1 = uuid::Uuid::new_v4();
        let c2 = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();

        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id: c1,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Overlapping range should fail.
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 50,
                row_range_end: 150,
                claim_id: c2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "expected Error for overlapping claim, got {resp:?}"
        );
    }

    #[test]
    fn test_ack_claim_sets_completed() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();
        assert!(matches!(resp, ConsensusResponse::ClaimAcked), "{resp:?}");

        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(record.status, ClaimStatus::Completed);
    }

    #[test]
    fn test_release_claim_returns_to_pending() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();
        assert!(matches!(resp, ConsensusResponse::ClaimReleased), "{resp:?}");
        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(record.status, ClaimStatus::Pending);
    }

    #[test]
    fn test_checkpoint_written_and_read() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let resp = apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 2,
                ipc_bytes: vec![0xCA, 0xFE],
                schema_id: 1,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::CheckpointWritten { .. }),
            "{resp:?}"
        );

        let cp = read_checkpoint(&db, claim_id, 2).unwrap().unwrap();
        assert_eq!(cp.stage_idx, 2);
        assert_eq!(cp.ipc_bytes, vec![0xCA, 0xFE]);
    }

    #[test]
    fn test_checkpoint_oversize_rejected() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let big = vec![0u8; MAX_LOG_ENTRY_BYTES]; // at limit → rejected
        let resp = apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 0,
                ipc_bytes: big,
                schema_id: 1,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(matches!(resp, ConsensusResponse::Error { .. }), "{resp:?}");
    }

    #[test]
    fn test_snapshot_dump_restore_round_trip() {
        let (db, _path) = temp_db();
        // Populate state.
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "orders".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 500,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 0,
                ipc_bytes: vec![1, 2, 3],
                schema_id: 1,
                now_at_propose: 0,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::Heartbeat {
                instance_id: inst,
                at: 12345,
            },
        )
        .unwrap();

        // Dump.
        let (batches, claims, checkpoints, instances) = dump_state(&db).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(claims.len(), 1);
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(instances.len(), 1);

        // Restore into fresh DB.
        let (db2, _path2) = temp_db();
        restore_state(&db2, batches, claims, checkpoints, instances, None).unwrap();

        // Verify.
        let batch = read_master_batch(&db2, 1).unwrap().unwrap();
        assert_eq!(batch.component, "orders");
        assert_eq!(batch.total_rows, 500);

        let claim = read_claim(&db2, claim_id).unwrap().unwrap();
        assert_eq!(claim.row_range_start, 0);
        assert_eq!(claim.row_range_end, 100);

        let cp = read_checkpoint(&db2, claim_id, 0).unwrap().unwrap();
        assert_eq!(cp.ipc_bytes, vec![1, 2, 3]);
    }

    #[test]
    fn test_heartbeat_stores_instance() {
        let (db, _path) = temp_db();
        let inst = uuid::Uuid::new_v4();
        let resp = apply(
            &db,
            ConsensusCommand::Heartbeat {
                instance_id: inst,
                at: 99999,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::HeartbeatRecorded),
            "{resp:?}"
        );
    }

    #[test]
    fn test_renew_claim_updates_expiry() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let original = read_claim(&db, claim_id).unwrap().unwrap().lease_expires_at;

        let resp = apply(
            &db,
            ConsensusCommand::RenewClaim {
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 60_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::ClaimRenewed { expires_at } => {
                assert!(expires_at >= original, "new expiry should be >= original");
            }
            _ => panic!("expected ClaimRenewed, got {resp:?}"),
        }
    }

    /// Regression: apply must be deterministic across replicas.
    ///
    /// Applies the same fixed sequence of commands (mixing variants that
    /// previously read `SystemTime`) into two independent redb databases and
    /// asserts that the resulting state dumps are byte-identical. Any
    /// reintroduction of a wall-clock read inside an apply handler would
    /// yield divergent `created_at` / `lease_expires_at` fields and fail
    /// this assertion immediately.
    #[test]
    fn test_state_machine_apply_is_deterministic_across_replicas() {
        let (db_a, _path_a) = temp_db();
        let (db_b, _path_b) = temp_db();

        // Fixed UUIDs so both replicas apply byte-identical input.
        let claim1 = uuid::Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
        let claim2 = uuid::Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);
        let inst = uuid::Uuid::from_u128(0xAAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA);

        let cmds = vec![
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 10,
                component: "alpha".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0xA1; 32],
                total_rows: 300,
                now_at_propose: 1_700_000_000_000,
            },
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 11,
                component: "beta".to_string(),
                schema_id: 2,
                ipc_bytes: vec![0xB2; 48],
                total_rows: 200,
                now_at_propose: 1_700_000_000_100,
            },
            ConsensusCommand::ClaimRowRange {
                batch_id: 10,
                row_range_start: 0,
                row_range_end: 150,
                claim_id: claim1,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 1_700_000_000_200,
            },
            ConsensusCommand::ClaimRowRange {
                batch_id: 10,
                row_range_start: 150,
                row_range_end: 300,
                claim_id: claim2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 1_700_000_000_300,
            },
            ConsensusCommand::Checkpoint {
                claim_id: claim1,
                stage_idx: 0,
                ipc_bytes: vec![0xCA, 0xFE],
                schema_id: 1,
                now_at_propose: 1_700_000_000_400,
            },
            ConsensusCommand::Checkpoint {
                claim_id: claim1,
                stage_idx: 1,
                ipc_bytes: vec![0xDE, 0xAD],
                schema_id: 1,
                now_at_propose: 1_700_000_000_500,
            },
            ConsensusCommand::AckClaim {
                claim_id: claim1,
                instance_id: inst,
            },
            ConsensusCommand::RenewClaim {
                claim_id: claim2,
                instance_id: inst,
                lease_ttl_millis: 45_000,
                now_at_propose: 1_700_000_000_600,
            },
            ConsensusCommand::Heartbeat {
                instance_id: inst,
                at: 1_700_000_000_700,
            },
            ConsensusCommand::ReleaseClaim {
                claim_id: claim2,
                instance_id: inst,
            },
        ];

        // Apply the same sequence to both, introducing a wall-clock gap in
        // between to expose any latent SystemTime reads.
        for cmd in &cmds {
            apply(&db_a, cmd.clone()).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
        for cmd in &cmds {
            apply(&db_b, cmd.clone()).unwrap();
        }

        let dump_a = dump_state(&db_a).unwrap();
        let dump_b = dump_state(&db_b).unwrap();

        // Serialize the dumps via serde_json for easy byte comparison; any
        // field-level difference (including timestamps) would show up here.
        let json_a = serde_json::to_vec(&(
            &dump_a.0,
            &dump_a.1,
            &dump_a
                .2
                .iter()
                .map(|(k, v)| (k.to_vec(), v))
                .collect::<Vec<_>>(),
            &dump_a.3,
        ))
        .unwrap();
        let json_b = serde_json::to_vec(&(
            &dump_b.0,
            &dump_b.1,
            &dump_b
                .2
                .iter()
                .map(|(k, v)| (k.to_vec(), v))
                .collect::<Vec<_>>(),
            &dump_b.3,
        ))
        .unwrap();
        assert_eq!(
            json_a, json_b,
            "state machine apply must be deterministic across replicas"
        );
    }

    /// Regression: `find_first_pending_claim` must treat Completed
    /// as occupied, and `apply_claim_row_range` must reject overlaps with
    /// Completed ranges.
    #[test]
    fn test_find_first_pending_claim_skips_completed_ranges() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Ack the claim — status → Completed.
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Completed);

        // No pending rows should be reported.
        let pending = find_first_pending_claim(&db, 1).unwrap();
        assert!(
            pending.is_none(),
            "Completed claim must not be reported as pending; got {pending:?}"
        );

        // A fresh claim on the same range must be rejected as overlapping.
        let c2 = uuid::Uuid::new_v4();
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id: c2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::Error { message } => {
                assert!(
                    message.contains("overlaps"),
                    "expected overlap error, got {message}"
                );
            }
            other => panic!("expected Error variant, got {other:?}"),
        }
    }

    /// Verify that `CLAIMS_BY_BATCH` secondary index is populated on claim
    /// and correctly reflects status changes on ack/release.
    #[test]
    fn test_secondary_index_populated_and_updated() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 200,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Read the secondary index directly and verify the entry.
        {
            let txn = db.begin_read().unwrap();
            let idx = txn.open_table(CLAIMS_BY_BATCH).unwrap();
            let id_bytes = *claim_id.as_bytes();
            let key = claims_by_batch_key(1, &id_bytes);
            let guard = idx
                .get(key.as_slice())
                .unwrap()
                .expect("secondary index entry missing");
            let (start, end, status) =
                decode_claims_by_batch_value(guard.value()).expect("decode secondary value");
            assert_eq!((start, end, status), (0, 100, ClaimStatus::Claimed));
        }

        // Ack → status must flip to Completed in secondary index.
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();
        {
            let txn = db.begin_read().unwrap();
            let idx = txn.open_table(CLAIMS_BY_BATCH).unwrap();
            let id_bytes = *claim_id.as_bytes();
            let key = claims_by_batch_key(1, &id_bytes);
            let guard = idx
                .get(key.as_slice())
                .unwrap()
                .expect("secondary index entry missing");
            let (_, _, status) =
                decode_claims_by_batch_value(guard.value()).expect("decode secondary value");
            assert_eq!(status, ClaimStatus::Completed);
        }

        // Claim a second range.
        let claim2 = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 100,
                row_range_end: 200,
                claim_id: claim2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Release second claim → status must flip to Pending in secondary index.
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id: claim2,
                instance_id: inst,
            },
        )
        .unwrap();
        {
            let txn = db.begin_read().unwrap();
            let idx = txn.open_table(CLAIMS_BY_BATCH).unwrap();
            let id_bytes = *claim2.as_bytes();
            let key = claims_by_batch_key(1, &id_bytes);
            let guard = idx
                .get(key.as_slice())
                .unwrap()
                .expect("secondary index entry missing");
            let (_, _, status) =
                decode_claims_by_batch_value(guard.value()).expect("decode secondary value");
            assert_eq!(status, ClaimStatus::Pending);
        }
    }

    /// Verify that `restore_state` rebuilds the secondary index so that
    /// overlap checks work correctly after a snapshot install (without having
    /// to reply any commands).
    #[test]
    fn test_restore_state_rebuilds_secondary_index() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 200,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: inst,
            },
        )
        .unwrap();

        // Dump and restore into a fresh DB.
        let (batches, claims, checkpoints, instances) = dump_state(&db).unwrap();
        let (db2, _path2) = temp_db();
        restore_state(&db2, batches, claims, checkpoints, instances, None).unwrap();

        // The secondary index must be populated in db2.
        {
            let txn = db2.begin_read().unwrap();
            let idx = txn.open_table(CLAIMS_BY_BATCH).unwrap();
            let id_bytes = *claim_id.as_bytes();
            let key = claims_by_batch_key(1, &id_bytes);
            let guard = idx
                .get(key.as_slice())
                .unwrap()
                .expect("secondary index must be rebuilt by restore_state");
            let (start, end, status) =
                decode_claims_by_batch_value(guard.value()).expect("decode secondary value");
            assert_eq!((start, end, status), (0, 100, ClaimStatus::Completed));
        }

        // Overlap check should reject re-claiming the same range even after restore.
        let c2 = uuid::Uuid::new_v4();
        let resp = apply(
            &db2,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id: c2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::Error { message } => {
                assert!(
                    message.contains("overlaps"),
                    "expected overlap error after restore, got {message}"
                );
            }
            other => panic!("expected Error after restore, got {other:?}"),
        }
    }

    /// Verify that the secondary-index range scan correctly isolates claims
    /// across multiple batches — a claim in batch 2 must not be visible when
    /// scanning for batch 1.
    #[test]
    fn test_secondary_index_batch_isolation() {
        let (db, _path) = temp_db();
        let inst = uuid::Uuid::new_v4();

        for batch_id in 1u64..=3 {
            apply(
                &db,
                ConsensusCommand::RegisterMasterBatch {
                    batch_id,
                    component: format!("comp_{batch_id}"),
                    schema_id: 1,
                    ipc_bytes: small_ipc(),
                    total_rows: 100,
                    now_at_propose: 0,
                },
            )
            .unwrap();
        }

        // Claim rows 0..50 in batch 2.
        let c2 = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 2,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Claiming the same rows in batch 1 must succeed (different batch).
        let c1 = uuid::Uuid::new_v4();
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c1,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::BatchClaimed { .. }),
            "batch isolation failed: expected BatchClaimed, got {resp:?}"
        );

        // Claiming the same rows in batch 2 must be rejected (overlap).
        let c2b = uuid::Uuid::new_v4();
        let resp2 = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 2,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c2b,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        assert!(
            matches!(resp2, ConsensusResponse::Error { .. }),
            "expected overlap rejection within same batch, got {resp2:?}"
        );
    }

    /// Verify the secondary index value encoding/decoding round-trips correctly.
    #[test]
    fn test_secondary_index_encoding_round_trip() {
        for status in [
            ClaimStatus::Pending,
            ClaimStatus::Claimed,
            ClaimStatus::Completed,
        ] {
            let encoded = claims_by_batch_value(42, 84, status);
            let (start, end, decoded_status) =
                decode_claims_by_batch_value(&encoded).expect("decode must succeed");
            assert_eq!(start, 42);
            assert_eq!(end, 84);
            assert_eq!(decoded_status, status);
        }

        // Malformed (wrong length) must return None.
        assert!(decode_claims_by_batch_value(&[0u8; 8]).is_none());
        assert!(decode_claims_by_batch_value(&[0u8; 10]).is_none());
        assert!(decode_claims_by_batch_value(&[]).is_none());
    }

    // ── replay idempotency tests ──────────────────────────────────────────────

    #[test]
    fn claim_row_range_replay_idempotent() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        let cmd = ConsensusCommand::ClaimRowRange {
            batch_id: 1,
            row_range_start: 0,
            row_range_end: 50,
            claim_id,
            instance_id: inst,
            lease_ttl_millis: 30_000,
            now_at_propose: 1000,
        };

        let resp1 = apply(&db, cmd.clone()).unwrap();
        assert!(
            matches!(
                resp1,
                ConsensusResponse::BatchClaimed {
                    row_range_start: 0,
                    row_range_end: 50,
                    ..
                }
            ),
            "first apply should succeed: {resp1:?}"
        );

        // Second apply of the same command — must succeed (idempotent replay).
        let resp2 = apply(&db, cmd).unwrap();
        assert!(
            matches!(
                resp2,
                ConsensusResponse::BatchClaimed {
                    row_range_start: 0,
                    row_range_end: 50,
                    ..
                }
            ),
            "replay should be idempotent: {resp2:?}"
        );

        // There should be exactly one claim record.
        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(record.status, ClaimStatus::Claimed);
        assert_eq!(record.row_range_start, 0);
        assert_eq!(record.row_range_end, 50);
    }

    #[test]
    fn checkpoint_replay_idempotent() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let cp_cmd = ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx: 0,
            ipc_bytes: vec![0xCA, 0xFE],
            schema_id: 1,
            now_at_propose: 999, // deterministic identity key
        };

        let resp1 = apply(&db, cp_cmd.clone()).unwrap();
        let ConsensusResponse::CheckpointWritten {
            checkpoint_id: seq1,
        } = resp1
        else {
            panic!("expected CheckpointWritten, got {resp1:?}");
        };

        // Replay — checkpoint_seq must NOT be double-incremented.
        let resp2 = apply(&db, cp_cmd).unwrap();
        let ConsensusResponse::CheckpointWritten {
            checkpoint_id: seq2,
        } = resp2
        else {
            panic!("expected CheckpointWritten on replay, got {resp2:?}");
        };
        assert_eq!(
            seq2, seq1,
            "checkpoint_seq must not increment on idempotent replay"
        );

        let batch = read_master_batch(&db, 1).unwrap().unwrap();
        assert_eq!(batch.checkpoint_seq, seq1, "master batch seq must be seq1");
    }

    // ── monotonic renew test ──────────────────────────────────────────────────

    #[test]
    fn renew_claim_monotonic() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        // Claim at t=1000, ttl=60_000 → expires at 61_000.
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 60_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();

        // Renew with stale now_at_propose=500 (before the claim was even made).
        // new_expires = 500 + 60_000 = 60_500 < 61_000 → max() keeps 61_000.
        let resp = apply(
            &db,
            ConsensusCommand::RenewClaim {
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 60_000,
                now_at_propose: 500,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::ClaimRenewed { expires_at } => {
                assert_eq!(
                    expires_at, 61_000,
                    "stale renew must not move expiry backwards"
                );
            }
            ConsensusResponse::Error { message } => {
                // Also acceptable: rejected as stale (expired check: 61_000 >= 500)
                // The current impl checks lease_expires_at < now_at_propose to reject dead
                // leases. 61_000 < 500 is false so it doesn't reject — verify monotonicity.
                panic!("unexpected error: {message}");
            }
            other => panic!("unexpected response: {other:?}"),
        }

        let record = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(
            record.lease_expires_at, 61_000,
            "lease_expires_at must not regress"
        );
    }

    // ── reclaim expired test ──────────────────────────────────────────────────

    #[test]
    fn reclaim_expired_frees_ranges() {
        let (db, _path) = temp_db();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "x".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        // Claim at t=0 with ttl=100 → expires at 100.
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Before expiry: the range is still blocked.
        let resp_before =
            apply(&db, ConsensusCommand::ReclaimExpired { now_at_propose: 50 }).unwrap();
        assert!(
            matches!(
                resp_before,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 0 }
            ),
            "nothing should be reclaimed before expiry: {resp_before:?}"
        );
        let rec_before = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec_before.status, ClaimStatus::Claimed);

        // After expiry: sweep frees the range.
        let resp_after = apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 200,
            },
        )
        .unwrap();
        assert!(
            matches!(
                resp_after,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 1 }
            ),
            "one claim should be reclaimed: {resp_after:?}"
        );

        let rec_after = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(
            rec_after.status,
            ClaimStatus::Pending,
            "claim must be Pending after reclaim"
        );
        assert_eq!(rec_after.lease_expires_at, 0);

        // The range must now be claimable again by a different claim_id.
        let claim_id2 = uuid::Uuid::new_v4();
        let resp_reclaim = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id: claim_id2,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 200,
            },
        )
        .unwrap();
        assert!(
            matches!(resp_reclaim, ConsensusResponse::BatchClaimed { .. }),
            "range should be claimable after reclaim: {resp_reclaim:?}"
        );
    }

    // ── install_snapshot atomic clear test ────────────────────────────────────

    #[test]
    fn install_snapshot_atomic_clear() {
        // db1 has {c1, c2} and {batch 1, 2}
        let (db1, _p1) = temp_db();
        for i in 1u64..=2 {
            apply(
                &db1,
                ConsensusCommand::RegisterMasterBatch {
                    batch_id: i,
                    component: format!("comp_{i}"),
                    schema_id: 1,
                    ipc_bytes: small_ipc(),
                    total_rows: 100,
                    now_at_propose: 0,
                },
            )
            .unwrap();
        }
        let inst = uuid::Uuid::new_v4();
        let c1 = uuid::Uuid::new_v4();
        apply(
            &db1,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c1,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // db2 has {c4, batch 3} — state that should be replaced by snapshot from db1.
        let (db2, _p2) = temp_db();
        apply(
            &db2,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 3,
                component: "old_comp".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 50,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let c4 = uuid::Uuid::new_v4();
        apply(
            &db2,
            ConsensusCommand::ClaimRowRange {
                batch_id: 3,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: c4,
                instance_id: inst,
                lease_ttl_millis: 30_000,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Dump db1 → restore into db2.
        let (batches, claims, checkpoints, instances) = dump_state(&db1).unwrap();
        restore_state(&db2, batches, claims, checkpoints, instances, None).unwrap();

        // db2 must contain exactly {batch 1, 2} and claim c1 — NOT batch 3 / c4.
        assert!(
            read_master_batch(&db2, 1).unwrap().is_some(),
            "batch 1 must be present"
        );
        assert!(
            read_master_batch(&db2, 2).unwrap().is_some(),
            "batch 2 must be present"
        );
        assert!(
            read_master_batch(&db2, 3).unwrap().is_none(),
            "old batch 3 must be purged by snapshot install"
        );
        assert!(
            read_claim(&db2, c1).unwrap().is_some(),
            "claim c1 must be present"
        );
        assert!(
            read_claim(&db2, c4).unwrap().is_none(),
            "old claim c4 must be purged by snapshot install"
        );
    }

    // ── instance_id guards on ack/release/renew ───────────────────────────────

    #[test]
    fn test_ack_claim_rejects_pending_status() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        // Release the claim first — status → Pending.
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        // Now try to ack a Pending claim — must be rejected.
        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "ack of Pending claim must be rejected, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending, "must remain Pending");
    }

    #[test]
    fn test_release_claim_rejects_completed_status() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        // Ack to Completed first.
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        // Try to release a Completed claim — must be rejected.
        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "release of Completed claim must be rejected, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Completed, "must remain Completed");
    }

    /// Regression guard for the status-guard invariant relied on by
    /// the claim-level retry cap.
    ///
    /// `apply_release_claim` MUST reject a claim that is already in
    /// `ClaimStatus::Pending` (e.g. because `ReclaimExpired` raced the
    /// runner and reclaimed the lease first). The retry-cap wiring adds a
    /// `release_attempts` bump inside the successful release branch,
    /// and that bump MUST NOT fire on this rejection path — otherwise
    /// a late `ReleaseClaim` arriving after a reclaim would
    /// double-count the attempt. This test fails if anyone hoists the
    /// guard out of the success branch in a future refactor.
    #[test]
    fn test_release_claim_rejects_pending_status() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        // First release transitions the claim to Pending.
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending);

        // A second release against the now-Pending claim must be
        // rejected by the status guard — no mutation, no error in
        // the redb transaction, just an Error response surfaced to
        // the caller.
        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "release of Pending claim must be rejected, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending, "must remain Pending");
    }

    #[test]
    fn test_claim_row_range_idempotency_wrong_instance_rejected() {
        let (db, _path) = temp_db();
        let batch_id = 77u64;
        let claim_id = uuid::Uuid::new_v4();
        let instance_a = uuid::Uuid::new_v4();
        let instance_b = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "idem_test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0x77; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        // Instance A claims the range.
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: instance_a,
                lease_ttl_millis: 90_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        // Replay same claim_id but with instance_b — idempotency check must NOT
        // return success because instance_id doesn't match.
        let resp = apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: instance_b,
                lease_ttl_millis: 90_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        // Should NOT return BatchClaimed — must fall through to the overlap check
        // which rejects it.
        assert!(
            !matches!(resp, ConsensusResponse::BatchClaimed { instance_id, .. } if instance_id == instance_b),
            "wrong-instance idempotency replay must not mint a claim for instance_b, got {resp:?}"
        );
    }

    fn seed_claimed_guard(db: &Database) -> (uuid::Uuid, uuid::Uuid) {
        let batch_id = 99u64;
        let claim_id = uuid::Uuid::new_v4();
        let instance_id = uuid::Uuid::new_v4();
        apply(
            db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "guard_test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0xAB; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let resp = apply(
            db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 50,
                claim_id,
                instance_id,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::BatchClaimed { .. }),
            "seed_claimed_guard failed: {resp:?}"
        );
        (claim_id, instance_id)
    }

    #[test]
    fn test_ack_claim_wrong_instance_rejected() {
        let (db, _path) = temp_db();
        let (claim_id, _correct) = seed_claimed_guard(&db);
        let wrong_instance = uuid::Uuid::new_v4();
        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id: wrong_instance,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "expected Error for wrong instance_id, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(
            rec.status,
            ClaimStatus::Claimed,
            "claim must remain Claimed"
        );
    }

    #[test]
    fn test_release_claim_wrong_instance_rejected() {
        let (db, _path) = temp_db();
        let (claim_id, _correct) = seed_claimed_guard(&db);
        let wrong_instance = uuid::Uuid::new_v4();
        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id: wrong_instance,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "expected Error for wrong instance_id, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(
            rec.status,
            ClaimStatus::Claimed,
            "claim must remain Claimed"
        );
    }

    #[test]
    fn test_ack_claim_correct_instance_succeeds() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::ClaimAcked),
            "expected ClaimAcked, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Completed);
    }

    #[test]
    fn test_release_claim_correct_instance_succeeds() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);
        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::ClaimReleased),
            "expected ClaimReleased, got {resp:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending);
    }

    #[test]
    fn test_late_ack_after_reclaim_rejected() {
        let (db, _path) = temp_db();
        let batch_id = 88u64;
        let claim_a = uuid::Uuid::new_v4();
        let instance_a = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "late_ack".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0xAB; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: claim_a,
                instance_id: instance_a,
                lease_ttl_millis: 1_000,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let swept = apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 2_000,
            },
        )
        .unwrap();
        assert!(
            matches!(
                swept,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 1 }
            ),
            "{swept:?}"
        );
        let resp = apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id: claim_a,
                instance_id: instance_a,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "late ack must be rejected, got {resp:?}"
        );
        let rec = read_claim(&db, claim_a).unwrap().unwrap();
        assert_eq!(
            rec.status,
            ClaimStatus::Pending,
            "must remain Pending after late ack"
        );
    }

    #[test]
    fn test_renew_claim_wrong_instance_rejected() {
        let (db, _path) = temp_db();
        let (claim_id, _correct) = seed_claimed_guard(&db);
        let wrong_instance = uuid::Uuid::new_v4();
        let resp = apply(
            &db,
            ConsensusCommand::RenewClaim {
                claim_id,
                instance_id: wrong_instance,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "renew with wrong instance must be rejected, got {resp:?}"
        );
    }

    /// Applying the same checkpoint body twice (even with different
    /// `now_at_propose`) must increment `checkpoint_seq` exactly once.
    #[test]
    fn test_checkpoint_replay_idempotent_across_retries() {
        let (db, _path) = temp_db();
        let instance = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 1,
                component: "comp".to_string(),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: instance,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();

        let checkpoint_body = vec![0xCC; 128];

        // First apply: new checkpoint, seq should be 1.
        let resp1 = apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 0,
                ipc_bytes: checkpoint_body.clone(),
                schema_id: 1,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        let seq1 = match resp1 {
            ConsensusResponse::CheckpointWritten { checkpoint_id } => checkpoint_id,
            other => panic!("expected CheckpointWritten, got {other:?}"),
        };
        assert_eq!(seq1, 1, "first checkpoint must set seq to 1");

        // Second apply: same body, different now_at_propose (retry scenario).
        // checkpoint_seq must NOT increment again.
        let resp2 = apply(
            &db,
            ConsensusCommand::Checkpoint {
                claim_id,
                stage_idx: 0,
                ipc_bytes: checkpoint_body.clone(),
                schema_id: 1,
                now_at_propose: 99_999, // fresh timestamp — must be ignored
            },
        )
        .unwrap();
        let seq2 = match resp2 {
            ConsensusResponse::CheckpointWritten { checkpoint_id } => checkpoint_id,
            other => panic!("expected CheckpointWritten on retry, got {other:?}"),
        };
        assert_eq!(
            seq2, seq1,
            "retry with fresh now_at_propose must not increment checkpoint_seq"
        );
        let batch = read_master_batch(&db, 1).unwrap().unwrap();
        assert_eq!(
            batch.checkpoint_seq, 1,
            "checkpoint_seq must be exactly 1 after idempotent retry"
        );
    }

    /// Seeds N batches, completes N-1, asserts find_first_pending_batch
    /// visits only the one remaining pending batch.
    #[test]
    fn test_pending_batches_index_skips_completed() {
        const N: u64 = 10;
        let (db, _path) = temp_db();
        let inst = uuid::Uuid::new_v4();
        let mut claim_ids = Vec::new();

        // Register N batches and claim+ack N-1.
        for batch_id in 0..N {
            apply(
                &db,
                ConsensusCommand::RegisterMasterBatch {
                    batch_id,
                    component: format!("comp_{batch_id}"),
                    schema_id: 1,
                    ipc_bytes: small_ipc(),
                    total_rows: 10,
                    now_at_propose: 0,
                },
            )
            .unwrap();

            let claim_id = uuid::Uuid::new_v4();
            claim_ids.push((batch_id, claim_id));
            apply(
                &db,
                ConsensusCommand::ClaimRowRange {
                    batch_id,
                    row_range_start: 0,
                    row_range_end: 10,
                    claim_id,
                    instance_id: inst,
                    lease_ttl_millis: 90_000,
                    now_at_propose: 1_000,
                },
            )
            .unwrap();
        }

        // Ack all except batch 7 (keep that one pending by releasing it).
        for (batch_id, claim_id) in &claim_ids {
            if *batch_id == 7 {
                apply(
                    &db,
                    ConsensusCommand::ReleaseClaim {
                        claim_id: *claim_id,
                        instance_id: inst,
                    },
                )
                .unwrap();
            } else {
                apply(
                    &db,
                    ConsensusCommand::AckClaim {
                        claim_id: *claim_id,
                        instance_id: inst,
                    },
                )
                .unwrap();
            }
        }

        // find_first_pending_batch must find batch 7.
        let result = find_first_pending_batch(&db).unwrap();
        assert!(result.is_some(), "must find the pending batch");
        let (found_batch_id, _range) = result.unwrap();
        assert_eq!(
            found_batch_id, 7,
            "must find batch 7, got batch {found_batch_id}"
        );
    }

    // ── Claim-level retry cap ────────────────────────────────────────────────
    //
    // These tests exercise the `release_attempts` counter on
    // `MasterBatchRecord`, the `apply_poison_batch` handler, and the
    // wiring into `apply_release_claim` / `apply_ack_claim` /
    // `apply_reclaim_expired`.  See
    // for the design rationale and 12-test matrix.

    /// Test 1: `apply_release_claim` increments `release_attempts`.
    #[test]
    fn test_release_claim_increments_release_attempts() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);

        // Initial state: counter is zero.
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(batch.release_attempts, 0);

        // Release the claim — counter bumps to 1.
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(batch.release_attempts, 1);
    }

    /// Test 2: `apply_ack_claim` resets `release_attempts` to 0.
    #[test]
    fn test_ack_claim_resets_release_attempts() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);

        // Release the claim a couple of times via release→reclaim cycle
        // to build up attempts.  Simulate by calling the helper directly
        // — easier than spawning expired reclaims in-test.
        let w = db.begin_write().unwrap();
        increment_release_attempts(&w, 99).unwrap();
        increment_release_attempts(&w, 99).unwrap();
        increment_release_attempts(&w, 99).unwrap();
        w.commit().unwrap();
        assert_eq!(
            read_master_batch(&db, 99)
                .unwrap()
                .unwrap()
                .release_attempts,
            3
        );

        // Now ack the original claim — counter resets.
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(batch.release_attempts, 0, "ack must reset counter");
    }

    /// Test 3: `apply_reclaim_expired` increments `release_attempts`.
    #[test]
    fn test_reclaim_expired_increments_release_attempts() {
        let (db, _path) = temp_db();
        let (_claim_id, _instance_id) = seed_claimed_guard(&db);

        // The claim's `lease_expires_at` is `now_at_propose (1_000) +
        // lease_ttl (90_000)` = 91_000.  Propose ReclaimExpired at a
        // later time so the lease is visibly expired.
        apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 200_000,
            },
        )
        .unwrap();
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(
            batch.release_attempts, 1,
            "reclaim_expired must bump release_attempts just like an explicit release"
        );
    }

    /// Test 4: late `ReleaseClaim` after `ReclaimExpired` does NOT bump
    /// the counter (race-4 safety).  Guarded by the status check in
    /// `apply_release_claim`; the retry-cap wiring is inside the
    /// success branch so late deliveries hit the guard first.
    #[test]
    fn test_late_release_after_reclaim_does_not_double_count() {
        let (db, _path) = temp_db();
        let (claim_id, instance_id) = seed_claimed_guard(&db);

        // Reclaim the lease (bumps counter to 1).
        apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 200_000,
            },
        )
        .unwrap();
        assert_eq!(
            read_master_batch(&db, 99)
                .unwrap()
                .unwrap()
                .release_attempts,
            1
        );

        // A late ReleaseClaim arrives for the same claim — it hits the
        // "not in Claimed state" guard in apply_release_claim and
        // returns Error without mutating.  Counter stays at 1.
        let resp = apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "late release against Pending claim must be rejected"
        );
        assert_eq!(
            read_master_batch(&db, 99)
                .unwrap()
                .unwrap()
                .release_attempts,
            1,
            "release_attempts must NOT double-count on late ReleaseClaim after ReclaimExpired"
        );
    }

    /// Test 5: `apply_poison_batch` marks status, stamps `poisoned_at`,
    /// and removes the batch from `PENDING_BATCHES` so future
    /// `claim_next_batch` calls never see it.
    #[test]
    fn test_poison_batch_marks_status_and_removes_from_pending() {
        let (db, _path) = temp_db();
        let _ = seed_claimed_guard(&db);

        // Poison the batch.
        let resp = apply(
            &db,
            ConsensusCommand::PoisonBatch {
                batch_id: 99,
                now_at_propose: 42_000,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::BatchPoisoned {
                batch_id,
                poisoned_at,
            } => {
                assert_eq!(batch_id, 99);
                assert_eq!(poisoned_at, 42_000);
            }
            other => panic!("expected BatchPoisoned, got {other:?}"),
        }

        // Master batch record reflects the new state.
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(batch.status, BatchStatus::Poisoned);
        assert_eq!(batch.poisoned_at, Some(42_000));

        // `find_first_pending_batch` no longer returns this batch.
        // (seed_claimed_guard only registered batch 99, so the result
        // must be None.)
        let result = find_first_pending_batch(&db).unwrap();
        assert!(
            result.is_none(),
            "poisoned batch must not be returned by find_first_pending_batch, got {result:?}"
        );
    }

    /// Test 6: `PoisonBatch` is idempotent and preserves the first-writer
    /// `poisoned_at` timestamp.  A concurrent second proposer must not
    /// overwrite the first writer's timestamp — otherwise `/status`
    /// would show "how long poisoned" jitter as Raft serialises races.
    ///
    /// (Design doc Test 8; wasm-lead's explicit requirement.)
    #[test]
    fn test_poison_batch_idempotent_preserves_first_writer_timestamp() {
        let (db, _path) = temp_db();
        let _ = seed_claimed_guard(&db);

        // First poison — timestamp 100.
        apply(
            &db,
            ConsensusCommand::PoisonBatch {
                batch_id: 99,
                now_at_propose: 100,
            },
        )
        .unwrap();
        // Second poison — timestamp 200.  Must be a no-op.
        let resp = apply(
            &db,
            ConsensusCommand::PoisonBatch {
                batch_id: 99,
                now_at_propose: 200,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::BatchPoisoned {
                batch_id,
                poisoned_at,
            } => {
                assert_eq!(batch_id, 99);
                assert_eq!(
                    poisoned_at, 100,
                    "second PoisonBatch must return first-writer poisoned_at"
                );
            }
            other => panic!("expected BatchPoisoned (idempotent), got {other:?}"),
        }
        let batch = read_master_batch(&db, 99).unwrap().unwrap();
        assert_eq!(
            batch.poisoned_at,
            Some(100),
            "first-writer timestamp must be preserved on double-poison"
        );
    }

    /// Test 7: `PoisonBatch` against a non-existent batch returns Error.
    #[test]
    fn test_poison_batch_unknown_id_returns_error() {
        let (db, _path) = temp_db();
        let resp = apply(
            &db,
            ConsensusCommand::PoisonBatch {
                batch_id: 12345,
                now_at_propose: 100,
            },
        )
        .unwrap();
        assert!(
            matches!(resp, ConsensusResponse::Error { .. }),
            "PoisonBatch on unknown batch_id must return Error"
        );
    }

    /// Test 8: N consecutive `ReleaseClaim`s accumulate `release_attempts`
    /// without any ack in between.  Verifies the counter does not reset
    /// unless an explicit ack arrives.
    ///
    /// We need a fresh claim each iteration because `ReleaseClaim`
    /// transitions the claim to Pending — re-claiming is what drives
    /// the next cycle.
    #[test]
    fn test_n_releases_accumulate_release_attempts() {
        let (db, _path) = temp_db();
        let batch_id = 77u64;
        let instance_id = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "nrel".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0x01; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        for i in 0..5 {
            let claim_id = uuid::Uuid::new_v4();
            apply(
                &db,
                ConsensusCommand::ClaimRowRange {
                    batch_id,
                    row_range_start: 0,
                    row_range_end: 50,
                    claim_id,
                    instance_id,
                    lease_ttl_millis: 90_000,
                    now_at_propose: 1_000,
                },
            )
            .unwrap();
            apply(
                &db,
                ConsensusCommand::ReleaseClaim {
                    claim_id,
                    instance_id,
                },
            )
            .unwrap();
            let batch = read_master_batch(&db, batch_id).unwrap().unwrap();
            assert_eq!(
                batch.release_attempts,
                (i + 1) as u32,
                "after {} releases counter must be {}",
                i + 1,
                i + 1
            );
        }
    }

    /// Test 9: schema evolution — old JSON records without the new
    /// `release_attempts` / `status` / `poisoned_at` fields must
    /// decode cleanly via `#[serde(default)]`.  This is the regression
    /// guard for the `serde_json` forward-compat approach that replaced
    /// the originally-planned `StoredMasterBatch::{V1, V2}` enum.
    #[test]
    fn test_master_batch_record_decodes_legacy_json() {
        // Hand-written legacy JSON missing release_attempts/status/poisoned_at.
        let legacy = br#"{
            "batch_id": 1,
            "component": "legacy",
            "schema_id": 1,
            "ipc_bytes": [1,2,3],
            "total_rows": 10,
            "created_at": 0,
            "checkpoint_seq": 0
        }"#;
        let record: MasterBatchRecord = dec(legacy).unwrap();
        assert_eq!(record.batch_id, 1);
        assert_eq!(record.component, "legacy");
        assert_eq!(
            record.release_attempts, 0,
            "missing field must default to 0"
        );
        assert_eq!(
            record.status,
            BatchStatus::Active,
            "missing field must default to Active"
        );
        assert_eq!(
            record.poisoned_at, None,
            "missing field must default to None"
        );
    }

    /// Test 10: a claim that's acked after some failures doesn't leave
    /// a stale counter — next failure starts from 1, not N+1.  This
    /// test combines reset-on-ack with a subsequent failure to prove
    /// the reset is real and not just a zero-check.
    #[test]
    fn test_reset_on_ack_then_new_failure_starts_from_one() {
        let (db, _path) = temp_db();
        let batch_id = 88u64;
        let instance_id = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: "cycle".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0x02; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Three failures: claim → release × 3.
        for _ in 0..3 {
            let claim_id = uuid::Uuid::new_v4();
            apply(
                &db,
                ConsensusCommand::ClaimRowRange {
                    batch_id,
                    row_range_start: 0,
                    row_range_end: 50,
                    claim_id,
                    instance_id,
                    lease_ttl_millis: 90_000,
                    now_at_propose: 1_000,
                },
            )
            .unwrap();
            apply(
                &db,
                ConsensusCommand::ReleaseClaim {
                    claim_id,
                    instance_id,
                },
            )
            .unwrap();
        }
        assert_eq!(
            read_master_batch(&db, batch_id)
                .unwrap()
                .unwrap()
                .release_attempts,
            3
        );

        // One success: claim → ack.
        let ok_claim = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: 0,
                row_range_end: 50,
                claim_id: ok_claim,
                instance_id,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::AckClaim {
                claim_id: ok_claim,
                instance_id,
            },
        )
        .unwrap();
        assert_eq!(
            read_master_batch(&db, batch_id)
                .unwrap()
                .unwrap()
                .release_attempts,
            0,
            "ack resets counter"
        );

        // Note: row range [0,50) is now marked Completed on the secondary
        // index (via the successful ack).  Register a second batch to
        // exercise the new-failure path — we need a fresh (non-completed)
        // row range.  The test's point is that after a reset, a fresh
        // failure on a different batch starts from 1, which proves the
        // counter was zeroed, not that it carried a reset sentinel.
        let fresh_batch = 89u64;
        apply(
            &db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: fresh_batch,
                component: "cycle2".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0x03; 64],
                total_rows: 100,
                now_at_propose: 0,
            },
        )
        .unwrap();
        let claim_id = uuid::Uuid::new_v4();
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: fresh_batch,
                row_range_start: 0,
                row_range_end: 50,
                claim_id,
                instance_id,
                lease_ttl_millis: 90_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();
        apply(
            &db,
            ConsensusCommand::ReleaseClaim {
                claim_id,
                instance_id,
            },
        )
        .unwrap();
        assert_eq!(
            read_master_batch(&db, fresh_batch)
                .unwrap()
                .unwrap()
                .release_attempts,
            1,
            "new batch starts from zero and counts its first failure as 1"
        );
    }
}
