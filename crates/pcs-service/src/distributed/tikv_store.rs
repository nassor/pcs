//! TiKV-backed shared store: partitions, checkpoints, and the persistent
//! state ledgers (configs, cursors, processor priors).
//!
//! All TiKV access goes through this module or [`TikvStateClient`]
//! (`crate::service::tikv_state`); nothing else speaks the raw protocol.
//! Keys are namespaced under a configurable prefix and laid out so byte
//! order = numeric order for scans: integer segments are fixed-width
//! uppercase hex (`hex_u64` = 16 chars, `hex_u32` = 8 chars, UUID = 32
//! chars).
//!
//! # Key layout (`{p}` = configured prefix)
//!
//! ```text
//! {p}/config/{name}                     raw KDL bytes
//! {p}/cursor/{workflow_id}/{node_id}/prior   processor state blob
//! {p}/cursor/{workflow_id}/{source_id}/meta  postcard(SourceCursorMeta)
//! {p}/batch/{batch_id:016X}             postcard(MasterBatchRecord)
//! {p}/rows/{batch_id:016X}/{start:08X}  postcard(RowRangeRecord)
//! {p}/claim/{claim_id_hex}              postcard(ClaimIndexRecord)
//! {p}/checkpoint/{claim_id_hex}/{stage_idx:08X}  postcard(CheckpointRecord)
//! {p}/meta/schema_id                    postcard(u32)
//! ```
//!
//! Values are postcard-encoded records (deterministic byte layout — no
//! HashMap). Mutations that must be atomic (claim transitions, renewals)
//! go through TiKV's `compare_and_swap`; everything else is a plain
//! put/get/delete.

use std::ops::Bound;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tikv_client::{BoundRange, Config, Key, KvPair, RawClient};
use uuid::Uuid;

use crate::PcsResult;
use crate::distributed::checkpoint::{Checkpoint, CheckpointStore};
use crate::distributed::partition::{BatchClaim, MAX_LOG_ENTRY_BYTES, PartitionSource};
use crate::error::PcsError;

/// Resolved TiKV connection options, converted from the config schema's
/// `store "tikv" { … }` block.
#[derive(Debug, Clone)]
pub struct TikvStoreConfig {
    /// PD endpoints, `host:port` strings.
    pub pd_endpoints: Vec<String>,
    /// Key prefix for every key this store writes.
    pub key_prefix: String,
    /// Per-operation timeout.
    pub timeout: Duration,
    /// Claim lease TTL in milliseconds.
    pub lease_ttl_millis: u64,
}

/// Largest `ipc_bytes` a TiKV-backed checkpoint accepts.
///
/// TiKV raw values cap around 6 MiB; 4 MiB leaves headroom for the postcard
/// record framing and the metadata keys written alongside a checkpoint.
pub const TIKV_MAX_CHECKPOINT_BYTES: usize = 4 * 1024 * 1024;

/// Rows per claimable row range; `register_master_batch` splits the batch
/// into `ceil(total_rows / TIKV_ROWS_PER_RANGE)` pending ranges so a large
/// batch can be processed by several instances in parallel.
pub const TIKV_ROWS_PER_RANGE: u32 = 512;

// ── Records ────────────────────────────────────────────────────────────────

/// The replicated master batch: one row-range source of work per batch id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MasterBatchRecord {
    /// Arrow component name for the batch's rows.
    pub component: String,
    /// Schema version of the IPC bytes.
    pub schema_id: u32,
    /// Arrow IPC bytes of the master `RecordBatch`.
    pub ipc_bytes: Vec<u8>,
    /// Total rows in the master batch; every row range lies within it.
    pub total_rows: u32,
    /// Unix milliseconds when the batch was registered.
    pub created_at_ms: u64,
    /// Poisoned batches are skipped by [`claim_next_batch`].
    ///
    /// [`claim_next_batch`]: PartitionSource::claim_next_batch
    pub poisoned: bool,
}

/// Status of one row range of a master batch.
///
/// Serialized as `u8` for compact postcard framing; the names are the
/// readable form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RowRangeStatus {
    /// Available to be claimed.
    Pending = 0,
    /// Claimed; the claim fields are populated.
    Claimed = 1,
    /// Processed; never reclaimed.
    Completed = 2,
}

/// One row range of a master batch and its claim state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowRangeRecord {
    /// Exclusive end of the half-open `[start, end)` range (`start` is the
    /// key segment).
    pub end: u32,
    /// [`RowRangeStatus`] as its raw byte.
    pub status: u8,
    /// Claim owner, zeroed while `status == Pending`.
    pub claim_id: [u8; 16],
    /// Instance that holds the claim, zeroed while pending.
    pub instance_id: [u8; 16],
    /// Lease expiry in unix milliseconds, zero while pending.
    pub lease_expires_at_ms: u64,
}

/// Index from a claim id back to its row range; lets claim-driven reads
/// (renew, ack, release, checkpoint) resolve the batch without a scan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimIndexRecord {
    /// Master batch the claim belongs to.
    pub batch_id: u64,
    /// Inclusive start of the claimed row range.
    pub start: u32,
}

/// A persisted per-stage pipeline checkpoint, TiKV flavor of
/// [`Checkpoint`](crate::distributed::Checkpoint).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointRecord {
    /// Master batch the checkpoint belongs to.
    pub batch_id: u64,
    /// Dataset stage index after which this checkpoint was taken.
    pub stage_idx: u32,
    /// Arrow IPC bytes of the intermediate state.
    pub ipc_bytes: Vec<u8>,
    /// Schema version of the IPC bytes.
    pub schema_id: u32,
    /// Unix milliseconds when the checkpoint was created.
    pub created_at_ms: u64,
}

// ── Key helpers ────────────────────────────────────────────────────────────

/// Fixed-width uppercase hex so byte order equals numeric order.
fn hex_u64(v: u64) -> String {
    format!("{v:016X}")
}

/// Fixed-width uppercase hex so byte order equals numeric order.
fn hex_u32(v: u32) -> String {
    format!("{v:08X}")
}

/// 32-char lowercase hex (UUID's canonical form sorts by v7 timestamp).
fn hex_uuid(u: Uuid) -> String {
    u.simple().to_string()
}

/// Parse the trailing hex segment of a key back into its integer.
fn parse_hex_tail(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()
        .and_then(|h| u64::from_str_radix(h, 16).ok())
}

/// Raw KDL bytes of a persisted config file.
pub fn config_key(prefix: &str, name: &str) -> String {
    format!("{prefix}/config/{name}")
}

/// Processor state blob key.
pub fn prior_key(prefix: &str, workflow_id: &str, node_id: &str) -> String {
    format!("{prefix}/cursor/{workflow_id}/{node_id}/prior")
}

/// Source cursor metadata key.
pub fn cursor_key(prefix: &str, workflow_id: &str, source_id: &str) -> String {
    format!("{prefix}/cursor/{workflow_id}/{source_id}/meta")
}

/// Master batch record key.
pub fn batch_key(prefix: &str, batch_id: u64) -> String {
    format!("{prefix}/batch/{}", hex_u64(batch_id))
}

/// Row-range scan prefix for one master batch.
pub fn rows_prefix(prefix: &str, batch_id: u64) -> String {
    format!("{prefix}/rows/{}", hex_u64(batch_id))
}

/// One row-range record key; `start` is the range's inclusive start.
pub fn rows_key(prefix: &str, batch_id: u64, start: u32) -> String {
    format!("{prefix}/rows/{}/{}", hex_u64(batch_id), hex_u32(start))
}

/// Claim index key.
pub fn claim_key(prefix: &str, claim_id: Uuid) -> String {
    format!("{prefix}/claim/{}", hex_uuid(claim_id))
}

/// Checkpoint key for one claim at one stage.
pub fn checkpoint_key(prefix: &str, claim_id: Uuid, stage_idx: u32) -> String {
    format!(
        "{prefix}/checkpoint/{}/{}",
        hex_uuid(claim_id),
        hex_u32(stage_idx)
    )
}

/// Schema-id metadata key, updated on every data-stage checkpoint.
pub fn schema_id_key(prefix: &str) -> String {
    format!("{prefix}/meta/schema_id")
}

/// Byte-ordered key immediately after `key`, for exclusive scan resumption.
fn key_after(key: &[u8]) -> Vec<u8> {
    let mut out = key.to_vec();
    out.push(0x00);
    out
}

/// Paged prefix scan: TiKV caps a scan at `limit` pairs, so loop until a
/// short page. Resume with an exclusive start at `last key + 0x00`.
async fn scan_prefix(client: &RawClient, prefix: &str) -> PcsResult<Vec<KvPair>> {
    let end = {
        let mut bytes = prefix.as_bytes().to_vec();
        *bytes.last_mut().expect("prefix is non-empty") += 1;
        Key::from(bytes)
    };
    let mut out: Vec<KvPair> = Vec::new();
    let mut start = Key::from(prefix.to_string());
    loop {
        let range = BoundRange::new(Bound::Included(start), Bound::Excluded(end.clone()));
        let page = client
            .scan(range, 1024)
            .await
            .map_err(|e| PcsError::store(format!("tikv scan {prefix:?}: {e}")))?;
        let page_len = page.len();
        let last = page.last().map(|p| p.0.clone());
        out.extend(page);
        if page_len < 1024 {
            break;
        }
        let last_bytes: Vec<u8> = last.expect("a full page has a last key").into();
        start = Key::from(key_after(&last_bytes));
    }
    Ok(out)
}

// ── Shared store ───────────────────────────────────────────────────────────

/// TiKV-backed [`PartitionSource`] + [`CheckpointStore`].
///
/// [`PartitionSource`]: crate::distributed::PartitionSource
/// [`CheckpointStore`]: crate::distributed::CheckpointStore
pub struct TikvSharedStore {
    pub(crate) client: RawClient,
    pub(crate) prefix: String,
    lease_ttl_millis: u64,
}

impl TikvSharedStore {
    /// Connect to PD and prepare an atomic (CAS-capable) raw client.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] when the PD endpoints are unreachable or
    /// the handshake fails.
    pub async fn connect(config: &TikvStoreConfig) -> PcsResult<Self> {
        let client = RawClient::new_with_config(
            config.pd_endpoints.clone(),
            Config::default().with_timeout(config.timeout),
        )
        .await
        .map_err(|e| PcsError::store(format!("tikv connect: {e}")))?
        .with_atomic_for_cas();
        Ok(Self {
            client,
            prefix: config.key_prefix.clone(),
            lease_ttl_millis: config.lease_ttl_millis,
        })
    }

    /// Register a master batch whose row ranges become claimable work.
    ///
    /// The batch is split into fixed-size pending row ranges
    /// ([`TIKV_ROWS_PER_RANGE`]) so several instances can claim parts of it
    /// in parallel. Rejects payloads at or above [`MAX_LOG_ENTRY_BYTES`] and
    /// refuses to overwrite an existing batch id.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when the batch id is already
    /// registered, [`PcsError::Store`] for transport/encode failures.
    pub async fn register_master_batch(
        &self,
        batch_id: u64,
        component: String,
        schema_id: u32,
        ipc_bytes: Vec<u8>,
        total_rows: u32,
    ) -> PcsResult<()> {
        if ipc_bytes.len() >= MAX_LOG_ENTRY_BYTES {
            return Err(PcsError::configuration(format!(
                "master batch {batch_id}: ipc payload {} bytes exceeds the {} byte limit",
                ipc_bytes.len(),
                MAX_LOG_ENTRY_BYTES
            )));
        }
        let key = Key::from(batch_key(&self.prefix, batch_id));
        if self
            .client
            .get(key.clone())
            .await
            .map_err(|e| PcsError::store(format!("tikv get master batch {batch_id}: {e}")))?
            .is_some()
        {
            return Err(PcsError::configuration(format!(
                "master batch {batch_id} already registered"
            )));
        }
        let record = MasterBatchRecord {
            component,
            schema_id,
            ipc_bytes,
            total_rows,
            created_at_ms: now_millis(),
            poisoned: false,
        };
        let bytes = postcard::to_allocvec(&record)
            .map_err(|e| PcsError::store(format!("tikv encode master batch: {e}")))?;
        self.client
            .put(key, bytes)
            .await
            .map_err(|e| PcsError::store(format!("tikv put master batch {batch_id}: {e}")))?;

        // Create the claimable row ranges. Re-running registration after a
        // partially-failed first attempt overwrites the same records with
        // identical content, so this is idempotent.
        let mut start = 0u32;
        while start < total_rows {
            let end = (start + TIKV_ROWS_PER_RANGE).min(total_rows);
            let row_record = RowRangeRecord {
                end,
                status: RowRangeStatus::Pending as u8,
                claim_id: [0u8; 16],
                instance_id: [0u8; 16],
                lease_expires_at_ms: 0,
            };
            let row_bytes = postcard::to_allocvec(&row_record)
                .map_err(|e| PcsError::store(format!("tikv encode row range: {e}")))?;
            self.client
                .put(
                    Key::from(rows_key(&self.prefix, batch_id, start)),
                    row_bytes,
                )
                .await
                .map_err(|e| {
                    PcsError::store(format!("tikv put row range {batch_id}/{start}: {e}"))
                })?;
            start = end;
        }
        Ok(())
    }

    /// Read the registered master batch at `batch_id`; `Ok(None)` when absent.
    ///
    /// The runner builds each partition dataset from its own world factory, so
    /// nothing on the claim path needs the payload. A caller that wants to
    /// hydrate rows from the batch itself — as
    /// `examples/distributed_fulfillment` does — reads it back here.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Store`] on a transport failure or a record that
    /// does not decode.
    pub async fn read_master_batch(&self, batch_id: u64) -> PcsResult<Option<MasterBatchRecord>> {
        let key = Key::from(batch_key(&self.prefix, batch_id));
        let Some(value) = self
            .client
            .get(key)
            .await
            .map_err(|e| PcsError::store(format!("tikv get master batch {batch_id}: {e}")))?
        else {
            return Ok(None);
        };
        let record = postcard::from_bytes(&value)
            .map_err(|e| PcsError::store(format!("tikv decode master batch: {e}")))?;
        Ok(Some(record))
    }

    /// Read the claim index record for `claim_id`; `Ok(None)` when absent.
    async fn read_claim_index(&self, claim_id: Uuid) -> PcsResult<Option<ClaimIndexRecord>> {
        let key = Key::from(claim_key(&self.prefix, claim_id));
        let Some(value) = self
            .client
            .get(key)
            .await
            .map_err(|e| PcsError::store(format!("tikv get claim index {claim_id}: {e}")))?
        else {
            return Ok(None);
        };
        let record = postcard::from_bytes(&value)
            .map_err(|e| PcsError::store(format!("tikv decode claim index: {e}")))?;
        Ok(Some(record))
    }

    /// Read the row-range record at `batch_id`/`start`.
    async fn read_row_range(&self, batch_id: u64, start: u32) -> PcsResult<Option<RowRangeRecord>> {
        let key = Key::from(rows_key(&self.prefix, batch_id, start));
        let Some(value) =
            self.client.get(key).await.map_err(|e| {
                PcsError::store(format!("tikv get row range {batch_id}/{start}: {e}"))
            })?
        else {
            return Ok(None);
        };
        let record = postcard::from_bytes(&value)
            .map_err(|e| PcsError::store(format!("tikv decode row range: {e}")))?;
        Ok(Some(record))
    }
}

#[async_trait]
impl PartitionSource for TikvSharedStore {
    async fn claim_next_batch(&self, instance_id: Uuid) -> PcsResult<Option<BatchClaim>> {
        let batches = scan_prefix(&self.client, &format!("{}/batch/", self.prefix)).await?;
        for batch_pair in batches {
            let batch_bytes: Vec<u8> = batch_pair.0.into();
            let batch_key_str = String::from_utf8_lossy(&batch_bytes);
            let Some(batch_id) = parse_hex_tail(&batch_key_str) else {
                continue;
            };
            let master: MasterBatchRecord = postcard::from_bytes(&batch_pair.1)
                .map_err(|e| PcsError::store(format!("tikv decode master batch: {e}")))?;
            if master.poisoned {
                continue;
            }

            let rows = scan_prefix(&self.client, &rows_prefix(&self.prefix, batch_id)).await?;
            for row_pair in rows {
                let row_key = row_pair.0;
                let row_value = row_pair.1;
                let row_bytes: Vec<u8> = row_key.clone().into();
                let row_key_str = String::from_utf8_lossy(&row_bytes);
                let Some(start) = parse_hex_tail(&row_key_str) else {
                    continue;
                };
                let start = start as u32;
                let pending: RowRangeRecord = postcard::from_bytes(&row_value)
                    .map_err(|e| PcsError::store(format!("tikv decode row range: {e}")))?;
                if pending.status != RowRangeStatus::Pending as u8 {
                    continue;
                }

                let claim_id = Uuid::now_v7();
                let lease_expires_at_ms = now_millis() + self.lease_ttl_millis;
                let claimed = RowRangeRecord {
                    end: pending.end,
                    status: RowRangeStatus::Claimed as u8,
                    claim_id: *claim_id.as_bytes(),
                    instance_id: *instance_id.as_bytes(),
                    lease_expires_at_ms,
                };
                let claimed_bytes = postcard::to_allocvec(&claimed)
                    .map_err(|e| PcsError::store(format!("tikv encode claimed range: {e}")))?;

                // CAS makes the claim atomic; a lost race just moves the scan
                // to the next pending range.
                let (_, swapped) = self
                    .client
                    .compare_and_swap(row_key, Some(row_value), claimed_bytes)
                    .await
                    .map_err(|e| PcsError::store(format!("tikv claim CAS: {e}")))?;
                if !swapped {
                    continue;
                }

                let index = ClaimIndexRecord { batch_id, start };
                let index_bytes = postcard::to_allocvec(&index)
                    .map_err(|e| PcsError::store(format!("tikv encode claim index: {e}")))?;
                self.client
                    .put(Key::from(claim_key(&self.prefix, claim_id)), index_bytes)
                    .await
                    .map_err(|e| {
                        PcsError::store(format!("tikv put claim index {claim_id}: {e}"))
                    })?;

                let master_bytes = self
                    .client
                    .get(Key::from(batch_key(&self.prefix, batch_id)))
                    .await
                    .map_err(|e| PcsError::store(format!("tikv get master batch: {e}")))?
                    .ok_or_else(|| {
                        PcsError::store(format!(
                            "tikv: master batch {batch_id} missing after claim"
                        ))
                    })?;
                let master: MasterBatchRecord = postcard::from_bytes(&master_bytes)
                    .map_err(|e| PcsError::store(format!("tikv decode master batch: {e}")))?;

                return Ok(Some(BatchClaim {
                    batch_id,
                    component: master.component,
                    row_range: start..pending.end,
                    schema_id: master.schema_id,
                    claim_id,
                    instance_id,
                    lease_expires_at: lease_expires_at_ms,
                    lease_ttl_millis: self.lease_ttl_millis,
                    claimed_at: Instant::now(),
                }));
            }
        }
        Ok(None)
    }

    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<u64> {
        let index = self.read_claim_index(claim_id).await?.ok_or_else(|| {
            PcsError::store(format!("tikv: claim {claim_id} not found; cannot renew"))
        })?;
        let Some(row) = self.read_row_range(index.batch_id, index.start).await? else {
            return Err(PcsError::store(format!(
                "tikv: row range for claim {claim_id} missing; cannot renew"
            )));
        };
        if row.status != RowRangeStatus::Claimed as u8
            || row.claim_id != *claim_id.as_bytes()
            || row.instance_id != *instance_id.as_bytes()
        {
            // The lease contract: a lost claim must stop the runner, not
            // silently continue processing under a stale lease.
            return Err(PcsError::store(format!(
                "tikv: claim {claim_id} is no longer held by instance {instance_id}"
            )));
        }
        // An expired lease is a lost claim: only `reclaim_expired` may free
        // it, and renewing must surface the loss instead of silently
        // extending a dead lease.
        if row.lease_expires_at_ms <= now_millis() {
            return Err(PcsError::store(format!(
                "tikv: claim {claim_id} lease expired at {}; reclaiming requires \
                 reclaim_expired",
                row.lease_expires_at_ms
            )));
        }

        let new_expiry = now_millis() + self.lease_ttl_millis;
        let mut renewed = row.clone();
        renewed.lease_expires_at_ms = new_expiry;
        let renewed_bytes = postcard::to_allocvec(&renewed)
            .map_err(|e| PcsError::store(format!("tikv encode renewed range: {e}")))?;
        let (_, swapped) = self
            .client
            .compare_and_swap(
                Key::from(rows_key(&self.prefix, index.batch_id, index.start)),
                Some(
                    postcard::to_allocvec(&row)
                        .map_err(|e| PcsError::store(format!("tikv encode row range: {e}")))?,
                ),
                renewed_bytes,
            )
            .await
            .map_err(|e| PcsError::store(format!("tikv renew CAS: {e}")))?;
        if !swapped {
            return Err(PcsError::store(format!(
                "tikv: renew lost a race for claim {claim_id}"
            )));
        }
        Ok(new_expiry)
    }

    async fn ack_claim(&self, claim_id: Uuid, _instance_id: Uuid) -> PcsResult<()> {
        // Idempotent: a claim already acked (index deleted) is Ok.
        let Some(index) = self.read_claim_index(claim_id).await? else {
            return Ok(());
        };
        let key = Key::from(rows_key(&self.prefix, index.batch_id, index.start));
        let Some(row_value) = self
            .client
            .get(key.clone())
            .await
            .map_err(|e| PcsError::store(format!("tikv get row range: {e}")))?
        else {
            return Ok(());
        };
        let row: RowRangeRecord = postcard::from_bytes(&row_value)
            .map_err(|e| PcsError::store(format!("tikv decode row range: {e}")))?;
        let mut completed = row;
        completed.status = RowRangeStatus::Completed as u8;
        let completed_bytes = postcard::to_allocvec(&completed)
            .map_err(|e| PcsError::store(format!("tikv encode completed range: {e}")))?;
        // The swap result does not matter: a duplicate ack is harmless and the
        // index is deleted regardless so the next ack is idempotent.
        let _ = self
            .client
            .compare_and_swap(key, Some(row_value), completed_bytes)
            .await
            .map_err(|e| PcsError::store(format!("tikv ack CAS: {e}")))?;
        self.client
            .delete(Key::from(claim_key(&self.prefix, claim_id)))
            .await
            .map_err(|e| PcsError::store(format!("tikv delete claim index {claim_id}: {e}")))?;
        Ok(())
    }

    async fn release_claim(&self, claim_id: Uuid, _instance_id: Uuid) -> PcsResult<()> {
        let Some(index) = self.read_claim_index(claim_id).await? else {
            return Ok(());
        };
        let key = Key::from(rows_key(&self.prefix, index.batch_id, index.start));
        let Some(row_value) = self
            .client
            .get(key.clone())
            .await
            .map_err(|e| PcsError::store(format!("tikv get row range: {e}")))?
        else {
            return Ok(());
        };
        let row: RowRangeRecord = postcard::from_bytes(&row_value)
            .map_err(|e| PcsError::store(format!("tikv decode row range: {e}")))?;
        let mut pending = row;
        pending.status = RowRangeStatus::Pending as u8;
        pending.claim_id = [0u8; 16];
        pending.instance_id = [0u8; 16];
        pending.lease_expires_at_ms = 0;
        let pending_bytes = postcard::to_allocvec(&pending)
            .map_err(|e| PcsError::store(format!("tikv encode pending range: {e}")))?;
        let (_, swapped) = self
            .client
            .compare_and_swap(key, Some(row_value), pending_bytes)
            .await
            .map_err(|e| PcsError::store(format!("tikv release CAS: {e}")))?;
        if !swapped {
            return Err(PcsError::store(format!(
                "tikv: release lost a race for claim {claim_id}"
            )));
        }
        self.client
            .delete(Key::from(claim_key(&self.prefix, claim_id)))
            .await
            .map_err(|e| PcsError::store(format!("tikv delete claim index {claim_id}: {e}")))?;
        Ok(())
    }

    async fn reclaim_expired(&self, now_millis: u64) -> PcsResult<u32> {
        let rows = scan_prefix(&self.client, &format!("{}/rows/", self.prefix)).await?;
        let mut reclaimed = 0u32;
        for row_pair in rows {
            let row_key = row_pair.0;
            let row_value = row_pair.1;
            let row: RowRangeRecord = postcard::from_bytes(&row_value)
                .map_err(|e| PcsError::store(format!("tikv decode row range: {e}")))?;
            if row.status != RowRangeStatus::Claimed as u8 || row.lease_expires_at_ms > now_millis {
                continue;
            }
            let claim_id = Uuid::from_bytes(row.claim_id);
            let mut pending = row;
            pending.status = RowRangeStatus::Pending as u8;
            pending.claim_id = [0u8; 16];
            pending.instance_id = [0u8; 16];
            pending.lease_expires_at_ms = 0;
            let pending_bytes = postcard::to_allocvec(&pending)
                .map_err(|e| PcsError::store(format!("tikv encode pending range: {e}")))?;
            let (_, swapped) = self
                .client
                .compare_and_swap(row_key, Some(row_value), pending_bytes)
                .await
                .map_err(|e| PcsError::store(format!("tikv reclaim CAS: {e}")))?;
            if swapped {
                self.client
                    .delete(Key::from(claim_key(&self.prefix, claim_id)))
                    .await
                    .map_err(|e| {
                        PcsError::store(format!("tikv delete claim index {claim_id}: {e}"))
                    })?;
                reclaimed += 1;
            }
        }
        Ok(reclaimed)
    }
}

#[async_trait]
impl CheckpointStore for TikvSharedStore {
    fn max_checkpoint_bytes(&self) -> usize {
        TIKV_MAX_CHECKPOINT_BYTES
    }

    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> PcsResult<()> {
        if ipc_bytes.len() >= TIKV_MAX_CHECKPOINT_BYTES {
            return Err(PcsError::configuration(format!(
                "save_checkpoint: ipc_bytes ({} bytes) exceeds TIKV_MAX_CHECKPOINT_BYTES ({}). \
                 Split per component.",
                ipc_bytes.len(),
                TIKV_MAX_CHECKPOINT_BYTES
            )));
        }
        // The claim index resolves the batch id; a checkpoint without a live
        // claim is rejected, matching the redb state machine's "claim_id not
        // found" behaviour.
        let index = self.read_claim_index(claim_id).await?.ok_or_else(|| {
            PcsError::store(format!(
                "tikv: claim {claim_id} not found; cannot save checkpoint"
            ))
        })?;
        let record = CheckpointRecord {
            batch_id: index.batch_id,
            stage_idx,
            ipc_bytes,
            schema_id,
            created_at_ms: now_millis(),
        };
        let bytes = postcard::to_allocvec(&record)
            .map_err(|e| PcsError::store(format!("tikv encode checkpoint: {e}")))?;
        self.client
            .put(
                Key::from(checkpoint_key(&self.prefix, claim_id, stage_idx)),
                bytes,
            )
            .await
            .map_err(|e| PcsError::store(format!("tikv put checkpoint {claim_id}: {e}")))?;

        // Data-stage checkpoints (everything below the two sentinels) update
        // the schema-id ledger used by validate_schema_fingerprint.
        if stage_idx < u32::MAX - 1 {
            let sid = postcard::to_allocvec(&schema_id)
                .map_err(|e| PcsError::store(format!("tikv encode schema id: {e}")))?;
            self.client
                .put(Key::from(schema_id_key(&self.prefix)), sid)
                .await
                .map_err(|e| PcsError::store(format!("tikv put schema id: {e}")))?;
        }
        Ok(())
    }

    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> PcsResult<Option<Checkpoint>> {
        let key = Key::from(checkpoint_key(&self.prefix, claim_id, stage_idx));
        let Some(value) = self
            .client
            .get(key)
            .await
            .map_err(|e| PcsError::store(format!("tikv get checkpoint {claim_id}: {e}")))?
        else {
            return Ok(None);
        };
        let record: CheckpointRecord = postcard::from_bytes(&value)
            .map_err(|e| PcsError::store(format!("tikv decode checkpoint: {e}")))?;
        Ok(Some(Checkpoint {
            batch_id: record.batch_id,
            stage_idx: record.stage_idx,
            payload: record.ipc_bytes,
            schema_id: record.schema_id,
            created_at: record.created_at_ms,
        }))
    }

    async fn persisted_schema_id(&self) -> PcsResult<Option<u32>> {
        let key = Key::from(schema_id_key(&self.prefix));
        let Some(value) = self
            .client
            .get(key)
            .await
            .map_err(|e| PcsError::store(format!("tikv get schema id: {e}")))?
        else {
            return Ok(None);
        };
        let id = postcard::from_bytes(&value)
            .map_err(|e| PcsError::store(format!("tikv decode schema id: {e}")))?;
        Ok(Some(id))
    }
}

/// Unix milliseconds now (wall clock; used for leases and timestamps).
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_ordering_is_bytewise() {
        assert!(hex_u64(9) < hex_u64(10));
        assert!(hex_u32(0x00FF_FFFF) < hex_u32(0x0100_0000));
        assert_eq!(hex_u64(42).len(), 16);
        assert_eq!(hex_u32(42).len(), 8);
    }

    #[test]
    fn test_key_layout_is_stable() {
        let claim = Uuid::from_u128(0xDEAD_BEEF);
        assert_eq!(batch_key("pcs", 1), "pcs/batch/0000000000000001");
        assert_eq!(rows_key("pcs", 1, 0), "pcs/rows/0000000000000001/00000000");
        assert_eq!(
            claim_key("pcs", claim),
            format!("pcs/claim/{:032x}", 0xDEAD_BEEF_u128)
        );
        assert_eq!(
            checkpoint_key("pcs", claim, 3),
            format!("pcs/checkpoint/{:032x}/00000003", 0xDEAD_BEEF_u128)
        );
        assert_eq!(schema_id_key("pcs"), "pcs/meta/schema_id");
    }

    #[test]
    fn test_parse_hex_tail_round_trips() {
        assert_eq!(parse_hex_tail("pcs/batch/000000000000000A"), Some(10));
        assert_eq!(
            parse_hex_tail("pcs/rows/0000000000000001/00000020"),
            Some(0x20)
        );
        assert_eq!(parse_hex_tail("pcs/batch/"), None);
    }

    #[test]
    fn test_tikv_checkpoint_cap_constant() {
        assert_eq!(TIKV_MAX_CHECKPOINT_BYTES, 4 * 1024 * 1024);
        assert_eq!(TIKV_ROWS_PER_RANGE, 512);
    }

    #[test]
    fn test_key_after_appends_zero() {
        assert_eq!(key_after(b"abc"), b"abc\0");
    }

    #[test]
    fn test_record_round_trips_postcard() {
        let range = RowRangeRecord {
            end: 512,
            status: RowRangeStatus::Claimed as u8,
            claim_id: *Uuid::now_v7().as_bytes(),
            instance_id: [0u8; 16],
            lease_expires_at_ms: 1234,
        };
        let bytes = postcard::to_allocvec(&range).unwrap();
        let back: RowRangeRecord = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, range);
        assert!(bytes.len() < 64, "compact framing, got {}", bytes.len());

        let index = ClaimIndexRecord {
            batch_id: 7,
            start: 512,
        };
        let bytes = postcard::to_allocvec(&index).unwrap();
        assert_eq!(
            postcard::from_bytes::<ClaimIndexRecord>(&bytes).unwrap(),
            index
        );
    }

    /// The store's own checkpoint cap override beats the trait default.
    #[test]
    fn test_max_checkpoint_bytes_default_vs_override() {
        struct DefaultStore;
        #[async_trait]
        impl CheckpointStore for DefaultStore {
            async fn save_checkpoint(&self, _: Uuid, _: u32, _: Vec<u8>, _: u32) -> PcsResult<()> {
                Ok(())
            }
            async fn load_checkpoint(&self, _: Uuid, _: u32) -> PcsResult<Option<Checkpoint>> {
                Ok(None)
            }
        }
        assert_eq!(
            DefaultStore.max_checkpoint_bytes(),
            MAX_LOG_ENTRY_BYTES,
            "the trait default is the shared 1 MiB payload cap"
        );
        // TIKV_MAX_CHECKPOINT_BYTES (4 MiB) > MAX_LOG_ENTRY_BYTES (1 MiB) is
        // the whole point of the override; the constant's own test asserts
        // its value.
    }
}
