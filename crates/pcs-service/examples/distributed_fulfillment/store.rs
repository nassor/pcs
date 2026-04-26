//! FulfillmentStore — thin wrapper around [`RedbSharedStore`].
//!
//! The [`DistributedRunner`] calls `world_factory()` to get a fresh [`Dataset`],
//! but the runner does **not** automatically load the master-batch IPC bytes
//! that the generator registered via `register_master_batch`. This wrapper
//! intercepts [`PartitionSource::claim_next_batch`], reads the IPC from the
//! Raft state-machine database, and stashes it in a shared slot. The
//! `world_factory` closure reads the slot and hydrates the dataset with real
//! `Order` rows before the pipeline starts.

use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use pcs_service::PcsError;
use pcs_service::PcsResult;
use pcs_service::component::Component;
use pcs_service::distributed::RedbSharedStore;
use pcs_service::distributed::checkpoint::{Checkpoint, CheckpointStore};
use pcs_service::distributed::consensus::state_machine::read_master_batch;
use pcs_service::distributed::partition::{BatchClaim, PartitionSource};
use pcs_service::pipeline::Dataset;

use crate::components::{Invoice, Order};
use crate::resources::{FxRateTable, InventoryCatalog, NodeId, TaxRateTable};

// ── FulfillmentStore ──────────────────────────────────────────────────────────

/// Wraps [`RedbSharedStore`], intercepting `claim_next_batch` to pre-load the
/// master-batch Arrow IPC into a shared slot so `world_factory` can hydrate
/// the dataset with real `Order` rows.
#[derive(Clone)]
pub struct FulfillmentStore {
    /// Underlying store — used for all partition and checkpoint operations.
    pub inner: Arc<RedbSharedStore>,
    /// Read-only access to the Raft state-machine database (same `Arc` the
    /// state machine owns).  Used to call `read_master_batch`.
    app_db: Arc<std::sync::Mutex<redb::Database>>,
    /// IPC bytes stashed by `claim_next_batch`, consumed by `world_factory`.
    pending_world_ipc: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

impl FulfillmentStore {
    /// Construct a new store.
    ///
    /// `app_db` must be the same `Arc<Mutex<Database>>` that was passed to
    /// [`RedbSharedStore::multi_node`] so reads see the same committed data.
    pub fn new(inner: Arc<RedbSharedStore>, app_db: Arc<std::sync::Mutex<redb::Database>>) -> Self {
        Self {
            inner,
            app_db,
            pending_world_ipc: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Register a master batch through the inner store.
    ///
    /// Delegates to [`RedbSharedStore::register_master_batch`].  The generator
    /// calls this to push new `Order` batches into the cluster.
    pub async fn register_batch(
        &self,
        batch_id: u64,
        component: String,
        schema_id: u32,
        ipc_bytes: Vec<u8>,
        total_rows: u32,
    ) -> PcsResult<()> {
        self.inner
            .register_master_batch(batch_id, component, schema_id, ipc_bytes, total_rows)
            .await
    }

    /// Return a `Fn() -> Dataset` closure suitable for [`DistributedRunner::run`].
    ///
    /// The closure:
    /// 1. Takes the pending IPC bytes written by `claim_next_batch` (if any).
    /// 2. Hydrates a dataset from those bytes (`Dataset::read_ipc`).
    /// 3. Registers the `Invoice` component (not present in generator IPC).
    /// 4. Inserts all resources (FxRateTable, TaxRateTable, InventoryCatalog, NodeId).
    ///
    /// If no IPC bytes are in the slot (e.g. on the very first call before any
    /// batch is claimed) an empty dataset is returned.
    pub fn world_factory(&self, node_id: u64) -> impl Fn() -> Dataset {
        let pending = Arc::clone(&self.pending_world_ipc);
        move || {
            // Build a fresh dataset with BOTH components registered up-front.
            // Components must be registered before any rows are appended, so
            // we can't reuse the dataset returned by `Dataset::read_ipc` (which
            // arrives with Order rows already in place).
            let mut dataset = Dataset::builder().with::<Order>().with::<Invoice>().build();

            // Hydrate Order rows from the pending IPC slot, if any. We parse
            // the IPC into a throwaway dataset, extract the Order RecordBatch,
            // deserialise it back into `Vec<Order>`, then append into the
            // fresh (already-registered) dataset.
            if let Some(ipc) = pending.lock().unwrap().take() {
                let mut cursor = Cursor::new(ipc);
                let tmp_world = Dataset::read_ipc(&mut cursor)
                    .expect("FulfillmentStore: corrupted master-batch IPC");
                if let Some(batch) = tmp_world.columns::<Order>() {
                    let orders = Order::from_record_batch(batch)
                        .expect("FulfillmentStore: decode Order RecordBatch");
                    if !orders.is_empty() {
                        dataset
                            .append::<Order>(&orders)
                            .expect("FulfillmentStore: append Order rows");
                    }
                }
            }

            // Resources are stateless lookups; always insert defaults.
            dataset.insert_resource(FxRateTable::default());
            dataset.insert_resource(TaxRateTable::default());
            dataset.insert_resource(InventoryCatalog::default());
            dataset.insert_resource(NodeId(node_id));
            dataset
        }
    }
}

// ── PartitionSource ───────────────────────────────────────────────────────────

#[async_trait]
impl PartitionSource for FulfillmentStore {
    /// Claim the next pending batch AND pre-load its IPC bytes into the shared
    /// slot so the pipeline factory can hydrate a real `Order` pipeline.
    async fn claim_next_batch(&self, instance_id: Uuid) -> PcsResult<Option<BatchClaim>> {
        let claim_opt = self.inner.claim_next_batch(instance_id).await?;

        if let Some(ref claim) = claim_opt {
            let batch_id = claim.batch_id;
            let db = Arc::clone(&self.app_db);

            // Blocking DB read — offload to thread pool to avoid blocking async runtime.
            let ipc_opt = tokio::task::spawn_blocking(move || {
                let db = db
                    .lock()
                    .map_err(|_| PcsError::store("app_db mutex poisoned"))?;
                let record = read_master_batch(&db, batch_id)?;
                Ok::<Option<Vec<u8>>, PcsError>(record.map(|r| r.ipc_bytes))
            })
            .await
            .map_err(|e| PcsError::generic(format!("spawn_blocking: {e}")))??;

            *self.pending_world_ipc.lock().unwrap() = ipc_opt;

            #[cfg(feature = "tracing")]
            tracing::debug!(
                batch_id,
                "FulfillmentStore: stashed master-batch IPC for pipeline factory"
            );
        }

        Ok(claim_opt)
    }

    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<u64> {
        self.inner.renew_claim(claim_id, instance_id).await
    }

    async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<()> {
        self.inner.ack_claim(claim_id, instance_id).await
    }

    async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<()> {
        self.inner.release_claim(claim_id, instance_id).await
    }
}

// ── CheckpointStore ───────────────────────────────────────────────────────────

#[async_trait]
impl CheckpointStore for FulfillmentStore {
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> PcsResult<()> {
        self.inner
            .save_checkpoint(claim_id, stage_idx, ipc_bytes, schema_id)
            .await
    }

    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> PcsResult<Option<Checkpoint>> {
        self.inner.load_checkpoint(claim_id, stage_idx).await
    }
}
