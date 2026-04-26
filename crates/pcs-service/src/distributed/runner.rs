//! [`DistributedRunner`] — orchestration loop for distributed Arrow batch execution.
//!
//! ## Design: template runtime and per-partition Dataset
//!
//! [`DistributedRunner`] holds a `runtime: Box<dyn PipelineRuntime>` as a
//! **template**. The runtime owns whatever execution backend applies (native
//! system DAG for `Pipeline`, or a WASM guest component), and its
//! internal state is never used directly — data arrives via [`PartitionSource`],
//! not the IO layer.
//!
//! For each claimed batch the runner:
//! 1. Calls `world_factory()` to produce a fresh, empty [`Dataset`].
//! 2. Optionally loads prior window accumulator state into the dataset.
//! 3. Calls `self.runtime.run_on(&mut partition_data)` to execute the runtime
//!    against the partition dataset.
//! 4. Optionally saves a checkpoint of the resulting [`Dataset`] via
//!    [`CheckpointStore`].
//! 5. Acks or releases the claim based on success or failure.
//!
//! **The runtime is re-used across all iterations** — it holds no per-batch
//! state outside the dataset passed to `run_on`.
//!
//! ## Lease contract
//!
//! The runner checks whether lease renewal is needed BEFORE executing the
//! system DAG. If [`PartitionSource::renew_claim`] returns an error, the runner
//! STOPS immediately and returns `Err(PcsError::Generic("lease renewal failed"))`.
//! It does NOT continue processing — charging ahead after a lease failure
//! violates at-most-once processing semantics.
//!
//! ## Checkpoint size contract
//!
//! Checkpoints must fit within [`MAX_LOG_ENTRY_BYTES`]. A runner whose dataset
//! grows beyond this limit returns [`PcsError::configuration`] so the operator
//! can fix the pipeline (shorter batches, smaller per-entity state, or
//! additional component splitting upstream). Silent data loss via a best-effort
//! empty marker checkpoint is **not** an option: the runner will never
//! acknowledge a batch whose state cannot be durably checkpointed.
//!
//! ## Release-not-ack on checkpoint failure
//!
//! If [`CheckpointStore::save_checkpoint`] fails mid-batch (e.g. transient
//! Raft unavailability), the runner releases the claim via
//! [`PartitionSource::release_claim`] rather than acking it. At-least-once
//! semantics rely on the next runner (or this one, after recovery) seeing
//! the batch as pending so it can retry.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use pcs_core::PipelineRuntime;

use crate::PcsError;
use crate::PcsResult;
use crate::distributed::checkpoint::CheckpointStore;
use crate::distributed::partition::{MAX_LOG_ENTRY_BYTES, PartitionSource};
use crate::distributed::strategy::CheckpointStrategy;
use crate::pipeline::Dataset;
#[cfg(test)]
use crate::pipeline::Pipeline;

// Re-export for backwards compatibility with `pcs_service::distributed::runner::KeyPartition`.
pub use crate::partition::KeyPartition;

/// Configuration for an [`DistributedRunner`].
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Unique identifier for this runner instance.
    pub instance_id: Uuid,
    /// Checkpoint frequency.
    pub checkpoint_strategy: CheckpointStrategy,
    /// Default schema version applied when writing checkpoints.
    pub schema_id: u32,
    /// Maximum number of batches to process (useful for testing).
    pub max_batches: Option<usize>,
    /// How frequently to check lease renewal (in milliseconds).
    pub lease_renewal_check_interval_millis: u64,
    /// Optional key-based partition mask for multi-instance window accumulation.
    ///
    /// When `Some`, the runner injects a [`KeyPartition`] resource into the
    /// dataset before each pipeline run.  `WindowedSystem` reads this resource
    /// to filter out rows that belong to other instances.
    ///
    /// `None` (default) disables partitioning — all rows are processed by
    /// every runner (suitable for single-instance deployments).
    #[cfg(feature = "windows")]
    pub partition_mask: Option<KeyPartition>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            instance_id: Uuid::new_v4(),
            checkpoint_strategy: CheckpointStrategy::EveryStage,
            schema_id: 1,
            max_batches: None,
            lease_renewal_check_interval_millis: 5_000,
            #[cfg(feature = "windows")]
            partition_mask: None,
        }
    }
}

/// Distributed runner that claims Arrow row-range batches, runs them through
/// a [`PipelineRuntime`] template, checkpoints intermediate state, and acks on
/// completion.
///
/// The template runtime owns the execution backend (native system DAG or WASM
/// guest). Per-partition [`Dataset`]s are delivered via [`PartitionSource`];
/// sources and sinks held by the runtime (if any) are unused in distributed
/// mode.
///
/// ## Example
///
/// ```no_run
/// # #[cfg(feature = "distributed")]
/// # {
/// use pcs_service::distributed::runner::{DistributedRunner, RunnerConfig};
///
/// async fn example() {}
/// # }
/// ```
pub struct DistributedRunner<S> {
    store: S,
    runtime: Box<dyn PipelineRuntime>,
    config: RunnerConfig,
}

impl<S> DistributedRunner<S>
where
    S: PartitionSource + CheckpointStore,
{
    /// Create a new runner with the given template `runtime`.
    ///
    /// The runtime is driven via [`PipelineRuntime::run_on`] once per claimed
    /// batch. Per-partition data is supplied by `world_factory` on each call
    /// to [`run`](Self::run).
    pub fn new(store: S, runtime: Box<dyn PipelineRuntime>, config: RunnerConfig) -> Self {
        Self {
            store,
            runtime,
            config,
        }
    }

    /// Run the processing loop until `max_batches` is reached (if configured)
    /// or no more batches are available.
    ///
    /// `world_factory` is called once per batch to produce a fresh, empty
    /// [`Dataset`]. Register components and resources in the factory; the
    /// runner will call `runtime.run_on(&mut dataset)` against the result.
    ///
    /// ## Lease behaviour
    ///
    /// A background task renews the lease at `TTL/3` cadence while `run_on`
    /// executes. If renewal fails mid-execution, `run_on` is cancelled, the
    /// claim is released, and the runner continues to the next batch.
    ///
    /// ## Graceful shutdown
    ///
    /// Pass a [`CancellationToken`] to stop the loop cleanly between batches.
    /// If a batch is in-flight when the token is cancelled, the loop exits
    /// after the current batch completes.
    ///
    /// # Errors
    ///
    /// Returns the first hard [`PcsError`] encountered (lease failures during
    /// mid-execution are handled by releasing + continuing, not returning).
    pub async fn run(&self, world_factory: impl Fn() -> Dataset) -> PcsResult<usize> {
        self.run_with_shutdown(world_factory, CancellationToken::new())
            .await
    }

    /// Like [`run`](Self::run) but accepts a [`CancellationToken`] for graceful shutdown.
    ///
    /// When the token is cancelled the loop exits cleanly between batches
    /// without holding a lease.
    pub async fn run_with_shutdown(
        &self,
        world_factory: impl Fn() -> Dataset,
        shutdown: CancellationToken,
    ) -> PcsResult<usize> {
        let mut processed = 0usize;

        // Periodic sweeper: proposes ReclaimExpired at lease_ttl/3 cadence so
        // orphaned leases from crashed runners are recycled without operator
        // intervention. We track the next sweep time as an Instant; the check
        // is non-blocking (no .await) so it only fires when the deadline passes
        // naturally as the loop iterates between batches.
        let sweep_interval = Duration::from_secs(30); // ≈ default TTL (90 s) / 3
        let mut next_sweep = std::time::Instant::now() + sweep_interval;

        loop {
            if let Some(max) = self.config.max_batches
                && processed >= max
            {
                break;
            }

            // Check shutdown between batches — no claim held, clean exit.
            if shutdown.is_cancelled() {
                break;
            }

            // Periodic expired-lease sweep (non-blocking: only fires when the
            // interval has elapsed, immediately returns otherwise).
            let now_instant = std::time::Instant::now();
            if now_instant >= next_sweep {
                next_sweep = now_instant + sweep_interval;
                match self.store.reclaim_expired(Self::now_millis()).await {
                    Ok(n) => {
                        #[cfg(feature = "tracing")]
                        if n > 0 {
                            tracing::info!(reclaimed_count = n, "swept expired leases");
                        }
                        #[cfg(not(feature = "tracing"))]
                        let _ = n;
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %e, "reclaim_expired sweep failed");
                        #[cfg(not(feature = "tracing"))]
                        let _ = e;
                    }
                }
            }

            let claim = match self.store.claim_next_batch(self.config.instance_id).await? {
                None => break, // no more work
                Some(c) => c,
            };

            let mut partition_data = world_factory();

            // ── Load prior window accumulator state ─────────────────────────
            #[cfg(all(feature = "windows", feature = "distributed"))]
            {
                if let Some(kp) = self.config.partition_mask {
                    partition_data.insert_resource(kp);
                }

                use crate::component::Component as _;
                use crate::distributed::accumulator_store::load_accumulator_state;
                use crate::windows::accumulator::WindowAccumulator;

                match load_accumulator_state(&self.store, &claim).await {
                    Ok(Some(batch)) => {
                        if partition_data
                            .batch_for(WindowAccumulator::name())
                            .is_some()
                        {
                            let rows =
                                WindowAccumulator::from_record_batch(&batch).map_err(|e| {
                                    PcsError::generic(format!(
                                        "DistributedRunner: failed to decode accumulator: {e}"
                                    ))
                                })?;
                            partition_data
                                .append::<WindowAccumulator>(&rows)
                                .map_err(|e| {
                                    PcsError::generic(format!(
                                        "DistributedRunner: failed to restore accumulator rows: {e}"
                                    ))
                                })?;
                        }
                    }
                    Ok(None) => {}
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            claim_id = %claim.claim_id,
                            error = %_e,
                            "accumulator load failed; releasing claim for retry"
                        );
                        Self::release_with_log(&self.store, &claim).await;
                        continue;
                    }
                }
            }

            // ── Pre-run lease renewal check ──────────────────────────────────
            if self.store.should_renew(&claim) {
                match self
                    .store
                    .renew_claim(claim.claim_id, claim.instance_id)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        Self::release_with_log(&self.store, &claim).await;
                        return Err(PcsError::generic(format!(
                            "lease renewal failed for claim {}: {e}",
                            claim.claim_id
                        )));
                    }
                }
            }

            // Renewal runs as a sibling branch of `run_on` in the same select
            // so tokio polls both concurrently. If renewal fails first the
            // select returns an Err and dropping the select cancels `run_on`
            // at its next `.await`. If `run_on` finishes first the renewal
            // branch is dropped (and its sleep cancelled).
            let renewal_interval = Duration::from_millis((claim.lease_ttl_millis / 3).max(1));
            let claim_id = claim.claim_id;
            let claim_instance_id = claim.instance_id;
            let store_ref = &self.store;
            let renewal_branch = async {
                loop {
                    tokio::time::sleep(renewal_interval).await;
                    if let Err(e) = store_ref.renew_claim(claim_id, claim_instance_id).await {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            %claim_id,
                            error = %e,
                            "mid-execution lease renewal failed; cancelling run_on"
                        );
                        #[cfg(not(feature = "tracing"))]
                        let _ = e;
                        return;
                    }
                }
            };

            enum RunOutcome {
                Ran(PcsResult<()>),
                RenewalFailed,
            }
            let runtime = &*self.runtime;
            let outcome = tokio::select! {
                biased;
                result = runtime.run_on(&mut partition_data) => RunOutcome::Ran(result),
                () = renewal_branch => RunOutcome::RenewalFailed,
            };

            let run_result: PcsResult<()> = match outcome {
                RunOutcome::Ran(r) => r,
                RunOutcome::RenewalFailed => {
                    Self::release_with_log(&self.store, &claim).await;
                    continue;
                }
            };

            let mut run_error: Option<PcsError> = run_result.err();
            let mut claim_released = false;

            // ── Checkpoint ───────────────────────────────────────────────────
            if run_error.is_none()
                && self.config.checkpoint_strategy.should_checkpoint(0)
                && let Err(e) = self
                    .write_checkpoint(&claim.claim_id, 0, &partition_data)
                    .await
            {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    claim_id = %claim.claim_id,
                    error = %e,
                    "checkpoint save failed; releasing claim for retry"
                );
                Self::release_with_log(&self.store, &claim).await;
                claim_released = true;
                run_error = Some(e);
            }

            // ── Save window accumulator state ───────────────────────────────
            #[cfg(all(feature = "windows", feature = "distributed"))]
            if run_error.is_none() && !claim_released {
                use crate::distributed::accumulator_store::save_accumulator_state;
                if let Err(e) = save_accumulator_state(&self.store, &claim, &partition_data).await {
                    #[cfg(feature = "tracing")]
                    tracing::error!(
                        claim_id = %claim.claim_id,
                        error = %e,
                        "accumulator save failed; releasing claim for retry"
                    );
                    Self::release_with_log(&self.store, &claim).await;
                    claim_released = true;
                    run_error = Some(e);
                }
            }

            match (run_error, claim_released) {
                (Some(e), true) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        claim_id = %claim.claim_id,
                        error = %e,
                        "skipping ack: claim was released on checkpoint or accumulator failure"
                    );
                    let _ = e;
                    continue;
                }
                (Some(e), false) => {
                    Self::release_with_log(&self.store, &claim).await;
                    return Err(e);
                }
                (None, _) => {
                    self.store
                        .ack_claim(claim.claim_id, claim.instance_id)
                        .await?;
                    processed += 1;
                }
            }
        }

        Ok(processed)
    }

    async fn release_with_log(store: &S, claim: &crate::distributed::partition::BatchClaim) {
        if let Err(e) = store.release_claim(claim.claim_id, claim.instance_id).await {
            #[cfg(feature = "tracing")]
            tracing::error!(
                claim_id = %claim.claim_id,
                error = %e,
                "release_claim failed; claim may be orphaned until lease expiry"
            );
            // Log even without the tracing feature so the error isn't silently swallowed.
            #[cfg(not(feature = "tracing"))]
            eprintln!("release_claim failed for {}: {e}", claim.claim_id);
        }
    }

    /// Serialize `data` as Arrow IPC and write a checkpoint.
    ///
    /// The serialized dataset must fit within [`MAX_LOG_ENTRY_BYTES`]. If it
    /// doesn't, the runner returns [`PcsError::configuration`] so the
    /// operator can respond: shorten batches, split components upstream, or
    /// reduce per-entity state. The previous "empty marker" fallback has
    /// been removed — silent data loss on crash recovery is unacceptable for
    /// any batch.
    async fn write_checkpoint(
        &self,
        claim_id: &Uuid,
        stage_idx: u32,
        data: &Dataset,
    ) -> PcsResult<()> {
        let mut buf = Vec::new();
        data.write_ipc(&mut buf)?;

        if buf.len() > MAX_LOG_ENTRY_BYTES {
            return Err(PcsError::configuration(format!(
                "checkpoint dataset size {} bytes exceeds MAX_LOG_ENTRY_BYTES {} — \
                 reduce pipeline state or shorten batches",
                buf.len(),
                MAX_LOG_ENTRY_BYTES
            )));
        }

        let schema_id = data.schemas().fingerprint();
        self.store
            .save_checkpoint(*claim_id, stage_idx, buf, schema_id)
            .await
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::checkpoint::{Checkpoint, CheckpointStore};
    use crate::distributed::consensus::state_machine::apply as sm_apply;
    use crate::distributed::consensus::store::RedbSharedStore;
    use crate::distributed::consensus::types::ConsensusCommand;
    use crate::distributed::partition::{BatchClaim, PartitionSource};
    use crate::pipeline::Dataset;
    use crate::system::{SystemMeta, system_fn};
    use async_trait::async_trait;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn temp_path() -> PathBuf {
        let dir = std::env::temp_dir();
        dir.join(format!("pcs_runner_test_{}.db", Uuid::new_v4()))
    }

    fn empty_data() -> Dataset {
        Dataset::new()
    }

    #[tokio::test]
    async fn test_runner_happy_path_no_batches() {
        let path = temp_path();
        let store = RedbSharedStore::single_node(&path).unwrap();
        let pipeline = Pipeline::new("test");
        let config = RunnerConfig {
            max_batches: Some(5),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(store, Box::new(pipeline), config);
        let processed = runner.run(empty_data).await.unwrap();
        assert_eq!(processed, 0); // no batches registered
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_runner_processes_one_batch() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let path = temp_path();
        let store = RedbSharedStore::single_node(&path).unwrap();

        // Seed a batch.
        let seed_db = match &store {
            RedbSharedStore::SingleNode(s) => Arc::clone(&s.db),
            #[cfg(feature = "distributed-raft")]
            _ => panic!("expected SingleNode"),
        };
        sm_apply(
            &seed_db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 0,
                component: "test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 64],
                total_rows: 10,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // Counter to verify system ran.
        let counter = StdArc::new(AtomicU32::new(0));
        let counter_clone = StdArc::clone(&counter);

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("increment"), move |_data| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));

        let config = RunnerConfig {
            max_batches: Some(1),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(store, Box::new(pipeline), config);
        let processed = runner.run(empty_data).await.unwrap();
        assert_eq!(processed, 1);
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        let _ = std::fs::remove_file(&path);
    }

    // ── Simulated lease expiry ────────────────────────────────────────────────

    /// A partition source that fails lease renewal to simulate expiry.
    struct ExpirableSource {
        inner: RedbSharedStore,
        fail_renewal: bool,
    }

    #[async_trait]
    impl PartitionSource for ExpirableSource {
        async fn claim_next_batch(&self, id: Uuid) -> PcsResult<Option<BatchClaim>> {
            self.inner.claim_next_batch(id).await
        }
        async fn renew_claim(&self, _claim_id: Uuid, _instance_id: Uuid) -> PcsResult<u64> {
            if self.fail_renewal {
                Err(PcsError::generic("simulated lease expiry"))
            } else {
                Ok(u64::MAX)
            }
        }
        async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<()> {
            self.inner.ack_claim(claim_id, instance_id).await
        }
        async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<()> {
            self.inner.release_claim(claim_id, instance_id).await
        }
        fn should_renew(&self, _claim: &BatchClaim) -> bool {
            self.fail_renewal
        }
    }

    // ExpirableSource also needs to implement CheckpointStore to be used
    // as combined store. Use inner.
    #[async_trait]
    impl CheckpointStore for ExpirableSource {
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

    #[tokio::test]
    async fn test_runner_lease_expiry_causes_clean_exit() {
        let path = temp_path();
        let inner = RedbSharedStore::single_node(&path).unwrap();

        // Seed a batch.
        let seed_db = match &inner {
            RedbSharedStore::SingleNode(s) => Arc::clone(&s.db),
            #[cfg(feature = "distributed-raft")]
            _ => panic!("expected SingleNode"),
        };
        sm_apply(
            &seed_db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 0,
                component: "test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 64],
                total_rows: 10,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let source = ExpirableSource {
            inner,
            fail_renewal: true,
        };

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data| Ok(())));

        let config = RunnerConfig {
            max_batches: Some(1),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(source, Box::new(pipeline), config);
        let result = runner.run(empty_data).await;

        // Runner must return an error (lease failure), not panic or continue.
        assert!(
            result.is_err(),
            "expected lease expiry error, got {result:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    // ── Checkpoint failure must release, not ack ────────────────────────────

    /// Partition source that counts `release_claim` and `ack_claim` calls
    /// and delegates everything else to an inner real store.
    ///
    /// `claims_issued` tracks how many times `claim_next_batch` has returned a
    /// real claim. After one claim has been issued (and presumably released), we
    /// return `Ok(None)` to avoid the infinite re-claim loop that would occur
    /// when the batch is put back to `Pending` by `release_claim` — the runner
    /// loop would otherwise re-find it on every iteration and cycle forever.
    struct CountingSource {
        inner: RedbSharedStore,
        release_count: Arc<AtomicUsize>,
        ack_count: Arc<AtomicUsize>,
        claims_issued: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PartitionSource for CountingSource {
        async fn claim_next_batch(&self, id: Uuid) -> PcsResult<Option<BatchClaim>> {
            // Only issue one claim; after that, pretend no work is available so
            // the runner loop exits instead of cycling on the released batch.
            if self.claims_issued.load(AtomicOrdering::SeqCst) >= 1 {
                return Ok(None);
            }
            let result = self.inner.claim_next_batch(id).await?;
            if result.is_some() {
                self.claims_issued.fetch_add(1, AtomicOrdering::SeqCst);
            }
            Ok(result)
        }
        async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<u64> {
            self.inner.renew_claim(claim_id, instance_id).await
        }
        async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<()> {
            self.ack_count.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.ack_claim(claim_id, instance_id).await
        }
        async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> PcsResult<()> {
            self.release_count.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.release_claim(claim_id, instance_id).await
        }
        fn should_renew(&self, _claim: &BatchClaim) -> bool {
            false
        }
    }

    /// Checkpoint store that always fails `save_checkpoint`.
    #[async_trait]
    impl CheckpointStore for CountingSource {
        async fn save_checkpoint(
            &self,
            _claim_id: Uuid,
            _stage_idx: u32,
            _ipc_bytes: Vec<u8>,
            _schema_id: u32,
        ) -> PcsResult<()> {
            Err(PcsError::generic("simulated checkpoint failure"))
        }
        async fn load_checkpoint(
            &self,
            claim_id: Uuid,
            stage_idx: u32,
        ) -> PcsResult<Option<Checkpoint>> {
            self.inner.load_checkpoint(claim_id, stage_idx).await
        }
    }

    /// Regression test: when `save_checkpoint` fails mid-batch,
    /// the runner must call `release_claim` (for at-least-once retry) and
    /// must NOT call `ack_claim`.
    #[tokio::test]
    async fn test_checkpoint_failure_releases_not_acks() {
        let path = temp_path();
        let inner = RedbSharedStore::single_node(&path).unwrap();

        // Seed a batch so the runner has work to do.
        let seed_db = match &inner {
            RedbSharedStore::SingleNode(s) => Arc::clone(&s.db),
            #[cfg(feature = "distributed-raft")]
            _ => panic!("expected SingleNode"),
        };
        sm_apply(
            &seed_db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 0,
                component: "test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 64],
                total_rows: 10,
                now_at_propose: 0,
            },
        )
        .unwrap();

        let release_count = Arc::new(AtomicUsize::new(0));
        let ack_count = Arc::new(AtomicUsize::new(0));
        let source = CountingSource {
            inner,
            release_count: Arc::clone(&release_count),
            ack_count: Arc::clone(&ack_count),
            claims_issued: Arc::new(AtomicUsize::new(0)),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data| Ok(())));

        let config = RunnerConfig {
            max_batches: Some(1),
            // EveryStage triggers a checkpoint on the run → immediate failure.
            checkpoint_strategy: CheckpointStrategy::EveryStage,
            ..Default::default()
        };
        let runner = DistributedRunner::new(source, Box::new(pipeline), config);

        // The runner should NOT return an error — it logs and continues to the
        // next batch (which doesn't exist, so the loop exits cleanly with
        // `processed == 0`).
        let processed = runner
            .run(empty_data)
            .await
            .expect("runner should skip failed batch without surfacing error");
        assert_eq!(
            processed, 0,
            "batch whose checkpoint failed must not be counted as processed"
        );

        assert_eq!(
            release_count.load(AtomicOrdering::SeqCst),
            1,
            "expected exactly one release on checkpoint failure"
        );
        assert_eq!(
            ack_count.load(AtomicOrdering::SeqCst),
            0,
            "must NOT ack a claim whose checkpoint failed"
        );
        let _ = std::fs::remove_file(&path);
    }

    // ── Accumulator persistence integration tests ───────────────────────────

    /// Two-run scenario: first run creates accumulator rows, second run loads
    /// and merges them.
    ///
    /// Uses a no-op system with a dataset that registers `WindowAccumulator`.
    /// We manually append accumulator rows on first run and verify they persist
    /// across two sequential `runner.run()` calls.
    #[cfg(feature = "windows")]
    #[tokio::test]
    async fn test_accumulator_persists_across_two_runs() {
        use crate::component::Component as _;
        use crate::windows::accumulator::WindowAccumulator;
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let path = temp_path();
        let store = RedbSharedStore::single_node(&path).unwrap();

        let seed_db = match &store {
            RedbSharedStore::SingleNode(s) => Arc::clone(&s.db),
            #[cfg(feature = "distributed-raft")]
            _ => panic!("expected SingleNode"),
        };

        // Seed two batches so two runs happen.
        for batch_id in 0u64..2 {
            sm_apply(
                &seed_db,
                ConsensusCommand::RegisterMasterBatch {
                    batch_id,
                    component: "test".to_string(),
                    schema_id: 1,
                    ipc_bytes: vec![0u8; 64],
                    total_rows: 10,
                    now_at_propose: 0,
                },
            )
            .unwrap();
        }

        // A system that appends one accumulator row per run.
        let run_count = StdArc::new(AtomicU32::new(0));
        let run_count_clone = StdArc::clone(&run_count);

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(
            SystemMeta::new("append_accumulator"),
            move |data: &mut Dataset| {
                let run = run_count_clone.fetch_add(1, Ordering::Relaxed);
                if data.batch_for(WindowAccumulator::name()).is_some() {
                    let row = WindowAccumulator {
                        version: Some(1),
                        source_component: "test".to_string(),
                        window_id: run as i64,
                        key_hash: 0,
                        count: 1,
                        sum_f64: Some(run as f64 + 1.0),
                        min_f64: None,
                        max_f64: None,
                        session_start_ts: None,
                        session_end_ts: None,
                        finalized_at_watermark: None,
                    };
                    data.append::<WindowAccumulator>(&[row]).unwrap();
                }
                Ok(())
            },
        ));

        let world_factory = || {
            let mut d = Dataset::new();
            d.register_component::<WindowAccumulator>().unwrap();
            d
        };

        let config = RunnerConfig {
            max_batches: Some(2),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(store, Box::new(pipeline), config);
        let processed = runner.run(world_factory).await.unwrap();
        assert_eq!(processed, 2);
        // The system ran once per batch.
        assert_eq!(run_count.load(Ordering::Relaxed), 2);

        let _ = std::fs::remove_file(&path);
    }

    /// Simulated stage failure mid-run: verifies the accumulator save is
    /// skipped and the batch is released (not acked), so replay is possible.
    #[cfg(feature = "windows")]
    #[tokio::test]
    async fn test_accumulator_save_failure_releases_not_acks() {
        use crate::distributed::checkpoint::Checkpoint;
        use crate::windows::accumulator::WindowAccumulator;
        use std::sync::Arc as StdArc;
        use std::sync::atomic::AtomicUsize;

        let path = temp_path();
        let inner = RedbSharedStore::single_node(&path).unwrap();

        let seed_db = match &inner {
            RedbSharedStore::SingleNode(s) => Arc::clone(&s.db),
            #[cfg(feature = "distributed-raft")]
            _ => panic!("expected SingleNode"),
        };
        sm_apply(
            &seed_db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id: 0,
                component: "test".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 64],
                total_rows: 10,
                now_at_propose: 0,
            },
        )
        .unwrap();

        // A CheckpointStore that always fails save_checkpoint.
        let release_count = StdArc::new(AtomicUsize::new(0));
        let ack_count = StdArc::new(AtomicUsize::new(0));
        let claims_issued = StdArc::new(AtomicUsize::new(0));

        struct FailingAccumStore {
            inner: RedbSharedStore,
            release_count: StdArc<AtomicUsize>,
            ack_count: StdArc<AtomicUsize>,
            claims_issued: StdArc<AtomicUsize>,
        }

        #[async_trait]
        impl PartitionSource for FailingAccumStore {
            async fn claim_next_batch(&self, id: Uuid) -> PcsResult<Option<BatchClaim>> {
                if self.claims_issued.load(AtomicOrdering::SeqCst) >= 1 {
                    return Ok(None);
                }
                let result = self.inner.claim_next_batch(id).await?;
                if result.is_some() {
                    self.claims_issued.fetch_add(1, AtomicOrdering::SeqCst);
                }
                Ok(result)
            }
            async fn renew_claim(&self, id: Uuid, instance_id: Uuid) -> PcsResult<u64> {
                self.inner.renew_claim(id, instance_id).await
            }
            async fn ack_claim(&self, id: Uuid, instance_id: Uuid) -> PcsResult<()> {
                self.ack_count.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.ack_claim(id, instance_id).await
            }
            async fn release_claim(&self, id: Uuid, instance_id: Uuid) -> PcsResult<()> {
                self.release_count.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.release_claim(id, instance_id).await
            }
            fn should_renew(&self, _: &BatchClaim) -> bool {
                false
            }
        }

        #[async_trait]
        impl CheckpointStore for FailingAccumStore {
            async fn save_checkpoint(&self, _: Uuid, _: u32, _: Vec<u8>, _: u32) -> PcsResult<()> {
                Err(PcsError::generic("simulated accumulator save failure"))
            }
            async fn load_checkpoint(
                &self,
                claim_id: Uuid,
                stage_idx: u32,
            ) -> PcsResult<Option<Checkpoint>> {
                self.inner.load_checkpoint(claim_id, stage_idx).await
            }
        }

        let source = FailingAccumStore {
            inner,
            release_count: Arc::clone(&release_count),
            ack_count: Arc::clone(&ack_count),
            claims_issued: Arc::clone(&claims_issued),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data| Ok(())));

        let world_factory = || {
            let mut d = Dataset::new();
            d.register_component::<WindowAccumulator>().unwrap();
            d
        };

        let config = RunnerConfig {
            max_batches: Some(1),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(source, Box::new(pipeline), config);

        // The runner should NOT return an error — it logs and continues.
        let processed = runner
            .run(world_factory)
            .await
            .expect("runner should skip failed accumulator save without surfacing error");

        assert_eq!(
            processed, 0,
            "batch with failed accumulator save must not be counted"
        );
        assert_eq!(
            release_count.load(AtomicOrdering::SeqCst),
            1,
            "expected exactly one release on accumulator save failure"
        );
        assert_eq!(
            ack_count.load(AtomicOrdering::SeqCst),
            0,
            "must NOT ack a claim whose accumulator save failed"
        );

        let _ = std::fs::remove_file(&path);
    }
    #[tokio::test]
    async fn test_shutdown_between_batches_clean() {
        let path = temp_path();
        let inner = RedbSharedStore::single_node(&path).unwrap();

        let seed_db = match &inner {
            RedbSharedStore::SingleNode(s) => Arc::clone(&s.db),
            #[cfg(feature = "distributed-raft")]
            _ => panic!("expected SingleNode"),
        };
        // Seed two batches.
        for batch_id in 0u64..2 {
            sm_apply(
                &seed_db,
                ConsensusCommand::RegisterMasterBatch {
                    batch_id,
                    component: "test".to_string(),
                    schema_id: 1,
                    ipc_bytes: vec![0u8; 64],
                    total_rows: 10,
                    now_at_propose: 0,
                },
            )
            .unwrap();
        }

        // Signal shutdown before the runner can process any batch.
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let pipeline = Pipeline::new("test");
        let config = RunnerConfig {
            max_batches: None,
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(inner, Box::new(pipeline), config);

        // With the token already cancelled, the loop must exit immediately with 0 processed.
        let processed = runner
            .run_with_shutdown(empty_data, shutdown)
            .await
            .unwrap();
        assert_eq!(processed, 0, "cancelled runner must process 0 batches");

        let _ = std::fs::remove_file(&path);
    }
}
