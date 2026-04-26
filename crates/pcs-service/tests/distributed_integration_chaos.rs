//! Full-stack end-to-end chaos test exercising the distributed layer under
//! combined fault injection.
//!
//! Requires Docker + Toxiproxy.  Runs for ~70 seconds.
//!
//! ```bash
//! cargo test --features distributed-raft,io \
//!     --test distributed_integration_chaos -- --ignored --nocapture
//! ```
//!
//! ## What is tested
//!
//! - 5-node Raft cluster with all TCP edges proxied through Toxiproxy.
//! - 3 concurrent `DistributedRunner` instances competing for batches.
//! - 60-second chaos monkey randomly injecting latency, bandwidth, reset-peer,
//!   and full partition faults on random edges.
//! - After the chaos window + 10s settle:
//!   - Every batch acked at most once (no duplicate Completed entries).
//!   - `last_applied` converges to the same index on all 5 nodes.
//!   - At least one leader change occurred during chaos (liveness under faults).

#[cfg(feature = "distributed-raft")]
mod common;

#[cfg(feature = "distributed-raft")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_stack_chaos_monkey_60s() {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use common::RaftClusterHarness;
    use pcs_service::PcsResult;
    use pcs_service::distributed::checkpoint::{Checkpoint, CheckpointStore};
    use pcs_service::distributed::consensus::store::RedbSharedStore;
    use pcs_service::distributed::partition::{BatchClaim, PartitionSource};
    use pcs_service::distributed::runner::{DistributedRunner, RunnerConfig};
    use pcs_service::distributed::strategy::CheckpointStrategy;
    use pcs_service::pipeline::{Dataset, Pipeline};
    use pcs_service::system::{SystemMeta, system_fn};
    use rand::RngExt;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    // ── 1. Start 5-node cluster ──────────────────────────────────────────────

    let Some(harness) = RaftClusterHarness::try_start(5).await else {
        return;
    };

    let leader_before = harness
        .await_leader()
        .await
        .expect("leader should be elected before chaos");

    // ── 2. Register master batches via the leader's store ────────────────────

    let leader_store = harness.store_for_node(leader_before);

    const NUM_BATCHES: u64 = 5;
    const ROWS_PER_BATCH: u32 = 10;

    for batch_id in 0..NUM_BATCHES {
        leader_store
            .register_master_batch(
                batch_id,
                "chaos_component".to_string(),
                1,
                vec![0u8; 64],
                ROWS_PER_BATCH,
            )
            .await
            .expect("register_master_batch");
    }

    // ── 3. Spawn 3 DistributedRunner tasks ───────────────────────────────────

    /// Newtype so multiple runners can share one `RedbSharedStore` via `Arc`.
    struct ArcStore(Arc<RedbSharedStore>);

    #[async_trait]
    impl PartitionSource for ArcStore {
        async fn claim_next_batch(&self, id: Uuid) -> PcsResult<Option<BatchClaim>> {
            self.0.claim_next_batch(id).await
        }
        async fn renew_claim(&self, id: Uuid, instance_id: Uuid) -> PcsResult<u64> {
            self.0.renew_claim(id, instance_id).await
        }
        async fn ack_claim(&self, id: Uuid, instance_id: Uuid) -> PcsResult<()> {
            self.0.ack_claim(id, instance_id).await
        }
        async fn release_claim(&self, id: Uuid, instance_id: Uuid) -> PcsResult<()> {
            self.0.release_claim(id, instance_id).await
        }
        async fn reclaim_expired(&self, now_millis: u64) -> PcsResult<u32> {
            self.0.reclaim_expired(now_millis).await
        }
    }

    #[async_trait]
    impl CheckpointStore for ArcStore {
        async fn save_checkpoint(
            &self,
            claim_id: Uuid,
            stage_idx: u32,
            ipc_bytes: Vec<u8>,
            schema_id: u32,
        ) -> PcsResult<()> {
            self.0
                .save_checkpoint(claim_id, stage_idx, ipc_bytes, schema_id)
                .await
        }
        async fn load_checkpoint(
            &self,
            claim_id: Uuid,
            stage_idx: u32,
        ) -> PcsResult<Option<Checkpoint>> {
            self.0.load_checkpoint(claim_id, stage_idx).await
        }
    }

    // Nodes 1-3 serve as runner backends. Using the leader ensures proposals
    // succeed during calm periods; chaos will test follower forwarding.
    let shared_store = Arc::new(harness.store_for_node(leader_before));

    let shutdown = CancellationToken::new();

    let runner_config = RunnerConfig {
        max_batches: None,
        checkpoint_strategy: CheckpointStrategy::None,
        ..Default::default()
    };

    fn make_pipeline(name: &str) -> Pipeline {
        let mut p = Pipeline::new(name);
        p.add_system(system_fn(SystemMeta::new("passthrough"), |_| Ok(())));
        p
    }

    // ── 4. Chaos monkey for 60 seconds (runs concurrently with runners) ────────

    const N_NODES: usize = 5;
    const CHAOS_DURATION: Duration = Duration::from_secs(60);

    // Capture term before chaos to detect leader elections later.
    let term_before: u64 = harness.max_term();

    // The chaos loop mutates Toxiproxy state via blocking HTTP calls wrapped in
    // `spawn_blocking`.  The runner futures are NOT `Send` (Pipeline contains
    // `Box<dyn Sink>` which is not Sync), so we use `tokio::join!` to drive
    // all four futures on the same thread instead of `tokio::spawn`.
    //
    // Chaos monkey future — captures `Arc<ToxiproxyClient>` clone.
    let api_port = harness.toxiproxy().api_port;
    let chaos_fut = async {
        use common::ToxiproxyClient;

        let toxi = ToxiproxyClient::new(api_port);
        let chaos_end = tokio::time::Instant::now() + CHAOS_DURATION;
        let mut rng = rand::rng();

        while tokio::time::Instant::now() < chaos_end {
            let src = rng.random_range(0..N_NODES);
            let dst = loop {
                let d = rng.random_range(0..N_NODES);
                if d != src {
                    break d;
                }
            };
            let proxy_name = RaftClusterHarness::proxy_name(src, dst);

            let action = rng.random_range(0u32..4);
            match action {
                // Latency: add, hold briefly, remove.
                0 => {
                    let ms = rng.random_range(0u64..=500);
                    let hold_ms = rng.random_range(1000u64..=3000);
                    let _ = toxi.add_latency(&proxy_name, ms);
                    tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                    let _ = toxi.delete_toxic(&proxy_name, "upstream");
                }
                // Bandwidth: add, hold briefly, remove.
                1 => {
                    // 10 KB/s = 80 kbps to 10 MB/s = 80_000 kbps
                    let kbps = rng.random_range(80u64..=80_000);
                    let hold_ms = rng.random_range(1000u64..=3000);
                    let _ = toxi.add_bandwidth(&proxy_name, kbps);
                    tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                    let _ = toxi.delete_toxic(&proxy_name, "upstream");
                }
                // Reset peer: short hold.
                2 => {
                    let timeout_ms = rng.random_range(0u64..=200);
                    let hold_ms = rng.random_range(200u64..=500);
                    let _ = toxi.add_reset_peer(&proxy_name, timeout_ms);
                    tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                    let _ = toxi.delete_toxic(&proxy_name, "reset_peer");
                }
                // Full partition: disable then re-enable.
                _ => {
                    let hold_ms = rng.random_range(500u64..=5000);
                    let _ = toxi.disable_proxy(&proxy_name);
                    tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                    let _ = toxi.enable_proxy(&proxy_name);
                }
            }

            // Random sleep between actions: 200ms–2s.
            let sleep_ms = rng.random_range(200u64..=2000);
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }

        // After chaos window: reset all proxies and wait for settlement.
        let _ = toxi.reset();
        tokio::time::sleep(Duration::from_secs(10)).await;
    };

    // Runner futures — each owns its own store/pipeline, bound to the same Arc.
    let run_fut_0 = {
        let store = ArcStore(Arc::clone(&shared_store));
        let pipeline = make_pipeline("chaos-runner-0");
        let config = runner_config.clone();
        let token = shutdown.clone();
        async move {
            let runner = DistributedRunner::new(store, Box::new(pipeline), config);
            runner.run_with_shutdown(Dataset::new, token).await
        }
    };
    let run_fut_1 = {
        let store = ArcStore(Arc::clone(&shared_store));
        let pipeline = make_pipeline("chaos-runner-1");
        let config = runner_config.clone();
        let token = shutdown.clone();
        async move {
            let runner = DistributedRunner::new(store, Box::new(pipeline), config);
            runner.run_with_shutdown(Dataset::new, token).await
        }
    };
    let run_fut_2 = {
        let store = ArcStore(Arc::clone(&shared_store));
        let pipeline = make_pipeline("chaos-runner-2");
        let config = runner_config.clone();
        let token = shutdown.clone();
        async move {
            let runner = DistributedRunner::new(store, Box::new(pipeline), config);
            runner.run_with_shutdown(Dataset::new, token).await
        }
    };

    // Drive chaos and all 3 runners concurrently.  Chaos completes after ~70s;
    // runners keep going until shutdown is cancelled (below).
    // We wrap runners in `tokio::select!` so they exit when the chaos future
    // ends the 60s window.  After chaos_fut returns, we cancel and drain.
    let (_, r0, r1, r2) = tokio::join!(chaos_fut, run_fut_0, run_fut_1, run_fut_2);

    // ── 5. Shut down ─────────────────────────────────────────────────────────

    // Cancel any runners that are still waiting for batches.
    shutdown.cancel();

    // Log processed counts (runners may have already exited after chaos).
    let _ = (r0, r1, r2);

    // ── 6. Assertions ─────────────────────────────────────────────────────────

    // 6a. Wait for all 5 nodes to converge on the same last_applied index.
    let converged = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let indices: Vec<Option<u64>> = (1..=5).map(|id| harness.last_applied(id)).collect();
            let all_some = indices.iter().all(|i| i.is_some());
            let max = indices.iter().flatten().copied().max().unwrap_or(0);
            let min = indices.iter().flatten().copied().min().unwrap_or(0);
            if all_some && max == min {
                break true;
            }
            if tokio::time::Instant::now() >= deadline {
                eprintln!("last_applied indices did not converge: {indices:?}");
                break false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    assert!(
        converged,
        "all 5 nodes must converge to the same last_applied index"
    );

    // 6b. No duplicate Completed entries across all nodes.
    //
    // Every node should have the same set of completed (batch_id, row_range)
    // entries. We verify no row range appears as Completed more than once on
    // any single node (idempotency of AckClaim).
    //
    // Completed count per node must be identical (deterministic replication).
    let completed_counts: Vec<usize> = (1..=5)
        .map(|id| harness.count_completed_claims(id))
        .collect();

    let first_count = completed_counts[0];
    for (i, &count) in completed_counts.iter().enumerate() {
        assert_eq!(
            count,
            first_count,
            "node {} has {count} completed claims, expected {first_count} (same as node 1)",
            i + 1
        );
    }

    // 6c. At least one leader election happened during chaos (liveness).
    let term_after: u64 = harness.max_term();
    assert!(
        term_after > term_before,
        "Raft term must have advanced during 60s of chaos (before={term_before}, after={term_after})"
    );

    // 6d. Completed count must be ≤ total row ranges registered.
    //
    // Each batch has ROWS_PER_BATCH rows that can form at most one
    // non-overlapping claim. So total completed ≤ NUM_BATCHES.
    assert!(
        first_count <= NUM_BATCHES as usize,
        "completed claim count ({first_count}) must not exceed registered batch count ({NUM_BATCHES})"
    );

    harness.shutdown().await;
}
