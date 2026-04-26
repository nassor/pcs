//! Raft consensus fault-injection and chaos tests.
//!
//! Unit-level tests (no Docker) cover the core state-machine invariants:
//! idempotency, monotonicity, reclaim sweeps, snapshot atomicity.
//!
//! Docker-gated cluster tests use the `RaftClusterHarness` from `tests/common`.

#![cfg(feature = "distributed-raft")]

mod common;

// ── Unit-level tests (no Docker) ─────────────────────────────────────────────
//
// These tests exercise the state machine and storage directly without a
// running Raft cluster.

#[cfg(test)]
mod unit {
    use pcs_service::distributed::consensus::state_machine::{
        apply, dump_state, read_claim, read_master_batch, restore_state,
    };
    use pcs_service::distributed::consensus::types::{
        ClaimStatus, ConsensusCommand, ConsensusResponse,
    };
    use redb::Database;
    use tempfile::NamedTempFile;

    fn temp_db() -> (Database, tempfile::TempPath) {
        let file = NamedTempFile::new().expect("tempfile");
        let path = file.into_temp_path();
        let db = Database::create(&path).expect("redb create");
        (db, path)
    }

    fn small_ipc() -> Vec<u8> {
        vec![0xAB; 64]
    }

    fn register_batch(db: &Database, batch_id: u64, total_rows: u32) {
        apply(
            db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: format!("comp_{batch_id}"),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows,
                now_at_propose: 0,
            },
        )
        .unwrap();
    }

    fn claim_range(
        db: &Database,
        batch_id: u64,
        start: u32,
        end: u32,
        claim_id: uuid::Uuid,
        now: u64,
        ttl: u64,
    ) -> ConsensusResponse {
        apply(
            db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: start,
                row_range_end: end,
                claim_id,
                instance_id: uuid::Uuid::new_v4(),
                lease_ttl_millis: ttl,
                now_at_propose: now,
            },
        )
        .unwrap()
    }

    // P2-4: ClaimRowRange must be idempotent on replay.
    #[test]
    fn claim_row_range_replay_idempotent() {
        let (db, _p) = temp_db();
        register_batch(&db, 1, 100);

        let claim_id = uuid::Uuid::new_v4();
        let cmd = ConsensusCommand::ClaimRowRange {
            batch_id: 1,
            row_range_start: 0,
            row_range_end: 50,
            claim_id,
            instance_id: uuid::Uuid::new_v4(),
            lease_ttl_millis: 30_000,
            now_at_propose: 1_000,
        };

        let r1 = apply(&db, cmd.clone()).unwrap();
        assert!(
            matches!(r1, ConsensusResponse::BatchClaimed { .. }),
            "{r1:?}"
        );

        // Replay — must not return Error.
        let r2 = apply(&db, cmd).unwrap();
        assert!(
            matches!(
                r2,
                ConsensusResponse::BatchClaimed {
                    row_range_start: 0,
                    row_range_end: 50,
                    ..
                }
            ),
            "replay must be idempotent, got: {r2:?}"
        );

        // Exactly one claim record.
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Claimed);
    }

    // P2-4: Checkpoint must be idempotent on replay — checkpoint_seq increments once.
    #[test]
    fn checkpoint_replay_idempotent() {
        let (db, _p) = temp_db();
        register_batch(&db, 1, 100);
        let claim_id = uuid::Uuid::new_v4();
        claim_range(&db, 1, 0, 100, claim_id, 0, 30_000);

        let cp_cmd = ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx: 0,
            ipc_bytes: vec![0xCA, 0xFE],
            schema_id: 1,
            now_at_propose: 42,
        };

        let r1 = apply(&db, cp_cmd.clone()).unwrap();
        let ConsensusResponse::CheckpointWritten {
            checkpoint_id: seq1,
        } = r1
        else {
            panic!("expected CheckpointWritten: {r1:?}");
        };

        let r2 = apply(&db, cp_cmd).unwrap();
        let ConsensusResponse::CheckpointWritten {
            checkpoint_id: seq2,
        } = r2
        else {
            panic!("expected CheckpointWritten on replay: {r2:?}");
        };

        assert_eq!(
            seq2, seq1,
            "checkpoint_seq must not double-increment on replay"
        );
        let batch = read_master_batch(&db, 1).unwrap().unwrap();
        assert_eq!(batch.checkpoint_seq, seq1);
    }

    // P1-3: RenewClaim must be monotonic — stale now_at_propose cannot move expiry backwards.
    #[test]
    fn renew_claim_monotonic() {
        let (db, _p) = temp_db();
        register_batch(&db, 1, 100);
        let claim_id = uuid::Uuid::new_v4();
        let inst = uuid::Uuid::new_v4();
        // Claim at t=1000, ttl=60_000 → expires_at=61_000.
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

        // Renew with stale now=500: new_expires=60_500 < 61_000 → max() keeps 61_000.
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
                assert_eq!(expires_at, 61_000, "stale renew must not regress expiry");
            }
            other => panic!("unexpected: {other:?}"),
        }

        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.lease_expires_at, 61_000);
    }

    // P0-1: ReclaimExpired sweeps Claimed → Pending for expired claims.
    #[test]
    fn reclaim_expired_frees_ranges() {
        let (db, _p) = temp_db();
        register_batch(&db, 1, 100);
        let claim_id = uuid::Uuid::new_v4();
        // Claim at t=0, ttl=100 → expires_at=100.
        claim_range(&db, 1, 0, 100, claim_id, 0, 100);

        // Before expiry: nothing reclaimed.
        let r1 = apply(&db, ConsensusCommand::ReclaimExpired { now_at_propose: 50 }).unwrap();
        assert!(
            matches!(
                r1,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 0 }
            ),
            "{r1:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Claimed);

        // After expiry: claim freed.
        let r2 = apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 200,
            },
        )
        .unwrap();
        assert!(
            matches!(
                r2,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 1 }
            ),
            "{r2:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending);
        assert_eq!(rec.lease_expires_at, 0);

        // The range must now be claimable.
        let claim_id2 = uuid::Uuid::new_v4();
        let r3 = claim_range(&db, 1, 0, 100, claim_id2, 200, 30_000);
        assert!(
            matches!(r3, ConsensusResponse::BatchClaimed { .. }),
            "range should be reclaimable after expiry sweep: {r3:?}"
        );
    }

    // P0-4: install_snapshot_atomic_clear — snapshot install purges all pre-existing state.
    #[test]
    fn install_snapshot_atomic_clear() {
        // db1 has batch 1 + claim c1.
        let (db1, _p1) = temp_db();
        register_batch(&db1, 1, 100);
        let c1 = uuid::Uuid::new_v4();
        claim_range(&db1, 1, 0, 50, c1, 0, 30_000);

        // db2 has batch 3 + claim c4 (old state that must be replaced).
        let (db2, _p2) = temp_db();
        register_batch(&db2, 3, 50);
        let c4 = uuid::Uuid::new_v4();
        claim_range(&db2, 3, 0, 50, c4, 0, 30_000);

        // Restore db1 snapshot into db2.
        let (batches, claims, checkpoints, instances) = dump_state(&db1).unwrap();
        restore_state(&db2, batches, claims, checkpoints, instances, None).unwrap();

        // db2 must contain exactly db1's state.
        assert!(
            read_master_batch(&db2, 1).unwrap().is_some(),
            "batch 1 must be present"
        );
        assert!(
            read_master_batch(&db2, 3).unwrap().is_none(),
            "old batch 3 must be purged"
        );
        assert!(
            read_claim(&db2, c1).unwrap().is_some(),
            "claim c1 must be present"
        );
        assert!(
            read_claim(&db2, c4).unwrap().is_none(),
            "old claim c4 must be purged"
        );
    }

    // P2-1: Snapshot format magic, version, and CRC-32 are validated on install.
    #[test]
    fn snapshot_format_magic_version_crc() {
        use pcs_service::distributed::consensus::snapshot::{
            build_snapshot_bytes, install_snapshot_bytes,
        };

        let (db, _p) = temp_db();
        let snap = build_snapshot_bytes(&db).unwrap();
        assert!(
            snap.len() >= 16,
            "snapshot must have at least 16-byte header"
        );

        // Verify magic bytes.
        assert_eq!(&snap[..8], b"ARROWSNA", "magic bytes must match");

        let version = u32::from_le_bytes(snap[8..12].try_into().unwrap());
        assert_eq!(version, 2, "snapshot version must be 2");

        // Valid snapshot installs cleanly.
        let (db2, _p2) = temp_db();
        install_snapshot_bytes(&db2, &snap, None).unwrap();

        // Tampered magic → rejected.
        let mut bad_magic = snap.clone();
        bad_magic[0] ^= 0xFF;
        let (db3, _p3) = temp_db();
        assert!(
            install_snapshot_bytes(&db3, &bad_magic, None).is_err(),
            "tampered magic must be rejected"
        );

        // Tampered body → CRC mismatch.
        let mut bad_body = snap.clone();
        if bad_body.len() > 16 {
            bad_body[16] ^= 0xFF;
        }
        let (db4, _p4) = temp_db();
        assert!(
            install_snapshot_bytes(&db4, &bad_body, None).is_err(),
            "tampered body (CRC mismatch) must be rejected"
        );
    }
}

// ── Idempotency tests via public RedbSharedStore API ────────────────────────
//
// These tests exercise end-to-end invariants through the public surface only:
// `RedbSharedStore::single_node`, `register_master_batch`, `claim_next_batch`,
// and `propose_reclaim_expired`.  No direct state machine access.

#[cfg(test)]
mod idempotency {
    use pcs_service::distributed::consensus::store::RedbSharedStore;
    use pcs_service::distributed::partition::PartitionSource;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::NamedTempFile;
    use uuid::Uuid;

    fn temp_path() -> PathBuf {
        let file = NamedTempFile::new().expect("tempfile");
        let path = file.into_temp_path();
        path.to_path_buf()
    }

    fn small_ipc() -> Vec<u8> {
        vec![0xAB; 64]
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Register a batch then claim it twice. The second `claim_next_batch`
    /// must return `Ok(None)` — the range is already claimed and must not be
    /// double-issued to a different runner.
    #[tokio::test]
    async fn end_to_end_claim_replay_via_raft() {
        let path = temp_path();
        let store = RedbSharedStore::single_node(&path).unwrap();

        store
            .register_master_batch(1, "comp".to_string(), 1, small_ipc(), 100)
            .await
            .unwrap();

        let instance_id = Uuid::new_v4();
        let claim1 = store
            .claim_next_batch(instance_id)
            .await
            .unwrap()
            .expect("first claim must succeed");
        assert_eq!(claim1.batch_id, 1);

        // Second caller with a different instance sees no available batch.
        let claim2 = store.claim_next_batch(Uuid::new_v4()).await.unwrap();
        assert!(
            claim2.is_none(),
            "already-claimed range must not be re-issued: got {claim2:?}"
        );
    }

    /// Claim a batch with a short TTL, sleep past expiry, sweep with
    /// `propose_reclaim_expired`, and verify the range is re-claimable.
    #[tokio::test]
    async fn reclaim_expired_sweep_via_store() {
        let path = temp_path();
        let store = RedbSharedStore::single_node(&path)
            .unwrap()
            .with_lease_ttl_millis(50); // 50 ms TTL

        store
            .register_master_batch(2, "comp".to_string(), 1, small_ipc(), 100)
            .await
            .unwrap();

        let instance_id = Uuid::new_v4();
        let claim = store
            .claim_next_batch(instance_id)
            .await
            .unwrap()
            .expect("initial claim must succeed");
        assert_eq!(claim.batch_id, 2);

        // Wait for the lease to expire.
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        // Sweep with a timestamp well past expiry.
        let reclaimed = store.propose_reclaim_expired(now_millis()).await.unwrap();
        assert_eq!(reclaimed, 1, "exactly one expired claim must be freed");

        // The range is now pending again — a new runner can claim it.
        let reclaim = store.claim_next_batch(Uuid::new_v4()).await.unwrap();
        assert!(
            reclaim.is_some(),
            "reclaimed range must be claimable again after sweep"
        );
    }
}

// ── Docker-gated cluster chaos tests ─────────────────────────────────────────
//
// These tests use only the existing RaftClusterHarness API:
//   - start(n), await_leader(), propose_noop(leader_id), last_applied(node_id)
//   - toxiproxy() + proxy_name(src, dst) for network fault injection
//   - shutdown()

#[cfg(test)]
mod cluster {
    use super::common::RaftClusterHarness;
    use std::time::Duration;

    /// After cutting all inbound+outbound Toxiproxy links for the leader,
    /// the remaining two nodes elect a new leader within 10 s and can commit
    /// a no-op on that leader.
    #[tokio::test]
    async fn isolate_leader_elects_new() {
        let Some(harness) = RaftClusterHarness::try_start(3).await else {
            return;
        };
        let leader_id = harness.await_leader().await.unwrap();

        // Disable all directed proxies TO the old leader and FROM it.
        let n = 3usize;
        let leader_idx = (leader_id - 1) as usize;
        for peer in 0..n {
            if peer == leader_idx {
                continue;
            }
            // leader → peer
            harness
                .toxiproxy()
                .disable_proxy(&RaftClusterHarness::proxy_name(leader_idx, peer))
                .unwrap();
            // peer → leader
            harness
                .toxiproxy()
                .disable_proxy(&RaftClusterHarness::proxy_name(peer, leader_idx))
                .unwrap();
        }

        // A new leader must be elected on the remaining quorum.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let new_leader = harness.await_leader().await.unwrap();
        assert_ne!(
            new_leader, leader_id,
            "a different node must become leader after isolation"
        );

        // No-op proposal on new leader must succeed.
        harness.propose_noop(new_leader).await.unwrap();

        // Re-enable all proxies.
        harness.toxiproxy().reset().unwrap();
        harness.shutdown().await;
    }

    /// Isolating a single follower (minority) must not prevent the leader from
    /// committing proposals. After healing, all nodes converge to the same
    /// last_applied index.
    #[tokio::test]
    async fn minority_partition_no_divergence() {
        let Some(harness) = RaftClusterHarness::try_start(3).await else {
            return;
        };
        let leader_id = harness.await_leader().await.unwrap();

        // Pick a follower (node_id ∈ {1,2,3}, different from leader).
        let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
        let follower_idx = (follower_id - 1) as usize;
        let leader_idx = (leader_id - 1) as usize;

        // Sever all links to/from the follower.
        let n = 3usize;
        for peer in 0..n {
            if peer == follower_idx {
                continue;
            }
            harness
                .toxiproxy()
                .disable_proxy(&RaftClusterHarness::proxy_name(follower_idx, peer))
                .unwrap();
            harness
                .toxiproxy()
                .disable_proxy(&RaftClusterHarness::proxy_name(peer, follower_idx))
                .unwrap();
        }

        // Leader can still commit with quorum (leader + one non-isolated node).
        for _ in 0..5 {
            harness.propose_noop(leader_id).await.unwrap();
        }
        let leader_applied = harness.last_applied(leader_id).unwrap_or(0);

        // Heal the follower.
        harness.toxiproxy().reset().unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;

        // All nodes must reach at least the same last_applied.
        let follower_applied = harness.last_applied(follower_id).unwrap_or(0);
        assert!(
            follower_applied >= leader_applied,
            "follower last_applied {follower_applied} must catch up to leader {leader_applied}"
        );

        // Sanity: the other node also converged.
        let other_id = (1u64..=3)
            .find(|&id| id != leader_id && id != follower_id)
            .unwrap();
        let other_applied = harness.last_applied(other_id).unwrap_or(0);
        assert_eq!(
            other_applied, leader_applied,
            "non-isolated node must match leader"
        );

        let _ = leader_idx; // used for proxy_name above
        harness.shutdown().await;
    }

    /// Add 50 ms latency on all inbound links for a follower, commit traffic on
    /// the leader, then remove the latency and verify the follower catches up.
    #[tokio::test]
    async fn lagging_follower_catches_up() {
        let Some(harness) = RaftClusterHarness::try_start(3).await else {
            return;
        };
        let leader_id = harness.await_leader().await.unwrap();

        let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
        let follower_idx = (follower_id - 1) as usize;

        // Add 200 ms latency on all links inbound to the follower so it lags.
        let n = 3usize;
        for peer in 0..n {
            if peer == follower_idx {
                continue;
            }
            harness
                .toxiproxy()
                .add_latency(&RaftClusterHarness::proxy_name(peer, follower_idx), 200)
                .unwrap();
        }

        // Commit traffic on the leader.
        for _ in 0..10 {
            harness.propose_noop(leader_id).await.unwrap();
        }
        let leader_applied = harness.last_applied(leader_id).unwrap_or(0);

        // Remove latency.
        harness.toxiproxy().reset().unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;

        let follower_applied = harness.last_applied(follower_id).unwrap_or(0);
        assert!(
            follower_applied >= leader_applied,
            "follower must catch up after latency removed: follower={follower_applied} leader={leader_applied}"
        );

        harness.shutdown().await;
    }

    /// With a healthy 3-node cluster, proposing no-ops via the leader advances
    /// last_applied on all nodes (basic liveness check).
    #[tokio::test]
    async fn cluster_commits_advance_all_nodes() {
        let Some(harness) = RaftClusterHarness::try_start(3).await else {
            return;
        };
        let leader_id = harness.await_leader().await.unwrap();

        for _ in 0..10 {
            harness.propose_noop(leader_id).await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_applied = harness.last_applied(leader_id).unwrap_or(0);
        assert!(
            leader_applied >= 10,
            "leader must have applied at least 10 entries"
        );

        // All nodes must have replicated.
        for node_id in 1u64..=3 {
            let applied = harness.last_applied(node_id).unwrap_or(0);
            assert!(
                applied >= leader_applied,
                "node {node_id} applied={applied} must reach leader={leader_applied}"
            );
        }

        harness.shutdown().await;
    }

    /// TCP RST injection between two nodes does not cause divergence — the
    /// cluster remains available and logs converge after the fault clears.
    #[tokio::test]
    async fn tcp_rst_does_not_cause_divergence() {
        let Some(harness) = RaftClusterHarness::try_start(3).await else {
            return;
        };
        let leader_id = harness.await_leader().await.unwrap();

        // Inject TCP RST on the leader→follower direction.
        let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
        let leader_idx = (leader_id - 1) as usize;
        let follower_idx = (follower_id - 1) as usize;
        harness
            .toxiproxy()
            .add_reset_peer(
                &RaftClusterHarness::proxy_name(leader_idx, follower_idx),
                100,
            )
            .unwrap();

        // Leader can still commit with 2 of 3 nodes.
        for _ in 0..5 {
            harness.propose_noop(leader_id).await.unwrap();
        }
        let leader_applied = harness.last_applied(leader_id).unwrap_or(0);

        // Remove fault and let cluster heal.
        harness.toxiproxy().reset().unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Follower must eventually catch up.
        let follower_applied = harness.last_applied(follower_id).unwrap_or(0);
        assert!(
            follower_applied >= leader_applied,
            "follower must converge after RST clears: follower={follower_applied} leader={leader_applied}"
        );

        harness.shutdown().await;
    }
}
