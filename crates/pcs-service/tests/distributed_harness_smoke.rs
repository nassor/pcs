//! Smoke test for the multi-node Raft chaos harness.
//!
//! Soft-skips when Docker is unavailable. Run explicitly with:
//!
//! ```text
//! cargo test --features distributed-raft --test distributed_harness_smoke
//! ```

#[cfg(feature = "distributed-raft")]
mod common;

#[cfg(feature = "distributed-raft")]
#[tokio::test(flavor = "multi_thread")]
async fn cluster_forms_and_applies_noop() {
    use common::RaftClusterHarness;
    use std::time::Duration;

    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return;
    };

    let leader: u64 = harness
        .await_leader()
        .await
        .expect("leader should be elected within 10 s");

    harness
        .propose_noop(leader)
        .await
        .expect("noop proposal should succeed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    for node_id in 1..=3_u64 {
        let applied = harness.last_applied(node_id);
        assert!(
            applied.is_some() && applied.unwrap() >= 1,
            "node {node_id} should have applied at least 1 entry, got {applied:?}"
        );
    }

    harness.shutdown().await;
}
