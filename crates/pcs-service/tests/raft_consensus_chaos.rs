//! Raft consensus fault-injection and chaos tests.
//!
//! Docker-gated throughout: every test drives a real multi-node cluster
//! through the `RaftClusterHarness` in `tests/common` and soft-skips when no
//! daemon is reachable.
//!
//! The PCS raft carries membership and leadership only, so the invariants under
//! test are election safety and log convergence: a partitioned majority elects,
//! a minority does not, a lagging follower catches up, and no node keeps a
//! divergent log after a fault heals.

#![cfg(feature = "distributed-raft")]

mod common;

use common::RaftClusterHarness;
use std::time::Duration;

/// Sever every link into and out of `node_idx` (0-indexed).
fn isolate(harness: &RaftClusterHarness, node_idx: usize) -> anyhow::Result<()> {
    for peer in 0..harness.node_count() {
        if peer == node_idx {
            continue;
        }
        harness
            .toxiproxy()
            .disable_proxy(&RaftClusterHarness::proxy_name(node_idx, peer))?;
        harness
            .toxiproxy()
            .disable_proxy(&RaftClusterHarness::proxy_name(peer, node_idx))?;
    }
    Ok(())
}

/// After cutting all inbound and outbound links for the leader, the remaining
/// two nodes must elect a new leader on a higher term.
#[tokio::test]
async fn isolate_leader_elects_new() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;
    let term_before = harness.max_term();

    isolate(&harness, (leader_id - 1) as usize)?;

    // A new leader must be elected on the remaining quorum.
    // The isolated node keeps reporting itself leader, and `await_leader`
    // returns whichever node answers first, so poll for a *different* leader
    // and tolerate windows with none at all.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let new_leader = loop {
        if let Ok(candidate) = harness.await_leader().await
            && candidate != leader_id
        {
            break candidate;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "a different node must become leader after isolation: {}",
            harness.diagnostics()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_ne!(new_leader, leader_id);
    assert!(
        harness.max_term() > term_before,
        "a new election must advance the term past {term_before}"
    );

    // Heal, then the old leader must adopt the new leader's log.
    harness.toxiproxy().reset()?;
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}

/// Isolating a single follower (minority) must not cost the leader its term:
/// the remaining quorum keeps it in office, and the follower reconverges after
/// healing.
#[tokio::test]
async fn minority_partition_no_divergence() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;
    harness
        .await_applied_at_least(leader_id, 1, Duration::from_secs(10))
        .await?;
    let term_before = harness.max_term();

    // Pick a follower (node_id ∈ {1,2,3}, different from leader).
    let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
    isolate(&harness, (follower_id - 1) as usize)?;

    // Two of three nodes still form a quorum, so leadership must not move.
    // The isolated follower campaigns while cut off and raises its own term,
    // so `max_term` across all nodes is expected to climb; what must hold is
    // that the original leader is still the leader.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        harness.await_leader().await?,
        leader_id,
        "a minority partition must not unseat the leader: {}",
        harness.diagnostics()
    );
    let _ = term_before;

    // Heal the follower; every node must agree on one applied index.
    harness.toxiproxy().reset()?;
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}

/// 200 ms of latency on every link inbound to one follower must not desync it:
/// once the latency clears, its applied index matches the rest of the cluster.
#[tokio::test]
async fn lagging_follower_catches_up() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;

    let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
    let follower_idx = (follower_id - 1) as usize;

    // Add 200 ms latency on all links inbound to the follower so it lags.
    for peer in 0..3 {
        if peer == follower_idx {
            continue;
        }
        harness
            .toxiproxy()
            .add_latency(&RaftClusterHarness::proxy_name(peer, follower_idx), 200)?;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    harness.toxiproxy().reset()?;
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}

/// A healthy 3-node cluster settles: one leader, a non-zero applied index on
/// every node, and the same index everywhere.
#[tokio::test]
async fn healthy_cluster_converges_on_one_leader() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;

    // `await_convergence` is the all-nodes-agree assertion: it only returns
    // once every node reports the same applied index. Re-reading the per-node
    // indices afterwards would race a further commit, so the returned snapshot
    // is the assertion.
    let converged = harness.await_convergence(Duration::from_secs(20)).await?;
    assert!(
        converged >= 1,
        "the leader's own term entry must be applied everywhere, got {converged}"
    );
    assert!(
        (1..=3).contains(&leader_id),
        "the elected leader must be one of the three nodes, got {leader_id}"
    );

    harness.shutdown().await;
    Ok(())
}

/// TCP RST injection between two nodes must not cause divergence: the cluster
/// stays available and the logs converge once the fault clears.
#[tokio::test]
async fn tcp_rst_does_not_cause_divergence() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;

    // Inject TCP RST on the leader→follower direction.
    let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
    let leader_idx = (leader_id - 1) as usize;
    let follower_idx = (follower_id - 1) as usize;
    harness.toxiproxy().add_reset_peer(
        &RaftClusterHarness::proxy_name(leader_idx, follower_idx),
        100,
    )?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Remove fault and let the cluster heal.
    harness.toxiproxy().reset()?;
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}
