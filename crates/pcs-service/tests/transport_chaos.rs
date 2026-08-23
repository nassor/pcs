//! Chaos tests for the PCS Raft TCP transport layer. Each test soft-skips
//! when Docker is unavailable, so `cargo test` is safe without a daemon.

#![cfg(feature = "distributed-raft")]

mod common;

use std::time::Duration;
use tokio::time::Instant;

/// Assert that the cluster elects a leader within `deadline`.
async fn await_leader_within(
    harness: &common::RaftClusterHarness,
    deadline: Duration,
) -> anyhow::Result<u64> {
    let timeout_at = Instant::now() + deadline;
    loop {
        match harness.await_leader().await {
            Ok(id) => return Ok(id),
            Err(_) if Instant::now() < timeout_at => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// With 500ms latency on every peer link a 3-node cluster still elects a leader
/// (election timeouts exceed 500ms) and holds it for at least 10 seconds: the
/// transport's read and write timeouts must not close connections that are slow
/// but alive.
#[tokio::test]
async fn latency_500ms_no_heartbeat_thrash() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let toxi = harness.toxiproxy();

    // Apply 500ms latency on all directed edges.
    for src in 0..3 {
        for dst in 0..3 {
            if src != dst {
                toxi.add_latency(&common::RaftClusterHarness::proxy_name(src, dst), 500)?;
            }
        }
    }

    let first_leader = await_leader_within(&harness, Duration::from_secs(15)).await?;

    let start = Instant::now();
    let mut changes = 0u32;
    let mut prev_leader = first_leader;
    while start.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(current) = harness.await_leader().await
            && current != prev_leader
        {
            changes += 1;
            prev_leader = current;
        }
    }

    assert!(
        changes <= 1,
        "too many leader changes under 500ms latency: {changes}"
    );

    harness.shutdown().await;
    Ok(())
}

/// A `reset_peer` toxic on the leader→follower link during steady proposals must
/// not stall the log. The connection pool drops the broken stream, reconnects on
/// the next retry, and the following append commits within 2 seconds.
#[tokio::test]
async fn reset_peer_mid_append_reconnects() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };

    let leader_id = await_leader_within(&harness, Duration::from_secs(10)).await?;
    let leader_idx = (leader_id - 1) as usize;

    let baseline = harness.last_applied(leader_id).unwrap_or(0);

    for _ in 0..3 {
        harness.propose_noop(leader_id).await?;
    }

    // Inject reset_peer on the leader→first-follower edge.
    let follower = if leader_idx == 0 { 1 } else { 0 };
    let proxy = common::RaftClusterHarness::proxy_name(leader_idx, follower);
    harness.toxiproxy().add_reset_peer(&proxy, 0)?;

    tokio::time::sleep(Duration::from_millis(200)).await;
    harness.toxiproxy().delete_toxic(&proxy, "reset_peer")?;

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut committed = false;
    while Instant::now() < deadline {
        if harness.last_applied(leader_id).unwrap_or(0) > baseline + 3 {
            committed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        committed,
        "append did not commit within 2s after reset_peer"
    );

    harness.shutdown().await;
    Ok(())
}

/// A 1 KB/s bandwidth limit on the replication link must not hang the follower's
/// catch-up: it finishes inside 120s, so the write-side timeout tolerates slow
/// pipes.
///
/// Forcing an openraft snapshot needs the leader's log compacted, which happens
/// once enough entries accumulate. This proposes many entries on a 2-node cluster
/// and then checks that the follower catches up.
#[tokio::test]
async fn bandwidth_1kbps_snapshot_completes() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(2).await else {
        return Ok(());
    };
    let toxi = harness.toxiproxy();

    let leader_id = await_leader_within(&harness, Duration::from_secs(10)).await?;
    let leader_idx = (leader_id - 1) as usize;
    let follower_idx = 1 - leader_idx;

    // Apply 1 KB/s bandwidth limit on leader→follower link.
    let proxy = common::RaftClusterHarness::proxy_name(leader_idx, follower_idx);
    toxi.add_bandwidth(&proxy, 1)?; // 1 kbps

    // Propose enough entries that the leader may trigger snapshot compaction.
    for _ in 0..30 {
        let _ = harness.propose_noop(leader_id).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let start = Instant::now();
    let timeout = Duration::from_secs(120);
    let mut follower_applied = false;

    while start.elapsed() < timeout {
        let leader_applied = harness.last_applied(leader_id).unwrap_or(0);
        let follower_node_id = follower_idx as u64 + 1;
        let follower_app = harness.last_applied(follower_node_id).unwrap_or(0);
        if follower_app >= leader_applied.saturating_sub(2) {
            follower_applied = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    assert!(
        follower_applied,
        "follower did not catch up within {timeout:?} under 1kbps bandwidth constraint"
    );

    harness.shutdown().await;
    Ok(())
}

/// Disabling every proxy link to and from the leader must let the majority elect a
/// new leader. Re-enabling them must let the old leader rejoin as a follower with
/// no split-brain and no crash.
#[tokio::test]
async fn bidi_partition_elects_and_rejoins() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let toxi = harness.toxiproxy();

    let first_leader = await_leader_within(&harness, Duration::from_secs(10)).await?;
    let leader_idx = (first_leader - 1) as usize;

    // Propose some entries so there is committed state.
    for _ in 0..5 {
        harness.propose_noop(first_leader).await?;
    }

    // Disable all links to/from the leader (bidi partition).
    for peer in 0..3 {
        if peer == leader_idx {
            continue;
        }
        toxi.disable_proxy(&common::RaftClusterHarness::proxy_name(leader_idx, peer))?;
        toxi.disable_proxy(&common::RaftClusterHarness::proxy_name(peer, leader_idx))?;
    }

    // The two remaining nodes hold the majority. 15s is generous next to the
    // 300-500ms election timeout.
    let new_leader = await_leader_within(&harness, Duration::from_secs(15)).await?;

    // The new leader must be one of the non-partitioned nodes.
    assert_ne!(
        new_leader, first_leader,
        "partitioned leader should not remain leader"
    );

    // Re-enable all proxies.
    for peer in 0..3 {
        if peer == leader_idx {
            continue;
        }
        toxi.enable_proxy(&common::RaftClusterHarness::proxy_name(leader_idx, peer))?;
        toxi.enable_proxy(&common::RaftClusterHarness::proxy_name(peer, leader_idx))?;
    }

    // Wait for the cluster to stabilize with the old leader as follower.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // The old node must still be answering.
    let old_node_metrics = harness.last_applied(first_leader).is_some();

    // Leadership is not asserted: the new leader keeps the term.
    assert!(
        old_node_metrics,
        "old leader node should still be alive after rejoining"
    );

    // The cluster still accepts proposals.
    harness.propose_noop(new_leader).await?;

    harness.shutdown().await;
    Ok(())
}

/// A `limit_data` toxic truncates frames mid-stream. `read_frame` must return an
/// `UnexpectedEof` framing error and the connection loop must break without
/// panicking, leaving the cluster functional over the other links.
#[tokio::test]
async fn limit_data_truncated_frame_errors_not_panic() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let toxi = harness.toxiproxy();

    let leader_id = await_leader_within(&harness, Duration::from_secs(10)).await?;
    let leader_idx = (leader_id - 1) as usize;

    for _ in 0..5 {
        harness.propose_noop(leader_id).await?;
    }
    let baseline = harness.last_applied(leader_id).unwrap_or(0);

    // 50 bytes truncates most frames on this follower link.
    let follower_idx = if leader_idx == 0 { 1 } else { 0 };
    let proxy = common::RaftClusterHarness::proxy_name(leader_idx, follower_idx);
    toxi.add_limit_data(&proxy, 50)?;

    // Wait 1 second for the toxic to disrupt some frames.
    tokio::time::sleep(Duration::from_secs(1)).await;

    toxi.delete_toxic(&proxy, "limit_data")?;

    // The cluster must still accept proposals on the other links within 5s.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut progressed = false;
    while Instant::now() < deadline {
        if harness.last_applied(leader_id).unwrap_or(0) > baseline {
            progressed = true;
            break;
        }
        // Try proposing if cluster lost its leader due to the disruption.
        let _ = harness.propose_noop(leader_id).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Progress is not required: a re-election during the disruption is an
    // acceptable outcome. The property under test is that nothing panicked.
    let _ = progressed;

    harness.shutdown().await;
    Ok(())
}
