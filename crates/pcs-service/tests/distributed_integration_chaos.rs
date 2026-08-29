//! Full-stack chaos test for the Raft consensus layer under combined fault
//! injection. Requires Docker and Toxiproxy, and runs for about 70 seconds.
//!
//! ```bash
//! cargo test --features distributed-raft \
//!     --test distributed_integration_chaos -- --ignored --nocapture
//! ```
//!
//! - 5-node Raft cluster with all TCP edges proxied through Toxiproxy.
//! - 60-second chaos monkey randomly injecting latency, bandwidth, reset-peer,
//!   and full partition faults on random edges.
//! - After the chaos window + 10s settle:
//!   - The applied index converges to the same value on all 5 nodes, so no node
//!     kept a divergent log.
//!   - At least one leader change occurred during chaos (liveness under faults).
//!   - The cluster still elects a leader after healing.

#[cfg(feature = "distributed-raft")]
mod common;

#[cfg(feature = "distributed-raft")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spins up a 5-node Raft cluster behind Toxiproxy and chaos-injects faults for ~70-100s"]
async fn full_stack_chaos_monkey_60s() {
    use std::time::Duration;

    use common::RaftClusterHarness;
    use rand::RngExt;

    let Some(harness) = RaftClusterHarness::try_start(5).await else {
        return;
    };

    harness
        .await_leader()
        .await
        .expect("leader should be elected before chaos");

    const N_NODES: usize = 5;
    const CHAOS_DURATION: Duration = Duration::from_secs(60);

    // Capture term before chaos to detect leader elections later.
    let term_before: u64 = harness.max_term();

    let api_port = harness.toxiproxy().api_port;
    {
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

            let sleep_ms = rng.random_range(200u64..=2000);
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }

        // After the chaos window: reset all proxies and let the cluster settle.
        let _ = toxi.reset();
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    // Replication is deterministic, so every node must land on the same applied
    // index. A node that kept a divergent log would never match.
    harness
        .await_convergence(Duration::from_secs(60))
        .await
        .expect("all 5 nodes must converge to the same applied index");

    // At least one leader election happened during chaos: liveness under faults.
    let term_after: u64 = harness.max_term();
    assert!(
        term_after > term_before,
        "Raft term must have advanced during 60s of chaos (before={term_before}, after={term_after})"
    );

    // The healed cluster is still electable.
    harness
        .await_leader()
        .await
        .expect("a healed cluster must still hold a leader");

    harness.shutdown().await;
}
