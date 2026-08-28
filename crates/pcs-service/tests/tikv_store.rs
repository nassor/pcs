//! Integration tests for the TiKV-backed shared store and state client.
//!
//! Each test opens with the Docker soft-skip pattern: no daemon → printed
//! `SKIP:` and early return; containers up but the store unreachable → a real
//! failure.

#![cfg(feature = "tikv-store")]

use std::time::Duration;

use pcs_service::PcsResult;
use pcs_service::distributed::SharedStore;
use pcs_service::distributed::checkpoint::CheckpointStore;
use pcs_service::distributed::partition::PartitionSource;
use pcs_service::distributed::tikv_store::{TIKV_ROWS_PER_RANGE, TikvSharedStore, TikvStoreConfig};
use pcs_service::service::tikv_state::{SourceCursorMeta, TikvStateClient};
use uuid::Uuid;

#[path = "common/tikv.rs"]
mod tikv;

fn store_config(fx: &tikv::TikvFixture, lease_ttl_millis: u64) -> TikvStoreConfig {
    TikvStoreConfig {
        pd_endpoints: vec![fx.pd_endpoint.clone()],
        key_prefix: fx.prefix("pcs"),
        timeout: Duration::from_secs(10),
        lease_ttl_millis,
    }
}

/// A master batch split into two claimable row ranges (0..512 and 512..1024).
async fn register_two_range_batch(store: &TikvSharedStore, batch_id: u64) -> PcsResult<()> {
    store
        .register_master_batch(
            batch_id,
            "orders".to_string(),
            1,
            vec![0u8; 64],
            TIKV_ROWS_PER_RANGE * 2,
        )
        .await
}

#[tokio::test]
async fn config_and_cursor_roundtrip() {
    let Some(fx) = tikv::try_start().await else {
        return;
    };

    let client = TikvStateClient::connect(&store_config(&fx, 90_000))
        .await
        .expect("connect");
    let workflow = "wf-roundtrip";
    let node = "proc-1";
    let source = "src-1";

    // Config bytes come back identical.
    let config_bytes = b"mode \"standalone\"\nnode id=1 data_dir=\"/tmp/pcs\"\n";
    client
        .put_config("pcs.kdl", config_bytes)
        .await
        .expect("put config");

    // Prior save/load/delete.
    assert_eq!(
        client.load_prior(workflow, node).await.expect("load prior"),
        None,
        "no prior before the first save"
    );
    client
        .save_prior(workflow, node, b"state-blob")
        .await
        .expect("save prior");
    assert_eq!(
        client
            .load_prior(workflow, node)
            .await
            .expect("load prior")
            .as_deref(),
        Some(&b"state-blob"[..])
    );
    client
        .delete_prior(workflow, node)
        .await
        .expect("delete prior");
    assert_eq!(
        client
            .load_prior(workflow, node)
            .await
            .expect("load prior after delete"),
        None
    );

    // Cursor save/load.
    let meta = SourceCursorMeta {
        items_processed: 42,
        last_batch_at_ms: 1_700_000_000_000,
    };
    assert_eq!(
        client
            .load_source_cursor(workflow, source)
            .await
            .expect("load cursor"),
        None
    );
    client
        .save_source_cursor(workflow, source, meta)
        .await
        .expect("save cursor");
    assert_eq!(
        client
            .load_source_cursor(workflow, source)
            .await
            .expect("load cursor after save"),
        Some(meta)
    );
}

#[tokio::test]
async fn store_claim_checkpoint_ack_flow() {
    let Some(fx) = tikv::try_start().await else {
        return;
    };

    let store = TikvSharedStore::connect(&store_config(&fx, 90_000))
        .await
        .expect("connect");
    register_two_range_batch(&store, 1).await.expect("register");

    let instance = Uuid::now_v7();

    // First claim: range 0..512 with the documented lease math.
    let claim = store
        .claim_next_batch(instance)
        .await
        .expect("claim")
        .expect("one pending range");
    assert_eq!(claim.batch_id, 1);
    assert_eq!(claim.component, "orders");
    assert_eq!(claim.row_range, 0..TIKV_ROWS_PER_RANGE);
    assert_eq!(claim.schema_id, 1);
    assert_eq!(claim.instance_id, instance);
    assert_eq!(claim.lease_ttl_millis, 90_000);
    assert!(
        claim.lease_expires_at > 0,
        "lease expiry must be stamped in the future"
    );

    // Checkpoint round-trip; the schema-id ledger updates on data stages.
    assert_eq!(store.persisted_schema_id().await.expect("schema id"), None);
    let payload = vec![0xAB; 128];
    store
        .save_checkpoint(claim.claim_id, 3, payload.clone(), 7)
        .await
        .expect("save checkpoint");
    let cp = store
        .load_checkpoint(claim.claim_id, 3)
        .await
        .expect("load checkpoint")
        .expect("checkpoint exists");
    assert_eq!(cp.payload, payload);
    assert_eq!(cp.schema_id, 7);
    assert_eq!(
        store.persisted_schema_id().await.expect("schema id"),
        Some(7)
    );

    // Renewal extends the expiry.
    let renewed = store
        .renew_claim(claim.claim_id, instance)
        .await
        .expect("renew");
    assert!(
        renewed >= claim.lease_expires_at,
        "renewal must not move the lease backward"
    );

    // Ack; the next claim returns the other range.
    store
        .ack_claim(claim.claim_id, instance)
        .await
        .expect("ack");
    let claim2 = store
        .claim_next_batch(instance)
        .await
        .expect("claim 2")
        .expect("second pending range");
    assert_eq!(
        claim2.row_range,
        TIKV_ROWS_PER_RANGE..TIKV_ROWS_PER_RANGE * 2
    );
    assert_ne!(claim2.claim_id, claim.claim_id);
    store
        .ack_claim(claim2.claim_id, instance)
        .await
        .expect("ack 2");

    // Everything acked: nothing left to claim.
    assert!(
        store
            .claim_next_batch(instance)
            .await
            .expect("claim")
            .is_none()
    );
}

#[tokio::test]
async fn restart_resumes_from_completed_ranges() {
    let Some(fx) = tikv::try_start().await else {
        return;
    };

    let config = store_config(&fx, 90_000);
    let store = TikvSharedStore::connect(&config).await.expect("connect");
    register_two_range_batch(&store, 1).await.expect("register");

    let instance = Uuid::now_v7();
    let claim = store
        .claim_next_batch(instance)
        .await
        .expect("claim")
        .expect("range 1");
    assert_eq!(claim.row_range, 0..TIKV_ROWS_PER_RANGE);
    store
        .ack_claim(claim.claim_id, instance)
        .await
        .expect("ack range 1");

    // A "restart": a fresh store over the same prefix.
    drop(store);
    let store2 = TikvSharedStore::connect(&config).await.expect("reconnect");
    let resumed = store2
        .claim_next_batch(instance)
        .await
        .expect("claim after restart")
        .expect("range 2 still pending");
    assert_eq!(
        resumed.row_range,
        TIKV_ROWS_PER_RANGE..TIKV_ROWS_PER_RANGE * 2,
        "a restarted service must never re-claim a completed range"
    );
}

#[tokio::test]
async fn concurrent_claims_are_exclusive() {
    let Some(fx) = tikv::try_start().await else {
        return;
    };

    let config = store_config(&fx, 90_000);
    let store = TikvSharedStore::connect(&config).await.expect("connect");
    store
        .register_master_batch(
            1,
            "orders".to_string(),
            1,
            vec![0u8; 64],
            TIKV_ROWS_PER_RANGE,
        )
        .await
        .expect("register");

    let store_a = TikvSharedStore::connect(&config).await.expect("connect a");
    let store_b = TikvSharedStore::connect(&config).await.expect("connect b");
    let (a, b) = tokio::join!(
        store_a.claim_next_batch(Uuid::now_v7()),
        store_b.claim_next_batch(Uuid::now_v7()),
    );
    let a = a.expect("a claim");
    let b = b.expect("b claim");
    match (a, b) {
        (Some(_), None) | (None, Some(_)) => {}
        (Some(a), Some(b)) => panic!("both clients claimed the same range: {a:?} vs {b:?}"),
        (None, None) => panic!("neither client claimed the pending range"),
    }
}

#[tokio::test]
async fn reclaim_expired_frees_claim() {
    let Some(fx) = tikv::try_start().await else {
        return;
    };

    let store = TikvSharedStore::connect(&store_config(&fx, 1_000))
        .await
        .expect("connect");
    store
        .register_master_batch(
            1,
            "orders".to_string(),
            1,
            vec![0u8; 64],
            TIKV_ROWS_PER_RANGE,
        )
        .await
        .expect("register");

    let claim = store
        .claim_next_batch(Uuid::now_v7())
        .await
        .expect("claim")
        .expect("pending range");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64;

    // Lease is 1 s; after 2 s it has expired and must be reclaimed.
    assert_eq!(
        store.reclaim_expired(now + 2_000).await.expect("reclaim"),
        1,
        "the expired claim must be freed"
    );
    let _ = claim;
    let re_claimed = store
        .claim_next_batch(Uuid::now_v7())
        .await
        .expect("claim after reclaim")
        .expect("range claimable again");
    assert_eq!(re_claimed.row_range, 0..TIKV_ROWS_PER_RANGE);
}

#[tokio::test]
async fn renew_after_expiry_fails() {
    let Some(fx) = tikv::try_start().await else {
        return;
    };

    let store = TikvSharedStore::connect(&store_config(&fx, 1_000))
        .await
        .expect("connect");
    store
        .register_master_batch(
            1,
            "orders".to_string(),
            1,
            vec![0u8; 64],
            TIKV_ROWS_PER_RANGE,
        )
        .await
        .expect("register");

    let instance = Uuid::now_v7();
    let claim = store
        .claim_next_batch(instance)
        .await
        .expect("claim")
        .expect("pending range");

    // Wait out the 1 s lease (plus a margin), then renew: the lease contract
    // says a lost claim must surface as an error, not a silent success.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let err = store
        .renew_claim(claim.claim_id, instance)
        .await
        .expect_err("renewing an expired claim must fail");
    assert!(
        err.to_string().contains("claim") || err.to_string().contains("lease"),
        "error should name the claim/lease: {err}"
    );
}

/// The accumulator chain-carry fix over a TiKV-backed runner: claim 2 must
/// start from claim 1's accumulator rows, and both claims leave their own
/// checkpoint in the store.
#[cfg(feature = "windows")]
#[tokio::test]
async fn runner_window_accumulator_carries_across_claims() {
    use std::sync::Arc as StdArc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use pcs_core::SystemMeta;
    use pcs_core::component::Component as _;
    use pcs_core::dataset::Dataset;
    use pcs_core::pipeline::Pipeline;
    use pcs_core::system::system_fn;
    use pcs_core::windows::accumulator::WindowAccumulator;
    use pcs_service::distributed::runner::{DistributedRunner, RunnerConfig};
    use pcs_service::distributed::strategy::CheckpointStrategy;

    let Some(fx) = tikv::try_start().await else {
        return;
    };

    let store = TikvSharedStore::connect(&store_config(&fx, 90_000))
        .await
        .expect("connect");

    // Two batches → two claims; only the chained accumulator carries rows
    // from the first claim to the second.
    for batch_id in 0u64..2 {
        store
            .register_master_batch(batch_id, "test".to_string(), 1, vec![0u8; 64], 10)
            .await
            .expect("register");
    }
    let store: Box<dyn SharedStore> = Box::new(store);

    let run_count = StdArc::new(AtomicU32::new(0));
    let entry_rows = StdArc::new(StdMutex::new(Vec::<u64>::new()));
    let run_count_clone = StdArc::clone(&run_count);
    let entry_rows_clone = StdArc::clone(&entry_rows);

    let mut pipeline = Pipeline::new("test");
    pipeline.add_system(system_fn(
        SystemMeta::new("append_accumulator"),
        move |data: &mut Dataset| {
            entry_rows_clone.lock().unwrap().push(data.rows() as u64);
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
    let processed = runner.run(world_factory).await.expect("run");
    assert_eq!(processed, 2);
    assert_eq!(run_count.load(Ordering::Relaxed), 2);
    assert_eq!(
        entry_rows.lock().unwrap().as_slice(),
        &[0, 1],
        "the second claim must start from the first claim's accumulator rows"
    );
}
