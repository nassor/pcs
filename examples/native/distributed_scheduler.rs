//! Arrow-IPC distributed scheduler: TiKV-backed batch processing with checkpoints.
//!
//! Master batches are registered with [`TikvSharedStore`]. A
//! [`DistributedRunner`] then claims a row-range, runs a [`Pipeline`] over a
//! fresh [`Dataset`], checkpoints the resulting IPC bytes, and acks the claim.
//! Recovery after a crash resumes from the checkpoint.
//!
//! Claims are atomic compare-and-swap transitions on TiKV keys, so several
//! instances pointed at the same `key_prefix` share one work pool.
//!
//! Needs the `tikv-store` feature and a reachable TiKV: PD at
//! `PCS_PD_ENDPOINTS` (default `127.0.0.1:2379`). The example exits with a
//! clear message when the store cannot be reached.
//!
//! ```bash
//! cargo run --example distributed_scheduler --features tikv-store
//! ```

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use pcs_service::PcsError;
use pcs_service::component::Component;
use pcs_service::dataset::Dataset;
use pcs_service::distributed::runner::{DistributedRunner, RunnerConfig};
use pcs_service::distributed::strategy::CheckpointStrategy;
use pcs_service::distributed::{TikvSharedStore, TikvStoreConfig};
use pcs_service::pipeline::Pipeline;
use pcs_service::system::{SystemMeta, system_fn};

/// A sales order with an amount and currency.
#[derive(Debug, Serialize, Deserialize)]
struct Order {
    order_id: u64,
    amount: f64,
    currency: String,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::UInt64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("currency", DataType::Utf8, false),
        ]))
    }
}

fn make_order_ipc() -> Vec<u8> {
    use arrow_array::RecordBatch;
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};

    let schema = Order::schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![100.0, 250.0, 75.5])),
            Arc::new(StringArray::from(vec!["USD", "EUR", "GBP"])),
        ],
    )
    .expect("build order batch");

    let options = IpcWriteOptions::default();
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, &schema, options).expect("ipc writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish ipc");
    }
    buf
}

/// Connection options, overridable through the environment so the example runs
/// against a local `tiup playground` or a Compose deployment unchanged.
fn store_config() -> TikvStoreConfig {
    let pd_endpoints = std::env::var("PCS_PD_ENDPOINTS")
        .unwrap_or_else(|_| "127.0.0.1:2379".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    TikvStoreConfig {
        pd_endpoints,
        // A unique prefix per run keeps repeated runs of the example from
        // claiming each other's batches.
        key_prefix: format!("pcs-example-{}", uuid::Uuid::now_v7().simple()),
        timeout: Duration::from_secs(10),
        lease_ttl_millis: 30_000,
    }
}

#[tokio::main]
async fn main() -> Result<(), PcsError> {
    println!("=== Arrow Distributed Scheduler (TiKV-backed) ===\n");

    let store_cfg = store_config();
    println!("PD endpoints: {:?}", store_cfg.pd_endpoints);
    println!("Key prefix:   {}\n", store_cfg.key_prefix);

    let store = TikvSharedStore::connect(&store_cfg).await.map_err(|e| {
        PcsError::configuration(format!(
            "cannot reach TiKV at {:?}: {e}\n\
             Start one (for example `tiup playground`) or set PCS_PD_ENDPOINTS.",
            store_cfg.pd_endpoints
        ))
    })?;

    // In production the master batch comes from an ingestion layer such as a
    // Parquet or S3 reader, not a hand-rolled helper.
    let ipc_bytes = make_order_ipc();
    println!(
        "Master batch IPC size: {} bytes ({} rows)",
        ipc_bytes.len(),
        3
    );

    store
        .register_master_batch(0, Order::name().to_string(), 1, ipc_bytes, 3)
        .await?;
    println!("Registered master batch 0 (3 rows)\n");

    let mut pipeline = Pipeline::new("distributed-etl");

    pipeline.add_system(system_fn(
        SystemMeta::new("summarise"),
        |data: &mut Dataset| {
            let row_count = data.rows();
            println!("  [summarise] dataset has {} rows", row_count);
            Ok(())
        },
    ));

    println!("Pipeline: 1 system (stages computed on first run)");

    let config = RunnerConfig {
        // One batch only, so the example terminates.
        max_batches: Some(1),
        checkpoint_strategy: CheckpointStrategy::EveryStage,
        schema_id: 1,
        ..Default::default()
    };

    println!("Instance: {}", config.instance_id);
    println!("Checkpoint strategy: {:?}\n", config.checkpoint_strategy);

    // The dataset factory runs fresh for each claimed batch.
    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner.run(Dataset::new).await?;

    println!("\n=== Result ===");
    println!("Batches processed: {}", processed);
    println!("Checkpoint written: yes (EveryStage)");
    println!("At-least-once guarantee: claim was acked after successful run");

    Ok(())
}
