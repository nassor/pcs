//! Arrow-IPC distributed scheduler: single-node batch processing with checkpoint.
//!
//! Shows how to register master batches with [`RedbSharedStore`], then run
//! a [`DistributedRunner`] to process them with a [`Pipeline`].
//!
//! The runner claims a row-range from the store, executes the pipeline on a
//! freshly-constructed [`Dataset`], checkpoints the resulting IPC bytes, and
//! acks the claim. Crash recovery would resume from the checkpoint.
//!
//! This example uses single-node mode (no Raft). Replace with a multi-node
//! [`RedbSharedStore`] plus [`ArrowRaftDriver`] for production.
//!
//! ## Running
//!
//! ```bash
//! cargo run --example distributed_scheduler --features distributed
//! ```

use std::sync::Arc;

use arrow_array::{Float64Array, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use pcs_service::PcsError;
use pcs_service::component::Component;
use pcs_service::distributed::RedbSharedStore;
use pcs_service::distributed::runner::{DistributedRunner, RunnerConfig};
use pcs_service::distributed::strategy::CheckpointStrategy;
use pcs_service::pipeline::{Dataset, Pipeline};
use pcs_service::system::{SystemMeta, system_fn};

// ---------------------------------------------------------------------------
// Component: Order
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Helper: serialise a small Order RecordBatch as Arrow IPC bytes
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), PcsError> {
    println!("=== Arrow Distributed Scheduler (single-node) ===\n");

    // 1. Open an in-process redb store (tempfile cleaned up on exit).
    let db_path = std::env::temp_dir().join(format!(
        "pcs_arrow_distributed_example_{}.redb",
        uuid::Uuid::new_v4()
    ));
    let store = RedbSharedStore::single_node(&db_path)?;

    // 2. Register a master batch.  In production this comes from your ingestion
    //    layer (e.g. a Parquet/S3 reader), not from a hand-rolled test helper.
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

    // 3. Build a pipeline with one system that summarises the loaded data.
    let mut pipeline = Pipeline::new("distributed-etl");

    // System: count alive rows and print a summary.
    pipeline.add_system(system_fn(
        SystemMeta::new("summarise"),
        |data: &mut Dataset| {
            let row_count = data.rows();
            println!("  [summarise] dataset has {} rows", row_count);
            Ok(())
        },
    ));

    println!("Pipeline: 1 system (stages computed on first run)");

    // 4. Configure the runner.
    let config = RunnerConfig {
        // Process at most 1 batch so the example terminates.
        max_batches: Some(1),
        // Write a checkpoint after every stage.
        checkpoint_strategy: CheckpointStrategy::EveryStage,
        schema_id: 1,
        ..Default::default()
    };

    println!("Instance: {}", config.instance_id);
    println!("Checkpoint strategy: {:?}\n", config.checkpoint_strategy);

    // 5. Run.  The world_factory is called fresh for each claimed batch.
    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner.run(Dataset::new).await?;

    // 6. Report.
    println!("\n=== Result ===");
    println!("Batches processed: {}", processed);
    println!("Checkpoint written: yes (EveryStage)");
    println!("At-least-once guarantee: claim was acked after successful run");

    // Clean up temp file.
    let _ = std::fs::remove_file(&db_path);

    Ok(())
}
