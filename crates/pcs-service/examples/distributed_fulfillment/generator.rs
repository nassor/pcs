//! Synthetic order generator.
//!
//! [`run_generator`] runs as a background task on node 1 (the bootstrap node).
//! Every `interval` seconds it produces a batch of `Order` rows, serialises
//! them to Arrow IPC, and registers the batch via the cluster store.
//!
//! If the local node is not the Raft leader, `register_master_batch` returns
//! an error — the generator logs a warning and skips the round.  This is the
//! idiomatic at-most-once pattern: only the leader produces new work.

use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;

use pcs_service::Dataset;

use crate::components::Order;
use crate::store::FulfillmentStore;

// ── Constants ─────────────────────────────────────────────────────────────────

const CURRENCIES: &[&str] = &["USD", "EUR", "GBP", "JPY", "CAD"];
const REGIONS: &[&str] = &["us-east", "us-west", "eu-west", "ap-south"];

/// Arrow schema version — bump when Order schema changes.
pub const SCHEMA_ID: u32 = 1;

// ── Public entry point ────────────────────────────────────────────────────────

/// Background task: generate and register synthetic `Order` batches.
///
/// Runs indefinitely until the process is killed.  `node_id` is used only for
/// trace annotations.
pub async fn run_generator(store: Arc<FulfillmentStore>, node_id: u64, interval: Duration) {
    let mut batch_counter: u64 = 0;

    loop {
        let orders = generate_orders(300, 500);
        let row_count = orders.len() as u32;

        match serialise_orders(&orders) {
            Ok(ipc) => {
                match store
                    .register_batch(
                        batch_counter,
                        "Order".to_string(),
                        SCHEMA_ID,
                        ipc,
                        row_count,
                    )
                    .await
                {
                    Ok(()) => {
                        #[cfg(feature = "tracing")]
                        tracing::info!(
                            node_id,
                            batch_id = batch_counter,
                            rows = row_count,
                            "generator: registered batch"
                        );
                        batch_counter += 1;
                    }
                    Err(e) => {
                        // Not the leader or cluster unavailable — skip this round.
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            node_id,
                            batch_id = batch_counter,
                            error = %e,
                            "generator: skipping batch (not leader or cluster error)"
                        );
                    }
                }
            }
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(node_id, error = %e, "generator: failed to serialise orders");
            }
        }

        tokio::time::sleep(interval).await;
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Generate between `min_rows` and `max_rows` synthetic `Order` rows.
fn generate_orders(min_rows: usize, max_rows: usize) -> Vec<Order> {
    let mut rng = rand::rng();
    let count = rng.random_range(min_rows..=max_rows);
    (0..count).map(|_| random_order(&mut rng)).collect()
}

fn random_order(rng: &mut impl rand::Rng) -> Order {
    let product_idx = rng.random_range(1u32..=20u32);
    let product_id = format!("P{product_idx:03}");

    let customer_idx = rng.random_range(100u32..=999u32);
    let customer_id = format!("C{customer_idx}");

    let currency = CURRENCIES[rng.random_range(0..CURRENCIES.len())];
    let region = REGIONS[rng.random_range(0..REGIONS.len())];
    let quantity = rng.random_range(1i64..=10i64);
    let amount_original = rng.random_range(10.0f64..=5000.0f64);

    Order {
        id: uuid::Uuid::new_v4().to_string(),
        customer_id,
        product_id,
        quantity,
        amount_original,
        currency: currency.to_string(),
        amount_usd: 0.0, // filled by ConvertCurrencySystem
        region: region.to_string(),
        fraud_score: 0.0, // filled by DetectFraudSystem
        tax_rate: 0.0,    // filled by ComputeTaxSystem
        tax_amount: 0.0,  // filled by ComputeTaxSystem
        validation_status: "pending".to_string(),
        inventory_status: "pending".to_string(),
        processing_status: "pending".to_string(),
    }
}

/// Serialise a batch of `Order` rows to Arrow IPC bytes via a `Dataset`.
fn serialise_orders(orders: &[Order]) -> pcs_service::PcsResult<Vec<u8>> {
    let mut dataset = Dataset::new();
    dataset.register_component::<Order>()?;
    dataset.append::<Order>(orders)?;

    let mut buf = Vec::new();
    dataset.write_ipc(&mut buf)?;
    Ok(buf)
}
