//! DataFusion interop: SQL query results ingested into a PCS pipeline.
//!
//! Demonstrates using [`DataFusionSource`] to execute a SQL query against a
//! DataFusion [`SessionContext`] and stream the results into a PCS [`Dataset`]
//! for downstream processing by a [`Pipeline`].
//!
//! ## What this example shows
//!
//! 1. Create a DataFusion SessionContext.
//! 2. Register a MemTable with synthetic sales data.
//! 3. Run a SQL query (filter + projection) via DataFusionSource.
//! 4. Drain the source into a PCS Pipeline.
//! 5. Run a PCS pipeline that aggregates revenue.
//! 6. Print the result.
//!
//! ## Running
//!
//! ```bash
//! cargo run --example datafusion_interop --features datafusion
//! ```

use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use pcs_service::io::datafusion_source::DataFusionSource;
use pcs_service::io::source::{Source, drain_into_dataset};
use pcs_service::pipeline::Dataset;
use pcs_service::{PcsError, Pipeline, System, SystemMeta};

// ── Schema ────────────────────────────────────────────────────────────────────

fn sales_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("unit_price", DataType::Float64, false),
    ]))
}

// ── Synthetic data ────────────────────────────────────────────────────────────

fn make_sales_batch() -> arrow_array::RecordBatch {
    let schema = sales_schema();
    let product_ids: Arc<dyn arrow_array::Array> =
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
    let regions: Arc<dyn arrow_array::Array> = Arc::new(StringArray::from(vec![
        "north", "south", "north", "east", "west", "north", "south", "east", "west", "north",
    ]));
    let quantities: Arc<dyn arrow_array::Array> =
        Arc::new(Int32Array::from(vec![10, 5, 20, 8, 15, 12, 3, 7, 18, 9]));
    let unit_prices: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(vec![
        9.99, 24.99, 4.99, 49.99, 14.99, 9.99, 24.99, 49.99, 14.99, 9.99,
    ]));
    arrow_array::RecordBatch::try_new(schema, vec![product_ids, regions, quantities, unit_prices])
        .expect("make_sales_batch failed")
}

// ── Revenue resource ──────────────────────────────────────────────────────────

struct TotalRevenue(f64);
struct RegionFilter(String);

// ── Pipeline system ───────────────────────────────────────────────────────────

/// Compute total revenue = SUM(quantity * unit_price) over the ingested batch.
struct ComputeRevenue;

#[async_trait]
impl System for ComputeRevenue {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("compute_revenue")
            .read("sales", "quantity")
            .read("sales", "unit_price")
            .write_resource::<TotalRevenue>()
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), PcsError> {
        let filter = pipeline
            .get_resource::<RegionFilter>()
            .map(|r| r.0.clone())
            .unwrap_or_default();

        let batch = pipeline
            .batch_for("sales")
            .ok_or_else(|| PcsError::generic("sales component not found"))?;

        let qty_col = batch
            .column_by_name("quantity")
            .ok_or_else(|| PcsError::generic("quantity column missing"))?
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("quantity is Int32");

        let price_col = batch
            .column_by_name("unit_price")
            .ok_or_else(|| PcsError::generic("unit_price column missing"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("unit_price is Float64");

        let region_col = batch
            .column_by_name("region")
            .ok_or_else(|| PcsError::generic("region column missing"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region is Utf8");

        let revenue: f64 = (0..qty_col.len())
            .filter(|&i| filter.is_empty() || region_col.value(i) == filter)
            .map(|i| qty_col.value(i) as f64 * price_col.value(i))
            .sum();

        pipeline.insert_resource(TotalRevenue(revenue));
        Ok(())
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create a DataFusion SessionContext.
    let ctx = SessionContext::new();

    // 2. Register synthetic sales data as a MemTable.
    let batch = make_sales_batch();
    println!("Registered {} rows of sales data", batch.num_rows());

    let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("sales_raw", Arc::new(provider))?;

    // 3. Run a SQL query: filter to "north" region only, project needed columns.
    let sql = "SELECT product_id, region, quantity, unit_price \
               FROM sales_raw \
               WHERE region = 'north'";
    println!("SQL: {sql}");

    let mut src = DataFusionSource::from_sql(&ctx, sql).await?;
    println!("Result schema: {:?}", src.schema());

    // 4. Drain the DataFusionSource into a PCS Dataset.
    let src_schema = src.schema();
    let mut dataset = Dataset::new();
    dataset.register_raw_component("sales", src_schema);

    let rows = drain_into_dataset(&mut src, &mut dataset, "sales").await?;
    println!("Ingested {rows} rows into PCS Dataset");

    // Also insert a region filter resource (for demonstration — the SQL already
    // filtered, so this will match all ingested rows).
    dataset.insert_resource(RegionFilter("north".to_string()));

    // 5. Run a PCS pipeline on the ingested data.
    let mut pipeline = Pipeline::new("datafusion");
    *pipeline.data_mut() = dataset;
    pipeline.add_system(ComputeRevenue);
    pipeline.run().await?;

    // 6. Print result.
    let revenue = pipeline
        .data()
        .get_resource::<TotalRevenue>()
        .map(|r| r.0)
        .unwrap_or(0.0);
    println!("Total revenue (north region): ${revenue:.2}");

    // Verify it matches manual calculation:
    // product 1: 10 * 9.99 = 99.90
    // product 3: 20 * 4.99 = 99.80
    // product 6: 12 * 9.99 = 119.88
    // product 10: 9 * 9.99 = 89.91
    // Total: 99.90 + 99.80 + 119.88 + 89.91 = 409.49
    println!("Expected: $409.49");
    assert!(
        (revenue - 409.49).abs() < 0.01,
        "revenue mismatch: got {revenue:.2}"
    );
    println!("Correctness check passed.");

    Ok(())
}
