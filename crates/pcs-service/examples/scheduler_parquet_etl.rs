//! End-to-end Parquet ETL example using the `arrow-io` Source/Sink layer.
//!
//! This example:
//! 1. Generates a synthetic 10 000-row Parquet file (`input.parquet`) in a
//!    temp directory.
//! 2. Reads it with [`ParquetSource`] via [`Pipeline::run_with_io`].
//! 3. Applies two [`ParallelSystem`]s:
//!    - `ScaleSystem` — multiplies the `price` column by 1.1.
//!    - `RoundSystem` — reads `price` and writes a `rounded` column (Int64).
//! 4. Writes output with [`ParquetSink`].
//! 5. Reads output back and prints the first 5 rows.
//! 6. Cleans up temp files on exit.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

use pcs_service::PcsError;
use pcs_service::io::parquet_sink::ParquetSink;
use pcs_service::io::parquet_source::ParquetSource;
use pcs_service::io::sink::Sink;
use pcs_service::io::source::Source;
use pcs_service::pipeline::{Dataset, Pipeline};
use pcs_service::system::{ParallelSystem, SystemMeta, WriteSet};

// ---------------------------------------------------------------------------
// Component schema
// ---------------------------------------------------------------------------

/// Schema with `id` (Int64), `price` (Float64), `rounded` (Int64).
fn trade_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("rounded", DataType::Int64, false),
    ]))
}

/// Minimal input schema (no `rounded` column — added by the pipeline).
fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

// ---------------------------------------------------------------------------
// Systems
// ---------------------------------------------------------------------------

/// Multiply `price` by 1.1.
struct ScaleSystem;

#[async_trait]
impl ParallelSystem for ScaleSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("scale_price")
            .read("Trade", "price")
            .write("Trade", "price")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let batch = pipeline
            .batch_for("Trade")
            .ok_or_else(|| PcsError::generic("ScaleSystem: Trade component not found"))?;
        let prices = batch
            .column_by_name("price")
            .ok_or_else(|| PcsError::generic("ScaleSystem: price column missing"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PcsError::generic("ScaleSystem: price is not Float64"))?;

        let scaled: Float64Array =
            Float64Array::from_iter_values(prices.values().iter().map(|&p| p * 1.1));

        Ok(WriteSet::new().put("Trade", "price", Arc::new(scaled)))
    }
}

/// Round `price` into a new `rounded` (Int64) column.
struct RoundSystem;

#[async_trait]
impl ParallelSystem for RoundSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("round_price")
            .read("Trade", "price")
            .write("Trade", "rounded")
    }

    async fn run(&self, pipeline: &Dataset) -> Result<WriteSet, PcsError> {
        let batch = pipeline
            .batch_for("Trade")
            .ok_or_else(|| PcsError::generic("RoundSystem: Trade component not found"))?;
        let prices = batch
            .column_by_name("price")
            .ok_or_else(|| PcsError::generic("RoundSystem: price column missing"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PcsError::generic("RoundSystem: price is not Float64"))?;

        let rounded: Int64Array =
            Int64Array::from_iter_values(prices.values().iter().map(|&p| p.round() as i64));

        Ok(WriteSet::new().put("Trade", "rounded", Arc::new(rounded)))
    }
}

// ---------------------------------------------------------------------------
// Helper: generate input.parquet
// ---------------------------------------------------------------------------

async fn generate_input(path: &std::path::Path, n: usize) -> Result<(), PcsError> {
    let schema = input_schema();
    let ids = Arc::new(Int64Array::from_iter_values(0..n as i64));
    let prices = Arc::new(Float64Array::from_iter_values(
        (0..n).map(|i| (i as f64) * 0.01 + 1.0),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, prices])
        .map_err(|e| PcsError::generic(format!("generate_input: {e}")))?;

    let mut sink = ParquetSink::from_path(path, schema)?;
    sink.write_batch(&batch).await?;
    sink.finish().await
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let input_path = dir.path().join("input.parquet");
    let output_path = dir.path().join("output.parquet");

    println!("Generating 10 000-row input.parquet …");
    generate_input(&input_path, 10_000).await?;

    // -----------------------------------------------------------------------
    // Build the pipeline — register the full Trade schema (with `rounded`),
    // then wire in the source, sink, and systems.
    // -----------------------------------------------------------------------
    let full_schema = trade_schema();
    let mut pipeline = Pipeline::new("parquet-etl");
    pipeline
        .data_mut()
        .register_raw_component("Trade", full_schema.clone());

    // -----------------------------------------------------------------------
    // The Parquet file only has `id` and `price`; we need to add a `rounded`
    // column before appending into the fully-schemed component.
    // Use CastingSource to adapt, then manually add the `rounded` column as
    // zeros via a pre-flight batch.
    //
    // Simpler approach: use a custom Source that reads the file and pads.
    // -----------------------------------------------------------------------

    // Source: read input, add a zero `rounded` column, adapt to full schema.
    struct PaddedParquetSource {
        inner: ParquetSource,
        full_schema: Arc<Schema>,
    }

    #[async_trait]
    impl Source for PaddedParquetSource {
        fn schema(&self) -> Arc<Schema> {
            self.full_schema.clone()
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
            match self.inner.next_batch().await? {
                None => Ok(None),
                Some(batch) => {
                    let n = batch.num_rows();
                    // Add a zero-filled `rounded` column.
                    let rounded: Arc<dyn arrow_array::Array> =
                        Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(0i64, n)));
                    let id_col = batch.column_by_name("id").unwrap().clone();
                    let price_col = batch.column_by_name("price").unwrap().clone();
                    let padded = RecordBatch::try_new(
                        self.full_schema.clone(),
                        vec![id_col, price_col, rounded],
                    )
                    .map_err(|e| PcsError::generic(format!("PaddedParquetSource: {e}")))?;
                    Ok(Some(padded))
                }
            }
        }

        fn estimated_rows(&self) -> Option<usize> {
            self.inner.estimated_rows()
        }
    }

    let inner_src = ParquetSource::from_path_async(&input_path).await?;
    let src = PaddedParquetSource {
        inner: inner_src,
        full_schema: full_schema.clone(),
    };
    let sink = ParquetSink::from_path(&output_path, full_schema.clone())?;

    pipeline
        .add_parallel_system(ScaleSystem)
        .add_parallel_system(RoundSystem)
        .add_source("Trade", src)
        .add_sink("Trade", sink);

    println!("Running pipeline (scale + round) …");
    pipeline.run_with_io().await?;
    println!(
        "Pipeline complete. {} rows processed.",
        pipeline.data().rows()
    );

    // -----------------------------------------------------------------------
    // Read output and print first 5 rows.
    // -----------------------------------------------------------------------
    println!("\nFirst 5 rows of output.parquet:");
    println!("{:<8} {:<14} {:<10}", "id", "price", "rounded");
    println!("{}", "-".repeat(34));

    let mut out_src = ParquetSource::from_path_async(&output_path).await?;
    let mut printed = 0usize;
    'outer: while let Some(batch) = out_src.next_batch().await? {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let prices = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let rounded = batch
            .column_by_name("rounded")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            if printed >= 5 {
                break 'outer;
            }
            println!(
                "{:<8} {:<14.4} {:<10}",
                ids.value(i),
                prices.value(i),
                rounded.value(i)
            );
            printed += 1;
        }
    }

    // dir is automatically cleaned up when dropped.
    println!("\nTemp files cleaned up.");
    Ok(())
}
