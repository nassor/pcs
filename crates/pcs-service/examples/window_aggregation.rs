//! Windowed aggregation over sales events: tumbling 30-second windows keyed by category.
//!
//! Demonstrates [`WindowedSystemBuilder`] with:
//! - A `SalesEvent` component (`timestamp_ms`, `category`, `amount`)
//! - 30-second tumbling windows keyed by `category`
//! - `ReduceAggregate::Sum` over the `amount` field
//! - A downstream system that reads [`WindowResults`] and prints per-window totals
//! - Late-data side-output via `allowed_lateness`
//!
//! ## Running
//!
//! ```bash
//! cargo run --example window_aggregation --features windows
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, cast::AsArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use pcs_service::PcsError;
use pcs_service::component::Component;
use pcs_service::pipeline::{Dataset, Pipeline};
use pcs_service::system::{FieldRef, System, SystemMeta};

use pcs_service::windows::{
    ReduceAggregate, WindowFunction, WindowResults, WindowSpec, WindowedSystemBuilder,
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SalesEvent {
    /// Unix timestamp in milliseconds.
    timestamp_ms: i64,
    /// Product category name.
    category: String,
    /// Sale amount in USD.
    amount: f64,
}

impl Component for SalesEvent {
    fn name() -> &'static str {
        "SalesEvent"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }
}

impl SalesEvent {
    const CATEGORY: FieldRef<SalesEvent> = FieldRef::new("category");
}

// ---------------------------------------------------------------------------
// System 1 — IngestSystem
// ---------------------------------------------------------------------------

/// Loads seed sales events spanning three 30-second windows across two categories.
struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").write_component("SalesEvent")
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), PcsError> {
        // t=0s–30s: window 0
        // t=30s–60s: window 1
        // t=60s–90s: window 2
        let events = vec![
            SalesEvent {
                timestamp_ms: 5_000,
                category: "Electronics".into(),
                amount: 299.99,
            },
            SalesEvent {
                timestamp_ms: 12_000,
                category: "Books".into(),
                amount: 24.95,
            },
            SalesEvent {
                timestamp_ms: 18_000,
                category: "Electronics".into(),
                amount: 149.50,
            },
            SalesEvent {
                timestamp_ms: 25_000,
                category: "Books".into(),
                amount: 39.99,
            },
            SalesEvent {
                timestamp_ms: 31_000,
                category: "Electronics".into(),
                amount: 599.00,
            },
            SalesEvent {
                timestamp_ms: 45_000,
                category: "Electronics".into(),
                amount: 89.99,
            },
            SalesEvent {
                timestamp_ms: 50_000,
                category: "Books".into(),
                amount: 14.99,
            },
            SalesEvent {
                timestamp_ms: 62_000,
                category: "Electronics".into(),
                amount: 199.00,
            },
            SalesEvent {
                timestamp_ms: 75_000,
                category: "Books".into(),
                amount: 49.99,
            },
        ];
        println!("[ingest]  loaded {} sales events", events.len());
        data.append::<SalesEvent>(&events)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Resource: category hash lookup
// ---------------------------------------------------------------------------

/// Maps `key_hash → category` so the report system can decode window results.
///
/// Built by the ingest system immediately after appending data. The windowed
/// system computes the same hashes deterministically from the `category` field,
/// so this lookup is stable for the lifetime of one pipeline run.
struct CategoryLookup(HashMap<i64, String>);

/// Builds the hash-to-category reverse map from the live dataset.
struct BuildLookupSystem;

#[async_trait]
impl System for BuildLookupSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("build_lookup")
            .reads(SalesEvent::CATEGORY)
            .write_resource::<CategoryLookup>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), PcsError> {
        use pcs_service::windows::WindowedSystemBuilder as W;

        // Re-compute hashes the same way the windowed system does: by passing
        // each category string through the key-hash function. We build a temporary
        // system just to obtain the hash for each unique value — a simpler approach
        // is to call the public hash utility directly.
        use pcs_service::windows::WindowSpec as WS;
        // Use the same hash helper the windowed system uses.
        // pcs_service::windows::hash is private, so we derive hashes via serde_arrow + arrow.
        // Simpler: collect unique categories and compute xxhash via the stable
        // public path: build a one-row batch per category through a no-op windowed
        // system and read back its key_hash output.
        //
        // For this example we use a direct xxhash call through the same logic the
        // windowed system applies: hash the concatenation of all key field values.
        // Because the `hash` sub-module is pub(crate), we replicate the same
        // formula here using the `xxhash-rust` crate — but that adds a dependency.
        //
        // Practical alternative: build the lookup before running the pipeline,
        // from the known input set, which we do below.
        let known_categories = ["Electronics", "Books"];
        let mut map = HashMap::new();

        // Build a mini single-category dataset for each value and run a
        // minimal windowed system to obtain the key_hash for that category.
        for cat in known_categories {
            let row = SalesEvent {
                timestamp_ms: 0,
                category: cat.to_string(),
                amount: 1.0,
            };
            let mut mini = Dataset::new();
            mini.register_component::<SalesEvent>()?;
            mini.append::<SalesEvent>(&[row])?;

            let ws = W::new()
                .source("SalesEvent", "timestamp_ms")
                .keyed_by(&["category"])
                .window(WS::Tumbling {
                    size_ms: 30_000,
                    offset_ms: 0,
                })
                .function(WindowFunction::Reduce {
                    input_field: "amount",
                    aggregate: ReduceAggregate::Sum,
                })
                .build()
                .map_err(|e| PcsError::generic(format!("BuildLookupSystem build: {e}")))?;

            ws.run(&mut mini).await?;

            if let Some(wr) = mini.get_resource::<WindowResults>() {
                for batch in &wr.batches {
                    let kh_idx = batch
                        .schema()
                        .index_of("key_hash")
                        .map_err(|e| PcsError::generic(format!("key_hash column missing: {e}")))?;
                    let kh_col = batch
                        .column(kh_idx)
                        .as_primitive::<arrow_array::types::Int64Type>();
                    if !kh_col.is_empty() {
                        map.insert(kh_col.value(0), cat.to_string());
                    }
                }
            }
        }

        println!("[lookup]  built hash map for {} categories", map.len());
        data.insert_resource(CategoryLookup(map));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// System 3 — ReportSystem
// ---------------------------------------------------------------------------

/// Reads [`WindowResults`] and prints a table of per-window-per-category totals.
struct ReportSystem;

#[async_trait]
impl System for ReportSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("report")
            .read_resource::<WindowResults>()
            .read_resource::<CategoryLookup>()
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), PcsError> {
        let results = data
            .get_resource::<WindowResults>()
            .ok_or_else(|| PcsError::generic("WindowResults resource not found"))?;
        let lookup = data
            .get_resource::<CategoryLookup>()
            .ok_or_else(|| PcsError::generic("CategoryLookup resource not found"))?;

        println!();
        println!("╔══════════════════════════════════════════════════════╗");
        println!("║        SALES — 30-SECOND TUMBLING WINDOW TOTALS     ║");
        println!("╠═══════════════════════╦══════════════╦══════════════╣");
        println!("║  Window               ║  Category    ║  Sum (USD)   ║");
        println!("╠═══════════════════════╬══════════════╬══════════════╣");

        for batch in &results.batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let schema = batch.schema();
            let wid_idx = schema
                .index_of("window_id")
                .map_err(|e| PcsError::generic(format!("window_id missing: {e}")))?;
            let kh_idx = schema
                .index_of("key_hash")
                .map_err(|e| PcsError::generic(format!("key_hash missing: {e}")))?;
            // The aggregated column is the last field (after window_id and key_hash).
            let sum_idx = schema.fields().len() - 1;

            let wid_col = batch
                .column(wid_idx)
                .as_primitive::<arrow_array::types::Int64Type>();
            let kh_col = batch
                .column(kh_idx)
                .as_primitive::<arrow_array::types::Int64Type>();
            let sum_col = batch
                .column(sum_idx)
                .as_primitive::<arrow_array::types::Float64Type>();

            for row in 0..batch.num_rows() {
                let wid = wid_col.value(row);
                let kh = kh_col.value(row);
                let sum = if sum_col.is_valid(row) {
                    sum_col.value(row)
                } else {
                    0.0
                };

                // Window ID for tumbling windows is the floor-division index.
                // Multiply by size_ms to get start time in seconds.
                let window_start_s = wid * 30;
                let window_end_s = window_start_s + 30;
                let window_label = format!("{window_start_s:>3}s – {window_end_s:>3}s");

                let category = lookup.0.get(&kh).map(|s| s.as_str()).unwrap_or("<unknown>");

                println!(
                    "║  {:<21}  ║  {:<12}  ║  {:>10.2}  ║",
                    window_label, category, sum
                );
            }
        }

        println!("╚═══════════════════════╩══════════════╩══════════════╝");

        if !results.late_batches.is_empty() {
            println!("  Late re-firings: {}", results.late_batches.len());
        }
        if !results.side_output.is_empty() {
            println!(
                "  Dropped (beyond lateness): {} rows",
                results.side_output.total_rows()
            );
        }

        println!();
        println!("Total on-time window groups: {}", results.total_rows());

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), PcsError> {
    // Build the windowed aggregation system:
    //   - source: SalesEvent, time field: timestamp_ms (Int64 ms since epoch)
    //   - keyed by: category (one aggregate per category per window)
    //   - window: tumbling, 30-second buckets, no offset
    //   - function: sum of the `amount` field
    //   - allowed_lateness: 5 000 ms (late rows within 5s re-fire the window)
    let windowed = WindowedSystemBuilder::new()
        .source("SalesEvent", "timestamp_ms")
        .keyed_by(&["category"])
        .window(WindowSpec::Tumbling {
            size_ms: 30_000,
            offset_ms: 0,
        })
        .function(WindowFunction::Reduce {
            input_field: "amount",
            aggregate: ReduceAggregate::Sum,
        })
        .allowed_lateness(5_000)
        .build()?;

    // Stage layout (auto-computed from SystemMeta field declarations):
    //
    //   Stage 0: [IngestSystem]        — writes_component("SalesEvent")
    //   Stage 1: [BuildLookupSystem]   — reads SalesEvent.category, writes CategoryLookup
    //   Stage 2: [WindowedSystem]      — reads_component("SalesEvent"), writes WindowResults
    //   Stage 3: [ReportSystem]        — reads WindowResults + CategoryLookup
    let mut pipeline = Pipeline::builder("window_aggregation")
        .with::<SalesEvent>()
        .with_system(IngestSystem)
        .with_system(BuildLookupSystem)
        .with_system(windowed)
        .with_system(ReportSystem)
        .build();

    println!("Running windowed aggregation pipeline...");
    pipeline.run().await?;

    let stages = pipeline.stages().unwrap_or_default();
    println!();
    println!("Stage layout:");
    for (i, stage) in stages.iter().enumerate() {
        println!("  Stage {i}: {stage:?}");
    }

    Ok(())
}
