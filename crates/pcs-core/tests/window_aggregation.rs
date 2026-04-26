// Integration test ported from examples/window_aggregation.rs
// Requires the `windows` feature.

#![cfg(feature = "windows")]

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use pcs_core::PcsError;
use pcs_core::component::Component;
use pcs_core::pipeline::{Dataset, Pipeline};
use pcs_core::system::{System, SystemMeta};
use pcs_core::windows::{
    ReduceAggregate, WindowFunction, WindowResults, WindowSpec, WindowedSystemBuilder,
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SalesEvent {
    timestamp_ms: i64,
    category: String,
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

// ---------------------------------------------------------------------------
// Systems
// ---------------------------------------------------------------------------

struct IngestSystem;

#[async_trait]
impl System for IngestSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("ingest").write_component("SalesEvent")
    }
    async fn run(&self, data: &mut Dataset) -> Result<(), PcsError> {
        let events = vec![
            // window 0 (0–30s)
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
            // window 1 (30–60s)
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
            // window 2 (60–90s)
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
        data.append::<SalesEvent>(&events)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_window_aggregation_produces_results_for_all_windows() {
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
        .build()
        .unwrap();

    let mut pipeline = Pipeline::builder("window_aggregation")
        .with::<SalesEvent>()
        .with_system(IngestSystem)
        .with_system(windowed)
        .build();

    pipeline.run().await.unwrap();

    let results = pipeline
        .data()
        .get_resource::<WindowResults>()
        .expect("WindowResults resource not found after pipeline run");

    // 9 events across 3 windows × 2 categories → at least 3 result batches/rows.
    assert!(
        results.total_rows() >= 3,
        "expected at least 3 window groups"
    );
    // No late events in this dataset (all timestamps are well within their window).
    assert!(
        results.side_output.is_empty(),
        "no events should be dropped"
    );
}

#[tokio::test]
async fn test_window_aggregation_stage_layout_is_valid() {
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
        .build()
        .unwrap();

    let mut pipeline = Pipeline::builder("window_stages")
        .with::<SalesEvent>()
        .with_system(IngestSystem)
        .with_system(windowed)
        .build();

    pipeline.run().await.unwrap();

    let stages = pipeline.stages().unwrap_or_default();
    // Stage 0: IngestSystem; Stage 1: WindowedSystem.
    assert!(stages.len() >= 2, "expected at least 2 stages");
}
