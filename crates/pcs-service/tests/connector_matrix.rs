//! The connector/transformer/processor matrix, in one test.
//!
//! Every `{source connector, sink connector, byte format, processor runtime}`
//! combination is built from KDL, assembled through the real factory registry,
//! run through the real standalone or stream runner, and asserted against the
//! capability table in [`matrix`]. The maximal workflow follows in the same
//! test.
//!
//! There is exactly one test function because nextest gives every test its own
//! process: a second one would start a second Kafka broker, NATS server,
//! PostgreSQL server and S3 endpoint. All four are started once here, behind a
//! `OnceCell`, and shared by every case.
//!
//! ```bash
//! rustup target add wasm32-wasip2
//! cargo build --release -p pcs-processor-smoketest --target wasm32-wasip2
//! cargo build -p pcs-plugin-smoketest
//! cargo nextest run -p pcs-service --all-features --test connector_matrix \
//!     --run-ignored ignored-only
//! ```
//!
//! `#[ignore]`d: it is a whole-matrix sweep against four containers, not a
//! per-edit check.

#![cfg(all(
    feature = "service",
    feature = "wasm",
    feature = "plugin",
    feature = "connector-channel",
    feature = "connector-file",
    feature = "connector-http",
    feature = "connector-kafka",
    feature = "connector-nats",
    feature = "connector-postgresql",
    feature = "connector-s3",
    feature = "connector-tcp",
    feature = "transformer-arrow-ipc",
    feature = "transformer-avro",
    feature = "transformer-csv",
    feature = "transformer-ndjson",
    feature = "transformer-parquet",
))]

use std::time::Instant;

use tokio::sync::Semaphore;

#[path = "common/matrix.rs"]
mod matrix;
#[path = "common/smoketest.rs"]
mod smoketest;

use matrix::{Case, Fixtures, Report, Resources, run_case, run_maximal};

/// How many cases build and run at once.
///
/// Each case owns its own temp directory, channel, topic, stream, table and
/// object prefix, and every listener binds `127.0.0.1:0`, so the only shared
/// state is the four containers.
const CONCURRENCY: usize = 16;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "whole connector matrix against four containers; run explicitly"]
async fn full_matrix() {
    let started = Instant::now();
    let fixtures = Fixtures::resolve(smoketest::smoketest_wasm_path()).expect("processor fixtures");
    let resources = Resources::default();
    let permits = Semaphore::new(CONCURRENCY);

    // Every container is started before any case runs: a readiness poll that
    // shares the poll loop with a thousand case futures can miss its budget
    // for want of being polled.
    let up = resources.warm().await;
    println!("resources: {}", Resources::describe(&up));

    // `PipelineRuntime` is `?Send`, so a case future cannot be spawned onto the
    // runtime: `join_all` polls every case on this one task and the semaphore
    // bounds how many are past their resource gate. The `tcp`-sourced cases run
    // as a second phase because their stream runner is stopped by cancellation
    // on a wall-clock settle window, which only holds when the task is not also
    // driving the other thousand cases.
    let (batch_cases, stream_cases) = Case::phases();
    let batch_reports = futures::future::join_all(
        batch_cases
            .iter()
            .copied()
            .map(|case| run_case(case, &resources, &fixtures, &permits)),
    )
    .await;
    let stream_reports = futures::future::join_all(
        stream_cases
            .iter()
            .copied()
            .map(|case| run_case(case, &resources, &fixtures, &permits)),
    )
    .await;

    let maximal = run_maximal(&resources, &fixtures).await;

    let mut reports = batch_reports;
    reports.extend(stream_reports);
    let report = Report::new(reports, started.elapsed());
    report.print();
    match &maximal {
        Ok(report) => report.print(),
        Err(e) => println!("\n=== maximal workflow ===\nFAILED: {e}"),
    }

    let failures = report.failures();
    let named = failures
        .iter()
        .map(|c| format!("  {} [{}]: {}", c.name(), c.outcome.label(), c.detail))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        failures.is_empty(),
        "{} of {} matrix cases did not meet their expectation:\n{named}",
        failures.len(),
        report.cases.len()
    );
    maximal.expect("the maximal workflow must build and deliver rows");
}
