//! One transport, three formats: a [`FileSink`] write followed by a
//! [`FileSource`] read for csv, ndjson and parquet.
//!
//! This is what the connector-versus-transformer split buys. The connector code
//! under test is identical in all three cases; only the `Arc<dyn Transformer>`
//! differs.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use tempfile::TempDir;

use pcs_connector_file::{FileSink, FileSource};
use pcs_core::io::sink::Sink;
use pcs_core::io::source::Source;
use pcs_transformer::Transformer;
use pcs_transformer_csv::CsvTransformer;
use pcs_transformer_ndjson::NdjsonTransformer;
use pcs_transformer_parquet::ParquetTransformer;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]))
}

fn batch(rows: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows)),
            Arc::new(Float64Array::from_iter_values(
                (0..rows).map(|i| i as f64 * 1.5),
            )),
        ],
    )
    .expect("batch")
}

/// Write `rows` rows in `format`, read them back, and return what came back.
async fn round_trip(
    file_name: &str,
    transformer: Arc<dyn Transformer>,
    declared_on_read: bool,
    rows: i64,
) -> Vec<RecordBatch> {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join(file_name);

    let mut sink = FileSink::create(&path, Arc::clone(&transformer), schema()).expect("sink opens");
    sink.write_batch(&batch(rows)).await.expect("write");
    sink.finish().await.expect("finish");

    let declared = declared_on_read.then(schema);
    let mut source = FileSource::open_async(&path, transformer, declared)
        .await
        .expect("source opens");

    let mut batches = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("read") {
        batches.push(batch);
    }
    batches
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn first_value(batches: &[RecordBatch], row: usize) -> f64 {
    batches[0]
        .column_by_name("val")
        .expect("val column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float column")
        .value(row)
}

#[tokio::test]
async fn csv_round_trips_through_one_file_connector() {
    let batches = round_trip("out.csv", Arc::new(CsvTransformer::new(true)), true, 64).await;
    assert_eq!(total_rows(&batches), 64);
    assert!((first_value(&batches, 3) - 4.5).abs() < 1e-9);
}

#[tokio::test]
async fn ndjson_round_trips_through_one_file_connector() {
    let batches = round_trip(
        "out.ndjson",
        Arc::new(NdjsonTransformer::default()),
        true,
        64,
    )
    .await;
    assert_eq!(total_rows(&batches), 64);
    assert!((first_value(&batches, 3) - 4.5).abs() < 1e-9);
}

#[tokio::test]
async fn parquet_round_trips_through_one_file_connector() {
    // Parquet carries its own schema, so nothing is declared on the read.
    let batches = round_trip(
        "out.parquet",
        Arc::new(ParquetTransformer::new()),
        false,
        64,
    )
    .await;
    assert_eq!(total_rows(&batches), 64);
    assert!((first_value(&batches, 3) - 4.5).abs() < 1e-9);
}

#[tokio::test]
async fn only_parquet_reports_estimated_rows() {
    let dir = TempDir::new().expect("temp dir");

    let parquet_path = dir.path().join("rows.parquet");
    let mut sink = FileSink::create(&parquet_path, Arc::new(ParquetTransformer::new()), schema())
        .expect("sink opens");
    sink.write_batch(&batch(42)).await.expect("write");
    sink.finish().await.expect("finish");
    let parquet = FileSource::open(&parquet_path, Arc::new(ParquetTransformer::new()), None)
        .expect("source opens");
    assert_eq!(parquet.estimated_rows(), Some(42));

    let csv_path = dir.path().join("rows.csv");
    let mut sink = FileSink::create(&csv_path, Arc::new(CsvTransformer::new(true)), schema())
        .expect("sink opens");
    sink.write_batch(&batch(42)).await.expect("write");
    sink.finish().await.expect("finish");
    let csv = FileSource::open(
        &csv_path,
        Arc::new(CsvTransformer::new(true)),
        Some(schema()),
    )
    .expect("source opens");
    assert_eq!(csv.estimated_rows(), None);
}

#[tokio::test]
async fn a_write_after_finish_is_an_error_naming_the_sink() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("closed.parquet");

    let mut sink =
        FileSink::create(&path, Arc::new(ParquetTransformer::new()), schema()).expect("sink opens");
    sink.write_batch(&batch(1)).await.expect("write");
    sink.finish().await.expect("finish");

    let Err(err) = sink.write_batch(&batch(1)).await else {
        panic!("a finished sink has no writer left");
    };
    assert_eq!(err.message(), "FileSink: write_batch called after finish");

    // A second finish is a no-op, which is what makes `run_with_io`'s
    // unconditional finish on every call safe.
    sink.finish().await.expect("finish is idempotent");
}

#[tokio::test]
async fn opening_a_missing_file_names_the_path() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("absent.csv");
    let Err(err) = FileSource::open(&path, Arc::new(CsvTransformer::new(true)), Some(schema()))
    else {
        panic!("a missing file must not open");
    };
    assert!(
        err.message().starts_with("FileSource: cannot open"),
        "got: {err}"
    );
    assert!(err.message().contains("absent.csv"), "got: {err}");
}
