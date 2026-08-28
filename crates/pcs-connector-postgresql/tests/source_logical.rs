//! `cdc_logical` against a real PostgreSQL server.
//!
//! Soft-skips without Docker; see `common::try_start`.

mod common;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use pcs_connector::from_kdl_str;
use pcs_connector_postgresql::{PostgresSource, PostgresSourceConfig};
use pcs_core::io::source::Source;
use serde::Deserialize as _;

const FIELDS: &str = "
schema_fields \"__op\" type=\"utf8\" nullable=#false
schema_fields \"__lsn\" type=\"int64\" nullable=#false
schema_fields \"__xid\" type=\"int64\"
schema_fields \"__commit_ts\" type=\"timestamp_micros_utc\"
schema_fields \"__table\" type=\"utf8\" nullable=#false
schema_fields \"id\" type=\"int64\"
schema_fields \"label\" type=\"utf8\"
";

fn source(dsn: &str, body: &str) -> PostgresSource {
    let text = format!(
        "{body}\n\nconnection dsn={} sslmode=\"disable\"\n",
        common::quoted(dsn)
    );
    let cfg = PostgresSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
        .expect("parse config");
    PostgresSource::new(cfg).expect("build source")
}

async fn drain(source: &mut PostgresSource) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("next_batch") {
        batches.push(batch);
    }
    batches
}

fn strings(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("column {column}"))
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 column");
            (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        None
                    } else {
                        Some(array.value(row).to_string())
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn int64s(batches: &[RecordBatch], column: &str) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("column {column}"))
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column");
            (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        None
                    } else {
                        Some(array.value(row))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// `__commit_ts` is a timestamp column, not an int64 one.
fn commit_timestamps(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name("__commit_ts")
                .expect("__commit_ts column")
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp column");
            (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        None
                    } else {
                        Some(array.value(row))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

const SLOT: &str = "pcs_test_slot";
const PUBLICATION: &str = "pcs_test_pub";

fn config(table: &str, extra: &str) -> String {
    format!(
        "name \"orders\"\nbatch_rows 100\n{extra}\n\n\
         mode kind=\"cdc_logical\" slot=\"{SLOT}\" \
         publication=\"{PUBLICATION}\" table=\"{table}\"\n{FIELDS}"
    )
}

#[tokio::test]
async fn insert_update_delete_arrive_with_their_ops_and_the_slot_advances() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(&format!(
            "CREATE TABLE orders (id bigint PRIMARY KEY, label text); \
             ALTER TABLE orders REPLICA IDENTITY FULL; \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE orders"
        ))
        .await
        .expect("fixture");

    let mut source = source(&pg.dsn(), &config("public.orders", ""));

    // The slot is created on the first call, so changes made before it exists
    // are not decoded. Create it by draining an empty cycle first.
    assert!(drain(&mut source).await.is_empty());

    client
        .batch_execute(
            "INSERT INTO orders VALUES (1, 'a'); \
             UPDATE orders SET label = 'b' WHERE id = 1; \
             DELETE FROM orders WHERE id = 1",
        )
        .await
        .expect("mutate");

    let batches = drain(&mut source).await;
    assert_eq!(
        strings(&batches, "__op"),
        vec![
            Some("I".to_string()),
            Some("U".to_string()),
            Some("D".to_string())
        ]
    );
    assert_eq!(
        strings(&batches, "__table"),
        vec![Some("public.orders".to_string()); 3]
    );
    assert_eq!(
        strings(&batches, "label"),
        vec![
            Some("a".to_string()),
            Some("b".to_string()),
            // REPLICA IDENTITY FULL, so the delete carries the old row.
            Some("b".to_string())
        ]
    );
    assert_eq!(int64s(&batches, "id"), vec![Some(1), Some(1), Some(1)]);

    let lsns: Vec<i64> = int64s(&batches, "__lsn")
        .into_iter()
        .map(|lsn| lsn.expect("__lsn is NOT NULL"))
        .collect();
    assert!(
        lsns.windows(2).all(|pair| pair[0] < pair[1]),
        "LSNs must increase: {lsns:?}"
    );
    assert!(
        int64s(&batches, "__xid").iter().all(Option::is_some),
        "every change carries a transaction id"
    );
    let commit_ts = commit_timestamps(&batches);
    assert!(
        commit_ts.iter().all(Option::is_some),
        "every change carries its transaction's commit timestamp"
    );
    // Rebased onto Arrow's 1970 epoch, so a real commit is far past it.
    assert!(
        commit_ts
            .iter()
            .all(|ts| ts.expect("some") > 1_600_000_000_000_000),
        "commit timestamps must be rebased to the Arrow epoch: {commit_ts:?}"
    );

    let before: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .await
        .expect("slot row")
        .get(0);

    // A second cycle advances the slot past what the first cycle emitted, and
    // then finds nothing left.
    let batches = drain(&mut source).await;
    assert!(
        batches.is_empty(),
        "the advance must have consumed the changes, got {} batch(es)",
        batches.len()
    );

    let after: String = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .await
        .expect("slot row")
        .get(0);
    assert_ne!(before, after, "confirmed_flush_lsn must have moved");
}

#[tokio::test]
async fn changes_for_another_published_table_are_skipped() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(&format!(
            "CREATE TABLE orders (id bigint PRIMARY KEY, label text); \
             CREATE TABLE other (id bigint PRIMARY KEY, label text); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE orders, other"
        ))
        .await
        .expect("fixture");

    let mut source = source(&pg.dsn(), &config("public.orders", ""));
    assert!(drain(&mut source).await.is_empty());

    client
        .batch_execute(
            "INSERT INTO other VALUES (1, 'x'); \
             INSERT INTO orders VALUES (2, 'y'); \
             INSERT INTO other VALUES (3, 'z')",
        )
        .await
        .expect("mutate");

    let batches = drain(&mut source).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(int64s(&batches, "id"), vec![Some(2)]);
}

#[tokio::test]
async fn a_delete_leaves_non_key_columns_null_under_default_replica_identity() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(&format!(
            "CREATE TABLE orders (id bigint PRIMARY KEY, label text); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE orders"
        ))
        .await
        .expect("fixture");

    let mut source = source(&pg.dsn(), &config("public.orders", ""));
    assert!(drain(&mut source).await.is_empty());

    client
        .batch_execute("INSERT INTO orders VALUES (1, 'a'); DELETE FROM orders WHERE id = 1")
        .await
        .expect("mutate");

    let batches = drain(&mut source).await;
    assert_eq!(
        strings(&batches, "__op"),
        vec![Some("I".to_string()), Some("D".to_string())]
    );
    assert_eq!(
        strings(&batches, "label"),
        vec![Some("a".to_string()), None],
        "the delete carries only the replica-identity column"
    );
    assert_eq!(int64s(&batches, "id"), vec![Some(1), Some(1)]);
}

#[tokio::test]
async fn batch_rows_chunks_one_peek_and_the_cycle_serves_every_chunk() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(&format!(
            "CREATE TABLE orders (id bigint PRIMARY KEY, label text); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE orders"
        ))
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        &config("public.orders", "").replace("batch_rows 100", "batch_rows 3"),
    );
    assert!(drain(&mut source).await.is_empty());

    client
        .batch_execute("INSERT INTO orders SELECT g, 'p' || g FROM generate_series(1, 7) g")
        .await
        .expect("mutate");

    let batches = drain(&mut source).await;
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![3, 3, 1]
    );
    assert_eq!(
        int64s(&batches, "id"),
        (1..=7).map(Some).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_role_without_replication_is_told_what_it_needs() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(&format!(
            "CREATE TABLE orders (id bigint PRIMARY KEY, label text); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE orders; \
             CREATE ROLE reader LOGIN PASSWORD 'reader'; \
             GRANT pg_read_all_data TO reader"
        ))
        .await
        .expect("fixture");

    let mut source = source(&pg.dsn_as("reader", "reader"), &config("public.orders", ""));
    let err = source
        .next_batch()
        .await
        .expect_err("a role without REPLICATION cannot create a slot");
    assert!(
        err.message().contains("REPLICATION"),
        "the message must name the attribute: {}",
        err.message()
    );
}

#[tokio::test]
async fn a_declared_column_the_publication_omits_is_rejected() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(&format!(
            "CREATE TABLE orders (id bigint PRIMARY KEY); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE orders"
        ))
        .await
        .expect("fixture");

    let mut source = source(&pg.dsn(), &config("public.orders", ""));
    assert!(drain(&mut source).await.is_empty());
    client
        .batch_execute("INSERT INTO orders VALUES (1)")
        .await
        .expect("mutate");

    let err = source
        .next_batch()
        .await
        .expect_err("the declared 'label' column does not exist");
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("'label'"), "{}", err.message());
}
