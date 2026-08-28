//! [`KafkaSource`] and [`KafkaSink`] against a real Kafka broker.
//!
//! Soft-skips without Docker; see `common::try_start`. Each test uses its own
//! topic name so the whole suite can share the one broker.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rdkafka::Message as _;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;

use pcs_connector_kafka::{
    KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig, TopicProvision,
};
use pcs_core::dataset::Dataset;
use pcs_core::io::sink::Sink;
use pcs_core::io::source::{Source, drain_into_dataset};
use pcs_transformer::Transformer;
use pcs_transformer_arrow_ipc::ArrowIpcTransformer;
use pcs_transformer_ndjson::NdjsonTransformer;

const COMPONENT: &str = "TestRow";

/// The payload format most of these tests use. The factories are handed the
/// transformer the host resolved; a direct construction like these tests'
/// builds it here.
fn ndjson() -> Arc<dyn Transformer> {
    Arc::new(NdjsonTransformer::default())
}

fn arrow_ipc() -> Arc<dyn Transformer> {
    Arc::new(ArrowIpcTransformer::new())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("total", DataType::Float64, true),
    ]))
}

fn batch_of(schema: Arc<Schema>, ids: &[i64], names: &[&str], totals: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Float64Array::from(totals.to_vec())),
        ],
    )
    .expect("valid batch")
}

fn sample_batch(schema: Arc<Schema>) -> RecordBatch {
    batch_of(schema, &[1, 2, 3], &["a", "b", "c"], &[1.5, 2.5, 3.5])
}

/// A source config with sane, fast-failing defaults; override fields with
/// `..source_cfg(...)`.
fn source_cfg(brokers: &str, topic: &str, group_id: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        group_id: group_id.to_string(),
        batch_size: 1000,
        poll_timeout_ms: 5_000,
        auto_offset_reset: "earliest".to_string(),
        commit_on_drain: true,
        stop_at_end: true,
        provision: TopicProvision::default(),
        properties: BTreeMap::new(),
        schema_fields: vec![],
    }
}

/// A sink config with sane defaults; override fields with `..sink_cfg(...)`.
fn sink_cfg(brokers: &str, topic: &str) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        key_field: None,
        flush_timeout_ms: 30_000,
        provision: TopicProvision::default(),
        properties: BTreeMap::new(),
        schema_fields: vec![],
    }
}

fn assert_sample_values(out: &RecordBatch) {
    assert_eq!(out.num_rows(), 3);
    let ids = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64");
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column is Utf8");
    let totals = out
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total column is Float64");
    assert_eq!(ids.values(), &[1, 2, 3]);
    assert_eq!(
        (0..names.len()).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "c"]
    );
    assert_eq!(totals.values(), &[1.5, 2.5, 3.5]);
}

#[tokio::test]
async fn json_roundtrip() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("json-roundtrip");
    let schema = schema();

    let mut sink = KafkaSink::new(sink_cfg(kafka.brokers(), &topic), schema.clone(), ndjson())
        .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, "json-roundtrip-group"),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let rows = drain_into_dataset(&mut source, &mut dataset, COMPONENT)
        .await
        .expect("drain");
    assert_eq!(rows, 3);
    assert_sample_values(dataset.batch_for(COMPONENT).expect("component present"));
}

#[tokio::test]
async fn arrow_ipc_roundtrip() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("arrow-ipc-roundtrip");
    let schema = schema();

    let mut sink = KafkaSink::new(
        sink_cfg(kafka.brokers(), &topic),
        schema.clone(),
        arrow_ipc(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, "arrow-ipc-roundtrip-group"),
        schema.clone(),
        arrow_ipc(),
    )
    .expect("source builds");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let rows = drain_into_dataset(&mut source, &mut dataset, COMPONENT)
        .await
        .expect("drain");
    assert_eq!(rows, 3);
    let out = dataset.batch_for(COMPONENT).expect("component present");
    assert_eq!(out.schema().fields(), schema.fields());
    assert_sample_values(out);
}

#[tokio::test]
async fn sink_creates_topic_by_default() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("sink-creates-by-default");
    let schema = schema();

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            provision: TopicProvision {
                partitions: 3,
                ..TopicProvision::default()
            },
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("write_batch provisions the topic before writing");

    let partitions = common::topic_partitions(kafka.brokers(), &topic)
        .await
        .expect("topic must exist after write_batch, though AUTO_CREATE_TOPICS_ENABLE=false");
    assert_eq!(partitions, 3);
}

#[tokio::test]
async fn create_false_does_not_create() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("create-false");
    let schema = schema();

    let mut properties = BTreeMap::new();
    // Bounds the produce error's arrival regardless of librdkafka's default
    // 5-minute `message.timeout.ms`, so a wrongly-succeeding send can only
    // make this test slow, never hang.
    properties.insert("message.timeout.ms".to_string(), "5000".to_string());

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            provision: TopicProvision {
                create: false,
                ..TopicProvision::default()
            },
            properties,
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");

    let err = sink
        .write_batch(&sample_batch(schema))
        .await
        .expect_err("a topic that does not exist and cannot auto-create must error");
    assert_eq!(err.category(), "generic");

    assert!(
        common::topic_partitions(kafka.brokers(), &topic)
            .await
            .is_none(),
        "create = false must not have created the topic"
    );
}

#[tokio::test]
async fn source_creates_topic_by_default() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("source-creates-by-default");
    let schema = schema();

    let mut source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, "source-creates-group"),
        schema,
        ndjson(),
    )
    .expect("source builds");
    let batch = source
        .next_batch()
        .await
        .expect("first next_batch on an empty, freshly-created topic must not error");
    assert!(
        batch.is_none(),
        "empty topic with stop_at_end must report EOF"
    );

    assert!(
        common::topic_partitions(kafka.brokers(), &topic)
            .await
            .is_some(),
        "the source must have created the topic before subscribing"
    );
}

#[tokio::test]
async fn topic_config_is_applied() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("topic-config-applied");
    let schema = schema();

    let mut provision_config = BTreeMap::new();
    provision_config.insert("retention.ms".to_string(), "60000".to_string());

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            provision: TopicProvision {
                config: provision_config,
                ..TopicProvision::default()
            },
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("write_batch provisions the topic before writing");

    let value = common::describe_topic_config(kafka.brokers(), &topic, "retention.ms")
        .await
        .expect("retention.ms must be readable back from the broker");
    assert_eq!(value, "60000");
}

#[tokio::test]
async fn properties_reach_librdkafka() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let schema = schema();

    let mut bad_properties = BTreeMap::new();
    bad_properties.insert("this.is.not.a.property".to_string(), "x".to_string());
    let Err(err) = KafkaSource::new(
        KafkaSourceConfig {
            properties: bad_properties,
            ..source_cfg(
                kafka.brokers(),
                &kafka.topic("properties-bad"),
                "properties-group",
            )
        },
        schema.clone(),
        ndjson(),
    ) else {
        panic!("an unknown librdkafka property must be rejected at construction");
    };
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("this.is.not.a.property"));

    let mut good_properties = BTreeMap::new();
    good_properties.insert("client.id".to_string(), "pcs-test".to_string());
    KafkaSource::new(
        KafkaSourceConfig {
            properties: good_properties,
            ..source_cfg(
                kafka.brokers(),
                &kafka.topic("properties-good"),
                "properties-group",
            )
        },
        schema,
        ndjson(),
    )
    .expect("a known librdkafka property must construct successfully");
}

#[tokio::test]
async fn offsets_commit_between_drains() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("offsets-commit");
    let schema = schema();
    let group_id = "offsets-commit-g1";

    let mut sink = KafkaSink::new(sink_cfg(kafka.brokers(), &topic), schema.clone(), ndjson())
        .expect("sink builds");
    sink.write_batch(&batch_of(
        schema.clone(),
        &[1, 2, 3, 4],
        &["a", "b", "c", "d"],
        &[1.0, 2.0, 3.0, 4.0],
    ))
    .await
    .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut first_source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, group_id),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let mut first_dataset = Dataset::new();
    first_dataset.register_raw_component(COMPONENT, schema.clone());
    let first = drain_into_dataset(&mut first_source, &mut first_dataset, COMPONENT)
        .await
        .expect("first drain");
    assert_eq!(first, 4);
    drop(first_source);

    // `commit_on_drain` commits asynchronously (`CommitMode::Async`): give the
    // background poll thread time to actually reach the broker before a
    // second consumer in the same group checks what was committed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut second_source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, group_id),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let mut second_dataset = Dataset::new();
    second_dataset.register_raw_component(COMPONENT, schema);
    let second = drain_into_dataset(&mut second_source, &mut second_dataset, COMPONENT)
        .await
        .expect("second drain");
    assert_eq!(
        second, 0,
        "a second consumer in the same group must see no rows after offsets were committed"
    );
}

#[tokio::test]
async fn key_field_sets_the_message_key() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("key-field");
    let schema = schema();

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            key_field: Some("id".to_string()),
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut raw_cfg = rdkafka::ClientConfig::new();
    raw_cfg.set("bootstrap.servers", kafka.brokers());
    raw_cfg.set("group.id", "key-field-check");
    raw_cfg.set("auto.offset.reset", "earliest");
    let consumer: BaseConsumer = raw_cfg.create().expect("raw consumer");
    consumer.subscribe(&[topic.as_str()]).expect("subscribe");

    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && seen.len() < 3 {
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(message)) => {
                let key = message
                    .key()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .unwrap_or_default();
                let payload = message
                    .payload()
                    .map(|p| String::from_utf8_lossy(p).into_owned())
                    .unwrap_or_default();
                seen.push((key, payload));
            }
            Some(Err(KafkaError::PartitionEOF(_))) | None => {}
            Some(Err(e)) => panic!("poll failed: {e}"),
        }
    }
    assert_eq!(seen.len(), 3, "expected all 3 messages within the deadline");

    for (key, payload) in &seen {
        let value: serde_json::Value = serde_json::from_str(payload).expect("valid json payload");
        let id = value["id"].as_i64().expect("payload has an id field");
        assert_eq!(
            key,
            &id.to_string(),
            "message key must equal the rendered id"
        );
    }
}
