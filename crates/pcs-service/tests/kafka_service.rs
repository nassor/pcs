//! The whole standalone service path over Kafka: config file, factories,
//! source drain, pipeline run, sink drain.
//!
//! `run_standalone` is what the `pcs-service serve` subcommand calls, so this
//! is the end to end proof that a `connector-kafka` build moves rows from one
//! topic through a pipeline and into another via the registered
//! `KafkaSourceFactory`/`KafkaSinkFactory`, not just via the connector crate's
//! own direct API (already covered by `pcs-connector-kafka`'s own tests).
//!
//! Soft-skips without Docker.

#![cfg(all(feature = "service", feature = "connector-kafka", feature = "wasm"))]

use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pcs_connector_kafka::{KafkaSink, KafkaSinkConfig, TopicProvision};
use pcs_core::dataset::Dataset;
use pcs_core::pipeline::Pipeline;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;

use pcs_service::service::builder::ServiceBuilder;
use pcs_service::service::config::ServiceConfig;
use pcs_service::service::factories::register_builtin_factories;
use pcs_service::service::run_standalone;
use pcs_transformer_ndjson::NdjsonTransformer;

const COMPONENT: &str = "Order";

/// Start a single-node KRaft Kafka broker and return it with its
/// `bootstrap.servers` value, or `None` when Docker is unavailable.
async fn start_kafka() -> Option<(ContainerAsync<GenericImage>, String)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).ok()?;
    let port = listener.local_addr().ok()?.port();
    drop(listener);

    let image = GenericImage::new("apache/kafka", "3.9.0")
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://127.0.0.1:{port}"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_mapped_port(port, 9092.tcp());

    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: kafka container unavailable: {e}");
            return None;
        }
    };
    let brokers = format!("127.0.0.1:{port}");

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &brokers);
        if let Ok(consumer) = cfg.create::<BaseConsumer>()
            && consumer
                .fetch_metadata(None, Duration::from_secs(5))
                .is_ok()
        {
            return Some((container, brokers));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("SKIP: kafka broker never accepted a connection");
    None
}

/// A topic name unique to this test run.
fn unique_topic(stem: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    format!("{stem}-{nanos}")
}

/// The component schema both the source and the sink speak.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("total", DataType::Float64, true),
    ]))
}

/// A passthrough pipeline: it registers the component and runs no systems,
/// so this test proves the registry-driven IO path, not transform logic
/// (already covered by `crates/pcs-connector-kafka`'s own tests).
fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("kafka_service");
    pipeline.data.register_raw_component(COMPONENT, schema());
    pipeline
}

/// The one-shot service config this test loads.
///
/// `poll_timeout_ms` is the whole budget the `stop_at_end` source has to join
/// its consumer group, get a partition assignment and fetch: one elapsed window
/// returns an empty buffer, and the run then reports zero rows processed rather
/// than an error. A cold group's JoinGroup/SyncGroup round trip is slow enough
/// under a full parallel suite (every Docker-backed test on one daemon) that a
/// few seconds is not a budget, it is a coin flip. Nothing waits out the window
/// on the happy path: `PartitionEOF` ends the drain as soon as the topic is
/// exhausted.
fn config_kdl(brokers: &str, input_topic: &str, output_topic: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="pcs-kafka-service" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "kafka-service-test" {{
    transformer "ndjson_fmt" format="ndjson"

    source "orders_in" type="KafkaSource" component="{COMPONENT}" transformer="ndjson_fmt" {{
        config {{
            brokers "{brokers}"
            topic "{input_topic}"
            group_id "kafka-service-test"
            stop_at_end #true
            poll_timeout_ms 20000

            schema_fields "id" type="int64" nullable=#false
            schema_fields "name" type="utf8"
            schema_fields "total" type="float64"
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="KafkaSink" component="{COMPONENT}" transformer="ndjson_fmt" {{
        config {{
            brokers "{brokers}"
            topic "{output_topic}"

            schema_fields "id" type="int64" nullable=#false
            schema_fields "name" type="utf8"
            schema_fields "total" type="float64"
        }}
    }}

    link from="orders_in" to="transform"
    link from="transform" to="orders_out"
}}

http disabled=#true

observability log_level="warn"
"#
    )
}

#[tokio::test]
async fn a_kafka_source_and_sink_move_rows_through_the_standalone_runner() {
    let Some((_container, brokers)) = start_kafka().await else {
        return;
    };
    let input_topic = unique_topic("kafka-service-in");
    let output_topic = unique_topic("kafka-service-out");
    let schema = schema();

    // Seed the input topic directly through the connector, not the registry:
    // the registry path is what this test exists to prove, so seeding must
    // not depend on it.
    let mut seed_sink = KafkaSink::new(
        KafkaSinkConfig {
            brokers: brokers.clone(),
            topic: input_topic.clone(),
            key_field: None,
            tombstones: false,
            flush_timeout_ms: 30_000,
            provision: TopicProvision::default(),
            properties: Default::default(),
            schema_fields: vec![],
        },
        schema.clone(),
        Arc::new(NdjsonTransformer::default()),
    )
    .expect("seed sink builds");
    let seed_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
        ],
    )
    .expect("valid batch");
    {
        use pcs_core::io::sink::Sink;
        seed_sink
            .write_batch(&seed_batch)
            .await
            .expect("seed write");
        seed_sink.finish().await.expect("seed finish");
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let path = dir.path().join("service.kdl");
    std::fs::write(
        &path,
        config_kdl(&brokers, &input_topic, &output_topic, &data_dir),
    )
    .expect("write config");

    let config = ServiceConfig::load(&path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("transform", Box::new(build_pipeline()))
        .build_all(&config)
        .expect("service builds")
        .remove(0);

    let stats = run_standalone(built, &config, CancellationToken::new(), None, None)
        .await
        .expect("one-shot run");
    assert_eq!(stats.iterations, 1);
    assert_eq!(stats.rows_processed, 3, "every seeded row was drained");
    assert_eq!(stats.iteration_errors, 0);
    assert!(stats.sink_batches_written >= 1);

    // Read the output topic back through the connector, proving the
    // registry-built `KafkaSink` actually reached the broker.
    let mut read_source = pcs_connector_kafka::KafkaSource::new(
        pcs_connector_kafka::KafkaSourceConfig {
            brokers,
            topic: output_topic,
            group_id: "kafka-service-readback".to_string(),
            batch_size: 1000,
            poll_timeout_ms: 5_000,
            auto_offset_reset: "earliest".to_string(),
            commit_on_drain: true,
            stop_at_end: true,
            key_field: None,
            compacted: false,
            provision: TopicProvision::default(),
            properties: Default::default(),
            schema_fields: vec![],
        },
        schema.clone(),
        Arc::new(NdjsonTransformer::default()),
    )
    .expect("readback source builds");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema);
    let rows = pcs_core::io::source::drain_into_dataset(&mut read_source, &mut dataset, COMPONENT)
        .await
        .expect("drain output topic");
    assert_eq!(rows, 3);

    let out = dataset.batch_for(COMPONENT).expect("component present");
    let ids = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64");
    assert_eq!(ids.values(), &[1, 2, 3]);
}

/// The pipeline must actually declare the component the config names, or the
/// assertions above would pass on an empty run.
#[test]
fn the_pipeline_declares_the_component_the_config_names() {
    let pipeline = build_pipeline();
    let declared = pcs_core::runtime::PipelineRuntime::declared_components(&pipeline);
    assert!(declared.contains(&COMPONENT), "declared: {declared:?}");
}
