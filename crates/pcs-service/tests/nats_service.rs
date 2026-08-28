//! The whole standalone service path over NATS: config file, factories, source
//! drain, pipeline run, sink drain.
//!
//! `run_standalone` is what the `pcs-service serve` subcommand calls, so this is
//! the end to end proof that a `connector-nats` build moves rows from one
//! JetStream stream through a pipeline into another via the registered
//! `NatsSourceFactory`/`NatsSinkFactory`, not just via the connector crate's own
//! direct API (already covered by `pcs-connector-nats`'s own tests).
//!
//! Soft-skips without Docker. The `is_live_source` test needs no daemon.

#![cfg(all(feature = "service", feature = "connector-nats"))]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pcs_connector_nats::{
    ConnectionConfig, JetstreamSinkMode, JetstreamSourceMode, NatsSink, NatsSinkConfig, NatsSource,
    NatsSourceConfig, SinkMode, SourceMode,
};
use pcs_core::dataset::Dataset;
use pcs_core::pipeline::Pipeline;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;

use pcs_service::service::builder::ServiceBuilder;
use pcs_service::service::config::ServiceConfig;
use pcs_service::service::factories::register_builtin_factories;
use pcs_service::service::run_standalone;
use pcs_transformer_ndjson::NdjsonTransformer;

const COMPONENT: &str = "Order";

/// Start a single-node NATS server with JetStream on and return it with its URL,
/// or `None` when Docker is unavailable.
async fn start_nats() -> Option<(ContainerAsync<GenericImage>, String)> {
    let image = GenericImage::new("nats", "2.11-alpine")
        .with_exposed_port(4222_u16.tcp())
        .with_cmd(["-js", "-sd", "/tmp/nats"]);

    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: nats container unavailable: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(4222_u16.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("SKIP: nats container port unavailable: {e}");
            return None;
        }
    };
    let url = format!("nats://127.0.0.1:{port}");

    // A real connect plus a JetStream API round-trip is a stricter readiness
    // gate than any log line: `-js` starts the subsystem after the client port
    // is already open.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(client) = async_nats::connect(&url).await {
            let js = async_nats::jetstream::new(client);
            match js.get_stream("PCS_PROBE").await {
                Err(e) if e.to_string().contains("timed out") => {}
                _ => return Some((container, url)),
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("SKIP: nats server never answered the JetStream API");
    None
}

/// A name unique to this test run. Stream names admit no `.`.
fn unique(stem: &str, separator: char) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    format!("{stem}{separator}{nanos}")
}

/// The component schema both the source and the sink speak.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("total", DataType::Float64, true),
    ]))
}

/// A passthrough pipeline: it registers the component and runs no systems, so
/// this test proves the registry-driven IO path, not transform logic (already
/// covered by `crates/pcs-connector-nats`'s own tests).
fn build_pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("nats_service");
    pipeline.data.register_raw_component(COMPONENT, schema());
    pipeline
}

fn config_kdl(
    url: &str,
    in_stream: &str,
    out_stream: &str,
    out_subject: &str,
    data_dir: &str,
    stop_at_end: bool,
) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="pcs-nats-service" data_dir="{data_dir}"

run_mode kind="one_shot"

workflow "nats-service-test" {{
    transformer "ndjson_fmt" format="ndjson"

    source "orders_in" type="NatsSource" component="{COMPONENT}" transformer="ndjson_fmt" {{
        config {{
            stop_at_end #{stop_at_end}
            poll_timeout_ms 2000

            connection {{
                servers "{url}"
            }}

            mode kind="jetstream" {{
                stream "{in_stream}"
                durable_name "pcs-service-in"
                fetch_expires_ms 2000
            }}

            schema_fields "id" type="int64" nullable=#false
            schema_fields "name" type="utf8"
            schema_fields "total" type="float64"
        }}
    }}

    wasm "transform" name="Transform"

    sink "orders_out" type="NatsSink" component="{COMPONENT}" transformer="ndjson_fmt" {{
        config {{
            connection {{
                servers "{url}"
            }}

            mode kind="jetstream" {{
                stream "{out_stream}"
                subject "{out_subject}"
            }}

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

fn connection(url: &str) -> ConnectionConfig {
    ConnectionConfig {
        servers: vec![url.to_string()],
        ..ConnectionConfig::default()
    }
}

#[tokio::test]
async fn a_nats_source_and_sink_move_rows_through_the_standalone_runner() {
    let Some((_container, url)) = start_nats().await else {
        return;
    };
    let in_stream = unique("PCS_SERVICE_IN", '_');
    let in_subject = unique("pcs.service.in", '.');
    let out_stream = unique("PCS_SERVICE_OUT", '_');
    let out_subject = unique("pcs.service.out", '.');
    let schema = schema();

    // Seed the input stream directly through the connector, not the registry:
    // the registry path is what this test exists to prove, so seeding must not
    // depend on it.
    let mut seed_sink = NatsSink::new(
        NatsSinkConfig {
            connection: connection(&url),
            mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
                stream: in_stream.clone(),
                subject: in_subject.clone(),
                ..JetstreamSinkMode::default()
            })),
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
        config_kdl(&url, &in_stream, &out_stream, &out_subject, &data_dir, true),
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

    // Read the output stream back through the connector, proving the
    // registry-built `NatsSink` actually reached the server.
    let mut read_source = NatsSource::new(
        NatsSourceConfig {
            connection: connection(&url),
            mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
                stream: out_stream,
                durable_name: Some("pcs-service-readback".to_string()),
                fetch_expires_ms: 2_000,
                ..JetstreamSourceMode::default()
            })),
            batch_size: 1000,
            poll_timeout_ms: 2_000,
            stop_at_end: true,
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
        .expect("drain output stream");
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

/// `is_live_source` must know `NatsSource`, or a `one_shot` config with a live
/// consumer would be accepted and then block forever. No daemon needed.
#[test]
fn a_nats_source_without_stop_at_end_is_refused_outside_stream_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().display().to_string().replace('\\', "/");
    let path = dir.path().join("service.kdl");

    let live = config_kdl(
        "nats://127.0.0.1:4222",
        "IN",
        "OUT",
        "out.subject",
        &data_dir,
        false,
    );
    std::fs::write(&path, &live).expect("write config");
    let err = ServiceConfig::load(&path).expect_err("a live source cannot run one_shot");
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("never reaches EOF"), "got: {err}");
    assert!(err.message().contains("NatsSource"), "got: {err}");

    let bounded = live.replace("stop_at_end #false", "stop_at_end #true");
    std::fs::write(&path, bounded).expect("write config");
    ServiceConfig::load(&path).expect("stop_at_end makes the source drivable by one_shot");
}

/// The shipped `examples/configs/nats.kdl` must parse and its two IO tables
/// must be accepted by the factories.
///
/// Gated on `wasm` because the example declares a `wasm` node, which the
/// config parser only knows about with that feature on. The factories are
/// driven directly rather than through `ServiceBuilder::build_all`, which would also
/// try to load the module the example points at. No daemon needed: both
/// factories open no connection.
#[cfg(feature = "wasm")]
#[test]
fn the_shipped_example_config_loads_and_its_io_tables_are_accepted() {
    use pcs_service::service::ConnectorContext;
    use pcs_service::service::factories::{NatsSinkFactory, NatsSourceFactory};
    use pcs_service::service::registry::{SinkFactory, SourceFactory};

    let config = ServiceConfig::load("../../examples/configs/nats.kdl")
        .expect("the shipped example must load");
    assert_eq!(config.workflows[0].sources.len(), 1);
    assert_eq!(config.workflows[0].sinks.len(), 1);
    assert_eq!(config.workflows[0].sources[0].type_name, "NatsSource");
    assert_eq!(config.workflows[0].sinks[0].type_name, "NatsSink");

    let ctx = ConnectorContext::new(Some(Arc::new(NdjsonTransformer::default())));
    let source = NatsSourceFactory
        .build(&config.workflows[0].sources[0].config, &ctx)
        .expect("the example source table must be accepted");
    let sink = NatsSinkFactory
        .build(&config.workflows[0].sinks[0].config, &ctx)
        .expect("the example sink table must be accepted");
    assert_eq!(source.schema().fields().len(), 3);
    assert_eq!(sink.schema().fields().len(), 3);
}
