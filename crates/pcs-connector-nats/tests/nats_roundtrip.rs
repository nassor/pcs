//! [`NatsSource`] and [`NatsSink`] against a real NATS server.
//!
//! Soft-skips without Docker; see `common::try_start`. Each test uses its own
//! subject and stream name so the whole suite can share the one server.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures_util::StreamExt;

use pcs_connector_nats::{
    ConnectionConfig, CoreSinkMode, CoreSourceMode, DeliverPolicyConfig, JetstreamSinkMode,
    JetstreamSourceMode, NatsSink, NatsSinkConfig, NatsSource, NatsSourceConfig, SinkMode,
    SourceMode, StreamProvision,
};
use pcs_core::dataset::Dataset;
use pcs_core::io::sink::Sink;
use pcs_core::io::source::{Source, drain_into_dataset};
use pcs_transformer::Transformer;
use pcs_transformer_arrow_ipc::ArrowIpcTransformer;
use pcs_transformer_ndjson::NdjsonTransformer;

const COMPONENT: &str = "TestRow";

/// The default payload format. The factories resolve it from the registry; a
/// direct construction like these tests' hands it over explicitly.
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

fn connection(url: &str) -> ConnectionConfig {
    ConnectionConfig {
        servers: vec![url.to_string()],
        ..ConnectionConfig::default()
    }
}

/// A bounded core source, so `drain_into_dataset` terminates.
fn core_source_cfg(url: &str, subject: &str) -> NatsSourceConfig {
    NatsSourceConfig {
        connection: connection(url),
        mode: SourceMode::Core(CoreSourceMode {
            subject: subject.to_string(),
            queue_group: None,
        }),
        batch_size: 1000,
        poll_timeout_ms: 2_000,
        stop_at_end: true,
        schema_fields: vec![],
    }
}

fn core_sink_cfg(url: &str, subject: &str) -> NatsSinkConfig {
    NatsSinkConfig {
        connection: connection(url),
        mode: SinkMode::Core(CoreSinkMode {
            subject: subject.to_string(),
            ..CoreSinkMode::default()
        }),
        schema_fields: vec![],
    }
}

/// A bounded JetStream source over a durable pull consumer.
fn js_source_cfg(url: &str, stream: &str, durable: &str) -> NatsSourceConfig {
    NatsSourceConfig {
        connection: connection(url),
        mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
            stream: stream.to_string(),
            durable_name: Some(durable.to_string()),
            fetch_expires_ms: 2_000,
            ..JetstreamSourceMode::default()
        })),
        batch_size: 1000,
        poll_timeout_ms: 2_000,
        stop_at_end: true,
        schema_fields: vec![],
    }
}

/// A JetStream sink over `subject`. It names no `stream_provision`, so it
/// exercises the defaults: the stream is created, and its subject list is
/// derived from `subject`.
fn js_sink_cfg(url: &str, stream: &str, subject: &str) -> NatsSinkConfig {
    NatsSinkConfig {
        connection: connection(url),
        mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
            stream: stream.to_string(),
            subject: subject.to_string(),
            ..JetstreamSinkMode::default()
        })),
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

/// Every id the source read, in order.
fn ids_of(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64")
        .values()
        .to_vec()
}

#[tokio::test]
async fn core_ndjson_round_trips_through_a_subject() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("core-json");
    let schema = schema();

    // Core NATS drops a message with no subscriber, so the source subscribes
    // before anything is published.
    let mut source = NatsSource::new(
        core_source_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    assert!(
        source.next_batch().await.expect("empty poll").is_none(),
        "an empty subject with stop_at_end reports EOF, and subscribes on the way"
    );

    let mut sink = NatsSink::new(
        core_sink_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let rows = drain_into_dataset(&mut source, &mut dataset, COMPONENT)
        .await
        .expect("drain");
    assert_eq!(rows, 3);
    assert_sample_values(dataset.batch_for(COMPONENT).expect("component present"));
}

#[tokio::test]
async fn core_arrow_ipc_sends_one_message_per_batch() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("core-arrow-ipc");
    let schema = schema();

    // A raw subscription counts the messages, which is what proves arrow-ipc
    // sent one for the whole batch rather than one per row.
    let client = nats.client().await;
    let mut raw = client
        .subscribe(subject.clone())
        .await
        .expect("raw subscribe");

    let mut source = NatsSource::new(
        core_source_cfg(&nats.url(), &subject),
        schema.clone(),
        arrow_ipc(),
    )
    .expect("source builds");
    assert!(source.next_batch().await.expect("empty poll").is_none());

    let mut sink = NatsSink::new(
        core_sink_cfg(&nats.url(), &subject),
        schema.clone(),
        arrow_ipc(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let message = tokio::time::timeout(Duration::from_secs(5), raw.next())
        .await
        .expect("one message arrives")
        .expect("the subscription is live");
    assert_eq!(message.subject.as_str(), subject);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), raw.next())
            .await
            .is_err(),
        "arrow-ipc emits one message per batch, so there is no second one"
    );

    let out = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("the batch arrived");
    assert_eq!(out.schema().fields(), schema.fields());
    assert_sample_values(&out);
}

#[tokio::test]
async fn core_subject_field_routes_each_row_to_its_own_subject() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let base = nats.subject("core-route");
    let schema = schema();
    let client = nats.client().await;
    let mut first = client
        .subscribe(format!("{base}.a"))
        .await
        .expect("subscribe a");
    let mut second = client
        .subscribe(format!("{base}.b"))
        .await
        .expect("subscribe b");

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Core(CoreSinkMode {
                subject: format!("{base}.fallback"),
                subject_field: Some("name".to_string()),
                ..CoreSinkMode::default()
            }),
            ..core_sink_cfg(&nats.url(), &base)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    // A rendered cell replaces the configured subject outright, so the column
    // holds the whole subject.
    sink.write_batch(&batch_of(
        schema,
        &[1, 2],
        &[&format!("{base}.a"), &format!("{base}.b")],
        &[1.5, 2.5],
    ))
    .await
    .expect("write_batch");
    sink.finish().await.expect("finish");

    for (label, subscriber) in [("a", &mut first), ("b", &mut second)] {
        let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
            .await
            .unwrap_or_else(|_| panic!("a message must reach {base}.{label}"))
            .expect("the subscription is live");
        assert_eq!(message.subject.as_str(), format!("{base}.{label}"));
    }
}

#[tokio::test]
async fn core_header_fields_and_static_headers_reach_the_message() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("core-headers");
    let schema = schema();
    let client = nats.client().await;
    let mut raw = client
        .subscribe(subject.clone())
        .await
        .expect("raw subscribe");

    let mut headers = BTreeMap::new();
    headers.insert("X-Producer".to_string(), "pcs".to_string());
    let mut header_fields = BTreeMap::new();
    header_fields.insert("X-Id".to_string(), "id".to_string());

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Core(CoreSinkMode {
                subject: subject.clone(),
                headers,
                header_fields,
                ..CoreSinkMode::default()
            }),
            ..core_sink_cfg(&nats.url(), &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&batch_of(schema, &[7], &["a"], &[1.5]))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let message = tokio::time::timeout(Duration::from_secs(5), raw.next())
        .await
        .expect("one message arrives")
        .expect("the subscription is live");
    let got = message.headers.expect("the message carries headers");
    assert_eq!(
        got.get("X-Producer").map(|v| v.as_str()),
        Some("pcs"),
        "the static header must be on every message"
    );
    assert_eq!(
        got.get("X-Id").map(|v| v.as_str()),
        Some("7"),
        "the rendered cell must become the header value"
    );
}

#[tokio::test]
async fn a_queue_group_splits_a_subject_between_two_sources() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("core-queue");
    let schema = schema();
    let group = "pcs-queue".to_string();

    let mut first = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Core(CoreSourceMode {
                subject: subject.clone(),
                queue_group: Some(group.clone()),
            }),
            ..core_source_cfg(&nats.url(), &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("first source builds");
    let mut second = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Core(CoreSourceMode {
                subject: subject.clone(),
                queue_group: Some(group),
            }),
            ..core_source_cfg(&nats.url(), &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("second source builds");
    // A first poll subscribes; the stream runner primes every source's first
    // poll before the round-robin blocks on any of them, so both
    // subscriptions exist before publishing starts.
    assert!(first.next_batch().await.expect("empty poll").is_none());
    assert!(second.next_batch().await.expect("empty poll").is_none());

    let mut sink = NatsSink::new(
        core_sink_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    let ids: Vec<i64> = (1..=8).collect();
    let names: Vec<&str> = ids.iter().map(|_| "x").collect();
    let totals: Vec<f64> = ids.iter().map(|i| *i as f64).collect();
    sink.write_batch(&batch_of(schema, &ids, &names, &totals))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut seen = Vec::new();
    for source in [&mut first, &mut second] {
        while let Some(batch) = source.next_batch().await.expect("next_batch") {
            seen.extend(ids_of(&batch));
        }
    }
    seen.sort_unstable();
    assert_eq!(
        seen, ids,
        "a queue group must deliver every message exactly once across the group"
    );
}

#[tokio::test]
async fn jetstream_round_trips_through_a_provisioned_stream() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_ROUNDTRIP");
    let subject = nats.subject("js-roundtrip");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    // A stream that did not exist before the sink ran now holds the three
    // messages, which is what `stream_provision.create = true` bought.
    let mut info = nats
        .jetstream()
        .await
        .get_stream(&stream)
        .await
        .expect("the sink provisioned the stream");
    assert_eq!(
        info.info().await.expect("stream info").state.messages,
        3,
        "ndjson emits one message per row"
    );

    let mut source = NatsSource::new(
        js_source_cfg(&nats.url(), &stream, "pcs-roundtrip"),
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
async fn jetstream_acks_the_previous_batch_at_the_start_of_the_next_call() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_ACK");
    let subject = nats.subject("js-ack");
    let schema = schema();
    let durable = "pcs-ack";

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = NatsSource::new(
        js_source_cfg(&nats.url(), &stream, durable),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let batch = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("three messages are waiting");
    assert_eq!(batch.num_rows(), 3);

    let js = nats.jetstream().await;
    let stream_handle = js.get_stream(&stream).await.expect("stream exists");
    let outstanding = stream_handle
        .consumer_info(durable)
        .await
        .expect("consumer info")
        .num_ack_pending;
    assert_eq!(
        outstanding, 3,
        "the acks must trail the handover by one call, which is what makes \
         delivery at-least-once"
    );

    assert!(
        source
            .next_batch()
            .await
            .expect("second next_batch")
            .is_none(),
        "the stream is drained"
    );
    let outstanding = stream_handle
        .consumer_info(durable)
        .await
        .expect("consumer info")
        .num_ack_pending;
    assert_eq!(
        outstanding, 0,
        "the second call must have acknowledged the first batch"
    );
}

#[tokio::test]
async fn a_duplicate_message_id_is_deduplicated_inside_the_window() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_DEDUPE");
    let subject = nats.subject("js-dedupe");
    let schema = schema();

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
                stream: stream.clone(),
                subject: subject.clone(),
                message_id_field: Some("id".to_string()),
                // Only the duplicate window is named; `create` and the subject
                // list are the defaults.
                stream_provision: StreamProvision {
                    duplicate_window_ms: 120_000,
                    ..StreamProvision::default()
                },
                ..JetstreamSinkMode::default()
            })),
            ..js_sink_cfg(&nats.url(), &stream, &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("first write");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("second write of the same rows");
    sink.finish().await.expect("finish");

    let mut info = nats
        .jetstream()
        .await
        .get_stream(&stream)
        .await
        .expect("stream exists");
    assert_eq!(
        info.info().await.expect("stream info").state.messages,
        3,
        "the duplicate window must drop the second copy of every Nats-Msg-Id"
    );
}

#[tokio::test]
async fn deliver_policy_new_skips_what_was_already_in_the_stream() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_NEW");
    let subject = nats.subject("js-new");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&batch_of(schema.clone(), &[1, 2], &["a", "b"], &[1.0, 2.0]))
        .await
        .expect("seed write");

    // Creating the consumer is what fixes "new", so the source must start
    // before the second write and after the first.
    let mut source = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
                stream: stream.clone(),
                durable_name: Some("pcs-new".to_string()),
                deliver_policy: DeliverPolicyConfig::New,
                fetch_expires_ms: 2_000,
                ..JetstreamSourceMode::default()
            })),
            ..js_source_cfg(&nats.url(), &stream, "pcs-new")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    assert!(
        source.next_batch().await.expect("first poll").is_none(),
        "kind = \"new\" must skip the seeded rows"
    );

    sink.write_batch(&batch_of(schema, &[3], &["c"], &[3.0]))
        .await
        .expect("second write");
    sink.finish().await.expect("finish");

    let batch = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("the row published after the consumer was created");
    assert_eq!(ids_of(&batch), vec![3]);
}

#[tokio::test]
async fn stop_at_end_reports_eof_once_the_stream_is_drained() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_EOF");
    let subject = nats.subject("js-eof");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = NatsSource::new(
        js_source_cfg(&nats.url(), &stream, "pcs-eof"),
        schema,
        ndjson(),
    )
    .expect("source builds");
    assert_eq!(
        source
            .next_batch()
            .await
            .expect("first next_batch")
            .expect("three messages are waiting")
            .num_rows(),
        3
    );
    assert!(
        source
            .next_batch()
            .await
            .expect("second next_batch")
            .is_none(),
        "a drained stream with stop_at_end reports EOF"
    );
}

#[tokio::test]
async fn an_empty_window_is_not_eof_while_the_consumer_still_owes_messages() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_OWED");
    let subject = nats.subject("js-owed");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    // `ack_wait` deliberately outlasts one `fetch_expires_ms` window: a
    // redelivery therefore cannot land inside the single window a source that
    // trusts an empty window would have asked for.
    let cfg = |url: &str| NatsSourceConfig {
        mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
            stream: stream.clone(),
            durable_name: Some("pcs-owed".to_string()),
            fetch_expires_ms: 1_000,
            ack_wait_ms: 4_000,
            ..JetstreamSourceMode::default()
        })),
        poll_timeout_ms: 3_000,
        ..js_source_cfg(url, &stream, "pcs-owed")
    };

    // One source takes the whole stream and is dropped without ever
    // acknowledging it: the server counts all three messages as delivered and
    // holds them until `ack_wait` expires. That is the state a window nobody
    // read leaves behind, and from the next consumer's side it is
    // indistinguishable from a drained stream until the server is asked.
    let mut abandoned =
        NatsSource::new(cfg(&nats.url()), schema.clone(), ndjson()).expect("source builds");
    let taken = abandoned
        .next_batch()
        .await
        .expect("first next_batch")
        .expect("three messages are waiting");
    assert_eq!(ids_of(&taken), vec![1, 2, 3]);
    drop(abandoned);

    let mut resumed =
        NatsSource::new(cfg(&nats.url()), schema.clone(), ndjson()).expect("source builds");
    let redelivered = tokio::time::timeout(Duration::from_secs(20), resumed.next_batch())
        .await
        .expect("the confirm-drained budget covers one ack_wait")
        .expect("next_batch")
        .expect("a stream that still owes three messages must not report EOF");
    assert_eq!(
        ids_of(&redelivered),
        vec![1, 2, 3],
        "every message the consumer still owed must come back"
    );
}

#[tokio::test]
async fn a_missing_stream_names_itself_when_provisioning_is_off() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_MISSING");
    let schema = schema();

    let mut source = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
                stream: stream.clone(),
                durable_name: Some("pcs-missing".to_string()),
                stream_provision: StreamProvision {
                    create: false,
                    ..StreamProvision::default()
                },
                ..JetstreamSourceMode::default()
            })),
            ..js_source_cfg(&nats.url(), &stream, "pcs-missing")
        },
        schema,
        ndjson(),
    )
    .expect("source builds: it opens nothing");
    let err = source
        .next_batch()
        .await
        .expect_err("create = false against a stream that does not exist");
    assert_eq!(err.category(), "generic");
    assert!(err.message().contains(&stream), "got: {err}");
    assert!(
        err.message().contains("stream_provision.create = true"),
        "the error must name the opt-in that would have created it, got: {err}"
    );
}

/// The flipped default is load bearing: a source pointed at a stream nobody has
/// created yet creates it, rather than failing.
#[tokio::test]
async fn a_source_creates_its_stream_by_default() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_SOURCE_CREATES");
    let subject = nats.subject("js-source-creates");
    let schema = schema();

    let mut source = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
                stream: stream.clone(),
                durable_name: Some("pcs-creates".to_string()),
                // The subject list of the created stream comes from here.
                filter_subjects: vec![subject.clone()],
                fetch_expires_ms: 2_000,
                ..JetstreamSourceMode::default()
            })),
            ..js_source_cfg(&nats.url(), &stream, "pcs-creates")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    assert!(
        source.next_batch().await.expect("first poll").is_none(),
        "a freshly created stream is empty, so this is EOF and not an error"
    );

    let mut info = nats
        .jetstream()
        .await
        .get_stream(&stream)
        .await
        .expect("the source provisioned the stream");
    assert_eq!(
        info.info().await.expect("stream info").config.subjects,
        vec![subject.clone()],
        "filter_subjects supplied the created stream's subject list"
    );

    // And the stream it made actually captures what the source reads.
    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("write");
    sink.finish().await.expect("finish");

    let batch = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("the rows the sink published");
    assert_eq!(ids_of(&batch), vec![1, 2, 3]);
}

#[tokio::test]
async fn estimated_rows_reports_what_jetstream_still_owes() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_PENDING");
    let subject = nats.subject("js-pending");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = NatsSource::new(
        NatsSourceConfig {
            batch_size: 1,
            ..js_source_cfg(&nats.url(), &stream, "pcs-pending")
        },
        schema,
        ndjson(),
    )
    .expect("source builds");
    assert_eq!(
        source.estimated_rows(),
        None,
        "nothing has been pulled yet, so there is no number to report"
    );
    source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("one message");
    assert_eq!(
        source.estimated_rows(),
        Some(2),
        "two of the three messages are still waiting for this consumer"
    );
}
