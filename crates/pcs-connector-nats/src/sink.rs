//! [`NatsSink`]: a core NATS subject publisher or a JetStream publisher
//! [`Sink`].
//!
//! The resolved format's [`MessageShape`] decides whether a batch becomes one
//! message per row or one message in total. Only a row-per-message format can
//! honour `subject_field`, `header_fields` and `message_id_field`.
//!
//! # Delivery semantics
//!
//! A JetStream publish is acknowledged by the stream, and `write_batch` waits
//! for every ack by default, so a returned `write_batch` means the stream has
//! the rows. Core NATS has no per-message ack; `flush_every_batch` waits for the
//! server to acknowledge the whole write instead, which is the strongest
//! boundary the protocol offers.

use std::future::IntoFuture;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::{self, context::PublishAckFuture};
use async_nats::{Client, HeaderMap, HeaderName, HeaderValue, Subject, header};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;

use pcs_core::error::PcsError;
use pcs_core::io::sink::Sink;
use pcs_transformer::{MessageShape, Transformer};

use crate::config::{NatsSinkConfig, SinkMode};
use crate::connect::{connect, jetstream_context};
use crate::provision::resolve_stream;
use crate::render::render_column;

const WHAT: &str = "NatsSink";

/// Everything the first `write_batch` opened.
enum Started {
    Core {
        client: Client,
    },
    /// The context owns its own `Client` clone, so the connection lives as long
    /// as the context does.
    Jetstream {
        context: jetstream::Context,
    },
}

/// NATS [`Sink`]: one publish per encoded message.
///
/// Connects lazily: [`new`](Self::new) validates the config and opens nothing,
/// so `pcs-service validate` stays broker-free. The first
/// [`write_batch`](Sink::write_batch) connects and, in JetStream mode, resolves
/// the stream.
pub struct NatsSink {
    cfg: NatsSinkConfig,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    /// Cached from the transformer at construction, where the capability check
    /// already proved it is `Some`.
    shape: MessageShape,
    /// The subject a `PerBatch` format always uses, and the fallback when a
    /// rendered `subject_field` cell is null.
    default_subject: Subject,
    /// The `[headers]` table, plus `Nats-Expected-Stream` when configured.
    /// Parsed once: `HeaderMap::insert` panics on an illegal name or value.
    static_headers: HeaderMap,
    /// `header_fields`, with each header name already parsed.
    header_columns: Vec<(HeaderName, String)>,
    state: Option<Started>,
}

impl NatsSink {
    /// Validate the config and build the sink. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when `cfg` fails validation, the
    /// format has no message codec, or a per-row key is set against a format
    /// that emits one message per batch.
    pub fn new(
        cfg: NatsSinkConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, PcsError> {
        cfg.validate()?;

        // Both capability checks happen here rather than in `validate`: only
        // the resolved transformer knows its message shape.
        let format = transformer.format();
        let Some(shape) = transformer.message_shape() else {
            return Err(PcsError::configuration(format!(
                "{WHAT}: format '{format}' has no message codec"
            )));
        };

        let (subject, headers, header_fields, message_id_field, expected_stream) = match &cfg.mode {
            SinkMode::Core(core) => (
                &core.subject,
                &core.headers,
                &core.header_fields,
                None,
                None,
            ),
            SinkMode::Jetstream(js) => (
                &js.subject,
                &js.headers,
                &js.header_fields,
                js.message_id_field.as_deref(),
                js.expected_stream.then_some(js.stream.as_str()),
            ),
        };

        if shape == MessageShape::PerBatch
            && let Some(key) = [
                ("mode.subject_field", subject_field(&cfg.mode).is_some()),
                ("mode.header_fields", !header_fields.is_empty()),
                ("mode.message_id_field", message_id_field.is_some()),
            ]
            .into_iter()
            .find_map(|(key, set)| set.then_some(key))
        {
            return Err(PcsError::configuration(format!(
                "{WHAT} config: '{key}' needs a row-per-message format; '{format}' emits one \
                 message per batch"
            )));
        }

        // Every name and value here was proved legal by `cfg.validate`, so the
        // panicking `insert` cannot fire; the parses below keep that local.
        let mut static_headers = HeaderMap::new();
        for (name, value) in headers {
            static_headers.insert(header_name(name)?, header_value(value, name)?);
        }
        if let Some(stream) = expected_stream {
            static_headers.insert(
                header::NATS_EXPECTED_STREAM,
                header_value(stream, "mode.stream")?,
            );
        }
        let header_columns = header_fields
            .iter()
            .map(|(name, column)| Ok((header_name(name)?, column.clone())))
            .collect::<Result<Vec<_>, PcsError>>()?;

        let default_subject = Subject::from(subject.as_str());
        Ok(Self {
            cfg,
            schema,
            transformer,
            shape,
            default_subject,
            static_headers,
            header_columns,
            state: None,
        })
    }

    async fn ensure_started(&mut self) -> Result<(), PcsError> {
        if self.state.is_some() {
            return Ok(());
        }
        let client = connect(&self.cfg.connection, WHAT).await?;
        let started = match &self.cfg.mode {
            SinkMode::Core(_) => Started::Core { client },
            SinkMode::Jetstream(js) => {
                let context = jetstream_context(
                    client,
                    js.domain.as_deref(),
                    js.api_prefix.as_deref(),
                    Duration::from_millis(js.api_timeout_ms),
                    Duration::from_millis(js.ack_timeout_ms),
                    js.max_ack_inflight,
                    js.backpressure_on_inflight,
                );
                // Even with `create = false` this fetches the stream, so a
                // stream typo is a startup error rather than a black hole:
                // JetStream answers an unmatched subject with `no responders`.
                resolve_stream(
                    &context,
                    &js.stream,
                    &js.stream_provision,
                    // What this sink publishes to is the right subject set for a
                    // stream it creates.
                    std::slice::from_ref(&js.subject),
                    WHAT,
                )
                .await?;
                Started::Jetstream { context }
            }
        };
        self.state = Some(started);
        Ok(())
    }

    /// Encode the batch and publish every payload.
    async fn publish(&self, batch: &RecordBatch) -> Result<(), PcsError> {
        let payloads = self.transformer.encode_messages(batch)?;
        if self.shape == MessageShape::PerRow && payloads.len() != batch.num_rows() {
            return Err(PcsError::generic(format!(
                "{WHAT}: format '{}' produced {} messages for {} rows",
                self.transformer.format(),
                payloads.len(),
                batch.num_rows()
            )));
        }

        // Rendered once per batch, not once per message.
        let subjects = match subject_field(&self.cfg.mode) {
            None => None,
            Some(field) => Some(render_column(batch, field, WHAT, "subject_field")?),
        };
        let message_ids = match message_id_field(&self.cfg.mode) {
            None => None,
            Some(field) => Some(render_column(batch, field, WHAT, "message_id_field")?),
        };
        let mut header_values = Vec::with_capacity(self.header_columns.len());
        for (name, column) in &self.header_columns {
            header_values.push((name, render_column(batch, column, WHAT, "header_fields")?));
        }

        match &self.cfg.mode {
            SinkMode::Core(core) => {
                let Some(Started::Core { client }) = &self.state else {
                    return Err(not_started());
                };
                let reply = core.reply_subject.as_deref().map(Subject::from);
                for (i, payload) in payloads.iter().enumerate() {
                    let subject = self.subject_at(subjects.as_deref(), i);
                    let headers = self.headers_at(&header_values, message_ids.as_deref(), i)?;
                    let payload = Bytes::from(payload.clone());
                    let sent = match (&reply, headers) {
                        (None, None) => client.publish(subject.clone(), payload).await,
                        (None, Some(headers)) => {
                            client
                                .publish_with_headers(subject.clone(), headers, payload)
                                .await
                        }
                        (Some(reply), None) => {
                            client
                                .publish_with_reply(subject.clone(), reply.clone(), payload)
                                .await
                        }
                        (Some(reply), Some(headers)) => {
                            client
                                .publish_with_reply_and_headers(
                                    subject.clone(),
                                    reply.clone(),
                                    headers,
                                    payload,
                                )
                                .await
                        }
                    };
                    sent.map_err(|e| {
                        PcsError::generic(format!("{WHAT}: publish to '{subject}' failed: {e}"))
                    })?;
                }
                if core.flush_every_batch {
                    flush(client, core.flush_timeout_ms).await?;
                }
            }
            SinkMode::Jetstream(js) => {
                let Some(Started::Jetstream { context, .. }) = &self.state else {
                    return Err(not_started());
                };
                let mut acks: Vec<(Subject, PublishAckFuture)> = if js.await_ack {
                    Vec::with_capacity(payloads.len())
                } else {
                    Vec::new()
                };
                for (i, payload) in payloads.iter().enumerate() {
                    let subject = self.subject_at(subjects.as_deref(), i);
                    let mut message = PublishMessage::build().payload(Bytes::from(payload.clone()));
                    if let Some(headers) =
                        self.headers_at(&header_values, message_ids.as_deref(), i)?
                    {
                        message = message.headers(headers);
                    }
                    let ack = context
                        .send_publish(subject.clone(), message)
                        .await
                        .map_err(|e| {
                            PcsError::generic(format!("{WHAT}: publish to '{subject}' failed: {e}"))
                        })?;
                    if js.await_ack {
                        acks.push((subject, ack));
                    }
                    // Dropping the future instead hands it to the client's own
                    // background acker, so fire-and-forget leaks nothing.
                }
                let (subjects, futures): (Vec<_>, Vec<_>) = acks.into_iter().unzip();
                let results = join_all(futures.into_iter().map(IntoFuture::into_future)).await;
                for (subject, result) in subjects.into_iter().zip(results) {
                    result.map_err(|e| {
                        PcsError::generic(format!(
                            "{WHAT}: publish to '{subject}' was not acknowledged: {e}"
                        ))
                    })?;
                }
            }
        }
        Ok(())
    }

    /// The subject for message `i`: the rendered cell when it is not null, else
    /// the configured subject. A `PerBatch` format always takes the latter,
    /// because `subject_field` is refused for it.
    fn subject_at(&self, subjects: Option<&[Option<String>]>, i: usize) -> Subject {
        match subjects
            .and_then(|rendered| rendered.get(i))
            .and_then(Option::as_deref)
        {
            Some(cell) => Subject::from(cell),
            None => self.default_subject.clone(),
        }
    }

    /// The headers for message `i`, `None` when there are none to send.
    ///
    /// With no per-row headers the static map is cloned, or skipped entirely
    /// when it is empty, so the common case allocates nothing.
    fn headers_at(
        &self,
        header_values: &[(&HeaderName, Vec<Option<String>>)],
        message_ids: Option<&[Option<String>]>,
        i: usize,
    ) -> Result<Option<HeaderMap>, PcsError> {
        let dynamic_id = message_ids
            .and_then(|rendered| rendered.get(i))
            .and_then(Option::as_deref);
        if header_values.is_empty() && dynamic_id.is_none() {
            return Ok(if self.static_headers.is_empty() {
                None
            } else {
                Some(self.static_headers.clone())
            });
        }
        let mut headers = self.static_headers.clone();
        for (name, rendered) in header_values {
            if let Some(cell) = rendered.get(i).and_then(Option::as_deref) {
                headers.insert((*name).clone(), header_value(cell, name.as_ref())?);
            }
        }
        if let Some(id) = dynamic_id {
            headers.insert(header::NATS_MESSAGE_ID, header_value(id, "message_id")?);
        }
        Ok(Some(headers))
    }
}

/// `subject_field`, whichever mode is configured.
fn subject_field(mode: &SinkMode) -> Option<&str> {
    match mode {
        SinkMode::Core(core) => core.subject_field.as_deref(),
        SinkMode::Jetstream(js) => js.subject_field.as_deref(),
    }
}

/// `message_id_field`, which only JetStream has.
fn message_id_field(mode: &SinkMode) -> Option<&str> {
    match mode {
        SinkMode::Core(_) => None,
        SinkMode::Jetstream(js) => js.message_id_field.as_deref(),
    }
}

fn header_name(name: &str) -> Result<HeaderName, PcsError> {
    HeaderName::from_str(name).map_err(|e| {
        PcsError::configuration(format!(
            "{WHAT} config: '{name}' is not a legal NATS header name: {e}"
        ))
    })
}

/// A rendered cell reaches this too, so an illegal value from the data is an
/// error rather than the panic `HeaderMap::insert` would raise.
fn header_value(value: &str, key: &str) -> Result<HeaderValue, PcsError> {
    HeaderValue::from_str(value).map_err(|e| {
        PcsError::generic(format!(
            "{WHAT}: '{key}' value is not a legal NATS header value: {e}"
        ))
    })
}

async fn flush(client: &Client, timeout_ms: u64) -> Result<(), PcsError> {
    let timeout = Duration::from_millis(timeout_ms);
    match tokio::time::timeout(timeout, client.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(PcsError::generic(format!("{WHAT}: flush failed: {e}"))),
        Err(_elapsed) => Err(PcsError::generic(format!(
            "{WHAT}: flush timed out after {timeout_ms} ms"
        ))),
    }
}

fn not_started() -> PcsError {
    PcsError::generic(format!("{WHAT}: publish before start"))
}

#[async_trait]
impl Sink for NatsSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.ensure_started().await?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.publish(batch).await
    }

    async fn finish(&mut self) -> Result<(), PcsError> {
        match (&self.state, &self.cfg.mode) {
            // Core NATS has no per-message ack, so one last flush is the only
            // durability boundary left.
            (Some(Started::Core { client }), SinkMode::Core(core)) => {
                flush(client, core.flush_timeout_ms).await
            }
            // A JetStream `write_batch` already awaited its acks, and with
            // `await_ack = false` the client's own background acker owns what is
            // left, so there is nothing to drain here.
            _ => Ok(()),
        }
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
