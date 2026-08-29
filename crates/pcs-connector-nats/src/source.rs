//! [`NatsSource`]: a core NATS subscription or a JetStream pull consumer
//! [`Source`].
//!
//! Live by default: with `stop_at_end = false`,
//! [`next_batch`](Source::next_batch) blocks on the first message and never
//! returns `Ok(None)`, exactly like a live TCP ingestion source idling for a
//! connection. Setting `stop_at_end = true` makes it usable from the batch run
//! modes.
//!
//! # Delivery semantics
//!
//! JetStream is at-least-once. `Source` has no ack hook, so the acks for one
//! batch are sent at the start of the next `next_batch` call, not when the batch
//! is handed over; a crash between the two redelivers that batch.
//!
//! Core NATS is at-most-once and has no ack at all: a message consumed while the
//! pipeline later fails is gone. A `queue_group` spreads one core subject across
//! several PCS instances.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::message::Acker;
use async_nats::{Client, Subject, Subscriber};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::{SelectAll, select_all};
use tokio::time::{Instant, timeout_at};

use pcs_core::error::PcsError;
use pcs_core::io::source::Source;
use pcs_transformer::Transformer;

use crate::config::{AckPolicyConfig, NatsSourceConfig, SourceMode};
use crate::connect::{connect, jetstream_context};
use crate::provision::resolve_stream;

const WHAT: &str = "NatsSource";

/// How a collected JetStream message is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckMode {
    /// `ack_policy = "none"`: nothing is acknowledged, so no `Acker` is kept.
    Never,
    /// One `+ACK` per message, not waited for.
    Single,
    /// One `+ACK` per message, confirmed by the server.
    Double,
}

/// Where one collected payload came from, for a decode error's coordinates.
enum Origin {
    /// The core subject it arrived on.
    Subject(Subject),
    /// Its sequence in the stream.
    Sequence(u64),
}

/// One collected message: its payload, and enough to name it in an error.
struct Received {
    origin: Origin,
    payload: Bytes,
}

/// What the pull consumer needs per window, resolved once at start.
struct PullWindow {
    /// Stream name, for a decode error's coordinates.
    stream: String,
    /// Consumer name, same. Empty for an unnamed ephemeral consumer.
    consumer: String,
    /// How long a `stop_at_end` pull request stays open server-side.
    expires: Duration,
    /// Byte ceiling on one window. 0 leaves the library default.
    max_bytes: usize,
    /// Idle heartbeat interval. 0 leaves the library default.
    heartbeat: Duration,
}

/// Everything the first `next_batch` opened.
enum Started {
    Core {
        /// Held so the connection outlives an explicit unsubscribe.
        _client: Client,
        /// One subscriber per comma-separated subject, merged into one stream.
        subscribers: SelectAll<Subscriber>,
    },
    /// The consumer owns a JetStream context, which owns its own `Client` clone,
    /// so the connection lives as long as the consumer does. Boxed because a
    /// `PullConsumer` carries the server's whole consumer info.
    Jetstream {
        consumer: Box<PullConsumer>,
        window: PullWindow,
    },
}

/// NATS [`Source`]: one `RecordBatch` per collected window of messages.
///
/// Connects lazily: [`new`](Self::new) validates the config and opens nothing,
/// so `pcs-service validate` stays broker-free. The first
/// [`next_batch`](Source::next_batch) connects, provisions the stream when
/// asked to, and creates the subscription or consumer. The stream runner
/// primes every source's first poll at start, so a fan-in source's
/// subscription is open before the rotation blocks on any one of them; core
/// NATS drops a message published with no subscriber, so without that prime
/// the first message on a second fan-in subject is lost.
pub struct NatsSource {
    cfg: NatsSourceConfig,
    schema: Arc<Schema>,
    /// The payload codec. A decoder is opened per window rather than held for
    /// the source's life: `Source` is `Send + Sync`, and a decoder need only be
    /// `Send`.
    transformer: Arc<dyn Transformer>,
    /// How a JetStream message is acknowledged. [`AckMode::Never`] in core mode,
    /// which has no acks at all.
    ack_mode: AckMode,
    state: Option<Started>,
    /// Ackers for the batch already handed to the caller, sent at the start of
    /// the next call. This is what makes JetStream delivery at-least-once.
    pending_acks: Vec<Acker>,
    /// JetStream's own count of messages still waiting for this consumer.
    last_pending: Option<usize>,
}

impl NatsSource {
    /// Validate the config and build the source. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when `cfg` fails validation or
    /// `transformer` has no message codec.
    pub fn new(
        cfg: NatsSourceConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, PcsError> {
        cfg.validate()?;
        if transformer.message_shape().is_none() {
            return Err(PcsError::configuration(format!(
                "{WHAT}: format '{}' has no message codec",
                transformer.format()
            )));
        }
        let ack_mode = match &cfg.mode {
            SourceMode::Core(_) => AckMode::Never,
            SourceMode::Jetstream(js) => match (js.ack_policy, js.double_ack) {
                (AckPolicyConfig::None, _) => AckMode::Never,
                (_, true) => AckMode::Double,
                (_, false) => AckMode::Single,
            },
        };
        Ok(Self {
            cfg,
            schema,
            transformer,
            ack_mode,
            state: None,
            pending_acks: Vec::new(),
            last_pending: None,
        })
    }

    async fn ensure_started(&mut self) -> Result<(), PcsError> {
        if self.state.is_some() {
            return Ok(());
        }
        let client = connect(&self.cfg.connection, WHAT).await?;
        let started = match &self.cfg.mode {
            SourceMode::Core(core) => {
                let mut subscribers = Vec::new();
                for subject in core.subject.split(',').map(str::trim) {
                    let subject = Subject::from(subject);
                    let subscriber = match &core.queue_group {
                        Some(group) => client.queue_subscribe(subject.clone(), group.clone()).await,
                        None => client.subscribe(subject.clone()).await,
                    };
                    subscribers.push(subscriber.map_err(|e| {
                        PcsError::generic(format!("{WHAT}: cannot subscribe to '{subject}': {e}"))
                    })?);
                }
                Started::Core {
                    _client: client,
                    // A single subject stays a one-element merge, so the
                    // collect loop has one code path.
                    subscribers: select_all(subscribers),
                }
            }
            SourceMode::Jetstream(js) => {
                let context = jetstream_context(
                    client,
                    js.domain.as_deref(),
                    js.api_prefix.as_deref(),
                    Duration::from_millis(js.api_timeout_ms),
                    // The source publishes nothing, so the ack-inflight
                    // settings the sink exposes are irrelevant here and keep
                    // the client's own defaults.
                    Duration::from_secs(30),
                    5_000,
                    true,
                );
                let stream = resolve_stream(
                    &context,
                    &js.stream,
                    &js.stream_provision,
                    // What this source reads is the right subject set for a
                    // stream it creates.
                    &js.filter_subjects,
                    WHAT,
                )
                .await?;
                let config = js.to_consumer_config()?;
                let name = config.name.clone().unwrap_or_default();
                let consumer: PullConsumer = match &js.durable_name {
                    // A durable consumer is resumed if it already exists, which
                    // is what makes a restart continue where it left off.
                    Some(durable) => stream.get_or_create_consumer(durable, config).await,
                    None => stream.create_consumer(config).await,
                }
                .map_err(|e| {
                    PcsError::generic(format!(
                        "{WHAT}: cannot create consumer on stream '{}': {e}",
                        js.stream
                    ))
                })?;
                Started::Jetstream {
                    consumer: Box::new(consumer),
                    window: PullWindow {
                        stream: js.stream.clone(),
                        consumer: name,
                        expires: Duration::from_millis(js.fetch_expires_ms),
                        max_bytes: js.fetch_max_bytes,
                        heartbeat: Duration::from_millis(js.heartbeat_ms),
                    },
                }
            }
        };
        self.state = Some(started);
        Ok(())
    }

    /// Acknowledge the previous batch, if one is pending. Acknowledging here,
    /// at the start of the next call rather than when the batch was produced,
    /// is what makes delivery at-least-once.
    async fn flush_pending_acks(&mut self) -> Result<(), PcsError> {
        if self.ack_mode == AckMode::Never {
            self.pending_acks.clear();
            return Ok(());
        }
        for acker in self.pending_acks.drain(..) {
            let result = if self.ack_mode == AckMode::Double {
                acker.double_ack().await
            } else {
                acker.ack().await
            };
            result.map_err(|e| PcsError::generic(format!("{WHAT}: ack failed: {e}")))?;
        }
        Ok(())
    }

    async fn collect(&mut self) -> Result<Vec<Received>, PcsError> {
        let Self {
            cfg,
            ack_mode,
            state,
            pending_acks,
            last_pending,
            ..
        } = self;
        let state = state
            .as_mut()
            .ok_or_else(|| PcsError::generic(format!("{WHAT}: collect before start")))?;
        match state {
            Started::Core { subscribers, .. } => collect_core(subscribers, cfg).await,
            Started::Jetstream {
                consumer, window, ..
            } => {
                collect_jetstream(consumer, window, cfg, *ack_mode, pending_acks, last_pending)
                    .await
            }
        }
    }

    /// Feed a whole window through one decoder and take the batch out.
    ///
    /// Each push names the message it failed on, keeping per-message
    /// attribution; the transformer names the format that rejected it.
    fn decode_window(&self, received: &[Received]) -> Result<RecordBatch, PcsError> {
        let mut decoder = self
            .transformer
            .open_message_decoder(Arc::clone(&self.schema))?;
        for message in received {
            decoder.push(&message.payload).map_err(|e| {
                PcsError::generic(format!(
                    "{WHAT}: decode failed for {}: {e}",
                    self.origin_label(&message.origin)
                ))
            })?;
        }
        decoder
            .flush()?
            .ok_or_else(|| PcsError::generic(format!("{WHAT}: window decoded no rows")))
    }

    /// Where a message came from, in words. Built only on the error path, so a
    /// clean window allocates nothing for it.
    fn origin_label(&self, origin: &Origin) -> String {
        match (origin, &self.state) {
            (Origin::Subject(subject), _) => format!("subject '{subject}'"),
            (
                Origin::Sequence(sequence),
                Some(Started::Jetstream {
                    window:
                        PullWindow {
                            stream, consumer, ..
                        },
                    ..
                }),
            ) => format!("{stream}[{consumer}]@{sequence}"),
            (Origin::Sequence(sequence), _) => format!("stream sequence {sequence}"),
        }
    }
}

/// Collect a window off the merged core subscription.
///
/// A live source blocks on its first message with no deadline, then keeps taking
/// messages until `batch_size` or one `poll_timeout_ms` window. A `stop_at_end`
/// source bounds every receive by that window: core NATS has no end-of-stream
/// signal, so "the window elapsed with nothing on it" is the only EOF a
/// subscription can offer.
async fn collect_core(
    subscribers: &mut SelectAll<Subscriber>,
    cfg: &NatsSourceConfig,
) -> Result<Vec<Received>, PcsError> {
    let mut buffer = Vec::new();
    if !cfg.stop_at_end {
        match subscribers.next().await {
            Some(message) => buffer.push(received_core(message)),
            None => return Err(closed_error()),
        }
    }

    let deadline = Instant::now() + Duration::from_millis(cfg.poll_timeout_ms);
    while buffer.len() < cfg.batch_size {
        match timeout_at(deadline, subscribers.next()).await {
            Ok(Some(message)) => buffer.push(received_core(message)),
            // Every subscription ended, which the held client makes impossible
            // short of an explicit unsubscribe.
            Ok(None) if buffer.is_empty() => return Err(closed_error()),
            Ok(None) => break,
            Err(_elapsed) => break,
        }
    }
    Ok(buffer)
}

fn received_core(message: async_nats::Message) -> Received {
    Received {
        origin: Origin::Subject(message.subject),
        payload: message.payload,
    }
}

fn closed_error() -> PcsError {
    PcsError::generic(format!(
        "{WHAT}: every subscription closed while the connection was still held"
    ))
}

/// Collect a window off the pull consumer.
///
/// `stop_at_end` uses `fetch`, which sets `no_wait` and returns only what is
/// already there, so an empty result is EOF. A live source uses `batch`, which
/// waits up to `poll_timeout_ms` per request and loops until a window carries at
/// least one message.
async fn collect_jetstream(
    consumer: &PullConsumer,
    window: &PullWindow,
    cfg: &NatsSourceConfig,
    ack_mode: AckMode,
    pending_acks: &mut Vec<Acker>,
    last_pending: &mut Option<usize>,
) -> Result<Vec<Received>, PcsError> {
    let mut buffer = Vec::new();
    loop {
        let batch = if cfg.stop_at_end {
            let mut builder = consumer
                .fetch()
                .max_messages(cfg.batch_size)
                .expires(window.expires);
            if window.max_bytes > 0 {
                builder = builder.max_bytes(window.max_bytes);
            }
            if !window.heartbeat.is_zero() {
                builder = builder.heartbeat(window.heartbeat);
            }
            builder.messages().await
        } else {
            let mut builder = consumer
                .batch()
                .max_messages(cfg.batch_size)
                .expires(Duration::from_millis(cfg.poll_timeout_ms));
            if window.max_bytes > 0 {
                builder = builder.max_bytes(window.max_bytes);
            }
            if !window.heartbeat.is_zero() {
                builder = builder.heartbeat(window.heartbeat);
            }
            builder.messages().await
        }
        .map_err(|e| {
            PcsError::generic(format!(
                "{WHAT}: pull request on stream '{}' failed: {e}",
                window.stream
            ))
        })?;

        let mut batch = std::pin::pin!(batch);
        while let Some(item) = batch.next().await {
            let message = item.map_err(|e| {
                PcsError::generic(format!(
                    "{WHAT}: pull on stream '{}' failed: {e}",
                    window.stream
                ))
            })?;
            // `info` parses the reply subject, with no round-trip.
            let (sequence, pending) = {
                let info = message.info().map_err(|e| {
                    PcsError::generic(format!(
                        "{WHAT}: message on stream '{}' carries no JetStream metadata: {e}",
                        window.stream
                    ))
                })?;
                (info.stream_sequence, info.pending)
            };
            *last_pending = Some(usize::try_from(pending).unwrap_or(usize::MAX));

            let payload = if ack_mode == AckMode::Never {
                message.message.payload
            } else {
                let (message, acker) = message.split();
                pending_acks.push(acker);
                message.payload
            };
            buffer.push(Received {
                origin: Origin::Sequence(sequence),
                payload,
            });
        }

        // `fetch` completing empty is EOF; `batch` completing empty means the
        // window expired with nothing on the stream, so a live source asks
        // again.
        if cfg.stop_at_end || !buffer.is_empty() {
            return Ok(buffer);
        }
    }
}

#[async_trait]
impl Source for NatsSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        self.ensure_started().await?;
        self.flush_pending_acks().await?;

        let received = self.collect().await?;
        if received.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.decode_window(&received)?))
    }

    /// JetStream's own count of messages still waiting for this consumer, which
    /// is exactly the progress hint the trait asks for. Core NATS has no such
    /// number.
    fn estimated_rows(&self) -> Option<usize> {
        self.last_pending
    }
}
