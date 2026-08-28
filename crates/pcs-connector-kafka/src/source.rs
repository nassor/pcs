//! [`KafkaSource`]: a Kafka topic consumer [`Source`].
//!
//! Live by default: with `stop_at_end = false`, [`next_batch`](Source::next_batch)
//! blocks on the first message and never returns `Ok(None)`, exactly like a
//! live TCP ingestion source idling for a connection. Setting
//! `stop_at_end = true` makes it usable from the batch run modes: once every
//! assigned partition has reported end-of-partition, it reports EOF.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::{ClientConfig, Message};
use tokio::time::{Instant, timeout_at};

use pcs_core::error::PcsError;
use pcs_core::io::source::Source;
use pcs_transformer::Transformer;

use crate::admin::ensure_topics;
use crate::config::{KafkaSourceConfig, client_config};

/// One message pulled off the consumer, with its payload copied out of the
/// borrowed librdkafka buffer so it can outlive the `recv()` call.
struct ReceivedMessage {
    topic: String,
    partition: i32,
    offset: i64,
    /// `None` for a tombstone (null payload), which is skipped rather than
    /// decoded.
    payload: Option<Vec<u8>>,
}

/// Live Kafka [`Source`]: one `RecordBatch` per collected window of messages.
///
/// The listener/consumer connects lazily: [`new`](Self::new) only builds and
/// validates the `StreamConsumer`, so `pcs-service validate` stays
/// broker-free. The first [`next_batch`](Source::next_batch) call provisions
/// the topic (unless opted out) and subscribes.
pub struct KafkaSource {
    consumer: StreamConsumer,
    /// Kept so the admin client used for topic provisioning shares the same
    /// bootstrap servers and properties as the consumer.
    client_config: ClientConfig,
    topics: Vec<String>,
    schema: Arc<Schema>,
    /// The payload codec. A decoder is opened per window rather than held for
    /// the source's life: `Source` is `Send + Sync`, and a decoder need only be
    /// `Send`, which `arrow_json`'s is and no more. Opening one costs an
    /// allocation per schema, not per row.
    transformer: Arc<dyn Transformer>,
    cfg: KafkaSourceConfig,
    /// Set once the topic has been provisioned and `subscribe` has been
    /// called, so both happen exactly once.
    started: bool,
    /// Set once a batch has been handed to the caller and cleared once its
    /// offsets are committed at the start of the next call. This is what
    /// makes delivery at-least-once: a crash between the two replays the
    /// batch.
    pending_commit: bool,
    /// Partition indices observed as end-of-partition when `stop_at_end` is
    /// set. `enable.partition.eof` events from librdkafka carry only the
    /// partition index, not the topic name, so this cannot disambiguate two
    /// simultaneously subscribed topics that reuse the same partition index.
    /// That is a fast-path optimisation only: [`next_batch`](Source::next_batch)
    /// still falls back to its poll-window deadline to decide EOF correctly,
    /// so an index collision costs a slower drain, never a wrong result.
    eof: HashSet<i32>,
}

impl KafkaSource {
    /// Validate the config and create the consumer. Opens no connection:
    /// librdkafka connects lazily in the background on first use.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when `cfg` fails validation,
    /// `transformer` has no message codec, or librdkafka rejects a property in
    /// `cfg.properties`.
    pub fn new(
        cfg: KafkaSourceConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, PcsError> {
        cfg.validate()?;
        if transformer.message_shape().is_none() {
            return Err(PcsError::configuration(format!(
                "KafkaSource: format '{}' has no message codec",
                transformer.format()
            )));
        }
        let topics = cfg.topics();

        let eof_default = if cfg.stop_at_end { "true" } else { "false" };
        let defaults: [(&str, &str); 4] = [
            ("group.id", cfg.group_id.as_str()),
            ("auto.offset.reset", cfg.auto_offset_reset.as_str()),
            ("enable.auto.commit", "false"),
            ("enable.partition.eof", eof_default),
        ];
        let client_config = client_config(&cfg.brokers, &defaults, &cfg.properties);

        let consumer: StreamConsumer = client_config.create().map_err(|e| {
            PcsError::configuration(format!("KafkaSource: cannot create consumer: {e}"))
        })?;

        Ok(Self {
            consumer,
            client_config,
            topics,
            schema,
            transformer,
            cfg,
            started: false,
            pending_commit: false,
            eof: HashSet::new(),
        })
    }

    async fn ensure_started(&mut self) -> Result<(), PcsError> {
        if self.started {
            return Ok(());
        }
        // Provision before subscribe: librdkafka's default
        // `topic.metadata.refresh.interval.ms` is 300000, so a consumer that
        // subscribes to a not-yet-existing topic can sit blind for 5 minutes.
        ensure_topics(&self.client_config, &self.topics, &self.cfg.provision).await?;
        if self.cfg.provision.create {
            // `ensure_topics` just created these through a separate
            // `AdminClient` connection; this consumer's own metadata can lag
            // behind that by a moment. `subscribe` resolves partitions once
            // and does not retry a topic it did not find, so it must not run
            // until this consumer's own metadata view has caught up.
            self.await_topic_metadata().await?;
        }
        let refs: Vec<&str> = self.topics.iter().map(String::as_str).collect();
        self.consumer
            .subscribe(&refs)
            .map_err(|e| PcsError::generic(format!("KafkaSource: subscribe failed: {e}")))?;
        self.started = true;
        Ok(())
    }

    /// Poll metadata until every topic in `self.topics` is visible to this
    /// consumer's own connection, or `poll_timeout_ms` elapses.
    async fn await_topic_metadata(&self) -> Result<(), PcsError> {
        let deadline = Instant::now() + Duration::from_millis(self.cfg.poll_timeout_ms);
        loop {
            let all_visible = self.topics.iter().all(|topic| {
                self.consumer
                    .fetch_metadata(Some(topic), Duration::from_secs(5))
                    .is_ok_and(|metadata| {
                        metadata.topics().iter().any(|t| {
                            t.name() == topic && t.error().is_none() && !t.partitions().is_empty()
                        })
                    })
            });
            if all_visible {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(self.unknown_topic_error());
            }
            tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
        }
    }

    /// Commit the previous batch's offsets, if one is pending. Committing
    /// here (at the start of the *next* poll) rather than when the batch was
    /// produced is what makes delivery at-least-once.
    fn commit_pending(&mut self) -> Result<(), PcsError> {
        if self.pending_commit && self.cfg.commit_on_drain {
            self.consumer
                .commit_consumer_state(CommitMode::Async)
                .map_err(|e| PcsError::generic(format!("KafkaSource: commit failed: {e}")))?;
        }
        self.pending_commit = false;
        Ok(())
    }

    /// `true` once every currently assigned partition index has reported EOF.
    /// `false` while the assignment has not settled yet (right after
    /// `subscribe`, before the group rebalance completes).
    fn fully_drained(&self) -> Result<bool, PcsError> {
        let assignment = self
            .consumer
            .assignment()
            .map_err(|e| PcsError::generic(format!("KafkaSource: cannot read assignment: {e}")))?;
        if assignment.count() == 0 {
            return Ok(false);
        }
        Ok(assignment
            .elements()
            .iter()
            .all(|elem| self.eof.contains(&elem.partition())))
    }

    /// Copy a borrowed message's fields out so it can be buffered across
    /// `recv()` calls.
    fn take_message(message: &rdkafka::message::BorrowedMessage<'_>) -> ReceivedMessage {
        ReceivedMessage {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
            payload: message.payload().map(<[u8]>::to_vec),
        }
    }

    /// Collect messages for a live (non-`stop_at_end`) source: the first
    /// message blocks indefinitely, exactly like `TcpIngestSource` idling for
    /// a connection; the rest are bounded by one `poll_timeout_ms` window.
    async fn collect_live(&mut self) -> Result<Vec<ReceivedMessage>, PcsError> {
        let mut buffer = Vec::new();
        let unknown_topic_deadline =
            Instant::now() + Duration::from_millis(self.cfg.poll_timeout_ms);
        loop {
            match self.consumer.recv().await {
                Ok(message) => {
                    buffer.push(Self::take_message(&message));
                    break;
                }
                // Meaningless for a live source: more data may still arrive.
                Err(KafkaError::PartitionEOF(_)) => continue,
                Err(e) if is_unknown_topic(&e) => {
                    if Instant::now() >= unknown_topic_deadline {
                        return Err(self.unknown_topic_error());
                    }
                    tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
                }
                Err(e) => return Err(PcsError::generic(format!("KafkaSource: poll failed: {e}"))),
            }
        }

        let deadline = Instant::now() + Duration::from_millis(self.cfg.poll_timeout_ms);
        while buffer.len() < self.cfg.batch_size {
            match timeout_at(deadline, self.consumer.recv()).await {
                Ok(Ok(message)) => buffer.push(Self::take_message(&message)),
                Ok(Err(KafkaError::PartitionEOF(_))) => {}
                Ok(Err(e)) if is_unknown_topic(&e) => {
                    tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
                }
                Ok(Err(e)) => {
                    return Err(PcsError::generic(format!("KafkaSource: poll failed: {e}")));
                }
                Err(_elapsed) => break,
            }
        }
        Ok(buffer)
    }

    /// Collect messages for a `stop_at_end` source: every receive is bounded
    /// by one `poll_timeout_ms` window, and `PartitionEOF` is bookkeeping,
    /// not an error. Returns an empty buffer when the topic is fully
    /// drained; returns an error, not a silent empty buffer, when the window
    /// elapses while the topic's metadata never resolved (e.g.
    /// `provision.create = false` against a topic that does not exist).
    async fn collect_bounded(&mut self) -> Result<Vec<ReceivedMessage>, PcsError> {
        let mut buffer = Vec::new();
        let mut saw_unknown_topic = false;
        let deadline = Instant::now() + Duration::from_millis(self.cfg.poll_timeout_ms);
        while buffer.len() < self.cfg.batch_size {
            match timeout_at(deadline, self.consumer.recv()).await {
                Ok(Ok(message)) => buffer.push(Self::take_message(&message)),
                Ok(Err(KafkaError::PartitionEOF(partition))) => {
                    self.eof.insert(partition);
                    if buffer.is_empty() && self.fully_drained()? {
                        break;
                    }
                }
                Ok(Err(e)) if is_unknown_topic(&e) => {
                    saw_unknown_topic = true;
                    tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
                }
                Ok(Err(e)) => {
                    return Err(PcsError::generic(format!("KafkaSource: poll failed: {e}")));
                }
                Err(_elapsed) => break,
            }
        }
        if buffer.is_empty() && saw_unknown_topic {
            return Err(self.unknown_topic_error());
        }
        Ok(buffer)
    }

    /// Feed a whole window through one decoder and take the batch out.
    ///
    /// Each push names the message it failed on, keeping per-message
    /// attribution; the transformer names the format that rejected it.
    fn decode_window(&self, messages: &[ReceivedMessage]) -> Result<RecordBatch, PcsError> {
        let mut decoder = self
            .transformer
            .open_message_decoder(Arc::clone(&self.schema))?;
        for message in messages {
            let Some(payload) = &message.payload else {
                continue; // tombstone, already excluded from the buffer
            };
            decoder.push(payload).map_err(|e| {
                PcsError::generic(format!(
                    "KafkaSource: decode failed for {}[{}]@{}: {e}",
                    message.topic, message.partition, message.offset
                ))
            })?;
        }
        decoder
            .flush()?
            .ok_or_else(|| PcsError::generic("KafkaSource: window decoded no rows"))
    }

    /// A freshly created topic's metadata can take a moment to reach this
    /// consumer (`ensure_topics` created it moments ago through the admin
    /// client, a separate connection), and a topic that will never exist
    /// under `provision.create = false` looks identical until this window
    /// elapses. Named so the two cases are distinguishable in logs, not so
    /// PCS itself distinguishes them.
    fn unknown_topic_error(&self) -> PcsError {
        PcsError::generic(format!(
            "KafkaSource: poll failed: topic(s) {:?} were never visible to the consumer; if \
             provision.create = false, they may not exist",
            self.topics
        ))
    }
}

/// A freshly created topic briefly returns `UnknownTopicOrPartition` to a
/// consumer whose own metadata has not caught up yet, even though the admin
/// client that created it already got a success response.
const UNKNOWN_TOPIC_RETRY_BACKOFF: Duration = Duration::from_millis(100);

fn is_unknown_topic(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition)
    )
}

#[async_trait]
impl Source for KafkaSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, PcsError> {
        self.ensure_started().await?;
        self.commit_pending()?;

        let buffer = if self.cfg.stop_at_end {
            self.collect_bounded().await?
        } else {
            self.collect_live().await?
        };

        if buffer.is_empty() {
            return Ok(None);
        }

        let batch = self.decode_window(&buffer)?;
        self.pending_commit = true;
        Ok(Some(batch))
    }
}
