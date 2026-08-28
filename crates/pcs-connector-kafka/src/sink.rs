//! [`KafkaSink`]: a Kafka topic producer [`Sink`].

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, RecordBatch};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::Schema;
use async_trait::async_trait;
use futures_util::future::join_all;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use pcs_core::error::PcsError;
use pcs_core::io::sink::Sink;
use pcs_transformer::{MessageShape, Transformer};

use crate::admin::ensure_topics;
use crate::config::{KafkaSinkConfig, client_config};

/// Kafka [`Sink`]: the resolved format's [`MessageShape`] decides whether a
/// batch becomes one message per row or one message in total.
///
/// The producer connects lazily: [`new`](Self::new) only builds and validates
/// the `FutureProducer`, so `pcs-service validate` stays broker-free. The
/// first [`write_batch`](Sink::write_batch) call provisions the topic (unless
/// opted out).
pub struct KafkaSink {
    producer: FutureProducer,
    /// Kept so the admin client used for topic provisioning shares the same
    /// bootstrap servers and properties as the producer.
    client_config: ClientConfig,
    topic: String,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
    /// Cached from the transformer at construction, where the capability check
    /// already proved it is `Some`.
    shape: MessageShape,
    cfg: KafkaSinkConfig,
    /// Set once the topic has been provisioned, so it happens exactly once.
    provisioned: bool,
}

impl KafkaSink {
    /// Validate the config and create the producer. Opens no connection:
    /// librdkafka connects lazily in the background on first use.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when `cfg` fails validation, the
    /// format has no message codec, `key_field` is set against a format that
    /// emits one message per batch, or librdkafka rejects a property in
    /// `cfg.properties`.
    pub fn new(
        cfg: KafkaSinkConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, PcsError> {
        cfg.validate()?;

        // Both capability checks happen here rather than in `validate`: only
        // the resolved transformer knows its message shape.
        let format = transformer.format();
        let Some(shape) = transformer.message_shape() else {
            return Err(PcsError::configuration(format!(
                "KafkaSink: format '{format}' has no message codec"
            )));
        };
        if shape == MessageShape::PerBatch && cfg.key_field.is_some() {
            return Err(PcsError::configuration(format!(
                "KafkaSink config: 'key_field' needs a row-per-message format; '{format}' emits \
                 one message per batch"
            )));
        }

        let client_config = client_config(&cfg.brokers, &[], &cfg.properties);
        let producer: FutureProducer = client_config.create().map_err(|e| {
            PcsError::configuration(format!("KafkaSink: cannot create producer: {e}"))
        })?;
        let topic = cfg.topic.clone();
        Ok(Self {
            producer,
            client_config,
            topic,
            schema,
            transformer,
            shape,
            cfg,
            provisioned: false,
        })
    }

    async fn ensure_provisioned(&mut self) -> Result<(), PcsError> {
        if self.provisioned {
            return Ok(());
        }
        let topics = std::slice::from_ref(&self.topic);
        ensure_topics(&self.client_config, topics, &self.cfg.provision).await?;
        self.provisioned = true;
        Ok(())
    }

    /// Render `field` for every row as a message key, `None` for a null cell.
    fn render_keys(batch: &RecordBatch, field: &str) -> Result<Vec<Option<String>>, PcsError> {
        let column = batch.column_by_name(field).ok_or_else(|| {
            PcsError::generic(format!(
                "KafkaSink: key_field '{field}' is not a column in the batch"
            ))
        })?;
        let formatter = ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default())
            .map_err(|e| {
                PcsError::generic(format!("KafkaSink: formatting key_field '{field}': {e}"))
            })?;
        Ok((0..column.len())
            .map(|i| {
                if column.is_null(i) {
                    None
                } else {
                    Some(formatter.value(i).to_string())
                }
            })
            .collect())
    }

    /// Encode the batch and publish every payload. Every message is sent, then
    /// every delivery is awaited concurrently: that is the durability boundary,
    /// and it also bounds the producer queue without extra config.
    async fn publish(&self, batch: &RecordBatch) -> Result<(), PcsError> {
        let payloads = self.transformer.encode_messages(batch)?;
        if self.shape == MessageShape::PerRow && payloads.len() != batch.num_rows() {
            return Err(PcsError::generic(format!(
                "KafkaSink: format '{}' produced {} messages for {} rows",
                self.transformer.format(),
                payloads.len(),
                batch.num_rows()
            )));
        }

        let keys = match &self.cfg.key_field {
            None => None,
            Some(field) => Some(Self::render_keys(batch, field)?),
        };

        let flush_timeout = Duration::from_millis(self.cfg.flush_timeout_ms);
        let mut sends = Vec::with_capacity(payloads.len());
        for (i, payload) in payloads.iter().enumerate() {
            let mut record: FutureRecord<'_, str, [u8]> =
                FutureRecord::to(&self.topic).payload(payload.as_slice());
            if let Some(key) = keys.as_ref().and_then(|k| k[i].as_deref()) {
                record = record.key(key);
            }
            sends.push(self.producer.send(record, Timeout::After(flush_timeout)));
        }

        for result in join_all(sends).await {
            result.map_err(|(e, _)| {
                PcsError::generic(format!(
                    "KafkaSink: delivery to '{}' failed: {e}",
                    self.topic
                ))
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PcsError> {
        self.ensure_provisioned().await?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.publish(batch).await
    }

    async fn finish(&mut self) -> Result<(), PcsError> {
        let producer = self.producer.clone();
        let timeout = Duration::from_millis(self.cfg.flush_timeout_ms);
        // `Producer::flush` is a blocking call (it polls librdkafka's queue in
        // a loop up to `timeout`), so it must not run on the async executor.
        tokio::task::spawn_blocking(move || producer.flush(Timeout::After(timeout)))
            .await
            .map_err(|e| PcsError::generic(format!("KafkaSink: flush task panicked: {e}")))?
            .map_err(|e| PcsError::generic(format!("KafkaSink: flush failed: {e}")))
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}
