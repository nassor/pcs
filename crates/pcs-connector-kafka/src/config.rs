//! Serde-derived configuration for the Kafka source and sink.
//!
//! Named keys are values PCS itself interprets. Every librdkafka property goes
//! on the `properties` node and is passed through untouched. There is no
//! second way to set the same thing, and `properties` is applied last, so it
//! overrides any default this connector sets.
//!
//! Both top-level configs carry `#[serde(deny_unknown_fields)]`: a key the
//! connector cannot honour is a configuration error, not something to drop
//! silently. Each exposes `validate`, which the constructors in
//! [`crate::source`](crate) and [`crate::sink`](crate) call before they build
//! anything.

use std::collections::BTreeMap;

use rdkafka::ClientConfig;
use serde::Deserialize;

use pcs_connector::ConfigValue;
use pcs_core::error::PcsError;

/// How a topic is provisioned before the client uses it.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TopicProvision {
    /// Create the topic when it does not exist. On by default.
    #[serde(default = "default_true")]
    pub create: bool,
    /// Partition count for a topic this connector creates.
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    /// Replication factor for a topic this connector creates.
    #[serde(default = "default_replication")]
    pub replication_factor: i32,
    /// Broker-side topic config entries, e.g. `"retention.ms"="60000"`.
    #[serde(default)]
    pub config: BTreeMap<String, String>,
    /// Admin operation timeout.
    #[serde(default = "default_provision_timeout_ms")]
    pub timeout_ms: u64,
}

// `Default` must agree with the serde defaults above: omitting the whole
// `provision` node uses `Default`, and that is what makes "create by
// default" true when the user writes nothing.
impl Default for TopicProvision {
    fn default() -> Self {
        Self {
            create: true,
            partitions: default_partitions(),
            replication_factor: default_replication(),
            config: BTreeMap::new(),
            timeout_ms: default_provision_timeout_ms(),
        }
    }
}

/// Configuration for [`KafkaSource`](crate::KafkaSource).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceConfig {
    /// Comma-separated bootstrap servers.
    pub brokers: String,
    /// Topic to consume. Comma-separated for several.
    pub topic: String,
    /// Consumer group.
    #[serde(default = "default_group_id")]
    pub group_id: String,
    /// Maximum messages folded into one `RecordBatch`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// How long one `next_batch` keeps collecting after its first message.
    #[serde(default = "default_poll_timeout_ms")]
    pub poll_timeout_ms: u64,
    /// `auto.offset.reset` for a group with no committed offset.
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,
    /// Commit the previous batch's offsets at the start of the next poll.
    #[serde(default = "default_true")]
    pub commit_on_drain: bool,
    /// Report EOF once every assigned partition is drained, making the source
    /// usable from the batch run modes. Off by default: a Kafka consumer is a
    /// live source.
    #[serde(default)]
    pub stop_at_end: bool,
    /// Topic provisioning.
    #[serde(default)]
    pub provision: TopicProvision,
    /// librdkafka properties, applied after every default this crate sets.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    /// Declared Arrow schema. Parsed by `pcs_connector::parse_schema_fields`
    /// from the same table so the type vocabulary matches every other
    /// connector; declared here only so `deny_unknown_fields` accepts the key.
    #[serde(default, deserialize_with = "pcs_connector::one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

/// Configuration for [`KafkaSink`](crate::KafkaSink).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkConfig {
    /// Comma-separated bootstrap servers.
    pub brokers: String,
    /// Topic to produce to. One topic, not a list.
    pub topic: String,
    /// Column whose rendered value becomes the message key. Row-per-message
    /// formats only.
    #[serde(default)]
    pub key_field: Option<String>,
    /// How long `finish` waits for the producer queue to drain.
    #[serde(default = "default_flush_timeout_ms")]
    pub flush_timeout_ms: u64,
    /// Topic provisioning.
    #[serde(default)]
    pub provision: TopicProvision,
    /// librdkafka properties, applied after every default this crate sets.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    /// See [`KafkaSourceConfig::schema_fields`].
    #[serde(default, deserialize_with = "pcs_connector::one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

impl KafkaSourceConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), PcsError> {
        let what = "KafkaSource";
        validate_brokers(what, &self.brokers)?;
        validate_topic(what, &self.topic)?;

        if self.batch_size == 0 {
            return Err(PcsError::configuration(format!(
                "{what} config: 'batch_size' must be at least 1"
            )));
        }
        if !matches!(
            self.auto_offset_reset.as_str(),
            "earliest" | "latest" | "none"
        ) {
            return Err(PcsError::configuration(format!(
                "{what} config: 'auto_offset_reset' must be earliest, latest or none"
            )));
        }

        validate_provision(what, &self.provision)?;
        validate_properties(what, &self.properties)?;
        Ok(())
    }

    /// The subscribed topic names: `topic` split on commas and trimmed.
    pub(crate) fn topics(&self) -> Vec<String> {
        self.topic
            .split(',')
            .map(|t| t.trim().to_string())
            .collect()
    }
}

impl KafkaSinkConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), PcsError> {
        let what = "KafkaSink";
        validate_brokers(what, &self.brokers)?;
        validate_topic(what, &self.topic)?;

        // Whether `key_field` is honourable depends on the format's message
        // shape, which only the resolved transformer knows, so `KafkaSink::new`
        // checks it rather than this method.

        validate_provision(what, &self.provision)?;
        validate_properties(what, &self.properties)?;
        Ok(())
    }
}

fn validate_brokers(what: &str, brokers: &str) -> Result<(), PcsError> {
    if brokers.trim().is_empty() {
        return Err(PcsError::configuration(format!(
            "{what} config: 'brokers' must not be empty"
        )));
    }
    Ok(())
}

fn validate_topic(what: &str, topic: &str) -> Result<(), PcsError> {
    if topic.split(',').any(|t| t.trim().is_empty()) {
        return Err(PcsError::configuration(format!(
            "{what} config: 'topic' must name at least one non-empty topic"
        )));
    }
    Ok(())
}

fn validate_provision(what: &str, provision: &TopicProvision) -> Result<(), PcsError> {
    if provision.partitions < 1 {
        return Err(PcsError::configuration(format!(
            "{what} config: 'provision.partitions' must be at least 1"
        )));
    }
    if provision.replication_factor < 1 {
        return Err(PcsError::configuration(format!(
            "{what} config: 'provision.replication_factor' must be at least 1"
        )));
    }
    Ok(())
}

fn validate_properties(what: &str, properties: &BTreeMap<String, String>) -> Result<(), PcsError> {
    if properties.contains_key("bootstrap.servers") {
        return Err(PcsError::configuration(format!(
            "{what} config: set the brokers with 'brokers', not properties.bootstrap.servers"
        )));
    }
    Ok(())
}

/// Build a librdkafka client config: `bootstrap.servers`, then `defaults`,
/// then the user's `properties`, which therefore win.
pub(crate) fn client_config(
    brokers: &str,
    defaults: &[(&str, &str)],
    properties: &BTreeMap<String, String>,
) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers);
    for (key, value) in defaults {
        cfg.set(*key, *value);
    }
    for (key, value) in properties {
        cfg.set(key, value);
    }
    cfg
}

fn default_true() -> bool {
    true
}
fn default_partitions() -> i32 {
    1
}
fn default_replication() -> i32 {
    1
}
fn default_provision_timeout_ms() -> u64 {
    10_000
}
fn default_group_id() -> String {
    "pcs".to_string()
}
fn default_batch_size() -> usize {
    1000
}
fn default_poll_timeout_ms() -> u64 {
    1000
}
fn default_auto_offset_reset() -> String {
    "earliest".to_string()
}
fn default_flush_timeout_ms() -> u64 {
    30_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use pcs_connector::from_kdl_str;

    fn source(extra: &str) -> KafkaSourceConfig {
        let raw = format!("brokers \"localhost:9092\"\ntopic \"orders\"\n{extra}");
        KafkaSourceConfig::deserialize(from_kdl_str(&raw).expect("parse kdl")).expect("parse")
    }

    fn sink(extra: &str) -> KafkaSinkConfig {
        let raw = format!("brokers \"localhost:9092\"\ntopic \"orders\"\n{extra}");
        KafkaSinkConfig::deserialize(from_kdl_str(&raw).expect("parse kdl")).expect("parse")
    }

    #[test]
    fn source_defaults_are_populated_when_omitted() {
        let cfg = source("");
        assert_eq!(cfg.group_id, "pcs");
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.poll_timeout_ms, 1000);
        assert_eq!(cfg.auto_offset_reset, "earliest");
        assert!(cfg.commit_on_drain);
        assert!(!cfg.stop_at_end);
        assert!(cfg.provision.create);
        assert_eq!(cfg.provision.partitions, 1);
        assert_eq!(cfg.provision.replication_factor, 1);
        assert!(cfg.provision.config.is_empty());
        assert_eq!(cfg.provision.timeout_ms, 10_000);
        assert!(cfg.properties.is_empty());
        cfg.validate().expect("defaults are valid");
    }

    #[test]
    fn sink_defaults_are_populated_when_omitted() {
        let cfg = sink("");
        assert_eq!(cfg.key_field, None);
        assert_eq!(cfg.flush_timeout_ms, 30_000);
        assert!(cfg.provision.create);
        assert!(cfg.properties.is_empty());
        cfg.validate().expect("defaults are valid");
    }

    #[test]
    fn topic_provision_default_creates_by_default() {
        assert!(TopicProvision::default().create);
    }

    #[test]
    fn empty_brokers_is_a_configuration_error() {
        let raw = "brokers \"\"\ntopic \"orders\"\n";
        let cfg =
            KafkaSourceConfig::deserialize(from_kdl_str(raw).expect("parse kdl")).expect("parse");
        let err = cfg.validate().expect_err("empty brokers must fail");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'brokers'"));
    }

    #[test]
    fn empty_topic_is_a_configuration_error() {
        let raw = "brokers \"localhost:9092\"\ntopic \"\"\n";
        let cfg =
            KafkaSourceConfig::deserialize(from_kdl_str(raw).expect("parse kdl")).expect("parse");
        let err = cfg.validate().expect_err("empty topic must fail");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'topic'"));
    }

    #[test]
    fn a_blank_element_in_a_comma_separated_topic_list_is_a_configuration_error() {
        let raw = "brokers \"localhost:9092\"\ntopic \"a,,b\"\n";
        let cfg =
            KafkaSourceConfig::deserialize(from_kdl_str(raw).expect("parse kdl")).expect("parse");
        let err = cfg.validate().expect_err("blank element must fail");
        assert!(err.message().contains("'topic'"));
    }

    #[test]
    fn zero_batch_size_is_a_configuration_error() {
        let cfg = source("batch_size 0\n");
        let err = cfg.validate().expect_err("zero batch_size must fail");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'batch_size'"));
    }

    #[test]
    fn invalid_auto_offset_reset_is_a_configuration_error() {
        let cfg = source("auto_offset_reset \"oldest\"\n");
        let err = cfg
            .validate()
            .expect_err("invalid auto_offset_reset must fail");
        assert!(err.message().contains("'auto_offset_reset'"));
    }

    #[test]
    fn provision_partitions_below_one_is_a_configuration_error() {
        let cfg = source("provision partitions=0\n");
        let err = cfg.validate().expect_err("zero partitions must fail");
        assert!(err.message().contains("'provision.partitions'"));
    }

    #[test]
    fn provision_replication_factor_below_one_is_a_configuration_error() {
        let cfg = source("provision replication_factor=0\n");
        let err = cfg
            .validate()
            .expect_err("zero replication_factor must fail");
        assert!(err.message().contains("'provision.replication_factor'"));
    }

    #[test]
    fn properties_bootstrap_servers_is_a_configuration_error() {
        let cfg = source("properties \"bootstrap.servers\"=\"evil:9092\"\n");
        let err = cfg
            .validate()
            .expect_err("properties.bootstrap.servers must fail");
        assert!(err.message().contains("properties.bootstrap.servers"));
    }

    #[test]
    fn properties_override_a_default_in_client_config() {
        let mut properties = BTreeMap::new();
        properties.insert("auto.offset.reset".to_string(), "latest".to_string());
        let cfg = client_config(
            "localhost:9092",
            &[("auto.offset.reset", "earliest")],
            &properties,
        );
        assert_eq!(cfg.get("auto.offset.reset"), Some("latest"));
        assert_eq!(cfg.get("bootstrap.servers"), Some("localhost:9092"));
    }

    #[test]
    fn client_config_applies_defaults_when_not_overridden() {
        let cfg = client_config(
            "localhost:9092",
            &[("group.id", "pcs"), ("enable.auto.commit", "false")],
            &BTreeMap::new(),
        );
        assert_eq!(cfg.get("group.id"), Some("pcs"));
        assert_eq!(cfg.get("enable.auto.commit"), Some("false"));
    }
}
