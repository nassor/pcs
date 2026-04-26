//! Factory registry for the PCS service layer.
//!
//! The registry maps string type names (from TOML config) to factory objects
//! that can construct concrete [`Source`] and [`Sink`] instances.
//!
//! System and component factories were removed. The runtime is
//! now provided directly as a `Box<dyn PipelineRuntime>` (native) or loaded
//! from a WASM module via [`PipelineRuntimeLoader`](super::loader::PipelineRuntimeLoader).
//!
//! ## Usage
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use pcs_service::service::registry::Registry;
//!
//! let mut registry = Registry::new();
//! // register_source / register_sink go here
//! assert_eq!(registry.source_count(), 0);
//! # }
//! ```

use std::collections::HashMap;

use crate::error::PcsError;
use crate::io::sink::Sink;
use crate::io::source::Source;

// ---------------------------------------------------------------------------
// SourceFactory
// ---------------------------------------------------------------------------

/// Factory for building a [`Source`] from config.
///
/// Implement this trait for each source type you want to expose to the
/// TOML configuration. The `type_name` must match the `type` field in
/// [`SourceSpec`](crate::service::config::SourceSpec).
pub trait SourceFactory: Send + Sync + 'static {
    /// The type name that appears in TOML as `type = "<name>"`.
    fn type_name(&self) -> &'static str;

    /// Build a source instance from the user-supplied TOML config value.
    ///
    /// # Errors
    ///
    /// Return [`PcsError::Configuration`] if required config fields are
    /// missing or have invalid values.
    fn build(&self, config: &toml::Value) -> Result<Box<dyn Source>, PcsError>;
}

// ---------------------------------------------------------------------------
// SinkFactory
// ---------------------------------------------------------------------------

/// Factory for building a [`Sink`] from config.
///
/// Implement this trait for each sink type you want to expose to the
/// TOML configuration. The `type_name` must match the `type` field in
/// [`SinkSpec`](crate::service::config::SinkSpec).
pub trait SinkFactory: Send + Sync + 'static {
    /// The type name that appears in TOML as `type = "<name>"`.
    fn type_name(&self) -> &'static str;

    /// Build a sink instance from the user-supplied TOML config value.
    ///
    /// # Errors
    ///
    /// Return [`PcsError::Configuration`] if required config fields are
    /// missing or have invalid values.
    fn build(&self, config: &toml::Value) -> Result<Box<dyn Sink>, PcsError>;
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Central registry mapping type names to their IO factories.
///
/// Factories are registered at startup (before any config is loaded) and
/// looked up by the `type_name` string from the TOML config. System and
/// component factories were removed — the runtime is now
/// provided directly via [`ServiceBuilder::with_runtime`] or loaded from
/// a WASM module.
///
/// ## Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use pcs_service::service::registry::Registry;
///
/// let registry = Registry::new();
/// assert_eq!(registry.source_count(), 0);
/// assert_eq!(registry.sink_count(), 0);
/// # }
/// ```
#[derive(Default)]
pub struct Registry {
    sources: HashMap<String, Box<dyn SourceFactory>>,
    sinks: HashMap<String, Box<dyn SinkFactory>>,
}

impl Registry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a source factory.
    ///
    /// If a factory with the same `type_name` is already registered, it is
    /// silently replaced.
    pub fn register_source<F: SourceFactory>(&mut self, factory: F) -> &mut Self {
        self.sources
            .insert(factory.type_name().to_string(), Box::new(factory));
        self
    }

    /// Register a sink factory.
    ///
    /// If a factory with the same `type_name` is already registered, it is
    /// silently replaced.
    pub fn register_sink<F: SinkFactory>(&mut self, factory: F) -> &mut Self {
        self.sinks
            .insert(factory.type_name().to_string(), Box::new(factory));
        self
    }

    /// Look up a source factory by type name.
    pub fn source(&self, type_name: &str) -> Option<&dyn SourceFactory> {
        self.sources.get(type_name).map(|f| f.as_ref())
    }

    /// Look up a sink factory by type name.
    pub fn sink(&self, type_name: &str) -> Option<&dyn SinkFactory> {
        self.sinks.get(type_name).map(|f| f.as_ref())
    }

    /// Returns the number of registered source factories.
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Returns the number of registered sink factories.
    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    struct TestSourceFactory;
    impl SourceFactory for TestSourceFactory {
        fn type_name(&self) -> &'static str {
            "TestSource"
        }
        fn build(&self, _config: &toml::Value) -> Result<Box<dyn Source>, PcsError> {
            use crate::io::channel_source::ChannelSource;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (_tx, src) = ChannelSource::new(schema, 1);
            Ok(Box::new(src))
        }
    }

    struct TestSinkFactory;
    impl SinkFactory for TestSinkFactory {
        fn type_name(&self) -> &'static str {
            "TestSink"
        }
        fn build(&self, _config: &toml::Value) -> Result<Box<dyn Sink>, PcsError> {
            use crate::io::channel_sink::ChannelSink;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (sink, _rx) = ChannelSink::new(schema, 1);
            Ok(Box::new(sink))
        }
    }

    #[test]
    fn test_register_and_lookup_source_factory() {
        let mut reg = Registry::new();
        reg.register_source(TestSourceFactory);
        assert!(reg.source("TestSource").is_some());
        assert!(reg.source("Missing").is_none());
    }

    #[test]
    fn test_register_and_lookup_sink_factory() {
        let mut reg = Registry::new();
        reg.register_sink(TestSinkFactory);
        assert!(reg.sink("TestSink").is_some());
        assert!(reg.sink("Missing").is_none());
    }

    #[test]
    fn test_registry_counts() {
        let mut reg = Registry::new();
        assert_eq!(reg.source_count(), 0);
        assert_eq!(reg.sink_count(), 0);
        reg.register_source(TestSourceFactory);
        assert_eq!(reg.source_count(), 1);
        reg.register_sink(TestSinkFactory);
        assert_eq!(reg.sink_count(), 1);
    }

    #[test]
    fn test_duplicate_registration_replaces() {
        let mut reg = Registry::new();
        reg.register_source(TestSourceFactory);
        reg.register_source(TestSourceFactory);
        assert_eq!(reg.source_count(), 1);
    }

    #[test]
    fn test_source_factory_builds_source() {
        let factory = TestSourceFactory;
        let src = factory
            .build(&toml::Value::Table(toml::Table::new()))
            .unwrap();
        assert_eq!(src.schema().fields().len(), 1);
    }

    #[test]
    fn test_sink_factory_builds_sink() {
        let factory = TestSinkFactory;
        let sink = factory
            .build(&toml::Value::Table(toml::Table::new()))
            .unwrap();
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn test_default_registry_is_empty() {
        let reg = Registry::default();
        assert_eq!(reg.source_count(), 0);
        assert_eq!(reg.sink_count(), 0);
    }
}
