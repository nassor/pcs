//! The two factory traits a connector crate implements.

use pcs_config::ConfigValue;
use pcs_core::error::PcsError;
use pcs_core::io::sink::Sink;
use pcs_core::io::source::Source;

use crate::context::ConnectorContext;

/// Factory for building a [`Source`] from config.
///
/// Implement this trait for each source type you want to expose to the
/// configuration file. The `type_name` must match the `type` property of a
/// `source` node.
pub trait SourceFactory: Send + Sync + 'static {
    /// The type name that appears in config as `type="<name>"`.
    fn type_name(&self) -> &'static str;

    /// Build a source instance from the user-supplied config value.
    ///
    /// `ctx` carries the transformer registry, so a connector that moves bytes
    /// resolves its byte format through
    /// [`ConnectorContext::transformer`] instead of owning a format list.
    ///
    /// # Errors
    ///
    /// Return [`PcsError::Configuration`] if required config fields are
    /// missing or have invalid values.
    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, PcsError>;
}

/// Factory for building a [`Sink`] from config.
///
/// Implement this trait for each sink type you want to expose to the
/// configuration file. The `type_name` must match the `type` property of a
/// `sink` node.
pub trait SinkFactory: Send + Sync + 'static {
    /// The type name that appears in config as `type="<name>"`.
    fn type_name(&self) -> &'static str;

    /// Build a sink instance from the user-supplied config value.
    ///
    /// `ctx` carries the transformer registry, so a connector that moves bytes
    /// resolves its byte format through
    /// [`ConnectorContext::transformer`] instead of owning a format list.
    ///
    /// # Errors
    ///
    /// Return [`PcsError::Configuration`] if required config fields are
    /// missing or have invalid values.
    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, PcsError>;
}
