//! [`ConnectorContext`]: what a factory can reach while it builds.
//!
//! The context carries the transformer, if any, the host bound to this
//! connector instance through a declared `transformer` id. Resolution against
//! the `TransformerRegistry` happens in the host, once per workflow build, so
//! every byte-carrying connector uses what it is handed instead of resolving a
//! `format` key itself.
//!
//! It also carries the optional [`ChannelBridge`], the shared registry a
//! `ChannelSource`/`ChannelSink` factory resolves its named half through.

use std::sync::Arc;

use pcs_core::error::PcsError;
use pcs_transformer::Transformer;

use crate::ChannelBridge;

/// What a factory can reach while it builds: the transformer the config
/// bound to this connector instance, if any, and the channel bridge, if one
/// was registered.
pub struct ConnectorContext {
    transformer: Option<Arc<dyn Transformer>>,
    channels: Option<Arc<dyn ChannelBridge>>,
}

impl ConnectorContext {
    /// Wrap the transformer the host resolved for this connector instance.
    ///
    /// `None` for a connector declared with no `transformer` key, which is
    /// valid for a connector that produces `RecordBatch`es directly (for
    /// example `PostgresSource`) and an error for one that moves bytes.
    pub fn new(transformer: Option<Arc<dyn Transformer>>) -> Self {
        Self {
            transformer,
            channels: None,
        }
    }

    /// Attach the shared channel bridge every `ChannelSource`/`ChannelSink`
    /// node resolves its named half through.
    pub fn with_channels(mut self, channels: Arc<dyn ChannelBridge>) -> Self {
        self.channels = Some(channels);
        self
    }

    /// The channel bridge, if one was registered; `None` otherwise.
    pub fn channel_bridge(&self) -> Option<&Arc<dyn ChannelBridge>> {
        self.channels.as_ref()
    }

    /// The bound transformer, or a configuration error naming `what`.
    ///
    /// `what` prefixes the error, so a bad key names the connector that
    /// rejected it.
    ///
    /// # Errors
    ///
    /// Returns [`PcsError::Configuration`] when the source or sink declared no
    /// `transformer` key.
    pub fn transformer(&self, what: &str) -> Result<Arc<dyn Transformer>, PcsError> {
        self.transformer.clone().ok_or_else(|| {
            PcsError::configuration(format!(
                "{what} moves bytes and needs a 'transformer' key naming a declared transformer"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoTransformer(&'static str);

    impl Transformer for EchoTransformer {
        fn format(&self) -> &'static str {
            self.0
        }
    }

    #[test]
    fn a_bound_transformer_resolves() {
        let ctx = ConnectorContext::new(Some(Arc::new(EchoTransformer("csv"))));
        assert_eq!(ctx.transformer("FileSource").unwrap().format(), "csv");
    }

    #[test]
    fn an_unbound_transformer_is_a_configuration_error_naming_the_connector() {
        let ctx = ConnectorContext::new(None);
        let err = match ctx.transformer("FileSource") {
            Ok(_) => panic!("no transformer was bound"),
            Err(e) => e,
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "FileSource moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }
}
