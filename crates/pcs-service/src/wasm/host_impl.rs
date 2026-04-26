use std::collections::HashMap;

use super::bindings::{HostIo, LogLevel, TypesHost};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

/// Per-store guest state passed as the wasmtime `Store<T>` data.
///
/// Holds the static config map injected at load time so the guest can call
/// `get-config` during `init` and `run-batch`. Config values are strings;
/// guests parse numerics themselves.
///
/// WASI imports are required by transitive deps (arrow-ipc, serde_arrow, std).
pub struct HostState {
    /// Pipeline name, used to prefix log output.
    pub name: String,
    /// Key/value config extracted from the TOML `[pipeline.wasm.config]` table.
    pub config: HashMap<String, String>,
    pub wasi_ctx: WasiCtx,
    pub resource_table: ResourceTable,
}

impl HostState {
    pub fn new(name: impl Into<String>, config: HashMap<String, String>) -> Self {
        Self {
            name: name.into(),
            config,
            wasi_ctx: WasiCtxBuilder::new().build(),
            resource_table: ResourceTable::new(),
        }
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

// The `types` WIT interface generates an empty Host marker trait.
impl TypesHost for HostState {}

impl HostIo for HostState {
    fn log(&mut self, level: LogLevel, target: String, message: String) {
        // Bridge to the host tracing subscriber when available; plain eprintln
        // otherwise. The feature gate avoids the tracing dep in plain `wasm`
        // builds that don't also enable `tracing`.
        #[cfg(feature = "tracing")]
        {
            use tracing::{debug, error, info, trace, warn};
            match level {
                LogLevel::Trace => trace!(pipeline = %self.name, target = %target, "{}", message),
                LogLevel::Debug => debug!(pipeline = %self.name, target = %target, "{}", message),
                LogLevel::Info => info!(pipeline = %self.name, target = %target, "{}", message),
                LogLevel::Warn => warn!(pipeline = %self.name, target = %target, "{}", message),
                LogLevel::Error => error!(pipeline = %self.name, target = %target, "{}", message),
            }
        }
        #[cfg(not(feature = "tracing"))]
        {
            let level_str = match level {
                LogLevel::Trace => "TRACE",
                LogLevel::Debug => "DEBUG",
                LogLevel::Info => "INFO",
                LogLevel::Warn => "WARN",
                LogLevel::Error => "ERROR",
            };
            eprintln!("[{}] [{}] {}: {}", level_str, self.name, target, message);
        }
    }

    fn metric(&mut self, name: String, value: f64) {
        // Metrics are surfaced to prometheus by the host service layer, not
        // directly here. In a plain `wasm` build without the service layer we
        // log them at trace level so they're not silently dropped.
        #[cfg(feature = "tracing")]
        {
            tracing::trace!(pipeline = %self.name, metric = %name, value = value, "guest metric");
        }
        #[cfg(not(feature = "tracing"))]
        {
            let _ = (name, value);
        }
    }

    fn get_config(&mut self, key: String) -> Option<String> {
        self.config.get(&key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_host(config: &[(&str, &str)]) -> HostState {
        HostState::new(
            "test-pipeline",
            config
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn get_config_returns_value() {
        let mut host = make_host(&[("batch_size", "512")]);
        assert_eq!(host.get_config("batch_size".into()), Some("512".into()));
    }

    #[test]
    fn get_config_returns_none_for_missing() {
        let mut host = make_host(&[]);
        assert_eq!(host.get_config("missing".into()), None);
    }

    #[test]
    fn log_does_not_panic() {
        let mut host = make_host(&[]);
        host.log(LogLevel::Info, "test".into(), "hello".into());
        host.log(LogLevel::Error, "test".into(), "boom".into());
    }

    #[test]
    fn metric_does_not_panic() {
        let mut host = make_host(&[]);
        host.metric("rows_processed".into(), 1024.0);
    }
}
