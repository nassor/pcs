use std::time::Duration;

use wasmtime::Engine;

/// Epoch tick interval for guest deadline enforcement.
const EPOCH_TICK: Duration = Duration::from_millis(100);

/// Host-side wasmtime [`Engine`] with epoch interruption enabled.
///
/// Cheap to clone: wasmtime wraps `Arc<Engine>` internally. A background tokio
/// task increments the epoch every 100 ms so guests past their deadline are
/// interrupted cleanly. Create one with [`WasmEngine::new`] at service startup
/// and share it.
#[derive(Clone)]
pub struct WasmEngine {
    pub(crate) engine: Engine,
}

impl WasmEngine {
    /// Create the engine and spawn the epoch ticker task.
    pub fn new() -> wasmtime::Result<Self> {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        config.epoch_interruption(true);

        let engine = Engine::new(&config)?;
        Self::spawn_ticker(engine.clone());
        Ok(Self { engine })
    }

    fn spawn_ticker(engine: Engine) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(EPOCH_TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                engine.increment_epoch();
            }
        });
    }
}

impl Default for WasmEngine {
    fn default() -> Self {
        Self::new().expect("wasmtime Engine creation failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn engine_creates_successfully() {
        let engine = WasmEngine::new().unwrap();
        let _store: wasmtime::Store<()> = wasmtime::Store::new(&engine.engine, ());
    }
}
