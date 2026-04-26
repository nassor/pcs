use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use async_trait::async_trait;
use pcs_core::{Dataset, PcsError, PcsResult};
use wasmtime::Store;
use wasmtime::component::{Component, HasSelf, Linker};

use super::bindings::{PcsPipeline, PipelineDescriptor, RunError};
use super::engine::WasmEngine;
use super::host_impl::HostState;

/// Host-side WASM pipeline runtime implementing [`pcs_core::runtime::PipelineRuntime`].
///
/// Each `run_on` call:
/// 1. Serialises the dataset to Arrow IPC bytes.
/// 2. Calls the guest's `run-batch` export via wasmtime on a fresh `Store`.
/// 3. Deserialises the output IPC bytes back, replacing the dataset contents.
///
/// Guest traps are mapped to `PcsError::SystemExecution`. The epoch deadline
/// (100 ms ticks) limits runaway guest execution.
pub struct WasmPipelineRuntime {
    name: String,
    engine: WasmEngine,
    /// Compiled component — reused across all calls (compilation is expensive).
    component: Component,
    config: HashMap<String, String>,
    /// Per-call epoch deadline in ticks (100 ms / tick → `ticks * 100 ms`).
    epoch_deadline: u64,
    /// Cached descriptor, populated on first `describe()` call.
    descriptor: Mutex<Option<PipelineDescriptor>>,
    /// Component names extracted from the descriptor, for `declared_components()`.
    component_names: OnceLock<Vec<String>>,
}

impl WasmPipelineRuntime {
    /// Compile a WASM component from raw bytes.
    ///
    /// Compilation is synchronous and expensive; do this once at load time.
    /// The resulting runtime is `Send` and can be wrapped in `Arc` for sharing.
    pub fn from_bytes(
        engine: WasmEngine,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        config: HashMap<String, String>,
        epoch_deadline_ticks: u64,
    ) -> PcsResult<Self> {
        let component = Component::from_binary(&engine.engine, wasm_bytes)
            .map_err(|e| PcsError::Configuration(format!("wasm compile error: {e}")))?;
        Ok(Self {
            name: name.into(),
            engine,
            component,
            config,
            epoch_deadline: epoch_deadline_ticks,
            descriptor: Mutex::new(None),
            component_names: OnceLock::new(),
        })
    }

    fn make_store_and_instance(&self) -> PcsResult<(Store<HostState>, PcsPipeline)> {
        let host = HostState::new(self.name.clone(), self.config.clone());
        let mut store = Store::new(&self.engine.engine, host);
        store.set_epoch_deadline(self.epoch_deadline);

        let mut linker: Linker<HostState> = Linker::new(&self.engine.engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .map_err(|e| PcsError::Configuration(format!("wasi linker error: {e}")))?;
        PcsPipeline::add_to_linker::<_, HasSelf<_>>(&mut linker, |s| s)
            .map_err(|e| PcsError::Configuration(format!("wasm linker error: {e}")))?;

        let instance = PcsPipeline::instantiate(&mut store, &self.component, &linker)
            .map_err(|e| PcsError::SystemExecution(format!("guest trap (instantiate): {e}")))?;

        Ok((store, instance))
    }

    /// Call `describe()` and cache the result.
    ///
    /// The first call instantiates a fresh store; subsequent calls return the
    /// cached descriptor without any guest round-trip.
    pub fn describe(&self) -> PcsResult<PipelineDescriptor> {
        {
            let guard = self.descriptor.lock().unwrap();
            if let Some(d) = guard.as_ref() {
                return Ok(d.clone());
            }
        }

        let (mut store, instance) = self.make_store_and_instance()?;
        let iface = instance.pcs_pipeline_pipeline();
        let desc = iface
            .call_describe(&mut store)
            .map_err(|e| PcsError::SystemExecution(format!("guest trap (describe): {e}")))?;

        let names: Vec<String> = desc.components.iter().map(|c| c.name.clone()).collect();
        self.component_names.get_or_init(|| names);

        let mut guard = self.descriptor.lock().unwrap();
        *guard = Some(desc.clone());
        Ok(desc)
    }

    /// Call `init(config_json)` on a fresh store. Call after `describe()`.
    pub fn init(&self, config_json: &str) -> PcsResult<()> {
        let (mut store, instance) = self.make_store_and_instance()?;
        let iface = instance.pcs_pipeline_pipeline();
        iface
            .call_init(&mut store, config_json)
            .map_err(|e| PcsError::SystemExecution(format!("guest trap (init): {e}")))?
            .map_err(|msg| PcsError::Configuration(format!("guest init error: {msg}")))
    }
}

/// Decode an Arrow IPC schema-message (produced by the guest's `schema_to_ipc_bytes`)
/// back into an Arrow [`Schema`].
///
/// The guest writes a `StreamWriter` with no batches (schema-only stream). On the
/// host side `StreamReader` can read the schema from the stream header.
fn parse_ipc_schema(ipc_bytes: &[u8]) -> PcsResult<Arc<Schema>> {
    let reader = StreamReader::try_new(ipc_bytes, None)
        .map_err(|e| PcsError::configuration(format!("wasm component schema parse error: {e}")))?;
    Ok(reader.schema())
}

#[async_trait(?Send)]
impl pcs_core::runtime::PipelineRuntime for WasmPipelineRuntime {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
        // Serialize the current dataset state → Arrow IPC bytes.
        let mut ipc_bytes: Vec<u8> = Vec::new();
        data.write_ipc(&mut ipc_bytes)?;

        let (mut store, instance) = self.make_store_and_instance()?;
        let iface = instance.pcs_pipeline_pipeline();

        let run_result = iface
            .call_run_batch(&mut store, &ipc_bytes, None)
            .map_err(|e| PcsError::SystemExecution(format!("guest trap (run-batch): {e}")))?;

        match run_result {
            Ok(result) => {
                let mut out_slice: &[u8] = &result.output;
                *data = Dataset::read_ipc(&mut out_slice)?;
                Ok(())
            }
            Err(RunError::Retryable(msg)) => {
                Err(PcsError::SystemExecution(format!("guest retryable: {msg}")))
            }
            Err(RunError::Permanent(msg)) => {
                Err(PcsError::SystemExecution(format!("guest permanent: {msg}")))
            }
            // run-batch MUST NOT emit schema-mismatch; treat as permanent bug.
            Err(RunError::SchemaMismatch(msg)) => Err(PcsError::SystemExecution(format!(
                "guest schema-mismatch in run-batch (guest bug): {msg}"
            ))),
        }
    }

    fn declared_components(&self) -> Vec<&str> {
        match self.component_names.get() {
            Some(names) => names.iter().map(String::as_str).collect(),
            None => Vec::new(),
        }
    }

    fn template_dataset(&self) -> Dataset {
        let mut dataset = Dataset::new();

        let descriptor = match self.describe() {
            Ok(d) => d,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %e, "template_dataset: describe() failed, returning empty dataset");
                return dataset;
            }
        };

        for comp in &descriptor.components {
            let schema = match parse_ipc_schema(&comp.arrow_schema_ipc) {
                Ok(s) => s,
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(component = %comp.name, error = %e, "template_dataset: schema parse failed, skipping component");
                    continue;
                }
            };
            // Leak the name string to satisfy &'static str. This is a one-time
            // startup allocation (describe() is cached; template_dataset is called
            // once per service start).
            let name: &'static str = Box::leak(comp.name.clone().into_boxed_str());
            dataset.register_raw_component(name, schema);
        }

        dataset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn from_bytes_rejects_invalid_wasm() {
        let engine = WasmEngine::new().unwrap();
        let result =
            WasmPipelineRuntime::from_bytes(engine, "bad", b"not wasm at all", HashMap::new(), 10);
        let err = result.err().expect("expected error");
        let msg = err.to_string();
        assert!(msg.contains("wasm compile error"), "got: {msg}");
    }
}
