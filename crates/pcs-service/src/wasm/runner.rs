use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use pcs_core::{Dataset, PcsError, PcsResult};
use wasmtime::Store;
use wasmtime::component::{Component, HasSelf, Linker};

use crate::descriptor::template_dataset_from;

use super::bindings::{PcsPipeline, PcsPipelinePre, PipelineDescriptor, RunError};
use super::engine::WasmEngine;
use super::host_impl::HostState;

/// Host-side WASM pipeline runtime implementing [`pcs_core::runtime::PipelineRuntime`].
///
/// Each `run_on` call serialises the dataset to Arrow IPC bytes, calls the
/// guest's `run-batch` export via wasmtime on a fresh `Store`, then reads the
/// output IPC bytes back over the dataset contents.
///
/// Linking (WASI-p2 plus host-io) and instantiation planning happen once, at
/// load time, producing a reusable [`PcsPipelinePre`]; a call then costs store
/// creation plus instantiation from that plan. Guest linear memory never
/// survives a batch, so the checkpoint blob is the only channel for guest
/// state.
///
/// Guest traps are mapped to `PcsError::SystemExecution`. The epoch deadline
/// (100 ms ticks) limits runaway guest execution.
pub struct WasmPipelineRuntime {
    name: String,
    engine: WasmEngine,
    /// Pre-linked and pre-planned component, reused across calls. Cloning is an
    /// `Arc` bump.
    pre: PcsPipelinePre<HostState>,
    /// Shared with every per-call `HostState`; never mutated after load.
    config: Arc<HashMap<String, String>>,
    /// Per-call epoch deadline in ticks, 100 ms per tick.
    epoch_deadline: u64,
    /// Cached descriptor, populated on first `describe()` call.
    descriptor: Mutex<Option<PipelineDescriptor>>,
    /// Component names extracted from the descriptor, for `declared_components()`.
    component_names: OnceLock<Vec<String>>,
}

impl WasmPipelineRuntime {
    /// Compile a WASM component from raw bytes, then link and pre-instantiate it.
    ///
    /// Compilation and linking are synchronous and expensive; both happen here,
    /// once. The runtime is `Send` and can be wrapped in `Arc` for sharing.
    pub fn from_bytes(
        engine: WasmEngine,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        config: HashMap<String, String>,
        epoch_deadline_ticks: u64,
    ) -> PcsResult<Self> {
        let component = Component::from_binary(&engine.engine, wasm_bytes)
            .map_err(|e| PcsError::Configuration(format!("wasm compile error: {e}")))?;

        let mut linker: Linker<HostState> = Linker::new(&engine.engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .map_err(|e| PcsError::Configuration(format!("wasi linker error: {e}")))?;
        PcsPipeline::add_to_linker::<_, HasSelf<_>>(&mut linker, |s| s)
            .map_err(|e| PcsError::Configuration(format!("wasm linker error: {e}")))?;

        let instance_pre = linker
            .instantiate_pre(&component)
            .map_err(|e| PcsError::Configuration(format!("wasm pre-instantiate error: {e}")))?;
        let pre = PcsPipelinePre::new(instance_pre)
            .map_err(|e| PcsError::Configuration(format!("wasm binding error: {e}")))?;

        Ok(Self {
            name: name.into(),
            engine,
            pre,
            config: Arc::new(config),
            epoch_deadline: epoch_deadline_ticks,
            descriptor: Mutex::new(None),
            component_names: OnceLock::new(),
        })
    }

    /// Build a fresh `Store` and instantiate the pre-linked component.
    ///
    /// Associated rather than a method so it can run inside `spawn_blocking`
    /// without borrowing `self` across the thread boundary.
    fn make_store_and_instance(
        engine: &WasmEngine,
        pre: &PcsPipelinePre<HostState>,
        name: &str,
        config: Arc<HashMap<String, String>>,
        epoch_deadline: u64,
    ) -> PcsResult<(Store<HostState>, PcsPipeline)> {
        let host = HostState::new(name.to_string(), config);
        let mut store = Store::new(&engine.engine, host);
        store.set_epoch_deadline(epoch_deadline);

        let instance = pre
            .instantiate(&mut store)
            .map_err(|e| PcsError::SystemExecution(format!("guest trap (instantiate): {e}")))?;

        Ok((store, instance))
    }

    /// Everything `self` contributes to a guest call, owned so the call can move
    /// onto a blocking thread. Every clone is an `Arc` bump or a short string.
    fn call_parts(
        &self,
    ) -> (
        WasmEngine,
        PcsPipelinePre<HostState>,
        String,
        Arc<HashMap<String, String>>,
        u64,
    ) {
        (
            self.engine.clone(),
            self.pre.clone(),
            self.name.clone(),
            Arc::clone(&self.config),
            self.epoch_deadline,
        )
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

        let (engine, pre, name, config, epoch_deadline) = self.call_parts();
        let (mut store, instance) =
            Self::make_store_and_instance(&engine, &pre, &name, config, epoch_deadline)?;
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
}

#[async_trait(?Send)]
impl pcs_core::runtime::PipelineRuntime for WasmPipelineRuntime {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
        self.run_on_with_state(data, None).await.map(|_| ())
    }

    async fn run_on_with_state(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> PcsResult<Option<Vec<u8>>> {
        let mut ipc_bytes: Vec<u8> = Vec::new();
        data.write_ipc(&mut ipc_bytes)?;

        // `bindgen!` lowers `option<list<u8>>` to `Option<&Vec<u8>>`, so the
        // prior state has to be owned for the call.
        let prior_owned = prior.map(<[u8]>::to_vec);
        let (engine, pre, name, config, epoch_deadline) = self.call_parts();

        // The guest is linked against the synchronous WASI implementation
        // (`add_to_linker_sync`), so any WASI import it touches routes through
        // `wasmtime_wasi::runtime::in_tokio`, which calls `Handle::block_on`.
        // That panics outright on a thread already driving a tokio runtime, so
        // awaiting the call inline would kill the service on the first batch of
        // any guest that writes to stdout.
        //
        // `spawn_blocking` threads are not async execution contexts, so
        // `block_on` is legal there. Nothing borrowed from `data` crosses the
        // boundary: IPC bytes in, IPC bytes out.
        let joined = tokio::task::spawn_blocking(move || -> PcsResult<_> {
            let (mut store, instance) =
                Self::make_store_and_instance(&engine, &pre, &name, config, epoch_deadline)?;
            instance
                .pcs_pipeline_pipeline()
                .call_run_batch(&mut store, &ipc_bytes, prior_owned.as_ref())
                .map_err(|e| PcsError::SystemExecution(format!("guest trap (run-batch): {e}")))
        })
        .await
        .map_err(|e| PcsError::SystemExecution(format!("guest task join failed: {e}")))?;

        match joined? {
            Ok(result) => {
                let mut out_slice: &[u8] = &result.output;
                *data = Dataset::read_ipc(&mut out_slice)?;
                Ok(result.checkpoint)
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
        let descriptor = match self.describe() {
            Ok(d) => d,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "template_dataset: describe() failed, returning empty dataset");
                return Dataset::new();
            }
        };

        template_dataset_from(
            descriptor
                .components
                .iter()
                .map(|comp| (comp.name.as_str(), comp.arrow_schema_ipc.as_slice())),
        )
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
