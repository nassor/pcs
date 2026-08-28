use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use pcs_core::runtime::RuntimeOutput;
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
/// processor's `run-batch` export via wasmtime on a fresh `Store`, then reads the
/// output IPC bytes back over the dataset contents.
///
/// Linking (WASI-p2 plus host-io) and instantiation planning happen once, at
/// load time, producing a reusable [`PcsPipelinePre`]; a call then costs store
/// creation plus instantiation from that plan. Processor linear memory never
/// survives a batch, so the checkpoint blob is the only channel for processor
/// state.
///
/// Processor traps are mapped to `PcsError::SystemExecution`. The epoch deadline
/// (100 ms ticks) limits runaway processor execution.
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
    /// The workflow this runtime belongs to, and this node's declared id.
    ///
    /// Set by [`with_identity`](Self::with_identity), which the service
    /// builder calls immediately after loading — the last place that knows a
    /// runtime's workflow and node id before `Box<dyn PipelineRuntime>` erases
    /// it. Attributes this processor's `pcs_processor_*` samples and its
    /// `processor.batch` span; empty for a runtime built directly and never
    /// given an identity.
    workflow_id: String,
    processor_id: String,
    /// Cached descriptor, populated on first `describe()` call.
    descriptor: Mutex<Option<PipelineDescriptor>>,
    /// Component names extracted from the descriptor, for `declared_components()`.
    component_names: OnceLock<Vec<String>>,
}

/// Everything one processor call needs from a [`WasmPipelineRuntime`], owned so
/// the call can move onto a blocking thread.
///
/// Every field is an `Arc` bump or a short string, so building one is cheap
/// enough to do per batch.
struct CallParts {
    engine: WasmEngine,
    pre: PcsPipelinePre<HostState>,
    name: String,
    config: Arc<HashMap<String, String>>,
    epoch_deadline: u64,
    processor_id: String,
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
            workflow_id: String::new(),
            processor_id: String::new(),
            descriptor: Mutex::new(None),
            component_names: OnceLock::new(),
        })
    }

    /// Declare this runtime's workflow and node id.
    ///
    /// Consuming rather than a setter because a runtime's identity never
    /// changes once built, and the service builder assigns it immediately
    /// after loading — the last place that knows it before `Box<dyn
    /// PipelineRuntime>` erases it. Left unset, every sample this runtime
    /// writes carries an empty processor id.
    #[must_use]
    pub fn with_identity(mut self, workflow_id: String, processor_id: String) -> Self {
        self.workflow_id = workflow_id;
        self.processor_id = processor_id;
        self
    }

    /// Build a fresh `Store` and instantiate the pre-linked component.
    ///
    /// Takes [`CallParts`] by value rather than `&self` so it can run inside
    /// `spawn_blocking` without borrowing the runtime across the thread
    /// boundary, and so the pipeline name is allocated once per call rather
    /// than cloned again here.
    fn make_store_and_instance(parts: CallParts) -> PcsResult<(Store<HostState>, PcsPipeline)> {
        let CallParts {
            engine,
            pre,
            name,
            config,
            epoch_deadline,
            processor_id,
        } = parts;

        let host = HostState::new(name, config, processor_id);
        let mut store = Store::new(&engine.engine, host);
        store.set_epoch_deadline(epoch_deadline);

        let instance = pre
            .instantiate(&mut store)
            .map_err(|e| PcsError::SystemExecution(format!("processor trap (instantiate): {e}")))?;

        Ok((store, instance))
    }

    /// Everything `self` contributes to one processor call.
    fn call_parts(&self) -> CallParts {
        CallParts {
            engine: self.engine.clone(),
            pre: self.pre.clone(),
            name: self.name.clone(),
            config: Arc::clone(&self.config),
            epoch_deadline: self.epoch_deadline,
            processor_id: self.processor_id.clone(),
        }
    }

    /// Call `describe()` and cache the result.
    ///
    /// The first call instantiates a fresh store; subsequent calls return the
    /// cached descriptor without any processor round-trip.
    pub fn describe(&self) -> PcsResult<PipelineDescriptor> {
        {
            let guard = self.descriptor.lock().unwrap();
            if let Some(d) = guard.as_ref() {
                return Ok(d.clone());
            }
        }

        let (mut store, instance) = Self::make_store_and_instance(self.call_parts())?;
        let iface = instance.pcs_pipeline_pipeline();
        let desc = iface
            .call_describe(&mut store)
            .map_err(|e| PcsError::SystemExecution(format!("processor trap (describe): {e}")))?;

        let names: Vec<String> = desc.components.iter().map(|c| c.name.clone()).collect();
        self.component_names.get_or_init(|| names);

        let mut guard = self.descriptor.lock().unwrap();
        *guard = Some(desc.clone());
        Ok(desc)
    }

    /// Serialise, call the processor, and read the result back, carrying the
    /// batch's routing decision alongside the state blob.
    async fn run_batch(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> PcsResult<RuntimeOutput> {
        #[cfg(feature = "tracing")]
        let batch_span = tracing::info_span!(
            "processor.batch",
            workflow = %self.workflow_id,
            processor = %self.processor_id,
            rows_in = data.rows() as u64,
            rows_out = tracing::field::Empty,
            systems_run = tracing::field::Empty,
            retries = tracing::field::Empty,
            guest_wall_us = tracing::field::Empty
        );

        let mut ipc_bytes: Vec<u8> = Vec::new();
        data.write_ipc(&mut ipc_bytes)?;

        // `bindgen!` lowers `option<list<u8>>` to `Option<&Vec<u8>>`, so the
        // prior state has to be owned for the call.
        let prior_owned = prior.map(<[u8]>::to_vec);
        let parts = self.call_parts();

        // `spawn_blocking` runs outside the task-local context, so the span is
        // moved into the closure and entered there. That is what puts the
        // processor's own `host-io::log` lines inside this trace.
        #[cfg(feature = "tracing")]
        let call_span = batch_span.clone();

        // The processor is linked against the synchronous WASI implementation
        // (`add_to_linker_sync`), so any WASI import it touches routes through
        // `wasmtime_wasi::runtime::in_tokio`, which calls `Handle::block_on`.
        // That panics outright on a thread already driving a tokio runtime, so
        // awaiting the call inline would kill the service on the first batch of
        // any processor that writes to stdout.
        //
        // `spawn_blocking` threads are not async execution contexts, so
        // `block_on` is legal there. Nothing borrowed from `data` crosses the
        // boundary: IPC bytes in, IPC bytes out.
        let joined = tokio::task::spawn_blocking(move || -> PcsResult<_> {
            // The closure body has no `.await`, so a plain guard is correct.
            #[cfg(feature = "tracing")]
            let _call_guard = call_span.enter();
            let (mut store, instance) = Self::make_store_and_instance(parts)?;
            instance
                .pcs_pipeline_pipeline()
                .call_run_batch(&mut store, &ipc_bytes, prior_owned.as_ref())
                .map_err(|e| PcsError::SystemExecution(format!("processor trap (run-batch): {e}")))
        })
        .await
        .map_err(|e| PcsError::SystemExecution(format!("processor task join failed: {e}")))?;

        match joined? {
            Ok(result) => {
                let m = &result.metrics;
                crate::metrics::instruments().processor_batch(
                    &self.processor_id,
                    m.wall_ns,
                    m.rows_in,
                    m.rows_out,
                    m.systems_run,
                    m.retries,
                );
                #[cfg(feature = "tracing")]
                {
                    batch_span.record("rows_out", m.rows_out);
                    batch_span.record("systems_run", m.systems_run);
                    batch_span.record("retries", m.retries);
                    batch_span.record("guest_wall_us", m.wall_ns / 1_000);
                }
                let mut out_slice: &[u8] = &result.output;
                *data = Dataset::read_ipc(&mut out_slice)?;
                let routes = result.routes;
                Ok(RuntimeOutput {
                    state: result.checkpoint,
                    routes,
                })
            }
            Err(RunError::Retryable(msg)) => Err(PcsError::SystemExecution(format!(
                "processor retryable: {msg}"
            ))),
            Err(RunError::Permanent(msg)) => Err(PcsError::SystemExecution(format!(
                "processor permanent: {msg}"
            ))),
            // run-batch MUST NOT emit schema-mismatch; treat as permanent bug.
            Err(RunError::SchemaMismatch(msg)) => Err(PcsError::SystemExecution(format!(
                "processor schema-mismatch in run-batch (processor bug): {msg}"
            ))),
        }
    }
}

#[async_trait(?Send)]
impl pcs_core::runtime::PipelineRuntime for WasmPipelineRuntime {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
        self.run_batch(data, None).await.map(|_| ())
    }

    async fn run_on_with_state(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> PcsResult<Option<Vec<u8>>> {
        self.run_batch(data, prior).await.map(|out| out.state)
    }

    async fn run_on_with_state_and_routes(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> PcsResult<RuntimeOutput> {
        self.run_batch(data, prior).await
    }

    fn declared_components(&self) -> Vec<&str> {
        match self.component_names.get() {
            Some(names) => names.iter().map(String::as_str).collect(),
            None => Vec::new(),
        }
    }

    /// Report the processor's own `describe()` record.
    ///
    /// Reads the cache `PipelineRuntimeLoader::load` warms at startup, so this
    /// never instantiates a store. A runtime built directly, before any
    /// `describe()` call, reports the empty default.
    fn descriptor_info(&self) -> pcs_core::runtime::RuntimeDescriptorInfo {
        let guard = self
            .descriptor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.as_ref() {
            Some(d) => pcs_core::runtime::RuntimeDescriptorInfo {
                name: d.name.clone(),
                version: d.version.clone(),
                stateful: d.stateful,
                schema_fingerprint: d.schema_fingerprint.clone(),
            },
            None => pcs_core::runtime::RuntimeDescriptorInfo::default(),
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
