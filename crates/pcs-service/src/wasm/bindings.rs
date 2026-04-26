wasmtime::component::bindgen!({
    world: "pcs-pipeline",
    path: "../pcs-guest/wit",
});

// Convenience re-exports used across the wasm module.
// Some items are used only by downstream tasks (#15, #17) — the allows prevent
// spurious unused-import warnings while the module is being built up.
#[allow(unused_imports)]
pub use pcs::pipeline::host_io::{Host as HostIo, LogLevel};
#[allow(unused_imports)]
pub use pcs::pipeline::types::{ComponentDescriptor, PipelineDescriptor, RunError, RunResult};
// The `types` WIT interface generates an empty Host marker trait that every
// store-data type must implement alongside HostIo.
pub use pcs::pipeline::types::Host as TypesHost;
