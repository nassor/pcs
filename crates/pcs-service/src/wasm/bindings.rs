// `path` is `CARGO_MANIFEST_DIR`-relative. It names a vendored copy under
// this crate's own `wit/`, not `../pcs-processor/wit` directly: `cargo
// package`/`publish` never includes files outside this crate's own
// directory, so reaching into a sibling crate here would silently drop out
// of a published tarball and fail to compile. `wit/pipeline.wit`'s own doc
// comment, and the `wit_vendored_copy_matches_pcs_processor` test in
// `tests/wit_vendored.rs`, keep the two copies from drifting.
wasmtime::component::bindgen!({
    world: "pcs-pipeline",
    path: "wit",
});

// Convenience re-exports used across the wasm module.
#[allow(unused_imports)]
pub use pcs::pipeline::host_io::{Host as HostIo, LogLevel};
#[allow(unused_imports)]
pub use pcs::pipeline::types::{ComponentDescriptor, PipelineDescriptor, RunError, RunResult};
// The `types` WIT interface generates an empty Host marker trait that every
// store-data type must implement alongside HostIo.
pub use pcs::pipeline::types::Host as TypesHost;
