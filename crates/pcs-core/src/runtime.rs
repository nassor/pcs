//! [`PipelineRuntime`], the host-side seam for swappable pipeline implementations.
//!
//! A host (standalone runner, `DistributedRunner`, service builder) holds
//! `Box<dyn PipelineRuntime>` instead of a concrete [`Pipeline`]. Two impls exist:
//! [`Pipeline`] in this crate wraps [`Pipeline::run_on`], and `WasmPipelineRuntime`
//! in `pcs-service` serializes the dataset to Arrow IPC, calls a guest WASM
//! component's `run-batch` export via wasmtime, and reads the IPC result back.
//!
//! The trait only exists when the `runtime` feature is enabled. Guest builds
//! (`--no-default-features --features guest`) use [`Pipeline`] directly through
//! the sync executor in the guest SDK and never construct a `dyn PipelineRuntime`.

use async_trait::async_trait;

use crate::{Dataset, PcsResult, Pipeline};

/// Swappable host-side execution backend for a single pipeline.
///
/// Host components hold a `Box<dyn PipelineRuntime>` so they can drive either a
/// native [`Pipeline`] or a WASM guest module through the same call site.
///
/// Implementations must be `Send` so the box can move between threads. Futures
/// produced by `run_on` need not be `Send`: the host always drives them to
/// completion from the task that owns the runtime, and `Pipeline` holds
/// `Box<dyn Sink>` which may not be `Sync`, so `Send` futures would require
/// `Pipeline: Sync`.
#[async_trait(?Send)]
pub trait PipelineRuntime: Send {
    /// Human-readable name for logging, metrics, and status endpoints.
    fn name(&self) -> &str;

    /// Execute all systems against the provided dataset.
    ///
    /// `data` is owned by the caller. Sources and sinks are **not** drained
    /// here; the host layer handles IO before and after this call.
    async fn run_on(&self, data: &mut Dataset) -> PcsResult<()>;

    /// Execute against `data`, carrying opaque runtime-internal state.
    ///
    /// `prior` is the blob this runtime returned from the previous call for the
    /// same logical partition, or `None` on first run. The returned blob is
    /// persisted verbatim by the host and handed back as the next `prior`. Hosts
    /// must treat it as opaque; only the runtime knows the layout.
    ///
    /// The default implementation ignores `prior`, delegates to
    /// [`run_on`](Self::run_on), and returns `Ok(None)`, which is correct for any
    /// runtime whose state lives entirely in the `Dataset`.
    async fn run_on_with_state(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> PcsResult<Option<Vec<u8>>> {
        let _ = prior;
        self.run_on(data).await?;
        Ok(None)
    }

    /// Component names this runtime expects to find in the dataset.
    ///
    /// Called at load and validation time only. Hosts use the list to check
    /// `target_component` / `source_component` declarations in TOML config
    /// against the components the runtime handles, before the first `run_on`
    /// call.
    ///
    /// The default returns an empty slice, which suffices for runtimes that
    /// validate internally.
    fn declared_components(&self) -> Vec<&str> {
        Vec::new()
    }

    /// Return a schema-only, empty [`Dataset`] matching this runtime's components.
    ///
    /// Called once by host runners at construction time to seed the dataset used
    /// in each processing iteration. Every component schema must be registered in
    /// the returned dataset's `SchemaRegistry`; resources and row data are not
    /// required.
    ///
    /// There is no default body: a schemaless fallback would surface later as an
    /// opaque `ensure_plan` failure. A runtime that cannot build a valid template
    /// (e.g. corrupted guest describe) must fail in its constructor instead.
    fn template_dataset(&self) -> Dataset;
}

#[async_trait(?Send)]
impl PipelineRuntime for Pipeline {
    fn name(&self) -> &str {
        Pipeline::name(self)
    }

    async fn run_on(&self, data: &mut Dataset) -> PcsResult<()> {
        Pipeline::run_on(self, data).await
    }

    fn declared_components(&self) -> Vec<&str> {
        self.data.schemas().iter().map(|(k, _)| *k).collect()
    }

    fn template_dataset(&self) -> Dataset {
        self.data.clone_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::Component;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    struct Alpha;
    impl Component for Alpha {
        fn name() -> &'static str {
            "Alpha"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
        }
    }

    struct Beta;
    impl Component for Beta {
        fn name() -> &'static str {
            "Beta"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]))
        }
    }

    struct Gamma;
    impl Component for Gamma {
        fn name() -> &'static str {
            "Gamma"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]))
        }
    }

    #[test]
    fn pipeline_impls_runtime_name() {
        let p = Pipeline::new("example");
        let rt: &dyn PipelineRuntime = &p;
        assert_eq!(rt.name(), "example");
    }

    #[test]
    fn declared_components_lists_registered_schemas() {
        let mut p = Pipeline::new("trio");
        p.data_mut().register_component::<Alpha>().unwrap();
        p.data_mut().register_component::<Beta>().unwrap();
        p.data_mut().register_component::<Gamma>().unwrap();

        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let mut names = rt.declared_components();
        names.sort();
        assert_eq!(names, vec!["Alpha", "Beta", "Gamma"]);
    }

    #[tokio::test]
    async fn run_on_dispatches_through_trait_object() {
        let p = Pipeline::new("empty");
        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let mut data = Dataset::new();
        rt.run_on(&mut data).await.unwrap();
    }

    #[test]
    fn template_dataset_preserves_registered_schemas() {
        let mut p = Pipeline::new("trio");
        p.data_mut().register_component::<Alpha>().unwrap();
        p.data_mut().register_component::<Beta>().unwrap();
        p.data_mut().register_component::<Gamma>().unwrap();

        let rt: Box<dyn PipelineRuntime> = Box::new(p);
        let tmpl = rt.template_dataset();

        assert!(tmpl.schemas().contains("Alpha"));
        assert!(tmpl.schemas().contains("Beta"));
        assert!(tmpl.schemas().contains("Gamma"));
        assert_eq!(tmpl.rows(), 0);
    }
}
