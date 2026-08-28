pub use pcs_core::column;
pub use pcs_core::component;
pub use pcs_core::dataset;
pub use pcs_core::error;
pub use pcs_core::partition;
pub use pcs_core::pipeline;
pub use pcs_core::resource;
pub use pcs_core::retry;
pub use pcs_core::row;
pub use pcs_core::scheduler;
pub use pcs_core::schema;
pub use pcs_core::system;

#[cfg(feature = "windows")]
pub use pcs_core::windows;

pub use pcs_core::Component;
pub use pcs_core::Row;
pub use pcs_core::SchemaRegistry;
pub use pcs_core::{BackpressureSpec, DependencyKind, PipelineConfig, Scheduler};
pub use pcs_core::{Dataset, Pipeline, PipelineBuilder, RunStats};
pub use pcs_core::{
    FieldAccess, FieldRef, ParallelSystem, ResourceUpdate, SliceWriteSet, System, SystemMeta,
    WriteSet, system_fn,
};
pub use pcs_core::{PcsError, PcsResult};
pub use pcs_core::{RetryMode, SystemConfig};

// The service's own OpenTelemetry instruments. Not gated: the no-op impl keeps
// every call site free of `#[cfg]`.
pub mod metrics;

// In-process telemetry. Sibling of `metrics`, not part of `service`, so a
// library embedder can capture spans and samples without the axum control
// plane.
#[cfg(feature = "inspector")]
pub mod inspector;

#[cfg(feature = "distributed")]
pub mod distributed;

// Shared by the two out-of-process pipeline runtimes: both decode declared
// component schemas into a template dataset the same way.
#[cfg(any(feature = "wasm", feature = "plugin"))]
mod descriptor;

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "plugin")]
pub mod plugin;

#[cfg(feature = "service")]
pub mod service;

/// Convenience re-exports of the most commonly used types and traits.
///
/// `use pcs_service::prelude::*;`
pub mod prelude {
    pub use crate::{
        BackpressureSpec, Component, Dataset, DependencyKind, FieldAccess, ParallelSystem,
        PcsError, PcsResult, Pipeline, PipelineBuilder, PipelineConfig, ResourceUpdate, RetryMode,
        Row, RunStats, Scheduler, SchemaRegistry, SliceWriteSet, System, SystemConfig, SystemMeta,
        WriteSet, system_fn,
    };

    pub use crate::column::ComponentView;
    pub use crate::dataset::DatasetBuilder;
    pub use crate::system::FieldRef;

    pub use async_trait::async_trait;

    #[cfg(feature = "windows")]
    pub use crate::windows::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator};

    pub use crate::partition::KeyPartition;
}
