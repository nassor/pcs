pub mod error;
pub mod retry;

pub mod column;
pub mod component;
pub mod dataset;
pub mod partition;
pub mod pipeline;
pub mod resource;
pub mod row;
pub mod scheduler;
pub mod schema;
pub mod system;

#[cfg(feature = "runtime")]
pub mod runtime;

#[cfg(feature = "io")]
pub mod io;

#[cfg(feature = "windows")]
pub mod windows;

pub use error::{PcsError, PcsResult};
pub use retry::{RetryMode, SystemConfig};

pub use component::Component;
pub use partition::KeyPartition;
pub use pipeline::{Dataset, Pipeline, PipelineBuilder, RunStats};
pub use row::Row;
pub use scheduler::{BackpressureSpec, DependencyKind, PipelineConfig, Scheduler};
pub use schema::SchemaRegistry;
pub use system::{
    FieldAccess, FieldRef, ParallelSystem, ResourceUpdate, SliceWriteSet, System, SystemMeta,
    WriteSet, system_fn,
};

#[cfg(feature = "runtime")]
pub use runtime::PipelineRuntime;

pub mod prelude {
    pub use crate::{
        BackpressureSpec, Component, Dataset, DependencyKind, FieldAccess, KeyPartition,
        ParallelSystem, PcsError, PcsResult, Pipeline, PipelineBuilder, PipelineConfig,
        ResourceUpdate, RetryMode, Row, RunStats, Scheduler, SchemaRegistry, SliceWriteSet, System,
        SystemConfig, SystemMeta, WriteSet, system_fn,
    };

    pub use crate::column::ComponentView;
    pub use crate::pipeline::DatasetBuilder;
    pub use crate::system::FieldRef;

    pub use async_trait::async_trait;

    #[cfg(feature = "windows")]
    pub use crate::windows::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator};
}
