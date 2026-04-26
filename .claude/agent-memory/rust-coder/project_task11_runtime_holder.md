---
name: Task #11 RuntimeHolder — BuiltService escape hatch
description: Key design decisions and non-obvious gotchas from BuiltService refactor for WASM pivot Phase 2
type: project
---

Private `enum RuntimeHolder { Native(Pipeline) }` in `pcs-service/src/service/builder.rs`.

**Why:** `BuiltService.pipeline: Pipeline` (pub field) replaced so Phase 3 can add `Dynamic(Box<dyn PipelineRuntime>)` without touching call sites. `Pipeline` is not `Clone`, so `Option<Pipeline>` alongside a trait object would require two instances.

**4 escape hatches on BuiltService:**
- `as_native(&self) -> Option<&Pipeline>` — sequential read-only access
- `as_native_mut(&mut self) -> Option<&mut Pipeline>` — sequential mutable access
- `into_native(self) -> Option<Pipeline>` — cluster.rs pre-#10 (temporary; replace with `into_runtime` when dist-expert's #10 lands)
- `into_runtime(self) -> Box<dyn PipelineRuntime>` — permanent post-#10 path

**Critical borrow-checker gotcha:** `as_native_mut()` takes `&mut self` — cannot be called inside a `for built.sources.iter_mut()` loop because the source iterator already holds `&mut built`. Solution: `pub(crate) fn native_parts_mut(&mut self) -> (&mut Pipeline, &mut Vec<BuiltSource>, &mut Vec<BuiltSink>)` returns disjoint field refs in one call. Use inside loops; use `as_native_mut` only for sequential call sites.

**flush_sinks split:** `flush_sinks(&mut BuiltService, ...)` calls `native_parts_mut` and delegates to `flush_sinks_inner(&Pipeline, &mut [BuiltSink], ...)` to avoid re-borrowing `built` inside the sink loop.

**Source drain loop pattern** (standalone.rs): index-based loop with inner borrow scope:
```rust
for i in 0..built.sources.len() {
    let result = {
        let (pipeline, sources, _) = built.native_parts_mut();
        tokio::select! {
            r = drain_into_dataset(sources[i].source.as_mut(), pipeline.data_mut(), ...) => Some(r),
            _ = cancel.cancelled() => None,
        }
    }; // borrow drops here
    // handle None via cancelled_during_drain flag, flush after loop
}
```

**Test construction:** `#[cfg(test)] pub(crate) fn from_pipeline(pipeline, sources, sinks, registry) -> Self`. Used in standalone.rs and cluster.rs tests. Gated `#[cfg(test)]` to avoid dead-code lint in release builds.

**Feature flags:** `pcs_core::runtime::PipelineRuntime` available without extra feature because `pcs-core` has `default = ["runtime"]`. Import as `use pcs_core::runtime::PipelineRuntime` directly.

**How to apply:** When Phase 3 adds WASM Dynamic variant, add `Dynamic(Box<dyn PipelineRuntime>)` to `RuntimeHolder` and update all 4 escape hatches to handle it. `native_parts_mut` panics on `Dynamic` — replace standalone.rs with trait-object path at that point (#16 scope).
