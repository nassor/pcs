---
name: Phase 3 Host Service Layer Complete
description: Tasks #11,#14,#15,#16,#17 shipped in one session; PipelineRuntime trait, WASM host, ServiceBuilder rewrite, 3-gate validation; 306 tests green
type: project
---

Phase 3 host-side service layer fully shipped as of 2026-04-16.

Tasks completed by rust-coder (coder-host role):
- **#11**: BuiltService with RuntimeHolder enum + 4 escape hatches (later superseded by #16)
- **#14**: wasmtime host integration — wasm/ module, WasmPipelineRuntime, bindings, host_impl, engine
- **#15**: pipeline.wasm YAML stanza + PipelineRuntimeLoader (loader.rs), SHA-256 check, eager describe()
- **#16**: ServiceBuilder rewrite — RuntimeHolder collapsed to `pub runtime: Box<dyn PipelineRuntime>`, SystemFactory/ComponentFactory/BuiltSystem deleted, generic_component.rs deleted, standalone.rs rewritten with template_dataset() pattern, cluster.rs fixed, deprecation warnings for legacy YAML stanzas
- **#17**: 3-gate load-time validation — validation.rs with validate_io_coverage(), gate 3 wired into serve.rs (before ready flip) and validate CLI subcommand (after build)

**Why:** Architect-driven Phase 3 plan to add WASM guest support and clean up the service layer.

**How to apply:** Phase 3 is complete. Remaining open tasks (#24/#25 dist-expert, #28 minor) are not host-service work. Next host-side work would be Phase 4 (rip out legacy pipeline.systems/components YAML entirely).

Key design decisions that survived:
- `template_dataset()` is a **required method** on `PipelineRuntime` with no default — prevents schemaless fallback causing opaque runtime failures
- `validate_io_coverage` skips check when `declared_components()` returns empty — runtime opts out rather than false-positive blocking
- Destructuring `let BuiltService { runtime, mut sources, mut sinks, registry: _ } = built` in standalone.rs cleanly separates borrows without native_parts_mut()
- WASM runtimes return `Dataset::new()` from `template_dataset()` — schemas arrive at runtime via describe(), not compile-time
