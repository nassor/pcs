---
name: Schema Versioning (Task #19)
description: Component::version/migrate, SchemaEntry in registry, IPC version metadata, migration dispatch, service layer threading
type: project
---

Schema versioning feature implemented across the full stack.

**Key changes:**
- `src/component.rs`: `Component` trait gained `version() -> u32` (default 1) and `migrate(from_version, batch)` (default reject-on-mismatch)
- `src/schema.rs`: `SchemaRegistry` internals replaced with `SchemaEntry { version, schema, migrate: Arc<dyn Fn> }`. Manual `Debug` impl required (closure kills derive). New methods: `get_version`, `migrate`, `fingerprint` (FNV-1a, stable across Rust versions). `register_raw` takes `version: u32`.
- `src/dataset.rs`: Added `SCHEMA_VERSION_KEY = "__pcs_schema_version"` constant.
- `src/dataset/ipc.rs`: `annotate_batch` takes `version: u32`, embeds it in IPC metadata. `read_ipc` parses on-disk version from metadata and passes to `register_raw`.
- `src/dataset/register.rs`: `register_raw_component` delegates to new `register_raw_component_versioned(name, schema, version)`.
- `src/dataset/lifecycle.rs`: `clone_empty` reads version via `get_version` before calling `register_raw`.
- `src/windows/accumulator.rs`: `Component::version()` and `Component::migrate()` added; inner `migrate_to_current_inner(from_version, batch)` extracted; public `migrate_to_current(batch)` kept as auto-detect wrapper.
- `src/distributed/accumulator_store.rs`: Uses `WindowAccumulator::migrate(on_disk_version, batch)` via registry version lookup after `read_ipc`. Removed `migrate_to_current` import.
- `src/distributed/runner.rs`: `save_checkpoint` uses `data.schemas().fingerprint()` instead of `self.config.schema_id`.
- `src/service/config.rs`: `ComponentInstance` gained `pub version: Option<u32>` with `#[serde(default)]`.
- `src/service/registry.rs`: `ComponentFactory::register` takes `version: u32` param, calls `register_raw_component_versioned`.
- `src/service/builder.rs`: Threads `inst.version.unwrap_or(1)` to `factory.register`.

**Why:** FNV-1a required because `DefaultHasher` is not stable across Rust versions — fingerprint must be deterministic for checkpoint tagging.

**How to apply:** When adding a new component version, bump `fn version() -> u32`, add arm to `fn migrate`, and keep old schema handling in the migration fn.
