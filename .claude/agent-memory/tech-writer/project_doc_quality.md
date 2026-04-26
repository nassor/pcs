---
name: Documentation Quality Baseline
description: Post-Phase-8 doc coverage status — which markdown files are current and which issues were fixed in the April 2026 pass
type: project
---

## Status as of April 2026 (post-32-item code review)

All four owned markdown docs were reviewed and updated. Old ECS-API doc quality notes are no longer relevant — that API is gone. Notes below reflect the Arrow v1.0.0-alpha API.

## Files Updated (April 2026 doc pass)

### README.md
- Fixed stale benchmark numbers: 4.4×/19× encode/decode → 1.1×/11.1× (actual Phase 7 results)
- Fixed `SystemMeta` method calls: removed non-existent `read_component_field::<T>()` / `write_component_field::<T>()` — replaced with `.read("Component", "field")` / `.write("Component", "field")`
- Fixed `write_component::<Order>()` generic syntax → `write_component("Order")` (method takes `&'static str`)
- Fixed `world.len::<Order>()` → `world.rows()`
- Fixed `col.as_primitive::<Float64Type>()` → `col.as_any().downcast_ref::<Float64Array>()`
- Fixed wide-schema number: 2.7× → 3.1× (Phase 7 result)

### docs/interop.md
- Full rewrite: added proper async context, correct import paths, accurate method table for `DataFusionSource` and `ParquetCheckpointStore`
- Clarified `archive_checkpoint` is sync; `load_checkpoint` is async
- Added `service-cluster` feature note

### docs/migration/0.x-to-1.0.md
- Fixed non-existent `read_component_field::<T>()` references
- Fixed `world.len::<T>()` → `world.rows()`
- Fixed `col.as_primitive::<Float64Type>()` → `col.as_any().downcast_ref::<Float64Array>()`
- Updated benchmark numbers to Phase 7 actuals
- Fixed breaking-changes list: `reads_resource`/`writes_resource` are NOT removed — they ARE the v1.0 API

### docs/design/columnar-rewrite.md
- Fixed `ArrowWorld` → `World` (the type was renamed by the time v1.0-alpha shipped)
- Removed reference to non-existent `schema!` macro
- Removed reference to non-existent `WorldCodec` trait
- Added section 7 on Distributed Execution Architecture (was missing entirely)
- Updated section 8 (Migration Story) to reflect shipped state
- Updated Phase 7 exit criteria to reflect actual results
- Fixed benchmark file references: `benches/world_performance.rs` → actual bench names

## Known Remaining Gaps

- `src/lib.rs` crate-level doc: still minimal compared to what's possible
- No CHANGELOG.md anywhere
- HTML docs in `docs/` are not in scope for tech-writer agent (owned by web-designer agents)
- `docs/operations/running-canudo.md` and `docs/design/service.md` owned by `writer-ops` agent
