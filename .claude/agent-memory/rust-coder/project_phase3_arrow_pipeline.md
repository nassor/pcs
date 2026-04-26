---
name: Phase 3 ArrowSystem and ArrowPipeline
description: ArrowSystem trait + field-granular DAG pipeline added under src/arrow/; critical conflict detection bug and fix recorded
type: project
---

Phase 3 added `src/arrow/system.rs` and `src/arrow/pipeline.rs` to the ecs-rewrite branch.

**Key design decision — conflict detection direction:**
The `must_precede(a, a_idx, b, b_idx)` function uses a strict `a_idx >= b_idx → return false` guard.
ALL three conflict rules (write-read, write-write, read-write) only fire when `a_idx < b_idx`.
Without this guard, the write-read rule fires bidirectionally and creates cycles in the dependency graph,
making both systems end up at the same level despite having a real dependency.

**Why:** If A reads F and B writes F (A registered first), calling `must_precede(B, 1, A, 0)` would
fire Rule 1 (B.writes vs A.reads) and create edge (1, 0). Combined with Rule 3 creating edge (0, 1),
Kahn's algorithm sees a cycle and puts both systems at level 0 (same stage). The registration-order
guard prevents this: only the earlier system can impose ordering on the later one.

**OwnedFieldAccess vs FieldAccess:** User-facing `FieldAccess` uses `&'static str` for both component
and field. Internal `ExpandedMeta` uses `OwnedFieldAccess` with `String` field to handle dynamically
discovered fields from `reads_components`/`writes_components` expansion (Arrow schema field names
are heap-allocated Strings, not `&'static str`).

**ArrowWorld additions:**
- `schemas()` accessor — returns `&SchemaRegistry` for pipeline validation
- `replace_batch::<C>()` — replaces the whole RecordBatch for a component (used by systems that
  recompute derived columns); validates schema + row count match

**New files:**
- `src/arrow/system.rs` — 607 lines (trait + FnArrowSystem + tests)
- `src/arrow/pipeline.rs` — 1295 lines (pipeline + DAG builder + 15 tests)
- `examples/arrow_pipeline_etl.rs` — 534 lines

**Test count:** 152 (default) + 81 new (arrow) = 233 total with `--features arrow`

**How to apply:** When extending the conflict detection or scheduler, remember the registration-order
guard is load-bearing. Any rule that fires in both directions of (a, b) will create cycles.
