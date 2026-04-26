---
name: Source Tree Reorg (Phases 1–5)
description: Flat-sibling module split of dataset.rs, pipeline.rs, system.rs; final file layout, test count baseline, and two doc-fix gotchas
type: project
---

Completed 2026-04-15. Branch: `fixes`.

## Final layout (flat-sibling, no mod.rs anywhere)

```
src/dataset.rs          — 211 lines: struct Dataset, impl Default, DatasetBuilder struct, IPC helpers, mod declarations, 2 interner tests
src/dataset/
  append.rs             impl Dataset { append, append_record_batch }
  builder.rs            impl Dataset { builder } + impl DatasetBuilder { with, with_resource, build }
  chunks.rs             impl Dataset { compact_chunks (pub(super)), flush_all_pending (pub(super)), get_or_build_merged (pub(super)) }
  ipc.rs                impl Dataset { annotate_batch, alive_ipc_bytes, write_ipc, write_component_ipc, read_ipc }
  lifecycle.rs          impl Dataset { mark_dead, should_compact, compact, clone_empty, clear }
  reads.rs              impl Dataset { batch_for, column, columns, view, schemas, rows, live_rows, is_alive, row_range }
  register.rs           impl Dataset { register_component, register_raw_component }
  resources.rs          impl Dataset { insert_resource, get_resource, get_resource_mut }
  write.rs              impl Dataset { replace_batch, apply_write_set }

src/pipeline.rs         — 139 lines: pub struct Pipeline, pub struct PipelineBuilder, impl Pipeline { new, name, data, data_mut }, impl Default, pub(crate) use dag::SystemEntry, mod declarations, 2 tests
src/pipeline/
  builder.rs            impl Pipeline { builder } + impl PipelineBuilder { with, with_resource, with_system, with_parallel_system, with_source, with_sink, build }
  dag.rs                OwnedFieldAccess, ExpandedMeta, pub(crate) enum SystemEntry, build_stages_inner, validate_field, impl Pipeline { validate, stages, stage_count, ensure_plan (pub(super)) }
  execution.rs          impl Pipeline { run, run_on, run_with_io, try_run_sync } + all 4 retry/parallel helpers
  registration.rs       impl Pipeline { invalidate_cache, add_system, add_parallel_system, add_system_boxed, add_parallel_system_boxed, add_source, add_sink }

src/system.rs           — 509 lines: System trait, ParallelSystem trait, all trait method sigs, mod declarations (correct — traits stay in root)
src/system/
  closure.rs            system_fn closure helper
  field.rs              FieldAccess type
  meta.rs               SystemMeta builder
  write_set.rs          WriteSet, SliceWriteSet
```

## Test count baseline (post-reorg, 2026-04-15)

- `--lib` (no features): 151
- `--lib --features io`: 186
- `--lib --features datafusion`: 193
- `--lib --features distributed`: 199
- `--lib --features distributed-raft`: 222
- `--lib --features service`: 343
- `--lib --all-features`: **479 pass, 1 ignored**
- `--doc --all-features`: **55 pass, 4 ignored**

The task plan said "expect 458" for all-features; 479 is correct — submodule splits added net-new per-submodule tests.

## Key patterns learned

**Descendant visibility**: child modules (`src/dataset/append.rs`) can directly access private fields of the parent struct (`Dataset`) defined in `src/dataset.rs` without any `pub(super)` on the fields. Rust's privacy model allows descendant modules to see ancestor private items.

**Cross-sibling visibility**: methods called by sibling submodules (e.g. `chunks.rs` helpers called from `ipc.rs`) need `pub(super)`. Methods called only within the same file stay private.

**`ensure_plan` must be `pub(super)`** in `pipeline/dag.rs` because `pipeline/execution.rs` calls `self.ensure_plan()`. A method in a submodule without visibility is private to that submodule.

**`SystemEntry` re-export**: defined as `pub(crate)` in `pipeline/dag.rs`, re-exported from `pipeline.rs` root as `pub(crate) use dag::SystemEntry`. Other crate modules (scheduler, service) import it from `crate::pipeline::SystemEntry`.

## Doc warnings fixed

Two pre-existing intra-doc-link warnings fixed:
- `src/dataset.rs:60` — `[`Row`]` in doc comment where `Row` is not in scope → changed to plain `` `Row` ``
- `src/system.rs:3` — `[`Dataset`](crate::pipeline::Dataset)` redundant explicit target → changed to `[`Dataset`]`

**Why:** `system.rs` is in `crate::system`, and `Dataset` is re-exported at crate root via `prelude::*`, so rustdoc resolves `[`Dataset`]` without the explicit path.
