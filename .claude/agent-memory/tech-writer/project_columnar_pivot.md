---
name: Columnar Rewrite — Strategic Pivot (updated post 2026-04 refactor)
description: PCS Arrow-native engine; April 2026 refactor reversed Pipeline/Scheduler split; Phase 7 docs complete
type: project
---

PCS (was Canudo) is a distributed batch processing engine over Apache Arrow RecordBatch.

**April 2026 refactor — vocabulary reversed:**
- `Dataset` = Arrow-backed columnar data container (was `Pipeline`)
- `Pipeline` = self-contained workload: systems + Dataset + DAG + IO (was `Scheduler`)
- `Scheduler` = multi-pipeline orchestrator (new type)

**Why:** User wanted Scheduler to orchestrate multiple Pipelines; service-layer YAML `pipeline:` key already meant "workload", so Rust types were aligned. This reversed the previous counter-intuitive split.

**Phase 7 doc tasks completed (2026-04):**
- CLAUDE.md Architecture section rewritten with new vocabulary
- `service-cluster` feature added to Feature Flags in CLAUDE.md
- README.md Simple Example updated: `System::run` now takes `&mut Dataset`, uses `Pipeline::builder`
- 37 intra-doc link warnings fixed to 0
- Memory file `naming-vocabulary.md` rewritten for new vocab

**Benchmark results** (Apple Silicon, target-cpu=native, 1M rows) — authoritative numbers:
- IPC decode: **11.1×** faster than postcard — cite for recovery speed
- IPC encode: **1.1×** faster — do not cite as a win
- Wide-schema filter+project (4/30 cols): **3.1×** faster — cite for wide-schema
- Q6 narrow: 5.2× SLOWER than scalar — do NOT claim win
- Q1: 2.6× SLOWER than scalar — do NOT claim win
- SHA-256 parallelism: **5.6×** on 12 CPUs — cite for parallel scaling
- DataFusion Q6: 2.5× SLOWER — do NOT claim to beat DataFusion on SQL

**Positioning:**
- DO claim: IPC checkpoint decode 11.1×, wide-schema 3.1×, parallelism 5.6×
- Framing: "optimized for serialization, wide-schema, and distributed recovery"

**Current type names (critical):**
- `Dataset` — data container
- `Pipeline` — workload (systems + DAG + IO)
- `Scheduler` — multi-pipeline orchestrator
- `System::run` signature: `async fn run(&self, data: &mut Dataset)`
- `Pipeline::builder("name")` — builder takes a name string
