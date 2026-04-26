---
name: Phase 7 Benchmark Suite
description: Phase 7 delivered TPC-H Q1/Q6, parallelism compute, IPC checkpoint, and DataFusion comparison benchmarks with actual numbers
type: project
---

Phase 7 benchmark suite completed (2026-04-11). All 5 benchmarks compile, pass correctness checks, and produce honest numbers.

**Files created**:
- benches/tpch_q1.rs (764 lines) — Q1 aggregation pipeline
- benches/tpch_q6.rs (731 lines) — Q6 filter+sum, narrow+wide schema
- benches/parallelism_compute.rs (406 lines) — SHA-256 parallelism validation
- benches/ipc_checkpoint.rs (284 lines) — ArrowWorld IPC vs postcard
- benches/vs_datafusion_q6.rs (478 lines) — DataFusion comparison
- docs/benchmarks/phase7-results.md — results table and recommendations

**Key numbers (1M rows, 12 CPUs, native release)**:
- Q1: scalar=9.4ms, cano=24.3ms (2.6× slower — misses 1.5× gate, honest)
- Q6 wide: scalar=26.2ms, cano=8.6ms (**3.1× faster** — beats ≥2× target)
- SHA-256: seq=556ms, parallel=99ms (**5.6× speedup** on 12 CPUs)
- IPC encode: arrow=46ms, postcard=49ms (1.1× — Phase 1's 4.38× not reproduced at ArrowWorld level)
- IPC decode: arrow=10.5ms, postcard=117ms (**11.1×** — exceeds ≥10× floor)
- DataFusion Q6: cano=6.0ms, df=2.4ms (Canudo 2.5× slower — expected)

**Why:** Phase 4 sha2 dev-dep is 0.10 (not 0.11 — sha2 0.11 wasn't available, 0.10 was).  
DataFusion 53.0.0 requires features=["sql","parquet"] (not default-features=false with just datafusion-sql feature name).

**How to apply:** Phase 8 README should claim wide-schema win (3.1×), IPC decode (11.1×), parallelism (5.6×). Do NOT claim Q1 win or DataFusion SQL win.
