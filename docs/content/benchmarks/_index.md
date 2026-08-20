+++
title = "Benchmarks"
description = "Where columnar processing pays off and where it does not: TPC-H Q1 and Q6, slice parallelism, Arrow IPC versus postcard, and a DataFusion comparison."
template = "section.html"
sort_by = "title"

[extra]
kicker = "Reference"
+++

Every performance claim on this site should be traceable to a run you can
reproduce. These are those runs, reported whole — including the two where PCS
loses, and the one where it loses badly.

The short version: **PCS wins when the schema is wide and when checkpoints are
being decoded. It loses on narrow-schema single-pass work, and it loses to a
real query engine at SQL.** If your workload looks like the second group, the
honest recommendation is to use something else.

## How these were produced

```bash
# Columnar pipeline benchmarks
cargo bench -p pcs-core --bench tpch_q1
cargo bench -p pcs-core --bench tpch_q6
cargo bench -p pcs-core --bench parallelism_compute
cargo bench -p pcs-core --bench ipc_checkpoint

# SQL comparison — needs the datafusion feature
cargo bench -p pcs-service --features datafusion --bench vs_datafusion_q6
```

The figures below were recorded on **2026-04-11**, on an Apple M-series machine
with 12 logical CPUs, built with
`RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1"`. Criterion
sample size 10 with full warm-up, 1 000 000 rows, `seed=42` throughout. Absolute
times will differ on your hardware; the ratios are the durable part.

`FilterStage`, `ComputeStage` and `AggregateStage` below are systems defined
inside the benchmark files themselves, not library types — the first two
implement `ParallelSystem`, the third `System`.

## TPC-H Q1 — aggregation

Aggregation over a 12-column lineitem batch with `GROUP BY (returnflag,
linestatus)`. Source: `crates/pcs-core/benches/tpch_q1.rs`.

| Workload | Time | Versus scalar | |
|---|---|---|---|
| scalar baseline | 9.4 ms | 1.0× | single pass over `Vec` of row structs |
| PCS pipeline | 24.3 ms | **2.6× slower** | includes pipeline construction |

**PCS loses here, and the reason is not the aggregation.** Most of the delta is
fixed pipeline setup — construction plus a `RecordBatch` clone — which does not
amortise at 1M rows. The stage logic itself is fast.

Q1 is also the wrong shape for columnar processing: the `GROUP BY` has low
cardinality, every column is touched, and a scalar loop over a `HashMap` is
already close to optimal. Do not expect PCS to be fast at Q1-shaped work.

## TPC-H Q6 — filter and sum, narrow versus wide

Sum of revenue behind three compound predicates. Run twice: once on the
12-column schema, once on a 30-column schema where 18 columns are never read.
Source: `crates/pcs-core/benches/tpch_q6.rs`.

| Workload | Time | Versus narrow scalar | |
|---|---|---|---|
| narrow, scalar | 1.3 ms | 1.0× | 12 columns, all touched |
| narrow, PCS | 6.7 ms | **5.2× slower** | setup cost dominates again |
| wide, scalar | 26.2 ms | 20× slower | touches all 30 columns per row |
| wide, PCS | 8.6 ms | **3.1× faster than wide scalar** | reads 4 columns of 30 |

This pair is the clearest result on the page, because the only thing that
changed between the two halves is schema width.

The scalar baseline degrades linearly with column count: 1.3 ms → 26.2 ms for
2.5× the columns, because a row-oriented pass pulls every field into cache
whether it is read or not. PCS goes 6.7 ms → 8.6 ms over the same change,
because it reads the four columns the predicates name and never touches the
other 26.

**The crossover is schema width, not row count.** On a narrow schema PCS is
paying setup cost for nothing. Somewhere between 12 and 30 columns it starts
winning, and the advantage grows from there.

## Slice parallelism

SHA-256 over 128-byte blobs — genuinely CPU-bound, unlike the arithmetic
workload this benchmark originally used, which turned out to be memory-bound and
therefore measured nothing about parallelism. 1M rows, 128 MB of input, 12
logical CPUs. Source: `crates/pcs-core/benches/parallelism_compute.rs`.

| Configuration | Time | Speedup | |
|---|---|---|---|
| sequential | 556 ms | 1.0× | plain `System`, one thread |
| slice-parallel | 99 ms | **5.6×** | `ParallelSystem` with `run_slice` |
| threshold raised above row count | 548 ms | ~1.0× | confirms the fallback path |

5.6× on 12 CPUs is about 47% parallel efficiency. That is short of linear and
will stay short of linear: SHA-256 has internal data dependencies, and 128 MB of
input means the memory bus is contended as well as the CPUs.

The third row is the one worth noting for correctness rather than speed. With
the slice threshold raised above the row count, the executor falls back to the
whole-dataset path and the time returns to sequential — so the threshold gate
does what it claims, and small batches are not paying for thread coordination
they cannot recover.

## Arrow IPC versus postcard

Checkpoint encode and decode for 1M rows across 10 columns — three `i64`, three
`f64`, two strings with 100 distinct values, one `bool`, one `Option<f64>`.
91.6 MB as Arrow IPC, 89.7 MB as postcard. Source:
`crates/pcs-core/benches/ipc_checkpoint.rs`.

| Operation | Arrow IPC | postcard | |
|---|---|---|---|
| encode | 46 ms | 49 ms | **1.1× faster** — near parity |
| decode | 10.5 ms | 117 ms | **11.1× faster** |

Decode is the number that matters, because decode is what a crash costs you. A
node that dies mid-batch re-reads its last checkpoint before it can do any
useful work, so recovery time is dominated by this figure.

The asymmetry is structural. Decoding Arrow IPC is close to pointing at
contiguous buffers; decoding postcard means walking a stream and rebuilding
every value. Encoding has no such advantage — PCS writes per-component IPC
streams in sorted order plus an alive bitmap, and that framing costs about as
much as postcard's simpler layout saves. **Encode parity is the honest
description; only decode is a large win.**

## Against DataFusion

The same Q6 revenue sum, narrow schema, run through DataFusion 55 as SQL over a
`MemTable`. Source: `crates/pcs-service/benches/vs_datafusion_q6.rs`.

| System | Time | |
|---|---|---|
| DataFusion, SQL | 2.4 ms | `SELECT SUM(...)` on a `MemTable` |
| PCS pipeline | 6.0 ms | **2.5× slower** |

**PCS is slower and should be.** DataFusion is a mature vectorised query engine
with a cost-based optimiser and compiled expression evaluation, built for
exactly this. PCS has no query planner and no optimiser; it runs the imperative
Rust you wrote, in an order it derived from your field declarations.

If your transform is expressible as SQL, DataFusion will almost certainly beat
PCS at it, and that is the right reason to choose DataFusion. PCS exists for the
transforms that SQL expresses awkwardly, and for running them across a cluster
with checkpointing — neither of which this benchmark measures.

## Summary

| Benchmark | Baseline | PCS | Result |
|---|---|---|---|
| Q6, 30-column schema | 26.2 ms row-scan | 8.6 ms | **3.1× faster** |
| Checkpoint decode | 117 ms postcard | 10.5 ms | **11.1× faster** |
| Slice parallelism | 556 ms sequential | 99 ms | **5.6× on 12 CPUs** |
| Checkpoint encode | 49 ms postcard | 46 ms | 1.1× faster — parity |
| Q1, aggregation | 9.4 ms scalar | 24.3 ms | 2.6× slower |
| Q6, 12-column schema | 1.3 ms scalar | 6.7 ms | 5.2× slower |
| Q6 as SQL | 2.4 ms DataFusion | 6.0 ms | 2.5× slower |

Read top to bottom, that is a fair summary of the engine: wide schemas and
checkpoint recovery are where the columnar layout pays for itself, and
narrow-schema single-pass work is where the fixed cost of building a pipeline
has nothing to amortise against.
