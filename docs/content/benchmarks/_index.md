+++
title = "Benchmarks"
description = "Where columnar processing pays off and where it does not: batch versus stream item sizing, stage dispatch, TPC-H Q1 and Q6, slice parallelism, Arrow IPC versus postcard, and a DataFusion comparison."
template = "section.html"
sort_by = "title"

[extra]
kicker = "Reference"
+++

Every performance claim on this site should be traceable to a run you can
reproduce. These are those runs, reported whole — including the ones where PCS
loses.

The short version: **PCS wins when the schema is wide, when work arrives one
item at a time, and when checkpoints are being decoded. It loses on
narrow-schema single-pass bulk work, and it loses to a real query engine at
SQL.** If your workload looks like the second group, the honest recommendation
is to use something else.

## How these were produced

```bash
# Batch versus stream, and the stage-dispatch threshold
cargo bench -p pcs-core --features io --bench batch_vs_stream

# Columnar pipeline benchmarks
cargo bench -p pcs-core --features io --bench tpch_q1
cargo bench -p pcs-core --features io --bench tpch_q6
cargo bench -p pcs-core --features io --bench parallelism_compute
cargo bench -p pcs-core --features io --bench ipc_checkpoint

# SQL comparison — needs the datafusion feature
cargo bench -p pcs-service --features datafusion --bench vs_datafusion_q6

# Service-level per-item latency, native and WASM
cargo run --release -p pcs-service --features service,wasm --example stream_latency
```

Recorded on **2026-08-22**, on an **AMD Ryzen 9 9950X3D** (16 cores, 32 logical
CPUs, dual-CCD, DDR5) running **Windows 11**, Rust 1.98.0, built with
`RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1"`. Criterion
sample size 10 (20 for `item_size`), 1 000 000 rows and `seed=42` unless stated
otherwise.

**These numbers assume mimalloc**, which the `pcs-service` binary and every
benchmark binary install as the global allocator. A pipeline allocates and frees
its output Arrow arrays once per batch and those are routinely multi-megabyte;
Windows' heap sends allocations above ~512 KB straight to `VirtualAlloc` and
returns the pages on free, so the next batch soft-faults all of them again.
Switching allocator was worth 2.3–2.6× on its own. The library never installs an
allocator, so if you embed `pcs-core` this is your choice to make — and if you
leave it to the system allocator on Windows, expect the pipeline figures below to
roughly double. See `performance-improvement.md` in the repository root for the
full measurement log.

`FilterStage`, `ComputeStage`, `AggregateStage`, `RevenueStage` and `TaxStage`
are systems defined inside the benchmark files themselves, not library types.

## Batch versus stream: what item size costs

The two processing modes differ in exactly one way: batch mode makes one
pipeline invocation over N rows, stream mode makes N/k invocations over k rows
each. The systems, the DAG and the stage plan are identical. This benchmark
holds the total at 100 000 rows of Q6-shaped work — a filter-and-compute
`ParallelSystem` plus a sequential accumulator — and sweeps k, all through
`Pipeline::run_stream`, so item size is the only variable. Source:
`crates/pcs-core/benches/batch_vs_stream.rs`.

| Item size | Invocations | Total | Per item | Versus one batch |
|---|---|---|---|---|
| 100 000 (one batch) | 1 | 255.2 µs | 255.2 µs | 1.0× |
| 10 000 | 10 | 263.0 µs | 26.3 µs | 1.03× |
| 1 000 | 100 | 346.0 µs | 3.46 µs | 1.36× |
| 100 | 1 000 | 1.148 ms | 1.15 µs | 4.5× |
| 10 | 10 000 | 8.832 ms | 0.883 µs | 34.6× |
| 1 | 100 000 | 88.26 ms | **0.883 µs** | 346× |

Read the per-item column: from k=1 to k=10 it is flat at 0.883 µs, and it is
still only 1.15 µs at k=100. That is the fixed cost of an invocation — clear the
dataset, append, walk the stages, apply the write set, update stats — and below
about a hundred rows it is *all* you are paying, because the row work vanishes
into it.

So the tradeoff is explicit: **processing 100 000 rows one at a time costs 346×
the wall time of processing them in a single batch.** You buy latency with
throughput. A pipeline handed 100 000 rows should never run in stream mode; a
pipeline that must answer one item immediately has nothing to amortise anyway.

The floor is measurable on its own. An empty-DAG pipeline — one system that does
nothing — over 10 000 single-row items runs in 2.835 ms, i.e. **0.284 µs per
item** of pure framework overhead. Of the 0.883 µs a real item costs, roughly
0.28 µs is the runner and 0.6 µs is two systems doing Arrow array construction
and write-set application on a single row. Arrow's per-array fixed costs, not
the runner, are what make a one-row item cost anything at all.

For scale, a scalar row loop over the same 100 000 rows takes 173.2 µs, and
`Pipeline::run` — the batch entry point, no IO — takes 258.9 µs, which is within
1.5% of the equivalent single-item `run_stream` measurement of 255.2 µs, as it
should be.

### Service-level latency

The library numbers above exclude the service runner, the source and the sink.
The `stream_latency` example measures the whole path a real deployment takes,
timed from the producer: send one single-row batch, wait for the transformed row
to arrive at the sink.

| Path | n | mean | p50 | p99 | max |
|---|---|---|---|---|---|
| native, source → systems → sink | 10 000 | 2.0 µs | 2 µs | **3 µs** | 29 µs |
| WASM guest, `run_on_with_state` | 1 000 | 253.4 µs | 249 µs | **380 µs** | 544 µs |

Native stream mode is a **3 µs p99** round trip end to end. The WASM boundary
costs two orders of magnitude more: a fresh wasmtime `Store` per call, plus
Arrow IPC in and out. Linking and instantiation planning are hoisted to load
time via `InstancePre`, so what remains is store creation, instantiation, and
the IPC round trip — and the IPC section below shows that half is substantial at
one row.

## Stage dispatch: when concurrency starts paying

A stage holding several non-conflicting `ParallelSystem`s can either run them
inline, one after another, or dispatch one `spawn_blocking` each. PCS gates that
choice on row count with `STAGE_INLINE_THRESHOLD`. The same two-system stage,
measured both ways at identical row counts:

| Rows | Inline | Dispatched | Winner |
|---|---|---|---|
| 1 024 | 12.01 µs | 41.55 µs | inline **3.5×** |
| 4 096 | 22.67 µs | 60.06 µs | inline **2.6×** |
| 16 384 | 95.31 µs | 165.7 µs | inline **1.7×** |
| 65 536 | 389.1 µs | 466.0 µs | inline **1.20×** |
| 131 072 | 2.265 ms | 2.165 ms | dispatched 1.05× |
| 262 144 | 4.540 ms | 4.055 ms | dispatched 1.12× |
| 1 048 576 | 17.47 ms | 15.61 ms | dispatched 1.12× |

Dispatch costs roughly 30 µs of fixed overhead for a two-system stage and does
not earn it back until somewhere between 65 536 and 131 072 rows. Even at a
million rows it only wins by 12%. `STAGE_INLINE_THRESHOLD` is set to 100 000,
inside that bracket.

## TPC-H Q1 — aggregation

Aggregation over a 12-column lineitem batch with `GROUP BY (returnflag,
linestatus)`. Source: `crates/pcs-core/benches/tpch_q1.rs`.

| Workload | Time | Versus scalar | |
|---|---|---|---|
| scalar baseline | 7.867 ms | 1.0× | single pass over `Vec` of row structs |
| PCS pipeline | 11.86 ms | **1.51× slower** | includes pipeline construction |

Q1 is the wrong shape for columnar processing: the `GROUP BY` has low
cardinality, every column is touched, and a scalar loop over a `HashMap` is
already close to optimal. PCS is within striking distance rather than ahead, and
that is the honest expectation for Q1-shaped work.

## TPC-H Q6 — filter and sum, narrow versus wide

Sum of revenue behind three compound predicates. Run twice: once on the
12-column schema, once on a 30-column schema where 18 columns are never read.
Source: `crates/pcs-core/benches/tpch_q6.rs`.

| Workload | Time | Versus narrow scalar | |
|---|---|---|---|
| narrow, scalar | 2.116 ms | 1.0× | 12 columns, all touched |
| narrow, PCS | 4.233 ms | **2.00× slower** | |
| wide, scalar | 12.75 ms | 6.0× slower | touches all 30 columns per row |
| wide, PCS | 4.228 ms | **3.02× faster than wide scalar** | reads 4 columns of 30 |

This is the clearest result on the page, because the only thing that changed
between the two halves is schema width.

The scalar baseline degrades with column count: 2.116 ms → 12.75 ms for 2.5× the
columns, because a row-oriented pass pulls every field into cache whether it is
read or not. PCS goes 4.233 ms → 4.228 ms over the same change — flat — because
it reads the four columns the predicates name and never touches the other 26.

**The crossover is schema width, not row count.** On a narrow schema PCS pays
for materialising intermediate columns and wins nothing. By 30 columns it is 3×
ahead, and the advantage grows from there.

Decomposition, for anyone wanting to know where the 4.233 ms goes: the three
stage bodies measure 2.536 ms (filter), 0.777 ms (compute) and 1.054 ms
(aggregate), summing to 4.367 ms — which matches
`narrow_direct_systems_warm` at 4.347 ms and the full pipeline at 4.233 ms.
Framework overhead is therefore at or below measurement noise; what remains is
the cost of doing three passes and materialising two intermediate columns where
the scalar loop does one fused pass.

## Slice parallelism

SHA3-256 over 128-byte blobs — genuinely CPU-bound. 1M rows, 128 MB of input, 32
logical CPUs. Source: `crates/pcs-core/benches/parallelism_compute.rs`.

| Configuration | Time | Speedup | |
|---|---|---|---|
| sequential | 398.3 ms | 1.0× | plain `System`, one thread |
| slice-parallel | 40.66 ms | **9.80×** | `ParallelSystem` with `run_slice` |
| threshold raised above row count | 397.8 ms | ~1.0× | confirms the fallback path |

9.80× on 32 logical CPUs — 16 physical — is 30.6% efficiency against logical
count, or 61% against physical. SHA3-256 has internal data dependencies and SMT
siblings do not double throughput, so linear was never available.

Two things got this from an earlier 4.66×: chunking at 4 chunks per CPU instead
of one, which gives rayon something to steal when SMT siblings and the two CCDs
retire chunks at different rates; and making the merged-batch cache a read-mostly
lock, since `run_slice` calls `batch_for` once per chunk and an exclusive lock
there serialised the entire fan-out.

The third row is the one worth noting for correctness rather than speed. With the
slice threshold raised above the row count, the executor falls back to the
whole-dataset path and the time returns to sequential — so the threshold gate
does what it claims.

## Arrow IPC versus postcard

Checkpoint encode and decode across 10 columns — three `i64`, three `f64`, two
strings with 100 distinct values, one `bool`, one `Option<f64>`. Source:
`crates/pcs-core/benches/ipc_checkpoint.rs`. The 1-row and 1 000-row sizes
matter because stream mode round-trips one checkpoint *per item*.

| Rows | IPC encode | postcard encode | IPC decode | postcard decode |
|---|---|---|---|---|
| 1 | 4.503 µs | **72.6 ns** | 5.385 µs | **39.9 ns** |
| 1 000 | **7.224 µs** | 32.90 µs | **11.94 µs** | 41.49 µs |
| 10 000 | **226.8 µs** | 507.0 µs | **101.5 µs** | 1.212 ms |
| 100 000 | **3.979 ms** | 7.589 ms | **2.811 ms** | 13.60 ms |
| 1 000 000 | **39.39 ms** | 119.98 ms | **27.90 ms** | 175.59 ms |

**At one row, postcard is 62× faster to encode and 135× faster to decode.**
Arrow IPC writes a schema header, per-column framing and an alive bitmap; that
fixed cost is about 3 KB and 4.5 µs regardless of payload, so at one row it is
the entire measurement. postcard writes a handful of bytes.

The crossover is early — by 1 000 rows IPC already wins in both directions — and
from there the usual asymmetry takes over. At a million rows IPC decodes 6.3×
faster, because decoding Arrow IPC is close to pointing at contiguous buffers
while decoding postcard means walking a stream and rebuilding every value.
Decode is the figure that matters for recovery: a node that dies mid-batch
re-reads its last checkpoint before it can do any useful work.

The practical consequence for stream mode: a WASM guest that returns a
checkpoint pays about 10 µs of IPC round trip per item on top of everything
else, which is why the WASM p99 above sits at 380 µs while the native path is at
3 µs. A guest that carries no state should return `None` rather than an empty
dataset.

## Against DataFusion

The same Q6 revenue sum, narrow schema, run through DataFusion 55 as SQL over a
`MemTable`. Source: `crates/pcs-service/benches/vs_datafusion_q6.rs`.

| System | Time | |
|---|---|---|
| DataFusion, SQL | 1.282 ms | `SELECT SUM(...)` on a `MemTable` |
| PCS pipeline | 4.213 ms | **3.29× slower** |

**PCS is slower and should be.** DataFusion is a mature vectorised query engine
with a cost-based optimiser and compiled expression evaluation, built for
exactly this. PCS has no query planner and no optimiser; it runs the imperative
Rust you wrote, in an order it derived from your field declarations.

If your transform is expressible as SQL, DataFusion will beat PCS at it, and
that is the right reason to choose DataFusion. PCS exists for the transforms
that SQL expresses awkwardly, for per-item stream processing, and for running
either across a cluster with checkpointing — none of which this benchmark
measures. Note also that this is the *narrow* schema, which is Q6's worst case
for PCS; the wide-schema comparison would look different.

## Summary

| Benchmark | Baseline | PCS | Result |
|---|---|---|---|
| Stream mode, native p99 | — | 3 µs per item | **single-digit µs** |
| Stream mode, framework floor | — | 0.284 µs per item | **sub-microsecond** |
| Q6, 30-column schema | 12.75 ms row-scan | 4.228 ms | **3.02× faster** |
| Checkpoint decode, 1M rows | 175.6 ms postcard | 27.90 ms | **6.3× faster** |
| Checkpoint encode, 1M rows | 120.0 ms postcard | 39.39 ms | **3.0× faster** |
| Stage inline vs dispatch, 1k rows | 41.55 µs dispatched | 12.01 µs inline | **3.5× faster** |
| Slice parallelism | 398.3 ms sequential | 40.66 ms | **9.80× on 32 CPUs** |
| Q6, column-width scaling | 6.0× cost for 30 cols | flat | **projection works** |
| Q1, aggregation | 7.867 ms scalar | 11.86 ms | 1.51× slower |
| Q6, 12-column schema | 2.116 ms scalar | 4.233 ms | 2.00× slower |
| Q6 as SQL | 1.282 ms DataFusion | 4.213 ms | 3.29× slower |
| Checkpoint decode, 1 row | 39.9 ns postcard | 5.385 µs | 135× slower |
| Stream vs batch, 100k rows | 255.2 µs one batch | 88.26 ms at k=1 | 346× more wall time |

Read top to bottom, that is a fair summary of the engine: wide schemas, per-item
latency and checkpoint recovery are where the design pays for itself; narrow
single-pass bulk work is where materialising intermediate columns costs more than
it saves; and a query engine still wins at queries.

## What changed, and what is still open

An earlier version of this page reported figures roughly 3–6× worse for every
pipeline benchmark, and recorded the wide-schema Q6 result as failing to
reproduce. That was real, and it was our bug, not the hardware's. Two causes,
both now fixed:

- **Every component was concatenated on its first read.** Registering or
  clearing a component seeded it with a zero-row sentinel batch, and appending
  pushed onto that instead of replacing it — leaving two chunks, which made the
  next read run `concat_batches` over the whole component. For Q6 that was a
  ~92 MB copy per iteration.
- **The allocator.** See the note under "How these were produced".

Still open:

- **Q6 is bimodal on this machine.** Repeated runs of identical code land at
  either ~4.2 ms or ~4.9 ms and stay there for the run. The dual-CCD topology is
  the likely cause — one CCD has 3D V-cache and one does not — but it has not
  been confirmed with thread affinity. Treat sub-10% Q6 deltas with suspicion.
- **Slice parallelism past ~16 threads.** 9.80× on 32 logical CPUs is a large
  improvement on 4.66× but still short of the ~16× that 16 physical cores
  suggest. `compute_row_ranges` splits uniformly and knows nothing about which
  CCD a worker sits on.

Measured and closed: the per-system tracing span costs **0.65%** of
`stream_items_of_1` when `pcs-core/tracing` is compiled in without an active
subscriber, because `tracing` checks a global level filter before doing any work.
A build with a subscriber attached pays for what it asked to record, which is an
operator choice rather than framework overhead.
