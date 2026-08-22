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
loses, and there are more of those than there used to be.

The short version: **stream mode delivers single-digit-microsecond per-item
latency, and Arrow IPC decode still dominates postcard at checkpoint sizes.
Everything else on this page is a loss on this hardware — including the
wide-schema case that previously was the headline win.** If your workload looks
like the losing group, the honest recommendation is to use something else.

## These figures replace the 2026-04-11 run

> The previous version of this page reported an Apple M-series / 12-CPU run
> from 2026-04-11 in which the wide-schema Q6 case was **3.1× faster** than a
> scalar row scan and Arrow IPC encode was at parity with postcard. Neither
> reproduces here. On this machine wide-schema Q6 is **3.6× slower** than the
> scalar scan, and IPC encode is **3.0× faster** than postcard rather than
> level.
>
> The regression is **not** caused by the stream-mode work that added
> `run_stream`, the TCP source, and the stage-inline gate: re-running `tpch_q6`
> on a pristine checkout with those changes stashed gives `wide_pcs` 51.4 ms
> against 49.2 ms with them, i.e. identical within noise. It is also not
> per-iteration dataset construction, which is measured below at 19–24 µs
> against a 25–49 ms total. The cause is unresolved and tracked as an open
> question at the bottom of this page.

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
sample size 10 with full warm-up (20 for the `item_size` group, whose per-
iteration dataset rebuild is allocator-sensitive enough that 10 samples left a
2× confidence interval), 1 000 000 rows and `seed=42` unless stated otherwise.
Absolute times will differ on your hardware; as the callout above shows, on this
page the *ratios* turned out not to be as durable as previously claimed either.

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
| 100 000 (one batch) | 1 | 463.5 µs | 463.5 µs | 1.0× |
| 10 000 | 10 | 508.3 µs | 50.8 µs | 1.1× |
| 1 000 | 100 | 963.8 µs | 9.64 µs | 2.1× |
| 100 | 1 000 | 5.839 ms | 5.84 µs | 12.6× |
| 10 | 10 000 | 55.53 ms | 5.55 µs | 120× |
| 1 | 100 000 | 573.6 ms | 5.74 µs | **1237×** |

Read the per-item column: from k=1 to k=100 it is flat at about 5.6–5.8 µs.
That is the fixed cost of an invocation — clear the dataset, append, walk the
stages, apply the write set, update stats — and below ~100 rows it is *all* you
are paying, because the row work is lost in it. Above k≈1000 the row work starts
to dominate and per-item cost climbs with k.

So the tradeoff is explicit and it is not subtle: **processing 100 000 rows one
at a time costs 1237× the wall time of processing them in a single batch.** You
buy latency with throughput. A pipeline that receives one item and must answer
immediately has nothing to amortise; a pipeline handed 100 000 rows should never
be run in stream mode.

The floor is measurable on its own. An empty-DAG pipeline — one system that
does nothing — over 10 000 single-row items runs in 14.79 ms, i.e. **1.48 µs
per item** of pure framework overhead. Of the 5.74 µs a real item costs, roughly
1.5 µs is the runner and 4.2 µs is two systems doing Arrow array construction
and write-set application on a single row. Arrow's per-array fixed costs, not
the runner, are what make a one-row item expensive.

For scale, a scalar row loop over the same 100 000 rows takes 190.8 µs, and
`Pipeline::run` (the batch entry point, no IO) takes 748.2 µs. That
`Pipeline::run` figure is 1.6× the equivalent single-item `run_stream`
measurement despite doing the same work through the same stage executor; the
discrepancy is reproducible but unexplained, so it is reported here rather than
folded into the table above.

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
the IPC round trip — and the next section shows the IPC half of that is
substantial at one row.

## Stage dispatch: when concurrency starts paying

A stage holding several non-conflicting `ParallelSystem`s can either run them
inline, one after another, or dispatch one `spawn_blocking` each. PCS gates that
choice on row count with `STAGE_INLINE_THRESHOLD`. To find where the crossover
actually sits, the same two-system stage was measured both ways at identical row
counts, by building once with the gate in place and once with it disabled.

| Rows | Inline | Dispatched | Winner |
|---|---|---|---|
| 1 024 | 12.01 µs | 41.55 µs | inline **3.5×** |
| 4 096 | 22.67 µs | 60.06 µs | inline **2.6×** |
| 16 384 | 95.31 µs | 165.7 µs | inline **1.7×** |
| 65 536 | 389.1 µs | 466.0 µs | inline **1.20×** |
| 131 072 | 2.265 ms | 2.165 ms | dispatched 1.05× |
| 262 144 | 4.540 ms | 4.055 ms | dispatched 1.12× |
| 1 048 576 | 17.47 ms | 15.61 ms | dispatched 1.12× |

Dispatch costs roughly 30 µs of fixed overhead for a two-system stage, and does
not earn it back until somewhere between 65 536 and 131 072 rows. Even at a
million rows it only wins by 12%.

`STAGE_INLINE_THRESHOLD` was originally set to 1 024 on the reasoning that
thread dispatch dominates at small sizes. That was directionally right and
numerically wrong by two orders of magnitude: at 1 024 rows inline is 3.5×
faster, so the gate was handing work to `spawn_blocking` across the entire range
where inline wins. It is now **100 000**, inside the measured bracket and the
same order as `SLICE_PARALLEL_THRESHOLD`.

## TPC-H Q1 — aggregation

Aggregation over a 12-column lineitem batch with `GROUP BY (returnflag,
linestatus)`. Source: `crates/pcs-core/benches/tpch_q1.rs`.

| Workload | Time | Versus scalar | |
|---|---|---|---|
| scalar baseline | 8.08 ms | 1.0× | single pass over `Vec` of row structs |
| PCS pipeline | 34.59 ms | **4.3× slower** | includes pipeline construction |

Q1 is the wrong shape for columnar processing: the `GROUP BY` has low
cardinality, every column is touched, and a scalar loop over a `HashMap` is
already close to optimal. Do not expect PCS to be fast at Q1-shaped work.

## TPC-H Q6 — filter and sum, narrow versus wide

Sum of revenue behind three compound predicates. Run twice: once on the
12-column schema, once on a 30-column schema where 18 columns are never read.
Source: `crates/pcs-core/benches/tpch_q6.rs`.

| Workload | Time | Versus narrow scalar | |
|---|---|---|---|
| narrow, scalar | 2.07 ms | 1.0× | 12 columns, all touched |
| narrow, PCS | 25.50 ms | **12.3× slower** | |
| wide, scalar | 13.54 ms | 6.5× slower | touches all 30 columns per row |
| wide, PCS | 49.19 ms | **3.6× slower than wide scalar** | reads 4 columns of 30 |

**This is the result that reversed.** The column-projection advantage is still
visible in the *shape* of the numbers — going from 12 to 30 columns costs the
scalar scan 6.5× while costing PCS only 1.9×, exactly because PCS reads the four
columns the predicates name and ignores the other 26. But PCS starts from so far
behind on this hardware that narrowing the gap 3.4× is not enough to close it.
The crossover that used to happen between 12 and 30 columns now does not happen
by 30.

Two candidate explanations are ruled out by measurement. Per-iteration dataset
construction — which allocates and zero-fills a fresh 1M-row Revenue column
every iteration, something the scalar baselines never do — costs 18.97 µs
(narrow) and 24.34 µs (wide), i.e. under 0.1% of the total. And the stream-mode
changes are not responsible: on a pristine checkout the same benchmark reports
narrow 25.61 ms and wide 51.38 ms.

## Slice parallelism

SHA3-256 over 128-byte blobs — genuinely CPU-bound. 1M rows, 128 MB of input, 32
logical CPUs. Source: `crates/pcs-core/benches/parallelism_compute.rs`.

| Configuration | Time | Speedup | |
|---|---|---|---|
| sequential | 453.6 ms | 1.0× | plain `System`, one thread |
| slice-parallel | 97.29 ms | **4.66×** | `ParallelSystem` with `run_slice` |
| threshold raised above row count | 452.5 ms | ~1.0× | confirms the fallback path |

4.66× on 32 logical CPUs is **14.6% parallel efficiency**, against 47% reported
for the 12-CPU run. Single-thread throughput barely moved between the two
machines (453.6 ms here versus 556 ms), and the parallel time is essentially
identical (97.3 ms versus 99 ms) — so the extra 20 threads bought nothing. For a
workload with no shared state and no memory pressure at 128 MB, that is a poor
result and suggests the slice fan-out, not the hash, is the limit.

The third row is the one worth noting for correctness rather than speed. With
the slice threshold raised above the row count, the executor falls back to the
whole-dataset path and the time returns to sequential — so the threshold gate
does what it claims.

## Arrow IPC versus postcard

Checkpoint encode and decode across 10 columns — three `i64`, three `f64`, two
strings with 100 distinct values, one `bool`, one `Option<f64>`. Source:
`crates/pcs-core/benches/ipc_checkpoint.rs`. The 1-row and 1 000-row sizes are
new: stream mode round-trips one checkpoint *per item*, so the small end is now
a load-bearing number rather than a curiosity.

| Rows | IPC encode | postcard encode | IPC decode | postcard decode |
|---|---|---|---|---|
| 1 | 9.20 µs | **175 ns** | 7.77 µs | **91.6 ns** |
| 1 000 | **11.93 µs** | 32.93 µs | **15.16 µs** | 76.16 µs |
| 10 000 | **226.8 µs** | 507.0 µs | **101.5 µs** | 1.212 ms |
| 100 000 | **3.979 ms** | 7.589 ms | **2.811 ms** | 13.60 ms |
| 1 000 000 | **39.39 ms** | 119.98 ms | **27.90 ms** | 175.59 ms |

**At one row, postcard is 53× faster to encode and 85× faster to decode.**
Arrow IPC writes a schema header, per-column framing and an alive bitmap; that
fixed cost is about 3 KB and 9 µs regardless of payload, so at one row it is the
entire measurement. postcard writes a handful of bytes.

The crossover is early — by 1 000 rows IPC already wins on both directions —
and from there the usual asymmetry takes over. At a million rows IPC decodes
6.3× faster, because decoding Arrow IPC is close to pointing at contiguous
buffers while decoding postcard means walking a stream and rebuilding every
value. Decode is the figure that matters for recovery: a node that dies mid-batch
re-reads its last checkpoint before it can do any useful work.

Note that encode is no longer at parity as previously reported — IPC encode is
3.0× faster than postcard at a million rows here.

The practical consequence for stream mode: a WASM guest that returns a
checkpoint pays ~17 µs of IPC round trip per item on top of everything else,
which is why the WASM p99 above sits at 380 µs while the native path is at 3 µs.
A guest that carries no state, or one whose state is a few bytes, should return
`None` rather than an empty dataset.

## Against DataFusion

The same Q6 revenue sum, narrow schema, run through DataFusion 55 as SQL over a
`MemTable`. Source: `crates/pcs-service/benches/vs_datafusion_q6.rs`.

| System | Time | |
|---|---|---|
| DataFusion, SQL | 1.82 ms | `SELECT SUM(...)` on a `MemTable` |
| PCS pipeline | 26.36 ms | **14.5× slower** |

**PCS is slower and should be.** DataFusion is a mature vectorised query engine
with a cost-based optimiser and compiled expression evaluation, built for
exactly this. PCS has no query planner and no optimiser; it runs the imperative
Rust you wrote, in an order it derived from your field declarations.

If your transform is expressible as SQL, DataFusion will beat PCS at it, and
that is the right reason to choose DataFusion. PCS exists for the transforms
that SQL expresses awkwardly, for per-item stream processing, and for running
either across a cluster with checkpointing — none of which this benchmark
measures.

## Summary

| Benchmark | Baseline | PCS | Result |
|---|---|---|---|
| Stream mode, native p99 | — | 3 µs per item | **single-digit µs** |
| Checkpoint decode, 1M rows | 175.6 ms postcard | 27.9 ms | **6.3× faster** |
| Checkpoint encode, 1M rows | 120.0 ms postcard | 39.4 ms | **3.0× faster** |
| Stage inline vs dispatch, 1k rows | 41.6 µs dispatched | 12.0 µs inline | **3.5× faster** |
| Slice parallelism | 453.6 ms sequential | 97.3 ms | 4.66× on 32 CPUs |
| Q6, column projection effect | 6.5× cost for 30 cols | 1.9× cost | **3.4× better scaling** |
| Checkpoint decode, 1 row | 91.6 ns postcard | 7.77 µs | 85× slower |
| Q6, 30-column schema | 13.5 ms row-scan | 49.2 ms | 3.6× slower |
| Q1, aggregation | 8.08 ms scalar | 34.6 ms | 4.3× slower |
| Q6, 12-column schema | 2.07 ms scalar | 25.5 ms | 12.3× slower |
| Q6 as SQL | 1.82 ms DataFusion | 26.4 ms | 14.5× slower |
| Stream vs batch, 100k rows | 463.5 µs one batch | 573.6 ms at k=1 | 1237× more wall time |

Read top to bottom, that is a fair summary of the engine *as measured on this
machine*: stream mode's per-item latency and Arrow IPC's checkpoint decode are
real, durable wins, the column-projection effect is real but currently
insufficient to overcome a large constant, and single-pass bulk throughput is
losing to both a scalar loop and a query engine.

## Open questions

Three things on this page are unresolved and should be treated as bugs until
someone proves otherwise:

1. **The pipeline path is several times slower than the April run reported**,
   while the scalar and DataFusion baselines got faster. Verified not to be the
   stream-mode changes and not per-iteration dataset construction. Four months
   of dependency and code drift separate the two runs; nobody has bisected it.
2. **Slice parallelism scales badly past ~12 threads.** Going from 12 to 32
   logical CPUs left the parallel time unchanged at ~97 ms. The fan-out in
   `compute_row_ranges` splits into `num_cpus` chunks unconditionally, which on
   a dual-CCD part may be exactly the wrong shape.
3. **`Pipeline::run` is 1.6× slower than an equivalent single-item
   `run_stream`** on identical systems and identical input, with tight
   confidence intervals on both. Same stage executor, so the difference is
   somewhere in the entry path.
