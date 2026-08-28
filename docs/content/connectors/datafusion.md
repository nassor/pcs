+++
title = "DataFusion SQL"
description = "A DataFusion query as a Source, streaming lazily so the filter runs in DataFusion."
template = "subpage.html"
weight = 7

[[extra.facts]]
label = "Crate"
value = "<code>pcs-connector-datafusion</code>"
[[extra.facts]]
label = "Feature"
value = "None: there is no host feature for it"
[[extra.facts]]
label = "Config type"
value = "None: it has no factory"
[[extra.facts]]
label = "Transformer key"
value = "None: rows arrive as Arrow batches"
+++

## What it does

A DataFusion query becomes a `Source`, streaming lazily so a filter or aggregation runs in
DataFusion and only the result crosses into the `Dataset`.

There is no adapter in the other direction: a `Dataset` is not exposed as a `TableProvider`.

## In Rust

<div class="code">
<div class="code-cap"><span>Rust</span><em>register_raw_component pairs well: the projected schema is known only after planning</em></div>

```rust
use pcs_connector_datafusion::DataFusionSource;

DataFusionSource::from_sql(&SessionContext, &str).await  -> Result<Self>
DataFusionSource::from_stream(SendableRecordBatchStream) -> Self
source.with_estimated_rows(rows: usize)                  -> Self
```

</div>

`with_estimated_rows` is the only way `estimated_rows` returns `Some`. Neither constructor guesses a
row count, because a lazy stream has none until it is drained.

## No factory, no feature

<div class="note">
<span class="note-label">Constraint</span>
<p>
This connector has <b>no factory and no <code>pcs-service</code> feature</b>. It needs a live
<code>SessionContext</code> that the caller owns, which service config cannot express, so there is
no <code>type</code> string for it. The crate does not even depend on <code>pcs-connector</code>,
so it cannot implement <code>SourceFactory</code>.
</p>
</div>

Wiring is Rust, not config: build the source, drain it into a `Dataset` with `drain_into_dataset`,
and run a `Pipeline` over that dataset.

<div class="code">
<div class="code-cap"><span>Rust</span><em>the shape of examples/datafusion_interop.rs</em></div>

```rust
let mut src = DataFusionSource::from_sql(&ctx, sql).await?;
let src_schema = src.schema();

let mut dataset = Dataset::new();
dataset.register_raw_component("sales", src_schema)?;
drain_into_dataset(&mut src, &mut dataset, "sales").await?;

let mut pipeline = Pipeline::new("datafusion");
*pipeline.data_mut() = dataset;
pipeline.add_system(ComputeRevenue);
pipeline.run().await?;
```

</div>

## Worked example

`cargo run -p pcs-connector-datafusion --example datafusion_interop` runs the code above against an
in-memory table. The TPC-H Q6 comparison bench is `vs_datafusion_q6`, run through
`cargo xtask bench vs_datafusion_q6`.
