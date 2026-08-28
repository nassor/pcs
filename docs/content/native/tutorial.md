+++
title = "Build your first pipeline"
description = "A complete native pipeline in nine steps: one component, four systems, one resource, and the stage plan PCS derives instead of you writing it."
template = "page.html"
weight = 1
aliases = ["/getting-started/"]
+++

# Build your first pipeline

<dl class="page-facts">
<dt>Time</dt>
<dd><strong>15 minutes</strong>, most of it the first <code>cargo build</code></dd>
<dt>You need</dt>
<dd>Rust <strong>1.95</strong> or newer, and a clone of the repository</dd>
<dt>Not needed</dt>
<dd>No database, no broker, no cluster, no WebAssembly toolchain</dd>
</dl>

Every piece of code below is quoted from
`examples/native/first_pipeline.rs`, which CI compiles. The last
step runs it and shows the real output.

## 1. Clone and build

<div class="note">
<span class="note-label">Before you start</span>

PCS crates are **not published to crates.io**. Every command here assumes you
have cloned the repository and are running from its root.

</div>

```bash,name=Clone the repository and build
git clone https://github.com/nassor/pcs
cd pcs
cargo build
```

## 2. Describe your data

A `Component` is a plain Rust struct plus the Arrow schema for its fields. PCS
stores one `RecordBatch` per component, so a field is a **column**, not a
per-row lookup.

```rust,name=The Order component and its Arrow schema
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Order {
    id: u64,
    currency: String,
    amount: f64,
    usd_amount: f64,
    express: bool,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("usd_amount", DataType::Float64, false),
            Field::new("express", DataType::Boolean, false),
        ]))
    }
}
```

`FieldRef` constants are how a system names a column without a string literal
at every call site. Declare one per field:

```rust,name=One FieldRef constant per field
impl Order {
    const ID: FieldRef<Order> = FieldRef::new("id");
    const CURRENCY: FieldRef<Order> = FieldRef::new("currency");
    const AMOUNT: FieldRef<Order> = FieldRef::new("amount");
    const USD_AMOUNT: FieldRef<Order> = FieldRef::new("usd_amount");
    const EXPRESS: FieldRef<Order> = FieldRef::new("express");
}
```

[Dataset & Components](@/dataset.md) covers registration, appends, soft deletes
and the IPC round trip.

## 3. Seed rows

A `System` is one transform plus the `meta()` that declares what it touches.
`SeedOrders` appends the batch every later stage reads.

```rust,name=SeedOrders appends the first batch
struct SeedOrders;

#[async_trait]
impl System for SeedOrders {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("seed").write_component("Order")
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), PcsError> {
        let orders = vec![
            Order {
                id: 1,
                currency: "EUR".into(),
                amount: 120.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 2,
                currency: "USD".into(),
                amount: 4300.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 3,
                currency: "GBP".into(),
                amount: 75.50,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 4,
                currency: "JPY".into(),
                amount: 900000.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 5,
                currency: "EUR".into(),
                amount: 12.00,
                usd_amount: 0.0,
                express: false,
            },
        ];

        let n = orders.len();
        data.append::<Order>(&orders)?;
        println!("seeded {n} orders");
        Ok(())
    }
}
```

`write_component("Order")` means "writes every field of `Order`". That is the
strongest declaration there is, so nothing can share a stage with it, and this
system lands alone in stage 0.

## 4. Write two transforms

Here is the payoff. `ConvertCurrency` writes `usd_amount` and `FlagExpress`
writes `express`. The writes are disjoint, so PCS puts both in **one stage** and
runs them concurrently. You never wrote a stage list.

Both implement `ParallelSystem`, which takes `&Dataset` and returns the columns
it produced as a `WriteSet` rather than mutating in place.

```rust,name=ConvertCurrency writes usd_amount
struct ConvertCurrency;

#[async_trait]
impl ParallelSystem for ConvertCurrency {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("convert")
            .reads(Order::AMOUNT)
            .reads(Order::CURRENCY)
            .writes(Order::USD_AMOUNT)
            .read_resource::<FxRates>()
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, PcsError> {
        let rates = data
            .get_resource::<FxRates>()
            .ok_or_else(|| PcsError::generic("FxRates resource not found"))?;

        let orders = data.view::<Order>()?;
        let amount = orders.f64(Order::AMOUNT)?;
        let currency = orders.str(Order::CURRENCY)?;
        let values: Vec<f64> = (0..orders.len())
            .map(|i| amount.value(i) * rates.rate_for(currency.value(i)))
            .collect();

        Ok(WriteSet::new().put("Order", "usd_amount", Arc::new(Float64Array::from(values))))
    }
}
```

```rust,name=FlagExpress writes express
struct FlagExpress;

#[async_trait]
impl ParallelSystem for FlagExpress {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("express")
            .reads(Order::AMOUNT)
            .writes(Order::EXPRESS)
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, PcsError> {
        let orders = data.view::<Order>()?;
        let amount = orders.f64(Order::AMOUNT)?;
        let flags: Vec<bool> = (0..orders.len())
            .map(|i| amount.value(i) > 1000.0)
            .collect();

        Ok(WriteSet::new().put("Order", "express", Arc::new(BooleanArray::from(flags))))
    }
}
```

Both read `amount`, and two reads never conflict. Add a third system writing
`amount` and the plan re-derives itself with no change to these two.
[Systems](@/systems.md) has the full conflict table.

## 5. Read a resource

A `Resource` is a Rust singleton on the `Dataset`, keyed by type. Use one for
configuration and lookup tables: values that are not columnar and do not need a
row per entry.

```rust,name=The FxRates resource
/// USD-base exchange rates, held as a resource rather than a column.
struct FxRates {
    eur: f64,
    gbp: f64,
    jpy: f64,
}

impl FxRates {
    fn rate_for(&self, currency: &str) -> f64 {
        match currency {
            "EUR" => self.eur,
            "GBP" => self.gbp,
            "JPY" => self.jpy,
            // "USD" is the reporting currency, and an unknown code converts
            // one to one rather than collapsing the row to zero.
            _ => 1.0,
        }
    }
}
```

Install it with `with_resource(...)` at build time and claim it with
`read_resource::<FxRates>()` in `meta()`, as `ConvertCurrency` does above.
Resources are **not** serialised by `write_ipc`, so they never cross an Arrow
IPC boundary. That is why a WebAssembly processor keeps configuration on the
system struct instead.

## 6. Summarise with a closure

A transform that does not need its own type can be a closure. `system_fn` pairs
one `SystemMeta` with one `FnMut(&mut Dataset)`. This one writes a second
resource for `main` to read:

```rust,name=The resource the summary writes
struct Summary {
    rows: usize,
    express: usize,
    total_usd: f64,
}
```

```rust,name=The summary system built with system_fn
fn make_summary_system() -> impl System {
    system_fn(
        SystemMeta::new("summary")
            .reads(Order::ID)
            .reads(Order::CURRENCY)
            .reads(Order::AMOUNT)
            .reads(Order::USD_AMOUNT)
            .reads(Order::EXPRESS)
            .write_resource::<Summary>(),
        |data| {
            let rows;
            let mut express = 0usize;
            let mut total_usd = 0.0f64;

            {
                let orders = data.view::<Order>()?;
                rows = orders.len();

                let id = orders.u64(Order::ID)?;
                let currency = orders.str(Order::CURRENCY)?;
                let amount = orders.f64(Order::AMOUNT)?;
                let usd = orders.f64(Order::USD_AMOUNT)?;
                let is_express = orders.bool(Order::EXPRESS)?;

                for i in 0..rows {
                    if is_express.value(i) {
                        express += 1;
                    }
                    total_usd += usd.value(i);
                    println!(
                        "order {}  {:>10.2} {} -> {:>10.2} USD  express={}",
                        id.value(i),
                        amount.value(i),
                        currency.value(i),
                        usd.value(i),
                        is_express.value(i)
                    );
                }
            }

            data.insert_resource(Summary {
                rows,
                express,
                total_usd,
            });

            Ok(())
        },
    )
}
```

The inner block matters: the column view borrows the dataset, so it has to drop
before `insert_resource` takes `&mut`.

## 7. Assemble and run

Register the component, install the resource, add the systems, run.

```rust,name=Assemble the pipeline and run it
#[tokio::main]
async fn main() -> Result<(), PcsError> {
    let mut pipeline = Pipeline::builder("first-pipeline")
        .with::<Order>()
        .with_resource(FxRates {
            eur: 1.08,
            gbp: 1.27,
            jpy: 0.0067,
        })
        .with_system(SeedOrders)
        .with_parallel_system(ConvertCurrency)
        .with_parallel_system(FlagExpress)
        .with_system(make_summary_system())
        .build();

    pipeline.run().await?;

    println!("stages: {:?}", pipeline.stages().unwrap_or_default());

    let summary = pipeline
        .data()
        .get_resource::<Summary>()
        .ok_or_else(|| PcsError::generic("Summary resource missing"))?;
    println!(
        "{} orders, {} express, {:.2} USD total",
        summary.rows, summary.express, summary.total_usd
    );

    Ok(())
}
```

```bash,name=Run the example
cargo run -p pcs-service --example first_pipeline
```

```text,name=Expected console output
seeded 5 orders
order 1      120.00 EUR ->     129.60 USD  express=false
order 2     4300.00 USD ->    4300.00 USD  express=true
order 3       75.50 GBP ->      95.89 USD  express=false
order 4   900000.00 JPY ->    6030.00 USD  express=true
order 5       12.00 EUR ->      12.96 USD  express=false
stages: [[0], [1, 2], [3]]
5 orders, 2 express, 10568.44 USD total
```

## 8. Read the plan back

`pipeline.stages()` returns `Vec<Vec<usize>>`: one inner vector per stage,
holding system indices in **registration order**. The line above reads
`[[0], [1, 2], [3]]`, which is:

- Stage 0, `SeedOrders` alone, because `write_component` collides with
  everything.
- Stage 1, `ConvertCurrency` and `FlagExpress` together, because their writes
  are disjoint. Declared as `ParallelSystem`, so they run concurrently.
- Stage 2, the summary closure, because it reads both columns stage 1 wrote.

`stages()` returns `None` until the first `run()` builds the plan.
[Pipeline](@/pipeline.md) covers the conflict graph, the topological sort and
per-system retry.

## 9. Where to go next

- [Sources & Sinks](@/io.md): read and write Parquet, CSV and NDJSON instead of
  hardcoding a seed system.
- [Scheduler](@/scheduler.md): several pipelines in one process, with dependency
  edges between them.
- [Distributed Runner](@/distributed.md): the same pipeline against claimed row
  ranges across nodes, with checkpoints.
- [WASM Processors](@/processors/_index.md): ship this same pipeline as a
  component that `pcs-service` loads at runtime.
