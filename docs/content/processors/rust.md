+++
title = "A Rust processor"
description = "The only language with an SDK: a plain wasm32-wasip2 cargo build, pcs-processor, export_pipeline!, and a real component running under pcs-service at the end."
template = "page.html"
weight = 3
aliases = ["/guests/rust/"]
+++

# A Rust processor

Rust is the one language where the Arrow problem does not exist. `pcs-processor`
re-exports `Dataset`, `Pipeline`, `System` and the Arrow crates at the
workspace-pinned version, and `export_pipeline!` writes the WIT glue.

Every block below is from `examples/wasm/order_processing/`, which CI builds.

## 1. Install

```bash,name=Add the target and wasm-tools
rustup target add wasm32-wasip2
cargo install wasm-tools --locked --version 1.246.2
```
Runs the same on Linux, macOS and Windows (PowerShell).

No componentizer. `rustc` links a `wasm32-wasip2` cdylib into a Component Model
component itself, so `cargo build` is the whole toolchain.

## 2. Cargo.toml

A processor is a `cdylib`, not a binary. `cargo build --target wasm32-wasip2`
links it into a component exporting the world its bindings name.

```toml,name=Cargo.toml for a cdylib processor
[package]
name = "order-processing-wasm"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true
# A sample processor component, not a library a downstream consumer would
# depend on.
publish = false
description = "WASM Component Model port of the scheduler_etl example. Demonstrates field-granular DAG scheduling running inside a WASM processor via pcs-processor."

[lib]
crate-type = ["cdylib"]

[dependencies]
pcs-processor      = { path = "../../../crates/pcs-processor" }
serde          = { workspace = true }

# The bindings generator. Gated on wasm32 because `src/lib.rs` gates the
# generated module the same way: the host build of this crate is an empty cdylib
# and has no use for the generator or its proc-macro dependency tree.
[target.'cfg(target_arch = "wasm32")'.dependencies]
wit-bindgen = { workspace = true }
```

The `generate!` path in `src/lib.rs` points at the canonical WIT directory in the
workspace. Do not copy the file next to your crate; a vendored copy
desynchronises the moment the package version moves.

## 3. src/lib.rs

The full file is `examples/wasm/order_processing/src/lib.rs`. The blocks below
are all of it except the report printer.

The bindings are generated in place, gated on `wasm32`: the expansion emits the
canonical ABI intrinsics and the `component-type` custom section, neither of
which the host target can link.

```rust,name=The generated bindings and the imports
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../../crates/pcs-processor/wit",
        world: "pcs-pipeline",
        generate_all,
    });
}

use std::sync::Arc;

use pcs_processor::arrow_array::{BooleanArray, Float64Array, RecordBatch};
use pcs_processor::arrow_schema::{DataType, Field, Schema};
use pcs_processor::prelude::*;
```

The component is a plain struct plus its Arrow schema, identical to what a
native pipeline declares:

```rust,name=The component and its Arrow schema
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Transaction {
    pub id: u64,
    pub amount: f64,
    pub currency: String,
    pub valid: bool,
    pub usd_amount: f64,
}

impl Component for Transaction {
    fn name() -> &'static str {
        "Transaction"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("valid", DataType::Boolean, false),
            Field::new("usd_amount", DataType::Float64, false),
        ]))
    }
}

impl Transaction {
    const ID: FieldRef<Transaction> = FieldRef::new("id");
    const AMOUNT: FieldRef<Transaction> = FieldRef::new("amount");
    const CURRENCY: FieldRef<Transaction> = FieldRef::new("currency");
    const VALID: FieldRef<Transaction> = FieldRef::new("valid");
    const USD_AMOUNT: FieldRef<Transaction> = FieldRef::new("usd_amount");
}
```

`ValidateSystem` reads `amount` and writes `valid`:

```rust,name=ValidateSystem writes the valid column
struct ValidateSystem;

#[pcs_processor::prelude::async_trait]
impl System for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT)
            .writes(Transaction::VALID)
    }

    async fn run(&self, dataset: &mut Dataset) -> PcsResult<()> {
        let batch = dataset
            .columns::<Transaction>()
            .ok_or_else(|| PcsError::generic("Transaction batch not found"))?
            .clone();

        let n;
        let valid_flags: Vec<bool>;
        {
            let txns = dataset.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            n = txns.len();
            valid_flags = (0..n).map(|i| amount_col.value(i) > 0.0).collect();
        }
        let valid_count = valid_flags.iter().filter(|&&v| v).count();

        let new_valid = Arc::new(BooleanArray::from(valid_flags));
        let schema = batch.schema();
        let valid_idx = schema
            .index_of("valid")
            .map_err(|e| PcsError::generic(format!("Transaction.valid missing: {e}")))?;

        let new_columns: Vec<Arc<dyn pcs_processor::arrow_array::Array>> =
            (0..schema.fields().len())
                .map(|i| {
                    if i == valid_idx {
                        new_valid.clone() as Arc<dyn pcs_processor::arrow_array::Array>
                    } else {
                        batch.column(i).clone()
                    }
                })
                .collect();

        let new_batch = RecordBatch::try_new(schema, new_columns)
            .map_err(|e| PcsError::generic(format!("RecordBatch rebuild error: {e}")))?;

        dataset.replace_batch::<Transaction>(new_batch)?;

        println!(
            "[validate]  {valid_count} valid, {} rejected",
            n - valid_count
        );
        Ok(())
    }
}
```

`EnrichSystem` reads `amount` and `currency` and writes `usd_amount`. `valid`
and `usd_amount` are disjoint, so the field-granular DAG puts both systems in
one stage:

```rust,name=EnrichSystem converts amounts to USD
struct EnrichSystem {
    rates: FxRates,
}

#[pcs_processor::prelude::async_trait]
impl System for EnrichSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("enrich")
            .reads(Transaction::AMOUNT)
            .reads(Transaction::CURRENCY)
            .writes(Transaction::USD_AMOUNT)
    }

    async fn run(&self, dataset: &mut Dataset) -> PcsResult<()> {
        let batch = dataset
            .columns::<Transaction>()
            .ok_or_else(|| PcsError::generic("Transaction batch not found"))?
            .clone();

        let n;
        let usd_amounts: Vec<f64>;
        {
            let txns = dataset.view::<Transaction>()?;
            let amount_col = txns.f64(Transaction::AMOUNT)?;
            let currency_col = txns.str(Transaction::CURRENCY)?;
            n = txns.len();
            usd_amounts = (0..n)
                .map(|i| {
                    let rate = self.rates.rate_for(currency_col.value(i));
                    amount_col.value(i) * rate
                })
                .collect();
        }

        let new_usd = Arc::new(Float64Array::from(usd_amounts));
        let schema = batch.schema();
        let usd_idx = schema
            .index_of("usd_amount")
            .map_err(|e| PcsError::generic(format!("Transaction.usd_amount missing: {e}")))?;

        let new_columns: Vec<Arc<dyn pcs_processor::arrow_array::Array>> =
            (0..schema.fields().len())
                .map(|i| {
                    if i == usd_idx {
                        new_usd.clone() as Arc<dyn pcs_processor::arrow_array::Array>
                    } else {
                        batch.column(i).clone()
                    }
                })
                .collect();

        let new_batch = RecordBatch::try_new(schema, new_columns)
            .map_err(|e| PcsError::generic(format!("RecordBatch rebuild error: {e}")))?;

        dataset.replace_batch::<Transaction>(new_batch)?;

        println!("[enrich]    converted {n} rows to USD");
        Ok(())
    }
}
```

There is no ingest system. Rows arrive in the host's `run-batch` payload, from
whatever source the service config declares.

`build()` returns the `Pipeline`, and one macro line turns it into a component:

```rust,name=build returns the Pipeline the macro exports
pub fn build() -> Pipeline {
    // `pcs_config_parse` is emitted into this crate by `export_pipeline!` and
    // only exists on wasm32, where `crate::bindings` exists.
    #[cfg(target_arch = "wasm32")]
    let rates = FxRates::from_config();
    #[cfg(not(target_arch = "wasm32"))]
    let rates = FxRates::DEFAULT;

    Pipeline::builder("order_processing")
        .with::<Transaction>()
        .with_system(ValidateSystem)
        .with_system(EnrichSystem { rates })
        .with_system(make_report_system())
        .build()
}

#[cfg(target_arch = "wasm32")]
pcs_processor::export_pipeline!(build);
```

`export_pipeline!` calls `build()` lazily, once per component instance, on the
first WIT export call. It implements `describe` from the registered components
and `run-batch` from `Pipeline::run_on_with_stats`.

## 4. Config

The macro emits two accessors **into your crate**, not into `pcs-processor`:
`pcs_config_get(key) -> Option<String>` and
`pcs_config_parse::<T>(key) -> Option<Result<T, T::Err>>`. They live caller side
because the WIT bindings do, so they only exist under
`#[cfg(target_arch = "wasm32")]`.

This processor reads four keys and falls back per missing key:

```rust,name=Reading the four rate keys from config
impl FxRates {
    const DEFAULT: FxRates = FxRates {
        eur: 1.08,
        gbp: 1.27,
        jpy: 0.0067,
        cad: 0.74,
    };

    #[cfg(target_arch = "wasm32")]
    fn from_config() -> Self {
        fn rate(key: &str, default: f64) -> f64 {
            match crate::pcs_config_parse::<f64>(key) {
                Some(Ok(v)) => v,
                Some(Err(e)) => {
                    eprintln!("[config] {key} is not a valid f64 ({e}); using {default}");
                    default
                }
                None => default,
            }
        }

        Self {
            eur: rate("fx_eur", Self::DEFAULT.eur),
            gbp: rate("fx_gbp", Self::DEFAULT.gbp),
            jpy: rate("fx_jpy", Self::DEFAULT.jpy),
            cad: rate("fx_cad", Self::DEFAULT.cad),
        }
    }
}
```

The rates are a field on `EnrichSystem` rather than a `Dataset` resource.
`write_ipc` serialises registered components and the alive bitmap, not the
resource map, so a dataset rebuilt from IPC has no resources and
`get_resource::<FxRates>()` inside `run_on` would fail.

## 5. State across batches

A stateful processor declares its state type in the macro:

```rust,name=Declaring the state type in the macro
pcs_processor::export_pipeline!(build, state = Counter);
```

That installs a `ProcessorState<Counter>` resource on the batch dataset before any
system runs, deserialising it from `prior`, and serialises it back into
`run-result.checkpoint` afterwards. A system reaches it with
`data.get_resource_mut::<ProcessorState<Counter>>()`.

State is a **resource**, not a registered component. Resources do not
round-trip through Arrow IPC, so processor state never leaks into
`run-result.output` and never shows up in `describe()`.
`crates/pcs-processor-smoketest/src/lib.rs` is the minimal stateful
processor: one unregistered `Counter` component held in the resource, one
system incrementing it, and a host that threads `checkpoint` back in as
`prior` observing 1, 2, 3.

## 6. Build

```bash,name=Build the component
cargo build --release -p order-processing-wasm --target wasm32-wasip2
```
Runs the same on Linux, macOS and Windows (PowerShell).

<div class="note">
<span class="note-label">No generated files, no adapter</span>

The bindings live in the `generate!` expansion, so nothing lands in your source
tree and nothing has to exist on disk before `cargo fmt --all -- --check`. The
artifact under `target/wasm32-wasip2/release/` is the finished component: no
preview1 core module and no adapter step.

`.cargo/config.toml` adds `-C target-feature=+simd128` for the target, so the
built component carries `simd128` in its target features. wasmtime enables the
SIMD proposal by default.

</div>

## 7. Validate

```bash,name=Validate the built component
wasm-tools validate --features component-model \
  target/wasm32-wasip2/release/order_processing_wasm.wasm

wasm-tools component wit \
  target/wasm32-wasip2/release/order_processing_wasm.wasm | grep 'pcs:pipeline'
```
Windows (PowerShell):

```powershell
wasm-tools validate --features component-model target/wasm32-wasip2/release/order_processing_wasm.wasm
wasm-tools component wit target/wasm32-wasip2/release/order_processing_wasm.wasm | Select-String 'pcs:pipeline'
```

```text,name=Expected wasm-tools output
  import pcs:pipeline/host-io@0.3.0;
  export pcs:pipeline/pipeline@0.3.0;
```

## 8. Run it under pcs-service

`examples/configs/standalone_wasm.kdl` runs this processor
against a five-row CSV fixture. Its paths are relative to the repository root:

```bash,name=Validate the config then serve it
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- validate \
  --config examples/configs/standalone_wasm.kdl --strict

cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- serve \
  --config examples/configs/standalone_wasm.kdl
```
Windows (PowerShell):

```powershell
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- validate --config examples/configs/standalone_wasm.kdl --strict
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- serve --config examples/configs/standalone_wasm.kdl
```

`validate` parses the config, compiles the component, checks the WIT world, and
confirms every source and sink names a component `describe()` declared. Nothing
reads a row until it passes.

`run_mode` is `one_shot`, so `serve` processes the fixture once and exits:

```text,name=Expected service log for a one-shot run
DEBUG pcs_service::service::standalone: iteration starting workflow=order-processing iteration=1 mode=OneShot
DEBUG pcs_service::service::standalone: iteration complete workflow=order-processing iteration=1 rows_processed=5 duration_ms=2
INFO pcs_service::service::standalone: one-shot mode: exiting after first iteration
```

The two iteration lines are `debug` level, so the config's `log_level="info"`
shows only the third: pass `--log-level debug` (or set
`observability log_level="debug"`) to see the per-iteration lines.

The sink wrote `/tmp/pcs-order-processing-out.csv`:

```text,name=The CSV the sink wrote
id,amount,currency,valid,usd_amount
1,120.0,EUR,true,129.60000000000002
2,4300.0,USD,true,4300.0
3,75.5,GBP,true,95.885
4,-50.0,EUR,false,-54.0
5,900000.0,JPY,true,6030.0
```

`valid` and `usd_amount` entered as `false` and `0.0`; the processor wrote
both. Row 4 is the negative amount `ValidateSystem` rejects, and
`EnrichSystem` converts it anyway, because it reads `amount` rather than
`valid`.

## Where to go next

- [The WIT contract](@/processors/wit-contract.md): what every field of
  `describe()` is checked against.
- [Six languages, one pipeline](@/processors/_index.md#six-languages-one-pipeline): the same world
  implemented six times, chained.
- [Operating pcs-service](@/operations/running-pcs.md): deploying a built
  `.wasm`.
