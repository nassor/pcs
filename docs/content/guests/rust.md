+++
title = "A Rust guest"
description = "The only language with an SDK: cargo-component, pcs-guest, export_pipeline!, and a real component running under pcs-service at the end."
template = "page.html"
weight = 2
+++

# A Rust guest

Rust is the one language where the Arrow problem does not exist. `pcs-guest`
re-exports `Dataset`, `Pipeline`, `System` and the Arrow crates at the
workspace-pinned version, and `export_pipeline!` writes the WIT glue.

Every block below is from `examples/wasm/order_processing/`, which CI builds.

## 1. Install

```bash
rustup target add wasm32-wasip2
cargo install cargo-component --locked --version 0.21.1
cargo install wasm-tools --locked --version 1.246.2
```

## 2. Cargo.toml

A guest is a `cdylib`, not a binary. `cargo-component` wraps it into a
component targeting the world named in the metadata block.

```toml
[package]
name = "order-processing-wasm"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true
publish.workspace = true
description = "WASM Component Model port of the scheduler_etl example."

[lib]
crate-type = ["cdylib"]

[dependencies]
pcs-guest      = { path = "../../../crates/pcs-guest" }
serde          = { workspace = true }
# cargo-component generates src/bindings.rs against the WIT target and
# requires this runtime crate at link time.
wit-bindgen-rt = { version = "0.44.0", features = ["bitflags"] }

[package.metadata.component]
package = "pcs:order-processing"

[package.metadata.component.target]
path = "../../../crates/pcs-guest/wit"
world = "pcs-pipeline"
```

`target.path` points at the canonical WIT directory in the workspace. Do not
copy the file next to your crate; a vendored copy desynchronises the moment the
package version moves.

## 3. src/lib.rs

The full file is `examples/wasm/order_processing/src/lib.rs`. The blocks below
are all of it except the report printer.

`cargo-component` generates `src/bindings.rs` only for a `wasm32` target, so the
module declaration is gated:

```rust
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings;

use std::sync::Arc;

use pcs_guest::arrow_array::{BooleanArray, Float64Array, RecordBatch};
use pcs_guest::arrow_schema::{DataType, Field, Schema};
use pcs_guest::prelude::*;
```

The component is a plain struct plus its Arrow schema, identical to what a
native pipeline declares:

```rust
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

```rust
struct ValidateSystem;

#[pcs_guest::prelude::async_trait]
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

        let new_columns: Vec<Arc<dyn pcs_guest::arrow_array::Array>> = (0..schema.fields().len())
            .map(|i| {
                if i == valid_idx {
                    new_valid.clone() as Arc<dyn pcs_guest::arrow_array::Array>
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

```rust
struct EnrichSystem {
    rates: FxRates,
}

#[pcs_guest::prelude::async_trait]
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

        let new_columns: Vec<Arc<dyn pcs_guest::arrow_array::Array>> = (0..schema.fields().len())
            .map(|i| {
                if i == usd_idx {
                    new_usd.clone() as Arc<dyn pcs_guest::arrow_array::Array>
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

```rust
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
pcs_guest::export_pipeline!(build);
```

`export_pipeline!` calls `build()` lazily, once per component instance, on the
first WIT export call. It implements `describe` from the registered components
and `run-batch` from `Pipeline::run_on_with_stats`.

## 4. Config

The macro emits two accessors **into your crate**, not into `pcs-guest`:
`pcs_config_get(key) -> Option<String>` and
`pcs_config_parse::<T>(key) -> Option<Result<T, T::Err>>`. They live caller side
because the WIT bindings do, so they only exist under
`#[cfg(target_arch = "wasm32")]`.

This guest reads four keys and falls back per missing key:

```rust
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

A stateful guest declares its state type in the macro:

```rust
pcs_guest::export_pipeline!(build, state = Counter);
```

That installs a `GuestState<Counter>` resource on the batch dataset before any
system runs, deserialising it from `prior`, and serialises it back into
`run-result.checkpoint` afterwards. A system reaches it with
`data.get_resource_mut::<GuestState<Counter>>()`.

State is a **resource**, not a registered component. Resources do not round-trip
through Arrow IPC, so guest state never leaks into `run-result.output` and never
shows up in `describe()`. `crates/pcs-guest-smoketest/src/lib.rs` is the minimal
stateful guest: one unregistered `Counter` component held in the resource, one
system incrementing it, and a host that threads `checkpoint` back in as `prior`
observing 1, 2, 3.

## 6. Build

```bash
cargo component build --release -p order-processing-wasm --target wasm32-wasip2
```

<div class="note note-warn">
<span class="note-label">Gitignore the generated bindings</span>

`src/bindings.rs` is regenerated on every build, and a committed copy
desynchronises the moment the WIT changes, so gitignore it. One consequence:
`rustfmt` walks every `mod` declaration regardless of `#[cfg(...)]`, so CI
needs that file **on disk** before `cargo fmt --all -- --check` will pass, even
though the host build never compiles it.

</div>

<div class="note">
<span class="note-label">Expected</span>

The artifact lands under `target/wasm32-wasip1/release/`, not
`wasm32-wasip2`. `cargo-component` compiles the core module for wasip1 and
adapts it into a component afterward, keeping the pre-adapter directory name.

</div>

## 7. Validate

```bash
wasm-tools validate --features component-model \
  target/wasm32-wasip1/release/order_processing_wasm.wasm

wasm-tools component wit \
  target/wasm32-wasip1/release/order_processing_wasm.wasm | grep 'pcs:pipeline'
```

```text
  import pcs:pipeline/host-io@0.2.0;
  export pcs:pipeline/pipeline@0.2.0;
```

## 8. Run it under pcs-service

`crates/pcs-service/examples/configs/standalone_wasm.toml` runs this guest
against a five-row CSV fixture. Its paths are relative to `crates/pcs-service`:

```bash
cd crates/pcs-service

cargo run --features service,wasm --bin pcs-service -- validate \
  --config examples/configs/standalone_wasm.toml --strict

cargo run --features service,wasm --bin pcs-service -- serve \
  --config examples/configs/standalone_wasm.toml
```

`validate` parses the config, compiles the component, checks the WIT world, and
confirms every source and sink names a component `describe()` declared. Nothing
reads a row until it passes.

`run_mode` is `one_shot`, so `serve` processes the fixture once and exits:

```text
INFO pcs_service::service::standalone: iteration starting iteration=1 mode=OneShot
INFO pcs_service::service::standalone: iteration complete iteration=1 rows_processed=5 duration_ms=2
INFO pcs_service::service::standalone: one-shot mode: exiting after first iteration
```

The sink wrote `/tmp/pcs-order-processing-out.csv`:

```text
id,amount,currency,valid,usd_amount
1,120.0,EUR,true,129.60000000000002
2,4300.0,USD,true,4300.0
3,75.5,GBP,true,95.885
4,-50.0,EUR,false,-54.0
5,900000.0,JPY,true,6030.0
```

`valid` and `usd_amount` entered as `false` and `0.0`; the guest wrote both. Row
4 is the negative amount `ValidateSystem` rejects, and `EnrichSystem` converts
it anyway, because it reads `amount` rather than `valid`.

## Where to go next

- [The WIT contract](@/guests/wit-contract.md): what every field of
  `describe()` is checked against.
- [Six languages, one pipeline](@/guests/six-languages.md): the same world
  implemented six times, chained.
- [Operating pcs-service](@/operations/running-pcs.md): deploying a built
  `.wasm`.
