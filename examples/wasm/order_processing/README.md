# `order-processing-wasm` — WASM port of `scheduler_etl`

This is the WebAssembly Component Model port of the `scheduler_etl` example
from `examples/native/scheduler_etl.rs`. It exercises the
`pcs-processor` SDK and the `export_pipeline!` macro end-to-end against a real
pipeline with field-granular DAG scheduling.

## What it demonstrates

A 2-stage pipeline over a `Transaction` component:

```
Stage 0:  [ValidateSystem,        — reads "amount", writes "valid"
           EnrichSystem]          — reads "amount"/"currency", writes "usd_amount"
                                  — disjoint writes → same stage (parallel-safe)

Stage 1:  [ReportSystem]          — reads "valid" + "usd_amount" + others
                                  — must follow stage 0
```

Field-granular DAG scheduling is preserved across the WASM boundary because
the entire DAG planner runs **inside** the processor. The host hands the processor one
Arrow IPC payload per batch and gets one back; everything between (DAG build,
stage execution, retry handling) is native Rust running inside the sandbox.

## How it differs from the native example

The native `scheduler_etl` has four systems (Ingest, Validate, Enrich,
Report). The WASM port has three — `IngestSystem` is removed because in the
processor model **the data flows in via the host's `run-batch` Arrow IPC payload**,
not via a hardcoded Rust list. The host loads `Transaction` rows from a
configured source (Parquet/CSV/NDJSON) and ships them across the boundary.

`FxRates` is also handled differently. In the native example it's a
`Dataset` resource added via `Pipeline::builder().with_resource(...)`. In the
WASM port it's a struct field on `EnrichSystem`. The reason is that
`Dataset::write_ipc` only serializes registered components and the alive
bitmap — it does **not** serialize the resource map. A fresh dataset
reconstructed from IPC has zero resources, so any `get_resource::<FxRates>()`
inside `run_on` would fail. Folding configuration into the system struct
sidesteps that limitation entirely. The same reasoning applies to the
native example's `Report` resource — the WASM port prints summary lines via
`println!` (routed to the host's tracing layer through `wasi:cli/stdout`)
instead of writing a host-side resource.

FX rates come from the `config` child of the host's `wasm` node. `build()` reads
the `fx_eur` / `fx_gbp` / `fx_jpy` / `fx_cad` keys via `pcs_config_parse::<f64>`
— the accessor `export_pipeline!` emits into this crate, backed by the
`pcs:pipeline/host-io` `get-config` import — and falls back to
`FxRates::DEFAULT` per missing key.

## Build

From the workspace root:

```bash
cargo build --release \
  -p order-processing-wasm \
  --target wasm32-wasip2
```

This produces:

```
target/wasm32-wasip2/release/order_processing_wasm.wasm
```

(No componentizer: `rustc` links a `wasm32-wasip2` cdylib into a Component Model
component itself, so plain `cargo build` produces the finished component. Same
output path shape as the smoketest.)

Validate the component:

```bash
wasm-tools validate --features component-model \
  target/wasm32-wasip2/release/order_processing_wasm.wasm
```

Inspect the exported world to confirm `pcs:pipeline/pipeline@0.3.0` is
exported:

```bash
wasm-tools component wit \
  target/wasm32-wasip2/release/order_processing_wasm.wasm
```

## Run via `pcs-service`

`examples/configs/standalone_wasm.kdl` runs this component
against a five-row CSV fixture. Its paths are relative to the repository root:

```bash
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- validate \
  --config examples/configs/standalone_wasm.kdl --strict

cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- serve \
  --config examples/configs/standalone_wasm.kdl
```

`run_mode` is `one_shot`, so `serve` processes
`examples/configs/fixtures/order_processing_input.csv` once and writes
`/tmp/pcs-order-processing-out.csv` with `valid` and `usd_amount` filled in. The
fixture seeds both columns with `false` and `0.0`: the csv transformer turns an
empty field into a NULL, the `Transaction` schema is non-nullable, and one blank
cell fails the whole batch.

## Source layout

```
examples/wasm/order_processing/
├── Cargo.toml            # cdylib, wit-bindgen as a wasm32-only dependency
├── README.md             # this file
└── src/
    └── lib.rs            # wit_bindgen::generate! bindings module, Transaction
                          # component, 3 systems, build() fn, and
                          # export_pipeline!(build), all gated on wasm32
```

## Errors and traps

The `export_pipeline!` macro in `pcs-processor` converts pipeline failures into
the WIT `run-error` variant per the frozen contract:

| `PcsError` from `Pipeline::run_on`  | WIT `run-error`         | Host action            |
|-------------------------------------|-------------------------|------------------------|
| `RetryExhausted`, `SystemExecution` | `retryable(string)`     | Release claim, retry   |
| Everything else                     | `permanent(string)`     | Ack claim, surface     |

System authors should construct `PcsError::SystemExecution(...)` for
recoverable failures rather than calling `.unwrap()` or `panic!()`. Panics
become wasm traps and the host catches them as `permanent` via a
trap-specific override — the operator loses the batch instead of getting a
retry. Idiomatic processor pipelines avoid panics in `System::run` for this
reason.
