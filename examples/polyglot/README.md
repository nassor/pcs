# Polyglot example — one pipeline, four languages

A single PCS workload implemented as **four separate WebAssembly components**,
written in Go, Python, JavaScript and Rust, all exporting the same
`pcs:pipeline@0.2.0` WIT world and all operating on the same `Order` component.

The point it makes: **the pipeline contract is the WIT world, not the Rust SDK.**
`pcs-guest` is a convenience for Rust authors. Any language that compiles to a
WASI 0.2 component can be a PCS stage.

Prose version, with the byte-level contract and a per-language recipe:
<https://nassor.github.io/pcs/polyglot/>.

## Layout

```
examples/polyglot/
├── PINS.md                       # Pinned toolchain versions + the caveats that cost an hour
├── order-schema/                 # crate `pcs-polyglot-order` — THE canonical Order schema
│   └── src/lib.rs                #   schema + the 5-row fixture every path starts from
├── stages/
│   ├── go-validate/              # stage 1 — Go, writes `valid`
│   │   ├── arrowipc/             #   hand-rolled Arrow IPC codec (stdlib only) + its tests
│   │   └── export_pcs_pipeline_pipeline/  # the guest exports
│   ├── python-enrich/            # stage 2 — Python, writes `usd_amount`
│   │   ├── arrow_ipc.py          #   same codec, stdlib only
│   │   ├── app.py                #   the guest exports
│   │   └── test_arrow_ipc.py
│   ├── js-score/                 # stage 3 — JavaScript, writes `risk_score` + `flagged`
│   │   ├── arrow-ipc.js          #   same codec, no dependencies
│   │   ├── score.js              #   the guest exports
│   │   └── test/
│   └── rust-settle/              # stage 4 — Rust, writes `settlement` + keeps a ledger
│       └── src/lib.rs            #   two Systems in a real two-stage DAG, via pcs-guest
├── generated/                    # gitignored — produced by the `emit` command below
└── build/                        # gitignored — the four .wasm components land here
```

The driver lives with the host, not here:
`crates/pcs-service/examples/polyglot_orders.rs`. The regression test is
`crates/pcs-service/tests/polyglot_chain.rs`.

## The chain

`Order` has nine fields. Each stage reads what the previous one wrote:

| # | stage | language | reads | writes |
|---|-------|----------|-------|--------|
| 1 | `polyglot-validate-go` | Go | `amount` | `valid` |
| 2 | `polyglot-enrich-py` | Python | `valid`, `currency`, `amount` | `usd_amount` |
| 3 | `polyglot-score-js` | JavaScript | `usd_amount` | `risk_score`, `flagged` |
| 4 | `polyglot-settle-rs` | Rust | `valid`, `flagged`, `usd_amount` | `settlement`, `Ledger` |

Expected output for the five-row fixture — every value except `settlement` is
produced by a component containing no Rust:

| id | currency | amount | valid | usd_amount | risk_score | flagged | settlement |
|----|----------|--------|-------|------------|------------|---------|------------|
| 1 | EUR | 100 | true | ≈110.0 | ≈0.0022 | false | SETTLED |
| 2 | GBP | -5 | false | 0.0 | 0.0 | false | REJECTED |
| 3 | JPY | 1000000 | true | ≈6800.0 | ≈0.136 | false | SETTLED |
| 4 | USD | 60000 | true | 60000.0 | 1.2 | true | HOLD |
| 5 | EUR | 0 | false | 0.0 | 0.0 | false | REJECTED |

Ledger after batch 1: `settled_count = 2`, `settled_usd ≈ 6910.0`. Run a second
batch with the first batch's checkpoint as `prior` and it reads `4` / `≈13820.0`
— the host builds a fresh wasmtime `Store` per call, so accumulation across the
boundary can only happen through the checkpoint blob.

## Prerequisites

See [`PINS.md`](./PINS.md) for versions and the platform caveats.

```bash
rustup target add wasm32-wasip2
cargo install cargo-component --locked --version 0.21.1
cargo install wasm-tools      --locked --version 1.246.2
go install github.com/bytecodealliance/componentize-go@v0.4.1
pip install componentize-py==0.25.0
npm install -g @bytecodealliance/jco@1.30.0
```

## Build and run

```bash
# All four components into examples/polyglot/build/, then wasm-tools validate each.
bash scripts/build-polyglot.sh

# Only have one toolchain? Build one stage. `emit` still runs, because every
# stage depends on the generated constants matching the Rust schema.
bash scripts/build-polyglot.sh --only=rust
bash scripts/build-polyglot.sh --only=go,js

# Drive the chain: four describe() blocks, then two batches and the ledger.
# `tracing` is what makes guest host-io metrics visible.
cargo run -p pcs-service --features wasm,tracing --example polyglot_orders

# The same assertions, automated. Soft-skips when build/ is absent.
cargo test -p pcs-service --features wasm --test polyglot_chain -- --nocapture
```

Set `PCS_POLYGLOT_BUILD_DIR` to point the driver and the test at a different
directory of components.

## Native codec tests — run these first when something breaks

Three of the four guests hand-roll their Arrow IPC codec. When a value comes out
wrong, the useful signal is which language's codec is misreading bytes, not that
the chain produced a bad number. These tests decode the real
`generated/fixture_input.pcs` and compare every column against
`generated/fixture_input.json`, with no WebAssembly involved:

```bash
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit

cd examples/polyglot/stages/go-validate     && go test ./arrowipc/...
cd examples/polyglot/stages/python-enrich   && python -m unittest test_arrow_ipc
cd examples/polyglot/stages/js-score        && npm install && node --test
```

Two scoping quirks, both from generated code that only compiles inside a
component: `go test ./...` fails in the Go stage because the generated binding
packages use `//go:wasmimport`, and `python -m unittest discover` fails in the
Python stage once `componentize-py bindings` has run, because discovery imports a
support package that only resolves inside the component. Use the scoped commands
above.

## Code generation

`examples/polyglot/generated/` is derived from `pcs_polyglot_order::Order` and is
gitignored. Regenerate it with:

```bash
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit
```

| file | consumed by |
|------|-------------|
| `order_schema.ipc` | reference copy of the schema-only IPC stream |
| `order_fingerprint.txt` | reference copy of the fingerprint (`d52f95a6` today) |
| `fixture_input.pcs` | the three native codec test suites |
| `fixture_input.json` | ground truth for those tests |
| `schema_gen.go` | copied to `stages/go-validate/arrowipc/schema_gen.go` |
| `schema_gen.py` | copied to `stages/python-enrich/schema_gen.py` |
| `schema_gen.js` | copied to `stages/js-score/schema_gen.js` |

The non-Rust guests embed the schema bytes and the fingerprint as **generated
constants** rather than encoding a Schema flatbuffer at run time. That keeps
flatbuffer *writing* out of every guest. The drift risk it introduces is covered:
both the driver and the integration test compare each stage's reported
`schema_fingerprint` against the live fingerprint of `pcs_polyglot_order::Order`
and fail loudly on mismatch.

## Why the codecs are hand-rolled

Deliberate, and not a recommendation for production guests. `arrow-go` is
documented as incompatible with TinyGo, `pyarrow` has no `wasm32-wasi` wheel, and
`apache-arrow` on npm is unproven under StarlingMonkey. Hand-rolling makes all
three stages depend on nothing beyond their standard library — which is also what
forced the wire format to be written down precisely enough to reimplement, which
is the actual deliverable here.

If you verify a real Arrow binding survives its componentizer, swapping one stage
over is a good follow-up. Do not swap all three: the point of this example is
that the contract is reimplementable.

## Design constraints worth knowing before you extend it

- **Non-Rust stages only overwrite fixed-width columns in place.** They never
  write a flatbuffer. That is why `settlement`, the one `Utf8` output, belongs to
  the Rust stage, and why every downstream column already exists (zeroed) in the
  input.
- **The chain is a driver, not TOML.** `ServiceConfig.pipeline` is one
  `PipelineSpec` with one `Option<WasmSpec>`, and `BuiltService.runtime` is one
  trait object, so a single `pcs-service` process runs exactly one component.
  `crates/pcs-service/examples/configs/standalone_polyglot.toml` shows the TOML
  path with one stage. A config-driven chain means one process per stage joined
  by a Parquet sink → Parquet source.
- **`cargo test --workspace --all-features` soft-skips the chain test** unless
  `build/` is populated, because the default CI job installs no Go/Node/Python.
  The `Polyglot Guests` job is where the chain is actually exercised.
