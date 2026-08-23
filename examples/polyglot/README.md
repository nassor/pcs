# Polyglot example: one pipeline, six languages

A single PCS workload implemented as **six separate WebAssembly components**,
written in Go, Python, TypeScript, Kotlin, C# and Rust, all exporting the same
`pcs:pipeline@0.2.0` WIT world and all operating on the same `Order` component.

The point it makes: **the pipeline contract is the WIT world, not the Rust SDK.**
`pcs-guest` is a convenience for Rust authors. Any language that compiles to a
WASI 0.2 component can be a PCS stage.

Prose version, with the byte-level contract and a per-language recipe:
<https://nassor.github.io/pcs/guests/>.

## Layout

```
examples/polyglot/
  PINS.md                        # Pinned toolchain versions + the caveats that cost an hour
  order-schema/                  # crate `pcs-polyglot-order`, THE canonical Order schema
    src/lib.rs                   #   schema + the 6-row fixture every path starts from
  stages/
    go-validate/                 # stage 1, Go, writes `valid`
      export_pcs_pipeline_pipeline/    # the guest exports
      go.mod                     #   requires the codec through a local `replace`
    python-enrich/               # stage 2, Python, writes `usd_amount`
      app.py                     #   the guest exports
    ts-score/                    # stage 3, TypeScript, writes `risk_score` + `flagged`
      score.ts                   #   the guest exports
      wit.d.ts                   #   types for the `pcs:pipeline/host-io@0.2.0` import
      tsconfig.json              #   checker only: nothing here emits
      package.json               #   `file:` link to the codec package
    kotlin-fee/                  # stage 4, Kotlin, writes `fee`
      src/wasmWasiMain/          #   the guest exports plus the generated WIT bindings
      build.gradle.kts           #   resolves the codec from mavenLocal()
    csharp-tier/                 # stage 5, C#, writes `review_tier`
      PipelineExports.cs         #   the guest exports
      tier-cs.csproj             #   ProjectReference to the codec package
      nuget.config               #   the dotnet-experimental feed the AOT backend needs
    rust-settle/                 # stage 6, Rust, writes `settlement` + keeps a ledger
      src/lib.rs                 #   two Systems in a real two-stage DAG, via pcs-guest
  generated/                     # gitignored, produced by the `emit` command below
  build/                         # gitignored, the six .wasm components land here
```

The five non-Rust stages share one Arrow IPC codec, released as `pcs-arrow-ipc`
and living in `packages/` at the repository root, one directory per language.

The driver lives with the host, not here:
`crates/pcs-service/examples/polyglot_orders.rs`. The regression test is
`crates/pcs-service/tests/polyglot_chain.rs`.

## The chain

`Order` has eleven fields. Each stage reads what an earlier one wrote:

| # | stage | language | reads | writes |
|---|-------|----------|-------|--------|
| 1 | `polyglot-validate-go` | Go | `amount` | `valid` |
| 2 | `polyglot-enrich-py` | Python | `valid`, `currency`, `amount` | `usd_amount` |
| 3 | `polyglot-score-ts` | TypeScript | `usd_amount` | `risk_score`, `flagged` |
| 4 | `polyglot-fee-kt` | Kotlin | `valid`, `region`, `usd_amount` | `fee` |
| 5 | `polyglot-tier-cs` | C# | `flagged`, `risk_score` | `review_tier` |
| 6 | `polyglot-settle-rs` | Rust | `valid`, `review_tier`, `usd_amount`, `fee` | `settlement`, `Ledger` |

Expected output for the six-row fixture. Every value except `settlement` is
produced by a component containing no Rust:

| id | region | currency | amount | valid | usd_amount | risk_score | flagged | fee | review_tier | settlement |
|----|--------|----------|--------|-------|------------|------------|---------|-----|-------------|------------|
| 1 | emea | EUR | 100 | true | ≈110.0 | ≈0.0022 | false | ≈1.32 | 0 | SETTLED |
| 2 | emea | GBP | -5 | false | 0.0 | 0.0 | false | 0.0 | 0 | REJECTED |
| 3 | apac | JPY | 1000000 | true | ≈6800.0 | ≈0.136 | false | ≈54.4 | 0 | SETTLED |
| 4 | amer | USD | 60000 | true | 60000.0 | 1.2 | true | 600.0 | 2 | HOLD |
| 5 | emea | EUR | 0 | false | 0.0 | 0.0 | false | 0.0 | 0 | REJECTED |
| 6 | apac | USD | 20000 | true | 20000.0 | 0.4 | false | 160.0 | 1 | REVIEW |

Ledger after batch 1: `settled_count = 2`, `settled_usd ≈ 6854.28`, which is
`(110.0 - 1.32) + (6800.0 - 54.4)`. Run a second batch with the first batch's
checkpoint as `prior` and it reads `4` / `≈13708.56`: the host builds a fresh
wasmtime `Store` per call, so accumulation across the boundary can only happen
through the checkpoint blob.

That total nets the Kotlin stage's `fee` out of the Python stage's converted
`usd_amount`, over the rows the C# stage cleared for settlement. One float
crosses five languages before the Rust stage adds it up.

## Prerequisites

See [`PINS.md`](./PINS.md) for versions and the platform caveats.

```bash
rustup target add wasm32-wasip2
cargo install cargo-component --locked --version 0.21.1
cargo install wasm-tools      --locked --version 1.246.2
go install github.com/bytecodealliance/componentize-go@v0.4.1
pip install componentize-py==0.25.0
npm install -g @bytecodealliance/jco@1.30.0
cargo install wit-bindgen-cli --git https://github.com/Kotlin/wit-bindgen --branch kotlin
```

The last line has no released version to pin, only the branch; it installs as
`wit-bindgen` v0.57.1. The Kotlin stage also needs JDK 21 and Gradle 8.14.4 or
newer, and Kotlin 2.4.0 itself needs no install: `build.gradle.kts` pins the
version and Gradle fetches the compiler. Gradle emits a core module, so
`wasm-tools` turns it into a component with the reactor adapter from the
wasmtime v47.0.3 release; the build script downloads that adapter into
`generated/` when it is absent, and `PCS_WASI_ADAPTER` overrides the path.

The C# stage needs the .NET 10 SDK, verified on 10.0.400, with no
`dotnet workload install` step. Its first build downloads wasi-sdk 29.0 into
`~/.wasi-sdk/`, about 535 MB.

## Build and run

```bash
# All six components into examples/polyglot/build/, then wasm-tools validate each.
bash scripts/build-polyglot.sh

# Only have one toolchain? Build one stage. `emit` still runs, because every
# stage depends on the generated constants matching the Rust schema.
bash scripts/build-polyglot.sh --only=rust
bash scripts/build-polyglot.sh --only=go,ts
bash scripts/build-polyglot.sh --only=kotlin,csharp

# Drive the chain: six describe() blocks, then two batches and the ledger.
# `tracing` is what makes guest host-io metrics visible.
cargo run -p pcs-service --features wasm,tracing --example polyglot_orders

# The same assertions, automated. Soft-skips when build/ is absent.
cargo test -p pcs-service --features wasm --test polyglot_chain -- --nocapture
```

Set `PCS_POLYGLOT_BUILD_DIR` to point the driver and the test at a different
directory of components.

A missing tool exits with a code of its own rather than failing generically: 2
cargo-component, 3 wasm-tools, 4 Go, 5 componentize-go, 6 componentize-py, 7
Node or npm, 9 Gradle, 10 wit-bindgen, 11 dotnet, 12 curl. Code 8 means a stage
built without producing an artifact. Code 12 is only reachable when the WASI
preview 1 reactor adapter is absent and the script has to fetch it.

## Native codec tests, run these first when something breaks

Five of the six guests share one Arrow IPC codec. When a value comes out wrong,
the useful signal is which language's codec is misreading bytes, not that the
chain produced a bad number. These tests decode the real
`generated/fixture_input.pcs` and compare every column against
`generated/fixture_input.json`, with no WebAssembly involved:

```bash
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit

cd packages/arrow-ipc-go && go test ./...
cd packages/arrow-ipc-py && PYTHONPATH=src python -m unittest discover -s tests
cd packages/arrow-ipc-ts && npm ci && npm run typecheck && npm run build && npm test
cd packages/arrow-ipc-kt && gradle jvmTest
cd packages/arrow-ipc-cs && dotnet test tests
```

Each package is an ordinary project for its language, outside every stage's
generated-code tree, so nothing here needs a scoped test command.

## Code generation

`examples/polyglot/generated/` is derived from `pcs_polyglot_order::Order` and is
gitignored. Regenerate it with:

```bash
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit
```

| file | consumed by |
|------|-------------|
| `order_schema.ipc` | reference copy of the schema-only IPC stream |
| `order_fingerprint.txt` | reference copy of the fingerprint (`8c0a76ff` today) |
| `fixture_input.pcs` | the five native codec test suites |
| `fixture_input.json` | ground truth for those tests |
| `schema_gen.go` | copied to `stages/go-validate/export_pcs_pipeline_pipeline/schema_gen.go` |
| `schema_gen.py` | copied to `stages/python-enrich/schema_gen.py` |
| `schema_gen.ts` | copied to `stages/ts-score/schema_gen.ts` |
| `SchemaGen.kt` | copied to `stages/kotlin-fee/src/wasmWasiMain/kotlin/impl/SchemaGen.kt` |
| `SchemaGen.cs` | copied to `stages/csharp-tier/SchemaGen.cs` |

The non-Rust guests embed the schema bytes and the fingerprint as **generated
constants** rather than encoding a Schema flatbuffer at run time. That keeps
flatbuffer *writing* out of every guest. The drift risk it introduces is covered:
both the driver and the integration test compare each stage's reported
`schema_fingerprint` against the live fingerprint of `pcs_polyglot_order::Order`
and fail loudly on mismatch.

## Why the codec is standard-library only

Deliberate, and not a recommendation for production guests. Each language's real
Arrow binding is either unusable or unproven under its componentizer: `arrow-go`
is documented as incompatible with TinyGo, `pyarrow` has no `wasm32-wasi` wheel,
`apache-arrow` on npm is unproven under StarlingMonkey, and `arrow-java` is a JVM
library that a `wasmWasi` target cannot reach. `Apache.Arrow` on NuGet is pure C#
with no native dependency, which makes it the most plausible of the six, and it
is unverified here too.

The constraint is what forced the wire format to be written down precisely enough
to reimplement. That specification is the actual deliverable, and it is why the
package is thin: a sixth language implements the same bytes from
`docs/content/reference/wire-format.md`.

If you verify a real Arrow binding survives its componentizer, swapping one stage
over is a good follow-up.

## Design constraints worth knowing before you extend it

- **Non-Rust stages only overwrite fixed-width columns in place.** They never
  write a flatbuffer. That is why `settlement`, the one `Utf8` output, belongs to
  the Rust stage, and why every downstream column already exists (zeroed) in the
  input.
- **The five fixed-width outputs cover every fixed-width type the format has.**
  `Boolean` for `valid` and `flagged`, `Float64` for `usd_amount`, `risk_score`
  and `fee`, `Int64` for `review_tier`. `review_tier` is the schema's only
  `Int64` output, so it is the one column that proves an `Int64` in-place write.
- **The chain is a driver, not TOML.** `ServiceConfig.pipeline` is one
  `PipelineSpec` with one `Option<WasmSpec>`, and `BuiltService.runtime` is one
  trait object, so a single `pcs-service` process runs exactly one component.
  `crates/pcs-service/examples/configs/standalone_polyglot.toml` shows the TOML
  path with one stage. A config-driven chain means one process per stage joined
  by a Parquet sink into a Parquet source.
- **`cargo test --workspace --all-features` soft-skips the chain test** unless
  `build/` is populated, because the default CI job installs no toolchains beyond
  Rust. The `Polyglot Guests` job is where the chain is actually exercised.
