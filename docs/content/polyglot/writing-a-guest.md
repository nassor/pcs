+++
title = "Writing a guest in your language"
description = "What a guest actually is, why describe() and run-batch() are the whole contract, and complete recipes — install, bindings, build, verify, and the gotchas that cost an hour — for Rust, Go, Python and JavaScript."
template = "page.html"
+++

# Writing a guest in your language

`pcs-service` does not know Rust. It knows one WIT world: `pcs:pipeline@0.2.0`.
Anything that compiles to that shape — a `describe` and a `run-batch` export, a
`host-io` import — is a guest, whatever wrote it. This page has two jobs:
explain that shape once, in general, and then hand you a complete, verified
recipe per language. Read the first half even if you already know which
language you're using — the four recipes assume you have.

## What a guest actually is

A **guest** is a WebAssembly component: a `.wasm` file built against the
[Component Model](https://component-model.bytecodealliance.org/), targeting
WASI 0.2. Not a Rust artifact — `cargo-component` is one way to produce one,
and Go, Python and JavaScript each have their own. `pcs-service` loads
whichever `.wasm` your config names and never learns, or cares, what built it.

The whole contract is two exported functions and one imported interface:

- **`describe() -> pipeline-descriptor`** — called once, when the guest loads.
  Reports its name, version, the Arrow schema of every component it declares,
  a `schema_fingerprint`, and whether it is stateful.
- **`run-batch(input: ipc-bytes, prior: option<checkpoint>) -> result<run-result, run-error>`**
  — called once per batch. `input` is Arrow IPC bytes; a successful
  `run-result` carries Arrow IPC bytes back, plus metrics and an optional
  `checkpoint`.
- **`host-io`** — the interface the guest *imports*, not exports:
  `get-config`, `metric`, `log`. Three things a sandboxed component cannot do
  for itself.

One detail trips up every language equally: the host builds a **fresh
wasmtime `Store` for every `run-batch` call**. Nothing your guest keeps in a
global or a struct field survives to the next call — only what you put in
`checkpoint`. The host persists that blob verbatim and hands it back as the
next call's `prior`. If your guest needs to remember anything across batches,
that is the only channel available.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 170" role="img" aria-labelledby="gd-title gd-desc">
        <title id="gd-title">The shape of a PCS guest: two calls in, one loop back</title>
        <desc id="gd-desc">
            pcs-service calls describe on the guest once, at load, to learn its component
            schemas and fingerprint. It then calls run-batch once per batch, passing Arrow IPC
            input and the prior checkpoint, and gets back a run-result or a run-error. The
            guest exports exactly those two functions and imports host-io for config, metrics
            and logging. A fresh wasmtime Store backs every run-batch call, so any state the
            guest keeps must round-trip through the checkpoint the host holds and replays as
            the next call's prior.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-ctl" x="0" y="40" width="170" height="56" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="40" width="170" height="20" rx="8"/>
            <rect class="hd hd-ctl" x="0" y="52" width="170" height="8"/>
            <text class="t-lbl" x="12" y="55">pcs-service</text>
            <text class="t-sm" x="12" y="76">wasmtime host</text>
            <text class="t-sm" x="12" y="89">fresh Store per batch</text>
        </g>
        <g class="anim anim-2">
            <rect class="blk blk-bnd" x="450" y="40" width="210" height="72" rx="8"/>
            <rect class="hd hd-bnd" x="450" y="40" width="210" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="450" y="52" width="210" height="8"/>
            <text class="t-lbl" x="462" y="55">your guest &middot; .wasm</text>
            <text class="t-sm t-bnd" x="462" y="76">exports pipeline</text>
            <text class="t-sm t-bnd" x="462" y="89">imports host-io</text>
            <text class="t-sm" x="462" y="102">any language, WASI 0.2</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-ctl t-mid" x="310" y="44">describe() &rarr; descriptor, once at load</text>
            <path class="arw arw-ctl" d="M170 50 H450" marker-end="url(#gd-c)"/>
            <text class="t-sm t-mid" x="310" y="62">run-batch(input, prior)</text>
            <path class="arw arw-data" d="M170 68 H450" marker-end="url(#gd-d)"/>
            <path class="arw arw-data" d="M450 86 H170" marker-end="url(#gd-d)"/>
            <text class="t-sm t-mid" x="310" y="98">run-result | run-error</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M25 96 V130 H105 V96" marker-end="url(#gd-b)"/>
            <text class="t-sm t-bnd t-mid" x="65" y="144">checkpoint &rarr; prior</text>
        </g>
        <defs>
            <marker id="gd-c" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--control)"/>
            </marker>
            <marker id="gd-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data)"/>
            </marker>
            <marker id="gd-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-control"><i></i> describe(), and whatever the guest calls into host-io</span>
        <span class="k-data"><i></i> run-batch: Arrow IPC bytes in, Arrow IPC bytes or an error out</span>
        <span class="k-boundary"><i></i> checkpoint &mdash; the one thing that survives a fresh Store</span>
    </div>
    <figcaption class="dgm-cap">
        <code>describe()</code> runs once, which makes it easy to under-test: get
        <code>schema_fingerprint</code> or the component list wrong and every local
        <code>run-batch</code> still succeeds — it's <code>validate_schema_fingerprint</code> on a
        cluster node that quietly refuses to start. <code>run-batch</code> runs every batch
        against a <b>new</b> <code>Store</code>: nothing survives in a global or a struct field,
        only in <code>checkpoint</code>.
    </figcaption>
</div>

## Before you start

Point every toolchain below at the same WIT package. It is the single
canonical copy — do not vendor it.

```text
crates/pcs-guest/wit/pipeline.wit
```

You'll need `wasm-tools` once, regardless of language:

```bash
cargo install wasm-tools --locked --version 1.246.2
```

All four recipes end the same way, and these two commands are the only proof
that matters:

```bash
wasm-tools validate --features component-model <component>.wasm
wasm-tools component wit <component>.wasm | grep 'pcs:pipeline'
```

The second command must print a world importing `pcs:pipeline/host-io@0.2.0`
and exporting `pcs:pipeline/pipeline@0.2.0`. If it does not, stop — nothing
past this point will work, and the fix is almost always the bindings step, not
the guest code.

<div class="note">
<span class="note-label">If your language has no Arrow library</span>

Go, Python and JavaScript have no WASI-0.2-friendly Arrow IPC library today, so
their recipes below don't show Arrow-parsing code — there's a whole page for
that instead. [The wire format](@/polyglot/wire-format.md) specifies exactly
the bytes `run-batch` receives and must return: segment framing, the
flatbuffer field ids, buffer layouts per type, and what a guest that can only
overwrite fixed-width bytes in place may and may not do. Rust doesn't need it
— `pcs-guest` re-exports the Arrow crates and handles IPC for you.

</div>

Versions below are the ones CI installs. The full pin list, including known
toolchain caveats per language, lives in `examples/polyglot/PINS.md`.

## Which language should I use?

| Language | Needs its own Arrow codec? | Toolchain | Verified runtime | Reach for it when |
|---|---|---|---|---|
| **Rust** | No — `pcs-guest` provides it | `cargo-component` 0.21.1 | Rust 1.95+ | You're already in the workspace, or ceremony and performance both matter |
| **Go** | Yes — hand-rolled, see the wire format | `componentize-go` 0.4.1 | Go 1.25.5+ (CI: 1.26.3) | Your team ships Go already; the transform is field-level logic, not text |
| **Python** | Yes — hand-rolled | `componentize-py` 0.25.0 | Python 3.10+ (CI: 3.14) | Fastest to prototype; keep batches large — CPython re-initialises every call |
| **JavaScript** | Yes — hand-rolled | `jco` 1.30.0 | Node 22+ (CI: 24.19) | Team is JS/TS-native; budget extra time for the gotchas below |

---

## Rust — `cargo-component` 0.21.1

The only language with an SDK. `pcs-guest` re-exports `Dataset`, `Pipeline`,
`System` and the Arrow crates at the workspace-pinned version, and
`export_pipeline!` writes the WIT glue for you.

```bash
cargo install cargo-component --locked --version 0.21.1
rustup target add wasm32-wasip2
```

`Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
pcs-guest      = { path = "../../crates/pcs-guest" }
serde          = { version = "1", features = ["derive"] }
wit-bindgen-rt = { version = "0.44.0", features = ["bitflags"] }

[package.metadata.component]
package = "pcs:my-stage"

[package.metadata.component.target]
path = "../../crates/pcs-guest/wit"
world = "pcs-pipeline"
```

`src/lib.rs`:

```rust
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings;

use pcs_guest::prelude::*;

pub fn build() -> Pipeline {
    Pipeline::builder("my-stage")
        .with::<MyComponent>()
        .with_system(MySystem)
        .build()
}

#[cfg(target_arch = "wasm32")]
pcs_guest::export_pipeline!(build);
```

Add `, state = MyState` to keep one component's rows across batches; the macro
serialises it into `run-result.checkpoint` and restores it from `prior`.

<div class="note note-warn">
<span class="note-label">Gitignore the generated bindings</span>

`src/bindings.rs` is regenerated on every build, and a committed copy silently
desynchronises the moment the WIT changes — gitignore it. One consequence:
because `rustfmt` walks every `mod` declaration regardless of `#[cfg(...)]`,
CI needs that file **on disk** (even though the host build never compiles it)
before `cargo fmt --all -- --check` will pass.

</div>

```bash
cargo component build --release -p my-stage --target wasm32-wasip2
```

<div class="note">
<span class="note-label">Expected</span>

The artifact lands under `target/wasm32-wasip1/release/`, not
`wasm32-wasip2`. `cargo-component` compiles the core module for wasip1 and
adapts it into a component afterward, keeping the pre-adapter directory name.

</div>

---

## Go — `componentize-go` 0.4.1

The Bytecode Alliance's current Go recommendation. Standard Go, not TinyGo —
the TinyGo component tooling page now carries a "not currently being
maintained" banner pointing here. Requires **Go 1.25.5 or newer** (CI
verifies 1.26.3).

```bash
go install github.com/bytecodealliance/componentize-go@v0.4.1
```

<div class="note">
<span class="note-label">componentize-go on Windows</span>

`go install` puts a thin wrapper on `PATH` that downloads the real binary on
first use — and it asks for `componentize-go-windows-amd64.tar.gz`, while the
v0.4.1 release only publishes a `.zip`. The wrapper 404s. Download the `.zip`
from the release page instead and put `componentize-go.exe` on `PATH`
yourself (overwriting the wrapper in `%GOPATH%\bin` is fine). Linux and macOS
are unaffected.

</div>

Generate bindings, then build (global flags come **before** the subcommand):

```bash
componentize-go -d ../../crates/pcs-guest/wit -w pcs-pipeline bindings --format
componentize-go -d ../../crates/pcs-guest/wit -w pcs-pipeline build -o my-stage.wasm
```

<div class="note note-warn">
<span class="note-label">componentize-go owns go.mod</span>

`bindings` **rewrites `go.mod`** to `module wit_component` every time it
runs, so every intra-module import is `wit_component/<pkg>`. Commit `go.mod`
with that module name rather than fighting it — this is why
`examples/polyglot/stages/go-validate/go.mod` reads exactly that way.

</div>

`bindings` writes `wit_exports.go` plus one package per WIT interface, and
expects **you** to supply the export package. Create
`export_pcs_pipeline_pipeline/exports.go`:

```go
package export_pcs_pipeline_pipeline

import (
    witTypes "go.bytecodealliance.org/pkg/wit/types"
    hostio "wit_component/pcs_pipeline_host_io"
    "wit_component/pcs_pipeline_types"
)

func Describe() pcs_pipeline_types.PipelineDescriptor { /* ... */ }

func RunBatch(
    input []uint8,
    prior witTypes.Option[[]uint8],
) witTypes.Result[pcs_pipeline_types.RunResult, pcs_pipeline_types.RunError] {
    // input is an Arrow IPC byte stream — see the wire format for the layout.
    // hostio.GetConfig / hostio.Metric / hostio.Log are available here.
    // On failure:
    //   return witTypes.Err[pcs_pipeline_types.RunResult](
    //       pcs_pipeline_types.MakeRunErrorPermanent(msg))
}
```

Add `--generate-stubs` to have componentize-go write those two panicking
signatures for you the first time.

<div class="note note-warn">
<span class="note-label"><code>go test ./...</code> does not work</span>

The generated packages use `//go:wasmimport`, which does not compile for the
host target, so a bare `go test ./...` fails on packages you didn't write.
Scope host-side tests to the packages that are pure Go, e.g.
`go test ./arrowipc/...`.

</div>

---

## Python — `componentize-py` 0.25.0

Requires **Python 3.10 or newer** (CI verifies 3.14).

```bash
pip install componentize-py==0.25.0
```

```bash
componentize-py -d ../../crates/pcs-guest/wit -w pcs-pipeline bindings .
componentize-py -d ../../crates/pcs-guest/wit -w pcs-pipeline componentize app -o my-stage.wasm
```

`bindings` generates a `wit_world` package. Implement the exported interface
as a class named after it:

```python
from typing import Optional

import wit_world
from wit_world import exports
from wit_world.imports import types as wtypes
from wit_world.imports.host_io import LogLevel, get_config, log, metric
from componentize_py_types import Err


class Pipeline(exports.Pipeline):
    def describe(self) -> wtypes.PipelineDescriptor:
        ...

    def run_batch(self, input: bytes, prior: Optional[bytes]) -> wtypes.RunResult:
        # input is an Arrow IPC byte stream — see the wire format for the layout.
        # On failure:
        #   raise Err(wtypes.RunError_Permanent("..."))
        ...
```

Do **not** pass `--stub-wasi`: the bundled CPython needs the real WASI
imports, which the host supplies through
`wasmtime_wasi::p2::add_to_linker_sync`.

<div class="note note-warn">
<span class="note-label">Imports must sit at module top level</span>

**componentize-py resolves imports at build time only.** A function-local
`import` works fine when you run the file with plain CPython, then fails
inside the component with no obvious connection to the code you just moved —
a confusing way to spend an hour. Keep every `import` at module scope.

</div>

<div class="note note-warn">
<span class="note-label">bindings output is stubs, nothing more</span>

The `bindings` command writes files for your IDE's benefit only —
`componentize` regenerates the real bindings itself and never reads them from
disk; the build succeeds with them deleted. Generate them anyway for type
checking, just don't treat the step as load-bearing. One side effect: those
stubs write a `componentize_py_async_support/` package whose `__init__`
imports `componentize_py_runtime`, which only exists *inside* the built
component. `unittest discover` imports every package under its start
directory and fails once bindings exist on disk — name the module instead:
`python -m unittest test_arrow_ipc`.

</div>

---

## JavaScript — `jco` 1.30.0 on Node 22+

Requires **Node 22 or newer** (CI verifies 24.19).

```bash
npm install --save-dev @bytecodealliance/jco@1.30.0
npx jco types ../../crates/pcs-guest/wit --world-name pcs-pipeline -o types/
```

`score.js`:

```js
import { getConfig, log, metric } from 'pcs:pipeline/host-io@0.2.0';

export const pipeline = {
  describe() {
    return {
      name: 'my-stage',
      version: '0.1.0',
      components: [{ name: 'Order', arrowSchemaIpc: SCHEMA_BYTES }],
      stateful: false,
      schemaFingerprint: FINGERPRINT,
    };
  },

  runBatch(input, prior) {
    // input is an Arrow IPC byte stream — see the wire format for the layout.
    try {
      // ...
      return {
        output,
        checkpoint: undefined,
        metrics: {
          wallNs: BigInt(ns), rowsIn: BigInt(rows), rowsOut: BigInt(rows),
          systemsRun: 1, retries: 0,
        },
      };
    } catch (err) {
      throw { tag: 'permanent', val: String(err) };
    }
  },
};
```

```bash
npx jco componentize score.js \
    --wit ../../crates/pcs-guest/wit \
    --world-name pcs-pipeline \
    --disable http \
    --disable fetch-event \
    --bundle \
    -o score-js.wasm
```

JavaScript has the most gotchas of the four — four groups of them:

<div class="note note-warn">
<span class="note-label">Module, bundling, and the versioned import</span>

`"type": "module"` in `package.json` is mandatory — jco only consumes ES
modules. The host-io import specifier must carry its version,
`'pcs:pipeline/host-io@0.2.0'`, not the unversioned form, which fails at
wizer time with `ReferenceError: Error loading module "pcs:pipeline/host-io"
... No such file or directory`. And `--bundle` becomes mandatory the moment
you `import` a second file of your own — StarlingMonkey's loader cannot
resolve relative modules at wizer time, so `import './arrow-ipc.js'` fails
with `Error loading module "./arrow-ipc.js"` unless the source is bundled
first.

</div>

<div class="note note-warn">
<span class="note-label">Don't just <code>--disable http</code></span>

`--disable http` alone is not enough to drop `wasi:http` — pair it with
`--disable fetch-event`, or the component still imports
`wasi:http/types@0.2.x` and fails to instantiate against a host that links
plain WASI. Going the other direction, do **not** disable `clocks`:
`Date.now()` silently returns garbage and `run-metrics.wall-ns` becomes
fiction.

</div>

<div class="note note-warn">
<span class="note-label">Values don't cross the boundary as themselves</span>

`wallNs`, `rowsIn` and `rowsOut` are `BigInt`, not `Number`. And
`list<u8>` arrives from a different realm: componentize-js lifts it into a
`Uint8Array` whose prototype is not yours, so `input instanceof Uint8Array`
is `false` and `input.constructor !== Uint8Array` — while
`constructor.name` is still `'Uint8Array'`. Use a realm-agnostic check
instead: `ArrayBuffer.isView(x) && x.BYTES_PER_ELEMENT === 1`.

</div>

<div class="note note-warn">
<span class="note-label">The error path is not what you'd guess</span>

componentize-js lowers a *thrown* value into the WIT `err` arm, but it
re-throws anything that is an `instanceof Error` instead of lowering it.
Throw a plain object — `{ tag: 'permanent', val: msg }` — because throwing
`new Error(msg)` traps the guest instead of returning a `run-error`.

</div>

---

## When your language has a real Arrow library, use it

The polyglot example hand-rolls its Arrow IPC codec in three languages. That
is a deliberate constraint for an example — it makes each stage depend on
nothing but its language's standard library, which is what let us document
the format at all. It is not a recommendation.

If you are writing a production guest, reach for the real binding first:
[`arrow-go`](https://github.com/apache/arrow-go),
[`apache-arrow`](https://www.npmjs.com/package/apache-arrow) on npm, or
[`pyarrow`](https://arrow.apache.org/docs/python/). The caveat is that each one
has to survive its own componentization toolchain — `arrow-go` is documented as
incompatible with TinyGo, `pyarrow` has no `wasm32-wasi` wheel, and
`apache-arrow` under StarlingMonkey is unproven here. Check that before you
commit to it, and if it works, you get schema evolution and variable-length
writes for free.

## Where to go next

- [The wire format](@/polyglot/wire-format.md) — the byte-level spec every
  hand-rolled codec above is built against.
- [Pipelines in any language](@/polyglot/_index.md) — the worked example these
  four recipes are recipes *for*: one `Order` schema, four stages, chained
  through the same host `pcs-service` uses.
- [Operating pcs-service](@/operations/running-pcs.md) — once your `.wasm` is
  built, this is how it actually gets deployed.
