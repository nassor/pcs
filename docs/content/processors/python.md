+++
title = "A Python processor"
description = "componentize-py, a dataclass that is the component, transforms registered while the build snapshots the CPython heap, and the two gotchas that only appear inside the component."
template = "page.html"
weight = 5
aliases = ["/guests/python/"]
+++

# A Python processor

`componentize-py` bundles CPython into the component, so a Python processor is the
fastest of the six to prototype and the slowest per call. The host builds a
fresh wasmtime `Store` for every `run-batch`, so the interpreter re-initialises
each batch. Keep batches large.

Every block below is from `examples/polyglot/stages/python-enrich/`, stage 2 of
the polyglot example. It reads `valid`, `currency` and `amount`, and writes
`usd_amount` with its display string. The stage is a dataclass and two
functions: `pcs_sdk` owns the descriptor, the schema bytes, the fingerprint, the
Arrow decode and re-encode, and the error mapping.

## 1. Install

Requires **Python 3.10 or newer**; CI verifies 3.14.

```bash,name=Install componentize-py
pip install componentize-py==0.25.0
```
Runs the same on Linux, macOS and Windows (PowerShell).

The processor imports `pcs_sdk`, which carries the codec as the internal
`pcs_sdk.arrow_ipc` module. Both are plain Python under `packages/`, and
`componentize` resolves them from the directory the next section names, so
nothing has to be installed to build the component.

## 2. Bindings and build

```bash,name=Write the stubs then build the component
componentize-py -d ../../../../crates/pcs-processor/wit -w pcs-pipeline bindings .
componentize-py -d ../../../../crates/pcs-processor/wit -w pcs-pipeline componentize app \
    -p . -p ../../../../packages/pcs-sdk-py/src -o enrich-py.wasm
```
Windows (PowerShell):

```powershell
componentize-py -d ..\..\..\..\crates\pcs-processor\wit -w pcs-pipeline bindings .
componentize-py -d ..\..\..\..\crates\pcs-processor\wit -w pcs-pipeline componentize app -p . -p ..\..\..\..\packages\pcs-sdk-py\src -o enrich-py.wasm
```

Do **not** pass `--stub-wasi`. The bundled CPython needs the real WASI imports,
which the host supplies through `wasmtime_wasi::p2::add_to_linker_sync`.

`-p` names every directory `componentize` resolves imports from, and it defaults
to `.`, so the SDK's `src` is the one directory named here. Resolution happens
once, during the pre-init snapshot, because the component has no runtime
filesystem.

<div class="note note-warn">
<span class="note-label">bindings output is stubs, nothing more</span>

The `bindings` command writes files for the IDE's benefit only. `componentize`
regenerates the real bindings itself and never reads them from disk; the build
succeeds with them deleted. It is also not idempotent, so a second run fails
with "Cannot create a file when that file already exists". `cargo xtask polyglot`
skips the step entirely for that reason.

</div>

## 3. The component

A `@dataclass` under `@pcs_sdk.component` is the component. The class name is the
wire component name and the field names are the column names, both verbatim: a
component is a cross-language contract, so nothing here renames anything.

```python,name=The dataclass that is the component
from dataclasses import dataclass

import pcs_sdk


@pcs_sdk.component
@dataclass
class Order:
    id: int
    region: str
    currency: str
    amount: float
    valid: bool = False
    usd_amount: float = 0.0
    usd_amount_display: str = ""
    risk_score: float = 0.0
    flagged: bool = False
    fee: float = 0.0
    review_tier: int = 0
    settlement: str = ""
```

`int`, `float`, `bool` and `str` annotations become `Int64`, `Float64`,
`Boolean` and `Utf8` columns, in declaration order. That order is the schema, the
buffer walk and the fingerprint, so reordering the fields is a wire change.

Every refusal, an annotation the SDK cannot map or a field with `init=False`,
is raised while the module is imported. That import is part of the build, so a
component that would encode the wrong schema fails the build instead of the
batch.

<div class="note note-warn">
<span class="note-label">Imports must sit at module top level</span>

**componentize-py resolves imports at build time only.** A function-local
`import` works under plain CPython, then fails inside the component with no
obvious connection to the code that moved. Keep every `import` at module scope,
in this file and in the packages it names.

</div>

## 4. The transforms

`@pcs_sdk.transform(Order)` runs once per row, with a mutable instance of the
dataclass. Whatever it leaves on the row is what gets encoded:

```python,name=The per row enrich transform
@pcs_sdk.transform(Order)
def enrich(row, config):
    """Convert one order into USD, or zero a rejected one."""
    if row.valid:
        row.usd_amount = row.amount * _rate(config, row.currency)
        row.usd_amount_display = f"{row.usd_amount:.2f} USD"
    else:
        row.usd_amount = 0.0
        row.usd_amount_display = ""
```

`usd_amount_display` is a `Utf8` column, and writing it is the reason this stage
re-encodes its segment rather than patching bytes: a different string length
moves the values buffer, the offsets buffer and the entries describing both.

`@pcs_sdk.batch(Order)` runs once, after every per-row transform has seen every
row, with the list of rows. A batch total belongs here, because `run-batch` is
the unit the host measures and a per-row `metric` call would report six rows as
six observations of one:

```python,name=The batch report
@pcs_sdk.batch(Order)
def report(rows, config):
    total = sum(row.usd_amount for row in rows)
    converted = sum(1 for row in rows if row.valid)
    config.metric("enrich.usd_total", total)
    config.log(
        "info",
        "enrich",
        f"converted {converted} of {len(rows)} rows, {total:.2f} USD total",
    )
```

`config` is the whole of `host-io` a transform can reach. `config.float(key,
default)` returns the default when the host injected nothing and raises
`ValueError` for a value that will not parse, and it caches by key, so a per-row
read costs one `get-config` call per batch. Rates come from one key per non-USD
currency:

```python,name=One config key per non-USD currency
_RATES = {
    "EUR": ("fx_eur", 1.10),
    "GBP": ("fx_gbp", 1.30),
    "JPY": ("fx_jpy", 0.0068),
}

#: Reporting currency, and the rate used for any code not listed above.
_IDENTITY_RATE = 1.0


def _rate(config, currency):
    """This batch's rate for a currency: host config, then the fallback."""
    entry = _RATES.get(currency)
    if entry is None:
        return _IDENTITY_RATE
    key, default = entry
    return config.float(key, default)
```

## 5. The export

One module-level name is the whole export. `componentize-py` looks up `Pipeline`,
and `pcs_sdk.processor` returns a class subclassing the generated
`wit_world.exports.Pipeline`, exactly as a hand-written processor would:

```python,name=The module level export
Pipeline = pcs_sdk.processor("polyglot-enrich-py", "0.1.0", enrich, report)
```

Transforms are grouped by the component they were registered against and run in
the order given. The descriptor, the schema bytes and the fingerprint are built
in that call, at import time, which is componentize-py's pre-init pass: the
finished component starts with all of it in memory rather than deriving it on the
first batch.

`run_batch` returns a `RunResult` with `checkpoint=None` and ignores `prior`,
which is what the `stateful: false` in the descriptor promises. Failure is an
`Err` carrying `RunError_Permanent`: a `ValueError` from the codec, the config or
a value no column can hold lands there, and so does everything else, because the
WIT variant has no "unknown" arm and `run-batch` must never emit
`schema-mismatch`. Nothing escapes uncaught, since an exception that reaches the
trampoline becomes a trap and the operator loses the batch with no message.

Segments this processor did not declare, the `__alive` bitmap included, come
straight out of the input. The host replaces the whole partition dataset with
what `run-batch` returns, so a dropped segment is lost data.

## 6. The schema fingerprint

`pipeline-descriptor.schema-fingerprint` is derived, not embedded.
`pcs_sdk.fingerprint` hashes each component's name, its version as four
little-endian bytes, and its field names in declaration order, with FNV-1a, over
the components sorted by name. Names only: adding a field changes the value,
retyping one does not.

Every language's SDK walks those same bytes, so the six polyglot stages report
one value from six independently written declarations. The driver
`examples/polyglot/polyglot_orders.rs` and the `polyglot_chain` integration test
load all six and compare their fingerprints against each other, and exit
non-zero on any disagreement.

## 7. Test, then validate

The SDK runs under plain CPython, where there is no `wit_world`: it falls back to
local stand-ins for the generated records and for `host-io`, so a processor class
is driven end to end on the host exactly as the host drives it inside the
component. `pcs_sdk.LOCAL_HOST` is that stand-in. Wire bytes are real either way,
because both paths go through `pcs_sdk.arrow_ipc`.

Linux/macOS:

```bash,name=Run the SDK test suite
cd packages/pcs-sdk-py && PYTHONPATH=src python -m unittest discover -s tests
```

Windows (PowerShell):

```powershell
cd packages/pcs-sdk-py
$env:PYTHONPATH = "src"
python -m unittest discover -s tests
```

<div class="note note-warn">
<span class="note-label"><code>unittest discover</code> breaks in the stage directory</span>

The `bindings` stubs include a `componentize_py_async_support/` package whose
`__init__` imports `componentize_py_runtime`, which only exists *inside* the
built component. `unittest discover` imports every package under its start
directory and fails on it. That is one reason host-side tests belong outside the
stage.

</div>

```bash,name=Validate the finished component
wasm-tools validate --features component-model enrich-py.wasm
wasm-tools component wit enrich-py.wasm | grep 'pcs:pipeline'
```
Windows (PowerShell):

```powershell
wasm-tools validate --features component-model enrich-py.wasm
wasm-tools component wit enrich-py.wasm | Select-String 'pcs:pipeline'
```

```text,name=Expected wasm-tools output
  import pcs:pipeline/host-io@0.3.0;
  export pcs:pipeline/pipeline@0.3.0;
```

## 8. Run it

`examples/configs/standalone_polyglot.kdl` runs **this exact processor** end to
end under `pcs-service`. Its paths are relative to the repository root:

```bash,name=Build the processors then run the service
cargo xtask polyglot   # builds all six processors
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- validate \
  --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- serve \
  --config examples/configs/standalone_polyglot.kdl
```
Runs the same on Linux, macOS and Windows (PowerShell), each command on one line.

```powershell
cargo xtask polyglot
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- validate --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- serve --config examples/configs/standalone_polyglot.kdl
```

It reads `examples/configs/fixtures/polyglot_orders.csv`, runs the component,
and writes `/tmp/pcs-polyglot-out.csv` with `usd_amount` and
`usd_amount_display` filled in. The `FileSource` and `FileSink` pair declares all
twelve `Order` columns, because the host registers the component from that list.
The fixture seeds `valid` explicitly, because the Go stage that would write it is
not in this pipeline: one `pcs-service` process runs exactly one runtime.

## Where to go next

- [The WIT contract](@/processors/wit-contract.md): every record the descriptor
  fills in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes the codec
  inside `pcs_sdk` implements.
- [Six languages, one pipeline](@/processors/_index.md#six-languages-one-pipeline): this stage in its
  chain.
