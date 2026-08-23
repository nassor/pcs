+++
title = "A Python guest"
description = "componentize-py, the exports class, the three gotchas that only appear inside the component, and a config that runs the real guest end to end."
template = "page.html"
weight = 4
+++

# A Python guest

`componentize-py` bundles CPython into the component, so a Python guest is the
fastest of the six to prototype and the slowest per call. The host builds a
fresh wasmtime `Store` for every `run-batch`, so the interpreter re-initialises
each batch. Keep batches large.

Every block below is from `examples/polyglot/stages/python-enrich/`, stage 2 of
the polyglot example. It reads `valid`, `currency` and `amount`, and writes
`usd_amount`.

## 1. Install

Requires **Python 3.10 or newer**; CI verifies 3.14.

```bash
pip install componentize-py==0.25.0
```

## 2. Bindings and build

```bash
componentize-py -d ../../../../crates/pcs-guest/wit -w pcs-pipeline bindings .
componentize-py -d ../../../../crates/pcs-guest/wit -w pcs-pipeline componentize app \
    -p . -p ../../../../packages/arrow-ipc-py/src -o enrich-py.wasm
```

Do **not** pass `--stub-wasi`. The bundled CPython needs the real WASI imports,
which the host supplies through `wasmtime_wasi::p2::add_to_linker_sync`.

`-p` names every directory `componentize` resolves imports from, and it defaults
to `.`, so naming the codec means naming both. Resolution happens once, during
the pre-init snapshot, because the component has no runtime filesystem.

<div class="note note-warn">
<span class="note-label">bindings output is stubs, nothing more</span>

The `bindings` command writes files for the IDE's benefit only. `componentize`
regenerates the real bindings itself and never reads them from disk; the build
succeeds with them deleted. It is also not idempotent, so a second run fails
with "Cannot create a file when that file already exists". `scripts/build-polyglot.sh`
skips the step entirely for that reason.

</div>

## 3. The exports class

`bindings` generates a `wit_world` package. Implement the exported interface as
a class named after it. The full file is
`examples/polyglot/stages/python-enrich/app.py`.

```python
import time
from typing import Optional

from componentize_py_types import Err
from wit_world import exports
from wit_world.imports import types as wtypes
from wit_world.imports.host_io import LogLevel, get_config, log, metric

import pcs_arrow_ipc as arrow_ipc
from schema_gen import ORDER_FINGERPRINT, ORDER_SCHEMA_IPC_BASE64
```

<div class="note note-warn">
<span class="note-label">Imports must sit at module top level</span>

**componentize-py resolves imports at build time only.** A function-local
`import` works under plain CPython, then fails inside the component with no
obvious connection to the code that moved. Keep every `import` at module scope,
in this file and in the codec.

</div>

`describe` returns the generated schema bytes and fingerprint, decoded once at
module load because the descriptor is identical every call:

```python
_ORDER_SCHEMA_IPC = arrow_ipc.decode_base64(ORDER_SCHEMA_IPC_BASE64)


class Pipeline(exports.Pipeline):
    """The `pcs:pipeline/pipeline` export."""

    def describe(self) -> wtypes.PipelineDescriptor:
        return wtypes.PipelineDescriptor(
            name=_NAME,
            version=_VERSION,
            components=[
                wtypes.ComponentDescriptor(
                    name=_COMPONENT,
                    arrow_schema_ipc=_ORDER_SCHEMA_IPC,
                )
            ],
            stateful=False,
            schema_fingerprint=ORDER_FINGERPRINT,
        )
```

`run_batch` returns a `RunResult` directly and signals failure by raising `Err`
with a `RunError_*` variant:

```python
    def run_batch(self, input: bytes, prior: Optional[bytes]) -> wtypes.RunResult:
        started = time.monotonic_ns()
        try:
            stream = arrow_ipc.PcsStream(input)
            orders = stream.component(_COMPONENT)
            rows = orders.rows

            valid = orders.bools("valid")
            currency = orders.strings("currency")
            amount = orders.float64s("amount")
            rates = _rate_table()

            total = 0.0
            for row in range(rows):
                if valid[row]:
                    usd = amount[row] * rates.get(currency[row], _IDENTITY_RATE)
                else:
                    usd = 0.0
                orders.set_float64("usd_amount", row, usd)
                total += usd

            metric("enrich.usd_total", total)
            log(
                LogLevel.INFO,
                "enrich",
                "converted {} of {} rows, {:.2f} USD total".format(
                    sum(valid), rows, total
                ),
            )

            return wtypes.RunResult(
                output=stream.to_bytes(),
                # Stateless: nothing to carry, so `prior` is ignored and the
                # host stores no checkpoint for this stage.
                checkpoint=None,
                metrics=wtypes.RunMetrics(
                    wall_ns=time.monotonic_ns() - started,
                    rows_in=rows,
                    rows_out=rows,
                    systems_run=1,
                    retries=0,
                ),
            )
        except ValueError as exc:
            # Malformed input or unusable config: replaying cannot help.
            raise Err(wtypes.RunError_Permanent("{}: {}".format(_NAME, exc))) from exc
        except Exception as exc:
            # The WIT variant has no "unknown" arm, and `run-batch` must never
            # emit `schema-mismatch`, so everything else collapses to permanent.
            raise Err(
                wtypes.RunError_Permanent(
                    "{}: unexpected {}: {}".format(_NAME, type(exc).__name__, exc)
                )
            ) from exc
```

The bare `except Exception` is deliberate. Anything that escapes `run_batch`
without an `Err` becomes a trap, and the operator loses the batch with no
message.

Rates come from `get-config`, one key per non-USD currency, and an unparseable
value fails the batch instead of silently defaulting:

```python
def _config_float(key, default):
    """Read a numeric config value, rejecting one that will not parse."""
    raw = get_config(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError("config {!r} is not a number: {!r}".format(key, raw)) from None
```

## 4. The Arrow codec

`pyarrow` has no `wasm32-wasi` wheel, so the stage depends on a pure-Python
codec instead. `pcs-arrow-ipc` is 512 lines of standard library Python: segment
splitting, the flatbuffer reads, typed column readers, and in-place setters for
fixed-width fields.

```bash
pip install pcs_arrow_ipc-0.1.0-py3-none-any.whl
```

The install directory then goes on `componentize`'s `-p` list, as above.
Alternatively, write your own against
[the wire format](@/reference/wire-format.md).

`stream.to_bytes()` returns the input buffer mutated, which is why this stage
can write `usd_amount`, a `Float64`, and could not write a `Utf8` column.

## 5. Test, then validate

The codec's suite lives with the codec:

```bash
cd packages/arrow-ipc-py && PYTHONPATH=src python -m unittest discover -s tests
```

<div class="note note-warn">
<span class="note-label"><code>unittest discover</code> breaks in the stage directory</span>

The `bindings` stubs include a `componentize_py_async_support/` package whose
`__init__` imports `componentize_py_runtime`, which only exists *inside* the
built component. `unittest discover` imports every package under its start
directory and fails on it. That is one reason host-side tests belong outside the
stage.

</div>

```bash
wasm-tools validate --features component-model enrich-py.wasm
wasm-tools component wit enrich-py.wasm | grep 'pcs:pipeline'
```

```text
  import pcs:pipeline/host-io@0.2.0;
  export pcs:pipeline/pipeline@0.2.0;
```

## 6. Run it

`crates/pcs-service/examples/configs/standalone_polyglot.toml` runs **this exact
guest** end to end under `pcs-service`. Its paths are relative to
`crates/pcs-service`:

```bash
cd crates/pcs-service
bash ../../scripts/build-polyglot.sh                # builds all six guests
cargo run --features service,wasm --bin pcs-service -- validate \
  --config examples/configs/standalone_polyglot.toml --strict
cargo run --features service,wasm --bin pcs-service -- serve \
  --config examples/configs/standalone_polyglot.toml
```

It reads `examples/configs/fixtures/polyglot_orders.csv`, runs the component,
and writes `/tmp/pcs-polyglot-out.csv` with `usd_amount` filled in. The fixture
seeds `valid` explicitly, because the Go stage that would write it is not in
this pipeline: one `pcs-service` process runs exactly one runtime.

## Where to go next

- [The WIT contract](@/guests/wit-contract.md): every record `describe` fills
  in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes `pcs-arrow-ipc`
  implements.
- [Six languages, one pipeline](@/guests/six-languages.md): this stage in its
  chain.
