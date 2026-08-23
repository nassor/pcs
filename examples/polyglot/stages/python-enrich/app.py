"""Stage 2 of the polyglot example: the **Python** guest.

Reads `valid` (written by the Go stage), `currency` and `amount`, and writes
`usd_amount`:

    usd_amount = amount * rate(currency)   when valid
    usd_amount = 0.0                       otherwise

Rejected rows are zeroed rather than converted, so a row the validator dropped
carries no misleading money downstream.

Exchange rates come from `pipeline.wasm.config` via `get-config`, falling back
to the constants below when a key is absent. `USD` is the reporting currency
and is never configurable. An unrecognised code converts 1:1, the conservative
choice: it keeps the row visible to the risk stage instead of silently
collapsing it to zero.

This stage is stateless: `prior` is ignored and `checkpoint` is `none`. The
host creates a fresh wasmtime `Store` per `run-batch`, so the bundled CPython
re-initialises every batch. That cost is why a hot inner loop belongs in the
Rust or Go stage rather than here.

Build (from this directory):

    componentize-py -d ../../../../crates/pcs-guest/wit -w pcs-pipeline bindings .
    componentize-py -d ../../../../crates/pcs-guest/wit -w pcs-pipeline componentize app \
        -p . -p ../../../../packages/arrow-ipc-py/src -o enrich-py.wasm

No `--stub-wasi`: the bundled CPython needs the real WASI imports, which the
host supplies through `wasmtime_wasi::p2::add_to_linker_sync`.

componentize-py resolves imports at build time only, and from the directories
named by `-p`, which is why the codec's `src` is on that list. Every import in
this file is module level: a function-local import is a runtime `ImportError`
inside the component, not a build failure.
"""

import time
from typing import Optional

from componentize_py_types import Err
from wit_world import exports
from wit_world.imports import types as wtypes
from wit_world.imports.host_io import LogLevel, get_config, log, metric

import pcs_arrow_ipc as arrow_ipc
from schema_gen import ORDER_FINGERPRINT, ORDER_SCHEMA_IPC_BASE64

_NAME = "polyglot-enrich-py"
_VERSION = "0.1.0"
_COMPONENT = "Order"

#: Config key and fallback rate per currency the host may override.
_RATES = {
    "EUR": ("fx_eur", 1.10),
    "GBP": ("fx_gbp", 1.30),
    "JPY": ("fx_jpy", 0.0068),
}

#: Reporting currency, and the rate used for any code not listed above.
_IDENTITY_RATE = 1.0

#: Decoded once at module load: the descriptor bytes are the same every call.
_ORDER_SCHEMA_IPC = arrow_ipc.decode_base64(ORDER_SCHEMA_IPC_BASE64)


def _config_float(key, default):
    """Read a numeric config value, rejecting one that will not parse."""
    raw = get_config(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError("config {!r} is not a number: {!r}".format(key, raw)) from None


def _rate_table():
    """Currency -> rate for this batch, resolved against host config."""
    table = {"USD": _IDENTITY_RATE}
    for code, (key, default) in _RATES.items():
        table[code] = _config_float(key, default)
    return table


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
