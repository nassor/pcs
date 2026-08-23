# pcs-arrow-ipc

Arrow IPC codec for [PCS](https://github.com/nassor/pcs) WebAssembly guests.
Standard library only: `pyarrow` has no `wasm32-wasi` wheel, and
`componentize-py` bundles whatever it can import at snapshot time, so a guest
that needs to read a batch needs a pure-Python decoder.

The bytes it reads are specified in
[the wire format reference](https://nassor.github.io/pcs/reference/wire-format/).

## Install

```bash
pip install pcs_arrow_ipc-0.1.0-py3-none-any.whl
```

Building a component with `componentize-py` resolves imports at snapshot time
from the directories named by `-p`, so name the install location there:

```bash
componentize-py -d <wit-dir> -w pcs-pipeline componentize app \
    -p . -p <site-packages-or-src> -o guest.wasm
```

## API

```python
from pcs_arrow_ipc import PcsStream, decode_base64

stream = PcsStream(input_bytes)          # owns a mutable copy
stream.component_names                    # ['Order', '__alive']
batch = stream.component("Order")
batch.rows                                # row count
batch.field_names                         # schema order
batch.int64s("id")                        # list[int]
batch.float64s("amount")                  # list[float]
batch.bools("valid")                      # list[bool]
batch.strings("region")                   # list[str]
batch.set_int64("review_tier", 0, 2)      # in place
batch.set_float64("fee", 0, 1.5)
batch.set_bool("valid", 0, True)
stream.to_bytes()                         # hand back to the host

decode_base64("...")                      # a generated schema constant
```

`set_*` write fixed-width value slots in place. A `Utf8` column cannot be
written: changing a string resizes the values buffer and invalidates both the
offsets buffer and the RecordBatch flatbuffer that describes them, which needs a
real Arrow writer. Everything the guest does not touch, framing and flatbuffers
included, is returned byte-identical.

## Tests

The suite decodes the PCS emitter's fixtures, so generate them first:

```bash
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit
PYTHONPATH=src python -m unittest discover -s tests
```

`tests/test_conformance.py` additionally runs the shared corpus in
`../arrow-ipc-conformance`, which every codec in `packages/` runs so all of them
refuse exactly the same streams. The corpus is committed, so nothing needs
generating for it, and a missing manifest or vector fails rather than skips: a
conformance suite that quietly runs zero cases still reports green.

## License

Apache-2.0.
