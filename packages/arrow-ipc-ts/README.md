# @nassor/pcs-arrow-ipc

Arrow IPC codec for [PCS](https://github.com/nassor/pcs) WebAssembly guests. No
dependencies: it reads Arrow flatbuffers with `DataView` and mutates fixed-width
value slots in place, so it runs unchanged under Node and under
StarlingMonkey, the engine `jco componentize` bundles.

`apache-arrow` pulls in Node built-ins StarlingMonkey does not provide, which is
why a guest needs this instead.

The bytes it reads are specified in
[the wire format reference](https://nassor.github.io/pcs/reference/wire-format/).

## Install

```bash
npm install @nassor/pcs-arrow-ipc
```

`jco componentize` bundles with Rolldown under `platform: "neutral"`, where
`resolve.mainFields` is empty. The package ships an `exports` map and compiled
`dist/*.js`, so a bare specifier import resolves without extra bundler
configuration.

## API

```ts
import { PcsStream, decodeBase64 } from '@nassor/pcs-arrow-ipc';

const stream = new PcsStream(input);   // owns a mutable copy
stream.componentNames();               // ['Order', '__alive']
const batch = stream.component('Order');
batch.rows;                            // row count
batch.fieldNames();                    // schema order
batch.int64s('id');                    // bigint[]
batch.float64s('amount');              // number[]
batch.bools('valid');                  // boolean[]
batch.strings('region');               // string[]
batch.setInt64('review_tier', 0, 2n);  // in place
batch.setFloat64('fee', 0, 1.5);
batch.setBool('valid', 0, true);
stream.toBytes();                      // hand back to the host

decodeBase64('...');                   // a generated schema constant
```

`set*` write fixed-width value slots in place. A `Utf8` column cannot be
written: changing a string resizes the values buffer and invalidates both the
offsets buffer and the RecordBatch flatbuffer that describes them, which needs a
real Arrow writer. Everything the guest does not touch, framing and flatbuffers
included, is returned byte-identical.

Every failure is `ArrowIpcError`, exported alongside `PcsStream`. It extends
`Error`, so a `catch` that already handles `Error` keeps working, but catching
it by name is what separates a malformed stream from a bug in the codec:
`instanceof Error` cannot draw that line, because `RangeError` and `TypeError`
satisfy it too. No native error escapes; a bad length is refused with a reason
rather than surfacing as a failed allocation somewhere deeper.

## Tests

The suite decodes the PCS emitter's fixtures, so generate them first:

```bash
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit
npm ci && npm run typecheck && npm run build && npm test
```

## License

Apache-2.0.
