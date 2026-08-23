# Pcs.ArrowIpc

Arrow IPC codec for [PCS](https://github.com/nassor/pcs) WebAssembly guests.
Base class library only, no NuGet dependencies, no reflection, so it survives the
`PublishTrimmed` NativeAOT-LLVM publish that componentize-dotnet performs.

The bytes it reads are specified in
[the wire format reference](https://nassor.github.io/pcs/reference/wire-format/).

## Install

```bash
dotnet nuget add source <download-dir> -n pcs-local
dotnet add package Pcs.ArrowIpc --version 0.1.0
```

This is a plain `net10.0` assembly. Only the component project carries
`RuntimeIdentifier=wasi-wasm`, `SelfContained` and `PublishTrimmed`; referencing
a host-targeted library from it is fine, and the codec needs no trimmer roots.

## API

```csharp
using Pcs.ArrowIpc;

PcsStream stream = new(input);              // takes ownership, no copy
stream.ComponentNames();                    // ["Order", "__alive"]
ArrowBatch batch = stream.Component("Order");
batch.Rows;                                 // row count
batch.FieldNames();                         // schema order
batch.Int64s("id");                         // long[]
batch.Float64s("amount");                   // double[]
batch.Bools("valid");                       // bool[]
batch.Strings("region");                    // string[]
batch.SetInt64("review_tier", 0, 2);        // in place
batch.SetFloat64("fee", 0, 1.5);
batch.SetBool("valid", 0, true);
stream.Buffer;                              // hand back to the host

ArrowIpc.DecodeBase64("...");               // a generated schema constant
```

`Set*` write fixed-width value slots in place. A `Utf8` column cannot be
written: changing a string resizes the values buffer and invalidates both the
offsets buffer and the RecordBatch flatbuffer that describes them, which needs a
real Arrow writer. Everything the guest does not touch, framing and flatbuffers
included, is returned byte-identical.

Every failure is `ArrowIpcException`, never an `IndexOutOfRangeException`: an
escaping runtime exception traps a component instead of reporting a reason.

## Tests

The suite decodes the PCS emitter's fixtures, so generate them first:

```bash
cargo run -p pcs-service --features wasm --example polyglot_orders -- emit
dotnet test tests
```

## License

Apache-2.0.
