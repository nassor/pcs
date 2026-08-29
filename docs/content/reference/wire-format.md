+++
title = "Host to processor wire format"
description = "The exact bytes crossing the Component Model boundary: segment framing, Arrow IPC message framing, the flatbuffer field ids a processor must read, buffer layout per Arrow type, and the schema fingerprint algorithm."
template = "page.html"
weight = 1
aliases = ["/polyglot/wire-format/"]
+++

# Host ↔ processor wire format

Read this only if you are writing an Arrow codec by hand. Five languages already
have one, carried inside [`the SDK packages`](@/reference/arrow-ipc-packages.md).
The implementation on both sides is `crates/pcs-core/src/dataset/ipc.rs`, called
identically by the host and by the `export_pipeline!` expansion in the processor.

`run-batch` takes `list<u8>` and returns `list<u8>`. This page specifies
those bytes precisely enough to implement a processor in a language with no
Arrow library, as the Go, Python, TypeScript, Kotlin and C# stages of
[the polyglot example](@/processors/_index.md#six-languages-one-pipeline) do.

The numbers here match the stream that
`cargo run -p pcs-service --features wasm --example polyglot_schema_emit -- emit`
writes to `examples/polyglot/generated/fixture_input.pcs`.

---

## Segment framing

The payload is **not** a single Arrow IPC stream. It is a sequence of
length-prefixed streams with a zero terminator:

```text,name=The segment framing grammar
pcs_stream := segment* terminator
segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
terminator := u32le 0x00000000
```

One segment per registered component, ordered by component name, then a final
`__alive` segment carrying the liveness bitmap as a single `Boolean` column named
`alive`, then the terminator.

Each `arrow_ipc_stream` is a complete standalone Arrow IPC **stream** as produced
by arrow-rs `StreamWriter` with `IpcWriteOptions::default()`: MetadataVersion V5,
8-byte alignment, no compression. It contains exactly one Schema message, one
RecordBatch message, and an end-of-stream marker.

Each segment's Schema carries custom metadata:

| key | value |
|-----|-------|
| `__pcs_component` | the component name, or `__alive` for the bitmap segment |
| `__pcs_schema_version` | decimal `u32`; absent on the `__alive` segment |

A segment with no `__pcs_component` key is fatal on the host side. So is a stream
where any component's row count exceeds the `__alive` length (a component may
hold *fewer* rows, a windowing processor's reduced result component), or one
with more than one `__alive` segment.

## Arrow IPC message framing

Within a segment, each message is:

```text,name=The Arrow IPC message framing
message := 0xFFFFFFFF (u32le continuation)
        ++ u32le metadata_len
        ++ flatbuffer[metadata_len]
        ++ body[bodyLength]
```

- `metadata_len` **already includes** the flatbuffer's padding to an 8-byte
  boundary.
- the body starts at `msg_start + 8 + metadata_len`.
- the next message starts at `body_start + align8(bodyLength)`.
- end-of-stream is `0xFFFFFFFF` followed by `u32le 0`.

`Buffer.offset` values inside a RecordBatch are relative to that message's body
start.

## Reading flatbuffers by hand

Enough of the format to read these two message types:

- The buffer starts with a `uoffset32` pointing at the root table.
- A table at absolute position `t` starts with a **signed** `soffset32`; its
  vtable is at `t - soffset`.
- A vtable is `u16 vtable_len`, `u16 table_len`, then one `u16` per field id. The
  field is **absent** if its offset is `0`, or if its field id is at or beyond
  `vtable_len`.
- A present field's value lives at `t + field_offset`.
- Strings, vectors and sub-tables are referenced by a `uoffset32` stored in that
  slot; the target is at `slot_position + uoffset_value`.
- A string is `u32 byte_len` followed by the bytes.
- A vector is `u32 count` followed by `count` elements: 4 bytes each for offsets
  to tables/strings, `sizeof(struct)` for inline structs.

## Field ids

A union in a flatbuffer schema occupies two vtable slots, a discriminant and a
value, which is where the gaps in this table come from.

| Table | field | id | type / notes |
|-------|-------|----|--------------|
| `Message` | `version` | 0 | i16; V5 is the value `4` |
| `Message` | `header_type` | 1 | u8: 1 = Schema, 2 = DictionaryBatch, 3 = RecordBatch |
| `Message` | `header` | 2 | uoffset to the header table |
| `Message` | `bodyLength` | 3 | i64 |
| `Schema` | `endianness` | 0 | i16, 0 = little |
| `Schema` | `fields` | 1 | vector of `Field` tables |
| `Schema` | `custom_metadata` | 2 | vector of `KeyValue` tables |
| `Field` | `name` | 0 | string |
| `Field` | `nullable` | 1 | bool |
| `Field` | `type_type` | 2 | u8 union discriminant |
| `RecordBatch` | `length` | 0 | i64 row count |
| `RecordBatch` | `nodes` | 1 | vector of inline `FieldNode { i64 length, i64 null_count }`, 16 B each |
| `RecordBatch` | `buffers` | 2 | vector of inline `Buffer { i64 offset, i64 length }`, 16 B each |
| `RecordBatch` | `compression` | 3 | uoffset; **must be absent**; reject the batch if present |
| `KeyValue` | `key` / `value` | 0 / 1 | string / string |

`type_type` values a PCS processor needs: **2 = Int, 3 = FloatingPoint, 5 = Utf8,
6 = Bool**. Cross-check against Arrow's [`Message.fbs`][msg] and
[`Schema.fbs`][sch] if you extend the set.

[msg]: https://github.com/apache/arrow/blob/main/format/Message.fbs
[sch]: https://github.com/apache/arrow/blob/main/format/Schema.fbs

## Buffer slots per Arrow type

Walk the schema's fields in order, accumulating a buffer index. The node index
equals the field index; the buffer index does not, because the slot count varies
by type.

| Arrow type | slots | meaning |
|------------|-------|---------|
| `Int` | 2 | validity, values |
| `FloatingPoint` | 2 | validity, values |
| `Bool` | 2 | validity, values |
| `Utf8` | 3 | validity, i32 offsets, values |

**arrow-rs emits the validity slot even when the field is non-nullable and the
null count is zero**, with a real, non-zero length (`ceil(n/8)` bytes, all bits
set). The slot count is therefore fixed by `type_type`, never inferred from
lengths. The eleven-field `Order` schema,
`Int, Utf8, Utf8, Float, Bool, Float, Float, Bool, Float, Int, Utf8`, consumes
exactly 25 buffer slots; after the walk, the accumulated index must equal the
`buffers` vector length, or the batch is malformed.

## Value layouts

| type | layout |
|------|--------|
| `Int64` | 8 bytes LE per row |
| `Float64` | IEEE-754, 8 bytes LE per row |
| `Boolean` | bit-packed LSB-first: row `i` is bit `i & 7` of byte `i >> 3`; `ceil(n/8)` bytes |
| `Utf8` | `n+1` i32 LE offsets into the values buffer; row `i` is `values[offsets[i]..offsets[i+1]]`, UTF-8 |

## What a byte-mutating processor may and may not do

A processor with no Arrow writer can still be a first-class stage, as long as it
never changes any length. The pattern the five non-Rust stages use:

1. split the input into segments,
2. find the segment whose Schema metadata `__pcs_component` matches the component
   it cares about,
3. parse that segment's Schema message (field names + `type_type`) and its
   RecordBatch message (`length`, `buffers`),
4. read the columns it needs, then overwrite fixed-width value bytes **in place**
   in the body,
5. return the input byte array, mutated. Every other byte passes through
   untouched: the `__alive` segment, both flatbuffers, the framing.

Writing a variable-length column this way is not possible: a different
string length changes the offsets buffer, the values buffer length, and the
`Buffer` entries in the RecordBatch flatbuffer. A processor that must write
`Utf8` needs a real RecordBatch-message *writer*; the field-id table above
is sufficient to build one.

## Schema fingerprint

`pipeline-descriptor.schema-fingerprint` is a `string`:
`format!("{:08x}", fnv1a32)`, lowercase and zero-padded to 8 characters. FNV-1a
32-bit, offset basis `2166136261`, prime `16777619`, all arithmetic mod 2^32:

```text,name=The fingerprint hash step by step
hash := 2166136261
for each component, sorted by name:
    for each byte of the component name:      hash = (hash XOR byte) * 16777619
    for each of the 4 little-endian bytes of the schema version:
                                              hash = (hash XOR byte) * 16777619
    for each field, in schema order:
        for each byte of the field name:      hash = (hash XOR byte) * 16777619
return lowercase_hex_8(hash)
```

Names, versions and field names only: no types, no nullability. Adding a field
changes it; changing a field's type does not.

Non-Rust processors should not reimplement this. The polyglot example
generates the value from the canonical Rust schema and embeds it as a
constant, and both the driver and the integration test fail if a
processor's reported fingerprint disagrees with the live one.

## `component-descriptor.arrow-schema-ipc`

A *schema-only* Arrow IPC stream: a `StreamWriter` opened on the schema and
immediately finished, with no batches. The host parses it with
`StreamReader::schema()` and uses it to build the template dataset that sources
and sinks are validated against. Same generated-constant treatment as the
fingerprint.

## The conformance corpus

A hand implementation has an acceptance suite the day it starts:
`packages/arrow-ipc-conformance/` lists, in `manifest.json`, one binary stream
per `vectors/*.pcs` case and what each must do. The five SDK codecs all run it,
so one answer covers which streams are valid. Regenerate it after any wire
format or `Order` schema change:

```bash,name=Regenerate the conformance corpus
cargo run -p pcs-service --features conformance --example conformance_vectors -- emit
```

The corpus is processor-side: it deliberately has no vector for the two rules
the host enforces, the `__alive` row-count cross-check and 8-byte buffer
alignment. It is covered with the codecs on
[the SDK packages page](@/reference/arrow-ipc-packages.md).

## Error mapping

`run-error` and what the host does with each variant are on
[the WIT contract](@/processors/wit-contract.md).
