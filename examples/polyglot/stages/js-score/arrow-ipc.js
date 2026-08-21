// Stage 3 of the polyglot example — the JavaScript side of the PCS wire format.
//
// A PCS guest never *writes* an Arrow flatbuffer. It locates the `Order`
// segment, reads the columns it needs, overwrites fixed-width value bytes in
// place, and hands the same array back. Both flatbuffers, the `__alive` segment
// and the stream framing pass through untouched, so this file needs a
// flatbuffer *reader* only — and therefore nothing beyond the JavaScript
// standard library.
//
// The `__alive` bitmap is deliberately ignored: liveness is the host's
// bookkeeping, and every row the host hands us is a row to score.
//
// Framing (verified byte-for-byte against
// `examples/polyglot/generated/fixture_input.pcs`):
//
//   pcs_stream := segment* terminator
//   segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
//   terminator := u32le 0x00000000
//   message    := u32le 0xffffffff ++ u32le metadata_len
//              ++ flatbuffer[metadata_len] ++ body[body_length]
//
// `metadata_len` already covers the flatbuffer's padding to 8 bytes; the body
// starts right after it and the next message starts at
// `body_start + align8(body_length)`.
//
// Every malformed-input path throws a plain `Error` with a precise message.
// This is the codec's own API; `score.js` is what maps a throw onto the WIT
// `run-error::permanent` arm, because a guest must never trap.

/** Arrow IPC message prefix; also the first word of the end-of-stream marker. */
const CONTINUATION = 0xffffffff;

/** `Message.header_type` discriminants (Arrow `Message.fbs`). */
const HEADER_SCHEMA = 1;
const HEADER_RECORD_BATCH = 3;

/** `Field.type_type` union discriminants (Arrow `Schema.fbs`). */
const TYPE_INT = 2;
const TYPE_FLOAT = 3;
const TYPE_UTF8 = 5;
const TYPE_BOOL = 6;

// Buffer slots a field consumes in `RecordBatch.buffers`, walked in schema
// order. Fixed by the Arrow type, never inferred from nullability or from a
// buffer's length: arrow-rs emits the validity slot with `ceil(n/8)` bytes of
// set bits even when the field is non-nullable.
const BUFFER_SLOTS = new Map([
  [TYPE_INT, 2], // validity, values
  [TYPE_FLOAT, 2], // validity, values
  [TYPE_BOOL, 2], // validity, values
  [TYPE_UTF8, 3], // validity, i32 offsets, values
]);

/** Arrow type names, for error messages only. */
const TYPE_NAMES = new Map([
  [TYPE_INT, 'Int'],
  [TYPE_FLOAT, 'FloatingPoint'],
  [TYPE_UTF8, 'Utf8'],
  [TYPE_BOOL, 'Bool'],
]);

/** Schema custom-metadata key naming the component a segment carries. */
const COMPONENT_KEY = '__pcs_component';

/** Bytes per row of a fixed-width 64-bit column. */
const WIDTH_64 = 8;

// `fatal` turns invalid UTF-8 into a throw instead of replacement characters:
// a mis-parsed offsets buffer should fail loudly, not yield mojibake.
const UTF8 = new TextDecoder('utf-8', { fatal: true });

/** Round `n` up to the next multiple of 8 without 32-bit bitwise truncation. */
function align8(n) {
  const rem = n % 8;
  return rem === 0 ? n : n + (8 - rem);
}

// ---------------------------------------------------------------------------
// FlatBuffers reader
//
// A table at absolute position `t` starts with an soffset32; its vtable lives
// at `t - soffset`. The vtable is `u16 vtable_len`, `u16 table_len`, then one
// u16 per field id. A zero offset, or a field id past `vtable_len`, means the
// field is absent.
// ---------------------------------------------------------------------------

/** Absolute position of the root table of the flatbuffer starting at `start`. */
function fbRoot(view, start) {
  return start + view.getUint32(start, true);
}

/** Absolute position of field `id`'s slot in `table`, or 0 when absent. */
function fbField(view, table, id) {
  const vtable = table - view.getInt32(table, true);
  const vtableLen = view.getUint16(vtable, true);
  const slot = 4 + id * 2;
  if (slot >= vtableLen) {
    return 0;
  }
  const offset = view.getUint16(vtable + slot, true);
  return offset === 0 ? 0 : table + offset;
}

/** Follow the uoffset stored at `slot` to the absolute position it names. */
function fbDeref(view, slot) {
  return slot + view.getUint32(slot, true);
}

/** Read the string referenced by `slot`. */
function fbString(view, bytes, slot) {
  const at = fbDeref(view, slot);
  const len = view.getUint32(at, true);
  return UTF8.decode(bytes.subarray(at + 4, at + 4 + len));
}

/** `{ start, count }` of the vector referenced by `slot`; `start` skips the count. */
function fbVector(view, slot) {
  const at = fbDeref(view, slot);
  return { start: at + 4, count: view.getUint32(at, true) };
}

// ---------------------------------------------------------------------------
// Arrow IPC message framing
// ---------------------------------------------------------------------------

/**
 * Read the message header at `pos`, bounded by `end`.
 *
 * Returns `null` at the end-of-stream marker.
 */
function readMessage(view, pos, end) {
  if (pos + 8 > end) {
    throw new Error(`arrow ipc: truncated message prefix at ${pos}`);
  }
  const continuation = view.getUint32(pos, true);
  if (continuation !== CONTINUATION) {
    throw new Error(
      `arrow ipc: expected continuation 0xffffffff at ${pos}, found 0x${continuation.toString(16)}`,
    );
  }
  const metadataLen = view.getUint32(pos + 4, true);
  if (metadataLen === 0) {
    return null;
  }
  const flatbuffer = pos + 8;
  const bodyStart = flatbuffer + metadataLen;
  if (bodyStart > end) {
    throw new Error(
      `arrow ipc: message at ${pos} claims ${metadataLen} metadata bytes, past the segment end`,
    );
  }
  const message = fbRoot(view, flatbuffer);
  const headerTypeSlot = fbField(view, message, 1);
  const headerSlot = fbField(view, message, 2);
  if (headerSlot === 0) {
    throw new Error(`arrow ipc: message at ${pos} has no header`);
  }
  const bodyLengthSlot = fbField(view, message, 3);
  const bodyLength = bodyLengthSlot === 0 ? 0 : Number(view.getBigInt64(bodyLengthSlot, true));
  if (bodyLength < 0 || bodyStart + bodyLength > end) {
    throw new Error(
      `arrow ipc: message at ${pos} claims a ${bodyLength}-byte body, past the segment end`,
    );
  }
  return {
    headerType: headerTypeSlot === 0 ? 0 : view.getUint8(headerTypeSlot),
    header: fbDeref(view, headerSlot),
    bodyStart,
    bodyLength,
    next: bodyStart + align8(bodyLength),
  };
}

/**
 * Parse a segment's leading Schema message and pull `__pcs_component` out of
 * its custom metadata.
 */
function scanSegment(view, bytes, start, end) {
  const schema = readMessage(view, start, end);
  if (schema === null || schema.headerType !== HEADER_SCHEMA) {
    throw new Error(`pcs stream: segment at ${start} does not start with a Schema message`);
  }
  const metadataSlot = fbField(view, schema.header, 2);
  if (metadataSlot !== 0) {
    const entries = fbVector(view, metadataSlot);
    for (let i = 0; i < entries.count; i += 1) {
      const keyValue = fbDeref(view, entries.start + i * 4);
      const keySlot = fbField(view, keyValue, 0);
      const valueSlot = fbField(view, keyValue, 1);
      if (keySlot === 0 || valueSlot === 0) {
        continue;
      }
      if (fbString(view, bytes, keySlot) === COMPONENT_KEY) {
        return { component: fbString(view, bytes, valueSlot), schema };
      }
    }
  }
  throw new Error(`pcs stream: segment at ${start} carries no "${COMPONENT_KEY}" schema metadata`);
}

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

/** Look up a field descriptor by name. */
function fieldOf(batch, name) {
  const field = batch.fields.get(name);
  if (field === undefined) {
    throw new Error(
      `arrow ipc: component "${batch.component}" has no field "${name}" (has: ${[...batch.fields.keys()].join(', ')})`,
    );
  }
  return field;
}

/** Resolve buffer slot `index` to an absolute `{ start, length }` in the body. */
function bufferAt(batch, index) {
  const at = batch.buffersStart + index * 16;
  const offset = Number(batch.view.getBigInt64(at, true));
  const length = Number(batch.view.getBigInt64(at + 8, true));
  if (offset < 0 || length < 0 || offset + length > batch.bodyLength) {
    throw new Error(
      `arrow ipc: buffer ${index} (offset ${offset}, length ${length}) escapes the ${batch.bodyLength}-byte body`,
    );
  }
  return { start: batch.bodyStart + offset, length };
}

/**
 * Values buffer of `field`, checked against the row count.
 *
 * `slot` is the offset within the field's slot run: 1 is the values buffer for
 * a fixed-width type and the i32 offsets buffer for `Utf8`.
 */
function valuesBuffer(batch, field, slot, minBytes) {
  const buffer = bufferAt(batch, field.firstBuffer + slot);
  if (buffer.length < minBytes) {
    throw new Error(
      `arrow ipc: field "${field.name}" needs ${minBytes} bytes for ${batch.rows} rows, buffer ${field.firstBuffer + slot} holds ${buffer.length}`,
    );
  }
  return buffer;
}

/** Reject a read/write against a field whose Arrow type is not the expected one. */
function expectType(batch, name, wanted) {
  const field = fieldOf(batch, name);
  if (field.typeType !== wanted) {
    throw new Error(
      `arrow ipc: field "${name}" is ${TYPE_NAMES.get(field.typeType)}, not ${TYPE_NAMES.get(wanted)}`,
    );
  }
  return field;
}

/** Reject an out-of-range row index. */
function checkRow(batch, row) {
  if (!Number.isInteger(row) || row < 0 || row >= batch.rows) {
    throw new Error(`arrow ipc: row ${row} is out of range for a ${batch.rows}-row batch`);
  }
}

/**
 * One component's RecordBatch, addressed by field name.
 *
 * Reads copy out of the underlying stream; writes go straight back into it.
 * Validity buffers are not consulted: every `Order` field is non-nullable.
 */
class Batch {
  constructor(stream, component, rows, fields, buffersStart, buffersCount, body) {
    this.view = stream.view;
    this.bytes = stream.bytes;
    this.component = component;
    /** Row count from `RecordBatch.length`. */
    this.rows = rows;
    this.fields = fields;
    this.buffersStart = buffersStart;
    this.buffersCount = buffersCount;
    this.bodyStart = body.start;
    this.bodyLength = body.length;
  }

  /** Field names in schema order. */
  fieldNames() {
    return [...this.fields.keys()];
  }

  /** Read an `Int64` column. Values are `BigInt`: a JS number cannot hold the full range. */
  int64s(name) {
    const field = expectType(this, name, TYPE_INT);
    const buffer = valuesBuffer(this, field, 1, this.rows * WIDTH_64);
    const out = new Array(this.rows);
    for (let row = 0; row < this.rows; row += 1) {
      out[row] = this.view.getBigInt64(buffer.start + row * WIDTH_64, true);
    }
    return out;
  }

  /** Read a `Float64` column. */
  float64s(name) {
    const field = expectType(this, name, TYPE_FLOAT);
    const buffer = valuesBuffer(this, field, 1, this.rows * WIDTH_64);
    const out = new Array(this.rows);
    for (let row = 0; row < this.rows; row += 1) {
      out[row] = this.view.getFloat64(buffer.start + row * WIDTH_64, true);
    }
    return out;
  }

  /** Read a `Boolean` column out of its LSB-first bitmap. */
  bools(name) {
    const field = expectType(this, name, TYPE_BOOL);
    const buffer = valuesBuffer(this, field, 1, Math.ceil(this.rows / 8));
    const out = new Array(this.rows);
    for (let row = 0; row < this.rows; row += 1) {
      const byte = this.bytes[buffer.start + (row >> 3)];
      out[row] = (byte & (1 << (row & 7))) !== 0;
    }
    return out;
  }

  /** Read a `Utf8` column via its i32 offsets buffer. */
  strings(name) {
    const field = expectType(this, name, TYPE_UTF8);
    const offsets = valuesBuffer(this, field, 1, (this.rows + 1) * 4);
    const values = bufferAt(this, field.firstBuffer + 2);
    const out = new Array(this.rows);
    for (let row = 0; row < this.rows; row += 1) {
      const from = this.view.getInt32(offsets.start + row * 4, true);
      const to = this.view.getInt32(offsets.start + (row + 1) * 4, true);
      if (from < 0 || to < from || to > values.length) {
        throw new Error(
          `arrow ipc: field "${name}" row ${row} offsets [${from}, ${to}) escape its ${values.length}-byte values buffer`,
        );
      }
      out[row] = UTF8.decode(this.bytes.subarray(values.start + from, values.start + to));
    }
    return out;
  }

  /** Overwrite one `Float64` cell in place. */
  setFloat64(name, row, value) {
    const field = writable(this, name, TYPE_FLOAT);
    checkRow(this, row);
    const buffer = valuesBuffer(this, field, 1, this.rows * WIDTH_64);
    this.view.setFloat64(buffer.start + row * WIDTH_64, value, true);
  }

  /** Overwrite one `Boolean` cell in place, flipping a single bit. */
  setBool(name, row, value) {
    const field = writable(this, name, TYPE_BOOL);
    checkRow(this, row);
    const buffer = valuesBuffer(this, field, 1, Math.ceil(this.rows / 8));
    const at = buffer.start + (row >> 3);
    const mask = 1 << (row & 7);
    this.bytes[at] = value ? this.bytes[at] | mask : this.bytes[at] & ~mask;
  }
}

/**
 * Field a `set*` call may target.
 *
 * Variable-length columns are refused outright: widening a `Utf8` cell would
 * mean rewriting its offsets buffer, its values buffer and the RecordBatch
 * flatbuffer that describes both. In this pipeline only the Rust stage — which
 * has a real Arrow writer — writes `settlement`.
 */
function writable(batch, name, wanted) {
  const field = fieldOf(batch, name);
  if (BUFFER_SLOTS.get(field.typeType) === 3) {
    throw new Error(
      `arrow ipc: field "${name}" is variable-length ${TYPE_NAMES.get(field.typeType)}; in-place writes are limited to fixed-width columns`,
    );
  }
  return expectType(batch, name, wanted);
}

/** Parse a segment's Schema + RecordBatch messages into an addressable [`Batch`]. */
function parseBatch(stream, segment) {
  const { view, bytes } = stream;
  const fields = new Map();
  const fieldsSlot = fbField(view, segment.schema.header, 1);
  if (fieldsSlot !== 0) {
    const vector = fbVector(view, fieldsSlot);
    for (let i = 0; i < vector.count; i += 1) {
      const table = fbDeref(view, vector.start + i * 4);
      const nameSlot = fbField(view, table, 0);
      if (nameSlot === 0) {
        throw new Error(`arrow ipc: schema field ${i} has no name`);
      }
      const name = fbString(view, bytes, nameSlot);
      const typeSlot = fbField(view, table, 2);
      const typeType = typeSlot === 0 ? 0 : view.getUint8(typeSlot);
      if (!BUFFER_SLOTS.has(typeType)) {
        throw new Error(`arrow ipc: field "${name}" has unsupported type_type ${typeType}`);
      }
      fields.set(name, { name, typeType, firstBuffer: 0 });
    }
  }

  const batch = readMessage(view, segment.schema.next, segment.end);
  if (batch === null || batch.headerType !== HEADER_RECORD_BATCH) {
    throw new Error(
      `pcs stream: segment "${segment.component}" has no RecordBatch message after its schema`,
    );
  }
  if (fbField(view, batch.header, 3) !== 0) {
    throw new Error(
      `pcs stream: segment "${segment.component}" declares body compression, which is not supported`,
    );
  }
  const buffersSlot = fbField(view, batch.header, 2);
  if (buffersSlot === 0) {
    throw new Error(`pcs stream: segment "${segment.component}" RecordBatch has no buffers vector`);
  }
  const buffers = fbVector(view, buffersSlot);
  const lengthSlot = fbField(view, batch.header, 0);
  const rows = lengthSlot === 0 ? 0 : Number(view.getBigInt64(lengthSlot, true));

  // Buffer slots are positional: walk the fields in schema order and hand each
  // one the next run of slots its type consumes.
  let bufIdx = 0;
  for (const field of fields.values()) {
    field.firstBuffer = bufIdx;
    bufIdx += BUFFER_SLOTS.get(field.typeType);
    if (bufIdx > buffers.count) {
      throw new Error(
        `arrow ipc: field "${field.name}" needs buffer slots ${field.firstBuffer}..${bufIdx - 1}, but the RecordBatch declares only ${buffers.count}`,
      );
    }
  }
  if (bufIdx !== buffers.count) {
    throw new Error(
      `arrow ipc: schema consumes ${bufIdx} buffer slots but the RecordBatch declares ${buffers.count}`,
    );
  }

  return new Batch(stream, segment.component, rows, fields, buffers.start, buffers.count, {
    start: batch.bodyStart,
    length: batch.bodyLength,
  });
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

/**
 * A parsed PCS host↔guest stream.
 *
 * The stream borrows `bytes` and mutates it in place — the host hands
 * `run-batch` a freshly allocated array per call, so copying it would be pure
 * waste. Callers that need the original must copy before constructing.
 */
export class PcsStream {
  constructor(bytes) {
    // `instanceof Uint8Array` is wrong here: componentize-js lifts `list<u8>`
    // into a Uint8Array allocated by the generated bindings, which live in a
    // different realm — same shape, different intrinsics, so `instanceof` and
    // `constructor === Uint8Array` are both false inside the component.
    // `ArrayBuffer.isView` plus the element width is the realm-agnostic check,
    // and every operation below (DataView, subarray, indexing, TextDecoder)
    // accepts a cross-realm view.
    if (!ArrayBuffer.isView(bytes) || bytes.BYTES_PER_ELEMENT !== 1) {
      throw new Error('pcs stream: expected a byte view (Uint8Array)');
    }
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.segments = [];

    // Component names come out of every segment up front: a segment without
    // `__pcs_component` is malformed whether or not this stage reads it.
    let pos = 0;
    for (;;) {
      if (pos + 4 > bytes.byteLength) {
        throw new Error(`pcs stream: truncated at ${pos}, before the segment terminator`);
      }
      const len = this.view.getUint32(pos, true);
      pos += 4;
      if (len === 0) {
        break;
      }
      const end = pos + len;
      if (end > bytes.byteLength) {
        throw new Error(
          `pcs stream: segment at ${pos} claims ${len} bytes, only ${bytes.byteLength - pos} remain`,
        );
      }
      const scanned = scanSegment(this.view, bytes, pos, end);
      this.segments.push({ start: pos, end, component: scanned.component, schema: scanned.schema });
      pos = end;
    }
  }

  /** Component names carried by this stream, in segment order. */
  componentNames() {
    return this.segments.map((segment) => segment.component);
  }

  /** The named component's batch. Throws when the stream carries no such segment. */
  component(name) {
    const segment = this.segments.find((candidate) => candidate.component === name);
    if (segment === undefined) {
      throw new Error(
        `pcs stream: no segment for component "${name}" (present: ${this.componentNames().join(', ') || 'none'})`,
      );
    }
    if (segment.batch === undefined) {
      segment.batch = parseBatch(this, segment);
    }
    return segment.batch;
  }

  /** The stream's bytes, including every in-place mutation. */
  toBytes() {
    return this.bytes;
  }
}

/**
 * Decode a base64 string to bytes.
 *
 * `atob` is the one base64 primitive both Node and StarlingMonkey expose, so
 * the guest decodes its generated schema constant with it and needs no
 * dependency.
 */
export function decodeBase64(text) {
  const raw = atob(text);
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) {
    out[i] = raw.charCodeAt(i);
  }
  return out;
}
