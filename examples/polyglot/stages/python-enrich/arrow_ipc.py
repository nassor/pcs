"""Arrow IPC reader/patcher for the PCS host<->guest wire format, stdlib only.

`pyarrow` has no `wasm32-wasi` wheel, so this stage hand-rolls the slice of the
format it actually needs. That is less code than it sounds: the guest never
*writes* a flatbuffer. It locates its component's segment, reads the columns it
consumes, overwrites fixed-width value bytes in place, and hands the input
buffer back. Framing, both flatbuffers, and every column it did not touch are
returned byte-identical.

`set_float64` / `set_bool` are therefore the whole write surface. A `Utf8`
column cannot be written here at all: changing a string resizes the values
buffer and invalidates the offsets buffer *and* the RecordBatch flatbuffer that
describes both, which needs a real Arrow writer. That is why `settlement` is
the Rust stage's output.

The `__alive` segment is parsed (so a malformed stream is still rejected) but
never consulted: this stage maps each row independently and deletes nothing, so
a dead row just receives a `usd_amount` that no downstream stage reads.

Format reference: `docs/content/polyglot/wire-format.md`. The flatbuffers field
ids below come from Arrow's `Message.fbs` and `Schema.fbs`.
"""

import base64
import struct

# --------------------------------------------------------------------------
# Format constants.
# --------------------------------------------------------------------------

#: Prefix of every Arrow IPC message in the encapsulated (post-0.15) format.
_CONTINUATION = 0xFFFFFFFF

_HEADER_SCHEMA = 1
_HEADER_RECORD_BATCH = 3

# `Message` field ids.
_MSG_HEADER_TYPE = 1
_MSG_HEADER = 2
_MSG_BODY_LENGTH = 3

# `Schema` field ids.
_SCHEMA_FIELDS = 1
_SCHEMA_CUSTOM_METADATA = 2

# `Field` field ids.
_FIELD_NAME = 0
_FIELD_TYPE_TYPE = 2

# `KeyValue` field ids.
_KV_KEY = 0
_KV_VALUE = 1

# `RecordBatch` field ids.
_RB_LENGTH = 0
_RB_BUFFERS = 2
_RB_COMPRESSION = 3

# `Type` union discriminants this codec understands.
TYPE_INT = 2
TYPE_FLOAT = 3
TYPE_UTF8 = 5
TYPE_BOOL = 6

_TYPE_NAMES = {
    TYPE_INT: "Int",
    TYPE_FLOAT: "FloatingPoint",
    TYPE_UTF8: "Utf8",
    TYPE_BOOL: "Bool",
}

#: Buffer slots each Arrow type consumes, in schema order. Fixed by the type,
#: never by nullability: arrow-rs emits the validity slot even when the field is
#: non-nullable and the batch has no nulls.
_BUFFER_SLOTS = {
    TYPE_INT: 2,  # validity, values
    TYPE_FLOAT: 2,  # validity, values
    TYPE_UTF8: 3,  # validity, i32 offsets, values
    TYPE_BOOL: 2,  # validity, bit-packed values
}

#: Schema metadata key carrying the pcs-core component name.
_COMPONENT_KEY = "__pcs_component"

_U8 = struct.Struct("<B")
_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_I32 = struct.Struct("<i")
_I64 = struct.Struct("<q")
_F64 = struct.Struct("<d")

# --------------------------------------------------------------------------
# Bounds-checked primitives.
#
# Every read goes through these so that a truncated or hostile buffer raises
# ValueError instead of `struct.error` or IndexError: the WIT contract says a
# guest maps bad input to `permanent(string)`, never to a trap.
# --------------------------------------------------------------------------


def _read(spec, buf, pos):
    """Read one little-endian scalar, rejecting out-of-range positions."""
    if pos < 0 or pos + spec.size > len(buf):
        raise ValueError(
            "arrow ipc: {} B read at {} is outside the {} B stream".format(
                spec.size, pos, len(buf)
            )
        )
    return spec.unpack_from(buf, pos)[0]


def _bounds(buf, pos, length):
    """Reject a span that leaves the buffer."""
    if pos < 0 or length < 0 or pos + length > len(buf):
        raise ValueError(
            "arrow ipc: span ({}, {}) is outside the {} B stream".format(
                pos, length, len(buf)
            )
        )


class _Table:
    """A FlatBuffers table addressed absolutely inside the stream buffer.

    FlatBuffers offsets are all self-relative, so absolute positions work
    unchanged as long as the root uoffset is read at the flatbuffer's start.
    """

    __slots__ = ("buf", "pos", "_vtable", "_vtable_len")

    def __init__(self, buf, pos):
        vtable = pos - _read(_I32, buf, pos)
        vtable_len = _read(_U16, buf, vtable)
        if vtable_len < 4:
            raise ValueError(
                "arrow ipc: vtable at {} is {} B, minimum is 4".format(vtable, vtable_len)
            )
        self.buf = buf
        self.pos = pos
        self._vtable = vtable
        self._vtable_len = vtable_len

    def _slot(self, field_id):
        """Byte offset of `field_id` from the table start; 0 when absent."""
        entry = 4 + 2 * field_id
        if entry + 2 > self._vtable_len:
            return 0
        return _read(_U16, self.buf, self._vtable + entry)

    def has(self, field_id):
        return self._slot(field_id) != 0

    def scalar(self, field_id, spec, default):
        offset = self._slot(field_id)
        if offset == 0:
            return default
        return _read(spec, self.buf, self.pos + offset)

    def _indirect(self, field_id):
        """Absolute target of the uoffset in `field_id`; -1 when absent."""
        offset = self._slot(field_id)
        if offset == 0:
            return -1
        at = self.pos + offset
        return at + _read(_U32, self.buf, at)

    def table(self, field_id):
        at = self._indirect(field_id)
        return None if at < 0 else _Table(self.buf, at)

    def string(self, field_id):
        at = self._indirect(field_id)
        if at < 0:
            return None
        length = _read(_U32, self.buf, at)
        _bounds(self.buf, at + 4, length)
        # UnicodeDecodeError subclasses ValueError, so invalid UTF-8 already
        # lands on the caller's rejection path.
        return bytes(self.buf[at + 4 : at + 4 + length]).decode("utf-8")

    def vector(self, field_id):
        """`(first element position, count)`; `(-1, 0)` when absent."""
        at = self._indirect(field_id)
        if at < 0:
            return -1, 0
        return at + 4, _read(_U32, self.buf, at)


def _vector_table(buf, start, index):
    """Table `index` of a FlatBuffers vector of offsets starting at `start`."""
    at = start + 4 * index
    return _Table(buf, at + _read(_U32, buf, at))


# --------------------------------------------------------------------------
# Stream, segment and message walking.
# --------------------------------------------------------------------------


def _split_segments(buf):
    """Yield `(start, length)` for each Arrow IPC stream in the PCS envelope.

    `segment* terminator`, where a segment is a u32le length followed by that
    many bytes and the terminator is a u32le zero.
    """
    pos = 0
    while True:
        length = _read(_U32, buf, pos)
        pos += 4
        if length == 0:
            return
        _bounds(buf, pos, length)
        yield pos, length
        pos += length


def _iter_messages(buf, start, length):
    """Yield `(header_type, header_table, body_start, body_length)`."""
    pos = start
    end = start + length
    while pos + 8 <= end:
        if _read(_U32, buf, pos) != _CONTINUATION:
            raise ValueError(
                "arrow ipc: message at {} lacks the 0xffffffff continuation marker".format(pos)
            )
        metadata_len = _read(_U32, buf, pos + 4)
        if metadata_len == 0:
            return  # end-of-stream marker
        flatbuffer = pos + 8
        if flatbuffer + metadata_len > end:
            raise ValueError(
                "arrow ipc: message at {} declares {} B of metadata but the "
                "segment ends at {}".format(pos, metadata_len, end)
            )
        message = _Table(buf, flatbuffer + _read(_U32, buf, flatbuffer))
        header = message.table(_MSG_HEADER)
        if header is None:
            raise ValueError("arrow ipc: message at {} carries no header table".format(pos))
        body_length = message.scalar(_MSG_BODY_LENGTH, _I64, 0)
        # `metadata_len` already covers the flatbuffer's padding to 8 bytes.
        body_start = flatbuffer + metadata_len
        if body_length < 0 or body_start + body_length > end:
            raise ValueError(
                "arrow ipc: message at {} declares a {} B body that overruns "
                "its segment".format(pos, body_length)
            )
        yield message.scalar(_MSG_HEADER_TYPE, _U8, 0), header, body_start, body_length
        pos = body_start + ((body_length + 7) & ~7)


def _parse_schema(schema):
    """Return `(custom_metadata, [(field name, type_type)])`."""
    fields = []
    start, count = schema.vector(_SCHEMA_FIELDS)
    for i in range(count):
        field = _vector_table(schema.buf, start, i)
        name = field.string(_FIELD_NAME)
        if name is None:
            raise ValueError("arrow ipc: schema field {} has no name".format(i))
        type_type = field.scalar(_FIELD_TYPE_TYPE, _U8, 0)
        if type_type not in _BUFFER_SLOTS:
            raise ValueError(
                "arrow ipc: field {!r} has arrow type id {}, which this codec "
                "does not implement".format(name, type_type)
            )
        fields.append((name, type_type))

    metadata = {}
    start, count = schema.vector(_SCHEMA_CUSTOM_METADATA)
    for i in range(count):
        entry = _vector_table(schema.buf, start, i)
        key = entry.string(_KV_KEY)
        if key is not None:
            metadata[key] = entry.string(_KV_VALUE) or ""
    return metadata, fields


def _parse_record_batch(record_batch, fields, body_start, body_length):
    """Resolve the batch's buffer table into absolute stream positions."""
    if record_batch.has(_RB_COMPRESSION):
        raise ValueError("arrow ipc: compressed record batches are not supported")

    rows = record_batch.scalar(_RB_LENGTH, _I64, 0)
    if rows < 0:
        raise ValueError("arrow ipc: record batch declares {} rows".format(rows))

    start, count = record_batch.vector(_RB_BUFFERS)
    buffers = []
    for i in range(count):
        at = start + 16 * i
        offset = _read(_I64, record_batch.buf, at)
        length = _read(_I64, record_batch.buf, at + 8)
        # Buffer offsets are relative to the message body.
        if offset < 0 or length < 0 or offset + length > body_length:
            raise ValueError(
                "arrow ipc: buffer {} spans ({}, {}), overrunning the {} B "
                "message body".format(i, offset, length, body_length)
            )
        buffers.append((body_start + offset, length))

    first_slot = []
    used = 0
    for _name, type_type in fields:
        first_slot.append(used)
        used += _BUFFER_SLOTS[type_type]
    if used != count:
        raise ValueError(
            "arrow ipc: schema needs {} buffer slots but the record batch "
            "carries {}".format(used, count)
        )
    return Batch(record_batch.buf, rows, fields, first_slot, buffers)


def _parse_segment(buf, start, length):
    """Return `(component name, Batch)` for one Arrow IPC stream."""
    fields = None
    metadata = None
    for header_type, header, body_start, body_length in _iter_messages(buf, start, length):
        if header_type == _HEADER_SCHEMA:
            metadata, fields = _parse_schema(header)
        elif header_type == _HEADER_RECORD_BATCH:
            if fields is None:
                raise ValueError("arrow ipc: record batch precedes its schema message")
            name = metadata.get(_COMPONENT_KEY)
            if not name:
                raise ValueError(
                    "arrow ipc: segment at {} has no {} schema metadata".format(
                        start, _COMPONENT_KEY
                    )
                )
            return name, _parse_record_batch(header, fields, body_start, body_length)
        else:
            raise ValueError(
                "arrow ipc: message header type {} is not supported (expected "
                "1=Schema or 3=RecordBatch)".format(header_type)
            )
    raise ValueError("arrow ipc: segment at {} has no record batch".format(start))


# --------------------------------------------------------------------------
# Public surface.
# --------------------------------------------------------------------------


class Batch:
    """One component's columns, backed by the stream's mutable buffer."""

    __slots__ = ("rows", "_buf", "_names", "_types", "_first_slot", "_index", "_buffers")

    def __init__(self, buf, rows, fields, first_slot, buffers):
        #: Row count declared by the RecordBatch message.
        self.rows = rows
        self._buf = buf
        self._names = [name for name, _type in fields]
        self._types = [type_type for _name, type_type in fields]
        self._first_slot = first_slot
        self._index = {name: i for i, name in enumerate(self._names)}
        self._buffers = buffers

    @property
    def field_names(self):
        return list(self._names)

    # -- lookup helpers ----------------------------------------------------

    def _field(self, name, expected):
        index = self._index.get(name)
        if index is None:
            raise ValueError(
                "arrow ipc: no field named {!r} (have {})".format(
                    name, ", ".join(self._names)
                )
            )
        actual = self._types[index]
        if actual != expected:
            raise ValueError(
                "arrow ipc: field {!r} is {}, not {}".format(
                    name, _TYPE_NAMES[actual], _TYPE_NAMES[expected]
                )
            )
        return index

    def _values(self, name, expected):
        """Values buffer of a fixed-width field: slot 1 of its slot group."""
        index = self._field(name, expected)
        return self._buffers[self._first_slot[index] + 1]

    def _writable(self, name, expected):
        """Like `_values`, but rejects variable-length fields explicitly."""
        index = self._index.get(name)
        if index is not None and self._types[index] == TYPE_UTF8:
            raise ValueError(
                "arrow ipc: {!r} is Utf8; an in-place guest cannot write a "
                "variable-length column".format(name)
            )
        return self._values(name, expected)

    def _require(self, name, have, need):
        if have < need:
            raise ValueError(
                "arrow ipc: field {!r} has a {} B buffer where {} rows need "
                "{} B".format(name, have, self.rows, need)
            )

    def _require_row(self, row):
        if row < 0 or row >= self.rows:
            raise ValueError(
                "arrow ipc: row {} is outside the batch's {} rows".format(row, self.rows)
            )

    # -- readers -----------------------------------------------------------

    def int64s(self, field):
        offset, length = self._values(field, TYPE_INT)
        self._require(field, length, 8 * self.rows)
        return list(struct.unpack_from("<{}q".format(self.rows), self._buf, offset))

    def float64s(self, field):
        offset, length = self._values(field, TYPE_FLOAT)
        self._require(field, length, 8 * self.rows)
        return list(struct.unpack_from("<{}d".format(self.rows), self._buf, offset))

    def bools(self, field):
        offset, length = self._values(field, TYPE_BOOL)
        self._require(field, length, (self.rows + 7) // 8)
        buf = self._buf
        # Bit-packed, LSB first.
        return [bool(buf[offset + (i >> 3)] >> (i & 7) & 1) for i in range(self.rows)]

    def strings(self, field):
        index = self._field(field, TYPE_UTF8)
        slot = self._first_slot[index]
        offsets_at, offsets_len = self._buffers[slot + 1]
        values_at, values_len = self._buffers[slot + 2]
        self._require(field, offsets_len, 4 * (self.rows + 1))
        offsets = struct.unpack_from("<{}i".format(self.rows + 1), self._buf, offsets_at)
        out = []
        for row in range(self.rows):
            begin, end = offsets[row], offsets[row + 1]
            if begin < 0 or end < begin or end > values_len:
                raise ValueError(
                    "arrow ipc: field {!r} row {} spans ({}, {}) outside its "
                    "{} B values buffer".format(field, row, begin, end, values_len)
                )
            out.append(bytes(self._buf[values_at + begin : values_at + end]).decode("utf-8"))
        return out

    # -- in-place writers --------------------------------------------------

    def set_float64(self, field, row, value):
        offset, length = self._writable(field, TYPE_FLOAT)
        self._require(field, length, 8 * self.rows)
        self._require_row(row)
        _F64.pack_into(self._buf, offset + 8 * row, float(value))

    def set_bool(self, field, row, value):
        offset, length = self._writable(field, TYPE_BOOL)
        self._require(field, length, (self.rows + 7) // 8)
        self._require_row(row)
        at = offset + (row >> 3)
        mask = 1 << (row & 7)
        if value:
            self._buf[at] |= mask
        else:
            self._buf[at] &= 0xFF ^ mask


class PcsStream:
    """A parsed PCS batch envelope that owns a mutable copy of the input."""

    __slots__ = ("_buf", "_batches")

    def __init__(self, data):
        self._buf = bytearray(data)
        self._batches = {}
        for start, length in _split_segments(self._buf):
            name, batch = _parse_segment(self._buf, start, length)
            self._batches[name] = batch

    @property
    def component_names(self):
        return sorted(self._batches)

    def component(self, name):
        """The named component's batch, or `ValueError` if the stream has none."""
        batch = self._batches.get(name)
        if batch is None:
            raise ValueError(
                "arrow ipc: no segment for component {!r} (have {})".format(
                    name, ", ".join(self.component_names)
                )
            )
        return batch

    def to_bytes(self):
        """The whole envelope, including every in-place mutation."""
        return bytes(self._buf)


def decode_schema_ipc(encoded):
    """Decode a generated `*_SCHEMA_IPC_BASE64` constant to descriptor bytes.

    Lives here so a guest needs one wire-format import and no `base64` of its
    own; `binascii.Error` subclasses `ValueError`, so a corrupt constant lands
    on the same rejection path as a corrupt stream.
    """
    return base64.b64decode(encoded)
