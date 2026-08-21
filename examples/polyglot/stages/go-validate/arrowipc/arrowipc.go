// Package arrowipc reads and mutates the PCS host<->guest wire format using
// nothing but the Go standard library.
//
// Wire format (every offset below verified against
// examples/polyglot/generated/fixture_input.pcs):
//
//	pcs_stream := segment* terminator
//	segment    := u32le segment_len ++ arrow_ipc_stream[segment_len]
//	terminator := u32le 0x00000000
//	message    := u32le 0xFFFFFFFF ++ u32le metadata_len
//	           ++ flatbuffer[metadata_len] ++ body[bodyLength]
//
// One segment per registered component ordered by component name, then an
// `__alive` bitmap segment. Each segment is a standalone Arrow IPC stream: one
// Schema message, one RecordBatch message, then an end-of-stream marker.
// `metadata_len` already includes the flatbuffer's padding to 8 bytes, and the
// next message starts at align8(body_start + bodyLength).
//
// # Why hand-rolled
//
// This stage only ever overwrites fixed-width value slots, which is a read of
// the flatbuffer metadata plus a byte write into the body. Keeping that in the
// standard library means the stage depends on nothing that has to survive
// componentization on its own, and it makes the host<->guest contract
// documentable: all of it is in this file.
//
// # What this codec deliberately cannot do
//
// It never *writes* a flatbuffer. SetFloat64 and SetBool accept fixed-width
// fields only: changing a Utf8 value would shift every following offset and
// force a rewrite of the RecordBatch metadata. That is why `settlement` — the
// chain's one variable-length output — belongs to the Rust stage, which has a
// real Arrow writer.
//
// The trailing `__alive` segment is never parsed and never touched: the host
// marks every row of a batch alive, and a guest that can neither add nor remove
// rows cannot change that. Those bytes pass through byte-identical, as does
// every flatbuffer and every framing word.
//
// Malformed input yields an error, never a panic: this code runs inside a
// component whose only failure channel is the WIT `permanent(string)` arm, and
// a Go panic there traps the instance instead of reporting the reason.
package arrowipc

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
)

// Framing and Arrow discriminants.
const (
	// continuation prefixes every IPC message; a metadata length of 0 right
	// after it is the end-of-stream marker.
	continuation uint32 = 0xFFFFFFFF

	// Message.header_type values.
	headerSchema      uint8 = 1
	headerDictionary  uint8 = 2
	headerRecordBatch uint8 = 3

	// Field.type_type values this codec understands. Anything else in a
	// component segment is rejected rather than guessed at.
	typeInt   uint8 = 2
	typeFloat uint8 = 3
	typeUtf8  uint8 = 5
	typeBool  uint8 = 6

	// Inline FlatBuffers struct sizes: FieldNode{i64,i64}, Buffer{i64,i64}.
	fieldNodeSize = 16
	bufferSize    = 16

	// componentKey names the Schema custom_metadata entry the host writes to
	// label a segment. A segment without it is not addressable, so its absence
	// is an error rather than a skip.
	componentKey = "__pcs_component"
)

// FlatBuffers vtable field ids, from Arrow's Message.fbs and Schema.fbs. A
// union occupies two consecutive slots (discriminant, then payload), which is
// where Field.type_type = 2 comes from.
const (
	msgHeaderType = 1
	msgHeader     = 2
	msgBodyLength = 3

	schemaFieldsID   = 1
	schemaMetadataID = 2

	fieldNameID     = 0
	fieldTypeTypeID = 2

	batchLengthID      = 0
	batchNodesID       = 1
	batchBuffersID     = 2
	batchCompressionID = 3

	kvKeyID   = 0
	kvValueID = 1
)

// orderSchemaIPC is decoded once here rather than on every Describe call: the
// constant is compiled in, so a decode failure is a corrupt generated file and
// is worth reporting exactly once.
var orderSchemaIPC, orderSchemaIPCErr = base64.StdEncoding.DecodeString(OrderSchemaIPCBase64)

// OrderSchemaIPC returns the canonical `Order` schema-only Arrow IPC stream the
// host parses out of `component-descriptor.arrow-schema-ipc`. The bytes are
// shared, so callers must not mutate them.
func OrderSchemaIPC() ([]byte, error) {
	if orderSchemaIPCErr != nil {
		return nil, fmt.Errorf("decode OrderSchemaIPCBase64: %w", orderSchemaIPCErr)
	}
	return orderSchemaIPC, nil
}

// ---------------------------------------------------------------------------
// Stream — segment framing.
// ---------------------------------------------------------------------------

// Stream is a parsed PCS wire-format stream.
//
// Buf is the guest-owned mutable copy of the input: every Set call writes into
// it, and it is what the guest hands back to the host as `run-result.output`.
type Stream struct {
	Buf      []byte
	segments []segment
}

// segment locates one embedded Arrow IPC stream inside Buf.
type segment struct {
	start int // absolute offset of the segment's first IPC message
	end   int // absolute offset one past the segment's last byte
}

// Parse splits input into segments.
//
// The input is copied. The slice the generated export glue hands a guest aliases
// memory pinned only for the duration of the call, so owning a copy is what
// makes in-place mutation and returning the buffer safe.
func Parse(input []byte) (*Stream, error) {
	buf := make([]byte, len(input))
	copy(buf, input)
	s := &Stream{Buf: buf}

	pos := 0
	for {
		if pos+4 > len(buf) {
			return nil, fmt.Errorf("truncated stream: no segment length at offset %d of %d bytes", pos, len(buf))
		}
		segLen := int(binary.LittleEndian.Uint32(buf[pos:]))
		pos += 4
		if segLen == 0 {
			break
		}
		if segLen < 0 || pos+segLen > len(buf) {
			return nil, fmt.Errorf("segment at offset %d declares %d bytes, %d remain", pos-4, segLen, len(buf)-pos)
		}
		s.segments = append(s.segments, segment{start: pos, end: pos + segLen})
		pos += segLen
	}
	if len(s.segments) == 0 {
		return nil, fmt.Errorf("stream declares no segments")
	}
	if pos != len(buf) {
		return nil, fmt.Errorf("%d bytes trail the stream terminator", len(buf)-pos)
	}
	return s, nil
}

// Component returns the batch of the segment whose Schema metadata declares the
// given component name.
func (s *Stream) Component(name string) (*Batch, error) {
	for i, seg := range s.segments {
		schema, err := s.message(seg.start, seg.end)
		if err != nil {
			return nil, fmt.Errorf("segment %d: %w", i, err)
		}
		if !schema.present {
			return nil, fmt.Errorf("segment %d is empty", i)
		}
		if schema.headerType != headerSchema {
			return nil, fmt.Errorf("segment %d opens with header_type %d, want %d (Schema)", i, schema.headerType, headerSchema)
		}
		header, ok, err := schema.root.child(msgHeader)
		if err != nil {
			return nil, fmt.Errorf("segment %d schema header: %w", i, err)
		}
		if !ok {
			return nil, fmt.Errorf("segment %d schema message carries no header", i)
		}
		declared, err := componentOf(header)
		if err != nil {
			return nil, fmt.Errorf("segment %d: %w", i, err)
		}
		if declared != name {
			continue
		}
		batch, err := s.batch(seg, schema, header, name)
		if err != nil {
			return nil, fmt.Errorf("segment %d (%s): %w", i, name, err)
		}
		return batch, nil
	}
	return nil, fmt.Errorf("no segment declares component %q", name)
}

// componentOf reads the `__pcs_component` label out of a Schema's
// custom_metadata.
func componentOf(schema fbTable) (string, error) {
	meta, ok, err := schema.vector(schemaMetadataID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("schema has no custom_metadata, so no %q label", componentKey)
	}
	for i := range meta.count {
		kv, err := meta.table(i)
		if err != nil {
			return "", fmt.Errorf("custom_metadata[%d]: %w", i, err)
		}
		key, _, err := kv.str(kvKeyID)
		if err != nil {
			return "", fmt.Errorf("custom_metadata[%d] key: %w", i, err)
		}
		if key != componentKey {
			continue
		}
		value, ok, err := kv.str(kvValueID)
		if err != nil {
			return "", fmt.Errorf("%s value: %w", componentKey, err)
		}
		if !ok {
			return "", fmt.Errorf("%s metadata entry has no value", componentKey)
		}
		return value, nil
	}
	return "", fmt.Errorf("schema custom_metadata has no %q key", componentKey)
}

// ---------------------------------------------------------------------------
// Message framing.
// ---------------------------------------------------------------------------

// message is one framed Arrow IPC message inside a segment.
type message struct {
	present    bool // false for the end-of-stream marker
	root       fbTable
	headerType uint8
	body       int // absolute offset of the message body in Stream.Buf
	bodyLen    int
	next       int // absolute offset of the following message
}

func (s *Stream) message(pos, limit int) (message, error) {
	if pos+8 > limit {
		return message{}, fmt.Errorf("truncated message prefix at offset %d", pos)
	}
	if binary.LittleEndian.Uint32(s.Buf[pos:]) != continuation {
		return message{}, fmt.Errorf("offset %d is not an IPC message: continuation marker missing", pos)
	}
	metaLen := int(binary.LittleEndian.Uint32(s.Buf[pos+4:]))
	if metaLen == 0 {
		return message{}, nil // end-of-stream
	}
	if metaLen < 0 || pos+8+metaLen > limit {
		return message{}, fmt.Errorf("message at offset %d declares %d metadata bytes, %d remain", pos, metaLen, limit-pos-8)
	}

	fb := fbBuf(s.Buf[pos+8 : pos+8+metaLen])
	root, err := fb.root()
	if err != nil {
		return message{}, fmt.Errorf("message at offset %d: %w", pos, err)
	}
	headerType, err := root.u8(msgHeaderType, 0)
	if err != nil {
		return message{}, fmt.Errorf("message at offset %d header_type: %w", pos, err)
	}
	rawBodyLen, err := root.i64(msgBodyLength, 0)
	if err != nil {
		return message{}, fmt.Errorf("message at offset %d bodyLength: %w", pos, err)
	}
	body := pos + 8 + metaLen
	bodyLen, err := asLength(rawBodyLen, "bodyLength")
	if err != nil {
		return message{}, err
	}
	if body+bodyLen > limit {
		return message{}, fmt.Errorf("message at offset %d declares a %d-byte body, %d remain", pos, bodyLen, limit-body)
	}
	return message{
		present:    true,
		root:       root,
		headerType: headerType,
		body:       body,
		bodyLen:    bodyLen,
		next:       body + align8(bodyLen),
	}, nil
}

func align8(n int) int { return (n + 7) & ^7 }

// asLength narrows an on-the-wire i64 to int, rejecting the values that would
// otherwise turn into an out-of-range slice index.
func asLength(v int64, what string) (int, error) {
	if v < 0 || v > int64(^uint(0)>>1) {
		return 0, fmt.Errorf("%s is %d, which is not a usable length", what, v)
	}
	return int(v), nil
}

// ---------------------------------------------------------------------------
// Batch — columns of one component segment.
// ---------------------------------------------------------------------------

// span is one Arrow buffer, resolved to absolute offsets in Stream.Buf.
type span struct {
	off int
	len int
}

// field is a schema field paired with the buffers the RecordBatch assigned it.
// The validity span is resolved but unused: every `Order` field is
// non-nullable, arrow-rs still emits an all-ones validity bitmap, and an
// in-place value write therefore never has to touch it.
type field struct {
	name     string
	typ      uint8
	validity span
	offsets  span // Utf8 only
	values   span
}

// Batch is the RecordBatch of one component segment, addressable by field name.
type Batch struct {
	// Rows is the RecordBatch row count.
	Rows int

	component string
	buf       []byte // aliases Stream.Buf: Set calls land in the stream
	fields    []field
}

func (s *Stream) batch(seg segment, schema message, header fbTable, name string) (*Batch, error) {
	fields, err := schemaFieldsOf(header)
	if err != nil {
		return nil, err
	}

	rb, err := s.message(schema.next, seg.end)
	if err != nil {
		return nil, err
	}
	if !rb.present {
		return nil, fmt.Errorf("segment ends after its schema, with no record batch")
	}
	if rb.headerType != headerRecordBatch {
		if rb.headerType == headerDictionary {
			return nil, fmt.Errorf("segment carries a dictionary batch, which this codec does not support")
		}
		return nil, fmt.Errorf("second message has header_type %d, want %d (RecordBatch)", rb.headerType, headerRecordBatch)
	}
	rbHeader, ok, err := rb.root.child(msgHeader)
	if err != nil {
		return nil, fmt.Errorf("record batch header: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("record batch message carries no header")
	}
	// Body compression would make every value offset below meaningless.
	if rbHeader.has(batchCompressionID) {
		return nil, fmt.Errorf("record batch body is compressed, which this codec does not support")
	}

	rawRows, err := rbHeader.i64(batchLengthID, 0)
	if err != nil {
		return nil, fmt.Errorf("record batch length: %w", err)
	}
	rows, err := asLength(rawRows, "record batch length")
	if err != nil {
		return nil, err
	}

	nodes, ok, err := rbHeader.vector(batchNodesID)
	if err != nil {
		return nil, fmt.Errorf("record batch nodes: %w", err)
	}
	if !ok || nodes.count != len(fields) {
		return nil, fmt.Errorf("record batch has %d field nodes for %d schema fields", nodes.count, len(fields))
	}
	if _, err := nodes.inline(nodes.count-1, fieldNodeSize); nodes.count > 0 && err != nil {
		return nil, fmt.Errorf("record batch nodes: %w", err)
	}
	buffers, ok, err := rbHeader.vector(batchBuffersID)
	if err != nil {
		return nil, fmt.Errorf("record batch buffers: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("record batch carries no buffers vector")
	}

	// Buffer slots are assigned by walking the schema in field order; the slot
	// count is fixed by type_type, never inferred from a buffer's length.
	next := 0
	take := func(fieldName string) (span, error) {
		if next >= buffers.count {
			return span{}, fmt.Errorf("field %q needs buffer slot %d, record batch has %d", fieldName, next, buffers.count)
		}
		s, err := buffers.buffer(next, rb.body, rb.bodyLen)
		next++
		return s, err
	}
	for i := range fields {
		f := &fields[i]
		if f.validity, err = take(f.name); err != nil {
			return nil, err
		}
		switch f.typ {
		case typeInt, typeFloat, typeBool:
		case typeUtf8:
			if f.offsets, err = take(f.name); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("field %q has unsupported type_type %d", f.name, f.typ)
		}
		if f.values, err = take(f.name); err != nil {
			return nil, err
		}
	}
	if next != buffers.count {
		return nil, fmt.Errorf("schema consumes %d buffer slots, record batch carries %d", next, buffers.count)
	}

	return &Batch{Rows: rows, component: name, buf: s.Buf, fields: fields}, nil
}

// schemaFieldsOf reads field names and type discriminants in schema order,
// which is also buffer-walk order.
func schemaFieldsOf(schema fbTable) ([]field, error) {
	vec, ok, err := schema.vector(schemaFieldsID)
	if err != nil {
		return nil, fmt.Errorf("schema fields: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("schema carries no fields vector")
	}
	fields := make([]field, vec.count)
	for i := range vec.count {
		t, err := vec.table(i)
		if err != nil {
			return nil, fmt.Errorf("schema field %d: %w", i, err)
		}
		name, ok, err := t.str(fieldNameID)
		if err != nil {
			return nil, fmt.Errorf("schema field %d name: %w", i, err)
		}
		if !ok {
			return nil, fmt.Errorf("schema field %d has no name", i)
		}
		typ, err := t.u8(fieldTypeTypeID, 0)
		if err != nil {
			return nil, fmt.Errorf("schema field %q type_type: %w", name, err)
		}
		fields[i] = field{name: name, typ: typ}
	}
	return fields, nil
}

// FieldIndex returns the schema position of a field.
func (b *Batch) FieldIndex(name string) (int, error) {
	for i := range b.fields {
		if b.fields[i].name == name {
			return i, nil
		}
	}
	return 0, fmt.Errorf("component %q has no field %q", b.component, name)
}

// Int64s decodes an Int64 column.
func (b *Batch) Int64s(name string) ([]int64, error) {
	f, err := b.reader(name, typeInt, 8)
	if err != nil {
		return nil, err
	}
	out := make([]int64, b.Rows)
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(b.buf[f.values.off+i*8:]))
	}
	return out, nil
}

// Float64s decodes a Float64 column.
func (b *Batch) Float64s(name string) ([]float64, error) {
	f, err := b.reader(name, typeFloat, 8)
	if err != nil {
		return nil, err
	}
	out := make([]float64, b.Rows)
	for i := range out {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b.buf[f.values.off+i*8:]))
	}
	return out, nil
}

// Bools decodes a Boolean column from its LSB-first bitmap.
func (b *Batch) Bools(name string) ([]bool, error) {
	f, err := b.field(name, typeBool)
	if err != nil {
		return nil, err
	}
	if err := b.needBits(f); err != nil {
		return nil, err
	}
	out := make([]bool, b.Rows)
	for i := range out {
		out[i] = b.buf[f.values.off+i/8]>>(uint(i)&7)&1 == 1
	}
	return out, nil
}

// Strings decodes a Utf8 column through its i32 offsets buffer.
func (b *Batch) Strings(name string) ([]string, error) {
	f, err := b.field(name, typeUtf8)
	if err != nil {
		return nil, err
	}
	if need := (b.Rows + 1) * 4; f.offsets.len < need {
		return nil, fmt.Errorf("field %q offsets buffer holds %d bytes, need %d for %d rows", name, f.offsets.len, need, b.Rows)
	}
	out := make([]string, b.Rows)
	for i := range out {
		start := int(int32(binary.LittleEndian.Uint32(b.buf[f.offsets.off+i*4:])))
		end := int(int32(binary.LittleEndian.Uint32(b.buf[f.offsets.off+(i+1)*4:])))
		if start < 0 || end < start || end > f.values.len {
			return nil, fmt.Errorf("field %q row %d offsets [%d,%d) escape its %d-byte values buffer", name, i, start, end, f.values.len)
		}
		out[i] = string(b.buf[f.values.off+start : f.values.off+end])
	}
	return out, nil
}

// SetFloat64 overwrites one Float64 value in place.
func (b *Batch) SetFloat64(name string, row int, v float64) error {
	f, err := b.writer(name, typeFloat, row, 8)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(b.buf[f.values.off+row*8:], math.Float64bits(v))
	return nil
}

// SetBool overwrites one bit of a Boolean column's bitmap in place.
func (b *Batch) SetBool(name string, row int, v bool) error {
	f, err := b.field(name, typeBool)
	if err != nil {
		return err
	}
	if err := b.checkRow(name, row); err != nil {
		return err
	}
	if err := b.needBits(f); err != nil {
		return err
	}
	mask := byte(1) << (uint(row) & 7)
	at := f.values.off + row/8
	if v {
		b.buf[at] |= mask
	} else {
		b.buf[at] &= ^mask
	}
	return nil
}

// field resolves a name and checks its Arrow type.
func (b *Batch) field(name string, want uint8) (*field, error) {
	i, err := b.FieldIndex(name)
	if err != nil {
		return nil, err
	}
	f := &b.fields[i]
	if f.typ != want {
		return nil, fmt.Errorf("field %q is %s, not %s", name, typeName(f.typ), typeName(want))
	}
	return f, nil
}

// reader resolves a fixed-width field and checks its values buffer covers every
// row.
func (b *Batch) reader(name string, want uint8, width int) (*field, error) {
	f, err := b.field(name, want)
	if err != nil {
		return nil, err
	}
	if need := b.Rows * width; f.values.len < need {
		return nil, fmt.Errorf("field %q values buffer holds %d bytes, need %d for %d rows", name, f.values.len, need, b.Rows)
	}
	return f, nil
}

// writer is reader plus a row bound and the variable-length refusal. A Utf8
// write would move every following offset, so it is rejected by type before the
// type-mismatch message, which would otherwise read as if a Float64 column of
// that name were merely missing.
func (b *Batch) writer(name string, want uint8, row, width int) (*field, error) {
	i, err := b.FieldIndex(name)
	if err != nil {
		return nil, err
	}
	if t := b.fields[i].typ; t != typeInt && t != typeFloat && t != typeBool {
		return nil, fmt.Errorf("field %q is %s: this codec writes fixed-width values only, because a variable-length write would have to rebuild the offsets buffer and the RecordBatch metadata", name, typeName(t))
	}
	if err := b.checkRow(name, row); err != nil {
		return nil, err
	}
	return b.reader(name, want, width)
}

func (b *Batch) checkRow(name string, row int) error {
	if row < 0 || row >= b.Rows {
		return fmt.Errorf("row %d is out of range for field %q of %d rows", row, name, b.Rows)
	}
	return nil
}

func (b *Batch) needBits(f *field) error {
	if need := (b.Rows + 7) / 8; f.values.len < need {
		return fmt.Errorf("field %q bitmap holds %d bytes, need %d for %d rows", f.name, f.values.len, need, b.Rows)
	}
	return nil
}

func typeName(typ uint8) string {
	switch typ {
	case typeInt:
		return "Int"
	case typeFloat:
		return "FloatingPoint"
	case typeUtf8:
		return "Utf8"
	case typeBool:
		return "Bool"
	default:
		return fmt.Sprintf("type_type %d", typ)
	}
}

// ---------------------------------------------------------------------------
// FlatBuffers reader — just enough to read Arrow's Message, Schema, Field,
// RecordBatch and KeyValue tables.
// ---------------------------------------------------------------------------

// fbBuf is one FlatBuffers-encoded Arrow metadata message. Every read is bounds
// checked: these bytes come from outside the component.
type fbBuf []byte

func (b fbBuf) bounds(off, n int) error {
	if off < 0 || n < 0 || off+n > len(b) {
		return fmt.Errorf("read of %d bytes at %d exceeds %d-byte metadata", n, off, len(b))
	}
	return nil
}

func (b fbBuf) u8(off int) (uint8, error) {
	if err := b.bounds(off, 1); err != nil {
		return 0, err
	}
	return b[off], nil
}

func (b fbBuf) u32(off int) (uint32, error) {
	if err := b.bounds(off, 4); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[off:]), nil
}

func (b fbBuf) i64(off int) (int64, error) {
	if err := b.bounds(off, 8); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[off:])), nil
}

// root follows the buffer's leading uoffset to the root table.
func (b fbBuf) root() (fbTable, error) {
	off, err := b.u32(0)
	if err != nil {
		return fbTable{}, err
	}
	return b.table(int(off))
}

// table reads the table header at pos: a signed offset back to its vtable.
func (b fbBuf) table(pos int) (fbTable, error) {
	soff, err := b.u32(pos)
	if err != nil {
		return fbTable{}, fmt.Errorf("table at %d: %w", pos, err)
	}
	vt := pos - int(int32(soff))
	if err := b.bounds(vt, 4); err != nil {
		return fbTable{}, fmt.Errorf("vtable of table at %d: %w", pos, err)
	}
	vtLen := int(binary.LittleEndian.Uint16(b[vt:]))
	if vtLen < 4 {
		return fbTable{}, fmt.Errorf("table at %d has a %d-byte vtable", pos, vtLen)
	}
	if err := b.bounds(vt, vtLen); err != nil {
		return fbTable{}, fmt.Errorf("vtable of table at %d: %w", pos, err)
	}
	return fbTable{buf: b, pos: pos, vt: vt, vtLen: vtLen}, nil
}

type fbTable struct {
	buf   fbBuf
	pos   int
	vt    int
	vtLen int
}

// slot returns the field's offset from the table position, or 0 for an absent
// field — FlatBuffers encodes absence as a zero vtable entry or as a vtable too
// short to hold the id. The vtable bounds were checked in table(), so this
// cannot fail.
func (t fbTable) slot(id int) int {
	off := 4 + id*2
	if off+2 > t.vtLen {
		return 0
	}
	return int(binary.LittleEndian.Uint16(t.buf[t.vt+off:]))
}

func (t fbTable) has(id int) bool { return t.slot(id) != 0 }

func (t fbTable) u8(id int, def uint8) (uint8, error) {
	slot := t.slot(id)
	if slot == 0 {
		return def, nil
	}
	return t.buf.u8(t.pos + slot)
}

func (t fbTable) i64(id int, def int64) (int64, error) {
	slot := t.slot(id)
	if slot == 0 {
		return def, nil
	}
	return t.buf.i64(t.pos + slot)
}

// child resolves a uoffset field to the table it points at.
func (t fbTable) child(id int) (fbTable, bool, error) {
	slot := t.slot(id)
	if slot == 0 {
		return fbTable{}, false, nil
	}
	at := t.pos + slot
	off, err := t.buf.u32(at)
	if err != nil {
		return fbTable{}, false, err
	}
	child, err := t.buf.table(at + int(off))
	if err != nil {
		return fbTable{}, false, err
	}
	return child, true, nil
}

func (t fbTable) str(id int) (string, bool, error) {
	slot := t.slot(id)
	if slot == 0 {
		return "", false, nil
	}
	at := t.pos + slot
	off, err := t.buf.u32(at)
	if err != nil {
		return "", false, err
	}
	head := at + int(off)
	n, err := t.buf.u32(head)
	if err != nil {
		return "", false, err
	}
	if err := t.buf.bounds(head+4, int(n)); err != nil {
		return "", false, err
	}
	return string(t.buf[head+4 : head+4+int(n)]), true, nil
}

func (t fbTable) vector(id int) (fbVector, bool, error) {
	slot := t.slot(id)
	if slot == 0 {
		return fbVector{}, false, nil
	}
	at := t.pos + slot
	off, err := t.buf.u32(at)
	if err != nil {
		return fbVector{}, false, err
	}
	head := at + int(off)
	n, err := t.buf.u32(head)
	if err != nil {
		return fbVector{}, false, err
	}
	count, err := asLength(int64(n), "vector length")
	if err != nil {
		return fbVector{}, false, err
	}
	return fbVector{buf: t.buf, start: head + 4, count: count}, true, nil
}

type fbVector struct {
	buf   fbBuf
	start int
	count int
}

// table resolves element i of a vector of tables.
func (v fbVector) table(i int) (fbTable, error) {
	at := v.start + i*4
	off, err := v.buf.u32(at)
	if err != nil {
		return fbTable{}, err
	}
	return v.buf.table(at + int(off))
}

// inline returns the position of inline struct element i.
func (v fbVector) inline(i, size int) (int, error) {
	at := v.start + i*size
	if i < 0 || i >= v.count {
		return 0, fmt.Errorf("element %d is out of range for a %d-element vector", i, v.count)
	}
	if err := v.buf.bounds(at, size); err != nil {
		return 0, err
	}
	return at, nil
}

// buffer reads inline Buffer{i64 offset, i64 length} element i and resolves it
// against the message body. Buffer.offset is body-relative.
func (v fbVector) buffer(i, body, bodyLen int) (span, error) {
	at, err := v.inline(i, bufferSize)
	if err != nil {
		return span{}, err
	}
	off, err := v.buf.i64(at)
	if err != nil {
		return span{}, err
	}
	length, err := v.buf.i64(at + 8)
	if err != nil {
		return span{}, err
	}
	if off < 0 || length < 0 || off > int64(bodyLen) || length > int64(bodyLen)-off {
		return span{}, fmt.Errorf("buffer %d spans [%d,%d) of a %d-byte body", i, off, off+length, bodyLen)
	}
	return span{off: body + int(off), len: int(length)}, nil
}
