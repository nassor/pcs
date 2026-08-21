package arrowipc_test

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"testing"

	"wit_component/arrowipc"
)

// generatedDir holds the emitter's output. It is gitignored, so every test that
// needs it skips rather than fails when it is absent — the codec must not be a
// landmine in a fresh clone.
const generatedDir = "../../../generated"

// emitHint is the command that produces the fixtures.
const emitHint = "run `cargo run -p pcs-service --features wasm --example polyglot_orders -- emit` first"

// tolerance for float comparisons: 100.0 * 1.10 is not exactly 110.0, and the
// same tolerance is used by every stage of the chain.
const tolerance = 1e-6

// order mirrors examples/polyglot/generated/fixture_input.json, which is the
// ground truth for what the codec must decode out of the wire bytes.
type order struct {
	ID         int64   `json:"id"`
	Region     string  `json:"region"`
	Currency   string  `json:"currency"`
	Amount     float64 `json:"amount"`
	Valid      bool    `json:"valid"`
	UsdAmount  float64 `json:"usd_amount"`
	RiskScore  float64 `json:"risk_score"`
	Flagged    bool    `json:"flagged"`
	Settlement string  `json:"settlement"`
}

// fieldOrder is load-bearing: it is the order the fingerprint is computed in and
// the order the buffer walk assigns slots in.
var fieldOrder = []string{
	"id", "region", "currency", "amount", "valid",
	"usd_amount", "risk_score", "flagged", "settlement",
}

func readGenerated(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(generatedDir, name))
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("%s/%s is absent: %s", generatedDir, name, emitHint)
	}
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return raw
}

// fixture returns the wire bytes and the JSON ground truth for the same rows.
func fixture(t *testing.T) ([]byte, []order) {
	t.Helper()
	wire := readGenerated(t, "fixture_input.pcs")
	var rows []order
	if err := json.Unmarshal(readGenerated(t, "fixture_input.json"), &rows); err != nil {
		t.Fatalf("decode fixture_input.json: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("fixture_input.json holds no rows")
	}
	return wire, rows
}

func orderBatch(t *testing.T, wire []byte) (*arrowipc.Stream, *arrowipc.Batch) {
	t.Helper()
	stream, err := arrowipc.Parse(wire)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	batch, err := stream.Component("Order")
	if err != nil {
		t.Fatalf("Component(Order): %v", err)
	}
	return stream, batch
}

func floats(t *testing.T, b *arrowipc.Batch, name string) []float64 {
	t.Helper()
	v, err := b.Float64s(name)
	if err != nil {
		t.Fatalf("Float64s(%q): %v", name, err)
	}
	return v
}

func bools(t *testing.T, b *arrowipc.Batch, name string) []bool {
	t.Helper()
	v, err := b.Bools(name)
	if err != nil {
		t.Fatalf("Bools(%q): %v", name, err)
	}
	return v
}

func strs(t *testing.T, b *arrowipc.Batch, name string) []string {
	t.Helper()
	v, err := b.Strings(name)
	if err != nil {
		t.Fatalf("Strings(%q): %v", name, err)
	}
	return v
}

// assertColumns checks all nine columns against the JSON ground truth.
func assertColumns(t *testing.T, b *arrowipc.Batch, want []order) {
	t.Helper()
	if b.Rows != len(want) {
		t.Fatalf("Rows = %d, want %d", b.Rows, len(want))
	}
	ids, err := b.Int64s("id")
	if err != nil {
		t.Fatalf("Int64s(id): %v", err)
	}
	regions := strs(t, b, "region")
	currencies := strs(t, b, "currency")
	settlements := strs(t, b, "settlement")
	amounts := floats(t, b, "amount")
	usd := floats(t, b, "usd_amount")
	risk := floats(t, b, "risk_score")
	valid := bools(t, b, "valid")
	flagged := bools(t, b, "flagged")

	for i, w := range want {
		if ids[i] != w.ID {
			t.Errorf("row %d id = %d, want %d", i, ids[i], w.ID)
		}
		if regions[i] != w.Region {
			t.Errorf("row %d region = %q, want %q", i, regions[i], w.Region)
		}
		if currencies[i] != w.Currency {
			t.Errorf("row %d currency = %q, want %q", i, currencies[i], w.Currency)
		}
		if settlements[i] != w.Settlement {
			t.Errorf("row %d settlement = %q, want %q", i, settlements[i], w.Settlement)
		}
		if math.Abs(amounts[i]-w.Amount) > tolerance {
			t.Errorf("row %d amount = %v, want %v", i, amounts[i], w.Amount)
		}
		if math.Abs(usd[i]-w.UsdAmount) > tolerance {
			t.Errorf("row %d usd_amount = %v, want %v", i, usd[i], w.UsdAmount)
		}
		if math.Abs(risk[i]-w.RiskScore) > tolerance {
			t.Errorf("row %d risk_score = %v, want %v", i, risk[i], w.RiskScore)
		}
		if valid[i] != w.Valid {
			t.Errorf("row %d valid = %v, want %v", i, valid[i], w.Valid)
		}
		if flagged[i] != w.Flagged {
			t.Errorf("row %d flagged = %v, want %v", i, flagged[i], w.Flagged)
		}
	}
}

func TestFixtureDecodesEveryColumn(t *testing.T) {
	wire, want := fixture(t)
	_, batch := orderBatch(t, wire)
	assertColumns(t, batch, want)
}

// TestFixtureProducesDocumentedValidColumn pins the fixture's amounts to the
// `valid` column every later stage of the chain is documented to receive. The
// stage rule lives in the guest package, which cannot compile for the host
// target, so this is where the fixture and the documented expectation are kept
// honest with each other.
func TestFixtureProducesDocumentedValidColumn(t *testing.T) {
	wire, _ := fixture(t)
	_, batch := orderBatch(t, wire)
	amounts := floats(t, batch, "amount")

	const minAmount = 0.0 // the min_amount config default
	want := []bool{true, false, true, true, false}
	if len(amounts) != len(want) {
		t.Fatalf("fixture has %d rows, the documented chain expects %d", len(amounts), len(want))
	}
	for row, amount := range amounts {
		if got := amount > minAmount; got != want[row] {
			t.Errorf("row %d: amount %v > %v = %v, documented valid is %v", row, amount, minAmount, got, want[row])
		}
	}
}

func TestFieldOrderMatchesSchema(t *testing.T) {
	wire, _ := fixture(t)
	_, batch := orderBatch(t, wire)
	for want, name := range fieldOrder {
		got, err := batch.FieldIndex(name)
		if err != nil {
			t.Fatalf("FieldIndex(%q): %v", name, err)
		}
		if got != want {
			t.Errorf("FieldIndex(%q) = %d, want %d", name, got, want)
		}
	}
}

// changedBytes counts how many of the eight little-endian bytes of two f64s
// differ — an in-place write only rewrites those.
func changedBytes(before, after float64) int {
	var b, a [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(before))
	binary.LittleEndian.PutUint64(a[:], math.Float64bits(after))
	n := 0
	for i := range b {
		if b[i] != a[i] {
			n++
		}
	}
	return n
}

// bitmap packs up to eight bools the way Arrow does, LSB first. The fixture is
// five rows, so one byte holds the whole valid column.
func bitmap(values []bool) byte {
	var mask byte
	for i, v := range values {
		if v {
			mask |= 1 << (uint(i) & 7)
		}
	}
	return mask
}

func validOf(rows []order) []bool {
	out := make([]bool, len(rows))
	for i, r := range rows {
		out[i] = r.Valid
	}
	return out
}

// TestSetRoundTrip is the real proof of the mutation contract: the written
// values read back after a fresh parse of the mutated buffer, every other column
// still matches the ground truth, and — byte for byte — nothing outside the two
// target value buffers moved.
func TestSetRoundTrip(t *testing.T) {
	wire, want := fixture(t)
	stream, batch := orderBatch(t, wire)

	wantUsd := []float64{110.0, 1.0, 6800.0, 60000.0, 2.0}
	wantValid := []bool{true, false, true, true, false}
	if len(wantUsd) != batch.Rows {
		t.Fatalf("fixture has %d rows, test expects %d", batch.Rows, len(wantUsd))
	}
	for row := range batch.Rows {
		if err := batch.SetFloat64("usd_amount", row, wantUsd[row]); err != nil {
			t.Fatalf("SetFloat64(usd_amount, %d): %v", row, err)
		}
		if err := batch.SetBool("valid", row, wantValid[row]); err != nil {
			t.Fatalf("SetBool(valid, %d): %v", row, err)
		}
	}

	mutated := stream.Buf
	if len(mutated) != len(wire) {
		t.Fatalf("mutation resized the stream: %d bytes, want %d", len(mutated), len(wire))
	}
	// The mutation may only touch the bytes the new values actually differ in:
	// the five usd_amount doubles and the one bitmap byte holding all five valid
	// bits. Anything more means the framing, a flatbuffer or the __alive segment
	// moved. The expected count is derived from the values, not hard-coded.
	wantDiff := 0
	for row, v := range wantUsd {
		wantDiff += changedBytes(want[row].UsdAmount, v)
	}
	if bitmap(wantValid) != bitmap(validOf(want)) {
		wantDiff++
	}
	diff := 0
	for i := range mutated {
		if mutated[i] != wire[i] {
			diff++
		}
	}
	if diff != wantDiff {
		t.Errorf("%d bytes changed, want exactly %d", diff, wantDiff)
	}

	_, reparsed := orderBatch(t, mutated)
	gotUsd := floats(t, reparsed, "usd_amount")
	gotValid := bools(t, reparsed, "valid")
	for row := range reparsed.Rows {
		if math.Abs(gotUsd[row]-wantUsd[row]) > tolerance {
			t.Errorf("row %d usd_amount = %v, want %v", row, gotUsd[row], wantUsd[row])
		}
		if gotValid[row] != wantValid[row] {
			t.Errorf("row %d valid = %v, want %v", row, gotValid[row], wantValid[row])
		}
	}

	// Neighbouring columns must be exactly what they were: patch the ground
	// truth with the two written columns and re-check all nine.
	for i := range want {
		want[i].UsdAmount = wantUsd[i]
		want[i].Valid = wantValid[i]
	}
	assertColumns(t, reparsed, want)
}

func TestComponentAbsent(t *testing.T) {
	wire, _ := fixture(t)
	stream, err := arrowipc.Parse(wire)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := stream.Component("Nope"); err == nil {
		t.Fatal("Component(Nope) succeeded, want an error")
	}
}

func TestSetOnUtf8Rejected(t *testing.T) {
	wire, _ := fixture(t)
	stream, batch := orderBatch(t, wire)
	before := make([]byte, len(stream.Buf))
	copy(before, stream.Buf)

	if err := batch.SetFloat64("settlement", 0, 1.0); err == nil {
		t.Fatal("SetFloat64 on the Utf8 settlement column succeeded, want an error")
	}
	for i := range stream.Buf {
		if stream.Buf[i] != before[i] {
			t.Fatalf("rejected write still changed byte %d", i)
		}
	}
}

func TestSetRejectsTypeAndRange(t *testing.T) {
	wire, _ := fixture(t)
	_, batch := orderBatch(t, wire)

	if err := batch.SetFloat64("valid", 0, 1.0); err == nil {
		t.Error("SetFloat64 on a Bool column succeeded, want an error")
	}
	if err := batch.SetBool("amount", 0, true); err == nil {
		t.Error("SetBool on a Float64 column succeeded, want an error")
	}
	if err := batch.SetFloat64("usd_amount", batch.Rows, 1.0); err == nil {
		t.Error("SetFloat64 past the last row succeeded, want an error")
	}
	if err := batch.SetFloat64("usd_amount", -1, 1.0); err == nil {
		t.Error("SetFloat64 at row -1 succeeded, want an error")
	}
	if err := batch.SetFloat64("nope", 0, 1.0); err == nil {
		t.Error("SetFloat64 on an unknown field succeeded, want an error")
	}
	if _, err := batch.Float64s("region"); err == nil {
		t.Error("Float64s on a Utf8 column succeeded, want an error")
	}
	if _, err := batch.Strings("amount"); err == nil {
		t.Error("Strings on a Float64 column succeeded, want an error")
	}
}

func TestParseRejectsMalformedFraming(t *testing.T) {
	wire, _ := fixture(t)

	if _, err := arrowipc.Parse(nil); err == nil {
		t.Error("Parse(nil) succeeded, want an error")
	}
	if _, err := arrowipc.Parse([]byte{4, 0, 0, 0}); err == nil {
		t.Error("Parse of a segment length with no segment succeeded, want an error")
	}
	if _, err := arrowipc.Parse(wire[:len(wire)-1]); err == nil {
		t.Error("Parse of a truncated stream succeeded, want an error")
	}
	if _, err := arrowipc.Parse(append(append([]byte{}, wire...), 0)); err == nil {
		t.Error("Parse of a stream with trailing bytes succeeded, want an error")
	}

	// A segment whose first message lost its continuation marker is not an
	// Arrow IPC stream, and must be reported rather than read past.
	corrupt := append([]byte{}, wire...)
	binary.LittleEndian.PutUint32(corrupt[4:], 0)
	stream, err := arrowipc.Parse(corrupt)
	if err != nil {
		t.Fatalf("Parse of a framing-valid stream: %v", err)
	}
	if _, err := stream.Component("Order"); err == nil {
		t.Error("Component on a segment with no continuation marker succeeded, want an error")
	}
}

// TestOrderSchemaConstant checks the generated constants against the emitter's
// own output: a stale schema_gen.go would otherwise only surface as a host-side
// fingerprint mismatch at load time.
func TestOrderSchemaConstant(t *testing.T) {
	schema, err := arrowipc.OrderSchemaIPC()
	if err != nil {
		t.Fatalf("OrderSchemaIPC: %v", err)
	}
	if len(schema) < 8 || binary.LittleEndian.Uint32(schema) != 0xFFFFFFFF {
		t.Fatalf("decoded schema does not open with an IPC continuation marker: %x", schema[:min(8, len(schema))])
	}
	if want := readGenerated(t, "order_schema.ipc"); string(schema) != string(want) {
		t.Errorf("OrderSchemaIPCBase64 decodes to %d bytes, order_schema.ipc holds %d", len(schema), len(want))
	}
	if want := string(readGenerated(t, "order_fingerprint.txt")); arrowipc.OrderFingerprint != want {
		t.Errorf("OrderFingerprint = %q, want %q", arrowipc.OrderFingerprint, want)
	}
	if _, err := base64.StdEncoding.DecodeString(arrowipc.OrderSchemaIPCBase64); err != nil {
		t.Errorf("OrderSchemaIPCBase64 is not valid base64: %v", err)
	}
}
