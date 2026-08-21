"""Native tests for the hand-rolled Arrow IPC codec.

Run with `python -m unittest discover` from this directory. No wasm involved:
this is where flatbuffer-offset mistakes surface, seconds after they are made,
instead of as an opaque trap inside a componentized guest.

Ground truth is the emitter's own output --- `fixture_input.pcs` decoded here
must equal `fixture_input.json` written by arrow-rs.

Use `python -m unittest test_arrow_ipc` instead once `componentize-py bindings
.` has been run here: discovery imports every package under the start
directory, and the generated `componentize_py_async_support` package imports a
module that exists only inside the component. Those stubs are for IDEs and
type checkers --- `componentize` regenerates the real bindings itself and never
reads them --- so deleting them also restores plain discovery.
"""

import json
import os
import unittest

import arrow_ipc

try:
    from schema_gen import ORDER_FINGERPRINT, ORDER_SCHEMA_IPC_BASE64
except ImportError:  # not yet copied in by scripts/build-polyglot.sh
    ORDER_FINGERPRINT = None
    ORDER_SCHEMA_IPC_BASE64 = None

_HERE = os.path.dirname(os.path.abspath(__file__))
_GENERATED = os.path.normpath(os.path.join(_HERE, "..", "..", "generated"))
_FIXTURE_PCS = os.path.join(_GENERATED, "fixture_input.pcs")
_FIXTURE_JSON = os.path.join(_GENERATED, "fixture_input.json")
_SCHEMA_IPC = os.path.join(_GENERATED, "order_schema.ipc")
_FINGERPRINT = os.path.join(_GENERATED, "order_fingerprint.txt")

_HAVE_FIXTURES = os.path.isfile(_FIXTURE_PCS) and os.path.isfile(_FIXTURE_JSON)
_MISSING = (
    "generated fixtures absent; run `cargo run -p pcs-service --features wasm "
    "--example polyglot_orders -- emit`"
)

_INT_FIELDS = ("id",)
_STRING_FIELDS = ("region", "currency", "settlement")
_FLOAT_FIELDS = ("amount", "usd_amount", "risk_score")
_BOOL_FIELDS = ("valid", "flagged")

#: Every byte of this value's IEEE-754 encoding is non-zero (0x40a1111111111111),
#: so overwriting a zeroed slot with it must flip exactly eight bytes. A round
#: number like 6800.0 would only flip three and hide a short write.
_ALL_BYTES_SET = float.fromhex("0x1.1111111111111p+11")


@unittest.skipUnless(_HAVE_FIXTURES, _MISSING)
class ArrowIpcTest(unittest.TestCase):
    """Decode, mutate, and re-decode the canonical five-row fixture."""

    @classmethod
    def setUpClass(cls):
        with open(_FIXTURE_PCS, "rb") as handle:
            cls.raw = handle.read()
        with open(_FIXTURE_JSON, "r", encoding="utf-8") as handle:
            cls.expected = json.load(handle)

    def order(self, raw=None):
        stream = arrow_ipc.PcsStream(self.raw if raw is None else raw)
        return stream, stream.component("Order")

    def columns(self, batch):
        """Every column of the batch as a list of Python values."""
        out = {}
        for name in _INT_FIELDS:
            out[name] = batch.int64s(name)
        for name in _FLOAT_FIELDS:
            out[name] = batch.float64s(name)
        for name in _BOOL_FIELDS:
            out[name] = batch.bools(name)
        for name in _STRING_FIELDS:
            out[name] = batch.strings(name)
        return out

    # -- decoding ----------------------------------------------------------

    def test_segments_are_order_then_alive(self):
        stream = arrow_ipc.PcsStream(self.raw)
        self.assertEqual(stream.component_names, ["Order", "__alive"])
        self.assertEqual(stream.component("__alive").rows, len(self.expected))

    def test_schema_field_order(self):
        _stream, batch = self.order()
        self.assertEqual(
            batch.field_names,
            [
                "id",
                "region",
                "currency",
                "amount",
                "valid",
                "usd_amount",
                "risk_score",
                "flagged",
                "settlement",
            ],
        )

    def test_every_column_matches_the_json_fixture(self):
        _stream, batch = self.order()
        self.assertEqual(batch.rows, len(self.expected))
        actual = self.columns(batch)
        for row, want in enumerate(self.expected):
            for name in _INT_FIELDS + _BOOL_FIELDS + _STRING_FIELDS:
                self.assertEqual(actual[name][row], want[name], "{}[{}]".format(name, row))
            for name in _FLOAT_FIELDS:
                self.assertAlmostEqual(
                    actual[name][row], want[name], delta=1e-6, msg="{}[{}]".format(name, row)
                )

    def test_stream_round_trips_byte_for_byte(self):
        stream, _batch = self.order()
        self.assertEqual(stream.to_bytes(), self.raw)

    # -- in-place mutation -------------------------------------------------

    def test_set_float64_writes_only_its_own_eight_bytes(self):
        stream, batch = self.order()
        before = self.columns(batch)
        batch.set_float64("usd_amount", 2, _ALL_BYTES_SET)
        mutated = stream.to_bytes()

        changed = [i for i, (a, b) in enumerate(zip(self.raw, mutated)) if a != b]
        self.assertEqual(len(changed), 8, "one f64 must touch exactly 8 bytes")
        self.assertEqual(changed, list(range(changed[0], changed[0] + 8)))

        _stream, reread = self.order(mutated)
        after = self.columns(reread)
        self.assertEqual(after["usd_amount"][2], _ALL_BYTES_SET)
        self.assertEqual(after["usd_amount"][:2], [0.0, 0.0])
        self.assertEqual(after["usd_amount"][3:], [0.0, 0.0])
        for name in set(before) - {"usd_amount"}:
            self.assertEqual(after[name], before[name], name)

    def test_set_bool_writes_only_its_own_bit(self):
        stream, batch = self.order()
        before = self.columns(batch)
        for row in (0, 2, 3):
            batch.set_bool("valid", row, True)
        mutated = stream.to_bytes()

        changed = [i for i, (a, b) in enumerate(zip(self.raw, mutated)) if a != b]
        self.assertEqual(len(changed), 1, "five bit-packed rows live in one byte")

        _stream, reread = self.order(mutated)
        after = self.columns(reread)
        self.assertEqual(after["valid"], [True, False, True, True, False])
        for name in set(before) - {"valid"}:
            self.assertEqual(after[name], before[name], name)

    def test_set_bool_clears_again(self):
        stream, batch = self.order()
        batch.set_bool("flagged", 3, True)
        batch.set_bool("flagged", 3, False)
        self.assertEqual(stream.to_bytes(), self.raw)

    # -- rejection contract ------------------------------------------------

    def test_unknown_component_raises(self):
        stream = arrow_ipc.PcsStream(self.raw)
        with self.assertRaises(ValueError):
            stream.component("Invoice")

    def test_set_float64_on_utf8_field_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.set_float64("settlement", 0, 1.0)

    def test_set_float64_on_bool_field_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.set_float64("valid", 0, 1.0)

    def test_unknown_field_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.float64s("no_such_field")

    def test_row_out_of_range_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.set_float64("usd_amount", batch.rows, 1.0)

    def test_truncated_stream_raises_instead_of_crashing(self):
        for cut in (3, 8, 64, 700, len(self.raw) - 1):
            with self.assertRaises(ValueError):
                arrow_ipc.PcsStream(self.raw[:cut])

    def test_corrupt_continuation_marker_raises(self):
        broken = bytearray(self.raw)
        broken[4] = 0x00  # first message's 0xffffffff prefix
        with self.assertRaises(ValueError):
            arrow_ipc.PcsStream(bytes(broken))


@unittest.skipUnless(
    ORDER_SCHEMA_IPC_BASE64 is not None and os.path.isfile(_SCHEMA_IPC),
    "schema_gen.py not copied in yet",
)
class SchemaConstantsTest(unittest.TestCase):
    """The generated constants must still describe the canonical `Order`."""

    def test_base64_constant_decodes_to_the_emitted_schema(self):
        with open(_SCHEMA_IPC, "rb") as handle:
            expected = handle.read()
        self.assertEqual(arrow_ipc.decode_schema_ipc(ORDER_SCHEMA_IPC_BASE64), expected)

    def test_fingerprint_constant_matches(self):
        with open(_FINGERPRINT, "r", encoding="utf-8") as handle:
            self.assertEqual(ORDER_FINGERPRINT, handle.read().strip())


if __name__ == "__main__":
    unittest.main()
