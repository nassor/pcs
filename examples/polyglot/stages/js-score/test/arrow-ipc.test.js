// Ground-truth test for the hand-rolled codec: decode the very stream the host
// produces and compare every column against the JSON the emitter wrote from the
// same Rust `Order` rows. Flatbuffer-offset bugs surface here, natively, long
// before anything is componentized.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';

import { PcsStream, decodeBase64 } from '../arrow-ipc.js';

const GENERATED = new URL('../../../generated/', import.meta.url);
const FIXTURE_PCS = new URL('fixture_input.pcs', GENERATED);
const FIXTURE_JSON = new URL('fixture_input.json', GENERATED);

const skip =
  existsSync(FIXTURE_PCS) && existsSync(FIXTURE_JSON)
    ? false
    : 'examples/polyglot/generated is absent — run `cargo run -p pcs-service --features wasm --example polyglot_orders -- emit`';

/** A fresh mutable copy of the fixture, so mutation tests cannot leak into each other. */
function fixtureBytes() {
  return new Uint8Array(readFileSync(FIXTURE_PCS));
}

function expectedRows() {
  return JSON.parse(readFileSync(FIXTURE_JSON, 'utf8'));
}

/** Every column of a batch, read through the public codec surface. */
function snapshot(batch) {
  return {
    id: batch.int64s('id').map(Number),
    region: batch.strings('region'),
    currency: batch.strings('currency'),
    amount: batch.float64s('amount'),
    valid: batch.bools('valid'),
    usd_amount: batch.float64s('usd_amount'),
    risk_score: batch.float64s('risk_score'),
    flagged: batch.bools('flagged'),
    settlement: batch.strings('settlement'),
  };
}

test('the fixture stream carries an Order segment and the alive bitmap', { skip }, () => {
  const stream = new PcsStream(fixtureBytes());
  assert.deepStrictEqual(stream.componentNames(), ['Order', '__alive']);
});

test('every Order column decodes to the emitted JSON', { skip }, () => {
  // Pass the Buffer straight through: it is a Uint8Array view that may sit at a
  // non-zero byteOffset inside a pooled ArrayBuffer, which the codec must honour.
  const batch = new PcsStream(readFileSync(FIXTURE_PCS)).component('Order');
  const rows = expectedRows();

  assert.strictEqual(batch.rows, rows.length);
  assert.deepStrictEqual(batch.fieldNames(), [
    'id',
    'region',
    'currency',
    'amount',
    'valid',
    'usd_amount',
    'risk_score',
    'flagged',
    'settlement',
  ]);

  const columns = snapshot(batch);
  for (const [row, expected] of rows.entries()) {
    for (const [field, value] of Object.entries(expected)) {
      assert.deepStrictEqual(
        columns[field][row],
        value,
        `row ${row} field ${field}: ${columns[field][row]} != ${value}`,
      );
    }
  }
});

test('int64s keeps full Int64 precision as BigInt', { skip }, () => {
  const batch = new PcsStream(fixtureBytes()).component('Order');
  assert.deepStrictEqual(batch.int64s('id'), [1n, 2n, 3n, 4n, 5n]);
});

test('in-place writes read back and leave neighbours byte-identical', { skip }, () => {
  const before = snapshot(new PcsStream(fixtureBytes()).component('Order'));

  const mutated = fixtureBytes();
  const stream = new PcsStream(mutated);
  const batch = stream.component('Order');
  batch.setFloat64('risk_score', 2, 1.75);
  batch.setBool('flagged', 2, true);
  batch.setBool('flagged', 4, true);
  batch.setBool('flagged', 4, false); // set then clear: the bit must end up back at 0

  // Re-parse the returned bytes from scratch — the same thing the host does.
  const after = snapshot(new PcsStream(stream.toBytes()).component('Order'));

  assert.strictEqual(after.risk_score[2], 1.75);
  assert.strictEqual(after.flagged[2], true);
  assert.deepStrictEqual(after.risk_score, [0, 0, 1.75, 0, 0]);
  assert.deepStrictEqual(after.flagged, [false, false, true, false, false]);

  for (const field of ['id', 'region', 'currency', 'amount', 'valid', 'usd_amount', 'settlement']) {
    assert.deepStrictEqual(after[field], before[field], `field ${field} changed`);
  }
});

test('an unknown component name is rejected', { skip }, () => {
  const stream = new PcsStream(fixtureBytes());
  assert.throws(() => stream.component('Widget'), /no segment for component "Widget"/);
});

test('writing a Utf8 column is rejected', { skip }, () => {
  const batch = new PcsStream(fixtureBytes()).component('Order');
  assert.throws(() => batch.setFloat64('settlement', 0, 1), /variable-length Utf8/);
  assert.throws(() => batch.setBool('settlement', 0, true), /variable-length Utf8/);
});

test('type, field-name and row-bound violations are rejected', { skip }, () => {
  const batch = new PcsStream(fixtureBytes()).component('Order');
  assert.throws(() => batch.float64s('valid'), /is Bool, not FloatingPoint/);
  assert.throws(() => batch.bools('amount'), /is FloatingPoint, not Bool/);
  assert.throws(() => batch.strings('id'), /is Int, not Utf8/);
  assert.throws(() => batch.float64s('nope'), /has no field "nope"/);
  assert.throws(() => batch.setFloat64('risk_score', 5, 1), /row 5 is out of range/);
  assert.throws(() => batch.setFloat64('risk_score', -1, 1), /row -1 is out of range/);
});

test('malformed input throws instead of trapping', { skip }, () => {
  const truncated = fixtureBytes().subarray(0, 40);
  assert.throws(() => new PcsStream(truncated), /claims 2568 bytes, only 36 remain/);

  const noTerminator = fixtureBytes().subarray(0, fixtureBytes().length - 2);
  assert.throws(() => new PcsStream(noTerminator), /before the segment terminator/);

  assert.throws(() => new PcsStream(new Uint8Array([9, 0, 0, 0, 1])), /claims 9 bytes, only 1 remain/);
  assert.throws(() => new PcsStream([]), /expected a byte view/);
});

test('decodeBase64 round-trips the generated schema constant', { skip }, () => {
  const schemaIpc = readFileSync(new URL('order_schema.ipc', GENERATED));
  const generated = readFileSync(new URL('schema_gen.js', GENERATED), 'utf8');
  const base64 = /ORDER_SCHEMA_IPC_BASE64 = "([^"]+)"/.exec(generated)[1];
  assert.deepStrictEqual(decodeBase64(base64), new Uint8Array(schemaIpc));
});
