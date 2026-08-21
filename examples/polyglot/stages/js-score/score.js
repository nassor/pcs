// Stage 3 of the polyglot example — the **JavaScript** guest.
//
// Reads `usd_amount`, which the Python stage wrote, and produces the two risk
// columns the Rust stage settles on:
//
//   risk_score = usd_amount / risk_threshold      (host config, default 50000)
//   flagged    = risk_score >= 1.0
//
// Both are fixed-width, so this stage only overwrites value bytes inside the
// input buffer and hands the same array back — see `arrow-ipc.js` for the wire
// format and why a guest never writes a flatbuffer.
//
// Build (jco 1.30.0, Node 22+):
//
//   npm install
//   npm run build
//
// Three gotchas, each worth an hour:
//
// 1. The host-io import specifier carries the WIT package version. Dropping
//    `@0.2.0` fails at wizer time with `Error loading module
//    "pcs:pipeline/host-io"`.
// 2. `jco componentize` needs `--bundle` (it is in the npm script): StarlingMonkey
//    cannot resolve the relative `./arrow-ipc.js` import on its own.
// 3. componentize-js lowers a thrown value into the WIT `err` arm, but it
//    *re-throws* anything that is `instanceof Error`, which traps the
//    component. Every failure path below therefore throws the plain object
//    `{ tag: 'permanent', val }`.

import { log, metric, getConfig } from 'pcs:pipeline/host-io@0.2.0';

import { PcsStream, decodeBase64 } from './arrow-ipc.js';
import { ORDER_SCHEMA_IPC_BASE64, ORDER_FINGERPRINT } from './schema_gen.js';

/** The one component this stage declares. */
const COMPONENT = 'Order';

/** USD volume at which a row scores 1.0 and gets flagged. */
const DEFAULT_RISK_THRESHOLD = 50000;

const FIELD_USD_AMOUNT = 'usd_amount';
const FIELD_RISK_SCORE = 'risk_score';
const FIELD_FLAGGED = 'flagged';

// Decoded at module load: jco snapshots the initialised module into the
// component, so this 584-byte decode never runs on the hot path.
const ORDER_SCHEMA_IPC = decodeBase64(ORDER_SCHEMA_IPC_BASE64);

/**
 * Read a numeric host config value.
 *
 * `get-config` hands over strings and leaves parsing to the guest; a value that
 * is not a usable positive number is an operator error, not a row error, so it
 * fails the whole batch rather than silently scoring every row `Infinity`.
 */
function configNumber(key, fallback) {
  const raw = getConfig(key);
  if (raw === undefined || raw === null) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`config "${key}" must be a positive number, got ${JSON.stringify(raw)}`);
  }
  return value;
}

export const pipeline = {
  describe() {
    // Every field is a generated constant: the schema bytes and the fingerprint
    // come from the canonical Rust `Order`, so this stage cannot drift from the
    // three others. The driver asserts all four fingerprints are equal.
    return {
      name: 'polyglot-score-js',
      version: '0.1.0',
      components: [{ name: COMPONENT, arrowSchemaIpc: ORDER_SCHEMA_IPC }],
      stateful: false,
      schemaFingerprint: ORDER_FINGERPRINT,
    };
  },

  /**
   * Score one batch.
   *
   * `prior` is ignored: this stage is stateless, so it returns no checkpoint and
   * the host persists nothing for it.
   */
  runBatch(input, prior) {
    // StarlingMonkey's clock is millisecond-resolution, so a small batch
    // honestly reports 0 ns. The number is measured, never invented.
    const startedMs = Date.now();
    try {
      const threshold = configNumber('risk_threshold', DEFAULT_RISK_THRESHOLD);
      const stream = new PcsStream(input);
      const batch = stream.component(COMPONENT);

      const usdAmount = batch.float64s(FIELD_USD_AMOUNT);
      let flaggedRows = 0;
      for (let row = 0; row < batch.rows; row += 1) {
        const score = usdAmount[row] / threshold;
        const flagged = score >= 1.0;
        batch.setFloat64(FIELD_RISK_SCORE, row, score);
        batch.setBool(FIELD_FLAGGED, row, flagged);
        if (flagged) {
          flaggedRows += 1;
        }
      }

      metric('score.flagged_rows', flaggedRows);
      log(
        'info',
        'score',
        `scored ${batch.rows} rows against threshold ${threshold}, flagged ${flaggedRows}`,
      );

      const rows = BigInt(batch.rows);
      return {
        output: stream.toBytes(),
        checkpoint: undefined,
        metrics: {
          wallNs: BigInt(Date.now() - startedMs) * 1_000_000n,
          rowsIn: rows,
          rowsOut: rows,
          systemsRun: 1,
          retries: 0,
        },
      };
    } catch (err) {
      // A malformed batch or a bad config value will not fix itself on a retry,
      // and `run-batch` must never surface `schema-mismatch`.
      throw { tag: 'permanent', val: String(err) };
    }
  },
};
