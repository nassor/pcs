+++
title = "A TypeScript guest"
description = "jco on Node 24, a WIT world that type-checks the guest export, the six gotchas that cost the most time, and a codec with zero npm dependencies of its own."
template = "page.html"
weight = 5
aliases = ["/guests/javascript/"]
+++

# A TypeScript guest

`jco` componentizes a TypeScript or JavaScript module into a WASI 0.2 component
using StarlingMonkey. TypeScript costs one config file and buys a compile error
whenever the guest stops matching the WIT world. It also has the most gotchas of
the six languages, so read section 5 before you start debugging.

Every block below is from `examples/polyglot/stages/ts-score/`, stage 3 of the
polyglot example. It reads `usd_amount` and writes `risk_score` and `flagged`.

## 1. Install

Requires **Node 24.12 or newer**; CI verifies 24. jco sets that floor, not
TypeScript: `node --test` strips types from 22.18, but jco's `@napi-rs/lzma`
declares `engines: ^22.20 || ^24.12 || >=25`.

```bash
npm install --save-dev @bytecodealliance/jco@1.30.0 typescript@5.9.3 @types/node@24.10.1
npx jco types ../../../../crates/pcs-guest/wit --world-name pcs-pipeline -o types/
```

`jco types` writes the world's TypeScript declarations into `types/`. In a
JavaScript guest they are editor decoration. Here they are the contract the
compiler checks the guest against, so `npm run types` is a build step and
`types/` is generated, never committed.

## 2. package.json

```json
{
  "name": "polyglot-score-ts",
  "version": "0.1.0",
  "private": true,
  "description": "Stage 3 of the PCS polyglot example: scores usd_amount into risk_score/flagged.",
  "type": "module",
  "engines": {
    "node": ">=24.12"
  },
  "scripts": {
    "build": "jco componentize score.ts --wit ../../../../crates/pcs-guest/wit --world-name pcs-pipeline --disable http --disable fetch-event -o score-ts.wasm",
    "types": "jco types ../../../../crates/pcs-guest/wit --world-name pcs-pipeline -o types",
    "typecheck": "npm run types && tsc"
  },
  "dependencies": {
    "@nassor/pcs-arrow-ipc": "file:../../../../packages/arrow-ipc-ts"
  },
  "devDependencies": {
    "@bytecodealliance/jco": "1.30.0",
    "@types/node": "24.10.1",
    "typescript": "5.9.3"
  }
}
```

`"type": "module"` is mandatory: jco only consumes ES modules. `--bundle` is
not in the build script because jco bundles a TypeScript entrypoint on its own.
`engines` restates jco's own floor so an old Node fails by name; see section 5.
The codec is a `file:` link here because it lives in this repository; a release
install is `npm install @nassor/pcs-arrow-ipc`.

## 3. tsconfig.json

Nothing emits. `jco componentize` transpiles for the component, so `tsc` runs as
a checker only.

```json
{
  "compilerOptions": {
    "target": "es2023",
    "lib": ["ES2023"],
    "module": "nodenext",
    "moduleResolution": "nodenext",
    "types": ["node"],
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "allowImportingTsExtensions": true,
    "erasableSyntaxOnly": true,
    "verbatimModuleSyntax": true,
    "noEmit": true,
    "skipLibCheck": true
  },
  "include": ["*.ts", "types/**/*.d.ts"]
}
```

`erasableSyntaxOnly` is the important one. It rejects enums, namespaces and
parameter properties, the TypeScript features Node cannot strip, so a source
that type-checks is a source Node can run.

`allowImportingTsExtensions` covers `./schema_gen.ts`. The codec is a bare
specifier resolved through its own `exports` map, so no `paths` entry is
involved.

The WIT import specifier needs a home. `wit.d.ts` gives it one:

```ts
declare module 'pcs:pipeline/host-io@0.2.0' {
  export { log, metric, getConfig, type LogLevel } from './types/interfaces/pcs-pipeline-host-io.js';
}
```

## 4. score.ts

The export is a plain object named after the WIT interface, with camelCase
method names. Typing it as `typeof WitPipeline` is what TypeScript adds: a
`describe` missing `schemaFingerprint`, or a `wallNs` returned as a number, is a
compile error instead of a load failure on the host. The full file is
`examples/polyglot/stages/ts-score/score.ts`.

```ts
import { log, metric, getConfig } from 'pcs:pipeline/host-io@0.2.0';

import type * as WitPipeline from './types/interfaces/pcs-pipeline-pipeline.js';

import { PcsStream, decodeBase64 } from '@nassor/pcs-arrow-ipc';
import { ORDER_SCHEMA_IPC_BASE64, ORDER_FINGERPRINT } from './schema_gen.ts';

const COMPONENT = 'Order';
const DEFAULT_RISK_THRESHOLD = 50000;

// Decoded at module load: jco snapshots the initialised module into the
// component, so this 584-byte decode never runs on the hot path.
const ORDER_SCHEMA_IPC = decodeBase64(ORDER_SCHEMA_IPC_BASE64);

export const pipeline: typeof WitPipeline = {
  describe() {
    return {
      name: 'polyglot-score-ts',
      version: '0.1.0',
      components: [{ name: COMPONENT, arrowSchemaIpc: ORDER_SCHEMA_IPC }],
      stateful: false,
      schemaFingerprint: ORDER_FINGERPRINT,
    };
  },

  runBatch(input) {
    // StarlingMonkey's clock is millisecond-resolution, so a small batch
    // reports 0 ns.
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
```

`checkpoint: undefined` is how a stateless guest returns the WIT
`option<checkpoint>` none arm. `runBatch` declares one parameter because this
stage ignores `prior`; the contextual type supplies the rest.

## 5. Six gotchas

<div class="note note-warn">
<span class="note-label">The versioned import, and why its types are not in tsconfig</span>

The host-io import specifier must carry its version,
`'pcs:pipeline/host-io@0.2.0'`, not the unversioned form, which fails at wizer
time with `ReferenceError: Error loading module "pcs:pipeline/host-io" ... No
such file or directory`. Typing it through a tsconfig `paths` entry then breaks
the build, because jco's bundler reads the same field, resolves the specifier to
a declaration file and reports `[MISSING_EXPORT] "getConfig" is not exported`.
An ambient `declare module` in `wit.d.ts` is invisible to the bundler, so the
import stays external and still type-checks.

</div>

<div class="note note-warn">
<span class="note-label">Import <code>./schema_gen.ts</code>, not <code>./schema_gen.js</code></span>

Node's type stripping never rewrites a `.js` specifier to `.ts`, so a relative
import names its real file and `allowImportingTsExtensions` tells `tsc` that is
deliberate. This is also why bundling is not optional: StarlingMonkey's loader
resolves neither a relative module nor a bare specifier at wizer time, and
`jco componentize` bundles a TypeScript entrypoint automatically. A JavaScript
entrypoint still needs an explicit `--bundle`.

</div>

<div class="note note-warn">
<span class="note-label">An old Node fails as a missing native binding</span>

jco pulls in `@napi-rs/lzma`, whose `engines` are
`^22.20 || ^24.12 || >=25`. Its platform binding is an *optional* dependency,
and npm skips an optional dependency that fails its engine check without
failing the install. On Node 22.18 the tree installs cleanly and
`jco componentize` then dies on `Cannot find native binding`, pointing at an
npm bug that is not the cause. The stage declares `"engines": { "node":
">=24.12" }`, which turns that into an `EBADENGINE` line naming the version.

</div>

<div class="note note-warn">
<span class="note-label">Don't just <code>--disable http</code></span>

`--disable http` alone is not enough to drop `wasi:http`. Pair it with
`--disable fetch-event`, or the component still imports `wasi:http/types@0.2.x`
and fails to instantiate against a host that links plain WASI. In the other
direction, do **not** disable `clocks`: `Date.now()` returns garbage and
`run-metrics.wall-ns` becomes fiction.

</div>

<div class="note note-warn">
<span class="note-label">Values don't cross the boundary as themselves</span>

`wallNs`, `rowsIn` and `rowsOut` are `BigInt`, not `Number`, and the WIT
declarations say so. `list<u8>` arrives from a different realm: componentize-js
lifts it into a `Uint8Array` whose prototype is not the local one, so `input
instanceof Uint8Array` is `false` and `input.constructor !== Uint8Array`, while
`constructor.name` is still `'Uint8Array'`. The static type cannot see that, so
keep a realm-agnostic check at the boundary:
`ArrayBuffer.isView(x) && x.BYTES_PER_ELEMENT === 1`.

</div>

<div class="note note-warn">
<span class="note-label">The error path is not what you'd guess</span>

componentize-js lowers a *thrown* value into the WIT `err` arm, but it re-throws
anything that is an `instanceof Error` instead of lowering it. Throw a plain
object, `{ tag: 'permanent', val: msg }`; throwing `new Error(msg)` traps the
guest instead of returning a `run-error`.

</div>

## 6. The Arrow codec

`apache-arrow` on npm is unproven under StarlingMonkey, so the stage depends on
`@nassor/pcs-arrow-ipc` instead: 620 lines with **zero npm dependencies**,
covering segment splitting, the flatbuffer reads, typed column readers, and
in-place setters for fixed-width fields.

```bash
npm install @nassor/pcs-arrow-ipc
```

Import it by bare specifier. jco bundles with Rolldown under
`platform: "neutral"`, where `resolve.mainFields` is empty, so the package ships
an `exports` map and compiled JavaScript for that resolution to work.
Alternatively, write your own against
[the wire format](@/reference/wire-format.md).

The Arrow type discriminants are a four-member union, so the buffer-slot and
type-name tables are total lookups and an unsupported `type_type` is rejected
where it is read rather than surfacing later as an undefined slot count.

`stream.toBytes()` returns the input buffer mutated, which is why this stage can
write `risk_score` and `flagged`, both fixed-width, and could not write a `Utf8`
column.

## 7. Test, build, validate

The codec is plain TypeScript, so it tests under Node with no component in the
picture. Its suite lives with the package:

```bash
cd packages/arrow-ipc-ts
npm ci && npm run typecheck && npm run build && npm test
```

The stage itself only type-checks:

```bash
npm run typecheck
```

```bash
npx jco componentize score.ts \
    --wit ../../../../crates/pcs-guest/wit \
    --world-name pcs-pipeline \
    --disable http \
    --disable fetch-event \
    -o score-ts.wasm

wasm-tools validate --features component-model score-ts.wasm
wasm-tools component wit score-ts.wasm | grep 'pcs:pipeline'
```

```text
  import pcs:pipeline/host-io@0.2.0;
  import pcs:pipeline/types@0.2.0;
  export pcs:pipeline/pipeline@0.2.0;
```

## Where to go next

- [The WIT contract](@/guests/wit-contract.md): every field `describe` fills in,
  and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes
  `@nassor/pcs-arrow-ipc` implements.
- [Six languages, one pipeline](@/guests/six-languages.md): this stage in its
  chain.
