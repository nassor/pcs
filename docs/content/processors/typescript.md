+++
title = "A TypeScript processor"
description = "jco on Node 24, a component declared as an object literal, the row type inferred from it, and the four jco gotchas the SDK cannot absorb."
template = "page.html"
weight = 6
aliases = ["/guests/typescript/", "/guests/javascript/", "/processors/javascript/"]
+++

# A TypeScript processor

`jco` componentizes a TypeScript or JavaScript module into a WASI 0.2
component using StarlingMonkey. TypeScript costs one config file and buys a
compile error whenever a transform stops matching its row type.

Every block below is from `examples/polyglot/stages/ts-score/`, stage 3 of the
polyglot example. It reads `usd_amount` and writes `risk_score` and `flagged`.
The stage is a component declaration and two transforms: `@nassor/pcs-sdk` owns
the descriptor, the schema fingerprint, the Arrow decode and encode, and the
`run-error` shape.

## 1. Install

Requires **Node 24.12 or newer**; CI verifies 24. jco sets that floor, not
TypeScript: `node --test` strips types from 22.18, but jco's `@napi-rs/lzma`
declares `engines: ^22.20 || ^24.12 || >=25`.

```bash,name=Install jco and generate the world types
npm install --save-dev @bytecodealliance/jco@1.30.0 typescript@5.9.3 @types/node@24.10.1
npx jco types ../../../../crates/pcs-processor/wit --world-name pcs-pipeline -o types/
```

The processor itself imports `@nassor/pcs-sdk`, which the stage's
`package.json` links; the next section is that file. `jco types` writes the
world's TypeScript declarations into `types/`, which is what `wit.d.ts` points
the `pcs:pipeline/host-io@0.3.0` specifier at. `npm run types` is a build step
and `types/` is generated, never committed.

## 2. package.json

```json,name=package.json for the score stage
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
    "build": "jco componentize score.ts --wit ../../../../crates/pcs-processor/wit --world-name pcs-pipeline --disable http --disable fetch-event -o score-ts.wasm",
    "types": "jco types ../../../../crates/pcs-processor/wit --world-name pcs-pipeline -o types",
    "typecheck": "npm run types && tsc"
  },
  "dependencies": {
    "@nassor/pcs-sdk": "file:../../../../packages/pcs-sdk-ts"
  },
  "devDependencies": {
    "@bytecodealliance/jco": "1.30.0",
    "@types/node": "24.10.1",
    "typescript": "5.9.3"
  }
}
```

`"type": "module"` is mandatory: jco only consumes ES modules. `--bundle` is not
in the build script because jco bundles a TypeScript entrypoint on its own, and
bundling is not optional: StarlingMonkey's loader resolves neither a relative
module nor a bare specifier at wizer time. `engines` restates jco's own floor so
an old Node fails by name; see section 5. The SDK is a `file:` link here because
it lives in this repository, and it carries the codec as internal source
(`src/arrow_ipc.ts`, re-exported from the package entry).

## 3. tsconfig.json

Nothing emits. `jco componentize` transpiles for the component, so `tsc` runs as
a checker only.

```json,name=tsconfig.json runs as a checker only
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
    "noFallthroughCasesInSwitch": true,
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
parameter properties, the TypeScript features Node cannot strip, so a source that
type-checks is a source Node can run. It is also why the SDK declares a
component with an object literal instead of decorators: TC39 decorators do not
survive the Oxc transform jco runs, and the legacy ones carry `design:type`
metadata only, which cannot describe a field list.

The WIT import specifier needs a home, and a tsconfig `paths` entry is the wrong
one; see section 5. `wit.d.ts` gives it one:

```ts,name=wit.d.ts types the versioned host-io import
declare module 'pcs:pipeline/host-io@0.3.0' {
  export { log, metric, getConfig, type LogLevel } from './types/interfaces/pcs-pipeline-host-io.js';
}
```

## 4. score.ts

`component` takes the declaration and returns a spec: property order is wire
order, and the SDK converts camelCase property names to snake_case columns, so
`usdAmountDisplay` addresses `usd_amount_display`. `InferRow` turns the same
declaration into the row type, so no code generator sits between the two.

```ts,name=score.ts declares the component and its row type
import { component, transform, transformBatch, processor, type InferRow } from '@nassor/pcs-sdk';

const Order = component('Order', {
  id: 'i64',
  region: 'utf8',
  currency: 'utf8',
  amount: 'f64',
  valid: 'bool',
  usdAmount: 'f64',
  usdAmountDisplay: 'utf8',
  riskScore: 'f64',
  flagged: 'bool',
  fee: 'f64',
  reviewTier: 'i64',
  settlement: 'utf8',
} as const);

/** The row type the declaration implies, erased before jco ever sees it. */
type Order = InferRow<typeof Order>;

/** USD volume at which a row scores 1.0 and gets flagged. */
const DEFAULT_RISK_THRESHOLD = 50000;
```

`'i64' | 'f64' | 'bool' | 'utf8'` are the wire format's four types. The
declaration is checked and encoded into descriptor bytes at module
initialisation, which jco snapshots into the component, so an unknown type or
two properties colliding on one column name is a build-time throw and nothing
runs on the hot path.

A `transform` runs over one row. Writes to `row` are what the batch returns,
because the SDK re-encodes the rows after every transform has run:

```ts,name=The per row scoring transform
const score = transform(Order, (row: Order, config) => {
  const threshold = config.float('risk_threshold', DEFAULT_RISK_THRESHOLD);
  row.riskScore = row.usdAmount / threshold;
  row.flagged = row.riskScore >= 1.0;
});
```

`config` is the whole of `host-io` a transform can reach: `float`, `metric` and
`log`. `float` refuses a value that is not a positive finite number, because a
threshold that is not a usable magnitude is an operator error rather than a row
error. Reads are memoised per batch, so asking per row costs one `get-config`
call.

A `transformBatch` runs once, after the per-row transforms, over the whole batch.
One metric observation and one log line belong here; per row they would each be
multiplied by the row count:

```ts,name=The batch report and the exported processor
const report = transformBatch(Order, (rows, config) => {
  let flaggedRows = 0;
  for (const row of rows) {
    if (row.flagged) {
      flaggedRows += 1;
    }
  }
  const threshold = config.float('risk_threshold', DEFAULT_RISK_THRESHOLD);
  config.metric('score.flagged_rows', flaggedRows);
  config.log(
    'info',
    'score',
    `scored ${rows.length} rows against threshold ${threshold}, flagged ${flaggedRows}`,
  );
});

export const pipeline = processor('polyglot-score-ts', '0.1.0', score, report);
```

`export const pipeline` is the shape jco looks for: the `pcs-pipeline` world
exports an interface, so the entrypoint's export is an object with one method per
interface function, and `processor` returns exactly that. Transforms run in
registration order. The processor reports `stateful: false`, returns
`checkpoint: undefined` and ignores `prior`, and forwards every segment it did
not declare byte for byte, the `__alive` bitmap included.

## 5. Four gotchas

<div class="note note-warn">
<span class="note-label">The versioned import, and why its types are not in tsconfig</span>

The host-io import specifier must carry its version,
`'pcs:pipeline/host-io@0.3.0'`, not the unversioned form, which fails at wizer
time with `ReferenceError: Error loading module "pcs:pipeline/host-io" ... No
such file or directory`. Typing it through a tsconfig `paths` entry then breaks
the build, because jco's bundler reads the same field, resolves the specifier to
a declaration file and reports `[MISSING_EXPORT] "getConfig" is not exported`.
An ambient `declare module` in `wit.d.ts` is invisible to the bundler, so the
import stays external and still type-checks. The SDK carries that import in one
file, `index.ts`, and its own copy of the declaration.

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
direction, do **not** disable `clocks`: the SDK reads `Date.now()` for
`run-metrics.wall-ns`, and StarlingMonkey's clock is millisecond-resolution
already, so a small batch reports 0 ns.

</div>

<div class="note note-warn">
<span class="note-label">Values don't cross the boundary as themselves</span>

`wallNs`, `rowsIn` and `rowsOut` are `BigInt`, not `Number`, and the WIT
declarations say so; the SDK builds them. `list<u8>` arrives from a different
realm: componentize-js lifts it into a `Uint8Array` whose prototype is not the
local one, so `input instanceof Uint8Array` is `false` and
`input.constructor !== Uint8Array`, while `constructor.name` is still
`'Uint8Array'`. A realm-agnostic check is the only reliable one:
`ArrayBuffer.isView(x) && x.BYTES_PER_ELEMENT === 1`.

</div>

The error path is the one the SDK handles for you, and it is worth knowing why.
componentize-js lowers a *thrown* value into the WIT `err` arm, but it re-throws
anything that is an `instanceof Error` instead of lowering it, which traps the
component. So `runBatch` throws a plain object, always:

```ts,name=How runBatch reports a permanent error
throw { tag: 'permanent', val: String(err) };
```

`permanent` is the arm the WIT contract designates for bad input shape and
processor bugs. A malformed batch or a bad config value will not fix itself on a
retry, and `run-batch` must never surface `schema-mismatch`.

## 6. The schema fingerprint

`pipeline-descriptor.schema-fingerprint` is derived, not embedded. The SDK hashes
each component's name, its version as four little-endian bytes, and its column
names in schema order, with FNV-1a, over the components sorted by name. Names
only: adding a field changes the value, retyping one does not.

Every language's SDK walks those same bytes, so the six polyglot stages report
one value from six independently written declarations. The driver
`examples/polyglot/polyglot_orders.rs` and the `polyglot_chain` integration test
load all six and compare their fingerprints against each other, and exit
non-zero on any disagreement.

## 7. Test, build, validate

Everything in the SDK except the WIT import lives in `core.ts`, so a test binds a
stub host and drives a processor end to end under Node with no component in the
picture. The SDK's suite and the codec's now live with one package:

```bash,name=Run the SDK test suite
cd packages/pcs-sdk-ts && npm ci && npm run typecheck && npm run build && npm test
```

The stage itself only type-checks:

```bash,name=Type check the stage
npm run typecheck
```

```bash,name=Componentize the stage then validate it
npx jco componentize score.ts \
    --wit ../../../../crates/pcs-processor/wit \
    --world-name pcs-pipeline \
    --disable http \
    --disable fetch-event \
    -o score-ts.wasm

wasm-tools validate --features component-model score-ts.wasm
wasm-tools component wit score-ts.wasm | grep 'pcs:pipeline'
```

```text,name=Expected wasm-tools output
  import pcs:pipeline/host-io@0.3.0;
  import pcs:pipeline/types@0.3.0;
  export pcs:pipeline/pipeline@0.3.0;
```

## Where to go next

- [The WIT contract](@/processors/wit-contract.md): every field the descriptor
  fills in, and what the host checks it against.
- [The wire format](@/reference/wire-format.md): the bytes the codec
  inside `@nassor/pcs-sdk` implements.
- [Six languages, one pipeline](@/processors/_index.md#six-languages-one-pipeline): this stage in its
  chain.
