---
name: writing-pcs-docs
description: Use when writing or editing README.md, docs/ content or templates, docs diagrams and figures, crate or item level Rust doc comments, or inline code comments in this repository
---

# Writing PCS docs

## Overview

Documentation states what the system does today, in plain words, with structure drawn as SVG.

Three rules carry most of the weight:

1. **Current state only.** No history, no process.
2. **Shorter is better.** Cut until only the meaning is left.
3. **Diagrams are SVG.** Never mermaid, never ASCII art, never a table standing in for a diagram.

## Where prose lives

| Surface | What it holds | Register |
|---|---|---|
| `README.md` | the pitch and the first command | simplest, shortest |
| `docs/content/*.md` (top level) | front matter only; prose sits in `docs/templates/<name>.html` | n/a |
| `docs/templates/*.html` | the concept pages: dataset, systems, pipeline, service, ... | simple, worked examples |
| `docs/content/{native,guests,operations,reference,benchmarks}/` | markdown pages with prose and inline SVG. A `template = "page.html"` page must open its body with its own `#` heading, because that template renders no title | simple, longer |
| `AGENTS.md` | the workspace map for agents | dense, terse |
| `//!` and `///` | API reference | precise, example driven |

README and the docs site are the simple tier. If a sentence needs extra clauses to stay exact, put
the exactness in `AGENTS.md` or a doc comment and keep the page plain.

## Current state only

Every sentence describes what is true now. None narrate how the code got here.

Delete on sight: optimization rounds, task or ticket references, PR numbers, prompt or agent
mentions, "previously", "used to", "we changed", "now improved", "as of the rewrite", the old path,
benchmark run history, TODO notes.

```text
Before: After the round 3 optimization pass, run_sync was added so the scheduler
        no longer allocates a boxed future.
After:  run_sync lets the scheduler skip the boxed future.
```

Benchmark numbers are a current measurement plus method, never a trend across runs.

## Simplify

A docs paragraph is one claim, its mechanism, then a number or a command. Four sentences at most.

- Lead with the fact, no "it is worth noting that".
- One idea per sentence, about 25 words at the ceiling.
- Prefer the concrete noun: "`pcs-service` loads the component" beats "the runtime layer handles
  component acquisition".
- Cut hedges: simply, basically, essentially, quite, very, in order to.
- Past two paragraphs, it is two topics, or it wants to be a code example.

```text
Before: It is important to note that, in general, the scheduler will typically
        attempt to group systems together into stages whenever it is possible.
After:  The scheduler groups systems into one stage when their field
        declarations do not conflict.
```

## Dashes

Use em dash, en dash and hyphen only when nothing else works.

- **Em dash:** replace with a period, comma or colon. Target zero per page.
- **En dash:** write ranges as words, "100k to 100M rows" not the dashed form.
- **Hyphen:** keep identifiers (`wasm32-wasip2`, `pcs-service`) and existing compounds (field-level,
  at-least-once, row-range). Do not coin new ones or stack three: "host to guest wire format", not
  "host-to-guest-wire-format".

The em dash in a template title block (`{% block title %}Systems — PCS{% endblock %}`) is the site
title separator. Leave it.

## Diagrams are SVG

Banned in `README.md`, `docs/content/**` and `docs/templates/**`: mermaid blocks, ASCII box
drawings, and tables used for flow or structure. A table stays legal when its columns are real data:
the crate list, the feature flags, or a stage-by-stage list of what each polyglot example step
writes.

A page diagram is inline SVG inside the site's diagram frame:

```html
<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 150" role="img" aria-labelledby="x-t x-d">
        <title id="x-t">One sentence naming what the diagram shows.</title>
        <desc id="x-d">A description that replaces the picture for a reader who cannot see it.</desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="40" width="70" height="56" rx="8"/>
            <text class="t-lbl" x="12" y="76">Order</text>
        </g>
    </svg>
    </div>
</div>
```

- Style only with the `.dgm` class vocabulary in `docs/static/styles.css`. Never inline `fill`,
  `stroke` or `font-family`.
- Colour carries meaning: `-data` amber is the data plane, `-ctl` teal is the control plane, `-bnd`
  purple is a boundary such as the WASM edge. `.row-r` marks a read, `.row-w` a write.
- Author at `viewBox` width 660; `.dgm-scroll` holds that as a minimum, so a wider drawing scrolls
  instead of shrinking its labels.
- Wrap each step in `<g class="anim anim-1">` through `anim-4` for the reveal animation.
- No blank line inside the block. One ends the raw HTML block, and the rest renders as literal text.
- Keep it under about eight boxes. More is two diagrams, or one sentence.

README figures are standalone files in `docs/static/*.svg`, each with its own `<style>` since site
CSS does not reach them. Reference one with `<img src="docs/static/NAME.svg" alt="...">` and write
the alt text as a full sentence.

Benchmark charts belong to `docs/figures/bench_figures.py`, which splices SVG into
`docs/content/benchmarks/_index.md` between `<!-- fig:NAME -->` and `<!-- /fig:NAME -->` markers.
Change the numbers in the script and run `python3 docs/figures/bench_figures.py`. Never hand-edit
the emitted markup.

Every SVG needs `role="img"`, `aria-labelledby`, a `<title>` and a `<desc>` that a non-visual reader
can follow in place of the picture.

## Code comments

Code is the exception to the diagram rule: ASCII sketches and aligned tables are fine in `//` and
`//!` comments, up to roughly ten lines.

- Explain why, not what. The signature already says what.
- No ticket ids, dates, author names, prompt references, or "changed in round 2". Delete a stale
  comment instead of annotating its history.
- `///` opens with one sentence; link types as ``[`Dataset`]`` so rustdoc resolves them.
- Doc examples compile: `cargo test --workspace --all-features --doc`.

## Before you finish

- Search the diff for the banned words above, and for `—`, `–`, a mermaid fence, and box characters
  such as `┌ │ └ +---`. Nothing should match in docs.
- Re-read each new paragraph and cut one sentence. If nothing was lost, leave it cut.
- Touched `bench_figures.py`: rerun it, confirm the diff moved numbers only.
- Touched doc comments: `cargo test --workspace --all-features --doc`.
- Touched templates or content: `cd docs && python3 build-local.py`, then open
  `docs/public/index.html`. Checking search needs a server: `python3 -m http.server -d public`.

## Common mistakes

| Mistake | Fix |
|---|---|
| Mermaid block for a pipeline flow | Inline SVG using the `.dgm` classes |
| ASCII box drawing on a docs page | Same, or one sentence |
| Table whose first column is a step number | Numbered list, or a diagram |
| "This was optimized to avoid the allocation" | "This avoids the allocation" |
| Long sentence held together by two em dashes | Three short sentences, no dashes |
| Hand-edited chart inside the benchmarks page | Edit `bench_figures.py` and rerun it |
| A new hyphenated coinage | Use the words |
| Inline `fill="#e8a33d"` on a page diagram | The `blk-data` class |
