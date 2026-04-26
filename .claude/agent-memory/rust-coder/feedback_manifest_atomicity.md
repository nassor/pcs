---
name: Workspace Manifest Atomicity
description: Rule for editing shared Cargo.toml manifests — changes must leave workspace parseable at every intermediate step
type: feedback
---

Workspace manifest edits must be atomic: `cargo check --workspace --all-features` must parse at every intermediate state, not just before and after.

**Why:** A broken root `Cargo.toml` (e.g. `[[bench]]` entry pointing at a moved file) blocks every `cargo` command for every agent. A stale manifest entry during task #5 blocked architect's #9 verification.

**How to apply:** When moving/deleting files referenced in root Cargo.toml (`[[bench]]`, `[[bin]]`, `[[example]]`, `[[test]]`), remove the manifest entry in the same edit as the file move — or remove the entry first, then move the file. Never leave an entry that points to a nonexistent path, even briefly. If multi-step moves are required, stage them so each intermediate commit parses clean.
