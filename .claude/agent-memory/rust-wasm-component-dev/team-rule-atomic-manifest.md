---
name: Team rule — atomic workspace manifest edits
description: Any edit to shared manifest state must leave cargo check --workspace --all-features parseable at every intermediate commit
type: feedback
---

**Rule:** Any task editing shared manifest state (root `Cargo.toml`, `[workspace.members]`, `[[bench]]`/`[[bin]]`/`[[test]]` entries, feature graphs) must leave `cargo check --workspace --all-features` parseable at **every intermediate point**. If the edit temporarily breaks manifest parse, rebase into a single atomic change before pushing.

**Why:** A broken root manifest blocks every `cargo` command for every agent in the workspace, not just the one making the edit. Team-lead flagged this 2026-04-15 after #5 (bench split) left stale `[[bench]]` entries pointing at moved files and blocked architect's #9 verification. Not critical, but the coordination cost is real when multiple agents are working concurrently.

**How to apply (specifically for task #13):**
- Adding `crates/pcs-guest/` and `crates/pcs-guest-smoketest/` to `[workspace.members]` must land in the SAME commit as the two crate directories being created.
- Do NOT commit the `[workspace.members]` addition before the crates exist on disk — `cargo check --workspace` would fail on the missing member.
- Do NOT create the crate directories without updating `[workspace.members]` — they wouldn't be reachable from the workspace root and downstream agents would see two orphan directories.
- Workflow: stage both crate skeletons + the manifest edit, verify `cargo check --workspace --all-features` locally, then one commit.
- Same rule applies to any `[profile.release]` tweaks (lto, codegen-units, opt-level="z", panic="abort", strip=true) I might add for the guest-side build optimization — land those in the same atomic commit.

**Adjacent rule**: #20 (tests/examples conversion, coder-tests) and #26 (bin/package layout) will touch similar state. Not my lane but good to know the convention is team-wide, not task-specific.
