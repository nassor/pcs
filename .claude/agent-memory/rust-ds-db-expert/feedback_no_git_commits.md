---
name: Team does not commit during task execution
description: Coordination is task status + files on disk, NOT git commits. Don't attempt commits per-task.
type: feedback
---

Do NOT run `git commit` when finishing a task. The working tree on disk IS the ground truth.

**Why**: This team's coordination model is (a) save files to disk, (b) update task status via TaskUpdate, (c) let sessions accumulate changes in the staging index without committing. Agents running concurrently produce interleaved staging states with hundreds of `new file` / `deleted` entries. Any per-task commit attempt either sweeps in other agents' work, or requires pathspec gymnastics to isolate — both are coordination risks. The final commit/PR happens at session end or on explicit user request, not per-task.

**How to apply**:
- After completing a task: verify tests are green, verify files are saved (the Edit tool already wrote them to disk), then `TaskUpdate status=completed`. No `git add`, no `git commit`, no `git reset`.
- If you need to show a reviewer what changed: point them at the working-tree file paths. They can read the files directly.
- The ~270-line `git status` with staged-but-uncommitted files from previous agents is EXPECTED state, not a coordination failure.
- Rule 1 ("manifest parseable at every intermediate point") is about **file content**, not git index state. A broken root Cargo.toml pointing at nonexistent benches blocks every `cargo` command. A 270-file staging interleave is invisible to cargo. The former is a real issue; the latter is cosmetic.
- Confirmed by team-lead on 2026-04-15 after I escalated a staging-state concern on #10. Same guidance was given to wasm-lead on #12 earlier the same session. Both tasks shipped clean by ignoring git.

**Exception**: only commit when the user explicitly asks, or at session-end finalization.
