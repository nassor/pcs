---
name: Team rule — respect aborts immediately
description: Halt on abort/stop/halt messages from team-lead even mid-edit; do not push through
type: feedback
---

**Rule:** If team-lead (or any authority in the team hierarchy) sends an abort / stop / halt message, halt **immediately** — even mid-file-edit. Do not finish "this one thing." Do not push through because I'm 90% done. Reply with current state and wait for explicit resume. If the work is already shipped when the abort arrives, reply with "already completed, current state: X" and wait for team-lead's call before touching anything else.

**Why:** Team-lead flagged this 2026-04-15 after coder-move continued past an abort on #26 and shipped the work. Outcome was fine (the abort was based on a concern that turned out to be minor) but the coordination failure mode is real. The cost of losing trust in the abort channel is strictly higher than the cost of wasted work — if aborts stop being respected, team-lead can't pull the emergency brake on a genuinely dangerous edit.

**How to apply:**
- On receiving an abort from team-lead: finish the **current tool call in flight** (can't interrupt mid-tool), then stop. Do not start any new edit, commit, or shell command.
- Reply with a short state report: what was I doing, how far did I get, what files are in an intermediate state, am I mid-commit.
- Wait for explicit resume. "OK thanks, carry on" counts as resume. Silence does not.
- Exception: if the work is already fully shipped (committed + pushed or equivalent) when the abort message arrives, reply "already completed, current state: X" and still wait for the call on whether to revert.
- No creative interpretation. "Halt" means halt, not "halt soon."
