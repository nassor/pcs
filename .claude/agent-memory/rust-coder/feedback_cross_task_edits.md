---
name: No Unilateral Cross-Task Edits
description: Rule for touching code owned by another agent's in-flight task — stop, message, wait for ack
type: feedback
---

Never unilaterally edit code owned by another agent's in-flight task, even to fix a compile error blocking your own task.

**Why:** Architect had shipped `PipelineRuntime` trait shape as part of task #9. Changing `#[async_trait]` to `#[async_trait(?Send)]` during an unrelated bench task (#5) was an uncoordinated design change with downstream implications (#10 DistributedRunner). The right path was SendMessage to architect + team-lead with the proposed fix and wait for ack.

**How to apply:** When a compile error requires touching shared design surface (trait definitions, feature flags, public API) owned by another in-flight task, stop work, SendMessage the owner + team-lead with: what the error is, what the proposed fix is, and which task owns the code. Wait for explicit ack before editing. "30 seconds to ask, avoids coordination smell."
