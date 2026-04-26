---
name: ECS Rewrite Design Review
description: April 2026 design review of ECS rewrite -- split/join gap, SystemMeta limitations, missing conditional routing, retry boundary shift
type: project
---

Design review of ecs-rewrite branch completed 2026-04-11. World/Entity/Component foundation is sound.

**Key gaps identified:**
1. Split/Join (6 JoinStrategy variants with cancellation/timeout) has no ECS equivalent -- needs a JoinBarrier or FanOutFanIn primitive
2. SystemMeta TypeId sets miss Resource conflicts, structural changes (spawn/despawn), and semantic ordering
3. Pipeline is static; no conditional routing to replace FSM's dynamic TaskResult::Single(next_state) branching
4. Retry boundary shifts from three-phase Node lifecycle to per-System; Pipeline runner must orchestrate

**Why:** The ECS rewrite is the next major version. These gaps, if unaddressed, would lose feature parity with the current Workflow API and block migration for users relying on split/join or conditional routing.

**How to apply:** When reviewing Pipeline or System implementations, verify these four gaps are addressed. The JoinBarrier design should precede Pipeline implementation since it shapes the execution model.
