---
name: Comprehensive FSM/Workflow Design Review
description: April 2026 product review findings -- P0 issues (graph validation, retry side effects), strategic gaps (durability, sub-workflows), and prioritized roadmap
type: project
---

Comprehensive product review completed 2026-04-11 covering FSM design, split/join strategies, error recovery, composition, scheduler, and API.

**P0 Findings:**
1. No workflow graph validation at construction time -- unregistered states only discovered at runtime
2. Node retry logic re-executes prep() when post() fails, causing side effects in non-idempotent prep phases

**Key P1 Findings:**
- No max iteration guard on the orchestration loop (infinite cycle risk)
- Split tasks share store with no isolation (race condition on parallel writes)
- Join state is static -- cannot route based on split results
- No error state routing / dead letter handling (retry exhaustion kills workflow)
- Workflow hardcodes MemoryStore -- KeyValueStore trait unused generically
- No sub-workflow composition support
- Scheduler has no overlapping run protection

**Strategic Gaps (vs Temporal/Airflow/Step Functions):**
- No workflow persistence/durability (in-memory only)
- No programmatic observability API beyond tracing
- No workflow versioning for in-flight migrations

**Why:** These findings inform the product roadmap. The P0 items block production use. The durability gap is the biggest strategic differentiator missing.

**How to apply:** When evaluating feature requests or implementation plans, check against this prioritized list. P0 items should be addressed before any new features.
