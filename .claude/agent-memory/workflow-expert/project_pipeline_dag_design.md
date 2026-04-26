---
name: Pipeline DAG Scheduling Design Decision
description: ECS rewrite chose sequential stages with &mut World over Arc<RwLock<>> per-column for Pipeline scheduling
type: project
---

Pipeline DAG scheduling uses topological sort (Kahn's algorithm) into stages, with sequential system execution within each stage via `&mut World`.

**Key design decisions:**
- Conflict rule: two systems conflict if any TypeId appears in both write sets, OR in one's read set and the other's write set. Read-read sharing is safe.
- Kahn's algorithm provides topological order, level assignment (stages), and cycle detection in one pass.
- DAG validation happens at Pipeline construction time, not execution time (addressing P0 from prior review).
- Self read-write (system reads and writes same type) needs no special handling beyond normal inter-system conflict detection.

**Why:** `Arc<RwLock<>>` per component column was rejected due to lock ordering complexity, deadlock risk, and lessons from the shared-store concurrency issues in the old Workflow split/join model. Sequential stages with `&mut World` eliminates data races by construction.

**How to apply:** Future parallelism improvements should target intra-stage concurrency for read-only systems (sharing `&World`) as a follow-up, not as part of the initial design. The sequential-stage model is the foundation.
