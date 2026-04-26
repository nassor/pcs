---
name: Canudo→PCS Rename
description: Project-wide rename from canudo/Canudo to pcs/PCS (Pipeline Component System); all identifiers, metrics, env vars, binary name, and IPC metadata key updated
type: project
---

Project was renamed from `canudo` to `pcs` (Pipeline Component System).

**Why:** Pre-1.0 branding decision; user explicitly approved all breaking changes including persistence-critical literals.

**Changes applied:**
- Crate `name = "canudo"` → `name = "pcs"` in Cargo.toml
- Binary `canudo-service` → `pcs-service`; directory `src/bin/canudo-service/` → `src/bin/pcs-service/` (via git mv)
- `CanudoError` → `PcsError`, `CanudoResult` → `PcsResult`
- `CanudoTypeConfig` → `PcsTypeConfig` (openraft type config in distributed consensus)
- IPC metadata key `__canudo_component` → `__pcs_component` (breaking: existing checkpoint files unreadable)
- All `canudo_` metric/tracing prefixes → `pcs_` (12 Prometheus metrics, tracing targets, test db filenames)
- `CANO_CONFIG`, `CANO_ADDR`, `CANO_LOG_FORMAT`, `CANO_LOG_LEVEL`, `CANO_NODE_ID`, `CANO_HTTP_PORT` → `PCS_*`
- `CANO_DATA_DIR`, `CANO_BOOTSTRAP` → `PCS_DATA_DIR`, `PCS_BOOTSTRAP` (in YAML configs/docs)
- `docs/operations/running-cano.md` → `docs/operations/running-pcs.md` (via git mv)
- GitHub URLs preserved: `github.com/nassor/canudo`, `nassor.github.io/canudo/`
- All YAML configs updated: node names `cano-*` → `pcs-*`, output paths `/tmp/cano-*` → `/tmp/pcs-*`

**How to apply:** All `use canudo::` imports are now `use pcs::`. All error handling uses `PcsError`/`PcsResult`. Env vars for the binary use `PCS_` prefix not `CANO_`.
