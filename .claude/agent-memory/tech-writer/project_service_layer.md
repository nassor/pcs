---
name: Service Layer — Implemented Design (v1.0.0-alpha.1)
description: Actual service layer implementation: CLI surface, config schema, factory model, known v1 limitations, and example config issues
type: project
---

Service layer is fully implemented as of 2026-04-12 (32-item review landed, `ecs-rewrite` branch).

**Feature flags — IMPORTANT correction from Phase S0 doc:**
- `service` — standalone-only. Includes `io`, `distributed`, `tracing`. Does NOT include Raft.
- `service-cluster` — adds `distributed-raft` (openraft, redb, TCP). Required for cluster mode.
- Earlier S0 doc said `service` implies `distributed-raft` — that was wrong. Always distinguish.

**CLI surface (verified against binary):**
- `canudo-service serve --config <path> [--node-id <id>] [--port <port>]`
- `canudo-service validate --config <path> [--strict]`
- `canudo-service status --addr <url> [--full]`
- `canudo-service cluster init|join|leave|status`
- Global flags: `-c/--config`, `--addr`, `--log-format`, `--log-level`
- Env vars: `CANO_CONFIG`, `CANO_NODE_ID`, `CANO_HTTP_PORT`, `CANO_ADDR`, `CANO_LOG_FORMAT`, `CANO_LOG_LEVEL`

**Config schema key facts:**
- `mode: standalone` / `mode: cluster` — YAML tag on a flattened enum
- `run_mode.kind: continuous|one_shot|interval` — standalone only; `interval_ms` required for `interval`
- `peers:`, `bootstrap:`, `lease_ttl_ms:`, `election_timeout_ms:`, `heartbeat_interval_ms:`, `snapshot_log_interval:` — cluster only; all top-level fields (not nested under a `cluster:` key)
- `sources:` must be ABSENT or EMPTY in cluster mode (ServiceConfig::validate enforces this)
- `http.disabled: true` bypasses bind validation
- Env-var syntax: `${VAR}` (required) and `${VAR:-default}` (with fallback)

**Eagerly-opening sinks:** `CsvSink`, `ParquetSink`, `JsonSink` call `File::create` inside `factory.build()`, which runs during `validate`. Parent directory must exist. Example configs MUST use paths like `/tmp/filename.csv` (not `/tmp/subdir/filename.csv`) unless the subdir is pre-created.

**Example configs fixed in this session:**
- `standalone.yaml` sink changed from `/tmp/cano-standalone-out/orders.csv` → `/tmp/cano-standalone-orders-out.csv`
- `cluster.yaml` sink changed from `${CANO_DATA_DIR}/events_out.csv` → `/tmp/cano-cluster-events-out.csv`
- Both now pass `validate --strict` without any preconditions

**validate output format (actual, not docs):**
```
OK: config is structurally valid
  node.id:  1
  node.name: <name>
  mode:     standalone|cluster
  systems:  N
  sources:  N
  sinks:    N
  http.bind: <addr>
  log_level: <level>
OK: all declared types resolved in built-in registry
```
(not "Config OK: N system, N source, N sink, mode=standalone" as old docs claimed)

**v1 limitations:**
- `cluster join` and `cluster leave` print manual workaround (not wired to HTTP)
- `cluster_probe` is None in serve.rs; `/status` returns `"cluster": null` in cluster mode
- `ready` flag flipped at startup, not after first successful iteration (v1.1 planned)
- `cluster init` only validates config and confirms `bootstrap: true` — writes no data

**Factory model:**
- `SystemFactory::type_name()` + `build(&serde_yaml::Value)` → `Result<BuiltSystem, CanudoError>`
- `BuiltSystem::Sequential(Box<dyn System>)` or `BuiltSystem::Parallel(Box<dyn ParallelSystem>)`
- `ComponentFactory::schema()` + default `register()` calls `world.register_raw_component`
- No built-in system factories. All processing systems require a custom binary.
- `register_builtin_factories` registers: ParquetSource/Sink, CsvSource/Sink, JsonSource/Sink, ChannelSource/Sink, GenericComponent

**Shutdown:** 30-second budget. Exceeding it forces `process::exit(1)`. SIGKILL causes claim to expire after `lease_ttl_ms` (cluster) or source re-delivery on restart (standalone).

**How to apply:** Always verify CLI behavior against `src/bin/canudo-service/commands/` before documenting. Cluster commands have significant v1 limitations. Feature flag precision matters — `service` vs `service-cluster` is a real operational difference.
