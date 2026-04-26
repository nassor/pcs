---
name: Service S3 Config Schema
description: ServiceConfig YAML schema and parser implemented in src/service/; key design decisions for downstream coders.
type: project
---

Phase S3 (config schema + YAML parser) completed. Key decisions:

- `service` feature added to Cargo.toml: pulls in `io`, `distributed`, `distributed-raft`, `dep:serde_yaml`.
- `src/service/mod.rs` — thin re-export module, gated on `#[cfg(feature = "service")]`.
- `src/service/config.rs` — 952 lines; all structs, validation, env-var substitution, 14 tests.
- `examples/configs/standalone.yaml` and `cluster.yaml` — operator-facing reference fixtures.

**Why:** Downstream runners (S5 standalone, S6 cluster, S7 HTTP, S8 CLI) all load `ServiceConfig::load(path)` and call `validate()` before doing anything else.

**How to apply:**
- `ServiceMode` uses serde `tag = "mode"` with `flatten` for the mode-specific fields. YAML writers use `mode: standalone` or `mode: cluster` at the top level; mode-specific fields (`run_mode`, `peers`, `bootstrap`, etc.) are siblings, not nested.
- `serde_yaml::Value` is used for opaque `config:` fields on SystemInstance, SourceSpec, SinkSpec, ComponentInstance. The factory registry (S4) receives these values and is responsible for interpreting them.
- `substitute_env_vars` is hand-rolled (no regex crate). Handles `${VAR}` and `${VAR:-default}`. Returns `CanudoError::Configuration` for unset vars without defaults and unclosed placeholders.
- Validation rules: data_dir non-empty; cluster needs peers + node.id in peer list + lease_ttl >= 3*election_timeout; system names unique; source/sink component refs must match ComponentInstance names; http.bind parsed as SocketAddr (unless disabled).
- Edition 2024 requires `unsafe` blocks around `std::env::set_var`/`remove_var` in tests.
- `serde_yaml 0.9` (not 0.8): `from_str` returns `serde_yaml::Error`, not `serde_json::Error`. The `0.9` series marks itself `+deprecated` in crates.io but is the correct stable 0.9 release.
