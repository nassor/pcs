---
name: Service S8 CLI + Binary
description: canudo-service binary layout, serde_yaml_ng migration, integration test patterns
type: project
---

Phase S8 (Wave C) delivered the canudo-service binary and integration tests.

**Why:** Final wave of the service layer — ties S0-S7 into a runnable binary with end-to-end tests.

**How to apply:** When extending the binary or adding new subcommands, follow the commands/ module pattern. When adding new service module files that use serde_yaml, add `use serde_yaml_ng as serde_yaml;` — both in the module's main code section and in any doc test examples (doc tests resolve crate names differently from unit tests).

Key decisions:
- serde_yaml 0.9 swapped to serde_yaml_ng 0.10; alias `use serde_yaml_ng as serde_yaml` added to every file and doc example that references `serde_yaml::`
- reqwest promoted from dev-dep to runtime dep gated on `service` feature (needed by status/cluster subcommands at runtime)
- clap 4 with `derive` + `env` features added to `service` feature
- `[[bin]]` entry uses `required-features = ["service"]`
- Integration tests use hardcoded ports 18100-18199; `libc` added as dev-dep for SIGTERM
- `wait_timeout` is NOT in std — use polling `try_wait()` loop instead
- `cluster join` and `cluster leave` print manual workaround (no HTTP membership endpoint in v1)
- `cluster_probe` is None in serve command (v1 limitation; /status returns "cluster": null in cluster mode)
- `ready` flag flipped immediately on runner spawn (v1 placeholder; proper hook is pre-iteration callback)
- drain_coord is a fresh ShutdownCoordinator (the original is consumed by `wait_for_signal`)
