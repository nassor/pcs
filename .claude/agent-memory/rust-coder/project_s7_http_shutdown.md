---
name: Service S7 HTTP+Shutdown
description: axum 0.8 HTTP control plane, ShutdownCoordinator with CancellationToken, init_logging, PrometheusHandle sharing, cross-coder StandaloneStats coordination
type: project
---

Phase S7 delivered the HTTP control plane and graceful shutdown coordination.

**Files created:**
- `src/service/http.rs` (~800 lines) — axum 0.8 router, 4 endpoints, ClusterProbe trait, ServiceState, watchdog, PrometheusHandle
- `src/service/shutdown.rs` (~160 lines) — ShutdownCoordinator with CancellationToken, budget-limited drain
- `src/service/logging.rs` (~100 lines) — init_logging via tracing-subscriber with Pretty/JSON format selection

**Key decisions:**
- StandaloneStats lives in `standalone.rs` (Coder 3). http.rs imports it rather than redefining; the JSON response uses a local DTO (`StandaloneStatusBody`) since standalone.rs version lacks Serialize and has an extra `total_duration_ms` field.
- ClusterProbe is an abstract trait (`Arc<dyn ClusterProbe>`) so http.rs has zero dependency on src/distributed/.
- PrometheusHandle is Clone; tests use `build_recorder().handle()` to avoid the global recorder conflict across tests.
- TimeoutLayer::new() is deprecated in tower-http 0.6; use `TimeoutLayer::with_status_code(status, duration)` (args are swapped vs new()).
- openraft driver.rs bug fixed: `.borrow()` on TokioWatchReceiver needed `use openraft::async_runtime::WatchReceiver as _` to bring `borrow_watched()` into scope.
- tokio `signal` feature added to [dependencies] tokio entry (needed for SIGTERM handler).

**Cargo.toml additions:**
- `axum = { version = "0.8", default-features = false, features = ["http1", "json", "tokio", "query"] }`
- `tower = "0.5"`, `tower-http = { version = "0.6", features = ["trace", "timeout", "limit"] }`
- `tokio-util = "0.7"` (also activated by distributed feature)
- `tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt", "json", "ansi"] }`
- `metrics = "0.24"`, `metrics-exporter-prometheus = "0.17"`
- `reqwest = { version = "0.12", features = ["json", "rustls-tls"] }` in dev-dependencies (for HTTP tests)

**Why:** bind_server in tests binds port 0 then drops the listener, then serve_http re-binds it. This has a small race window but works in practice since the OS doesn't immediately reassign ephemeral ports.

**How to apply:** When wiring S8 CLI, create PrometheusHandle via `PrometheusBuilder::new().install_recorder()`, call `register_standard_metrics()`, then build ServiceState and call `serve_http` + `spawn_watchdog` as concurrent tasks alongside runners.
