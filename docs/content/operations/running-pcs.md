+++
title = "Running PCS Service"
template = "page.html"
+++

# Running PCS Service

This guide covers day-to-day operation of `pcs-service`: building, configuring,
starting a standalone instance, bootstrapping a three-node cluster, monitoring,
and handling common failure scenarios.

---

## Build

**Standalone mode** (no Raft, single-process):

```bash
cargo build --release --features service --bin pcs-service
```

**Cluster mode** (Raft consensus, multi-node):

```bash
cargo build --release --features service-cluster --bin pcs-service
```

The `service` feature pulls in `io`, `distributed`, and `tracing`. It does
**not** include the Raft stack. Attempting to start with `mode: cluster` in a
`service`-only binary returns an error at startup and exits 1.

`service-cluster` adds `distributed-raft` (openraft, redb log store, TCP
transport). Use this feature when running multi-node deployments.

Place the compiled binary on `PATH` or invoke it as
`target/release/pcs-service`.

---

## CLI Reference

```
pcs-service [OPTIONS] <COMMAND>

Commands:
  serve     Run the service (standalone or cluster, determined by config)
  validate  Validate a config file without starting the service
  status    Query the status of a running service instance via its HTTP API
  cluster   Cluster management subcommands

Global options:
  -c, --config <PATH>       Config file path [env: PCS_CONFIG]
      --addr <URL>          HTTP control-plane address [env: PCS_ADDR]
      --log-format <FMT>    pretty | json [env: PCS_LOG_FORMAT]
      --log-level <LEVEL>   Tracing filter (e.g. info, debug) [env: PCS_LOG_LEVEL]
  -V, --version             Print version
```

### `serve`

```bash
pcs-service serve --config <path> [--node-id <id>] [--port <port>]
```

`--node-id` overrides `node.id` in the config. Useful when all nodes share the
same TOML file and the ID is injected via environment. Corresponds to env var
`PCS_NODE_ID`.

`--port` overrides the HTTP bind port only (host stays as configured). Pass `0`
to get an OS-assigned ephemeral port; the resolved address is printed to stdout
as `pcs-service listening on <addr>`.

### `validate`

```bash
pcs-service validate --config <path> [--strict]
```

Parses the TOML, checks semantic constraints, then attempts to build the service
using the built-in factory registry (`register_builtin_factories`). This catches
typos in `type` values before `serve` time.

**Exit codes:**

| Condition | Exit |
|-----------|------|
| Valid config, all built-in types resolve | 0 |
| Valid config, some types unknown (default mode) | 0 (warnings to stderr) |
| Unknown types present and `--strict` set | 1 |
| Config fails structural or semantic validation | 1 |

Without `--strict`, unknown type names (e.g. user-defined factories registered
in a custom binary) produce warnings but the command exits 0. Use `--strict` in
CI to catch typos in built-in type names.

**Important:** `validate` builds all factory instances, including sinks. Sinks
that open files (e.g. `CsvSink`, `ParquetSink`) will attempt to create the
output file immediately. The parent directory must exist before running
`validate`. See the example config comments for guidance.

Sample output for a valid config:

```
OK: config is structurally valid
  node.id:  1
  node.name: pcs-standalone
  mode:     standalone
  systems:  0
  sources:  1
  sinks:    1
  http.bind: 127.0.0.1:0
  log_level: info
OK: all declared types resolved in built-in registry
```

### `status`

```bash
pcs-service status --addr http://<host>:<port> [--full]
```

Queries `GET /status` on a running node and prints a one-line summary:

```
node 1 name=worker  mode=standalone  uptime=3601s
```

With `--full`, prints the raw JSON document.

### `cluster`

All cluster subcommands require `--features service-cluster`. Invoking them on a
`service`-only build returns an error.

```bash
pcs-service cluster init   --config <path>
pcs-service cluster join   --leader http://<leader>:<port>
pcs-service cluster leave
pcs-service cluster status --addr http://<host>:<port>
```

See the [Cluster Mode](#cluster-mode) section for details and current v1
limitations.

---

## Standalone Mode

### Quick Start

```toml
# standalone.toml
mode = "standalone"

[node]
id = 1
name = "worker"
data_dir = "/var/lib/pcs/data"

[run_mode]
kind = "continuous"   # continuous | one_shot | interval

[[pipeline.components]]
name = "orders"
type = "GenericComponent"

[[pipeline.components.config.fields]]
name = "id"
type = "Int64"
nullable = false

[[pipeline.components.config.fields]]
name = "amount"
type = "Float64"
nullable = true

# Add your registered system factories here.
pipeline.systems = []

[[sources]]
name = "input"
type = "ParquetSource"          # built-in; also: CsvSource, JsonSource
target_component = "orders"

[sources.config]
path = "/var/lib/pcs/in/orders.parquet"

[[sinks]]
name = "output"
type = "ParquetSink"            # built-in; also: CsvSink, JsonSink
source_component = "orders"

[sinks.config]
path = "/var/lib/pcs/out/orders.parquet"

[[sinks.config.schema_fields]]
name = "id"
type = "Int64"
nullable = false

[[sinks.config.schema_fields]]
name = "amount"
type = "Float64"
nullable = true

[http]
bind = "0.0.0.0:8080"

[observability]
log_format = "pretty"
log_level = "info"
```

```bash
pcs-service serve --config standalone.toml
```

```
pcs-service listening on 0.0.0.0:8080
```

Subsequent log lines depend on your `log_format`. Confirm liveness:

```bash
curl -s http://localhost:8080/health
# {"status":"alive","uptime_seconds":5,"liveness_counter":4}
```

### Execution Modes (`run_mode`)

| TOML `kind` | Behaviour |
|-------------|-----------|
| `continuous` (default) | Loop until cancelled — re-enter immediately when source has work |
| `one_shot` | Run the pipeline exactly once, then exit |
| `interval` | Sleep `interval_ms` ms between iterations |

`interval` example:

```toml
[run_mode]
kind = "interval"
interval_ms = 5000   # re-run every 5 seconds
```

### Failure Semantics (Standalone)

- **Source errors**: logged as WARN, `iteration_errors` incremented, loop
  continues on the next iteration.
- **Scheduler errors**: logged as ERROR, sinks still flushed (partial results
  preserved), `iteration_errors` incremented.
- **Sink errors**: logged as ERROR, loop continues.
- **Crash / SIGKILL**: the source re-delivers the unacknowledged batch on
  restart (at-least-once). No data is permanently lost.

---

## Cluster Mode

Cluster mode requires `--features service-cluster`. The `mode = "cluster"` config
key without this feature produces a startup error with a clear message.

### When to Use Cluster Mode

Use cluster when:
- A single node failure must not halt processing.
- The workload exceeds one machine's throughput.

Do not default to cluster mode. Running Raft requires at least three nodes for
quorum, manual membership management in v1, and operational overhead that
standalone with a good backup strategy rarely justifies.

### Cluster Mode and Sources

**`[[sources]]` entries are rejected in cluster mode.** Config validation returns
an error if `sources` is non-empty:

```
cluster mode does not support declared 'sources:' entries ...
Cluster mode ingests via PartitionSource — batches must be pre-registered via
register_master_batch or a separate producer service.
```

Remove `[[sources]]` from cluster configs. Cluster mode ingests through
`PartitionSource` (a distributed pull mechanism); batches are registered by a
producer external to the config.

### Cluster Config

```toml
mode = "cluster"
bootstrap = false                # true on the initial node ONLY at first start-up

# Timing. Invariant: lease_ttl_ms >= 3 * election_timeout_ms
lease_ttl_ms = 30000             # 30 s
election_timeout_ms = 1500       # 1.5 s
heartbeat_interval_ms = 300      # 300 ms
snapshot_log_interval = 10000    # trigger snapshot every 10 000 committed entries

[node]
id = 1                           # unique per node; override via --node-id or PCS_NODE_ID
name = "node-1"
data_dir = "/var/lib/pcs/data"   # redb files written here; must be persistent storage

# Raft transport addresses (not HTTP).
[[peers]]
id = 1
addr = "10.0.0.1:9000"

[[peers]]
id = 2
addr = "10.0.0.2:9000"

[[peers]]
id = 3
addr = "10.0.0.3:9000"

[[pipeline.components]]
name = "events"
type = "GenericComponent"

[[pipeline.components.config.fields]]
name = "event_id"
type = "Int64"
nullable = false

[[pipeline.components.config.fields]]
name = "event_type"
type = "Utf8"
nullable = true

pipeline.systems = []

[[sinks]]
name = "out"
type = "CsvSink"
source_component = "events"

[sinks.config]
path = "/var/lib/pcs/out/events.csv"

[[sinks.config.schema_fields]]
name = "event_id"
type = "Int64"
nullable = false

[[sinks.config.schema_fields]]
name = "event_type"
type = "Utf8"
nullable = true

[http]
bind = "0.0.0.0:8080"

[observability]
log_format = "json"
log_level = "info"
```

### Bootstrap a Three-Node Cluster

This walkthrough assumes three machines at `10.0.0.1–3`, Raft on port 9000,
HTTP on port 8080.

**Step 1 — Prepare data directories** (all three nodes):

```bash
mkdir -p /var/lib/pcs/data
```

**Step 2 — Pre-flight check on node 1**:

```bash
# On 10.0.0.1 with bootstrap = true in the config:
pcs-service cluster init --config node1.toml
```

`cluster init` validates the config, confirms `mode: cluster` and
`bootstrap = true`, and prints instructions. It does not start the node or write
any data. Sample output:

```
OK: config is valid and cluster.bootstrap = true
  node.id:  1
  peers:    3

To bootstrap the cluster, start this node with:
  pcs-service serve --config node1.toml

IMPORTANT: run `pcs-service serve` on ONE node first. After the leader is
elected, start the remaining nodes with bootstrap = false.
```

**Step 3 — Start node 1** (with `bootstrap = true`):

```bash
pcs-service serve --config node1.toml
```

**Step 4 — Start nodes 2 and 3** (with `bootstrap = false`):

```bash
# On 10.0.0.2:
pcs-service serve --config node2.toml

# On 10.0.0.3:
pcs-service serve --config node3.toml
```

**Step 5 — Verify**:

```bash
pcs-service cluster status --addr http://10.0.0.1:8080
```

In v1, the Raft probe is not yet wired into the HTTP state. Output:

```
node 1  mode=cluster
Note: cluster details are not available in v1. Full Raft metrics integration
is planned for v1.1.
```

Query raw JSON for whatever state is available:

```bash
curl -s http://10.0.0.1:8080/status | jq .
```

### Membership Management (v1)

Dynamic membership changes via `cluster join` and `cluster leave` are not
implemented in v1. Both commands print a manual workaround.

**Adding a node** (e.g. replacing a failed node):

1. Stop the failed node if still running.
2. On all surviving nodes, update `[[peers]]` in the config to include the new
   node's entry.
3. Write a config for the new node with `bootstrap = false`.
4. Restart all surviving nodes with the updated config.
5. Start the new node: `pcs-service serve --config new-node.toml`.

**Removing a node**:

1. Stop the node.
2. Remove its entry from `[[peers]]` in all remaining nodes' configs.
3. Restart the remaining nodes.

### Failure Semantics (Cluster)

At-least-once delivery enforced by Raft-backed leases:

- A node claims a row-range batch from `PartitionSource`. The claim carries a
  TTL equal to `lease_ttl_ms`.
- If the node does not ack within the TTL (crash, network partition, or slow
  processing), the lease expires and another node re-claims the batch.
- A node that loses its lease mid-pipeline stops processing immediately and
  releases the claim. The batch returns to pending and is re-claimed.
- Ack is issued only after the pipeline run and checkpoint write both complete.

**SIGKILL mid-claim**: the claim expires after `lease_ttl_ms` and is retried by
another node. Processing pauses by up to one TTL for any batch in flight at kill
time. No data is permanently lost.

---

## HTTP Control Plane

All endpoints are served on `http.bind` (default `0.0.0.0:8080`).

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Liveness probe |
| `/ready` | GET | Readiness probe |
| `/metrics` | GET | Prometheus text format (exposition 0.0.4) |
| `/status` | GET | JSON operational status |

### `/health`

Returns `200` while the internal watchdog counter has been incremented within
the last 5 seconds. Returns `503` if the counter is stale (possible deadlock).

```bash
curl -s http://localhost:8080/health
# 200: {"status":"alive","uptime_seconds":12,"liveness_counter":11}
# 503: {"status":"stale","uptime_seconds":60,"liveness_counter":3}
```

Use `/health` as the **liveness** probe (restart on failure).

### `/ready`

Returns `200` when the service is ready to handle work, `503` otherwise. In
v1, the ready flag is flipped immediately after the runner task is spawned. A
v1.1 pre-iteration callback will make this more accurate.

```bash
curl -s http://localhost:8080/ready
# 200: {"status":"ready"}
# 503: {"status":"not_ready"}
```

Use `/ready` as the **readiness** probe (hold traffic until ready).

### `/metrics`

Prometheus text format. Scrape with Prometheus or any compatible agent.

```bash
curl -s http://localhost:8080/metrics
```

Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `pcs_pipeline_runs_total` | Counter | Total scheduler runs |
| `pcs_pipeline_errors_total` | Counter | Scheduler runs that ended in error |
| `pcs_stage_duration_seconds` | Histogram | Per-stage execution time |
| `pcs_source_batches_drained_total` | Counter | Source batches drained into the world |
| `pcs_sink_batches_written_total` | Counter | Sink batches written from the world |
| `pcs_rows_processed_total` | Counter | Total rows processed |
| `pcs_liveness_counter` | Gauge | Watchdog counter (incremented every second) |
| `pcs_ready` | Gauge | 1 = ready, 0 = not ready |
| `pcs_uptime_seconds` | Gauge | Service uptime |
| `pcs_raft_commit_index` | Gauge | Raft commit index (cluster only) |
| `pcs_raft_term` | Gauge | Current Raft term (cluster only) |
| `pcs_raft_leader_id` | Gauge | Current leader node ID (cluster only) |

### `/status`

JSON document with node identity, uptime, build info, and mode-specific stats.
Published on every `/status` request; reflects the latest iteration counters.

**Standalone:**

```json
{
  "node_id": 1,
  "node_name": "worker",
  "mode": "standalone",
  "uptime_seconds": 3601,
  "build": { "version": "1.0.0-alpha.1" },
  "cluster": null,
  "standalone": {
    "iterations": 847,
    "rows_processed": 423500,
    "source_batches_drained": 847,
    "sink_batches_written": 847,
    "iteration_errors": 2
  }
}
```

**Cluster** (when `cluster_probe` is wired — see v1 note below):

```json
{
  "node_id": 1,
  "node_name": "node-1",
  "mode": "cluster",
  "uptime_seconds": 3601,
  "build": { "version": "1.0.0-alpha.1" },
  "cluster": {
    "role": "leader",
    "health": "healthy",
    "current_leader": 1,
    "raft_term": 3,
    "last_log_index": 412,
    "commit_index": 412,
    "last_applied": 412,
    "snapshot_last_applied": null,
    "membership": [
      { "id": 1, "addr": "10.0.0.1:9000" },
      { "id": 2, "addr": "10.0.0.2:9000" },
      { "id": 3, "addr": "10.0.0.3:9000" }
    ],
    "voters": [1, 2, 3],
    "learners": [],
    "batches_processed_total": 847,
    "batches_failed_total": 2
  },
  "standalone": null
}
```

In v1, `cluster_probe` is not wired in the serve command. The `cluster` field
is `null` even in cluster mode. Full Raft metrics integration is planned for
v1.1.

---

## Data Directory Layout

```
$DATA_DIR/
├── raft-log.redb       # Raft log entries (openraft, redb B-tree)
├── state-machine.redb  # Applied state: batch registry, claims, checkpoints
└── snapshots/          # Installed Raft snapshots (Arrow IPC format)
    └── <term>-<index>/
        └── state.ipc
```

The `node-id` written by previous versions is no longer used; node identity
comes from the config and CLI.

**`raft-log.redb`**: grows until a snapshot is installed and preceding log
entries are compacted. If this file grows unexpectedly, check that
`pcs_raft_commit_index` is advancing and that snapshot installation is
completing. Snapshots are triggered automatically when committed log entries
exceed `snapshot_log_interval` (default: 10 000). There is no manual
force-snapshot CLI command in v1.

**`state-machine.redb`**: the applied state — batch registrations, active
claims, secondary claims-by-batch index, and checkpoint IPC bytes. This file is
serialized into Raft snapshots. Back it up before manual maintenance. Steady-
state size growth is not expected.

**`snapshots/`**: written during `build_snapshot`, read during
`install_snapshot`. New nodes joining the cluster receive a full snapshot over
TCP (chunked at 4 MiB per frame). Old snapshots are cleaned up after a newer
one installs. Do not delete these manually while the node is running.

### `MAX_LOG_ENTRY_BYTES` Cap

The TCP transport enforces a maximum frame size of 16 MiB
(`MAX_FRAME_BYTES`). Checkpoint payloads embedded in Raft log entries are
capped at `MAX_LOG_ENTRY_BYTES`. Checkpoint snapshots larger than this limit
are rejected with a `Store` error. If your workload produces large checkpoints,
increase `MAX_LOG_ENTRY_BYTES` in `src/distributed/consensus/transport.rs` and
rebuild with `--features service-cluster`.

---

## Checkpoint Strategies

Checkpoint strategy is set on `DistributedRunner` (in code, not config in v1).

| Strategy | Behaviour | Use when |
|----------|-----------|----------|
| `EveryStage` | Checkpoint after every pipeline stage | Maximum recovery granularity; highest write amplification |
| `EveryNStages(n)` | Checkpoint every N stages | Balance durability and write cost |
| `None` | No checkpointing | Idempotent pipelines that can safely re-run from the start |

The default is `EveryStage`. For long pipelines with expensive stages, consider
`EveryNStages` to reduce redb write pressure.

---

## Graceful Shutdown

Send `SIGTERM`:

```bash
kill -TERM <pid>
```

**Standalone**: the runner finishes the current pipeline iteration, acks the
source batch, and exits. In-flight work is not lost.

**Cluster**: the claiming node completes or releases its current batch before
exiting. The leader may transfer leadership before stopping. The remaining nodes
elect a new leader after `election_timeout_ms * 2` if the exiting node was the
leader.

The shutdown budget is 30 seconds. If the process has not exited cleanly within
that window, it exits forcibly with exit code 1.

**SIGKILL**: bypasses the shutdown handler. In cluster mode, the claim expires
after `lease_ttl_ms` and is retried by another node. In standalone mode, the
source re-delivers on next startup. No data is permanently lost; processing may
pause for up to one lease TTL.

---

## Log Output

**Development** (default — colored, human-readable):

```bash
pcs-service serve --config config.toml
# or: observability.log_format = "pretty"
```

**Production** (structured JSON for Loki, CloudWatch, Datadog):

```bash
pcs-service serve --config config.toml --log-format json
# or: observability.log_format = "json"
```

**Level** — `PCS_LOG_LEVEL` env var, `--log-level` flag, or `observability.log_level` in config:

- `error` — production default when logs are expensive
- `info` — startup, shutdown, batch completion, leader changes
- `debug` — per-stage timing, lease renewal events
- `trace` — Arrow IPC encode/decode, Raft message flow (very verbose)

---

## Environment Variables

| Variable | Equivalent flag | Description |
|----------|----------------|-------------|
| `PCS_CONFIG` | `-c / --config` | Config file path |
| `PCS_NODE_ID` | `--node-id` | Node ID override (serve) |
| `PCS_HTTP_PORT` | `--port` | HTTP port override (serve) |
| `PCS_ADDR` | `--addr` | Control-plane address (status, cluster) |
| `PCS_LOG_FORMAT` | `--log-format` | `pretty` or `json` |
| `PCS_LOG_LEVEL` | `--log-level` | Tracing filter |

Config TOML also supports `${VAR}` and `${VAR:-default}` placeholder expansion.

---

## Exit Codes

| Exit | Condition |
|------|-----------|
| `0` | Clean exit (successful run, `one_shot` complete, SIGTERM drain) |
| `1` | Runner error, config validation failure, or 30-second shutdown budget exceeded |

Cluster sub-binary: `cluster join` and `cluster leave` exit 0 with a printed
workaround (not errors), because dynamic membership is not yet implemented.

---

## Common Operational Scenarios

### One node crashed permanently

1. Stop the failed node if still running.
2. On all surviving nodes, remove the failed node from `[[peers]]` in the config.
3. On the replacement machine, write a config with `bootstrap = false` and the
   node's new address in `[[peers]]`.
4. Restart all surviving nodes with the updated config.
5. Start the replacement: `pcs-service serve --config new-node.toml`.

### Leader is degraded

No `cluster transfer-leader` command exists in v1. If the leader is unreachable,
the remaining nodes elect a new leader automatically after `election_timeout_ms
* 2` (default: 3 s). Restart the degraded node to trigger a clean election.

### Cluster partition

**Majority side** (quorum present): continues operating. If the leader is on
this side it keeps committing; if not, the majority elects a new leader.

**Minority side** (no quorum): all nodes become followers and stop accepting
writes. In-flight claims are not acked; they expire and are retried by the
majority side after `lease_ttl_ms`.

When the partition heals, minority-side nodes re-join and receive a snapshot or
log replay from the leader. No manual action is required.

### Disk pressure (`raft-log.redb` growing)

1. Check `pcs_raft_commit_index` — if it has stopped advancing, the state
   machine may be stuck.
2. Snapshots are triggered automatically when committed entries exceed
   `snapshot_log_interval`. Reduce this value in the config and restart to
   force more frequent snapshots.
3. After a snapshot installs, log compaction removes old entries and frees
   space.

---

## Planned / Not Yet Implemented (v1)

| Feature | Status |
|---------|--------|
| `cluster join` (dynamic membership add) | CLI exists, prints manual workaround |
| `cluster leave` (dynamic membership remove) | CLI exists, prints manual workaround |
| `cluster transfer-leader` | Not implemented; use node restart |
| Full Raft metrics in `/status` (`cluster_probe`) | Planned for v1.1 |
| `ready` flag after first successful iteration | Planned for v1.1; currently flipped at startup |
