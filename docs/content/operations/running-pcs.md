+++
title = "Operating pcs-service"
description = "Build and install the binary, pick a config, run the modes, probe the control plane, and the failure modes to expect."
template = "page.html"
+++

# Operating pcs-service

Running a `pcs-service` deployment: building the binary, choosing a config,
running the modes, probing the control plane, and what to do when a node fails.

Three pages come first. [Installation](@/quickstart/installation.md) installs the
binary. [Configuration](@/service/configuration.md) is the config surface and the
load-time gates. [Observability](@/service/observability.md) is the HTTP probes,
readiness and graceful shutdown. This page starts where those leave off.

## 1. Build and install

`cargo install --path crates/pcs-service` from a clone of the repository builds the **default
bundle**: `mimalloc`, `service`, `wasm`, `windows`, `parquet-checkpoint`, every
connector except Kafka, and all five transformers. That binary serves, runs
WASM pipelines, and binds every node kind except `plugin` and cluster mode.

```bash,name=Install the default bundle
cargo install --path crates/pcs-service
```

Runs the same on Linux, macOS and Windows (PowerShell). Two features stay
opt-in because of what they pull in:

- `connector-kafka` builds vendored C through `librdkafka-sys`, which needs
  `cmake` and a C toolchain.
- `service-cluster` makes a cluster node, a deliberate deployment choice.
  `mode "cluster"` in a binary without it refuses to start. A cluster node
  needs no store block and no external service: its state lives in
  `node.data_dir`.

```bash,name=Install with cluster support and Kafka
cargo install --path crates/pcs-service --features service-cluster,connector-kafka
```

For a deliberately narrow binary, pick the features from
[the configuration page](@/service/configuration.md#which-features-add-which-nodes).

## 2. Pick a config

`pcs-service serve` needs no flags: `--config`/`-c` defaults to `pcs.kdl` in
the current directory and reads the `PCS_CONFIG` env var. The repository ships
runnable configs under `examples/configs/`, each with its build command in the
header comments:

| Config | What it runs | Needs |
|---|---|---|
| `standalone.kdl` | CSV in, one WASM processor, CSV out | `connector-file,transformer-csv,wasm` |
| `standalone_wasm.kdl` | the order-processing component over CSV | `connector-file,transformer-csv,wasm` |
| `standalone_plugin.kdl` | the native plugin fixture over CSV | `connector-file,transformer-csv,plugin` |
| `standalone_polyglot.kdl` | a polyglot processor component | the polyglot build |
| `nats.kdl` | NATS JetStream at both ends | `connector-nats,wasm` |
| `postgresql.kdl` | PostgreSQL logical replication to a sink table | `connector-postgresql,wasm` |
| `kafka.kdl` | Kafka source and sink | `connector-kafka,wasm` |
| `tcp.kdl` | a live TCP ingest stream | `connector-tcp,wasm` |
| `http.kdl` | an HTTP source spooled through a transformer | `connector-http,transformer-csv,wasm` |
| `redb.kdl` | a stream run persisting its config, cursors and priors to a local redb file | `connector-file,transformer-csv` |
| `cluster.kdl` | a three-node cluster template | `service-cluster,connector-file,transformer-csv,wasm` |

Linux/macOS:

```bash,name=Validate a config with the features it needs
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- \
  validate --config examples/configs/standalone.kdl
```

Windows (PowerShell):

```powershell
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- `
  validate --config examples/configs/standalone.kdl
```

## 3. Choose a run mode

`mode "standalone"` plus `run_mode` paces the loop; `mode "cluster"` runs the
distributed runner. The same `serve` command starts either, whichever the config
declares. `--node-id` (env `PCS_NODE_ID`) overrides `node.id`, and `--port` (env
`PCS_HTTP_PORT`) overrides the HTTP bind port; `--port 0` prints the
OS-assigned address on stdout. `one_shot` exits after one pass, which is what a
cron entry or a test harness wants:

```bash,name=Run one pass and exit
pcs-service serve --config examples/configs/standalone.kdl
```

Runs the same on Linux, macOS and Windows (PowerShell).

The rest of `run_mode` is on [the configuration page](@/service/configuration.md#pacing-a-standalone-run).

## 4. Probe the control plane

Four endpoints answer on `http.bind`, `0.0.0.0:8080` by default:

Linux/macOS:

```bash,name=Probe the three health endpoints
curl -s http://localhost:8080/health
curl -s http://localhost:8080/ready
curl -s http://localhost:8080/metrics | grep '^pcs_'
```

Windows (PowerShell):

```powershell
curl.exe -s http://localhost:8080/health
curl.exe -s http://localhost:8080/ready
curl.exe -s http://localhost:8080/metrics | Select-String '^pcs_'
```

- `/health` is the one to alert on: the watchdog counter behind it goes stale
  within 5 seconds if the main loop wedges, and the endpoint returns 503.
- `/ready` reports the process, not the workflow: the flag flips once the
  runner is spawned, so 200 never means an iteration succeeded. Read
  `iterations` on `/status` for that.
- `/metrics` is Prometheus exposition 0.0.4 with the nineteen series. A series
  appears once its writer has recorded a value, so `pcs_raft_*` show up on a
  cluster node and `pcs_processor_*` once a processor has run a batch.
- `/status` is the JSON node identity plus per-workflow runner counters. No
  `ClusterProbe` is wired into `serve`, so `"cluster"` is `null` even in
  cluster mode; the Raft gauges carry term, commit index and leader.

## 5. Tune what the config exposes

Every tuning knob is a config key, not a flag:

| Want to | Key |
|---|---|
| Pace a standalone run | `run_mode kind="continuous" | "one_shot" | "interval"` with `interval_ms` |
| Retry connector operations | the `retry` child on a `source` or `sink` |
| Persist the config, source cursors and processor priors | `store "redb" { path "/var/lib/pcs/state.redb" }`, standalone and stream only |
| Carry processor state across `interval` or `one_shot` iterations | `batch_resume #true` inside that `store "redb"` block |
| Bound the claim lease against elections | `lease_ttl_ms` (cluster only, default 30000), at least three `election_timeout_ms` |
| Drive elections | `election_timeout_ms`, `heartbeat_interval_ms` (cluster only) |
| Compact the raft log more often | `snapshot_log_interval` (cluster only) |
| See per-item spans | `observability log_level="debug"` (costs about 2.8 µs per item) |
| Size the in-process buffers | `observability.inspector.max_spans`, `max_logs`, `max_samples`, `retention_secs` |
| Accept late rows in a window | `window allowed_lateness_ms` |

The buffer caps matter on a busy node: a capacity eviction is counted and
reported as `buffers.dropped` in `/api/snapshot`, so a buffer sized too small
says so instead of quietly forgetting.

## 6. Failure modes

Each of these refuses to start or fails loudly; none is silently ignored.

| Condition | What happens |
|---|---|
| Config file missing | `error: Configuration error: reading config file pcs.kdl: <os error>`, exit 1 |
| `mode "cluster"` without `service-cluster` | startup error: rebuild with `--features service-cluster`, exit 1 |
| `mode "cluster"` with a `store` block | ``mode "cluster" does not take a `store` block: cluster state lives in node.data_dir``, at load time |
| `lease_ttl_ms` under three election timeouts | `lease_ttl_ms (1000) must be >= 3 × election_timeout_ms (1000) = 3000`, at load time |
| `store "redb"` with an empty `path` | `store redb: path must not be empty`, at load time |
| A key the service cannot honour | parse error (strict nodes) or ignored key (top level): the split is in [What is not a key](@/service/configuration.md#what-is-not-a-key) |
| `raft-log.redb` without `bootstrap.lock` | the previous start opened the log but never joined a cluster; the node refuses to start |
| `node-id` file disagreeing with `node.id` | the data directory belongs to a different node; refused |
| Raft not settling within 30 s | `serve` exits 1 |

`pcs-service validate --config <file>` runs the load-time gates without
starting anything, so most of these surface in CI.

## 7. Cluster mode

### Bootstrap a three-node cluster

Before step 1: three machines at `10.0.0.1` to `10.0.0.3`, Raft on port 9000
and HTTP on port 8080, a binary built with
`--features service-cluster,connector-file,transformer-csv,wasm`, and an empty
`node.data_dir` on each machine. No external service is involved.

The config template is `examples/configs/cluster.kdl`; the header and its keys
are on [the configuration page](@/service/configuration.md#the-cluster-header).
Every `pcs-service` command in this walkthrough is identical on Linux, macOS
and Windows (PowerShell).

**Step 1: Prepare the data directories** (all three nodes):

Linux/macOS:

```bash,name=Step 1 prepare the data directories
mkdir -p /var/lib/pcs/data
```

Windows (PowerShell):

```powershell
New-Item -ItemType Directory -Force -Path C:\pcs\data
```

**Step 2: Pre-flight check on node 1**, with `bootstrap #true` in its config:

```bash,name=Step 2 pre-flight check on node 1
pcs-service cluster init --config node1.kdl
```

`cluster init` validates the config, confirms `mode: cluster` and
`bootstrap #true`, and prints instructions. It does not start the node or write
any data. Sample output:

```text,name=Expected cluster init output
OK: config is valid and cluster.bootstrap = true
  node.id:  1
  peers:    3

To bootstrap the cluster, start this node with:
  pcs-service serve --config node1.kdl

IMPORTANT: run `pcs-service serve` on ONE node first. After the leader is
elected, start the remaining nodes with bootstrap: false.
```

**Step 3: Start node 1** (with `bootstrap #true`):

```bash,name=Step 3 start node 1
pcs-service serve --config node1.kdl
```

Node 1 creates its data directory contents while it bootstraps. Confirm them
before starting the other two.

Linux/macOS:

```bash,name=Confirm the data directory after bootstrap
ls /var/lib/pcs/data
```

Windows (PowerShell):

```powershell
Get-ChildItem C:\pcs\data | Select-Object -ExpandProperty Name
```

```text,name=Expected data directory contents
bootstrap.lock
cluster-app.redb
node-id
raft-log.redb
```

**Step 4: Start nodes 2 and 3** (with `bootstrap #false`):

```bash,name=Step 4 start nodes 2 and 3
pcs-service serve --config node2.kdl
pcs-service serve --config node3.kdl
```

**Step 5: Verify from node 1:**

```bash,name=Step 5 verify from node 1
pcs-service cluster status --addr http://10.0.0.1:8080
```

No Raft probe is wired into the HTTP state, so the command prints a summary
line and a note that cluster details are not available:

```text,name=Expected cluster status output
node 1  mode=cluster
Note: cluster details are not available in v1. Full Raft metrics integration is planned for v1.1.
```

Query the raw status JSON for what is there:

Linux/macOS:

```bash,name=Query the raw status JSON
curl -s http://10.0.0.1:8080/status | jq .
```

Windows (PowerShell):

```powershell
curl.exe -s http://10.0.0.1:8080/status | ConvertFrom-Json | ConvertTo-Json -Depth 10
```

The three Raft gauges on `/metrics` carry term, commit index and leader:
`pcs_raft_leader_id` reports `-1` while there is no leader.

### Membership management

`cluster join` and `cluster leave` do not change membership. Both commands print
a manual workaround.

**Adding a node** (e.g. replacing a failed node):

1. Stop the failed node if still running.
2. On all surviving nodes, add a `peer` node for the new member to the config.
3. Write a config for the new node with `bootstrap #false`.
4. Restart all surviving nodes with the updated config.
5. Start the new node: `pcs-service serve --config new-node.kdl`.

**Removing a node**:

1. Stop the node.
2. Remove its `peer` node from all remaining nodes' configs.
3. Restart the remaining nodes.

### Failure semantics (cluster)

At-least-once delivery, enforced by claim leases the Raft replicates:

- A node claims the pending row range of one registered master batch through
  `PartitionSource`. The claim, its holder and its expiry are one Raft log
  entry, so every node reads the same claim state.
- The shared store grants each claim a 90 s lease, and the runner renews it at
  a third of that cadence while the batch runs.
- If the node stops renewing (crash, network partition, or slow processing),
  the lease expires. A running node proposes an expiry sweep every 30 s, which
  returns the range to pending for another node to claim.
- A node whose renewal fails mid-run stops processing immediately and releases
  the claim.
- Ack is issued only after the processor run and checkpoint write both complete.

**SIGKILL mid-claim**: the lease expires and another node reclaims the range.
Processing pauses by up to one lease plus one sweep interval for any range in
flight at kill time. No data is permanently lost.

### Where state lives

One place: `node.data_dir` on every node. It holds four files.

- `raft-log.redb` holds this node's Raft log: its vote, its entries and its
  view of the membership.
- `cluster-app.redb` holds the replicated application state: registered master
  batches, one record per claimed row range carrying that range's claim state,
  checkpoint IPC bytes, the schema-id ledger and the per-instance heartbeats.
- `bootstrap.lock` records that a cluster was deliberately created here. A
  `raft-log.redb` without it is an unclean shutdown before bootstrap finished,
  and the node refuses to start.
- `node-id` records which node the directory belongs to. A start whose
  `node.id` disagrees with it is refused.

Every mutation is proposed through `Raft::client_write`, and a follower
forwards its proposal to the leader over the same length-prefixed TCP
transport that carries the Raft messages. Reads are local, served from this
node's own `cluster-app.redb`.

Every node therefore carries a full copy of the application state, so a
three-node cluster keeps three copies. Recover a lost node by starting it with
an empty data directory and `bootstrap #false`: the leader refills it by
replication or a snapshot. Never copy a data directory between nodes, because
the `node-id` file it carries belongs to the node that wrote it.

`raft-log.redb` grows until compaction. The driver compacts once applied
entries pass `snapshot_log_interval` (default: 10 000); there is no manual
force-compaction command. Registered batches and checkpoints travel in this
log, so its size tracks the payloads the pipeline proposes.

### Payload caps

| Cap | Value | Where |
|---|---|---|
| `MAX_LOG_ENTRY_BYTES` | 1 MiB | `crates/pcs-service/src/distributed/partition.rs`; caps the Arrow IPC payload of a registered master batch and, as the `CheckpointStore::max_checkpoint_bytes` default, of one stage checkpoint |
| `SNAPSHOT_CHUNK_BYTES` | 4 MiB | `crates/pcs-service/src/distributed/consensus/transport/mod.rs`; one chunk of a state-machine snapshot |
| `MAX_FRAME_BYTES` | 16 MiB | the same file; caps one length-prefixed TCP frame on the peer transport, which carries Raft messages, forwarded proposals and snapshot chunks |

If registration rejects a batch, split the input across several `batch_id`s
rather than raising the constant. A claim covers the pending row range of one
batch, so several smaller batches are also what spreads work across nodes.

### Checkpoint strategies

Checkpoint strategy is set on `DistributedRunner`, in code rather than config.

| Strategy | Behaviour | Use when |
|----------|-----------|----------|
| `EveryStage` | Checkpoint after every pipeline stage | Maximum recovery granularity; highest write amplification |
| `EveryNStages(n)` | Checkpoint every N stages | Balance durability and write cost |
| `None` | No checkpointing | Idempotent pipelines that can safely re-run from the start |

The default is `EveryStage`. For long pipelines with expensive stages,
`EveryNStages` reduces how much each batch writes into the Raft log.

### Shutdown

On a clean stop the claiming node completes or releases its current batch before
exiting. The remaining nodes elect a new leader after `election_timeout_ms * 2`
if the exiting node was the leader.

`SIGKILL` bypasses the handler. The claim's lease expires and another node
reclaims the range, so processing pauses by up to one lease plus one sweep
interval for any range in flight at kill time. No data is permanently lost.

## Log output

`log_format "json"` under `observability`, or `--log-format json`, switches
from the coloured development output to structured JSON for Loki, CloudWatch or
Datadog. `--log-level` (env `PCS_LOG_LEVEL`) sets the filter, and a set
`RUST_LOG` beats both: see the levels on
[the observability page](@/service/observability.md#span-levels).

`otlp_endpoint` exports the span tree over OTLP/HTTP; the exporter appends
`/v1/traces` to the collector root. Metrics are not exported that way; they stay
on `/metrics`.

## Environment variables

| Variable | Equivalent flag | Description |
|----------|----------------|-------------|
| `PCS_CONFIG` | `-c / --config` | Config file path |
| `PCS_NODE_ID` | `--node-id` | Node ID override (serve) |
| `PCS_HTTP_PORT` | `--port` | HTTP port override (serve) |
| `PCS_ADDR` | `--addr` | Control-plane address (status, cluster) |
| `PCS_LOG_FORMAT` | `--log-format` | `pretty` or `json` |
| `PCS_LOG_LEVEL` | `--log-level` | Tracing filter |
| `PCS_OTLP_ENDPOINT` | `--otlp-endpoint` | OTLP/HTTP collector root for span export |

The config file also supports `${VAR}` and `${VAR:-default}` placeholder
expansion, plus a `variables` block that wins over the environment.

## Exit codes

| Exit | Condition |
|------|-----------|
| `0` | Clean exit (successful run, `one_shot` complete, SIGTERM drain) |
| `1` | Runner error, config validation failure, or 30-second shutdown budget exceeded |

`cluster join` and `cluster leave` exit 0 and print a manual workaround rather
than an error, because membership changes are manual.

## Common operational scenarios

### One node crashed permanently

1. Stop the failed node if still running.
2. On all surviving nodes, remove the failed node's `peer` node from the config.
3. On the replacement machine, write a config with `bootstrap #false`, a `peer`
   node carrying the node's new address, and an empty `node.data_dir`.
4. Restart all surviving nodes with the updated config.
5. Start the replacement: `pcs-service serve --config new-node.kdl`. It joins
   as a follower, and the leader replicates the application state into its
   `cluster-app.redb`.

### Leader is degraded

There is no `cluster transfer-leader` command. If the leader is unreachable,
the remaining nodes elect a new leader automatically after `election_timeout_ms
* 2` (default: 3 s). Restart the degraded node to trigger a clean election.

### Cluster partition

Claims, checkpoints and acks are Raft proposals, so a node needs the leader to
make progress.

**Majority side**: keeps its leader, or elects one, and keeps claiming.

**Minority side**: proposals stop. A follower forwards its proposal to the
leader, reaches none, and the propose fails after 30 s. Claims, checkpoints and
acks all fail, in-flight leases expire, and `pcs_raft_leader_id` reports `-1`.
A node that *restarts* into a minority partition does start: it settles as a
follower against its persisted membership, then reports
`pcs_raft_leader_id` `-1` and fails every propose until the partition heals.

When the partition heals, minority-side nodes rejoin from the leader and resume
claiming. Ranges whose leases expired return to pending on the next sweep. No
manual action is required.

### Disk pressure (`raft-log.redb` growing)

1. Check `pcs_raft_commit_index`. If it has stopped advancing, the node is not
   committing and compaction cannot run.
2. Compaction is automatic once applied entries pass `snapshot_log_interval`.
   Reduce that value and restart to compact more often.
3. Registered batches and stage checkpoints travel in this log. Split a large
   input across several `batch_id`s instead of raising `MAX_LOG_ENTRY_BYTES`.
