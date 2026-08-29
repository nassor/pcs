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
- `service-cluster` and `tikv-store` make a cluster node, a deliberate
  deployment choice. `mode "cluster"` in a binary without `service-cluster`
  refuses to start, and so does a cluster config without a
  `store "tikv"` block: TiKV is the only cluster application-data store.

```bash,name=Install with cluster support and Kafka
cargo install --path crates/pcs-service --features service-cluster,tikv-store,connector-kafka
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
| `tikv.kdl` | a standalone stream run backed by a TiKV store | `tikv-store,connector-file,transformer-csv` |
| `cluster.kdl` | a three-node cluster template | `service-cluster,tikv-store,connector-file,transformer-csv,wasm` |

```bash,name=Validate a config with the features it needs
cargo run -p pcs-service --features connector-file,transformer-csv,wasm -- \
  validate --config examples/configs/standalone.kdl
```

Runs the same on all three platforms.

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

The rest of `run_mode` is on [the configuration page](@/service/configuration.md#pacing-a-standalone-run).

## 4. Probe the control plane

Four endpoints answer on `http.bind`, `0.0.0.0:8080` by default:

```bash,name=Probe the three health endpoints
curl -s http://localhost:8080/health
curl -s http://localhost:8080/ready
curl -s http://localhost:8080/metrics | grep '^pcs_'
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
| Change the claim lease | `store "tikv" { lease_ttl_ms }`, the only lease knob, at least 10000 |
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
| Cluster config without `store "tikv"` | ``mode "cluster" requires a `store "tikv"` block: TiKV is the only cluster application-data store``, at load time |
| `store` block without `tikv-store` | ``config declares a `store` block, but this binary was built without the `tikv-store` feature — rebuild with `--features tikv-store``, at load time |
| A key the service cannot honour | parse error (strict nodes) or ignored key (top level): the split is in [What is not a key](@/service/configuration.md#what-is-not-a-key) |
| Unreachable TiKV at cluster start | the node fails to start rather than running degraded |
| `raft-log.redb` without `bootstrap.lock` | unclean shutdown before bootstrap finished; the node refuses to start |
| `node-id` file disagreeing with `node.id` | the data directory belongs to a different node; refused |
| Raft not settling within 30 s | `serve` exits 1 |

`pcs-service validate --config <file>` runs the load-time gates without
starting anything, so most of these surface in CI.

## 7. Cluster mode

### Bootstrap a three-node cluster

This walkthrough assumes three machines at `10.0.0.1` to `10.0.0.3`, Raft on
port 9000, HTTP on port 8080, and a TiKV already serving PD on port 2379. Bring
TiKV up first: a node whose `store "tikv"` endpoints are unreachable fails to
start. The config template is `examples/configs/cluster.kdl`; the header and
its keys are on [the configuration page](@/service/configuration.md#the-cluster-header).

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

At-least-once delivery, enforced by TiKV-backed leases:

- A node claims one 512-row range through `PartitionSource`. The claim carries a
  TTL equal to the `lease_ttl_ms` in the `store "tikv"` block.
- If the node does not ack within the TTL (crash, network partition, or slow
  processing), the lease expires and another node re-claims the range.
- A node that loses its lease mid-run stops processing immediately and
  releases the claim. The range returns to pending and is re-claimed.
- Ack is issued only after the processor run and checkpoint write both complete.

**SIGKILL mid-claim**: the claim expires after `lease_ttl_ms` and is retried by
another node. Processing pauses by up to one TTL for any range in flight at kill
time. No data is permanently lost.

### Where state lives

Two places, and only one of them is on the node.

`node.data_dir` holds three files, all local, none of them application state:

- `raft-log.redb` holds this node's Raft log: its vote, its entries and its view
  of the membership.
- `bootstrap.lock` records that a cluster was deliberately created here. A
  `raft-log.redb` without it is an unclean shutdown before bootstrap finished,
  and the node refuses to start.
- `node-id` records which node the directory belongs to. A start whose `node.id`
  disagrees with it is refused.

Nothing else is written there. A node that loses its data directory loses its
place in the membership, never a claim or a checkpoint.

`raft-log.redb` grows until compaction. The driver compacts once applied
entries pass `snapshot_log_interval` (default: 10 000); there is no manual
force-compaction command. Nothing is proposed into this log, so its entries are
Raft's own per-term bookkeeping and it grows slowly. If it grows unexpectedly,
check that `pcs_raft_commit_index` is advancing.

Everything the pipeline touches lives in TiKV under the `key_prefix` the
`store "tikv"` block declares: registered master batches, one record per
512-row range carrying that range's claim state, checkpoint IPC bytes, the
schema-id ledger, source cursors, processor priors, and persisted configs. Every
node reads and writes the same copy. Claim transitions are compare-and-swap, so
TiKV arbitrates them without a coordinator and no PCS node dispatches work to
another.

Back up TiKV, not the data directories. Two deployments can share one TiKV by
picking different `key_prefix` values.

### Payload caps

| Cap | Value | Where |
|---|---|---|
| `MAX_LOG_ENTRY_BYTES` | 1 MiB | `crates/pcs-service/src/distributed/partition.rs`; caps the Arrow IPC payload of a registered master batch and the runner's own stage checkpoint |
| `TIKV_MAX_CHECKPOINT_BYTES` | 4 MiB | `crates/pcs-service/src/distributed/tikv_store.rs`; the ceiling the store reports through `CheckpointStore::max_checkpoint_bytes` |
| `MAX_FRAME_BYTES` | 16 MiB | `crates/pcs-service/src/distributed/consensus/transport/mod.rs`; caps one length-prefixed TCP frame on the peer transport, which carries Raft messages only |

If registration rejects a batch, split the input across several `batch_id`s
rather than raising the constant. Row ranges are 512 rows apiece however large
the batch is, so smaller batches cost no parallelism.

### Checkpoint strategies

Checkpoint strategy is set on `DistributedRunner`, in code rather than config.

| Strategy | Behaviour | Use when |
|----------|-----------|----------|
| `EveryStage` | Checkpoint after every pipeline stage | Maximum recovery granularity; highest write amplification |
| `EveryNStages(n)` | Checkpoint every N stages | Balance durability and write cost |
| `None` | No checkpointing | Idempotent pipelines that can safely re-run from the start |

The default is `EveryStage`. For long pipelines with expensive stages, consider
`EveryNStages` to reduce write pressure on TiKV.

### Shutdown

On a clean stop the claiming node completes or releases its current batch before
exiting. The remaining nodes elect a new leader after `election_timeout_ms * 2`
if the exiting node was the leader.

`SIGKILL` bypasses the handler. The claim expires after `lease_ttl_ms` and is
retried by another node, so processing pauses by up to one lease TTL for any range
in flight at kill time. No data is permanently lost.

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
3. On the replacement machine, write a config with `bootstrap #false` and a
   `peer` node carrying the node's new address.
4. Restart all surviving nodes with the updated config.
5. Start the replacement: `pcs-service serve --config new-node.kdl`.

### Leader is degraded

There is no `cluster transfer-leader` command. If the leader is unreachable,
the remaining nodes elect a new leader automatically after `election_timeout_ms
* 2` (default: 3 s). Restart the degraded node to trigger a clean election.

### Cluster partition

A PCS Raft partition and a TiKV partition are different failures, because the
two carry different things.

**PCS Raft partition**: the majority side keeps a leader, or elects one. The
minority side falls back to followers or candidates. Neither side stops
claiming: claims go to TiKV, not through the PCS Raft, so a running node keeps
working as long as TiKV is reachable. What the minority loses is a settled
leader, which is visible as `pcs_raft_leader_id` reporting `-1`. A node that
*restarts* into a minority partition does fail to start: startup waits 30 s for
Raft to leave Candidate and exits 1 if it does not.

**TiKV unreachable**: claims, checkpoints and acks all fail. In-flight leases
expire and the ranges return to pending once TiKV is back. Restore TiKV's own
quorum first; the PCS nodes need no manual action.

When a PCS Raft partition heals, minority-side nodes rejoin from the leader. No
manual action is required.

### Disk pressure (`raft-log.redb` growing)

1. Check `pcs_raft_commit_index`. If it has stopped advancing, the node is not
   committing and compaction cannot run.
2. Compaction is automatic once applied entries pass `snapshot_log_interval`.
   Reduce that value and restart to compact more often.
3. Nothing application-sized is in this log. A file growing past a few megabytes
   means entries are accumulating uncommitted, not that a payload is large.
