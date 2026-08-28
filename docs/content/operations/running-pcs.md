+++
title = "Operating pcs-service"
description = "Bootstrapping a three-node cluster, sizing the lease TTL, what a node writes to disk, and the runbooks for when one fails."
template = "page.html"
+++

# Operating pcs-service

Running a `pcs-service` deployment: bringing up a Raft cluster, what it writes to
disk, and what to do when a node fails.

Three pages come first. [Installation](@/quickstart/installation.md) installs the
binary. [Configuration](@/service/configuration.md) is the config surface and the
load-time gates. [Observability](@/service/observability.md) is the HTTP probes,
readiness and graceful shutdown. This page starts where those leave off.

A cluster node needs `--features service-cluster`, which is not in the default
bundle. `mode "cluster"` in a binary without it produces a startup error and
exits 1.

---

## Cluster Mode

### When to Use Cluster Mode

Use cluster when:
- A single node failure must not halt processing.
- The workload exceeds one machine's throughput.

Do not default to cluster mode. Raft needs at least three nodes for quorum,
membership is managed manually, and standalone with a good backup
strategy is usually enough.

### Cluster Mode and the Workflow Graph

**A cluster-mode workflow declares exactly one processor node and no `source`,
`sink` or `link`.** Config validation returns an error otherwise:

```text,name=The error config validation returns
cluster mode runs exactly one 'wasm' or 'plugin' node with no source, sink or
link (2 node(s), 1 link(s) declared)
```

Cluster mode ingests through `PartitionSource`, a distributed pull mechanism:
batches are registered by a producer external to the config, through
`register_master_batch`. Each node drives that one runtime and checkpoints its
output, rather than draining it into a locally declared sink.

### Cluster Config

```kdl,name=The cluster header and its timings
mode "cluster"
bootstrap #false                 // #true on the initial node ONLY at first start-up

// Timing. Invariant: lease_ttl_ms >= 3 * election_timeout_ms
lease_ttl_ms 30000               // 30 s
election_timeout_ms 1500         // 1.5 s
heartbeat_interval_ms 300        // 300 ms
snapshot_log_interval 10000      // trigger snapshot every 10 000 committed entries

// id is unique per node; override with --node-id or PCS_NODE_ID.
// data_dir holds the redb files and must be persistent storage.
node id=1 name="node-1" data_dir="/var/lib/pcs/data"

// Raft transport addresses, not HTTP.
peer id=1 addr="10.0.0.1:9000"
peer id=2 addr="10.0.0.2:9000"
peer id=3 addr="10.0.0.3:9000"

// Exactly one processor node, and no source, sink or link. The processor's
// `describe()` supplies its component list; nothing in the config names it.
workflow "events" {
    wasm "process_events" module="/var/lib/pcs/pipelines/events.wasm"
}

http bind="0.0.0.0:8080"

observability log_format="json" log_level="info"
```

### Bootstrap a Three-Node Cluster

This walkthrough assumes three machines at `10.0.0.1` to `10.0.0.3`, Raft on
port 9000 and HTTP on port 8080.

**Step 1: Prepare data directories** (all three nodes):

```bash,name=Step 1 prepare the data directories
mkdir -p /var/lib/pcs/data
```

**Step 2: Pre-flight check on node 1**:

```bash,name=Step 2 pre-flight check on node 1
# On 10.0.0.1 with bootstrap #true in the config:
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
# On 10.0.0.2:
pcs-service serve --config node2.kdl

# On 10.0.0.3:
pcs-service serve --config node3.kdl
```

**Step 5: Verify**:

```bash,name=Step 5 verify from node 1
pcs-service cluster status --addr http://10.0.0.1:8080
```

No Raft probe is wired into the HTTP state, so the command prints:

```text,name=Expected cluster status output
node 1  mode=cluster
Note: cluster details are not available in v1. Full Raft metrics integration
is planned for v1.1.
```

Query the raw status JSON:

```bash,name=Query the raw status JSON
curl -s http://10.0.0.1:8080/status | jq .
```

### Membership Management

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

### Failure Semantics (Cluster)

At-least-once delivery enforced by Raft-backed leases:

- A node claims a row-range batch from `PartitionSource`. The claim carries a
  TTL equal to `lease_ttl_ms`.
- If the node does not ack within the TTL (crash, network partition, or slow
  processing), the lease expires and another node re-claims the batch.
- A node that loses its lease mid-run stops processing immediately and
  releases the claim. The batch returns to pending and is re-claimed.
- Ack is issued only after the processor run and checkpoint write both complete.

**SIGKILL mid-claim**: the claim expires after `lease_ttl_ms` and is retried by
another node. Processing pauses by up to one TTL for any batch in flight at kill
time. No data is permanently lost.

---

## Data Directory Layout

- `raft-log.redb` holds the Raft log entries, an openraft redb B-tree.
- `state-machine.redb` holds the applied state: the batch registry, claims and
  checkpoints.
- `snapshots/<term>-<index>/state.ipc` holds each installed Raft snapshot, in
  Arrow IPC format.

Node identity comes from the config and the CLI, not from this directory.

**`raft-log.redb`**: grows until a snapshot installs and the preceding entries
are compacted. Snapshots trigger automatically once committed log entries
exceed `snapshot_log_interval` (default: 10 000); there is no manual
force-snapshot command. If the file grows unexpectedly, check that
`pcs_raft_commit_index` is advancing and that snapshot installation completes.

**`state-machine.redb`**: holds batch registrations, active claims, the
secondary claims-by-batch index, and checkpoint IPC bytes. It is serialized
into Raft snapshots. Back it up before manual maintenance. Its size is stable
in steady state.

**`snapshots/`**: written during `build_snapshot`, read during
`install_snapshot`. New nodes joining the cluster receive a full snapshot over
TCP (chunked at 4 MiB per frame). Old snapshots are cleaned up after a newer
one installs. Do not delete these manually while the node is running.

### `MAX_LOG_ENTRY_BYTES` Cap

Two independent limits apply:

- **`MAX_LOG_ENTRY_BYTES`**: 1 MiB, defined in
  `crates/pcs-service/src/distributed/partition.rs`. Caps the Arrow IPC
  payload carried *inside a single Raft log entry*: checkpoint snapshots and
  `RegisterMasterBatch` bodies. Payloads above it are rejected with a `Store`
  error before they reach Raft.
- **`MAX_FRAME_BYTES`**: 16 MiB, defined in
  `crates/pcs-service/src/distributed/consensus/transport.rs`. Caps a single
  length-prefixed TCP frame on the peer transport. It bounds the wire, not the
  log entry; snapshot transfers are chunked at 4 MiB to stay under it.

If the workload produces large checkpoints, raise `MAX_LOG_ENTRY_BYTES` in
`crates/pcs-service/src/distributed/partition.rs` and rebuild with
`--features service-cluster`. Prefer shorter batches, smaller per-entity
state, or splitting the component upstream first: every log entry is
replicated to every node.

---

## Checkpoint Strategies

Checkpoint strategy is set on `DistributedRunner`, in code rather than config.

| Strategy | Behaviour | Use when |
|----------|-----------|----------|
| `EveryStage` | Checkpoint after every pipeline stage | Maximum recovery granularity; highest write amplification |
| `EveryNStages(n)` | Checkpoint every N stages | Balance durability and write cost |
| `None` | No checkpointing | Idempotent pipelines that can safely re-run from the start |

The default is `EveryStage`. For long pipelines with expensive stages, consider
`EveryNStages` to reduce redb write pressure.

---

## Shutdown and SIGKILL

[Observability](@/service/observability.md#graceful-shutdown) covers the signal
handling and the 30-second budget. Two behaviours belong to a cluster.

On a clean stop the claiming node completes or releases its current batch before
exiting. The remaining nodes elect a new leader after `election_timeout_ms * 2` if
the exiting node was the leader.

`SIGKILL` bypasses the handler. The claim expires after `lease_ttl_ms` and is
retried by another node, so processing pauses by up to one lease TTL for any batch
in flight at kill time. No data is permanently lost.

---

## Log Output

`log_format "json"` under `observability`, or `--log-format json`, switches
from the coloured development output to structured JSON for Loki, CloudWatch or
Datadog. `--log-level`, `PCS_LOG_LEVEL` or `observability.log_level` sets the
filter:

- `error`: production default when logs are expensive
- `info`: startup, shutdown, batch completion, leader changes
- `debug`: per-stage timing, lease renewal events
- `trace`: Arrow IPC encode and decode, Raft message flow, very verbose

`otlp_endpoint` exports the `workflow.batch` span tree over OTLP/HTTP. Metrics are
not exported that way; they stay on `/metrics`. See [Tracing](@/tracing.md).

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
| `PCS_OTLP_ENDPOINT` | `--otlp-endpoint` | OTLP/HTTP collector root for span export |

The config file also supports `${VAR}` and `${VAR:-default}` placeholder expansion.

---

## Exit Codes

| Exit | Condition |
|------|-----------|
| `0` | Clean exit (successful run, `one_shot` complete, SIGTERM drain) |
| `1` | Runner error, config validation failure, or 30-second shutdown budget exceeded |

`cluster join` and `cluster leave` exit 0 and print a manual workaround rather
than an error, because dynamic membership is not implemented.

---

## Common Operational Scenarios

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

**Majority side** (quorum present): continues operating. If the leader is on
this side it keeps committing; if not, the majority elects a new leader.

**Minority side** (no quorum): all nodes become followers and stop accepting
writes. In-flight claims are not acked; they expire and are retried by the
majority side after `lease_ttl_ms`.

When the partition heals, minority-side nodes re-join and receive a snapshot or
log replay from the leader. No manual action is required.

### Disk pressure (`raft-log.redb` growing)

1. Check `pcs_raft_commit_index`. If it has stopped advancing, the state
   machine may be stuck.
2. Snapshots are triggered automatically when committed entries exceed
   `snapshot_log_interval`. Reduce this value in the config and restart to
   force more frequent snapshots.
3. After a snapshot installs, log compaction removes old entries and frees
   space.

