# Distributed Order Fulfillment

A self-contained example demonstrating most of PCS's distributed processing
features: field-granular DAG scheduling, parallel and sequential systems,
world resources, checkpointing, Raft consensus, and structured tracing across
three nodes.

---

## Architecture

### 1 — Cluster Topology

```mermaid
graph TB
    subgraph node1["Node 1 (Bootstrap + Generator)"]
        GEN["Order Generator\n(every 10 s)"]
        W1["DistributedRunner"]
        GEN -->|register_master_batch| SM1[("Raft State\nMachine")]
    end

    subgraph node2["Node 2 (Follower)"]
        W2["DistributedRunner"]
        SM2[("Raft State\nMachine")]
    end

    subgraph node3["Node 3 (Follower)"]
        W3["DistributedRunner"]
        SM3[("Raft State\nMachine")]
    end

    SM1 <-->|Raft TCP 9001| SM2
    SM1 <-->|Raft TCP 9001| SM3
    SM2 <-->|Raft TCP 9002| SM3

    SM1 --> W1
    SM2 --> W2
    SM3 --> W3

    W1 -->|invoices| OUT1["/data/output/node1"]
    W2 -->|invoices| OUT2["/data/output/node2"]
    W3 -->|invoices| OUT3["/data/output/node3"]
```

- **Node 1** bootstraps the Raft cluster and runs an embedded generator task.
- All 3 nodes run a `DistributedRunner` that claims, processes, checkpoints, and
  acks batches via Raft-replicated state.
- The generator only writes when node 1 is the leader; followers skip silently.

---

### 2 — Pipeline DAG (4 Stages)

```mermaid
graph LR
    subgraph Stage0["Stage 0 — parallel"]
        VO["ValidateOrder\n(→ validation_status)"]
        DF["DetectFraud\n(→ fraud_score)"]
        CC["ConvertCurrency\n(→ amount_usd)"]
    end

    subgraph Stage1["Stage 1 — sequential"]
        CI["CheckInventory\n(→ inventory_status)"]
    end

    subgraph Stage2["Stage 2 — parallel"]
        AO["ApproveOrder\n(→ processing_status)"]
        CT["ComputeTax\n(→ tax_rate, tax_amount)"]
    end

    subgraph Stage3["Stage 3 — sequential"]
        GI["GenerateInvoice\n(append Invoice rows)"]
    end

    Stage0 --> Stage1 --> Stage2 --> Stage3

    style Stage0 fill:#e8f4f8
    style Stage2 fill:#e8f4f8
```

Systems in Stage 0 and Stage 2 write **disjoint fields** — PCS's
field-level conflict analyser schedules them in the same stage and runs them
concurrently (`ParallelSystem`). Stage 1 and Stage 3 systems need exclusive
world access (`System` with `&mut Pipeline`).

---

### 3 — Data Flow

```mermaid
sequenceDiagram
    participant G as Generator (node1)
    participant R as Raft Cluster
    participant N as Any Node Runner
    participant W as Pipeline Factory
    participant P as Pipeline (4 stages)
    participant O as Output Dir

    loop every 10 s
        G->>G: generate 300–500 Order rows
        G->>R: register_master_batch(batch_id, ipc_bytes)
        R-->>G: MasterBatchRegistered
    end

    N->>R: claim_next_batch(instance_id)
    R-->>N: BatchClaim { batch_id, row_range }
    Note over N: FulfillmentStore intercepts claim<br/>reads IPC from state machine DB
    N->>W: world_factory()
    W-->>N: Pipeline (Order rows + resources)
    N->>P: pipeline.run(world)
    P->>P: Stage 0: validate, fraud, fx-convert
    P->>P: Stage 1: check inventory
    Note over P: checkpoint after stage 2
    P->>P: Stage 2: approve, compute tax
    P->>P: Stage 3: generate invoices
    P-->>N: Pipeline with Invoice rows
    N->>O: write invoices_{batch_id}.json
    N->>R: ack_claim(claim_id)
```

---

### 4 — Component Schema

```mermaid
erDiagram
    Order {
        Utf8   id
        Utf8   customer_id
        Utf8   product_id
        Int64  quantity
        Float64 amount_original
        Utf8   currency
        Float64 amount_usd
        Utf8   region
        Float64 fraud_score
        Float64 tax_rate
        Float64 tax_amount
        Utf8   validation_status
        Utf8   inventory_status
        Utf8   processing_status
    }

    Invoice {
        Utf8    order_id
        Float64 subtotal
        Float64 tax_rate
        Float64 tax_amount
        Float64 total
        Utf8    issued_at
        Utf8    status
    }

    Order ||--o| Invoice : "GenerateInvoice"
```

---

## Running Locally

### Prerequisites

- Rust 1.95+
- 3 terminals

### Build

```bash
cargo build --example distributed_fulfillment --features service-cluster
```

### Terminal 1 — Bootstrap node + generator

```bash
RUST_LOG=trace ./target/debug/examples/distributed_fulfillment \
  --node-id 1 --bootstrap \
  --listen 127.0.0.1:9001 \
  --data-dir /tmp/fulfillment/node1 \
  --output-dir /tmp/fulfillment/output/node1 \
  --peers 127.0.0.1:9002,127.0.0.1:9003 \
  --generator-interval 10
```

### Terminal 2

```bash
RUST_LOG=trace ./target/debug/examples/distributed_fulfillment \
  --node-id 2 \
  --listen 127.0.0.1:9002 \
  --data-dir /tmp/fulfillment/node2 \
  --output-dir /tmp/fulfillment/output/node2 \
  --peers 127.0.0.1:9001,127.0.0.1:9003
```

### Terminal 3

```bash
RUST_LOG=trace ./target/debug/examples/distributed_fulfillment \
  --node-id 3 \
  --listen 127.0.0.1:9003 \
  --data-dir /tmp/fulfillment/node3 \
  --output-dir /tmp/fulfillment/output/node3 \
  --peers 127.0.0.1:9001,127.0.0.1:9002
```

---

## Running with Docker Compose

```bash
# Build + start all 3 nodes
docker compose -f examples/distributed_fulfillment/docker-compose.yml up --build

# Watch logs in real-time
docker compose -f examples/distributed_fulfillment/docker-compose.yml \
  logs -f --tail=50 node1 node2 node3

# Stop
docker compose -f examples/distributed_fulfillment/docker-compose.yml down
```

---

## Observable Behaviour

| What you see | What it means |
|---|---|
| `generator: registered batch N (M rows)` | Node 1 is leader, new work available |
| `generator: skipping batch (not leader …)` | Node 1 lost leadership; another node is leading |
| `claimed batch N` | A node won the race for this batch |
| `stage 0–3 logs` | Pipeline executing on the claiming node |
| `acked batch N` | Batch fully processed; won't be retried |
| `checkpoint at stage 2` | Intermediate state saved; safe to resume after crash |

---

## Feature Highlights

| Feature | Where |
|---|---|
| Field-granular DAG scheduling | `systems.rs` → `build_pipeline()` |
| `ParallelSystem` (concurrent stage) | `ValidateOrderSystem`, `DetectFraudSystem`, `ConvertCurrencySystem`, `ApproveOrderSystem`, `ComputeTaxSystem` |
| `System` (sequential, `&mut Pipeline`) | `CheckInventorySystem`, `GenerateInvoiceSystem` |
| Pipeline resources (non-columnar) | `resources.rs` → `FxRateTable`, `TaxRateTable`, `InventoryCatalog`, `NodeId` |
| Retry config | `GenerateInvoiceSystem::config()` → `RetryMode::Fixed { retries: 3 }` |
| `world.append::<Invoice>()` | `GenerateInvoiceSystem::run()` — Invoice rows created at runtime |
| Raft consensus | `ArrowRaftDriver` via `distributed-raft` feature |
| Checkpoint every N stages | `CheckpointStrategy::EveryNStages(2)` in `RunnerConfig` |
| At-least-once semantics | `DistributedRunner` claim → ack cycle |
| Structured tracing | Every system emits `tracing::info!` with `node_id`, `batch_id`, `stage` |
