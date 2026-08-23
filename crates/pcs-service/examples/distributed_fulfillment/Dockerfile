# Multi-stage build for distributed_fulfillment example.
# Builder: compile the example binary with service-cluster feature.
# Runtime: minimal debian image with just the binary.

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM rust:1.89-slim AS builder

# System deps for openssl / native TLS used by reqwest
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency compilation first — copy manifests only.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY benches ./benches

RUN cargo build \
    --example distributed_fulfillment \
    --features service-cluster \
    --release

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder \
    /app/target/release/examples/distributed_fulfillment \
    /usr/local/bin/distributed_fulfillment

# Data directory — override via volume mount.
RUN mkdir -p /data

ENTRYPOINT ["distributed_fulfillment"]
