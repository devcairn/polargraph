# Multi-stage build for polargraphd
#
# Stage 1 (builder): compiles the full workspace with all Rust and C++ tooling.
# Stage 2 (runtime): slim image that ships only the binary.

# ── Stage 0: dependency skeleton ─────────────────────────────────────────────
# Manifests, build scripts and protos unchanged, plus a stub for every Cargo
# target root (lib, bins, benches, examples, tests) — see
# scripts/docker-skeleton.sh. Its output only changes when manifests or the
# set of targets change, so the dependency layer below stays cached.
FROM debian:bookworm-slim AS skeleton
WORKDIR /src
COPY scripts/docker-skeleton.sh /usr/local/bin/docker-skeleton.sh
COPY crates/ crates/
RUN sh /usr/local/bin/docker-skeleton.sh /src /skeleton

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM rust:1-bookworm AS builder

WORKDIR /build

# Build dependencies:
#   clang / libclang-dev  — required by the rocksdb crate's bindgen step
#   cmake                 — rocksdb bundled build
#   protobuf-compiler     — tonic-build compiles polargraph.proto at build time
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        libclang-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Workspace manifests + the stubbed crate skeleton, so Docker can cache the
# dependency build until a manifest (or the set of targets) changes.
COPY Cargo.toml Cargo.lock ./
COPY --from=skeleton /skeleton/crates crates

# Pre-compile dependencies (cached as long as Cargo.toml/lock don't change).
RUN cargo build --release -p polargraph-server 2>&1 || true
RUN cargo fetch

# Now copy the real source and do the proper release build.
COPY crates/ crates/

# Touch files so Cargo notices the source changed after the stub build.
RUN find crates -name "*.rs" | xargs touch

RUN cargo build --release -p polargraph-server

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# libgcc-s1 / libstdc++6 are needed for C++ code linked into the rocksdb
# bundled build.  ca-certificates is useful for TLS in future integrations.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgcc-s1 \
        libstdc++6 \
        ca-certificates \
        wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/polargraphd /app/polargraphd

# Default configuration (overridable via env vars or CLI flags).
ENV POLARGRAPH_DATA_DIR=/data
ENV POLARGRAPH_LISTEN_ADDR=0.0.0.0:50051
ENV RUST_LOG=info

VOLUME ["/data"]

EXPOSE 50051

ENTRYPOINT ["/app/polargraphd"]
