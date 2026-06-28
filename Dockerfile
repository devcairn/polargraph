# Multi-stage build for polargraphd
#
# Stage 1 (builder): compiles the full workspace with all Rust and C++ tooling.
# Stage 2 (runtime): slim image that ships only the binary.

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

# Copy workspace manifests first so Docker can cache the dependency-fetch layer.
COPY Cargo.toml Cargo.lock ./
COPY crates/polargraph-core/Cargo.toml      crates/polargraph-core/Cargo.toml
COPY crates/polargraph-storage/Cargo.toml   crates/polargraph-storage/Cargo.toml
COPY crates/polargraph-query/Cargo.toml     crates/polargraph-query/Cargo.toml
COPY crates/polargraph-server/Cargo.toml    crates/polargraph-server/Cargo.toml
COPY crates/polargraph-server/build.rs      crates/polargraph-server/build.rs
COPY crates/polargraph-server/proto/        crates/polargraph-server/proto/
COPY crates/polargraph-bench/Cargo.toml     crates/polargraph-bench/Cargo.toml
COPY crates/polargraph-import/Cargo.toml    crates/polargraph-import/Cargo.toml
COPY crates/polargraph-rest/Cargo.toml      crates/polargraph-rest/Cargo.toml
COPY crates/polargraph-rest/build.rs        crates/polargraph-rest/build.rs
COPY crates/polargraph-sparql/Cargo.toml   crates/polargraph-sparql/Cargo.toml

# Stub out every crate's source so `cargo fetch` / dependency compilation
# succeeds without the real source files.
RUN for crate in polargraph-core polargraph-storage polargraph-query polargraph-rest polargraph-sparql; do \
        mkdir -p crates/$crate/src && \
        printf 'pub fn _stub() {}' > crates/$crate/src/lib.rs; \
    done && \
    for crate in polargraph-server polargraph-bench polargraph-import; do \
        mkdir -p crates/$crate/src && \
        printf 'pub fn _stub() {}' > crates/$crate/src/lib.rs && \
        printf 'fn main() {}'      > crates/$crate/src/main.rs; \
    done && \
    mkdir -p crates/polargraph-storage/benches && \
    printf 'fn main() {}'  > crates/polargraph-storage/benches/storage.rs

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
