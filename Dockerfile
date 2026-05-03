FROM rust:1.94-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libssl-dev \
    pkg-config \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./

COPY crates/domain/Cargo.toml ./crates/domain/
COPY crates/application/Cargo.toml ./crates/application/
COPY crates/infrastructure/Cargo.toml ./crates/infrastructure/
COPY crates/presentation/Cargo.toml ./crates/presentation/
COPY crates/bootstraper/Cargo.toml ./crates/bootstraper/

RUN mkdir -p crates/domain/src && \
    mkdir -p crates/application/src && \
    mkdir -p crates/infrastructure/src && \
    mkdir -p crates/presentation/src && \
    mkdir -p crates/bootstraper/src

COPY proto ./proto
COPY crates/presentation/build.rs ./crates/presentation/build.rs

RUN cargo build --release 2>/dev/null || true

COPY crates ./crates
COPY migrations ./migrations

RUN cargo build --release

RUN strip /app/target/release/bootstraper

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN groupadd -r bank && useradd -r -g bank bank

COPY --from=builder /app/target/release/bootstraper /usr/local/bin/bank-service
COPY --from=builder /app/migrations ./migrations

RUN chown -R bank:bank /app

USER bank

EXPOSE 50051 8080 9090

ENV RUST_LOG=info

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["wget", "-q", "--spider", "http://localhost:8080/health"]

ENTRYPOINT ["/usr/local/bin/bank-service"]
