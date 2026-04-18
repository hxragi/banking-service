FROM rust:1.94-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libssl-dev \
    pkg-config \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install sqlx-cli --no-default-features --features postgres
COPY Cargo.toml Cargo.lock ./

COPY proto ./proto
COPY build.rs ./

COPY src ./src
COPY migrations ./migrations

RUN cargo build --release

RUN strip /app/target/release/bank-service

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN groupadd -r bank && useradd -r -g bank bank

COPY --from=builder /app/target/release/bank-service /usr/local/bin/bank-service

COPY --from=builder /app/migrations ./migrations

RUN chown -R bank:bank /app

USER bank

EXPOSE 50051 8080 9090

ENV RUST_LOG=info
ENV DATABASE_URL=""
ENV KAFKA_BOOTSTRAP_SERVERS=""

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["wget", "-q", "--spider", "http://localhost:8080/health"]

ENTRYPOINT ["/usr/local/bin/bank-service"]
