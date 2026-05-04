FROM rust:1.70-slim-bookworm as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples

# Build release binary
RUN cargo build --release --bin proxy-server

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/proxy-server /usr/local/bin/

EXPOSE 3000

ENV RUST_LOG=info

CMD ["proxy-server"]
