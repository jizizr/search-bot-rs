FROM rust:1.92 AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
# Cache dependencies by building a dummy project first
RUN mkdir -p src/bin && echo "fn main() {}" > src/main.rs && echo "fn main() {}" > src/bin/migrate.rs && cargo build --release && rm -rf src
COPY src/ src/
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/search-bot-rs /usr/local/bin/search-bot-rs
ENTRYPOINT ["search-bot-rs"]
