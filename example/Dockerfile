FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler cmake g++ libclang-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cd example && cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/example/target/release/cluster_example /usr/local/bin/cluster_example
COPY example/k8s/scripts/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 7000 8000

ENTRYPOINT ["/entrypoint.sh"]
