# syntax=docker/dockerfile:1.7

# Build the current sibling LakeCat checkout into an optimized Linux ARM64
# executable. The named `lakecat-source` context is declared by Compose, so the
# image never depends on a mutable, pre-staged host binary.
FROM rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS build

WORKDIR /src/lakecat
ARG LAKECAT_SOURCE_REVISION
RUN printf '%s' "$LAKECAT_SOURCE_REVISION" \
    | grep -Eq '^[0-9a-f]{40}$'
COPY --from=lakecat-source . .

ENV CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_RELEASE_OPT_LEVEL=3 \
    CARGO_PROFILE_RELEASE_LTO=fat \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    CARGO_PROFILE_RELEASE_DEBUG=false \
    CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_PROFILE_RELEASE_PANIC=abort

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      libpython3.11-dev \
      protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/lakecat/target \
    RUSTFLAGS="-Dwarnings -Ctarget-cpu=native" \
    cargo build --locked --release \
      -p lakecat-service \
      --bin lakecat-service \
      --features turso-local,sail-local \
      -j1 \
    && install -Dm755 target/release/lakecat-service /out/lakecat-service

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
ARG LAKECAT_SOURCE_REVISION
LABEL org.opencontainers.image.revision=$LAKECAT_SOURCE_REVISION

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libpython3.11 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/lakecat-service /usr/local/bin/lakecat-service

ENV LAKECAT_BIND_ADDR=0.0.0.0:8181 \
    LAKECAT_WAREHOUSE=local \
    LAKECAT_TURSO_PATH=/data/lakecat.db

RUN mkdir -p /data
EXPOSE 8181

HEALTHCHECK --interval=5s --timeout=3s --retries=20 \
    CMD curl -fsS http://localhost:8181/catalog/v1/config || exit 1

ENTRYPOINT ["/usr/local/bin/lakecat-service"]
