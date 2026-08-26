# syntax=docker/dockerfile:1.7

# Self-contained, production-optimized benchmark and conformance executables.
FROM rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
ENV CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_RELEASE_OPT_LEVEL=3 \
    CARGO_PROFILE_RELEASE_LTO=fat \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    CARGO_PROFILE_RELEASE_DEBUG=false \
    CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_PROFILE_RELEASE_PANIC=abort
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    RUSTFLAGS="-Dwarnings -Ctarget-cpu=native" \
    cargo build --locked --release \
      -p catalog-bench-commit \
      -p catalog-bench-conformance \
      -j1 \
    && install -Dm755 target/release/catalog-bench-commit /out/catalog-bench-commit \
    && install -Dm755 target/release/catalog-bench-conformance /out/catalog-bench-conformance

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/catalog-bench-commit /usr/local/bin/catalog-bench-commit
COPY --from=build /out/catalog-bench-conformance /usr/local/bin/catalog-bench-conformance
ENTRYPOINT ["/usr/local/bin/catalog-bench-commit"]
