# Self-contained, production-optimized build of the commit benchmark.
FROM rust:1.96.0-bookworm AS build
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
RUN RUSTFLAGS="-Ctarget-cpu=native" \
    cargo build --locked --release -p catalog-bench-commit --bin catalog-bench-commit

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/catalog-bench-commit /usr/local/bin/catalog-bench-commit
ENTRYPOINT ["/usr/local/bin/catalog-bench-commit"]
