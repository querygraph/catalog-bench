# syntax=docker/dockerfile:1.7

FROM rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS build
ARG DUCKDB_SOURCE_REVISION
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake ninja-build \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY --from=duckdb-source . .
RUN --mount=type=cache,target=/src/build \
    printf '%s' "${DUCKDB_SOURCE_REVISION}" | grep -Eq '^[0-9a-f]{40}$' \
    && cmake -S . -B build/release -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_SHELL=ON \
      -DBUILD_UNITTESTS=OFF \
      -DENABLE_EXTENSION_AUTOLOADING=ON \
      -DENABLE_EXTENSION_AUTOINSTALL=OFF \
      -DOVERRIDE_GIT_DESCRIBE=v1.5.3-0-g14eca11bd9 \
    && cmake --build build/release --target duckdb -j3 \
    && install -Dm755 build/release/duckdb /out/duckdb

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS extensions
ADD --checksum=sha256:4d869567d4e23b86a78388faf5e1fc2c1b9197be66cf51231ab9193ee8e3b22c \
    http://extensions.duckdb.org/v1.5.3/linux_arm64/iceberg.duckdb_extension.gz \
    /tmp/iceberg.duckdb_extension.gz
ADD --checksum=sha256:bde75aadc4ebf9edb4b9fecf4aac6025f35fdae56116b2673b0b4990242c3a02 \
    http://extensions.duckdb.org/v1.5.3/linux_arm64/httpfs.duckdb_extension.gz \
    /tmp/httpfs.duckdb_extension.gz
ADD --checksum=sha256:dcd28c0f227b714e524d6d7eae79db7d14a10029303faed6fce71cdcbb81e08d \
    http://extensions.duckdb.org/v1.5.3/linux_arm64/avro.duckdb_extension.gz \
    /tmp/avro.duckdb_extension.gz
RUN mkdir -p /out/v1.5.3/linux_arm64 \
    && gzip -dc /tmp/iceberg.duckdb_extension.gz > /out/v1.5.3/linux_arm64/iceberg.duckdb_extension \
    && gzip -dc /tmp/httpfs.duckdb_extension.gz > /out/v1.5.3/linux_arm64/httpfs.duckdb_extension \
    && gzip -dc /tmp/avro.duckdb_extension.gz > /out/v1.5.3/linux_arm64/avro.duckdb_extension \
    && printf '%s  %s\n' \
      162a90f01983e0c680822aedc72e0612ac938c6debd9686653ac2f13bc3f0402 \
      /out/v1.5.3/linux_arm64/iceberg.duckdb_extension \
      69fae819202f4aea2dfd2dbe8a4ef468684ddd4cc990f2f5bb86c3e83476f36b \
      /out/v1.5.3/linux_arm64/httpfs.duckdb_extension \
      d2e643e3408d553b9e2e6077dea548dc9ede6d85752ca6e8112c7fd50d1a0d40 \
      /out/v1.5.3/linux_arm64/avro.duckdb_extension \
      | sha256sum --check --strict

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS runtime
ARG DUCKDB_SOURCE_REVISION
COPY --from=build /out/duckdb /usr/local/bin/duckdb
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=extensions /out /opt/duckdb/.duckdb/extensions
RUN useradd --system --uid 10001 --home-dir /opt/duckdb --shell /usr/sbin/nologin duckdb \
    && test -x /usr/local/bin/duckdb
ENV HOME=/opt/duckdb
LABEL org.opencontainers.image.source="https://github.com/duckdb/duckdb" \
      org.opencontainers.image.version="1.5.3" \
      org.opencontainers.image.revision="${DUCKDB_SOURCE_REVISION}" \
      io.querygraph.catalog-bench.duckdb-iceberg-revision="4008894c57168e0e9dff00e87cd725c5168fd81e" \
      io.querygraph.catalog-bench.duckdb-httpfs-revision="52afb4204a3238d6ee132e83340f8d68c40ee91c" \
      io.querygraph.catalog-bench.duckdb-avro-revision="7f423d69709045e38f8431b3470e0395fce1a595"
USER duckdb
ENTRYPOINT ["/usr/local/bin/duckdb"]
