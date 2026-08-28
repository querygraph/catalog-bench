# syntax=docker/dockerfile:1.7

ARG TRINO_BASE_IMAGE=trinodb/trino@sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534
FROM catalog-bench-engine-runner AS runner

FROM ${TRINO_BASE_IMAGE} AS connector
LABEL org.opencontainers.image.source="https://github.com/apache/iceberg" \
      org.opencontainers.image.version="1.11.0" \
      org.opencontainers.image.revision="6976e020b894f6a6777704df2b8c4458cb291ae9"

FROM ${TRINO_BASE_IMAGE} AS runtime
ARG CATALOG_BENCH_SOURCE_REVISION
ARG TRINO_SOURCE_REVISION
USER root
COPY --from=runner /usr/local/bin/catalog-bench-engine /usr/local/bin/catalog-bench-engine
RUN test -x /usr/local/bin/catalog-bench-engine
LABEL org.opencontainers.image.source="https://github.com/trinodb/trino" \
      org.opencontainers.image.version="483" \
      org.opencontainers.image.revision="${TRINO_SOURCE_REVISION}" \
      org.opencontainers.image.base.digest="sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534" \
      io.querygraph.catalog-bench.runner-source-revision="${CATALOG_BENCH_SOURCE_REVISION}"
USER trino
ENTRYPOINT ["/usr/local/bin/catalog-bench-engine"]
