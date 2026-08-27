# BuildKit verifies both Maven Central payloads before they become an image
# layer. The connector target is independently taggable evidence; the runtime
# target copies these same bytes into the exact Spark image that executes them.
ARG SPARK_BASE_IMAGE=catalog-bench/spark-base:4.1.3-arm64-bf9d035a
FROM scratch AS connector

ARG ICEBERG_SOURCE_REVISION
LABEL org.opencontainers.image.title="Apache Iceberg Spark 4.1 runtime" \
      org.opencontainers.image.source="https://github.com/apache/iceberg" \
      org.opencontainers.image.version="1.11.0" \
      org.opencontainers.image.revision="$ICEBERG_SOURCE_REVISION" \
      io.querygraph.catalog-bench.iceberg-spark-runtime-coordinate="org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0" \
      io.querygraph.catalog-bench.iceberg-aws-bundle-coordinate="org.apache.iceberg:iceberg-aws-bundle:1.11.0"

ADD --checksum=sha256:d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242 \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    /opt/iceberg/iceberg-spark-runtime-4.1_2.13-1.11.0.jar
ADD --checksum=sha256:38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar \
    /opt/iceberg/iceberg-aws-bundle-1.11.0.jar

# `docker create` needs a command even though this evidence image is never run.
CMD ["/opt/iceberg/iceberg-spark-runtime-4.1_2.13-1.11.0.jar"]

# Named Compose build context backed by the exact public catalog-bench commit.
FROM catalog-bench-engine-runner AS engine-runner

# The materialization launcher creates this local indirection only after proving
# it resolves to the broad profile's exact index on Linux ARM64. This avoids a
# second registry lookup while keeping the identity check outside mutable tags.
FROM ${SPARK_BASE_IMAGE} AS runtime

ARG SPARK_SOURCE_REVISION
ARG ICEBERG_SOURCE_REVISION
ARG CATALOG_BENCH_SOURCE_REVISION
USER root
RUN printf '%s\n' \
      "$SPARK_SOURCE_REVISION" \
      "$ICEBERG_SOURCE_REVISION" \
      "$CATALOG_BENCH_SOURCE_REVISION" \
    | grep -Ecx '[0-9a-f]{40}' \
    | grep -qx '3'

COPY --from=engine-runner --chown=185:185 \
    /usr/local/bin/catalog-bench-engine \
    /usr/local/bin/catalog-bench-engine
COPY --from=engine-runner --chown=185:185 \
    /usr/local/share/catalog-bench/source-revision \
    /usr/local/share/catalog-bench/source-revision
RUN test "$(cat /usr/local/share/catalog-bench/source-revision)" \
      = "$CATALOG_BENCH_SOURCE_REVISION"

COPY --from=connector --chown=185:185 \
    /opt/iceberg/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar
COPY --from=connector --chown=185:185 \
    /opt/iceberg/iceberg-aws-bundle-1.11.0.jar \
    /opt/spark/jars/iceberg-aws-bundle-1.11.0.jar

LABEL org.opencontainers.image.title="catalog-bench Spark interoperability runtime" \
      org.opencontainers.image.source="https://github.com/apache/spark" \
      org.opencontainers.image.version="4.1.3" \
      org.opencontainers.image.revision="$SPARK_SOURCE_REVISION" \
      org.opencontainers.image.base.name="docker.io/apache/spark:4.1.3" \
      org.opencontainers.image.base.digest="sha256:f6831c619d0f6f07fe41912a5be499f6a7c0c1e9f18322d0c703ff21d2f30cd1" \
      io.querygraph.catalog-bench.runner-source-revision="$CATALOG_BENCH_SOURCE_REVISION" \
      io.querygraph.catalog-bench.iceberg-source-revision="$ICEBERG_SOURCE_REVISION"

USER 185:185
