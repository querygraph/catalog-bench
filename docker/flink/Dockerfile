# syntax=docker/dockerfile:1.7

ARG FLINK_BASE_IMAGE=catalog-bench/flink-base:2.1.3-arm64-99a499ed

FROM eclipse-temurin:17-jdk-jammy@sha256:7d5ae9dfe05f32e2a93abd14966de3373961ad18022ee99a647cdbb8867d74e9 AS java-build

ARG CATALOG_BENCH_SOURCE_REVISION
WORKDIR /src
RUN printf '%s' "$CATALOG_BENCH_SOURCE_REVISION" \
    | grep -Eq '^[0-9a-f]{40}$'

ADD --checksum=sha256:80ffca22aed9e8b9713a232f3394fd81d7f20322df75efdb2b047dbd3e3a23bb \
    https://downloads.apache.org/maven/maven-3/3.9.16/binaries/apache-maven-3.9.16-bin.tar.gz \
    /tmp/apache-maven.tar.gz
RUN printf '%s  %s\n' \
      '831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6' \
      '/tmp/apache-maven.tar.gz' \
      | sha512sum --check --strict \
    && mkdir /opt/maven \
    && tar --extract --gzip --file /tmp/apache-maven.tar.gz \
      --directory /opt/maven --strip-components=1 \
    && rm /tmp/apache-maven.tar.gz \
    && /opt/maven/bin/mvn --version \
      | grep -F 'Apache Maven 3.9.16'

COPY crates/engine/flink-runner/pom.xml crates/engine/flink-runner/pom.xml
COPY crates/engine/flink-runner/src crates/engine/flink-runner/src
RUN /opt/maven/bin/mvn \
      --batch-mode \
      --strict-checksums \
      --no-transfer-progress \
      --file crates/engine/flink-runner/pom.xml \
      clean verify \
    && install -Dm644 \
      crates/engine/flink-runner/target/catalog-bench-flink-runner-0.1.0-SNAPSHOT.jar \
      /out/catalog-bench-flink-runner.jar \
    && printf '%s\n' "$CATALOG_BENCH_SOURCE_REVISION" \
      > /out/source-revision

FROM scratch AS connector

ARG ICEBERG_SOURCE_REVISION
LABEL org.opencontainers.image.title="Apache Iceberg Flink 2.1 runtime" \
      org.opencontainers.image.source="https://github.com/apache/iceberg" \
      org.opencontainers.image.version="1.11.0" \
      org.opencontainers.image.revision="$ICEBERG_SOURCE_REVISION" \
      io.querygraph.catalog-bench.iceberg-flink-runtime-coordinate="org.apache.iceberg:iceberg-flink-runtime-2.1:1.11.0" \
      io.querygraph.catalog-bench.iceberg-aws-bundle-coordinate="org.apache.iceberg:iceberg-aws-bundle:1.11.0" \
      io.querygraph.catalog-bench.hadoop-client-version="3.4.3"

ADD --checksum=sha256:39e658d876f253815b4b17c6676bebf6a24d517afe93a21298106d1d7fa97331 \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar \
    /opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar
ADD --checksum=sha256:38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar \
    /opt/iceberg/iceberg-aws-bundle-1.11.0.jar
ADD --checksum=sha256:6443332781f26a7199009d9517cd1eb013fbf54ca1c9759a2a9e487542b1d52a \
    https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar \
    /opt/iceberg/hadoop-client-api-3.4.3.jar
ADD --checksum=sha256:bd20267b0555766ae84697dd888f074ca326d8e2ec3c0008928a6ac2966c67de \
    https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar \
    /opt/iceberg/hadoop-client-runtime-3.4.3.jar

CMD ["/opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar"]

FROM catalog-bench-engine-runner AS runner

ARG CATALOG_BENCH_SOURCE_REVISION
COPY --from=java-build \
    /out/catalog-bench-flink-runner.jar \
    /opt/catalog-bench/catalog-bench-flink-runner.jar
COPY --from=java-build \
    /out/source-revision \
    /opt/catalog-bench/source-revision
RUN test "$(cat /usr/local/share/catalog-bench/source-revision)" \
      = "$CATALOG_BENCH_SOURCE_REVISION" \
    && test "$(cat /opt/catalog-bench/source-revision)" \
      = "$CATALOG_BENCH_SOURCE_REVISION"

LABEL org.opencontainers.image.title="catalog-bench Flink runner donor" \
      org.opencontainers.image.revision="$CATALOG_BENCH_SOURCE_REVISION" \
      io.querygraph.catalog-bench.runner-source-revision="$CATALOG_BENCH_SOURCE_REVISION"

FROM ${FLINK_BASE_IMAGE} AS runtime

ARG FLINK_SOURCE_REVISION
ARG ICEBERG_SOURCE_REVISION
ARG CATALOG_BENCH_SOURCE_REVISION
USER root
RUN printf '%s\n' \
      "$FLINK_SOURCE_REVISION" \
      "$ICEBERG_SOURCE_REVISION" \
      "$CATALOG_BENCH_SOURCE_REVISION" \
    | grep -Ecx '[0-9a-f]{40}' \
    | grep -qx '3'

COPY --from=runner --chown=flink:flink \
    /usr/local/bin/catalog-bench-engine \
    /usr/local/bin/catalog-bench-engine
COPY --from=runner --chown=flink:flink \
    /usr/local/share/catalog-bench/source-revision \
    /usr/local/share/catalog-bench/source-revision
COPY --from=runner --chown=flink:flink \
    /opt/catalog-bench/catalog-bench-flink-runner.jar \
    /opt/catalog-bench/catalog-bench-flink-runner.jar
COPY --from=runner --chown=flink:flink \
    /opt/catalog-bench/source-revision \
    /opt/catalog-bench/source-revision
RUN test "$(cat /usr/local/share/catalog-bench/source-revision)" \
      = "$CATALOG_BENCH_SOURCE_REVISION" \
    && test "$(cat /opt/catalog-bench/source-revision)" \
      = "$CATALOG_BENCH_SOURCE_REVISION"

COPY --from=connector --chown=flink:flink \
    /opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar \
    /opt/flink/lib/iceberg-flink-runtime-2.1-1.11.0.jar
COPY --from=connector --chown=flink:flink \
    /opt/iceberg/iceberg-aws-bundle-1.11.0.jar \
    /opt/flink/lib/iceberg-aws-bundle-1.11.0.jar
COPY --from=connector --chown=flink:flink \
    /opt/iceberg/hadoop-client-api-3.4.3.jar \
    /opt/flink/lib/hadoop-client-api-3.4.3.jar
COPY --from=connector --chown=flink:flink \
    /opt/iceberg/hadoop-client-runtime-3.4.3.jar \
    /opt/flink/lib/hadoop-client-runtime-3.4.3.jar

LABEL org.opencontainers.image.title="catalog-bench Flink interoperability runtime" \
      org.opencontainers.image.source="https://github.com/apache/flink" \
      org.opencontainers.image.version="2.1.3" \
      org.opencontainers.image.revision="$FLINK_SOURCE_REVISION" \
      org.opencontainers.image.base.name="docker.io/library/flink:2.1.3-scala_2.12-java17" \
      org.opencontainers.image.base.digest="sha256:99a499ed147b28d358486066ab8308e351b232b2ac81aff69157fdb349c84e18" \
      io.querygraph.catalog-bench.runner-source-revision="$CATALOG_BENCH_SOURCE_REVISION" \
      io.querygraph.catalog-bench.iceberg-source-revision="$ICEBERG_SOURCE_REVISION"

USER flink
