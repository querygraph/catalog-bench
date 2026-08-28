# MinIO stopped publishing new community-server container images after the
# 2025-10-15 release. Build that exact upstream source revision with its pinned
# Go toolchain instead of silently substituting an older binary.
ARG GO_IMAGE=golang:1.24.8-bookworm@sha256:4ed690d6649d63c312b99a6120025ec79ce3b542968a37da53d6236c7c61a848
ARG CATALOG_BENCH_HELPER_SOURCE_REVISION=8e04f70f2d509f01a233ea3d54690748a03b7efc

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS tool-build
ARG TARGETARCH
ARG CATALOG_BENCH_HELPER_SOURCE_REVISION
WORKDIR /src/tools
RUN printf '%s' "$CATALOG_BENCH_HELPER_SOURCE_REVISION" \
    | grep -Eq '^[0-9a-f]{40}$'
COPY --from=catalog-bench-helper-source docker/minio/tools/go.mod docker/minio/tools/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked go mod download
COPY --from=catalog-bench-helper-source docker/minio/tools/ ./
RUN go test ./...
RUN CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w" -o /out/ensure-bucket ./cmd/ensure-bucket \
    && CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck \
    && CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w" -o /out/lakekeeper-setup ./cmd/lakekeeper-setup \
    && CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w" -o /out/polaris-setup ./cmd/polaris-setup \
    && CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w" -o /out/wait-http ./cmd/wait-http \
    && CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w" -o /out/fault-proxy ./cmd/fault-proxy \
    && CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w" -o /out/object-fault-probe ./cmd/object-fault-probe

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS minio-build
ARG TARGETARCH
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_REVISION=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
WORKDIR /src/minio
RUN git init \
    && git remote add origin https://github.com/minio/minio.git \
    && git fetch --depth=1 origin "refs/tags/${MINIO_TAG}:refs/tags/${MINIO_TAG}" \
    && git checkout --detach "$MINIO_TAG" \
    && test "$(git rev-parse HEAD)" = "$MINIO_REVISION"
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" \
    go build -tags kqueue -trimpath \
    --ldflags "$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)" \
    -o /out/minio

FROM scratch
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_REVISION=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
ARG CATALOG_BENCH_HELPER_SOURCE_REVISION=8e04f70f2d509f01a233ea3d54690748a03b7efc
LABEL org.opencontainers.image.title="catalog-bench MinIO" \
      org.opencontainers.image.description="Exact source-built MinIO for catalog interoperability evidence" \
      org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.version="$MINIO_TAG" \
      org.opencontainers.image.revision="$MINIO_REVISION" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      io.querygraph.catalog-bench.helper-source-revision="$CATALOG_BENCH_HELPER_SOURCE_REVISION"
COPY --from=minio-build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=minio-build /src/minio/LICENSE /licenses/minio/LICENSE
COPY --from=minio-build /out/minio /usr/local/bin/minio
COPY --from=tool-build /out/ensure-bucket /usr/local/bin/ensure-bucket
COPY --from=tool-build /out/healthcheck /usr/local/bin/healthcheck
COPY --from=tool-build /out/lakekeeper-setup /usr/local/bin/lakekeeper-setup
COPY --from=tool-build /out/polaris-setup /usr/local/bin/polaris-setup
COPY --from=tool-build /out/wait-http /usr/local/bin/wait-http
COPY --from=tool-build /out/fault-proxy /usr/local/bin/fault-proxy
COPY --from=tool-build /out/object-fault-probe /usr/local/bin/object-fault-probe
ENV MINIO_HEALTHCHECK_URL=http://127.0.0.1:9000/minio/health/ready \
    TMPDIR=/data/.tmp
WORKDIR /data
VOLUME ["/data"]
EXPOSE 9000 9001
HEALTHCHECK --interval=2s --timeout=3s --start-period=5s --retries=30 \
    CMD ["/usr/local/bin/healthcheck"]
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
