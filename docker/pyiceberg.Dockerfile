# The current profile records both the multi-platform index and this Linux
# ARM64 child manifest. Pinning the child makes the executed platform identity
# direct rather than relying on manifest-list selection at build time.
FROM python:3.13.15-slim-bookworm@sha256:e424b523c9296fdef9d2533c368facee1dc45be4c1f8e1555f90c4feac439594

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONPATH=/opt/catalog-bench

COPY clients/pyiceberg/requirements.lock /tmp/requirements.lock
RUN python -m pip install \
      --require-hashes \
      --only-binary=:all: \
      --requirement /tmp/requirements.lock

COPY clients/pyiceberg/catalog_bench_pyiceberg /opt/catalog-bench/catalog_bench_pyiceberg

USER 65534:65534
WORKDIR /work
ENTRYPOINT ["python", "-m", "catalog_bench_pyiceberg"]
