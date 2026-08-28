#!/usr/bin/env bash
# Materialize the exact Spark and Iceberg images in the local Docker engine.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_dir/.." && pwd -P)"
base_reference="apache/spark:4.1.3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211"
base_local_reference="catalog-bench/spark-base:4.1.3-arm64-bf9d035a"
expected_index_digest="sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211"

if ! command -v docker >/dev/null 2>&1; then
  echo "required command is unavailable: docker" >&2
  exit 1
fi

if ! docker image inspect "$base_reference" >/dev/null 2>&1; then
  docker pull "$base_reference"
fi

actual_index_digest="$(
  docker image inspect --format '{{.Descriptor.digest}}' "$base_reference"
)"
actual_operating_system="$(docker image inspect --format '{{.Os}}' "$base_reference")"
actual_architecture="$(docker image inspect --format '{{.Architecture}}' "$base_reference")"
if [[ "$actual_index_digest" != "$expected_index_digest" ]]; then
  echo "Spark base descriptor is $actual_index_digest, expected $expected_index_digest" >&2
  exit 1
fi
if [[ "$actual_operating_system" != "linux" || "$actual_architecture" != "arm64" ]]; then
  echo "Spark base platform is $actual_operating_system/$actual_architecture, expected linux/arm64" >&2
  exit 1
fi

# BuildKit resolves the local tag without a second registry lookup. The tag is
# created only after the immutable descriptor and selected platform above pass,
# and the resulting image records the audited platform-child digest
# independently.
docker tag "$base_reference" "$base_local_reference"
if [[ "$(docker image inspect --format '{{.Id}}' "$base_local_reference")" \
      != "$(docker image inspect --format '{{.Id}}' "$base_reference")" ]]; then
  echo "local Spark base indirection does not identify the verified image" >&2
  exit 1
fi

: "${COMPOSE_PROFILES:=lakekeeper,polaris,gravitino,spark}"
export COMPOSE_PROFILES
docker compose \
  --project-directory "$repository_root" \
  --file "$repository_root/docker-compose.yml" \
  build --provenance=false engine-runner-image iceberg-spark-runtime spark
