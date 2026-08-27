#!/usr/bin/env bash
# Materialize source-bound Flink, Iceberg, Java-child, and Rust-runner images.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_dir/.." && pwd -P)"
index_reference="flink:2.1.3-scala_2.12-java17@sha256:cc557bbe316d804e83195717a41788dc1ddb9a965887bd0ab83d148480a7802d"
child_reference="flink@sha256:99a499ed147b28d358486066ab8308e351b232b2ac81aff69157fdb349c84e18"
base_local_reference="catalog-bench/flink-base:2.1.3-arm64-99a499ed"
expected_child_digest="sha256:99a499ed147b28d358486066ab8308e351b232b2ac81aff69157fdb349c84e18"

for command in docker jq; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

actual_child_digest="$(
  docker buildx imagetools inspect "$index_reference" --raw \
    | jq -er '
        [
          .manifests[]
          | select(
              .platform.os == "linux"
              and .platform.architecture == "arm64"
            )
          | .digest
        ]
        | select(length == 1)
        | .[0]
      '
)"
if [[ "$actual_child_digest" != "$expected_child_digest" ]]; then
  echo "Flink index selects $actual_child_digest, expected $expected_child_digest" >&2
  exit 1
fi

if ! docker image inspect "$child_reference" >/dev/null 2>&1; then
  docker pull --platform linux/arm64 "$child_reference"
fi

actual_descriptor="$(
  docker image inspect --format '{{.Descriptor.digest}}' "$child_reference"
)"
actual_operating_system="$(docker image inspect --format '{{.Os}}' "$child_reference")"
actual_architecture="$(docker image inspect --format '{{.Architecture}}' "$child_reference")"
if [[ "$actual_descriptor" != "$expected_child_digest" ]]; then
  echo "Flink child descriptor is $actual_descriptor, expected $expected_child_digest" >&2
  exit 1
fi
if [[ "$actual_operating_system" != "linux" || "$actual_architecture" != "arm64" ]]; then
  echo "Flink child platform is $actual_operating_system/$actual_architecture, expected linux/arm64" >&2
  exit 1
fi

docker tag "$child_reference" "$base_local_reference"
if [[ "$(docker image inspect --format '{{.Id}}' "$base_local_reference")" \
      != "$(docker image inspect --format '{{.Id}}' "$child_reference")" ]]; then
  echo "local Flink base indirection does not identify the verified child" >&2
  exit 1
fi

COMPOSE_PROFILES=lakekeeper,polaris,gravitino,flink docker compose \
  --project-directory "$repository_root" \
  --file "$repository_root/docker-compose.yml" \
  build --provenance=false \
    flink-engine-runner-base \
    iceberg-flink-runtime \
    flink-runner-image \
    flink
