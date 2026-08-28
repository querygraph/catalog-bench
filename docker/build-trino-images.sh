#!/usr/bin/env bash
# Materialize the exact stock Trino child plus the source-bound Rust runner.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_dir/.." && pwd -P)"
base_reference="trinodb/trino@sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534"
expected_digest="sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534"

if ! command -v docker >/dev/null 2>&1; then
  echo "required command is unavailable: docker" >&2
  exit 1
fi
if ! docker image inspect "$base_reference" >/dev/null 2>&1; then
  docker pull --platform linux/arm64 "$base_reference"
fi
actual_digest="$(docker image inspect --format '{{.Descriptor.digest}}' "$base_reference")"
actual_os="$(docker image inspect --format '{{.Os}}' "$base_reference")"
actual_architecture="$(docker image inspect --format '{{.Architecture}}' "$base_reference")"
if [[ "$actual_digest" != "$expected_digest" ]]; then
  echo "Trino descriptor is $actual_digest, expected $expected_digest" >&2
  exit 1
fi
if [[ "$actual_os" != "linux" || "$actual_architecture" != "arm64" ]]; then
  echo "Trino platform is $actual_os/$actual_architecture, expected linux/arm64" >&2
  exit 1
fi

COMPOSE_PROFILES=lakekeeper,polaris,gravitino,trino docker compose \
  --project-directory "$repository_root" \
  --file "$repository_root/docker-compose.yml" \
  build --provenance=false trino-engine-runner-base trino
