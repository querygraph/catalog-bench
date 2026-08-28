#!/usr/bin/env bash
# Build and verify the pinned source DuckDB runtime and signed extensions.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_dir/.." && pwd -P)"
image="catalog-bench/duckdb:1.5.3-arm64"

COMPOSE_PROFILES=duckdb docker compose \
  --project-directory "$repository_root" \
  --file "$repository_root/docker-compose.yml" \
  build --provenance=false duckdb-runtime-base

actual_os="$(docker image inspect --format '{{.Os}}' "$image")"
actual_architecture="$(docker image inspect --format '{{.Architecture}}' "$image")"
if [[ "$actual_os" != "linux" || "$actual_architecture" != "arm64" ]]; then
  echo "DuckDB platform is $actual_os/$actual_architecture, expected linux/arm64" >&2
  exit 1
fi

version="$(docker run --rm "$image" -csv -noheader -c 'SELECT version();')"
if [[ "$version" != "v1.5.3" ]]; then
  echo "DuckDB reports $version, expected v1.5.3" >&2
  exit 1
fi

loaded="$(docker run --rm "$image" -csv -noheader -c \
  "LOAD httpfs; LOAD iceberg; SELECT string_agg(extension_name, '|' ORDER BY extension_name) FROM duckdb_extensions() WHERE loaded AND extension_name IN ('httpfs', 'iceberg');")"
if [[ "$loaded" != "httpfs|iceberg" ]]; then
  echo "DuckDB loaded $loaded, expected httpfs|iceberg" >&2
  exit 1
fi
