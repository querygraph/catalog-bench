#!/usr/bin/env bash
# Build and execute one fresh-state, same-Docker contention sweep.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: docker/run-contention.sh <run-id>" >&2
  echo "run-id: 1-24 lowercase ASCII letters, digits, or underscores" >&2
  exit 1
fi

run_id="$1"
if [[ ! "$run_id" =~ ^[a-z0-9][a-z0-9_]{0,23}$ ]]; then
  echo "invalid run ID: $run_id" >&2
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_dir/.." && pwd -P)"
evidence_dir="${CATALOG_BENCH_COMMIT_EVIDENCE_DIR:-$repository_root/target/commit-evidence}"
mkdir -p "$evidence_dir"
evidence_dir="$(cd "$evidence_dir" && pwd -P)"
evidence_path="$evidence_dir/$run_id.json"

if [[ -e "$evidence_path" ]]; then
  echo "refusing to overwrite existing evidence: $evidence_path" >&2
  exit 1
fi

export CATALOG_BENCH_RUN_ID="$run_id"
export CATALOG_BENCH_COMMIT_EVIDENCE_DIR="$evidence_dir"
export COMPOSE_PROFILES="lakekeeper,nessie,polaris,gravitino,bench"
unset COMPOSE_PROJECT_NAME

base_compose=(
  docker compose
  --project-directory "$repository_root"
  --file "$repository_root/docker-compose.yml"
)
clean_compose=(
  "${base_compose[@]}"
  --file "$repository_root/docker-compose.clean.yml"
)

"${clean_compose[@]}" config --quiet

volume_suffixes=(
  gravitino-data
  lakecat-data
  lakekeeper-postgres-data
  minio-data
)
for suffix in "${volume_suffixes[@]}"; do
  volume_name="${run_id}_${suffix}"
  if docker volume inspect "$volume_name" >/dev/null 2>&1; then
    echo "refusing reused state volume: $volume_name" >&2
    exit 1
  fi
done

if [[ -n "$("${clean_compose[@]}" ps --all --quiet)" ]]; then
  echo "refusing reused Compose project: $run_id" >&2
  exit 1
fi

# Release the fixed benchmark network and host diagnostic ports while preserving
# every prior project's named volumes. Refuse an unknown project or an unmanaged
# container instead of guessing that it belongs to this harness.
network_filter="network=catalog-bench-net"
unmanaged_containers="$(
  docker ps --all --filter "$network_filter" \
    --format '{{.ID}} {{.Label "com.docker.compose.project"}}' \
    | awk 'NF == 1 { print $1 }'
)"
if [[ -n "$unmanaged_containers" ]]; then
  echo "refusing to detach unmanaged containers from catalog-bench-net:" >&2
  echo "$unmanaged_containers" >&2
  exit 1
fi

active_projects="$(
  docker ps --all --filter "$network_filter" \
    --format '{{.Label "com.docker.compose.project"}}' \
    | sed '/^$/d' \
    | sort -u
)"
while IFS= read -r project; do
  [[ -z "$project" ]] && continue
  if [[ "$project" != "catalog-bench" && ! "$project" =~ ^[a-z0-9][a-z0-9_]{0,23}$ ]]; then
    echo "refusing unknown Compose project on catalog-bench-net: $project" >&2
    exit 1
  fi
done <<< "$active_projects"

while IFS= read -r project; do
  [[ -z "$project" ]] && continue
  if [[ "$project" == "catalog-bench" ]]; then
    "${base_compose[@]}" down --remove-orphans
  else
    CATALOG_BENCH_RUN_ID="$project" \
      "${base_compose[@]}" --file "$repository_root/docker-compose.clean.yml" \
      down --remove-orphans
  fi
done <<< "$active_projects"

# Also remove stopped ordinary-project containers that no longer appear in a
# network filter. This command still omits --volumes.
"${base_compose[@]}" down --remove-orphans

# A container can carry a recognized Compose project label without belonging to
# that project's current model. `compose down --remove-orphans` normally catches
# it, but the fixed benchmark network is an evidence boundary: verify the
# boundary directly before either build can consume host resources or any new
# service can start.
remaining_containers="$(
  docker ps --all --filter "$network_filter" \
    --format '{{.ID}} {{.Names}} {{.Label "com.docker.compose.project"}}'
)"
if [[ -n "$remaining_containers" ]]; then
  echo "refusing to build with containers still attached to catalog-bench-net:" >&2
  echo "$remaining_containers" >&2
  exit 1
fi

# Build under the stable ordinary project rather than the evidence run ID.
# Compose writes its project/service labels into exported image configs; using
# the run-scoped project here would make identical production bytes acquire a
# different local-image digest on every execution.
"${base_compose[@]}" build --provenance=false lakecat bench

set +e
"${clean_compose[@]}" run --rm bench \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json \
  --fixture-id "$run_id" \
  --output "/evidence/$run_id.json"
status=$?
set -e

case "$status" in
  0)
    echo "all catalog rounds passed: $evidence_path"
    ;;
  2)
    echo "complete transcript contains unranked catalogs: $evidence_path" >&2
    ;;
  *)
    echo "contention sweep failed before producing accepted evidence (exit $status)" >&2
    ;;
esac

exit "$status"
