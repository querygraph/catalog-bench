#!/usr/bin/env bash
# Build and execute one fresh-state, same-Docker contention sweep.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_dir/.." && pwd -P)"
# shellcheck source=docker/fresh-run-lib.sh
source "$script_dir/fresh-run-lib.sh"

if [[ $# -ne 1 ]]; then
  echo "usage: docker/run-contention.sh <run-id>" >&2
  echo "run-id: 1-24 lowercase ASCII letters, digits, or underscores" >&2
  exit 1
fi

run_id="$1"
catalog_bench_validate_run_id "$run_id"

evidence_dir="${CATALOG_BENCH_COMMIT_EVIDENCE_DIR:-$repository_root/target/commit-evidence}"
source_profile="$repository_root/profiles/v1/current-2026-08-26.json"
materialization="$repository_root/materializations/v1/contention-2026-08-27.json"
runnable_profile="$repository_root/profiles/v1/contention-2026-08-27.json"
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

catalog_bench_prepare_fresh_project "$repository_root" "$run_id"

# Build under the stable ordinary project rather than the evidence run ID.
# Compose writes its project/service labels into exported image configs; using
# the run-scoped project here would make identical production bytes acquire a
# different local-image digest on every execution.
catalog_bench_base_compose "$repository_root" \
  build --provenance=false minio lakecat bench
"$repository_root/docker/verify-contention-artifacts.sh" \
  "$source_profile" \
  "$materialization" \
  "$runnable_profile"

set +e
catalog_bench_clean_compose "$repository_root" run --rm bench \
  --profile /contracts/profiles/v1/contention-2026-08-27.json \
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
