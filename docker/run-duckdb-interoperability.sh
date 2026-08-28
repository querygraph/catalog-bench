#!/usr/bin/env bash
# Build and execute four stock-DuckDB workflows in one fresh Docker topology.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_dir/.." && pwd -P)"
# shellcheck source=docker/fresh-run-lib.sh
source "$script_dir/fresh-run-lib.sh"

if [[ $# -ne 1 ]]; then
  echo "usage: docker/run-duckdb-interoperability.sh <run-id>" >&2
  exit 1
fi
run_id="$1"
catalog_bench_validate_run_id "$run_id"

evidence_dir="${CATALOG_BENCH_DUCKDB_EVIDENCE_DIR:-$repository_root/target/duckdb-evidence}"
runnable_profile="$repository_root/profiles/v1/duckdb-1.5.3-lakecat-b8be6bc9-2026-08-28.json"
run_directory="$evidence_dir/$run_id"
catalogs=(lakecat polaris gravitino lakekeeper)

mkdir -p "$evidence_dir"
evidence_dir="$(cd "$evidence_dir" && pwd -P)"
run_directory="$evidence_dir/$run_id"
if [[ -e "$run_directory" ]]; then
  echo "refusing reused DuckDB evidence directory: $run_directory" >&2
  exit 1
fi

export CATALOG_BENCH_RUN_ID="$run_id"
export CATALOG_BENCH_DUCKDB_EVIDENCE_DIR="$evidence_dir"
export COMPOSE_PROFILES="lakekeeper,polaris,gravitino,trino,duckdb"
unset COMPOSE_PROJECT_NAME
catalog_bench_prepare_fresh_project "$repository_root" "$run_id"
catalog_bench_base_compose "$repository_root" build --provenance=false minio trino-lakecat
catalog_bench_base_compose "$repository_root" build --provenance=false duckdb-benchmark-base
cargo run --manifest-path "$repository_root/Cargo.toml" -p catalog-bench-contract --locked -- \
  validate "$runnable_profile"

behavioral_failure=0
fixture_collision=0
execution_failure=0
for catalog in "${catalogs[@]}"; do
  container_output="/evidence/$run_id/$catalog.json"
  host_output="$run_directory/$catalog.json"
  set +e
  catalog_bench_clean_compose "$repository_root" run --rm duckdb-engine \
    --profile /contracts/profiles/v1/duckdb-1.5.3-lakecat-b8be6bc9-2026-08-28.json \
    --scenario /contracts/scenarios/v1/engine.iceberg.write-read-evolution.v2.json \
    --catalog "$catalog" --fixture-id "$run_id" --output "$container_output"
  status=$?
  set -e
  case "$status" in 0) expected=pass ;; 2) expected=fail ;; 3) expected=fixture-collision ;; *) expected="" ;; esac
  if [[ -z "$expected" || ! -f "$host_output" ]]; then
    echo "$catalog failed before publishing accepted evidence (exit $status)" >&2
    execution_failure=1
    continue
  fi
  actual="$(jq -er '.execution.classification' "$host_output")" || { execution_failure=1; continue; }
  if [[ "$actual" != "$expected" ]]; then
    echo "$catalog exit/transcript mismatch: exit $status requires $expected, got $actual" >&2
    execution_failure=1
    continue
  fi
  echo "$catalog: $actual ($host_output)"
  case "$status" in 2) behavioral_failure=1 ;; 3) fixture_collision=1 ;; esac
done

if [[ "$execution_failure" -ne 0 ]]; then exit 1; fi
if [[ "$fixture_collision" -ne 0 ]]; then exit 3; fi
if [[ "$behavioral_failure" -ne 0 ]]; then exit 2; fi
echo "all four stock-DuckDB workflows passed: $run_directory"
