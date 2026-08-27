#!/usr/bin/env bash
# Build and execute four stock-Spark workflows in one fresh Docker topology.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_dir/.." && pwd -P)"
# shellcheck source=docker/fresh-run-lib.sh
source "$script_dir/fresh-run-lib.sh"

if [[ $# -ne 1 ]]; then
  echo "usage: docker/run-spark-interoperability.sh <run-id>" >&2
  echo "run-id: 1-24 lowercase ASCII letters, digits, or underscores" >&2
  exit 1
fi

run_id="$1"
catalog_bench_validate_run_id "$run_id"

evidence_dir="${CATALOG_BENCH_SPARK_EVIDENCE_DIR:-$repository_root/target/spark-evidence}"
source_profile="$repository_root/profiles/v1/current-2026-08-27.json"
materialization="$repository_root/materializations/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json"
runnable_profile="$repository_root/profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json"
run_directory="$evidence_dir/$run_id"
catalogs=(lakecat polaris gravitino lakekeeper)

mkdir -p "$evidence_dir"
evidence_dir="$(cd "$evidence_dir" && pwd -P)"
run_directory="$evidence_dir/$run_id"
if [[ -e "$run_directory" ]]; then
  echo "refusing reused Spark evidence directory: $run_directory" >&2
  exit 1
fi

export CATALOG_BENCH_RUN_ID="$run_id"
export CATALOG_BENCH_SPARK_EVIDENCE_DIR="$evidence_dir"
export COMPOSE_PROFILES="lakekeeper,polaris,gravitino,spark"
unset COMPOSE_PROJECT_NAME

catalog_bench_prepare_fresh_project "$repository_root" "$run_id"

# Local-image identities are evidence. Build under the stable ordinary project
# so Compose labels do not vary with the run ID, then independently admit every
# selected image and embedded artifact before starting the run-owned project.
catalog_bench_base_compose "$repository_root" \
  build --provenance=false minio lakecat
"$script_dir/build-spark-images.sh"
"$script_dir/verify-profile-artifacts.sh" \
  "$source_profile" \
  "$materialization" \
  "$runnable_profile"

behavioral_failure=0
fixture_collision=0
execution_failure=0

for catalog in "${catalogs[@]}"; do
  container_output="/evidence/$run_id/$catalog.json"
  host_output="$evidence_dir/$run_id/$catalog.json"

  set +e
  catalog_bench_clean_compose "$repository_root" run --rm spark-engine \
    --profile /contracts/profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json \
    --scenario /contracts/scenarios/v1/engine.iceberg.write-read-evolution.json \
    --catalog "$catalog" \
    --fixture-id "$run_id" \
    --output "$container_output"
  status=$?
  set -e

  case "$status" in
    0) expected_classification="pass" ;;
    2) expected_classification="fail" ;;
    3) expected_classification="fixture-collision" ;;
    *) expected_classification="" ;;
  esac

  if [[ -n "$expected_classification" ]]; then
    if [[ ! -f "$host_output" ]]; then
      echo "$catalog returned $status without publishing $host_output" >&2
      execution_failure=1
      continue
    fi
    if ! actual_classification="$(jq -er '.execution.classification' "$host_output")"; then
      echo "$catalog published an unreadable transcript: $host_output" >&2
      execution_failure=1
      continue
    fi
    if [[ "$actual_classification" != "$expected_classification" ]]; then
      echo "$catalog exit/transcript mismatch: exit $status requires $expected_classification, got $actual_classification" >&2
      execution_failure=1
      continue
    fi
    echo "$catalog: $actual_classification ($host_output)"
  else
    echo "$catalog execution failed before an accepted transcript (exit $status)" >&2
    execution_failure=1
    continue
  fi

  case "$status" in
    2) behavioral_failure=1 ;;
    3) fixture_collision=1 ;;
  esac
done

if [[ "$execution_failure" -ne 0 ]]; then
  echo "Spark interoperability run is incomplete: $run_directory" >&2
  exit 1
fi
if [[ "$fixture_collision" -ne 0 ]]; then
  echo "complete transcripts include a fixture collision: $run_directory" >&2
  exit 3
fi
if [[ "$behavioral_failure" -ne 0 ]]; then
  echo "complete transcripts include behavioral failures: $run_directory" >&2
  exit 2
fi

echo "all four stock-Spark workflows passed: $run_directory"
