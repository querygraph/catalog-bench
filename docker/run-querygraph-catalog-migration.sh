#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/docker/fresh-run-lib.sh"
RUN_ID="${1:-}"
QUERYGRAPH_ROOT="${QUERYGRAPH_ROOT:-$(cd "$ROOT_DIR/../querygraph" && pwd)}"
OUTPUT_DIR="${2:-$ROOT_DIR/target/querygraph-catalog-migration/$RUN_ID}"
catalog_bench_validate_run_id "$RUN_ID"
if [[ -e "$OUTPUT_DIR" ]]; then
  echo "refusing existing output directory: $OUTPUT_DIR" >&2
  exit 1
fi
if ! git -C "$QUERYGRAPH_ROOT" diff --quiet -- \
    python/querygraph/catalog_migration.py \
    python/querygraph/catalog_migration_live.py; then
  echo "refusing uncommitted QueryGraph migration implementation" >&2
  exit 1
fi
QUERYGRAPH_REVISION="$(git -C "$QUERYGRAPH_ROOT" rev-parse HEAD)"

catalog_bench_prepare_fresh_project "$ROOT_DIR" "$RUN_ID"
mkdir -p "$OUTPUT_DIR"

compose() {
  CATALOG_BENCH_RUN_ID="$RUN_ID" catalog_bench_clean_compose "$ROOT_DIR" "$@"
}
cleanup() {
  compose --profile '*' down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose --profile polaris --profile lakekeeper --profile pyiceberg \
  up -d lakecat-ready polaris-ready lakekeeper-ready

endpoint() {
  case "$1" in
    lakecat)
      printf '%s\n' 'http://lakecat:8181/catalog|local|||'
      ;;
    polaris)
      printf '%s\n' 'http://polaris:8181/api/catalog|bench|POLARIS_CREDENTIAL|http://polaris:8181/api/catalog/v1/oauth/tokens|PRINCIPAL_ROLE:ALL'
      ;;
    lakekeeper)
      printf '%s\n' 'http://lakekeeper:8181/catalog|bench|||'
      ;;
    *)
      echo "unknown catalog: $1" >&2
      return 1
      ;;
  esac
}

run_direction() {
  local source="$1" destination="$2" fixture="$3"
  local source_uri source_warehouse source_credential source_oauth source_scope
  local destination_uri destination_warehouse destination_credential
  local destination_oauth destination_scope
  IFS='|' read -r source_uri source_warehouse source_credential source_oauth source_scope \
    <<<"$(endpoint "$source")"
  IFS='|' read -r destination_uri destination_warehouse destination_credential destination_oauth destination_scope \
    <<<"$(endpoint "$destination")"
  local table_location=""
  if [[ "$source" == "lakecat" && "$destination" == "polaris" ]]; then
    table_location="s3://warehouse/bench/qg_migration_$fixture/events"
  elif [[ "$source" == "lakecat" && "$destination" == "lakekeeper" ]]; then
    table_location="s3://warehouse/lakekeeper/qg_migration_$fixture/events"
  fi
  compose run --rm --no-deps \
    -v "$QUERYGRAPH_ROOT:/querygraph:ro" \
    -e PYTHONPATH=/querygraph/python \
    -e CATALOG_BENCH_S3_ENDPOINT=http://minio:9000 \
    -e CATALOG_BENCH_S3_REGION=us-east-1 \
    -e POLARIS_CREDENTIAL=root:secret \
    -e SOURCE_TABLE_LOCATION="$table_location" \
    -e SOURCE_NAME="$source" \
    -e SOURCE_URI="$source_uri" \
    -e SOURCE_WAREHOUSE="$source_warehouse" \
    -e SOURCE_CREDENTIAL_ENV="$source_credential" \
    -e SOURCE_OAUTH_URI="$source_oauth" \
    -e SOURCE_SCOPE="$source_scope" \
    -e DESTINATION_NAME="$destination" \
    -e DESTINATION_URI="$destination_uri" \
    -e DESTINATION_WAREHOUSE="$destination_warehouse" \
    -e DESTINATION_CREDENTIAL_ENV="$destination_credential" \
    -e DESTINATION_OAUTH_URI="$destination_oauth" \
    -e DESTINATION_SCOPE="$destination_scope" \
    --entrypoint python pyiceberg \
    -m querygraph.catalog_migration_live --fixture "$fixture" \
    >"$OUTPUT_DIR/$source-to-$destination.json"
}

run_direction lakecat polaris "${RUN_ID}_lp"
run_direction polaris lakecat "${RUN_ID}_pl"
run_direction lakecat lakekeeper "${RUN_ID}_lk"
run_direction lakekeeper lakecat "${RUN_ID}_kl"

python3 - "$OUTPUT_DIR" "$RUN_ID" "$QUERYGRAPH_REVISION" <<'PY'
import hashlib
import json
import pathlib
import sys

directory = pathlib.Path(sys.argv[1])
directions = ["lakecat-to-polaris", "polaris-to-lakecat", "lakecat-to-lakekeeper", "lakekeeper-to-lakecat"]
results = [json.loads((directory / f"{name}.json").read_text()) for name in directions]
passed = all(item["semantic"]["preserved"] and item["semantic"]["proves_nonempty_history"] and item["data"]["preserved"] for item in results)
summary = {
    "contract": "catalog-bench/querygraph-catalog-migration/v1",
    "run_id": sys.argv[2],
    "querygraph_revision": sys.argv[3],
    "status": "verified" if passed else "failed",
    "directions": results,
}
encoded = json.dumps(summary, sort_keys=True, separators=(",", ":")).encode()
summary["content_sha256"] = "sha256:" + hashlib.sha256(encoded).hexdigest()
(directory / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
if not passed:
    raise SystemExit("migration verification failed")
PY

cleanup
trap - EXIT
echo "verified QueryGraph catalog migration evidence: $OUTPUT_DIR/summary.json"
