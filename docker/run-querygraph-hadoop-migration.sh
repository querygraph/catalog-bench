#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/docker/fresh-run-lib.sh"
RUN_ID="${1:-}"
QUERYGRAPH_ROOT="${QUERYGRAPH_ROOT:-$(cd "$ROOT_DIR/../querygraph" && pwd)}"
OUTPUT_DIR="${2:-$ROOT_DIR/target/querygraph-hadoop-migration/$RUN_ID}"
catalog_bench_validate_run_id "$RUN_ID"
if [[ -e "$OUTPUT_DIR" ]]; then
  echo "refusing existing output directory: $OUTPUT_DIR" >&2
  exit 1
fi
if ! git -C "$QUERYGRAPH_ROOT" diff --quiet -- \
    python/querygraph/hadoop_migration_live.py; then
  echo "refusing uncommitted QueryGraph Hadoop migration implementation" >&2
  exit 1
fi
QUERYGRAPH_REVISION="$(git -C "$QUERYGRAPH_ROOT" rev-parse HEAD)"
if docker volume inspect "${RUN_ID}_hadoop-data" >/dev/null 2>&1; then
  echo "refusing reused state volume: ${RUN_ID}_hadoop-data" >&2
  exit 1
fi

catalog_bench_prepare_fresh_project "$ROOT_DIR" "$RUN_ID"
mkdir -p "$OUTPUT_DIR"
compose() {
  COMPOSE_PROFILES=polaris,gravitino,lakekeeper,spark \
    CATALOG_BENCH_RUN_ID="$RUN_ID" docker compose \
      --project-directory "$ROOT_DIR" \
      --file "$ROOT_DIR/docker-compose.yml" \
      --file "$ROOT_DIR/docker-compose.clean.yml" \
      --file "$ROOT_DIR/docker-compose.hadoop.yml" "$@"
}
cleanup() {
  compose --profile '*' down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose up -d lakecat-ready
compose run --rm --no-deps \
  -v "$QUERYGRAPH_ROOT:/querygraph:ro" \
  -e PYTHONPATH=/querygraph/python \
  -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=password \
  -e AWS_REGION=us-east-1 \
  -e AWS_DEFAULT_REGION=us-east-1 \
  --entrypoint /opt/spark/bin/spark-submit spark \
  /querygraph/python/querygraph/hadoop_migration_live.py --fixture "$RUN_ID" \
  >"$OUTPUT_DIR/stdout.log"

python3 - "$OUTPUT_DIR" "$RUN_ID" "$QUERYGRAPH_REVISION" <<'PY'
import hashlib
import json
import pathlib
import sys

directory = pathlib.Path(sys.argv[1])
records = []
for line in (directory / "stdout.log").read_text().splitlines():
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        continue
    if isinstance(value, dict) and value.get("contract") == "querygraph/hadoop-to-rest-migration/v1":
        records.append(value)
if len(records) != 1:
    raise SystemExit(f"expected one migration record, found {len(records)}")
result = records[0]
passed = result["semantic"]["preserved"] and result["data"]["preserved"]
summary = {
    "contract": "catalog-bench/querygraph-hadoop-migration/v1",
    "run_id": sys.argv[2],
    "querygraph_revision": sys.argv[3],
    "status": "verified" if passed else "failed",
    "result": result,
}
encoded = json.dumps(summary, sort_keys=True, separators=(",", ":")).encode()
summary["content_sha256"] = "sha256:" + hashlib.sha256(encoded).hexdigest()
(directory / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
if not passed:
    raise SystemExit("Hadoop migration verification failed")
PY

rm "$OUTPUT_DIR/stdout.log"
cleanup
trap - EXIT
echo "verified QueryGraph Hadoop migration evidence: $OUTPUT_DIR/summary.json"
