#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/docker/fresh-run-lib.sh"
RUN_ID="${1:-}"
OUTPUT_DIR="${2:-$ROOT_DIR/target/catalog-backup/$RUN_ID}"
catalog_bench_validate_run_id "$RUN_ID"
if [[ -e "$OUTPUT_DIR" ]]; then
  echo "refusing existing output directory: $OUTPUT_DIR" >&2
  exit 1
fi
catalog_bench_prepare_fresh_project "$ROOT_DIR" "$RUN_ID"
mkdir -p "$OUTPUT_DIR/archives"
BACKUP_DIR="$OUTPUT_DIR/archives"

compose() {
  CATALOG_BENCH_RUN_ID="$RUN_ID" CATALOG_BENCH_BACKUP_DIR="$BACKUP_DIR" \
    docker compose --project-directory "$ROOT_DIR" \
      --file "$ROOT_DIR/docker-compose.yml" \
      --file "$ROOT_DIR/docker-compose.clean.yml" \
      --file "$ROOT_DIR/docker-compose.backup.yml" "$@"
}
cleanup() {
  compose --profile '*' down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose --profile polaris --profile gravitino --profile lakekeeper --profile backup \
  up -d lakecat-ready polaris-ready gravitino-ready lakekeeper-ready

fixture() {
  local operation="$1" catalog="$2" base="$3"
  shift 3
  python3 "$ROOT_DIR/clients/faults/catalog_fixture.py" "$operation" \
    --catalog "$catalog" --fixture-id "$RUN_ID" --base "$base" "$@"
}

fixture create lakecat http://127.0.0.1:8181/catalog \
  --warehouse local --location s3://warehouse/lakecat >"$OUTPUT_DIR/lakecat-before.json"
CATALOG_BENCH_POLARIS_CLIENT_ID=root CATALOG_BENCH_POLARIS_CLIENT_SECRET=secret \
  fixture create polaris http://127.0.0.1:8185/api/catalog \
    --warehouse bench --static-prefix bench --oauth >"$OUTPUT_DIR/polaris-before.json"
fixture create gravitino http://127.0.0.1:9002/iceberg >"$OUTPUT_DIR/gravitino-before.json"
fixture create lakekeeper http://127.0.0.1:8186/catalog \
  --warehouse bench >"$OUTPUT_DIR/lakekeeper-before.json"

compose stop lakecat polaris gravitino lakekeeper postgresql
compose run --rm lakecat-volume-archive backup /state /backup/lakecat.tar.gz
compose run --rm gravitino-volume-archive backup /state /backup/gravitino.tar.gz
compose run --rm lakekeeper-volume-archive backup /state /backup/lakekeeper-postgres.tar.gz

compose rm -f lakecat polaris gravitino gravitino-state-init lakekeeper postgresql
for suffix in lakecat-data gravitino-data lakekeeper-postgres-data; do
  docker volume rm "${RUN_ID}_${suffix}" >/dev/null
  docker volume create "${RUN_ID}_${suffix}" >/dev/null
done

compose run --rm lakecat-volume-archive restore /state /backup/lakecat.tar.gz
compose run --rm gravitino-volume-archive restore /state /backup/gravitino.tar.gz
compose run --rm lakekeeper-volume-archive restore /state /backup/lakekeeper-postgres.tar.gz
compose --profile polaris --profile gravitino --profile lakekeeper --profile backup \
  up -d lakecat polaris gravitino postgresql lakekeeper

ready=false
for _ in $(seq 1 180); do
  if curl -fsS http://127.0.0.1:8181/catalog/v1/config >/dev/null 2>&1 \
      && curl -fsS http://127.0.0.1:9002/iceberg/v1/config >/dev/null 2>&1 \
      && curl -fsS 'http://127.0.0.1:8186/catalog/v1/config?warehouse=bench' >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "restored catalogs did not become ready" >&2
  exit 1
fi
compose rm -f polaris-bootstrap polaris-ready >/dev/null 2>&1 || true
compose --profile polaris up -d polaris-ready

fixture verify lakecat http://127.0.0.1:8181/catalog \
  --warehouse local >"$OUTPUT_DIR/lakecat-after.json"
CATALOG_BENCH_POLARIS_CLIENT_ID=root CATALOG_BENCH_POLARIS_CLIENT_SECRET=secret \
  fixture verify polaris http://127.0.0.1:8185/api/catalog \
    --warehouse bench --static-prefix bench --oauth >"$OUTPUT_DIR/polaris-after.json"
fixture verify gravitino http://127.0.0.1:9002/iceberg >"$OUTPUT_DIR/gravitino-after.json"
fixture verify lakekeeper http://127.0.0.1:8186/catalog \
  --warehouse bench >"$OUTPUT_DIR/lakekeeper-after.json"

node - "$OUTPUT_DIR" "$RUN_ID" <<'NODE' >"$OUTPUT_DIR/summary.json"
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const [directory, runId] = process.argv.slice(2);
const catalogs = ["lakecat", "polaris", "gravitino", "lakekeeper"];
const results = {};
for (const catalog of catalogs) {
  const before = JSON.parse(fs.readFileSync(path.join(directory, `${catalog}-before.json`)));
  const after = JSON.parse(fs.readFileSync(path.join(directory, `${catalog}-after.json`)));
  const passed = after.restored === true && before.table_uuid === after.table_uuid && before.metadata_location === after.metadata_location;
  results[catalog] = {restored: after.restored, identity_preserved: passed, status: after.status};
}
const archives = {};
for (const name of ["lakecat.tar.gz", "gravitino.tar.gz", "lakekeeper-postgres.tar.gz"]) {
  const data = fs.readFileSync(path.join(directory, "archives", name));
  archives[name] = {bytes: data.length, sha256: "sha256:" + crypto.createHash("sha256").update(data).digest("hex")};
}
const failures = Object.values(results).filter(value => !value.identity_preserved).length;
process.stdout.write(JSON.stringify({
  schema_version: "catalog-bench.catalog-backup-restore.v1",
  run_id: runId,
  status: failures === 0 ? "verified" : "verified_with_failures",
  failures,
  results,
  archives
}, null, 2) + "\n");
NODE

fixture drop lakecat http://127.0.0.1:8181/catalog --warehouse local >/dev/null
fixture drop gravitino http://127.0.0.1:9002/iceberg >/dev/null
fixture drop lakekeeper http://127.0.0.1:8186/catalog --warehouse bench >/dev/null
cleanup
trap - EXIT
echo "verified catalog backup/restore evidence: $OUTPUT_DIR/summary.json"
