#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/docker/fresh-run-lib.sh"

RUN_ID="${1:-}"
OUTPUT_DIR="${2:-$ROOT_DIR/target/catalog-recovery/$RUN_ID}"
catalog_bench_validate_run_id "$RUN_ID"
if [[ -e "$OUTPUT_DIR" ]]; then
  echo "refusing existing output directory: $OUTPUT_DIR" >&2
  exit 1
fi

catalog_bench_prepare_fresh_project "$ROOT_DIR" "$RUN_ID"
mkdir -p "$OUTPUT_DIR"
cleanup() {
  CATALOG_BENCH_RUN_ID="$RUN_ID" \
    catalog_bench_fault_compose "$ROOT_DIR" --profile '*' down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

CATALOG_BENCH_RUN_ID="$RUN_ID" \
  catalog_bench_fault_compose "$ROOT_DIR" \
    --profile polaris --profile gravitino --profile lakekeeper \
    up -d lakecat-fault-proxy polaris-fault-proxy gravitino-fault-proxy lakekeeper-fault-proxy

for port in 19101 19102 19103 19104; do
  ready=false
  for _ in $(seq 1 300); do
    if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    echo "fault proxy control port did not become ready: $port" >&2
    exit 1
  fi
done

python3 "$ROOT_DIR/clients/faults/catalog_recovery.py" \
  --catalog lakecat --fixture-id "$RUN_ID" \
  --proxy-base http://127.0.0.1:19201/catalog \
  --direct-base http://127.0.0.1:8181/catalog \
  --control-base http://127.0.0.1:19101 \
  --repository-root "$ROOT_DIR" --run-id "$RUN_ID" --restart-service lakecat \
  --warehouse local --location s3://warehouse/lakecat >"$OUTPUT_DIR/lakecat.json"

CATALOG_BENCH_POLARIS_CLIENT_ID=root \
CATALOG_BENCH_POLARIS_CLIENT_SECRET=secret \
python3 "$ROOT_DIR/clients/faults/catalog_recovery.py" \
  --catalog polaris --fixture-id "$RUN_ID" \
  --proxy-base http://127.0.0.1:19202/api/catalog \
  --direct-base http://127.0.0.1:8185/api/catalog \
  --control-base http://127.0.0.1:19102 \
  --repository-root "$ROOT_DIR" --run-id "$RUN_ID" --restart-service polaris \
  --warehouse bench --static-prefix bench --oauth >"$OUTPUT_DIR/polaris.json"

python3 "$ROOT_DIR/clients/faults/catalog_recovery.py" \
  --catalog gravitino --fixture-id "$RUN_ID" \
  --proxy-base http://127.0.0.1:19203/iceberg \
  --direct-base http://127.0.0.1:9002/iceberg \
  --control-base http://127.0.0.1:19103 \
  --repository-root "$ROOT_DIR" --run-id "$RUN_ID" --restart-service gravitino \
  >"$OUTPUT_DIR/gravitino.json"

python3 "$ROOT_DIR/clients/faults/catalog_recovery.py" \
  --catalog lakekeeper --fixture-id "$RUN_ID" \
  --proxy-base http://127.0.0.1:19204/catalog \
  --direct-base http://127.0.0.1:8186/catalog \
  --control-base http://127.0.0.1:19104 \
  --repository-root "$ROOT_DIR" --run-id "$RUN_ID" --restart-service lakekeeper \
  --warehouse bench >"$OUTPUT_DIR/lakekeeper.json"

node - "$OUTPUT_DIR" "$RUN_ID" <<'NODE' >"$OUTPUT_DIR/summary.json"
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const [directory, runId] = process.argv.slice(2);
const catalogs = ["lakecat", "polaris", "gravitino", "lakekeeper"];
const hash = name => "sha256:" + crypto.createHash("sha256").update(fs.readFileSync(path.join(directory, name))).digest("hex");
const results = {};
for (const catalog of catalogs) {
  const value = JSON.parse(fs.readFileSync(path.join(directory, `${catalog}.json`), "utf8"));
  if (value.schema_version !== "catalog-bench.catalog-recovery-probe.v2" || value.catalog !== catalog) throw new Error(`${catalog}: identity mismatch`);
  const before = value.cases.before_upstream;
  const after = value.cases.after_upstream;
  if (!before.client_disconnected || before.observed_before_retry !== null || before.retry_status !== 200 || before.final_property !== "accepted") throw new Error(`${catalog}: before-upstream recovery failed`);
  if (!after.client_disconnected || after.observed_before_retry !== "accepted" || ![200, 409].includes(after.retry_status) || after.final_property !== "accepted") throw new Error(`${catalog}: after-upstream recovery failed`);
  if (before.fault_events.length !== 1 || before.fault_events[0].upstream_status != null) throw new Error(`${catalog}: before fault evidence mismatch`);
  if (after.fault_events.length !== 1 || after.fault_events[0].upstream_status !== 200) throw new Error(`${catalog}: after fault evidence mismatch`);
  const restart = value.cases.restart_during_commit;
  if (restart.observed_before_retry !== null || restart.retry_status !== 200 || restart.final_property !== "accepted") throw new Error(`${catalog}: restart recovery failed`);
  if (restart.fault_events.length !== 1 || restart.fault_events[0].phase !== "during-upstream") throw new Error(`${catalog}: restart fault evidence mismatch`);
  if (!value.cleanup.table_dropped || !value.cleanup.namespace_dropped) throw new Error(`${catalog}: cleanup failed`);
  results[catalog] = {
    artifact_sha256: hash(`${catalog}.json`),
    before_retry_status: before.retry_status,
    after_retry_status: after.retry_status,
    idempotency_advertised: after.idempotency_advertised,
    idempotency_drift_status: after.drift_status,
    idempotency_drift_mutated: after.drift_mutated,
    restart_request_outcome: restart.request_outcome,
    restart_retry_status: restart.retry_status
  };
}
process.stdout.write(JSON.stringify({
  schema_version: "catalog-bench.catalog-recovery-run.v2",
  run_id: runId,
  status: "verified",
  scenario: "iceberg-rest.commit.failure-recovery",
  results
}, null, 2) + "\n");
NODE

cleanup
trap - EXIT
echo "verified catalog recovery evidence: $OUTPUT_DIR/summary.json"
