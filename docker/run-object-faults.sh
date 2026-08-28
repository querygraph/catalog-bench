#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/docker/fresh-run-lib.sh"

RUN_ID="${1:-}"
OUTPUT_DIR="${2:-$ROOT_DIR/target/object-fault-evidence/$RUN_ID}"

catalog_bench_validate_run_id "$RUN_ID"
if [[ -e "$OUTPUT_DIR" ]]; then
  echo "refusing existing output directory: $OUTPUT_DIR" >&2
  exit 1
fi

catalog_bench_prepare_fresh_project "$ROOT_DIR" "$RUN_ID"
mkdir -p "$OUTPUT_DIR"

cleanup() {
  CATALOG_BENCH_RUN_ID="$RUN_ID" \
    catalog_bench_fault_compose "$ROOT_DIR" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

CATALOG_BENCH_RUN_ID="$RUN_ID" \
  catalog_bench_fault_compose "$ROOT_DIR" up -d minio minio-init object-store-fault-proxy

init_container="${RUN_ID}-minio-init-1"
init_state=""
for _ in $(seq 1 60); do
  init_state="$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$init_container" 2>/dev/null || true)"
  [[ "$init_state" == "exited 0" ]] && break
  sleep 1
done
if [[ "$init_state" != "exited 0" ]]; then
  echo "MinIO initialization did not complete successfully: $init_state" >&2
  exit 1
fi

for phase in before-upstream after-upstream; do
  CATALOG_BENCH_RUN_ID="$RUN_ID" \
    catalog_bench_fault_compose "$ROOT_DIR" run --rm --no-deps \
      --entrypoint /usr/local/bin/object-fault-probe \
      object-store-fault-proxy \
      --phase "$phase" \
      --proxy-endpoint http://object-store-fault-proxy:8080 \
      --direct-endpoint http://minio:9000 \
      --control-url http://object-store-fault-proxy:8081 \
      --bucket warehouse \
      --object "fault-$RUN_ID-$phase/metadata/00001.json" \
      --access-key admin \
      --secret-key password \
      --region us-east-1 >"$OUTPUT_DIR/$phase.json"
done

node - "$OUTPUT_DIR" "$RUN_ID" <<'NODE' >"$OUTPUT_DIR/summary.json"
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const [directory, runId] = process.argv.slice(2);
const read = name => JSON.parse(fs.readFileSync(path.join(directory, name), "utf8"));
const hash = name => "sha256:" + crypto.createHash("sha256").update(fs.readFileSync(path.join(directory, name))).digest("hex");
const before = read("before-upstream.json");
const after = read("after-upstream.json");
function requireCondition(condition, message) {
  if (!condition) throw new Error(message);
}
requireCondition(before.schema_version === "catalog-bench.object-fault-probe.v1", "unexpected before schema");
requireCondition(after.schema_version === before.schema_version, "probe schema mismatch");
requireCondition(before.client_disconnected && !before.object_persisted, "before-upstream persistence contract failed");
requireCondition(after.client_disconnected && after.object_persisted, "after-upstream persistence contract failed");
requireCondition(before.content_sha256 === after.content_sha256, "probe content hashes differ");
requireCondition(before.proxy_state.events.length === 1 && before.proxy_state.events[0].upstream_status == null, "before-upstream event is not exact");
requireCondition(after.proxy_state.events.length === 1 && after.proxy_state.events[0].upstream_status === 200, "after-upstream event is not exact");
process.stdout.write(JSON.stringify({
  schema_version: "catalog-bench.object-fault-run.v1",
  run_id: runId,
  status: "verified",
  scenario: "object-store.metadata-persistence-faults",
  content_sha256: before.content_sha256,
  before_upstream: {artifact_sha256: hash("before-upstream.json"), object_persisted: false},
  after_upstream: {artifact_sha256: hash("after-upstream.json"), object_persisted: true, upstream_status: 200}
}, null, 2) + "\n");
NODE

cleanup
trap - EXIT
echo "verified object-store fault evidence: $OUTPUT_DIR/summary.json"
