#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/docker/fresh-run-lib.sh"
RUN_ID="${1:-}"
QUERYGRAPH_ROOT="${QUERYGRAPH_ROOT:-$(cd "$ROOT_DIR/../querygraph" && pwd)}"
OUTPUT_DIR="${2:-$ROOT_DIR/target/querygraph-tpcds/$RUN_ID}"
catalog_bench_validate_run_id "$RUN_ID"
if [[ -e "$OUTPUT_DIR" ]]; then echo "refusing existing output directory: $OUTPUT_DIR" >&2; exit 1; fi
if ! git -C "$QUERYGRAPH_ROOT" diff --quiet -- python/querygraph/tpcds_fixture.py python/querygraph/tpcds_fixture_live.py ossie/upstream.json scripts/fetch-ossie.py; then
  echo "refusing uncommitted QueryGraph TPC-DS implementation" >&2; exit 1
fi
QUERYGRAPH_REVISION="$(git -C "$QUERYGRAPH_ROOT" rev-parse HEAD)"
catalog_bench_prepare_fresh_project "$ROOT_DIR" "$RUN_ID"
mkdir -p "$OUTPUT_DIR/runtime" "$OUTPUT_DIR/upstream"
python3 "$QUERYGRAPH_ROOT/scripts/fetch-ossie.py" fetch "$OUTPUT_DIR/upstream" \
  --manifest "$QUERYGRAPH_ROOT/ossie/upstream.json"
uv run --with pyyaml python - "$OUTPUT_DIR" <<'PY'
import json, pathlib, sys, yaml
root=pathlib.Path(sys.argv[1])
model=yaml.safe_load((root/'upstream/examples/tpcds_semantic_model.yaml').read_text())
(root/'runtime/model.json').write_text(json.dumps(model, sort_keys=True, separators=(',',':'))+'\n')
PY
rm -rf "$OUTPUT_DIR/upstream"

compose() {
  CATALOG_BENCH_SPARK_EVIDENCE_DIR="$OUTPUT_DIR/runtime" CATALOG_BENCH_RUN_ID="$RUN_ID" \
    docker compose --project-directory "$ROOT_DIR" -f "$ROOT_DIR/docker-compose.yml" \
    -f "$ROOT_DIR/docker-compose.clean.yml" -f "$ROOT_DIR/docker-compose.tpcds.yml" "$@"
}
cleanup() { compose --profile '*' down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT
compose up -d lakecat-ready
compose run --rm --no-deps -v "$QUERYGRAPH_ROOT:/querygraph:ro" \
  -e PYTHONPATH=/querygraph/python -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password \
  -e AWS_REGION=us-east-1 -e AWS_DEFAULT_REGION=us-east-1 \
  --entrypoint /opt/spark/bin/spark-submit spark \
  /querygraph/python/querygraph/tpcds_fixture_live.py --model /evidence/model.json \
  --rest-uri http://lakecat:8181/catalog --warehouse local --s3-endpoint http://minio:9000 \
  --namespace tpcds --output /evidence/result.json \
  --artifact-uri https://raw.githubusercontent.com/apache/ossie/1d9ebcea2932d3381c0840cc8304f0850d366509/examples/tpcds_semantic_model.yaml \
  --artifact-hash sha256:438372de9b8ca0f074aed72806f92ac9b84047851a0385423f004748efe5a316

python3 - "$OUTPUT_DIR" "$RUN_ID" "$QUERYGRAPH_REVISION" <<'PY'
import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1]); result=json.loads((root/'runtime/result.json').read_text())
if result.get('status')!='verified' or len(result.get('tables',[]))!=5 or any(x['row-count']!=3 for x in result['tables']) or result.get('publication',{}).get('version')!=1: raise SystemExit('TPC-DS fixture/publication verification failed')
summary={'contract':'catalog-bench/querygraph-tpcds-fixture/v1','run_id':sys.argv[2],'querygraph_revision':sys.argv[3],'status':'verified','result':result}
encoded=json.dumps(summary,sort_keys=True,separators=(',',':')).encode(); summary['content_sha256']='sha256:'+hashlib.sha256(encoded).hexdigest()
(root/'summary.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n')
PY
rm -rf "$OUTPUT_DIR/runtime"
cleanup; trap - EXIT
echo "verified QueryGraph TPC-DS fixture evidence: $OUTPUT_DIR/summary.json"
