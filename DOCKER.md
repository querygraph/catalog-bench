# Docker harness

The catalog interoperability harness owns its execution substrate.
`docker-compose.yml` creates
the `catalog-bench-net` bridge, the shared MinIO process and `warehouse` bucket,
and catalog-private state volumes. It does not depend on `~/src/boat`, an
external Docker network, host MinIO ports, or a host-built benchmark process.

The checked-in [current candidate profile](profiles/v1/current-2026-08-26.json)
is the broad authority for selected versions and provenance. Compose references
are digest-pinned for Linux ARM64. The generated
[runnable contention profile](profiles/v1/contention-2026-08-27.json) narrows
that pinset to the same-table contention topology and records the exact optimized
runner, LakeCat, MinIO, and helper artifacts. The generated
[Spark interoperability profile](profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json)
separately resolves the selected Spark/Iceberg runtime. The broad candidate
correctly remains `draft` for artifacts not resolved by either scenario.

## Phase 1 topology

```text
                         catalog-bench-net

  benchmark/client ──────────────┬───────────────────────┐
                                 │ Iceberg REST          │
                                 v                       v
                           Lakekeeper :8181         other catalogs
                                 │
                                 v
                         PostgreSQL :5432
                      dedicated role/database/volume

  benchmark/client ──────────────┬───────────────────────┐
                                 │ S3                    │ S3
                                 v                       v
                              MinIO :9000  ── s3://warehouse
```

All measured traffic uses service DNS names on this network. Host port 8186 is
published only for interactive inspection of Lakekeeper; it is not the endpoint
used by benchmark evidence. MinIO publishes no host port, which prevents an
unrelated local MinIO from accidentally entering a run.

## Exact infrastructure

- **MinIO** is built from upstream tag
  `RELEASE.2025-10-15T17-29-55Z`, commit
  `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`, using upstream's Go 1.24.8
  toolchain in the pinned `golang:1.24.8-bookworm` image. The final image is
  scratch-based and contains only MinIO, the typed readiness/setup helpers, CA
  roots, and MinIO's license.
- **PostgreSQL** is `17.11-bookworm` at the profile's immutable image digest.
  Lakekeeper receives a dedicated `lakekeeper` role, database, and named volume.
- **Lakekeeper** is `v0.13.3` at the profile's immutable image digest. The exact
  image runs migrations before `serve`; its own `healthcheck` subcommand gates
  process readiness.
- **Bootstrap** uses a typed helper compiled alongside the MinIO setup tools.
  It reads Lakekeeper's management state before writing, verifies the exact
  server version, and rejects an existing warehouse whose configuration drifts
  from the checked-in request. The JSON first accepts Lakekeeper's terms and
  creates the nil-ID default project, then creates warehouse `bench` at
  `s3://warehouse/lakekeeper`. The warehouse uses MinIO's S3-compatible endpoint,
  path-style requests, and STS credential vending.
- **Polaris setup** uses another typed helper in that same source-built image.
  It obtains OAuth client credentials without logging the token, reads before it
  writes, creates only a missing `bench` catalog, and then compares catalog type,
  base location, allowed locations, internal/external MinIO endpoints, region,
  path-style mode, MinIO STS endpoint, fixture role ARN, and unavailable KMS.
  MinIO implements STS, so `stsUnavailable` remains false: Polaris uses its
  standard AWS environment identity to call MinIO `AssumeRole` and returns
  scoped temporary credentials to a stock client that requests delegation. The
  helper also reads the grants on the catalog's built-in `catalog_admin` role,
  adds `CATALOG_MANAGE_CONTENT` only when absent, and reads back the role before
  succeeding. That catalog privilege supplies the table read/write data
  privileges required by stock clients; extra server-managed grants remain
  untouched. A separate gate proves authenticated config negotiation with
  `warehouse=bench`.
- **Nessie** uses static fixture credentials and advertises
  `http://minio:9000` as both its internal and client-visible S3 endpoint. Every
  benchmark client shares the Compose network, where service DNS is valid and
  `127.0.0.1` would incorrectly refer to the client container itself.
- **Gravitino** uses its 1.3.0 `GRAVITINO_ICEBERG_REST_*` rewrite namespace,
  SQLite-backed private state, `S3FileIO`, and the documented
  `s3-secret-key` credential provider. The provider turns the configured MinIO
  key pair into credentials for table operations instead of creating metadata
  and then failing the response because no credential provider was available.

The fixture credentials in Compose are intentionally obvious and local-only.
They are part of a reproducible benchmark topology, not production deployment
guidance. Do not expose this network or reuse the values in a real environment.

## Validate without starting containers

Static validation does not require a running Docker daemon:

```sh
docker compose \
  --profile lakekeeper \
  --profile nessie \
  --profile polaris \
  --profile gravitino \
  --profile bench \
  --profile pyiceberg \
  --profile spark \
  config --quiet

(cd docker/minio/tools && gofmt -d . && go mod tidy -diff && go vet ./... && go test ./...)
jq -e . docker/lakekeeper/*.json
```

## Start Lakekeeper independently

Build MinIO and start Lakekeeper through the client-facing readiness gate. The
explicit `docker wait` turns the final one-shot container's exit code into the
command's exit status; Compose's `up --wait` mode is intended for long-running
services and reports completed one-shots as stopped:

```sh
docker compose --profile lakekeeper build minio
docker compose --profile lakekeeper up --detach lakekeeper-ready
lakekeeper_ready_id="$(docker compose --profile lakekeeper ps \
  --all --quiet lakekeeper-ready)"
test "$(docker wait "$lakekeeper_ready_id")" = 0
```

The dependency chain is deliberate:

```text
postgresql healthy -> lakekeeper-migrate completed -> lakekeeper healthy
                                                        |
minio healthy -> minio-init completed ------------------+
                                                        v
                                          bootstrap completed
                                                        v
                                      warehouse creation completed
                                                        v
                               client config negotiation completed
```

Inspect the resulting state:

```sh
docker compose --profile lakekeeper ps --all
docker compose --profile lakekeeper logs lakekeeper-migrate lakekeeper
curl -fsS 'http://127.0.0.1:8186/catalog/v1/config?warehouse=bench'
```

For requests made from a benchmark/client container, use
`http://lakekeeper:8181/catalog` and warehouse/prefix `bench`. Do not use the
host-published endpoint in timed runs.

## State lifecycle

Ordinary shutdown preserves MinIO objects and Lakekeeper's PostgreSQL state:

```sh
docker compose --profile lakekeeper down
```

An evidence run must use `docker-compose.clean.yml`, which requires one new run
ID as the Compose project and explicitly names every persistent volume from that
ID. Never reuse a persistent developer or evidence volume and claim clean-state
evidence. The canonical launcher performs the preflight and does not delete old
state:

```sh
docker/run-contention.sh "run_$(date -u +%m%d%H%M%S)"
```

Before changing running services, the launcher rejects an existing transcript,
any container in the requested project, and each expected `<run-id>_<store>`
volume. It discovers any prior Compose project on `catalog-bench-net`, accepts
only the ordinary project or a scenario-safe run ID, and stops those containers
with `down --remove-orphans`; an unmanaged container or unknown project makes
the run fail closed. It then verifies that the network has no remaining
attachments, including containers whose recognized Compose label does not
belong to the current model, before spending resources on either production
build. Every source-built production image—MinIO, LakeCat, and the benchmark
runner—is built under the stable ordinary Compose project, not the run-scoped
evidence project, because Compose project labels are part of the exported image
config and therefore its local-image digest. The build also disables BuildKit's
default provenance wrapper: its attestation identity changes on each invocation
even when the platform manifest and executable bytes are identical. Immutable
source revisions, OCI revision labels, platform-image digests, embedded
executable digests, and the checked-in build recipe carry the reproducibility
evidence instead. The launcher never passes `--volumes`,
preserving prior state. The explicit network remains
`catalog-bench-net`, so production evidence runs are intentionally serialized;
all measured clients, catalogs, and MinIO continue to share that one network.
After a run, the project and volumes remain available for diagnosis. A rerun
must choose another ID.

## Production-optimized Rust images

Compose builds LakeCat directly from its profile-selected public Git commit
through an immutable named Docker build context; no sibling checkout, host-built,
or pre-staged ELF enters the image. The final OCI image records that exact source
revision.
`docker/lakecat/Dockerfile` and `docker/bench.Dockerfile` use the profile-pinned
Rust 1.97.1 image, locked dependencies, optimization level 3, fat LTO, one
codegen unit, stripped symbols, aborting panics, disabled incremental builds,
and the container CPU's native target features. Warnings are fatal. Persistent
BuildKit Cargo caches shorten source-only rebuilds, while the shipped executables
are copied through an ordinary `/out` layer and remain independent of cache
lifetime.

The MinIO image has two independent source identities. The server is fetched at
the exact upstream release revision, while bucket initialization, health,
Lakekeeper/Polaris setup, and readiness helpers are copied from the exact public
catalog-bench revision declared as a named Docker build context. The image
records both revisions; no helper source is copied from the mutable host tree.

The source-built artifacts are materialized in
[`profiles/v1/contention-2026-08-27.json`](profiles/v1/contention-2026-08-27.json)
from the audited
[`materializations/v1/contention-2026-08-27.json`](materializations/v1/contention-2026-08-27.json)
sidecar. After every production build, the launcher verifies the actual local
image IDs, Linux/ARM64 platform, relevant OCI and Compose labels, and the digest
and size of every selected executable copied directly from a stopped container.
Any mismatch aborts before the evidence project starts.

## Spark interoperability runtime

The Phase 2 Spark profile uses two production Docker targets from
[`docker/spark/Dockerfile`](docker/spark/Dockerfile): an independently
inspectable Iceberg connector image and the stock Spark image that executes
byte-identical copies of those connector JARs. Nothing runs in a host JVM or
downloads a connector at startup.

Materialize the images through the checked preflight:

```sh
docker/build-spark-images.sh
```

The script resolves the exact Spark 4.1.3 index selected by the broad profile,
requires Linux/ARM64, verifies the descriptor before creating BuildKit's local
base indirection, and builds with provenance wrappers disabled. The Dockerfile's
remote `ADD --checksum` instructions admit only the selected Iceberg 1.11.0
Spark 4.1/Scala 2.13 runtime and AWS/S3FileIO bundle.

Verify the deterministic profile and the actual local image bytes:

```sh
cargo run -p catalog-bench-contract --locked -- profile check-spark \
  --source-profile profiles/v1/current-2026-08-26.json \
  --materialization materializations/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json \
  --output profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json

docker/verify-profile-artifacts.sh \
  profiles/v1/current-2026-08-26.json \
  materializations/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json \
  profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json

COMPOSE_PROFILES=spark docker compose run --rm --no-deps spark --version
```

The Spark service runs as UID/GID 185 with a read-only root, no Linux
capabilities, `no-new-privileges`, read-only contract mounts, and only tmpfs plus
the evidence directory writable. It uses `catalog-bench-net` and shared MinIO,
the same execution boundary as the catalogs. The version smoke proves the stock
Spark binary and revision; it is not an interoperability result.

## Same-table contention sweep

The strict commit runner is the `bench` service. Its image resolves the exact
profile-selected public source commit as its Docker context and embeds that same
revision at compile time; startup rejects source, operating-system, or
architecture drift before reading credentials or making a request. The
container has a read-only root filesystem, no Linux capabilities, and read-only
contract mounts. Only `/evidence` is writable.

The host is limited to Docker orchestration and copying/hash-checking immutable
image files; no host-built service or benchmark executable participates. The
launcher requires Docker, `jq`, and either `sha256sum` or `shasum`, in addition
to ordinary POSIX command-line utilities.

Activate every catalog profile. The `bench` dependency graph waits for shared
MinIO initialization plus LakeCat, Lakekeeper, Nessie, Polaris, and Gravitino's
client-facing readiness gates:

```sh
docker/run-contention.sh "run_$(date -u +%m%d%H%M%S)"
```

The launcher passes only the runnable contention profile to the benchmark. The
benchmark process, all catalogs, readiness helpers, and MinIO communicate
only through `catalog-bench-net`; host-published catalog ports do not participate
in measured traffic. The default transcript directory is
`target/commit-evidence`. Set `CATALOG_BENCH_COMMIT_EVIDENCE_DIR` before running
Compose to choose another host directory.

The output is create-new and value-sanitized. Exit `0` means all 30 scheduled
catalog rounds passed, `2` means the complete full-ranking transcript was
written with one or more catalogs unranked, and `1` means no valid transcript
could be completed. See [Commit contention](docs/COMMIT-CONTENTION.md) for the
request, accounting, cleanup, aggregation, ranking, and publication contracts.

The accepted C110 transcript has been reviewed and materialized as the current
[immutable result bundle](results/v1/2026-08-27/manifest.json). Verify its exact
source hashes, recomputed aggregates, typed records, cross-document links, and
generated matrix with:

```sh
cargo run -p catalog-bench-contract --locked -- contention-import check --root .
```

## Stock PyIceberg interoperability

C1-07 adds a separate stock-client image built from
[`docker/pyiceberg.Dockerfile`](docker/pyiceberg.Dockerfile). It directly pins
the profile's Python 3.13.15 Linux ARM64 child manifest and installs the complete
PyIceberg 0.11.1 / PyArrow 25.0.1 / S3FS 2026.7.0 environment from wheel hashes in
[`clients/pyiceberg/requirements.lock`](clients/pyiceberg/requirements.lock).
The build cannot resolve a new dependency, accept a different wheel, or fall
back to a source distribution. The container runs unprivileged with a read-only
root filesystem on `catalog-bench-net`; both REST and S3 traffic remain inside
the same Docker topology as every catalog and MinIO.

Bring up all five catalogs and their readiness chains:

```sh
profiles=(
  --profile lakekeeper
  --profile nessie
  --profile polaris
  --profile gravitino
  --profile pyiceberg
)

docker compose "${profiles[@]}" build lakecat pyiceberg
docker compose "${profiles[@]}" up --detach \
  lakecat lakekeeper-ready nessie-ready polaris-ready gravitino-ready

for gate in lakekeeper-ready nessie-ready polaris-ready gravitino-ready; do
  gate_id="$(docker compose "${profiles[@]}" ps --all --quiet "$gate")"
  test -n "$gate_id"
  test "$(docker wait "$gate_id")" = 0
done

docker compose "${profiles[@]}" run --rm --no-deps \
  --env READY_URL=http://lakecat:8181/catalog/v1/config \
  --entrypoint /usr/local/bin/wait-http \
  minio
```

Run the complete matrix with one fresh fixture. The CLI creates the named output
directory exclusively and writes one transcript per profile adapter:

```sh
fixture_id="c107_$(date -u +%m%d%H%M%S)"
docker compose "${profiles[@]}" run --rm pyiceberg matrix \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/client.pyiceberg.interoperability.json \
  --fixture-id "$fixture_id" \
  --output-dir "/evidence/$fixture_id"
```

Exit `0` means every catalog passed all required stock-client assertions; exit
`2` means every attempted transcript was still written but at least one catalog
is `fail` or required-`unsupported`; exit `1` is an invocation, contract, or
evidence-write failure. Optional operation failures and unsupported capabilities
remain explicit inside an otherwise passing required result.

The runner never purges table data. It proves all run-owned catalog identifiers
and the namespace absent, while retained Parquet/metadata objects remain
available for the later shared-MinIO audit. The default host destination,
`target/pyiceberg-evidence`, is mutable smoke evidence and cannot be published
directly. See
[`clients/pyiceberg/README.md`](clients/pyiceberg/README.md) for operation,
classification, registration, sanitization, and lock-maintenance details. The
accepted five-catalog matrix, exact artifact identities, row hashes,
shared-MinIO audit, deployment findings, and rejected diagnostics are in
[`docs/PYICEBERG-INTEROPERABILITY.md`](docs/PYICEBERG-INTEROPERABILITY.md).

## Behavioral conformance smoke evidence

Build the optimized runner and LakeCat, start LakeCat on the shared network, then
execute the checked-in profile and scenario from the conformance container:

```sh
docker compose --profile conformance build lakecat conformance
docker compose --profile conformance up --detach --force-recreate lakecat
docker compose --profile conformance run --rm conformance config \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/iceberg-rest.config.negotiation.json \
  --catalog lakecat \
  --output /evidence/lakecat-config.json
```

Run the namespace lifecycle with a fresh portable fixture ID:

```sh
docker compose --profile conformance run --rm conformance namespace \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/iceberg-rest.namespace.behavior.json \
  --catalog lakecat \
  --fixture-id review_lakecat_01 \
  --output /evidence/lakecat-namespace.json
```

Run the table lifecycle with a different fresh fixture ID and output path:

```sh
docker compose --profile conformance run --rm conformance table \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/iceberg-rest.table.behavior.json \
  --catalog lakecat \
  --fixture-id review_lakecat_table_01 \
  --output /evidence/lakecat-table.json
```

Run deterministic commit correctness with another fresh fixture ID:

```sh
docker compose --profile conformance run --rm conformance commit \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/iceberg-rest.commit.correctness.json \
  --catalog lakecat \
  --fixture-id review_lakecat_commit_01 \
  --output /evidence/lakecat-commit.json
```

The required branch admits matching UUID/schema requirements, advances to
schema 1, submits a deterministically stale schema-0 requirement, and proves the
HTTP 409 leaves the metadata pointer, schema, and complete property map
unchanged. It verifies that required final state before any optional mutation.
The optional branch runs only when config advertises a nonempty standard
`idempotency-key-lifetime`; otherwise no `Idempotency-Key` header is sent and
the three idempotency assertions are explicitly `not-evaluated`. Advertised
catalogs receive one UUIDv7 key for an exact byte-identical replay and a
same-key content-drift attempt. The raw key can cross the HTTP boundary but is
redacted from request, response, failure, and serialized transcript evidence.

Fixture IDs use a conservative cross-catalog grammar. The runner derives
run-owned namespace and table names, rejects collisions before mutation, and
performs dependency-ordered cleanup plus post-drop verification after both
passing and failing assertions. Table cleanup reconciles the source, rename
destination, dropped sibling, and registration destination with
`purgeRequested=false` before dropping the fixture namespace. The commit
runner similarly reconciles its one table without purge and proves both
table and namespace absent. A failed preflight is the only path that forbids
cleanup mutation, preventing a colliding pre-existing fixture from being
deleted. The exact optimized five-catalog C1-04 namespace matrix is documented
in
[`docs/NAMESPACE-CONFORMANCE.md`](docs/NAMESPACE-CONFORMANCE.md); the C1-05
table matrix, shared-MinIO object audit, and rejected-run analysis are in
[`docs/TABLE-CONFORMANCE.md`](docs/TABLE-CONFORMANCE.md); and the C1-06
requirement, stale-state, exact-retry, and idempotency-content matrix is in
[`docs/COMMIT-CONFORMANCE.md`](docs/COMMIT-CONFORMANCE.md).

Choose a new output name and fixture ID for every run: the CLI refuses
to overwrite evidence or mutate a colliding fixture.
Exit `0` means all required assertions passed, `2` means a `fail` or declared
`unsupported` transcript was written, and `1` means invocation, contract, or I/O
failure. The default host destination is the ignored
`target/conformance-evidence` directory. Those files are smoke diagnostics, not
publishable results; publication requires immutable result/manifest wrapping,
environment capture, redaction review, and exact-byte hashes.

## Optional catalog profiles

Nessie, Polaris, and Gravitino remain behind Compose profiles and now share the
owned MinIO/network. Their selected images are digest-pinned. They are not
declared behaviorally ready merely because Compose can start them: C1-02 validates
each adapter binding and C1-03 through C1-07 establish operation-level outcomes.

```sh
docker compose --profile nessie up --detach nessie-ready
docker compose --profile polaris up --detach polaris-ready
docker compose --profile gravitino up --detach gravitino-ready
```

For the selected gate, resolve its container and require a zero exit before
running the conformance container, just as for `lakekeeper-ready`:

```sh
gate=polaris-ready
gate_id="$(docker compose --profile polaris ps --all --quiet "$gate")"
test "$(docker wait "$gate_id")" = 0
```

`nessie-ready` and `gravitino-ready` retry their anonymous config route for at
most 90 seconds. `polaris-ready` runs only after the typed catalog reconciler and
performs OAuth-backed config negotiation. A completed gate is readiness, not a
conformance outcome; the scenario runner still owns assertions and evidence.

Gravitino 1.3.0 rewrites its server configuration only from the exact
`GRAVITINO_ICEBERG_REST_*` environment namespace. After recreating that service,
inspect only the non-secret effective settings before accepting an S3-backed
run:

```sh
docker compose --profile gravitino exec --no-TTY gravitino \
  sed -n '1,240p' \
  /opt/gravitino-iceberg-rest-server/conf/gravitino-iceberg-rest-server.conf \
  | rg 'catalog-backend|warehouse|uri =|io-impl|s3-endpoint|s3-region|s3-path-style'
```

The accepted shape is JDBC at `jdbc:sqlite:/data/gravitino.db`, warehouse
`s3://warehouse/`, `S3FileIO`, endpoint `http://minio:9000`, region
`us-east-1`, and path-style access. Seeing `catalog-backend = memory` or
`warehouse = /tmp` means the container ignored its environment and the run is
not comparable shared-MinIO evidence. On a fresh named volume,
`gravitino-state-init` first assigns the dedicated `/data` directory to UID
1000 and exits successfully; the catalog waits for that gate and then runs as
the upstream image's unprivileged user. A permission error opening
`/data/gravitino.db` means that gate did not complete and also invalidates the
run.

Released Unity Catalog OSS 0.5.0 is not in this topology because its Iceberg REST
surface is read-only. It remains an explicit `unsupported` capability outcome,
not a failed or silently omitted commit benchmark.

Nessie 0.108.4 remains useful diagnostic infrastructure. Its historical
2026-08-08 concurrent run is an unranked `fail`, not a valid performance row,
because all measured rounds contained request-context HTTP 500 responses.
