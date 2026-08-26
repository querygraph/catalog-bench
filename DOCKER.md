# Docker harness

The Phase 1 harness owns its execution substrate. `docker-compose.yml` creates
the `catalog-bench-net` bridge, the shared MinIO process and `warehouse` bucket,
and catalog-private state volumes. It does not depend on `~/src/boat`, an
external Docker network, host MinIO ports, or a host-built benchmark process.

The checked-in [current candidate profile](profiles/v1/current-2026-08-26.json)
is the authority for selected versions and provenance. Compose references are
digest-pinned for Linux ARM64. Final public evidence additionally requires a
runnable profile containing the hashes of every optimized executable; the
candidate profile remains `draft` until those artifacts are materialized.

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
  path-style mode, and disabled STS vending. A separate gate proves authenticated
  config negotiation with `warehouse=bench`.

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

An evidence run that requires isolated fresh state must use a run-specific
Compose project name or explicitly remove only that project's volumes after the
run artifacts have been captured. Never reuse a persistent developer volume and
claim clean-state evidence. Example isolation without deletion:

```sh
COMPOSE_PROJECT_NAME=catalog-bench-smoke-001 \
  docker compose --profile lakekeeper up --detach lakekeeper-ready
```

Wait for that project's `lakekeeper-ready` container and require exit code zero
before collecting evidence, as in the normal startup sequence above.

The explicit network name is stable (`catalog-bench-net`) for the normal local
project. Concurrent isolated projects therefore need a future per-run network
override; C1-09 owns that full orchestration. Do not run two ordinary projects
with this Compose file concurrently until that unit lands.

## Production-optimized Rust images

Compose builds LakeCat directly from the adjacent source checkout through a
named Docker build context; no host-built or pre-staged ELF enters the image.
`docker/lakecat/Dockerfile` and `docker/bench.Dockerfile` use the profile-pinned
Rust 1.97.1 image, locked dependencies, optimization level 3, fat LTO, one
codegen unit, stripped symbols, aborting panics, disabled incremental builds,
and the container CPU's native target features. Warnings are fatal. Persistent
BuildKit Cargo caches shorten source-only rebuilds, while the shipped executables
are copied through an ordinary `/out` layer and remain independent of cache
lifetime.

This is the production build recipe required for final runs, but C1-09 still
owns artifact materialization: hash the resulting executables and images, create
a runnable profile containing those identities, and accept measurements only
from that same Docker environment.

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

Fixture IDs use a conservative cross-catalog grammar. The runner derives
run-owned top-level and multipart namespace names, rejects collisions before
mutation, and performs child-first cleanup plus post-drop verification after
both passing and failing assertions. The exact optimized five-catalog C1-04
matrix is documented in
[`docs/NAMESPACE-CONFORMANCE.md`](docs/NAMESPACE-CONFORMANCE.md).

Choose a new output name and namespace fixture ID for every run: the CLI refuses
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

Released Unity Catalog OSS 0.5.0 is not in this topology because its Iceberg REST
surface is read-only. It remains an explicit `unsupported` capability outcome,
not a failed or silently omitted commit benchmark.

Nessie 0.108.4 remains useful diagnostic infrastructure. Its historical
2026-08-08 concurrent run is an unranked `fail`, not a valid performance row,
because all measured rounds contained request-context HTTP 500 responses.
