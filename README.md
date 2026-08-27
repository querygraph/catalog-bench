# catalog-bench

A benchmark **suite** for Iceberg REST catalogs and the Rust data path around
them — built on one shared, impartial MinIO/S3 harness and driven by a single
`catalog-bench` CLI. It began as a commit-path comparison (LakeCat vs Apache
Nessie / Gravitino / Polaris) and now spans the read and write paths where a Rust
stack (LakeCat + [Sail](https://github.com/querygraph/sail), with the Foyer
object-store cache) is meant to shine.

Versioned, catalog-neutral interoperability evidence uses the
[`catalog-bench/v1` contract](docs/CONTRACT.md). Its Rust algebraic data types,
Draft 2020-12 JSON Schemas, semantic validators, and explicit extension points
keep `pass`, `fail`, `unsupported`, and `not-tested` distinct. The public matrix
must be generated from these records rather than maintained independently.

## Benchmarks

| Name | Status | What it measures |
| --- | --- | --- |
| `commit` | **ready** | Iceberg REST **commit-path** latency + throughput across catalogs — the impartial, catalog-only comparison (detailed below). LakeCat ranks **#1 among passing catalogs** in the 2026-08-27 production sweep at 147.536 accepted commits/s; Lakekeeper and Nessie remain visible but unranked after non-conflict request errors. |
| `write-data` | **ready** | Realistic **writes**: a real Parquet data file → the same MinIO bucket, then a LakeCat commit. Write throughput under realistic payloads. |
| `cache-scan` | **ready** | **Cold vs warm Parquet scan** via Sail's [Foyer object-store cache](https://github.com/lakehq/sail/issues/1015): measured **~26×** warm-vs-cold (per-file p50 warm 1.81 ms vs cold/no-cache ~47.5 ms; 87 MB dataset). |
| `rust-vs-jvm` | **ready** | **Sail/DataFusion (Rust) vs Apache Spark 3.5.3 (JVM)**, same query/files/MinIO: **1.63×** engine edge with no local cache (Rust 446 ms vs Spark-warm 729 ms p50), **57.5×** with the warm Foyer cache. |
| `read-write` | **ready** | A **proven stock-client Iceberg round-trip**: a raw pyiceberg 0.11.1 `RestCatalog` (no shim) does init → create_table → `table.append` (a real snapshot) → scan back 1000 rows through LakeCat — plus the Foyer read path (read-warm ~150× cold). |

Every workload uses the shared MinIO harness. The four data-path binaries emit
the legacy JSON `BenchReport`; the stricter multi-catalog commit sweep emits a
versioned, sanitized 30-round contention transcript with its generated full
ranking. See **[RESULTS.md](RESULTS.md)** for published measurements and
[Commit contention](docs/COMMIT-CONTENTION.md) for the current runner contract.

**Honest framing on `rust-vs-jvm`:** the 1.63× is the engine edge with no local
cache; the 57.5× is the warm RAM (Foyer) cache, not engine speed — and a warm
steady-state loop is the JVM's *best* case. `read-write` runs on a `sail-local`
LakeCat; the default build honestly rejects the write (finding H9).

The suite's read-path work also **surfaced and then validated** the five fixes that
made stock Iceberg writes work end-to-end — LakeCat's **H8** config-as-objects +
canonical endpoints + **listTables** + **H9**, and Sail's add-snapshot in
`apply_table_updates` — landed across `querygraph/lakecat` and
`querygraph/sail#lakecat`. (Details in [RESULTS.md](RESULTS.md) → *read-write*.)

## The driver

```sh
cargo run -p catalog-bench -- list                 # list benchmarks + status
cargo run -p catalog-bench -- run cache-scan -- ... # run one legacy BenchReport benchmark
cargo run -p catalog-bench -- run all               # run all four BenchReport benchmarks
```

The legacy driver aggregates only the four single-process `BenchReport`
workloads. Commit contention deliberately has its own profile-driven Docker CLI:
coercing 30 rotated catalog rounds into one host-spawned report would erase
failures, provenance, and the unranked rows.

Contract maintenance and validation use the companion CLI:

```sh
cargo run -p catalog-bench-contract --locked -- schemas check
cargo run -p catalog-bench-contract --locked -- validate profiles/v1 scenarios/v1 results/v1
cargo run -p catalog-bench-contract --locked -- bundle validate \
  --manifest results/v1/2026-08-27/manifest.json
cargo run -p catalog-bench-contract --locked -- contention-import check --root .
cargo run -p catalog-bench-contract --locked -- matrix check \
  --manifest results/v1/2026-08-27/manifest.json \
  --output results/v1/2026-08-27/MATRIX.md
cargo run -p catalog-bench-contract --locked -- profile check-contention \
  --source-profile profiles/v1/current-2026-08-26.json \
  --materialization materializations/v1/contention-2026-08-27.json \
  --output profiles/v1/contention-2026-08-27.json
cargo run -p catalog-bench-contract --locked -- profile check-spark \
  --source-profile profiles/v1/current-2026-08-27.json \
  --materialization materializations/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json \
  --output profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json
```

See **[RESULTS.md](RESULTS.md)** for measured results.

Phase 1 catalog routing is profile data rather than hidden launcher branches.
[The adapter contract](docs/ADAPTERS.md) records exact config/prefix/auth bindings,
requires complete capability coverage for all five catalogs, and distinguishes a
standard request path from any behavior-changing shim. Static adapter validation
does not claim that an operation passed; behavioral evidence begins with the
versioned scenarios.

The executable behavioral scenarios now cover
[`iceberg-rest.config.negotiation`](scenarios/v1/iceberg-rest.config.negotiation.json),
[`iceberg-rest.namespace.behavior`](scenarios/v1/iceberg-rest.namespace.behavior.json),
[`iceberg-rest.table.behavior`](scenarios/v1/iceberg-rest.table.behavior.json),
and
[`iceberg-rest.commit.correctness`](scenarios/v1/iceberg-rest.commit.correctness.json),
the strict performance scenario
[`iceberg-rest.commit.same-table-contention` v2](scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json),
plus the no-shim stock-client oracle
[`client.pyiceberg.interoperability`](scenarios/v1/client.pyiceberg.interoperability.json),
and the Phase 2 stock-engine authority
[`engine.iceberg.write-read-evolution`](scenarios/v1/engine.iceberg.write-read-evolution.json).
Their typed runners negotiate anonymous or OAuth2 client-credentials access,
validate config and prefix resolution, then exercise isolated namespace and
table lifecycles with hierarchy, immutable metadata updates, errors, pagination,
optional standard operations, commit requirements, deterministic stale-state
rejection, config-gated UUIDv7 replay safety, and guaranteed cleanup. The
commit-correctness scenario is distinct from the throughput benchmark below:
it proves one transition at a time and never interprets conflict rate as
correctness. Every runner writes a sanitized transcript even when a required
assertion fails. Run them inside the Compose network; [DOCKER.md](DOCKER.md)
contains the exact commands and explains why files under `target/` are smoke
evidence rather than publishable result records. The optimized five-catalog
outcomes are recorded in
[Iceberg REST Namespace Conformance](docs/NAMESPACE-CONFORMANCE.md) for C1-04
and [Iceberg REST Table Conformance](docs/TABLE-CONFORMANCE.md) for C1-05, and
[Iceberg REST Commit Correctness Conformance](docs/COMMIT-CONFORMANCE.md) for
C1-06. The accepted C1-07 five-catalog stock-client matrix, exact Linux ARM64
artifacts, row and object proofs, catalog findings, rejected diagnostics, and
publication boundary are in
[Stock PyIceberg Interoperability](docs/PYICEBERG-INTEROPERABILITY.md). Runner
internals and lock maintenance are documented in
[`clients/pyiceberg/README.md`](clients/pyiceberg/README.md).
The engine-neutral operation vocabulary, deterministic row oracle, shim boundary,
and independent REST/MinIO evidence requirements are documented in
[Stock Engine Interoperability](docs/ENGINE-INTEROPERABILITY.md). Spark is the
first runtime implementation: its exact Spark 4.1.3, Scala 2.13, Iceberg 1.11.0,
AWS/S3FileIO, JVM, image, and in-image JAR identities are now materialized in a
runnable Linux ARM64 profile. That is runtime evidence, not yet a workflow
result. Flink and Trino must execute the same scenario rather than maintaining
engine-specific definitions of success.

## The commit benchmark

A catalog-agnostic benchmark for the **commit path** of Iceberg REST catalogs,
scheduled identically across **LakeCat, Apache Polaris, Apache Gravitino,
Lakekeeper, and Apache Nessie**.

TPC-DS/TPC-H measure query engines; they touch the catalog only incidentally. The
`commit` benchmark isolates the part those benchmarks ignore: the catalog **commit
transaction** — update validation, writing the new `metadata.json`, the
metadata-pointer compare-and-swap, and the catalog's private-state transition. It
issues `set-properties` commits (no data files), so each request exercises the
catalog's commit machinery without engine or data-write noise. Every target
speaks the same Iceberg REST protocol, so one binary benchmarks all of them. URL,
prefix, authentication, shared-table location, and catalog identity come only
from the validated profile; the CLI has no catalog-specific request knobs.

### Published production concurrent ranking (2026-08-27)

The canonical [generated concurrent matrix](results/v1/2026-08-27/MATRIX.md)
ranks only `pass` outcomes by median accepted throughput with eight writers.
It is rendered from the immutable result records and checked against them in
tests; it is not a separately maintained table. Round 1 of six is explicitly
recorded as conditioning. LakeCat ranks first at 147.536 accepted commits/s,
followed by Apache Polaris at 58.110/s and Apache Gravitino at 56.823/s.
Lakekeeper's and Nessie's diagnostic measurements remain visible, but their
failed zero-request-errors assertions make both rows unranked.

#### Why did Nessie pass the previous benchmark?

It did not prove that it was error-free. The earlier public row used Nessie
0.107.5 and came from one retained reference sweep. At that point the concurrent
worker discarded every request failure other than an HTTP 409 as “transient,” so
HTTP 500 responses were neither counted nor allowed to fail the process. The new
driver records them and requires zero request errors in every measured round.

Strict preflights reproduced the same Quarkus request-context failure on Nessie
0.107.5, 0.107.6, and 0.108.4. The version update therefore does not explain the
failed result: the benchmark's observability and validity rules changed. In
C110, Nessie remains the fastest raw diagnostic concurrent row at 153.870
accepted commits/s, but 88 measured HTTP 500 responses (106 including
conditioning) make it ineligible for a numeric rank. The full forensic
explanation is in [Nessie's failed result](docs/NESSIE-ERROR.md).

The complete latency table, min–max ranges, production artifact hashes, Nessie
and Lakekeeper failure analysis, and the complete sanitized transcript are in
[RESULTS.md](RESULTS.md).

## What it measures

1. **Sequential latency** — 1,000 commits in series; reports accepted throughput
   and complete p50/p95/p99/min/max latency. The clean per-commit cost.
2. **Concurrent throughput** — eight barrier-synchronized writers commit in one
   six-second window; every started request completes and is classified as
   accepted, HTTP 409 conflict, or explicit error.
3. **Repeated evidence** — one conditioning round plus five measured rounds per
   catalog, with rotate-left execution order and median/min/max aggregation.
4. **Strict ranking** — only catalogs passing every round are ranked, by median
   concurrent accepted commits/s. Sequential p50 latency and catalog ID are the
   deterministic tie-breakers; failed catalogs remain visible but unranked.

## Impartiality: one object store, one unit of work

A commit-path comparison is only fair if every catalog does the **same work** to
the **same storage**. The harness is built around that:

- **Same object store.** Every catalog writes its Iceberg `metadata.json` to the
  **same MinIO/S3 bucket** (`s3://warehouse`). Each catalog's own state store
  (Turso for LakeCat, the version store for Nessie, the metastore for
  Polaris/Gravitino) is its private metadata-pointer bookkeeping — the analogue
  across all of them — but the Iceberg metadata object itself lands in the shared
  MinIO for everyone. Without this, you would be comparing object stores, not
  catalogs.
- **Same unit of work.** A `set-properties` commit: validate → apply the update →
  write a fresh `metadata.json` to S3 → advance the pointer. No data files, no
  engine. Verify it with MinIO object counts: growth must cover every accepted
  warmup, sequential, and concurrent commit. Some catalogs also leave objects
  from attempts that later lose the pointer race.
- **Same parameters, same Docker network.** The scenario fixes 50 warmups,
  1,000 sequential commits, eight writers, and six seconds; six rounds rotate
  catalog order; all
  containers on one Docker network so latency is not confounded by cross-host RTT.
- **Strict validity.** Every run records all request errors and its first failure.
  A public row must have zero errors and MinIO object growth of at least warmup +
  sequential accepts + concurrent accepts in every measured round.
- **Same request shape.** All use the standard `POST namespaces` / `POST tables` /
  bare `POST tables/{t}` commit. LakeCat's optional
  `createTable.location=s3://warehouse/lakecat` is profile data; other catalogs
  obtain their shared warehouse from their standard configuration.

## Docker setup for impartial runs with MinIO

The Phase 1 topology is now owned completely by this repository. Compose creates
`catalog-bench-net`, source-builds the pinned MinIO release, initializes the
shared `warehouse` bucket, and gives each catalog isolated private state. It no
longer relies on `~/src/boat`, an external Docker network, a host MinIO, or the
mutable `minio/mc` image. See [DOCKER.md](DOCKER.md) for the canonical topology,
exact provenance, readiness chain, and commands.

```
                          catalog-bench-net  (Compose-owned network)
   LakeCat   Nessie   Gravitino   Polaris   Lakekeeper   catalog-bench-commit
      └────────────── Iceberg REST + s3://warehouse ────────────────┘
                              ┌──────────────┐
                              │ minio :9000  │   admin / password, path-style
                              └──────────────┘
```

### 1. Validate and create the shared infrastructure

```sh
docker compose --profile lakekeeper config --quiet
docker compose --profile lakekeeper build minio
```

### 2. Bring up MinIO + Lakekeeper

The first Phase 1 catalog is Lakekeeper 0.13.3 with PostgreSQL 17.11. All services
join the owned network and use `s3://warehouse` on the owned MinIO. Lakekeeper
has a dedicated PostgreSQL role, database, and volume; its migration, process
health, management bootstrap, and warehouse creation are separate readiness
gates.

Bring them up and create the bucket:

```sh
docker compose --profile lakekeeper up --detach lakekeeper-ready
lakekeeper_ready_id="$(docker compose --profile lakekeeper ps \
  --all --quiet lakekeeper-ready)"
test "$(docker wait "$lakekeeper_ready_id")" = 0
```

### 3. Add LakeCat or another catalog profile

`docker-compose.yml` also runs LakeCat and optional Nessie, Polaris, and Gravitino
profiles on the same owned network. The checked-in Compose file—not a copied
excerpt—is the authority while C1-02 validates every current adapter. For local
diagnostics, start only the profile under inspection:

```sh
docker compose --profile nessie up --detach nessie-ready
docker compose --profile polaris up --detach polaris-ready
docker compose --profile gravitino up --detach gravitino-ready
```

Starting an image is not a conformance result. C1-03 through C1-07 establish
operation-level outcomes, while the C1-09 production pipeline now publishes the
strict contention result only after generated assertions, exact artifact checks,
environment capture, and redaction review.

**Why LakeCat is built from source.** LakeCat depends on Sail as a Cargo *git*
dependency on `querygraph/sail#lakecat` (fetched at build time); Grust and TypeSec
are published crates. Compose resolves the exact public LakeCat commit named by
the profile as a Docker build context, then the pinned Rust image compiles the
locked `turso-local` + `sail-local` production service with fat LTO and fatal
warnings. The slim runtime receives only the resulting executable—never a
mutable sibling checkout or host-staged ELF.

### 4. Benchmark launcher

`bench-stack.sh` and host URL recipes reproduce only the historical development
workflow. Current contention work runs from the optimized `bench` container on
`catalog-bench-net`; catalog routing, authentication, workload dimensions, and
MinIO policy are contract data and cannot be overridden at the command line.

## Run the current contention sweep

The runner, all five catalogs, and MinIO execute in one Docker Compose topology.
The production image uses Rust 1.97.1, locked dependencies, optimization level
3, fat LTO, one codegen unit, native CPU features, stripped symbols, and aborting
panics. It embeds the profile-pinned source revision at compile time and refuses
to touch credentials or the network if runtime or source identity drifts.

```sh
docker/run-contention.sh "run_$(date -u +%m%d%H%M%S)"
```

The launcher rejects reused output, containers, and run-scoped state volumes,
then builds the optimized runner, LakeCat, and MinIO under one stable Compose
identity. Before any measured service starts, it verifies their actual image
IDs, source/platform labels, and in-image executable hashes against the generated
[runnable contention profile](profiles/v1/contention-2026-08-27.json). It then
runs all five catalogs in the same Docker topology. The output is create-new.
Exit `0` means every catalog passed every round, `2` means the complete
transcript was written but at least one catalog is unranked, and `1` means
invocation, provenance, or evidence persistence failed. A transcript becomes
publishable only after immutable result/manifest materialization and review. The
accepted C110 transcript is materialized as the
[2026-08-27 bundle](results/v1/2026-08-27/manifest.json), and
`contention-import check` deterministically verifies it. See
[Commit contention](docs/COMMIT-CONTENTION.md) and [DOCKER.md](DOCKER.md).

## Bootstrap caveats (the externals are not turnkey)

- **Polaris** needs an OAuth2 token + an S3 catalog (it does not auto-serve a
  warehouse). The Compose `polaris-bootstrap` and `polaris-ready` one-shots use a
  typed in-Docker reconciler to create catalog `bench` on
  `s3://warehouse/bench`, verify its MinIO endpoint, region,
  `stsUnavailable`/path-style settings, and then prove authenticated
  `/v1/config?warehouse=bench`. Existing same-name configuration drift is an
  error, not an accepted 409.
- **Gravitino** uses the `apache/gravitino-iceberg-rest` image; confirm your tag
  serves the REST API on the expected port (older tags differ). Use the
  file-backed JDBC backend in this compose file: `memory` acknowledges commits
  without writing objects, while `jdbc:sqlite::memory:` creates a separate schema
  per pooled connection. The pinned 1.3.0 image's rewrite script recognizes only
  the exact `GRAVITINO_ICEBERG_REST_*` environment namespace; shorter historical
  names are silently ignored and leave the image on its memory catalog and
  `/tmp` warehouse defaults. The deployment test and live effective-config check
  in [DOCKER.md](DOCKER.md) protect that boundary. A dedicated one-shot prepares
  the fresh state volume for UID 1000 before the catalog starts, so the server
  itself does not need to run as root.
- **Lakekeeper 0.13.3** is reproducible through its profile, but its
  eight-writer commit path returned PostgreSQL deadlock-backed HTTP 503s in all
  six C110 rounds. Its diagnostic measurements remain visible in the generated
  matrix without a numeric rank; see
  [the failure analysis](docs/LAKEKEEPER-ERROR.md).
- **Nessie 0.108.4** is reproducible through its profile, but its eight-writer
  commit path failed the final integrity gate in all five measured rounds with
  Quarkus request-context HTTP 500s. It remains in the generated matrix as an
  unranked `fail` outcome; see [the failure analysis](docs/NESSIE-ERROR.md).
- **Unity (OSS)** released builds (≤ 0.5.0) serve Iceberg REST **read-only** — no
  external `updateTable` commit handler exists, so it is left out of the comparison.
  Commit support is in unmerged PR #1618 (unreleased 0.6.0); build from that branch
  to include it.

## Fairness notes

- `set-properties` is the lowest-common-denominator commit every conformant catalog
  accepts; it writes no data files, so the number is **catalog overhead**, not
  storage throughput.
- **Sequential latency is the clean cross-catalog signal.** The concurrent column
  reflects **commit-conflict policy** as much as speed: strict-CAS catalogs (LakeCat,
  Nessie) show lower successful throughput under 8 writers to one table because most
  commits correctly conflict. Nessie's additional HTTP 500s are errors, not
  conflicts, and void its row's rank. See
  [Understanding LakeCat's CAS Conflict Rate](docs/CAS-CONFLICTS.md) for the
  LakeCat/Turso boundary and recommended isolation benchmarks.
- Every catalog and the driver run on `catalog-bench-net`; host or cross-AZ RTT
  is outside this comparison.
- Optional idempotency headers are omitted for every catalog, so implementation-
  specific replay behavior cannot alter the common measured request.

## Commit runner inputs

| Input | Meaning |
|---|---|
| `--profile` | Validated catalog, routing, runtime, and shared-MinIO profile |
| `--scenario` | Canonical v2 same-table contention contract |
| `--fixture-id` | Run-owned lowercase suffix used to derive isolated catalog identifiers |
| `--output` | New transcript path; existing files are never overwritten |

Base URLs, route prefixes, OAuth bindings, table locations, workload dimensions,
request shape, and ranking policy are validated contract data rather than CLI
options.
