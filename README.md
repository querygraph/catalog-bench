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
| `commit` | **ready** | Iceberg REST **commit-path** latency + throughput across catalogs — the impartial, catalog-only comparison (detailed below). LakeCat ranks **#1 among passing catalogs** in the 2026-08-08 concurrent sweep; Nessie is faster raw but failed the zero-request-errors assertion and is unranked. |
| `write-data` | **ready** | Realistic **writes**: a real Parquet data file → the same MinIO bucket, then a LakeCat commit. Write throughput under realistic payloads. |
| `cache-scan` | **ready** | **Cold vs warm Parquet scan** via Sail's [Foyer object-store cache](https://github.com/lakehq/sail/issues/1015): measured **~26×** warm-vs-cold (per-file p50 warm 1.81 ms vs cold/no-cache ~47.5 ms; 87 MB dataset). |
| `rust-vs-jvm` | **ready** | **Sail/DataFusion (Rust) vs Apache Spark 3.5.3 (JVM)**, same query/files/MinIO: **1.63×** engine edge with no local cache (Rust 446 ms vs Spark-warm 729 ms p50), **57.5×** with the warm Foyer cache. |
| `read-write` | **ready** | A **proven stock-client Iceberg round-trip**: a raw pyiceberg 0.11.1 `RestCatalog` (no shim) does init → create_table → `table.append` (a real snapshot) → scan back 1000 rows through LakeCat — plus the Foyer read path (read-warm ~150× cold). |

All five run against the shared MinIO harness and emit a JSON `BenchReport`; see
**[RESULTS.md](RESULTS.md)** for the full measured numbers and methodology.

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
cargo run -p catalog-bench -- run commit -- ...     # run one (args after -- pass to the bench)
cargo run -p catalog-bench -- run all               # run all ready benchmarks, aggregate reports
```

Each benchmark emits a JSON `BenchReport` that the driver aggregates; `run all`
runs all five.

Contract maintenance and validation use the companion CLI:

```sh
cargo run -p catalog-bench-contract -- schemas check
cargo run -p catalog-bench-contract -- validate profiles/v1 scenarios/v1 results/v1
cargo run -p catalog-bench-contract -- bundle validate \
  --manifest results/v1/2026-08-08/manifest.json
cargo run -p catalog-bench-contract -- matrix check \
  --manifest results/v1/2026-08-08/manifest.json \
  --output results/v1/2026-08-08/MATRIX.md
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
plus the no-shim stock-client oracle
[`client.pyiceberg.interoperability`](scenarios/v1/client.pyiceberg.interoperability.json).
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

## The commit benchmark

A catalog-agnostic benchmark for the **commit path** of Iceberg REST catalogs —
measured across **LakeCat, Apache Nessie, Apache Gravitino, and Apache Polaris**.
(Unity Catalog OSS is *not yet measurable*: its Iceberg REST endpoint is read-only
until the commit endpoints in PR #1618 / 0.6.0 ship — see [RESULTS.md](RESULTS.md)
→ "Not measured".)

TPC-DS/TPC-H measure query engines; they touch the catalog only incidentally. The
`commit` benchmark isolates the part those benchmarks ignore: the catalog **commit
transaction** — update validation, writing the new `metadata.json`, the
metadata-pointer compare-and-swap, and the catalog's private-state transition. It
issues `set-properties` commits (no data files), so each request exercises the
catalog's commit machinery without engine or data-write noise. Every target
speaks the same Iceberg REST protocol, so one binary benchmarks all of them; only
the base URL, prefix, and auth differ.

### Latest concurrent ranking (2026-08-08)

The canonical [generated concurrent matrix](results/v1/2026-08-08/MATRIX.md)
ranks only `pass` outcomes by median successful throughput with eight writers.
It is rendered from the immutable result records and checked against them in
tests; it is not a separately maintained table. Round 1 of six is explicitly
recorded as conditioning. Nessie's faster raw measurements remain visible, but
its failed zero-request-errors assertion makes the row unranked.

#### Why did Nessie pass the previous benchmark?

It did not prove that it was error-free. The previous public row used Nessie
0.107.5 and came from one retained reference sweep. At that point the concurrent
worker discarded every request failure other than an HTTP 409 as “transient,” so
HTTP 500 responses were neither counted nor allowed to fail the process. The new
driver records them and requires zero request errors in every measured round.

Strict preflights reproduced the same Quarkus request-context failure on Nessie
0.107.5, 0.107.6, and 0.108.4. The version update therefore does not explain the
new failed result: the benchmark's observability and validity rules changed. Nessie
remains the fastest raw concurrent row at 190.0 successful commits/s, but 97 HTTP 500s
across the five measured rounds make it ineligible for a numeric rank. The full
forensic explanation is in [RESULTS.md](RESULTS.md#why-nessie-appeared-to-pass-previously).

The complete latency table, min–max ranges, production artifact hashes, Nessie
failure analysis, and all raw runs/object audits are in [RESULTS.md](RESULTS.md).

## What it measures

1. **Sequential latency** — `--iterations` commits in series; reports throughput
   and p50/p90/p99/max latency. The clean per-commit cost.
2. **Concurrent throughput** — `--concurrency` writers committing for
   `--duration-secs`; reports committed/s, 409 conflict rate, request-error rate,
   and a nonzero process status when any request fails. The report is emitted
   before that strict error gate so an error-voided run remains auditable.

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
- **Same parameters, same host/network.** Identical warmup, `--iterations`,
  `--concurrency`, and `--duration-secs`; six rounds with rotated order; all
  containers on one Docker network so latency is not confounded by cross-host RTT.
- **Strict validity.** Every run records all request errors and its first failure.
  A public row must have zero errors and MinIO object growth of at least warmup +
  sequential accepts + concurrent accepts in every measured round.
- **Same request shape.** All use the standard `POST namespaces` / `POST tables` /
  bare `POST tables/{t}` commit. LakeCat takes `--location s3://warehouse/lakecat`
  because it does not derive an S3 warehouse location on its own; the others get
  their S3 warehouse from their config.

## Docker setup for impartial runs with MinIO

The Phase 1 topology is now owned completely by this repository. Compose creates
`catalog-bench-net`, source-builds the pinned MinIO release, initializes the
shared `warehouse` bucket, and gives each catalog isolated private state. It no
longer relies on `~/src/boat`, an external Docker network, a host MinIO, or the
mutable `minio/mc` image. See [DOCKER.md](DOCKER.md) for the canonical topology,
exact provenance, readiness chain, and commands.

```
                          catalog-bench-net  (Compose-owned network)
   ┌───────────┐   ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌──────────────┐
   │  lakecat  │   │  nessie  │   │ gravitino │   │ polaris  │   │ catalog-     │
   │  :8181    │   │  :19120  │   │  :9001    │   │  :8181   │   │ bench-commit │
   └─────┬─────┘   └────┬─────┘   └─────┬─────┘   └────┬─────┘   └──────┬───────┘
         └──────────────┴───── s3://warehouse ─────────┴────────────────┘
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

Starting an image is not a conformance result. New public measurements wait for
C1-03 through C1-09 to provide operation assertions, generated evidence, and the
fully optimized same-Docker artifact pipeline.

**Why LakeCat is built from source.** LakeCat depends on Sail as a Cargo *git*
dependency on `querygraph/sail#lakecat` (fetched at build time); Grust and TypeSec
are published crates. Compose passes the adjacent LakeCat checkout as a named
build context, then the pinned Rust image compiles the locked `turso-local` +
`sail-local` production service with fat LTO and fatal warnings. The slim runtime
receives only the resulting executable—never a mutable host-staged ELF.

### 4. Benchmark launcher status

`bench-stack.sh` and the manual recipes below reproduce the earlier commit-only
development workflow. They are retained for historical diagnostics, but they
still build or execute part of the workload on the host and therefore cannot
produce new public Phase 1 evidence. C1-09 replaces them with smoke and full
commands that materialize and hash optimized production artifacts, then run every
measured process inside the same Docker environment.

## Build the driver alone

```sh
cargo build --release
```

## Manual run recipes

All use the same standard endpoints; they differ only in URL prefix, auth, and
(for LakeCat) the `--location` that pins writes to MinIO. Identical params:

```sh
P="--namespace bench --table commits --create --iterations 1000 --concurrency 8 --duration-secs 6"
```

### LakeCat
```sh
catalog-bench-commit --base-url http://127.0.0.1:8181/catalog \
  --location s3://warehouse/lakecat --idempotency $P
```

### Apache Nessie
```sh
catalog-bench-commit --base-url http://127.0.0.1:19120/iceberg --prefix main $P
```

### Apache Gravitino
```sh
catalog-bench-commit --base-url http://127.0.0.1:9002/iceberg $P
```

### Apache Polaris
OAuth2: `polaris-bootstrap.sh` fetches a token and creates an S3 catalog on the
shared MinIO `warehouse` bucket; the prefix is the catalog name.
```sh
TOKEN=$(./polaris-bootstrap.sh)
catalog-bench-commit --base-url http://127.0.0.1:8185/api/catalog \
  --prefix bench --token "$TOKEN" $P
```

### Unity Catalog (OSS) — not yet supported on the commit path
Released Unity OSS (0.5.0) serves its Iceberg REST endpoint **read-only**, so the
commit benchmark has nothing to exercise. The commit handler lands only in unmerged
draft PR [#1618](https://github.com/unitycatalog/unitycatalog/pull/1618) (unreleased
0.6.0). Against a **write-capable build** of that branch the recipe would be a bearer
token on the bare commit path:
```sh
catalog-bench-commit --base-url http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg \
  --prefix unity --token "$UC_TOKEN" $P
```

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
- **Nessie 0.108.4** is reproducible through its profile, but its eight-writer
  commit path failed the final integrity gate in all five measured rounds with
  Quarkus request-context HTTP 500s. It remains in the generated matrix as an
  unranked `fail` outcome.
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
- Put the catalog and the driver on the same host/network for the latency phase;
  cross-AZ RTT will dominate otherwise.
- `--idempotency` only affects catalogs that implement an idempotency key
  (LakeCat); others ignore the header.

## Key flags

| Flag | Meaning |
|---|---|
| `--base-url` | Up to and including any catalog-specific prefix path |
| `--prefix` | Iceberg REST prefix segment (warehouse/catalog/metalake); may be empty |
| `--location` | Explicit `createTable` location, e.g. `s3://warehouse/lakecat` (points writes at the shared MinIO) |
| `--create` | Create the namespace + table before benchmarking |
| `--idempotency` | Send a LakeCat-style `Idempotency-Key` per commit |
| `--token` | Bearer token (`Authorization: Bearer ...`) |
| `--iterations` / `--concurrency` / `--duration-secs` | Sequential count / concurrent writers / concurrent duration |
| `--json` | Machine-readable summary; emitted before a nonzero request-error exit |
