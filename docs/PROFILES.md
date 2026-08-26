# Versioned profiles and adapter policy

Profiles separate environment selection from benchmark results. A profile says
what would run; only a result plus its immutable manifest says what did run.

Two v1 profiles preserve the Phase 0 boundary and carry Phase 1 forward:

- [`reproduction-2026-08-08.json`](../profiles/v1/reproduction-2026-08-08.json)
  reconstructs the exact production artifacts used by the published commit sweep.
  It is `runnable` because all participating artifacts and the shared object-store
  image have immutable digests. Its purpose is historical reproduction; the
  profile itself does not claim a new execution.
- [`current-2026-08-26.json`](../profiles/v1/current-2026-08-26.json) is the input
  pinset for subsequent conformance and performance work. It is `draft`, lists
  every unresolved artifact, and cannot back a result until those artifacts are
  built or downloaded, hashed, and represented in a new runnable profile.

Both target Linux ARM64 and one Docker network. All catalog, client, engine, and
benchmark processes must run in that container environment against the same MinIO
warehouse. Each catalog may have only its necessary private state backend.

## Historical reproduction profile

The 2026-08-08 profile pins:

| Component | Immutable identity |
|---|---|
| Benchmark source/executable | `querygraph/catalog-bench@fbdf684566edb877abca94629ff702c93d6ca2fb`; ELF SHA-256 `c04e3634…` |
| LakeCat source/executable/image | `querygraph/lakecat@3cca8d1c749fcf1c7cbd30661ba2bd4805b256d3`; ELF `56b5081b…`; local image `5f661e70…` |
| LakeCat Sail dependency | `querygraph/sail@6471fb9a82620e046d825219eaad26cd569ed91f` |
| Polaris | 1.5.0 source `da952338…`; image index `03a04f04…` |
| Gravitino | 1.1.0 source `5a6b5ae7…`; image index `906b392c…` |
| Nessie | 0.108.4 source `41d69867…`; image index `c0f42874…` |
| MinIO | `RELEASE.2025-09-07T16-13-09Z` source `01ce918d…`; image index `14cea493…` |
| Runner | Rust 1.96.0 in `rust:1-bookworm`; image index `5e2214ab…` |

The profile also records the two Rust production build recipes, lockfile hashes,
LakeCat's exact Turso crate checksum, endpoints, shared warehouse, and state-backend
choices. Historical registry indexes are sufficient immutable multi-platform
identities even where the report did not retain a separate ARM64 manifest digest.

The raw arithmetic and hashes reproduce, and the historical LakeCat checkout
passes a locked source check. A new Docker timing run was not performed during the
2026-08-26 audit because Docker Desktop's VM reported `no space left on device`.
No images or volumes were deleted without authorization. This limitation belongs
in the imported bundle provenance and must not be rewritten as a live run.

The preserved TSVs have been migrated into an immutable
[`catalog-bench/v1` result bundle](../results/v1/2026-08-08/manifest.json). Its
[generated matrix](../results/v1/2026-08-08/MATRIX.md) ranks only passing results
by concurrent median and records Nessie's assertion failure separately. Both the
JSON records and matrix are reproducibly checked from their source evidence.

## Current candidate profile

Versions were selected from official release records and registry metadata on
2026-08-26:

| Role | Component | Selected identity |
|---|---|---|
| Catalog | LakeCat | `0.3.0-32-gef94b550` / `ef94b5508e94554f51f4764af932cbb819ae3e41` |
| Catalog | Apache Polaris | 1.7.0 / `4ac2f059…`; index `3495f67f…`, ARM64 `53022013…` |
| Catalog | Apache Gravitino | 1.3.0 / `40fdf6ab…`; index `80136ae7…`, ARM64 `01cf367b…` |
| Catalog | Lakekeeper | 0.13.3 / `12bb82fc…`; index `db2ba616…`, ARM64 `ba942413…` |
| Optional catalog | Apache Nessie | 0.108.4 / `41d69867…`; index `c0f42874…`, ARM64 `10d75169…` |
| Client | PyIceberg | 0.11.1 / `8dee48a8…`; CPython 3.13 Linux ARM64 wheel `ddb360da…` |
| Client runtime | CPython | 3.13.15 / `4061bc4c…`; image index `c45a22ea…`, ARM64 `e424b523…` |
| Client data plane | PyArrow | 25.0.1 / `beccec0d…`; CPython 3.13 Linux ARM64 wheel `44a9120c…` |
| Connector | Apache Iceberg Java | 1.11.0 / `6976e020…`; engine JAR hashes unresolved |
| Engine | Apache Spark | 3.5.9 / `7c14a3c2…`; image index `af02a459…` |
| Engine | Apache Spark | 4.1.3 / `77bbf77e…`; image index `bf9d035a…` |
| Engine | Apache Flink | 2.1.3 / `6cda56b0…`; image index `cc557bbe…` |
| Engine | Trino | 483 / `50b0b50b…`; image index `db58cc93…` |
| Engine | DuckDB | 1.5.3 / `14eca11b…`; production artifact unresolved |
| Object store | MinIO | `RELEASE.2025-10-15T17-29-55Z` / `9e49d5e7…`; source-built image unresolved |
| State store | PostgreSQL | 17.11-bookworm; index `051f7b7b…`, ARM64 `b2605730…` |
| Build runner | Rust 1.97.1 bookworm | index `0e2bcaef…`, ARM64 `6e957ef0…` |

LakeCat's identity is its reachable canonical commit after a privacy-only
history rewrite. An isolated pre/post-rewrite comparison verified `Cargo.toml`,
`Cargo.lock`, and the complete `crates/` tree are source-identical at every
affected conformance milestone. The current profile therefore names only
reproducible public history; historical artifact hashes in the C1-04 and C1-05
reports remain unchanged and are labeled as such.

Spark 4.1.3 is the maintained 4.x line selected with Iceberg 1.11; Spark 4.2 is
not silently substituted. Flink 2.1.3 is the newest selected line with the Iceberg
runtime; Flink 2.3 remains an explicit compatibility gap. Community MinIO is
source-only at the selected release, so the final image must be built from that
commit rather than replacing it with `latest`.

The draft's unresolved list is normative: `catalog-bench-commit`,
`catalog-bench-conformance`, `lakecat`, and `duckdb` need optimized production
executables; `minio` needs its source-built runtime image; `iceberg-java` needs
exact engine-specific JAR hashes. The materialization process must:

1. use the pinned Linux ARM64 runner and one Docker environment;
2. build with locked dependencies and the recorded optimization recipe;
3. hash executables before packaging and verify the same bytes in-container;
4. record image index/local-image and platform identities without conflating
   their digest scopes;
5. emit a new `runnable` profile and hash its exact bytes before any measured run.

The candidate also carries the Phase 1 adapter contract: 27 capability
definitions and exhaustive bindings for LakeCat, Polaris, Gravitino, Lakekeeper,
and Nessie. Every binding is protocol-native and cross-checked against its service
endpoint. `exercise` schedules a standard operation; it does not predict a pass.
See [ADAPTERS.md](ADAPTERS.md) for routing, authentication, capability, shim, and
historical-compatibility semantics.

The C1-05 smoke acceptance rebuilt and hashed the candidate conformance runner
and LakeCat production executable, then proved all five catalog metadata paths
against shared MinIO. Those observed local artifacts do not resolve this draft:
C1-09 must materialize them into a new immutable runnable profile and bundle.
Exact hashes and the behavioral matrix are in
[`TABLE-CONFORMANCE.md`](TABLE-CONFORMANCE.md).

The config-negotiation scenario additionally pins the exact Apache Iceberg 1.11.0
OpenAPI bytes at SHA-256
`80d2ec83a70eeff6e7194853f8791c17cceb14610fae6a0e6afdd2921806ee4a`.
The runner accepts only endpoint method/path entries defined by those bytes and
records omission as the specification's implicit default set. OAuth profiles
name environment-variable bindings only; secret values are runtime inputs and
never profile data.

Primary release sources: [Polaris](https://polaris.apache.org/downloads/),
[Gravitino](https://gravitino.apache.org/downloads/),
[Lakekeeper](https://github.com/lakekeeper/lakekeeper/releases),
[Nessie](https://github.com/projectnessie/nessie/releases),
[PyIceberg](https://github.com/apache/iceberg-python/releases),
[CPython](https://www.python.org/downloads/),
[Apache Arrow](https://arrow.apache.org/release/),
[Iceberg](https://iceberg.apache.org/releases/),
[Spark](https://spark.apache.org/news/),
[Flink](https://flink.apache.org/downloads/),
[Trino](https://trino.io/docs/current/release.html), and
[DuckDB](https://github.com/duckdb/duckdb/releases). Image and package digests
come from the corresponding official registry or package index.
