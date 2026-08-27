# Versioned profiles and adapter policy

Profiles separate environment selection from benchmark results. A profile says
what would run; only a result plus its immutable manifest says what did run.

Three v1 profiles preserve the Phase 0 boundary and carry Phase 1 forward:

- [`reproduction-2026-08-08.json`](../profiles/v1/reproduction-2026-08-08.json)
  reconstructs the exact production artifacts used by the published commit sweep.
  It is `runnable` because all participating artifacts and the shared object-store
  image have immutable digests. Its purpose is historical reproduction; the
  profile itself does not claim a new execution.
- [`current-2026-08-26.json`](../profiles/v1/current-2026-08-26.json) is the input
  pinset for subsequent conformance and performance work. It is `draft`, lists
  every unresolved artifact, and cannot back a result until those artifacts are
  built or downloaded, hashed, and represented in a new runnable profile.
- [`contention-2026-08-27.json`](../profiles/v1/contention-2026-08-27.json) is
  the generated, `runnable` Linux ARM64 performance profile for the same-table
  contention v2 scenario only. It retains all five catalog adapters but removes
  unrelated client and engine components, and replaces the runner, MinIO, and
  LakeCat source-build placeholders with the exact observed local images and
  embedded production executables.

All three target Linux ARM64 and one Docker network. All catalog, client, engine,
and benchmark processes must run in that container environment against the same
MinIO warehouse. Each catalog may have only its necessary private state backend.

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

Versions were initially selected from official release records and registry
metadata on 2026-08-26. LakeCat was advanced on 2026-08-27 to the exact
contention-recovery revision selected for the fresh production rerun:

| Role | Component | Selected identity |
|---|---|---|
| Catalog | LakeCat | `0.3.0-42-g962f43cb` / `962f43cb2d2f345addf188e63be0cf6059bc26b0` |
| Catalog | Apache Polaris | 1.7.0 / `4ac2f059…`; index `3495f67f…`, ARM64 `53022013…` |
| Catalog | Apache Gravitino | 1.3.0 / `40fdf6ab…`; index `80136ae7…`, ARM64 `01cf367b…` |
| Catalog | Lakekeeper | 0.13.3 / `12bb82fc…`; index `db2ba616…`, ARM64 `ba942413…` |
| Optional catalog | Apache Nessie | 0.108.4 / `41d69867…`; index `c0f42874…`, ARM64 `10d75169…` |
| Client | PyIceberg | 0.11.1 / `8dee48a8…`; CPython 3.13 Linux ARM64 wheel `ddb360da…` |
| Client runtime | CPython | 3.13.15 / `4061bc4c…`; image index `c45a22ea…`, ARM64 `e424b523…` |
| Client data plane | PyArrow | 25.0.1 / `beccec0d…`; CPython 3.13 Linux ARM64 wheel `44a9120c…` |
| Client S3 data plane | S3FS | 2026.7.0 / `609950a6…`; universal wheel `64edf3c0…` |
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

The C1-08 contention runner source is pinned to
`e5345a260a42148aa5cd1044fb3f43acfc2232d2`. The production Docker recipe embeds
that revision at compile time, resolves the same public commit as its immutable
build context, and lets the CLI check it, Linux, and ARM64 before credential
access or network I/O. The scenario-scoped materialization below resolves this
artifact without pretending that the broader current candidate is runnable for
conformance, stock clients, or engines whose artifacts remain unresolved.

The LakeCat image independently resolves the exact public Git commit named by
the profile, uses that immutable tree as its Docker build context, and labels
the resulting image with the same revision. The evidence launcher additionally
requires a new run ID and fresh run-scoped Turso, PostgreSQL, SQLite, and MinIO
volumes, preventing durable state from an earlier diagnostic sweep from entering
a current measurement.

The candidate also carries the Phase 1 adapter contract: 36 capability
definitions and exhaustive bindings for LakeCat, Polaris, Gravitino, Lakekeeper,
and Nessie. Every binding is protocol-native and cross-checked against its service
endpoint. `exercise` schedules a standard operation; it does not predict a pass.
See [ADAPTERS.md](ADAPTERS.md) for routing, authentication, capability, shim, and
historical-compatibility semantics.

The C1-05 smoke acceptance rebuilt and hashed the candidate conformance runner
and LakeCat production executable, then proved all five catalog metadata paths
against shared MinIO. Those observed local artifacts do not resolve this draft:
the contention profile uses a later production build and applies only to the v2
contention scenario. C1-09 has now wrapped the accepted C110 transcript in an
[immutable result bundle](../results/v1/2026-08-27/manifest.json), with its
[generated matrix](../results/v1/2026-08-27/MATRIX.md), exact source evidence,
environment capture, and reviewed failure attribution.
Exact hashes and the behavioral matrix are in
[`TABLE-CONFORMANCE.md`](TABLE-CONFORMANCE.md).

The config-negotiation scenario additionally pins the exact Apache Iceberg 1.11.0
OpenAPI bytes at SHA-256
`80d2ec83a70eeff6e7194853f8791c17cceb14610fae6a0e6afdd2921806ee4a`.
The runner accepts only endpoint method/path entries defined by those bytes and
records omission as the specification's implicit default set. OAuth profiles
name environment-variable bindings only; secret values are runtime inputs and
never profile data.

The published 2026-08-08 bundle continues to reference the byte-identical
same-table-contention v1 scenario. Current performance work uses the separately
versioned v2 scenario, which binds the common workload to profile routing,
run-owned MinIO evidence, cleanup, sanitization, repeated rounds, and generated
median-with-range aggregation without changing historical inputs. The published
2026-08-27 C110 bundle is the first immutable result bundle backed by that v2
scenario and the runnable profile below.

## Runnable contention profile

The runnable profile is generated, not hand-edited. Its two authoritative inputs
are the broad current candidate and the audited image observation sidecar
[`contention-2026-08-27.json`](../materializations/v1/contention-2026-08-27.json).
The sidecar binds source profile bytes, output identity, Linux/ARM64 platform,
stable Compose labels, exact source labels, local image IDs, and every executable
used by the scenario. Its SHA-256 is
`005e114fd696a89b4031a6ef1599ae4f0cd3a524e8a695ce7acdc3badb8adf1f`;
the source profile SHA-256 is
`648d02ec4df5faceeca95d60feb896a5598ff87f075ce77ab633ed580f594465`.

The materialized runtime identities are:

| Component | Local image SHA-256 | Embedded production artifact(s) |
|---|---|---|
| Contention runner | `79a83b934d72a2e6ea697cb514211afd6650cccfaa5619ddbd9aa30cd0f46236` | `catalog-bench-commit`: `470d706fdccd1f66cfcb3f98b2ce3b4600e63fc623d4b4c1ed405bbe61359813`, 4,723,816 bytes |
| LakeCat | `f10c056cd9c9534bdc4b9547c89501c44ebe9a0460cd2ed71440ef2fb061e41d` | `lakecat-service`: `ca2e4b6f456f139855f445eb447c810401b88211dd43946034af9f79321ad6f5`, 19,625,632 bytes |
| MinIO and typed helpers | `6ed436d0b5030603da533ab6747c01451cdd890e75e4cee7169efe476838cd5b` | `minio`: `16020fd2829fb8f23b29b2d108b35bfecfd73aa9ada05d499939bfb59abbe582` (105,251,000 bytes); `ensure-bucket`: `8152050fbe456b964902f547e5c2b38fe3f0503944aaf0d4383441a67d9606dd` (7,078,072); `healthcheck`: `63108d653e6e6e8c152973b43b16c0ebb30066bc603759ef99311e0729669dd8` (6,553,784); `lakekeeper-setup`: `0d140420e2775f78251955340b04195874f303062a90a1543b7713b082f7f107` (6,750,392); `polaris-setup`: `7109f6ec37d62f64488e30fcfbf46ca8e89372e8528dbc934352abb6f606c3f0` (6,750,392); `wait-http`: `2069bfb8047ac2e0a41a23d1328be009a1c543ec4d8c76ff0d9b1b3da1d8032c` (6,553,784) |

The output profile SHA-256 is
`8d63c1d74c6761b4f46724807eef8f3edaf8780ae0ba45eb9116662ff632741d`.
It retains exactly ten scenario components and all five catalog adapters. A
`local-image` digest deliberately identifies the locally exported OCI config and
layers; it is not mislabeled as an image-index or platform-manifest digest.

Regenerate or verify the derivation with stable Rust:

```sh
cargo run -p catalog-bench-contract -- profile materialize-contention \
  --source-profile profiles/v1/current-2026-08-26.json \
  --materialization materializations/v1/contention-2026-08-27.json \
  --output profiles/v1/contention-2026-08-27.json

cargo run -p catalog-bench-contract -- profile check-contention \
  --source-profile profiles/v1/current-2026-08-26.json \
  --materialization materializations/v1/contention-2026-08-27.json \
  --output profiles/v1/contention-2026-08-27.json
```

The production launcher performs a second, independent runtime gate after every
build. [`verify-contention-artifacts.sh`](../docker/verify-contention-artifacts.sh)
compares the profile projection with the sidecar, checks each actual Docker image
ID, platform, and recorded label, then copies every selected executable out of a
stopped container and verifies its byte count and SHA-256. A matching profile is
therefore necessary but not sufficient: locally rebuilt image and ELF drift also
abort before any measured service starts.

Primary release sources: [Polaris](https://polaris.apache.org/downloads/),
[Gravitino](https://gravitino.apache.org/downloads/),
[Lakekeeper](https://github.com/lakekeeper/lakekeeper/releases),
[Nessie](https://github.com/projectnessie/nessie/releases),
[PyIceberg](https://github.com/apache/iceberg-python/releases),
[CPython](https://www.python.org/downloads/),
[Apache Arrow](https://arrow.apache.org/release/),
[S3FS](https://pypi.org/project/s3fs/),
[Iceberg](https://iceberg.apache.org/releases/),
[Spark](https://spark.apache.org/news/),
[Flink](https://flink.apache.org/downloads/),
[Trino](https://trino.io/docs/current/release.html), and
[DuckDB](https://github.com/duckdb/duckdb/releases). Image and package digests
come from the corresponding official registry or package index.
