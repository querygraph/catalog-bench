# Commit-path results

Final public sweep: **2026-08-08 (America/Los_Angeles)**. Every request was sent
from one Linux ARM64 Docker container to catalogs on the same Docker network and
the same MinIO instance. Each accepted `set-properties` commit validates the
request, writes a fresh Iceberg `metadata.json` to `s3://warehouse`, and advances
the catalog pointer; there are no data files or query-engine work in this test.

## Concurrent ranking

The table is sorted by raw successful concurrent throughput, as requested. A
numeric rank requires **zero request errors in every measured round**. Nessie's
raw throughput is shown in its sorted position, but it is disqualified rather
than silently treating HTTP 500 responses as conflicts or successes.

| Raw order | Rank | Catalog | Valid rounds | Concurrent, 8 writers | Sequential | p50 | p99 | Conflict rate | Error rate | Errors |
|---:|:---:|---|:---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | **DQ** | Apache Nessie 0.108.4 | 0 / 5 | 190.0/s (173.3–223.8) | 312.3/s (215.9–328.9) | 2.986 ms | 5.602 ms | 81.00% | 0.366% | 97 |
| 2 | **1** | **LakeCat** `3cca8d1c` | **5 / 5** | **153.0/s** (130.0–166.5) | **335.5/s** (285.2–342.6) | **2.697 ms** | **5.641 ms** | 85.42% | **0%** | **0** |
| 3 | **2** | Apache Polaris 1.5.0 | **5 / 5** | 129.1/s (103.0–135.6) | 135.0/s (103.7–153.4) | 7.115 ms | 11.533 ms | 4.04% | **0%** | **0** |
| 4 | **3** | Apache Gravitino 1.1.0 | **5 / 5** | 116.9/s (105.4–126.2) | 74.2/s (63.9–78.0) | 12.838 ms | 19.225 ms | 1.10% | **0%** | **0** |

Values are medians of rounds 2–6; parenthesized values are the measured min–max
range. Throughput counts only accepted commits and uses the phase's actual elapsed
time. The conflict rate is HTTP 409 responses divided by accepted-plus-conflicting
responses. The error rate includes all other request failures.

**LakeCat is the valid concurrent and sequential leader.** Its same-table workload
is also the strictest in the valid set: about 85% of attempts correctly lose the
metadata-pointer CAS. The short per-table gate added at `3cca8d1c` covers only the
final Turso transaction; S3 preparation and commits to different tables remain
parallel. This converts stale same-table writers into deterministic 409 conflicts
instead of leaking `database is locked` as HTTP 500.

## Protocol and raw evidence

- Six interleaved rounds used rotated order to spread warmup and host drift:
  `L/N/G/P`, `N/G/P/L`, `G/P/L/N`, `P/L/N/G`, then the first two orders again.
  Round 1 was conditioning and discarded. The published result is the median of
  rounds 2–6.
- Every run created a unique namespace and table, performed 50 unmeasured warmup
  commits, 1,000 measured sequential commits, then eight same-table writers for
  six seconds.
- Every catalog began the final sweep with fresh private state. LakeCat used a
  file-backed Turso store; Gravitino used file-backed SQLite JDBC; Nessie and
  Polaris used their image defaults. All Iceberg metadata went to the same MinIO.
- A run is valid only when the driver exits zero, records zero request errors, and
  MinIO grows by at least `50 + 1000 + concurrent_ok` objects. All 24 object audits
  passed. LakeCat's object delta exactly equaled that minimum in every round.
- LakeCat idempotency keys include phase and writer scope. Warmup, sequential, and
  concurrent requests therefore cannot become cheap replays of one another.

Tracked evidence:

- [Median summary](results/commit-2026-08-08-summary.tsv)
- [All 24 runs, including discarded round 1](results/commit-2026-08-08-runs.tsv)
- [Per-run MinIO object audit](results/commit-2026-08-08-object-audit.tsv)

The source output hashes are respectively `ce0730e6…`, `6aa5cd51…`, and
`9cdfb8bb…`; the tracked files are byte-for-byte copies.

## Nessie disqualification

Nessie 0.108.4, the latest release at the time of the run, returned HTTP 500 in
all five measured rounds. The server logs consistently identify a Quarkus
`ContextNotActiveException`: asynchronous catalog work accesses request-scoped
`ObjectIO` / `S3ClientSupplier` or `SecurityIdentityProxy` after the request
context is inactive. The relevant upstream producers are explicitly
`@RequestScoped` in [Nessie's 0.108.4 source](https://github.com/projectnessie/nessie/blob/nessie-0.108.4/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java).

This was not a transient version or tuning artifact. Guarded preflights reproduced
it on 0.107.5, 0.107.6, and 0.108.4. Reducing the supported async task pool from 10
threads to one reduced but did not eliminate failures; setting the task minimum
delay to zero did not eliminate them either. Zero race waits made table creation
invalid. No patched or unreleased Nessie build is substituted in the public table.

The driver still exits nonzero on these runs. It now emits its complete report
first, allowing the raw 190.0 successful commits/s and 0.366% median error rate to
be published as disqualified evidence instead of disappearing.

## Why the previous public rows were replaced

The earlier LakeCat 287.8/s, Gravitino 272.6/s, and retained comparison rows are
not comparable to this sweep and must not be reused:

1. The old concurrent driver discarded request errors, which hid Nessie's HTTP
   500s and LakeCat's Turso busy failures.
2. LakeCat's old idempotency counters overlapped between sequential traffic and
   concurrent writer 0, so part of the concurrent phase measured cheap replay.
3. Gravitino's `memory` backend acknowledged commits and returned S3 metadata
   locations while writing zero objects. The final run uses its bundled
   file-backed SQLite JDBC backend; `jdbc:sqlite::memory:` is also invalid because
   each pooled connection receives an isolated database.
4. A single retained row per catalog amplified run-order and warmup effects. The
   final protocol uses fresh state, rotated order, a discarded conditioning round,
   and five-round medians.

## Exact production artifacts

| Component | Immutable source or image | Artifact |
|---|---|---|
| Benchmark driver | `catalog-bench@fbdf684566edb877abca94629ff702c93d6ca2fb` | stripped ARM64 ELF, 2,626,432 bytes, SHA-256 `c04e363420ae8152a229ad4e12e126b28a18deb056c976e4e9af48a6ced75139` |
| LakeCat | `lakecat@3cca8d1c749fcf1c7cbd30661ba2bd4805b256d3` | stripped ARM64 ELF, 19,494,560 bytes, SHA-256 `56b5081b82aab567eede1b42fbd6e5f4a767d992eff3c0b29915a7b79d076617` |
| LakeCat runtime | source ELF packaged without recompilation | image `sha256:5f661e70cd67f7c4eb720c2eb030b6373b49a1b7c9b86a25796d98547020ad06` |
| Nessie 0.108.4 | official `0.108.4-java` ARM64 image | `sha256:c0f42874c810f28ac30fc991e979c1b8cf5a2cbfa94212086cdddeae49629517` |
| Polaris 1.5.0 | official ARM64 image | `sha256:03a04f0459948da3977f7ea2ad2fb9ea672b2b503ec409c89c2934d400d71c67` |
| Gravitino 1.1.0 | official ARM64 image; bundled jars report 1.1.0 | `sha256:906b392c22df95bb3a26085e97a96d2ada3db570c2b40b630f130fa6e1c6648b` |
| MinIO | shared official ARM64 image | `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e` |
| Build/runner | `rust:1-bookworm`, Rust 1.96.0, 10 ARM64 CPUs, 7.8 GiB RAM | `sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc` |

The driver lockfile SHA-256 is `5c9c924c…`; LakeCat's is `2c580d64…`.
Both Rust executables were built in that same runner with locked dependencies,
`-Ctarget-cpu=native`, optimization level 3, fat LTO, one codegen unit, stripped
symbols, panic abort, debug disabled, and incremental compilation disabled. The
LakeCat feature set was `turso-local,sail-local`. MinIO was audited with `mc`
`RELEASE.2025-08-13T08-35-41Z`, binary SHA-256 `14c8c961…`.

Production build commands (both executed in `querygraph-bench-runner`):

```sh
cd /src/catalog-bench
CXXFLAGS= RUSTFLAGS="-Ctarget-cpu=native" \
  CARGO_TARGET_DIR=/target/catalog-bench-public-final \
  CARGO_PROFILE_RELEASE_OPT_LEVEL=3 \
  CARGO_PROFILE_RELEASE_LTO=fat \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
  CARGO_PROFILE_RELEASE_DEBUG=false \
  CARGO_PROFILE_RELEASE_STRIP=symbols \
  CARGO_PROFILE_RELEASE_PANIC=abort \
  CARGO_PROFILE_RELEASE_INCREMENTAL=false \
  cargo build --locked --release \
    -p catalog-bench-commit --bin catalog-bench-commit -j1

cd /src/lakecat
CXXFLAGS= RUSTFLAGS="-Ctarget-cpu=native" \
  CARGO_TARGET_DIR=/target/lakecat-public-final \
  CARGO_PROFILE_RELEASE_OPT_LEVEL=3 \
  CARGO_PROFILE_RELEASE_LTO=fat \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
  CARGO_PROFILE_RELEASE_DEBUG=false \
  CARGO_PROFILE_RELEASE_STRIP=symbols \
  CARGO_PROFILE_RELEASE_PANIC=abort \
  CARGO_PROFILE_RELEASE_INCREMENTAL=false \
  cargo build --locked --release \
    -p lakecat-service --bin lakecat-service \
    --features turso-local,sail-local -j1
```

The second ELF was copied directly into the runtime image and its in-container
SHA-256 was verified before acceptance and the final sweep. No debug or development
binary appears in the published data.

## How LakeCat got here (0.1.1 → `3cca8d1c`)

Six changes took LakeCat's S3 commit p50 from the historical **12.6 ms** to a
**2.697 ms** five-round median without removing its audit, outbox, idempotency, or
pointer-CAS guarantees:

1. **Turso MVCC concurrent writes.** 0.1.1 serialized every write through one
   per-store async mutex, so 8 concurrent commits effectively ran one-at-a-time
   (38.5 /s, 85% conflict). 0.2.0 uses `journal_mode=mvcc` + `BEGIN CONCURRENT` with
   bounded retry: different-table commits run in parallel and same-table races
   converge to the metadata-pointer CAS.
2. **Cache the object-store client.** LakeCat rebuilt the S3 client — credential
   chain, HTTP client, a *fresh connection with no keep-alive* — on every commit. A
   MinIO request trace showed ~1 PutObject/commit at ~1.7 ms server-side, so most of
   the old ~12 ms was per-commit client setup. Caching one client per bucket cut
   sequential p50 12.6 → 6.8 ms.
3. **Pool the write connection.** `write_txn` opened a new Turso connection and
   re-applied the MVCC pragmas on every commit. Pooling pragma-warmed connections
   still gives concurrent writers distinct connections, preserving MVCC.
4. **Pool read connections.** Commit validation performs several small catalog
   reads. Reusing those connections removes repeated connection setup without
   changing transaction boundaries.
5. **Bound busy retries, then gate only same-table final transactions.** Retrying
   Turso's busy/write-conflict/dependency-abort cases handles transient contention.
   A keyed weak-reference mutex around the final transaction prevents a continuous
   same-table writer stream from exhausting that retry budget. Distinct tables and
   S3 preparation remain parallel, and stale pointers still return 409.
6. **Sail as a git dependency.** LakeCat builds Sail from `querygraph/sail`'s
   `lakecat` branch (metadata evolution + planning helpers), so the benchmark image
   is reproducible without a local Sail checkout.

(Getting an *honest* baseline in the first place required making the default build
write a real `metadata.json` per commit — see History below; before that, the
"303 /s, 0 objects" figure was the catalog doing no metadata work at all.)

## Audit and idempotency

LakeCat's accepted commit advances the metadata pointer with compare-and-swap and
records pointer history, an audit event, a transactional-outbox entry, and an
idempotency result in the same embedded-store transaction. Namespace/table reads
validate the request first. A repeated key replays the prior result rather than
double-applying it; the final driver uses disjoint keys for every phase and writer.

The competitors do not expose an identical feature set or private-state backend,
so the ranking should not be read as a pure language comparison. It is the measured
cost of each released/configured catalog performing the common Iceberg REST unit of
work while retaining its own semantics.

### Why "Rust" did not make it fast (and why that is fine)

The commit path is substantially **I/O-bound**: an accepted commit includes a
network object write plus private-state work. Runtime CPU speed therefore explains
less than connection reuse, object-store reuse, transaction setup, and contention
behavior.

What actually made LakeCat slow at first (12.6 ms p50) was **missing connection
reuse** — rebuilding the S3 client and opening a new store connection on every commit
— the boring pooling the JVM data ecosystem standardized decades ago, which a young
Rust project simply had not done yet. Fixing it closed the gap (see *How LakeCat got
here*). And a 1000-commit loop against a warm, long-running server is the **JVM's
best case**: JIT-compiled hot paths and warm connection pools shine, while its real
weaknesses — cold start and memory footprint — never appear.

Cold start, resident memory, and GC behavior are outside this benchmark. They need
separate measurements rather than being inferred from the warm commit loop.

## Notes on fairness

- **Turso is LakeCat's catalog-state store, not table data.** It holds the
  metadata pointer, pointer log, idempotency, audit, and outbox rows — the
  analogue of Polaris's metastore, Nessie's version store, and Gravitino's
  backend. Gravitino specifically uses file-backed SQLite JDBC here. The Iceberg
  `metadata.json` itself goes to S3/MinIO for every accepted commit.
- **The concurrent column combines policy and speed.** LakeCat's 85.42% median
  conflict rate is much stricter than Polaris's 4.04% or Gravitino's 1.10%, so
  successful throughput is not a direct measure of equivalent acceptance policy.
- **Nessie's raw numbers are diagnostic only.** Its 81.00% median conflict rate is
  valid to report, but the additional HTTP 500 responses make it rank-ineligible.

## History: why the first LakeCat run was wrong (303 /s, 0 objects)

The initial run reported 303 /s and 0% conflicts because the default LakeCat
build **never wrote a `metadata.json`** — its `set-properties` commit only did a
Turso pointer CAS. Verified by MinIO object counts (Polaris/Nessie/Gravitino
wrote 1500–1700 objects; LakeCat wrote 0). Getting an honest number took three
fixes, in order:

1. **Sail `TableUpdate`/`ViewUpdate` discriminator** (lakehq/sail#2134) — the
   generated REST model was a flat all-required struct, so any real update
   failed to deserialize (`missing field uuid`). Now a tagged enum.
2. **Sail applies the updates** (`apply_table_updates`) + `lakecat-sail`
   `prepare_commit` rewrite — evolve the current metadata by the typed updates,
   emit a fresh `metadata.json` + new location, write it to S3, advance the
   pointer. This is what put LakeCat on equal footing.
3. **Turso write serialization** (0.1.1) — single-writer file + 8 concurrent
   commits = `database is locked`; first serialized via a per-store async mutex,
   then superseded by MVCC concurrent writes in 0.2.0.

# cache-scan results — Sail's Foyer object-store cache (cold vs warm)

A separate read-path benchmark (`catalog-bench run cache-scan`, status **Ready**)
measures Sail's new Foyer object-store cache
(`sail_object_store::CachingObjectStore`, branch `feat/object-store-foyer-cache`).
It writes a Parquet dataset to MinIO once, then fully scans every file — decoding
all row groups into Arrow `RecordBatch`es and counting rows + bytes — three ways:

- **no-cache** — read through the raw `AmazonS3` store (no cache).
- **cold** — wrap the raw store in a *fresh* `CachingObjectStore` (empty cache) and
  read once: each page is fetched from MinIO and cached.
- **warm** — read again through the *same*, now-populated cache: Foyer in-memory hits.

The reader is the `parquet` async reader (`ParquetObjectReader` +
`ParquetRecordBatchStreamBuilder`) over the object store directly, pinned to the
exact `object_store 0.13.2` / `parquet`+`arrow 58.3.0` Sail's cache layer uses, so
every byte routes through `CachingObjectStore`.

**Dataset:** 16 Parquet files × 200,000 rows (id `i64`, two measures `i64`/`f64`, a
low-cardinality `grp` string), ~5.4 MB/file, **86.9 MB / 3.2 M rows total**; default
`CacheConfig` (1 MiB pages, 1 GiB memory, 64 MiB metadata).

**Measured (live MinIO at `127.0.0.1:9000`, per-file p50/p95):**

| phase | per-file p50 | per-file p95 | throughput |
|---|---|---|---|
| no-cache (raw S3) | **47.7 ms** | 49.8 ms | 113 MB/s · 4.2 M rows/s |
| cold (fresh cache) | **47.5 ms** | 48.7 ms | 114 MB/s · 4.2 M rows/s |
| warm (Foyer hits) | **1.81 ms** | 2.09 ms | 2960 MB/s · 109 M rows/s |

**Speedup: warm is ~26× faster than cold and ~26× faster than no-cache** (warm
p50 1.81 ms vs cold 47.5 ms). Cold ≈ no-cache (cold pays the MinIO fetch to fill
the cache), confirming the win comes from cache hits, not the wrapper. The cache
**engaged** — `warm ≪ cold` is the hit/miss signal (the layer exposes no public
hit counter, so the latency collapse is the proof).

**Caveat — local MinIO understates the win.** On loopback, per-request latency is
sub-millisecond, so "cold" object reads are already cheap; the 26× warm speedup is
a *lower bound*. Against remote S3 (tens of ms per request, multiplied across the
many small range reads a Parquet scan issues for footers/row-group columns), the
warm-vs-cold advantage is dramatically larger.

Reproduce:

```sh
cd ~/src/catalog-bench
cargo run -p catalog-bench-cache-scan --release       # direct
cargo run -q -p catalog-bench -- run cache-scan --release   # via the driver
# knobs: --files --rows --row-group --no-cache-iters --warm-iters --rewrite
```

# rust-vs-jvm results — Sail/DataFusion (Rust) vs Spark (JVM)

A real **engine-vs-engine** read comparison (`catalog-bench run rust-vs-jvm`,
status **Ready**): **Sail's engine, DataFusion (Rust)**, vs **Apache Spark 3.5.3
(JVM)**, running the *same* filter+aggregate over the *same* Parquet files in the
*same* MinIO. Both engines scanned **3,200,000 rows into 8 groups** — verified equal
on both sides, so this is genuinely the identical query over identical bytes.

**The query (identical on both engines):**

```sql
SELECT grp, count(*) AS n, sum(measure_a) AS s1, avg(measure_b) AS a2
FROM cache_scan WHERE measure_a > 0 GROUP BY grp ORDER BY grp
```

It reuses the **cache-scan dataset** (`s3://warehouse/cache-scan/`, 16 Parquet ×
200k rows, ~87 MB). `measure_a` is always `>= 0`, so `measure_a > 0` keeps ~every
row: the query is **scan-bound**, not pruning-bound — it stresses scan + aggregate,
not predicate selectivity.

- **Rust side** — **DataFusion 54.0.0** (the engine inside Sail) registers the
  Parquet directory over `object_store` and runs the query via the DataFrame API.
  It shares **one** `object_store 0.13.2` (+ `parquet`/`arrow 58.3.0`) with Sail's
  `CachingObjectStore`, so the same Foyer cache used by `cache-scan` serves the warm
  phase. (DataFusion's SQL frontend is feature-disabled to mirror Sail's exact
  datafusion feature set; the DataFrame plan is logically identical to the SQL.)
- **JVM side** — Spark reads `s3a://warehouse/cache-scan/` via the Hadoop S3A
  connector (`hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262`, path-style, SSL
  off, `SimpleAWSCredentialsProvider`), reaching the host MinIO at
  `host.docker.internal:9000` from an `apache/spark:3.5.3` container. The query runs
  **N+1× in one long-lived session**; JVM startup + JIT + the cold first run are
  **discarded**, and the **warm steady-state** median/p95 is reported — the JVM's
  best case.

**Measured (live MinIO at `127.0.0.1:9000`; whole-query p50/p95):**

| phase | what | p50 | p95 | vs Spark-warm |
|---|---|---|---|---|
| **jvm-warm** | Spark 3.5.3, steady-state warm (S3A, no local cache) | **728.6 ms** | 889.0 ms | 1.00× (baseline) |
| **rust-no-cache** | DataFusion over raw MinIO (no local cache) | **446.1 ms** | 575.8 ms | **1.63× faster** |
| **rust-cold** | DataFusion, fresh Foyer cache (fills from MinIO) | **545.1 ms** | 545.1 ms | **1.34× faster** |
| **rust-warm** | DataFusion, warm Foyer cache (RAM hits) | **12.7 ms** | 14.4 ms | **57.5× faster** |

**The honest engine-to-engine number is rust-no-cache vs jvm-warm:** both re-read
every Parquet byte from MinIO on each query with **no local byte cache**, isolating
scan+aggregate efficiency. There DataFusion is **~1.63× faster** than Spark-warm —
a real but modest edge; the query is largely network-bound (87 MB over loopback
S3/S3A each run), and Spark also pays per-file task scheduling + S3A overhead.

**`rust-warm`'s 57× is NOT a language win** — it is Sail's Foyer object-store byte
cache (served from local RAM), which this Spark setup has **no equivalent of**
(Spark re-reads S3 on every query). Read it as *Sail-with-its-cache vs
Spark-without-one*. It mirrors the cache-scan result: the Foyer layer, not the
runtime, collapses the latency.

## Why the warm numbers look the way they do (fairness)

This mirrors the framing in *"Why Rust did not make it fast"* above. A warm,
long-lived, steady-state query is the **JVM's best case**: JIT-compiled hot paths
and warm connection pools shine, while the JVM's real weaknesses — **cold start**
and **memory footprint** — never appear (we deliberately excluded JVM startup, JIT
warmup, and the cold first scan from `jvm-warm`). On the apples-to-apples
no-cache scan both engines are mostly **waiting on the same 87 MB of S3 bytes**, so
DataFusion's 1.63× is a moderate scan-efficiency edge, not an order-of-magnitude
"Rust beats Java." Where Rust keeps a durable advantage is exactly what a warm
steady-state hides: **no GC pauses** (steadier tail latency), a far smaller
**resident footprint**, and **instant cold start** — the JVM startup + warmup we
discarded here is real cost in serverless / edge / many-tenant-per-host deployments.
And on a **local loopback MinIO** the network term is tiny; against remote S3 both
cold numbers grow and the Foyer-cache (`rust-warm`) advantage grows much larger.

Reproduce:

```sh
cd ~/src/catalog-bench
# Rust phases only (no Docker needed):
cargo run -p catalog-bench-rust-vs-jvm --release -- --skip-jvm
# Full head-to-head (needs Docker; first run downloads hadoop-aws via --packages):
cargo run -q -p catalog-bench -- run rust-vs-jvm --release
# JVM phase alone (debug / container recipe):
S3_ENDPOINT=http://host.docker.internal:9000 S3_PATH=s3a://warehouse/cache-scan/ \
  crates/rust-vs-jvm/run-spark.sh
```

If Docker is unavailable the bench stays honest: it reports the Rust phases and
marks the JVM phase **requires-container**, embedding the exact `run-spark.sh`
recipe in its `notes` rather than fabricating a Spark number.

# read-write results — stock-client Iceberg write→read round-trip (PROVEN) + Foyer read path

`catalog-bench run read-write` (status **Ready**) is an end-to-end round-trip
through the live LakeCat Iceberg REST catalog + MinIO. Its headline job is to answer
one question honestly — *does a **stock** Iceberg client's write path actually work
through LakeCat?* — and after five fixes landed together, the answer is now **yes**.

## PHASE 0 — does stock-client Iceberg write work through LakeCat? **Yes — full round-trip.**

A **raw, stock `pyiceberg 0.11.1` `RestCatalog`** (run with `SHIM=0` — no
response-rewriting, a genuinely stock client) completes the entire Iceberg
write→read against LakeCat:

```
RestCatalog init (GET /v1/config)  →  create_namespace  →  create_table
  →  table.append(arrow)   ← a REAL Iceberg snapshot append
  →  load_table            ← snapshots after append: 1
  →  table.scan()          ← scan row count: 1000
```

The `stock-roundtrip` phase the bench records:

| field | value |
|---|---|
| `status` | **ok** |
| `snapshots_after` | **1** (a fresh table; the append created exactly one snapshot) |
| `rows_scanned` | **1000** (the appended rows read back via `table.scan()`) |
| client | stock pyiceberg 0.11.1 `RestCatalog`, `SHIM=0` (no rewriting) |

This is enabled by **five fixes** landed together:

- **LakeCat** (`master`): **H8** — `GET /v1/config` now serializes `defaults` /
  `overrides` as JSON **objects** (was arrays of `{key,value}`, which a stock client
  could not parse); **canonical `{prefix}` endpoint advertisement** (was a baked-in
  `/catalog` base + `{warehouse}`); **listTables**; and **H9**.
- **Sail** (`querygraph/sail#lakecat`, @ `bddb1706`): `apply_table_updates` now
  handles **`add-snapshot`** + **`set-snapshot-ref`** (the snapshot-registration that
  was previously rejected with `apply_table_updates: add-snapshot`), plus the **Foyer**
  caching object store.

Before these fixes a stock client could not even parse `/v1/config`, and a snapshot
append was rejected HTTP 400 with `This feature is not implemented: TableUpdate not
yet supported by apply_table_updates: add-snapshot`. The bench drives this phase by
shelling out (the same pattern `rust-vs-jvm` uses for Spark) to
`crates/read-write/stock-append-probe.py` with `SHIM=0`, on the crate's pinned
`.venv` pyiceberg; it scrapes the helper's `ROUNDTRIP_RESULT {json}` line. It stays
**honest**: against an old catalog the append is rejected and the phase records
`status: gated` with the exact reason rather than faking a snapshot. Reproduce the
round-trip standalone: `crates/read-write/stock-append-probe.py` with `SHIM=0`.

> **Note — `sail-local` LakeCat.** The proven round-trip runs against a `sail-local`
> LakeCat binary (Sail's Iceberg format/commit/scan compiled in), bound to
> `127.0.0.1:8183` and pointed at MinIO. The default-features Docker image on `:8181`
> predates these fixes.

## PHASE 1/2 — bulk write + Foyer read path (still measured)

Alongside the stock round-trip, the bench keeps a high-volume bulk write + a
filtered Sail/DataFusion read that exercises the Foyer cache (the single small stock
append does not stress these):

- **WRITE** — write N real Parquet data files (cache-scan column shape
  `id i64, measure_a i64, measure_b f64, grp string`) to `s3://warehouse/read-write/`
  via `object_store`, each paired with an **accepted LakeCat `set-properties`
  commit** (validation → a fresh **durable `metadata.json`** on S3 → the
  metadata-pointer CAS) — the same accepted catalog mutation the `commit` /
  `write-data` benches measure.
- **READ** — a filtered scan `WHERE measure_a > <median>` over those files via
  **DataFusion** (the engine inside Sail), every byte routed through Sail's Foyer
  `CachingObjectStore`, reported **no-cache** / **cold** / **warm**.

**Measured (live `sail-local` LakeCat + MinIO at `127.0.0.1:8183`, 16 files × 200k
rows = 3.2 M rows / 86.9 MB; filter kept 1,600,533 rows ≈ 50%):**

| phase | samples | p50 | p95 | throughput |
|---|---|---|---|---|
| **stock-roundtrip** (pyiceberg init→create→append→scan) | 1 | **~0.9 s** | — | `snapshots_after=1`, `rows_scanned=1000` |
| data-write (Parquet → MinIO) | 16 files | **59.3 ms/file** | 67.5 ms | ~57 MB/s · 10.4 files/s |
| catalog-commit (set-properties, accepted) | 16 | **5.99 ms** | 8.5 ms | — |
| read-no-cache (raw S3) | 3 | **225.1 ms** | 227.8 ms | 7.2 M rows/s |
| read-cold (fresh Foyer cache) | 1 | **546.8 ms** | — | 2.9 M rows/s |
| read-warm (populated Foyer cache) | 5 | **3.9 ms** | 4.3 ms | 413 M rows/s |

Reading *within* the run: the accepted **catalog commit p50 (~6 ms)** matches the
commit-path bench's LakeCat number (~5.3 ms) — the same set-properties machinery.
The **filtered read** warms dramatically: **warm is ~150× faster than cold** and
**~62× faster than no-cache** at p50 (cold pays the one-time cache fill, so cold >
no-cache; warm then serves the whole filtered scan from Foyer RAM). As with the other
read benches this is a **lower bound** — loopback MinIO has tiny per-request latency,
so the cache win is far larger against remote S3.

## Status

`read-write` is **Ready**. The headline result is now **positive**: a raw stock
pyiceberg client completes a full Iceberg write+read through LakeCat (init → create →
append a real snapshot → scan it back; `snapshots_after=1`, `rows_scanned=1000`), and
the bulk write + Foyer read path is measured alongside it. The previously-gated
`apply_table_updates: add-snapshot` path is fixed in Sail's `querygraph/sail#lakecat`
branch and the stock-client config/endpoint breaks are fixed in LakeCat `master`.

Reproduce:

```sh
cd ~/src/catalog-bench
# stock round-trip needs the crate's pyiceberg venv (one-time):
#   cd crates/read-write && python3.12 -m venv .venv && .venv/bin/pip install "pyiceberg[pyarrow,s3fs]"
# run a sail-local LakeCat on :8183 pointed at MinIO, then:
LAKECAT_BASE=http://127.0.0.1:8183/catalog \
AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=password AWS_REGION=us-east-1 \
  cargo run -p catalog-bench-read-write --release            # direct
# knobs: --files --rows --row-group --no-cache-iters --warm-iters --namespace --table --prefix
# stock-client round-trip standalone: SHIM=0 crates/read-write/.venv/bin/python crates/read-write/stock-append-probe.py
```

## Reproduce

```sh
# 1. shared catalog stack + MinIO + network (from ~/src/boat)
cd ~/src/boat && docker compose up -d minio nessie gravitino polaris

# 2. build LakeCat from source, deploy its image, and bench every reachable catalog
cd ~/src/catalog-bench && ./bench-stack.sh
```

`bench-stack.sh` builds `lakecat-service` for Linux (Sail fetched from the
`querygraph/sail` git dep), packages + restarts the container, ensures the MinIO
`warehouse` bucket, and runs the identical `--create` + commit measurement against
each reachable catalog (LakeCat with `--location s3://warehouse/lakecat`). Polaris
is auto-bootstrapped via `polaris-bootstrap.sh` (OAuth2 token + an S3 catalog on the
same `warehouse` bucket); set `POLARIS_TOKEN` to skip the bootstrap.

That command is the one-round smoke path and correctly stops on request errors.
To reproduce the public table, use the six-round rotated protocol, fresh private
state, production build profile, and validity rules recorded at the top of this
file; do not average or retain rows from the older one-shot results.

## Not measured

- **Unity Catalog OSS** — *cannot* be benchmarked on the commit path yet. Released
  Unity OSS (latest **0.5.0**) exposes its Iceberg REST endpoint
  (`/api/2.1/unity-catalog/iceberg`) as **read-only** — it has no external
  `updateTable` / `set-properties` commit handler, so there is nothing to measure on
  this benchmark's axis. Commit support is implemented only in **unmerged draft PR
  [#1618](https://github.com/unitycatalog/unitycatalog/pull/1618)** ("Implement
  Iceberg REST catalog write endpoints"), targeting an unreleased **0.6.0**. To
  include Unity, build the image from that branch (or wait for a 0.6.0 release) and
  add it to `bench-stack.sh`; the compose file already carries a `unity` profile for
  when that lands. (Databricks-hosted Unity Catalog has Iceberg REST writes, but that
  is a separate product, not the Docker-deployable OSS server.)
