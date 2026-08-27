# Stock Engine Interoperability

Phase 2 compares interoperability, not query-engine speed. The first authority is
the versioned
[`engine.iceberg.write-read-evolution`](../scenarios/v1/engine.iceberg.write-read-evolution.json)
scenario. A pinned stock engine must execute one identical semantic workflow
through a profile-selected Iceberg REST catalog and the shared MinIO data plane.
Spark is implemented first; Flink and Trino must consume the same scenario and
produce the same evidence contract.

The checked-in scenario defines the boundary before execution code may make a
claim. The runnable profile says exactly which artifacts and topology execute
it. The implementation now enforces that boundary, but only sanitized
transcripts and validated result bundles say what actually happened.

## Common workflow

Every engine execution must:

1. verify the immutable engine and connector artifacts plus Linux/ARM64 runtime;
2. initialize the stock Iceberg connector from the selected catalog adapter;
3. prove its run-owned namespace absent before any mutation;
4. create and list that namespace through the engine;
5. create and reload one unpartitioned format-v2 Parquet table;
6. insert 16 deterministic rows and read their exact canonical projection;
7. add one optional string field named `note` without changing existing field
   identity;
8. insert four evolved rows and read all 20 rows under the evolved schema;
9. independently correlate the engine observation with standard Iceberg REST
   table state;
10. audit current metadata and data objects under the exact table root in the
    profile's shared MinIO;
11. reconcile all owned catalog identifiers without purge; and
12. write a bounded, sanitized transcript with no raw rows or secret values.

All 14 assertions are required. A known inability of a pinned engine or
connector is `unsupported` only when immutable profile evidence classifies it
before mutation and no substitute request is sent. An attempted operation that
violates an assertion is `fail`. An unavailable environment is `not-tested`.
These states are never collapsed into one another.

## Deterministic data oracle

Rows use integer and string semantics shared by all selected engines. For an ID
`i`:

- `category` is `category-` followed by `i mod 4`;
- `amount_cents` is `100 × i + 7`;
- evolved rows add `note = evolved-` followed by `i`.

The initial batch contains IDs 0–15. The evolved batch contains IDs 16–19. A
read is sorted by `id`, projected in scenario-declared column order, encoded as
one compact RFC 8259 JSON array per UTF-8 line, and terminated by a final line
feed. The checked-in identities are:

| Checkpoint | Rows | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| Initial projection | 16 | 346 | `e78b526d7e757090a9a90c80802c2a543cbf8166cfac6d6ed48c618926e85a15` |
| Evolved projection | 20 | 570 | `b2af6f475851e07d1ace3706d8867530c13dd5938bee90cfcc62d3939e01bea2` |

A focused Rust test regenerates both streams from the scenario parameters and
checks count, byte length, and digest. Runtime renderers consume those same
parameters; they must not carry a second engine-local row fixture.

## What may vary

The engines spell equivalent operations differently. Spark uses its DataSource
V2 Iceberg catalog, Flink creates an Iceberg catalog through its Table API, and
Trino configures its Iceberg connector. Their SQL types and namespace commands
also differ. A small renderer may translate neutral Iceberg types and operations
to one engine's stock public syntax.

That renderer is engine-specific but catalog-neutral. Given one engine and the
same semantic input, it must generate the same operation for LakeCat, Polaris,
Gravitino, and Lakekeeper. It cannot inspect a catalog ID to change SQL, skip an
assertion, rewrite a REST request, patch engine files after startup, or replace a
stock connector operation with harness HTTP. Engine and connector components are
recorded in the result; every behavior-changing adapter is disclosed separately.

Catalog-specific routing is data, not code. The existing profile adapter owns:

- REST base URL and config route;
- anonymous or OAuth2 client-credentials authentication;
- static, negotiated, or absent route prefix;
- optional standard create-table location;
- warehouse and shared-object-store topology; and
- capability limitations with upstream attribution.

Secret values enter only as runtime environment values and never enter a profile,
scenario, command transcript, query text, or result artifact.

## Independent evidence

An engine reporting query success is necessary but insufficient. The runner also
loads the table through the profile's already-tested protocol-native REST adapter
and compares table UUID, current metadata location, format version, schema,
snapshot count, and scenario-owned properties. It then traverses only the exact
returned table root in shared MinIO and requires:

- the current metadata object;
- at least four retained metadata objects, corresponding to create, first append,
  schema evolution, and second append; and
- at least two Parquet objects, one or more from each logical append.

The scenario disables metadata deletion and sets a large previous-version limit
at table creation. Cleanup uses standard Iceberg REST with purge disabled after
evidence capture. This avoids engine-specific `DROP TABLE` purge semantics and
keeps immutable metadata and data-plane evidence available for review.

## Pinned upstream behavior

The candidate profile pins Apache Iceberg Java 1.11.0 at
`6976e020b894f6a6777704df2b8c4458cb291ae9`. Its exact documentation defines:

- [Spark REST catalog configuration](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/spark-configuration.md#catalogs),
  [SQL writes](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/spark-writes.md#insert-into),
  and [additive `ALTER TABLE` evolution](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/spark-ddl.md#alter-table--add-column);
- [Flink REST catalog configuration](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/flink.md#rest-catalog)
  and [stock `INSERT INTO` writes](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/flink-writes.md#insert-into); and
- the [Iceberg REST OpenAPI](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/open-api/rest-catalog-open-api.yaml)
  used for independent state and cleanup evidence.

Trino 483 is pinned at `50b0b50b75abd47f830b7805ee1b51716eb4065e`.
Its exact source documentation defines the
[REST catalog properties](https://github.com/trinodb/trino/blob/50b0b50b75abd47f830b7805ee1b51716eb4065e/docs/src/main/sphinx/object-storage/metastores.md#rest-catalog)
and the [Iceberg connector's namespace, table, insert, read, and
additive-column operations](https://github.com/trinodb/trino/blob/50b0b50b75abd47f830b7805ee1b51716eb4065e/docs/src/main/sphinx/connector/iceberg.md).
These pinned sources, rather than moving `latest` pages, govern the runtime
implementation.

## Spark runtime materialization

The first runnable engine profile is generated from the broad candidate and the
audited
[`spark-4.1.3-iceberg-1.11.0-2026-08-27`](../materializations/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json)
image sidecar. It retains exactly Spark, the Java connector, shared MinIO,
LakeCat, Polaris, Gravitino, Lakekeeper, their required private-state
components, the exact optimized `catalog-bench-engine` runner, and the four
corresponding protocol-native catalog adapters. Nessie is not silently added to
the Phase 2 four-catalog requirement.

The Docker build separates three identities:

- `catalog-bench-engine:5e10f36e7e99` is the independently observed donor for
  the stripped production runner. It is built from public revision
  `5e10f36e7e99815df273c7b567e466749f04d4be` with stable Rust 1.97.1,
  optimization level 3, fat LTO, one codegen unit, native CPU features, stripped
  symbols, and aborting panics.

- `catalog-bench/iceberg-spark-runtime:1.11.0-spark4.1_2.13` contains only the
  Maven Central Iceberg Spark runtime and AWS bundle. BuildKit admits each URL
  only when its expected SHA-256 matches.
- `catalog-bench/spark:4.1.3-iceberg1.11.0` starts from the broad profile's
  immutable Spark index, selected as Linux ARM64, and copies those exact JAR
  bytes into `/opt/spark/jars` plus the exact runner into `/usr/local/bin`. Its
  labels bind the audited ARM64 child manifest and the Spark, Iceberg, and runner
  source revisions.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` | 47,959,591 | `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` |
| `iceberg-aws-bundle-1.11.0.jar` | 63,613,165 | `38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f` |
| `spark-sql_2.13-4.1.3.jar` | 13,604,536 | `6002f0e4430c36909db950a0b0863502260050ca2cc65ff8ca89baf404edb345` |
| `spark-submit` | 1,040 | `98e6f3b89b9092938a0b163a656c2b9051099821966fc7ab5ef9888fa9f62c6a` |
| `catalog-bench-engine` | 4,986,064 | `44e0aad6f2519678d335d6a437073da9674bb5a378df4b6d92fe88dfae038f5b` |

The connector local-image ID is
`c6fd71411aaffbf5b0d805a7e49886a97252a5d3297586ba79151f7ddc3a15a7`;
the engine-runner donor local-image ID is
`e011bf4c8a953768e237431a9e3a8a3dfaf313bd210444fe485a7aa59c0fc2c9`;
the executed Spark image ID is
`b2c7d6494c6b8fd407949e3894525b82c1bef9b6ab4fb95cbb702b3e10d01bec`.
The source index is `bf9d035a...`; the selected ARM64 child is
`f6831c619d0f...`. A Compose smoke reports Spark 4.1.3, Scala 2.13.17,
OpenJDK 21.0.11, and Spark revision `77bbf77e86ad...` from inside that image.

The source profile SHA-256 is
`f2bc773323a1438ee5f66553c3ae55b5706f3ab1dc627eb0c518197e8addb33e`.
The materialization SHA-256 is
`f73834ed8490efbd68f73331627b8061c72a6da159e78fc5e24a0002bd78d7be`;
the generated
[`runnable profile`](../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json)
SHA-256 is
`cb64b3b58db24e2380253b6992927ce044b349aaa061230fe62ad9680a99a969`.
The shared Docker verifier independently copies every recorded artifact from
stopped containers and rejects image, platform, label, digest, or byte-count
drift. The reusable materialization policy also rejects a copied JAR or runner
when its media type, SHA-256, or byte count differs between donor and executed
image, so a test-only equality assertion is not the trust boundary.

The materialization proves runtime identity only. The runner and launcher below
can produce raw workflow evidence, but neither this profile nor an unbundled
transcript claims that a catalog passed the common workflow.

## Evidence runner boundary

The C2-04 library implementation runs the stock Spark process before opening a
harness REST session or MinIO client. Runtime mismatch, preparation failure, and
fixture collision therefore cannot trigger independent network or cleanup
effects. Once the engine has emitted the ordered run-owned absence event, the
harness may independently load the final table, audit only its validated S3
root, and attempt every non-purging cleanup and absence check even when an
earlier cleanup operation fails.

The resulting transcript binds:

- the exact profile and scenario bytes through SHA-256;
- runner, catalog, engine, connector, and object-store identities;
- the run-owned fixture and bounded process event stream;
- independently projected REST table state and shared-MinIO counts; and
- complete cleanup evidence plus all 13 behavioral checks.

Sanitization is the fourteenth check. One shared credential source records only
values actually read by the admitted execution. Before a transcript can be
returned, a recursive value audit rejects those values, an unredacted bearer
form, or any complete deterministic input row. The serialized schema has no raw
engine stdout, row payload, REST response body, object-store error detail, or
raw backend exception. Offline validation re-derives the selected plan and
behavior checks, so changing a catalog identity, component, fixture, contract
digest, redaction record, or classification fails closed.

This implementation evidence is not a catalog result. A pass still requires the
optimized runner artifact to execute the common workflow in the declared
same-Docker/shared-MinIO topology and the resulting transcript to enter a
validated publication bundle.

The `catalog-bench-engine` executable exposes only `--profile`, `--scenario`,
`--catalog`, `--fixture-id`, and `--output`. It reads and hashes the exact
contract bytes, uses environment variables only through the profile-selected
credential names, and publishes one newline-terminated transcript without
overwriting an existing path. A valid behavioral failure is written before the
process returns status 2; a verified fixture collision is written before status
3. Contract, policy, sanitization, encoding, or publication failures return
status 1 and cannot create a claimed transcript.

The production Compose topology does not split that executable from the engine
it governs. `engine-runner-image` builds the optimized ELF from an exact public
catalog-bench revision; the Spark image copies the ELF and revision marker from
that named build context, verifies the marker, and records the revision as an
image label. `spark-engine` starts that ELF in the resulting Spark/Iceberg image,
so its `spark-submit` child, contracts, catalogs, and MinIO all remain inside the
declared Docker topology. The donor image exists only to make the source-built
artifact independently inspectable.

The corresponding profile role is `engine-runner`, and it may select only the
`catalog-bench-engine` benchmark-harness component. Its donor image declares one
ELF at `/usr/local/bin/catalog-bench-engine`; the Spark component must declare a
byte-identical artifact at the same path. Policy merges those two ownership
claims into one runtime expectation, and the process-side verifier hashes the
actual running path before credentials, network access, or Spark startup. The
transcript records the harness source revision from that component rather than
mistaking the Rust build-toolchain image for the executed runner.

## Fresh four-catalog execution

The canonical production invocation is:

```sh
docker/run-spark-interoperability.sh "spark_$(date -u +%m%d%H%M%S)"
```

The launcher admits one new run ID and one new evidence directory, then uses the
same shared fresh-state policy as the production contention sweep. It rejects
reused state volumes or Compose projects, refuses unknown users of the fixed
benchmark network, and preserves all prior volumes. Local source-built images
are built under the stable `catalog-bench` project identity and every profile
image, label, platform, executable, and JAR is independently verified before
the run project starts.

One run-owned Compose project starts LakeCat, Polaris, Gravitino, Lakekeeper,
their private state stores and readiness helpers, the source-built MinIO server,
and the immutable combined runner/Spark image. The four catalog workflows run
sequentially to avoid turning a correctness oracle into an undeclared resource
competition, but they retain the same catalog processes, Docker network, and
MinIO process. Each fixture name is already catalog-qualified by policy, so the
same run ID remains both comparable and collision-safe across catalogs. Nessie
is intentionally absent because it is not part of this Phase 2 requirement.

The launcher attempts every catalog after an ordinary behavioral failure. It
accepts runner status 0 only with a `pass` transcript, status 2 only with a
`fail` transcript, and status 3 only with `fixture-collision`. An unexpected
status, missing transcript, unreadable JSON, or classification mismatch makes
the complete run invalid. Raw transcripts remain outside public results until a
separate importer revalidates their contracts and invariants and creates an
immutable bundle.

## Phase 2 unit boundaries

C2-01 owns only the common write/read/evolution contract. It intentionally does
not claim a runtime result.

C2-02 owns the reusable scenario-profile derivation and fail-closed image policy.
C2-03 owns the Spark/Iceberg production images and runnable profile above.
C2-04 owns the stock Spark process, independent REST/MinIO reconciliation,
cleanup, sanitized transcript boundary, and fresh four-catalog launcher. Its
production artifact is admitted; live four-catalog evidence remains deliberately
unpublished until complete runs and a publication bundle are validated.

The remaining independently committed units will:

1. execute Spark against LakeCat, Polaris, Gravitino, and Lakekeeper and import
   the complete raw transcripts into a validated result bundle;
2. add Flink and Trino renderers against the same scenario;
3. define a separate deterministic conflict scenario with an honest
   synchronization boundary; and
4. define OpenLineage correlation only for engines whose pinned integrations can
   emit and identify the required events.

Each unit updates the changelog, runs focused tests plus contract/schema checks,
passes `git diff --check`, and is committed and pushed before the next unit.
