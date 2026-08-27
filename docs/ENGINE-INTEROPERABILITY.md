# Stock Engine Interoperability

Phase 2 compares interoperability, not query-engine speed. The first authority is
the versioned
[`engine.iceberg.write-read-evolution` v2](../scenarios/v1/engine.iceberg.write-read-evolution.v2.json)
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

## Reusable process evidence boundary

The common workflow does not depend on a Spark process type. Every
`EngineRunner` returns the same `EngineProcessExecution` algebraic data type:
verified runtime artifacts, one closed terminal outcome, an optional bounded
event capture, an optional exit code, and optional process elapsed time. Closed
credential-read and preparation failures retain only categories; they cannot
carry a credential value, command line, exception, or backend message.

The `0`/`2`/`3` mapping means completed, behavioral failure, and fixture
collision in the harness event protocol. It is not a Spark convention. The
neutral process layer combines an exit code with the decoded terminal event so
an engine cannot claim success, failure, or collision through status alone.
Timeout, stdout, wait, protocol, credential, preparation, and runtime failures
remain separate ADT variants. No-detail variants use empty closed shapes, which
preserve their existing JSON representation while rejecting stray fields.

`SparkProcessExecutor` is currently the sole production adapter that emits this
evidence. The generic workflow consumes it to decide whether fixture ownership
authorizes independent REST/MinIO reconciliation and cleanup. Flink and Trino
must implement the same boundary and event protocol; they may not fork the
classification rules or introduce engine-specific transcript meanings. This
refactor does not claim either runtime is implemented yet—the remaining
renderer and artifact policies must still be generalized in separate verified
units.

## Reusable execution-policy boundary

`InteroperabilityPlan` separates common semantics from renderer policy. Shared
catalog projection, reconciliation, transcript, sanitization, and workflow code
consume only its neutral `fixture` and `scenario` views. Renderer-specific
settings live in the `EngineExecutionPlan` algebraic data type. Its closed
variants contain `SparkExecutionPlan` and `FlinkExecutionPlan`; both reuse the
same catalog-neutral REST catalog, authentication, S3FileIO, fixture, and
scenario values while retaining only genuinely engine-specific settings.

Runnable profiles with one `stock-engine` use the singular convenience
constructor. Candidate profiles may contain several engines, so they must call
the explicit engine constructor. It accepts an engine only when exactly one
`stock-engine` service selects that component ID; merely naming an unrelated
profile component is insufficient. The selected component then passes the same
renderer version, connector, immutable artifact, and runner-copy checks. This
keeps engine choice in typed profile data and prevents component order or an
implicit “first engine” rule from affecting execution.

The Spark process adapter must explicitly select that Spark variant before it
can serialize `plan.json`. An adapter paired with the wrong execution-plan
variant fails preparation with a closed `execution-plan-mismatch` category; it
cannot reinterpret another engine's settings or panic. Flink now has a policy
variant but not yet a renderer or process adapter; Trino has neither. Those
adapters must reuse the same scenario, fixture, evidence, and classification
policy.

### Pinned Flink capability decision

The selected line is Apache Flink 2.1.3 with the Apache Iceberg 1.11.0 Flink
2.1 runtime. The checked-in candidate profile pins exact upstream source
revisions and the official image index; a future runnable profile must also pin
and observe every copied runtime artifact before execution is admitted. The
policy requires the stock `/opt/flink/bin/flink` CLI, engine `2.1.3`, Java
`17.0.20`, and Scala `2.12.20`. Java comes from the Linux ARM64 config selected
by that pinned image index; Scala comes from the exact Flink source revision's
root POM. A missing or extra dependency fails runtime verification.

There is an important discrepancy in the pinned upstream material. The
[Iceberg 1.11 Flink DDL page](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/flink-ddl.md#alter-table)
still describes alteration as property-only. However, the exact
[Iceberg 1.11 `FlinkCatalog`](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/FlinkCatalog.java#L559-L613)
implements Flink's change-list `alterTable` overload, and the exact
[`FlinkAlterTableUtil`](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/util/FlinkAlterTableUtil.java#L117-L164)
maps a physical `TableChange.AddColumn` to `UpdateSchema.addColumn` or
`addRequiredColumn` and commits it transactionally. Additive schema evolution
is therefore a supported stock connector operation for this pinned
combination. The forthcoming renderer must exercise that Flink catalog API and
may not substitute a direct REST or standalone Iceberg client call. This policy
unit is not runtime evidence and creates no Flink result or ranking row.

### Catalog-neutral Flink rendering

`FlinkRenderedProgram` is a pure boundary between the validated plan and the
future effectful process adapter. It emits one typed catalog setup and eight
ordered, purpose-tagged stock Flink statements: create the namespace, create
the table, append and read the initial rows, add the scenario column, append and
read the evolved rows, and inspect the Iceberg snapshots metadata table. The
exact pinned [Flink 2.1 parser node for `ALTER TABLE ... ADD`](https://github.com/apache/flink/blob/6cda56b084d5c337b36d2f8ed464bc92093b0a34/flink-table/flink-sql-parser/src/main/java/org/apache/flink/sql/parser/ddl/SqlAlterTableAdd.java)
documents and represents the syntax used by the renderer. Combined with the
pinned Iceberg change-list implementation above, this keeps the schema change
inside Flink's planner and stock Iceberg catalog path.

The renderer has no catalog-name branch and no HTTP client. Catalog endpoint,
standard `warehouse` and `prefix` options, S3FileIO settings, table location,
schema, properties, generators, and canonical read projections all come from
the plan. Identifiers are restricted to the scenario's closed lowercase
vocabulary, text is SQL-escaped, and credential-free HTTP/S3 routes are checked
again at the renderer boundary. Anonymous or OAuth mode is retained as a typed
catalog-setup value, but no client secret, object-store key, credential option,
or token can enter the rendered program. The process adapter will read secrets
only after runtime verification and inject them directly into the child
environment or in-memory catalog configuration.

The renderer is presently validation-only: tests prove deterministic rendering
for every selected profile catalog and reject format, parallelism, policy,
route, file-IO, fixture, identifier, and generator drift. No Flink process was
launched, so these tests are not interoperability evidence and do not add a
ranking row.

## Reusable runtime-identity boundary

Runtime-ready evidence contains a neutral engine version, an exact sorted map of
runtime dependencies, and the observed operating system and architecture. The
common reconciler delegates engine and dependency matching to the selected
`EngineExecutionPlan`; it separately compares normalized platform names with the
profile. It therefore contains no Spark, Scala, Java, Flink, or Trino field-name
branches.

The Spark variant requires engine version `4.1.3` and exactly `java=21.0.11`
plus `scala=2.13.17`. Its renderer rejects missing or extra dependency names,
legacy `spark_version`/`scala_version`/`java_version` shapes, non-text values,
and unbounded text before emitting an event. The Flink policy variant requires
engine version `2.1.3` and exactly `java=17.0.20` plus `scala=2.12.20`; its future
renderer must enforce that shape before emitting an event. Each engine variant
owns its exact dependency set without weakening the common vocabulary.

This event change deliberately advances the scenario revision and transcript
format to v2. The decoder does not use an untagged legacy alternative whose
shape could become ambiguous as engines are added. The checked-in immutable
Spark profile remains scoped to v1 because its source-bound runner bytes predate
this change; its [v1 scenario](../scenarios/v1/engine.iceberg.write-read-evolution.json)
is retained byte-for-byte rather than overwritten. Relabeling those bytes would
falsify provenance. A fresh optimized runner and combined Spark image must be
materialized, observed, and pinned before any v2 production execution. No
production Spark result using v1 had been published, so there is no published
result migration or rewritten evidence; any pre-publication v1 transcript must
be rerun with that future source-bound v2 profile.

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

The independent admission command is the first half of that boundary:

```sh
cargo run -p catalog-bench-contract --locked -- engine-evidence validate \
  --profile profiles/v1/SOURCE_BOUND_V2_ENGINE_PROFILE.json \
  --scenario scenarios/v1/engine.iceberg.write-read-evolution.v2.json \
  --evidence-directory target/spark-evidence/<run-id> \
  --fixture-id <run-id>
```

It does not trust the launcher's catalog list or exit summary. Expected file
names come from the profile's adapters; the directory may contain no missing,
extra, nested, symlinked, empty, oversized, or noncanonical entry. Each decoded
transcript must identify the catalog implied by its file name and the common
fixture, bind the exact profile and scenario digests, reproduce all derived
execution checks and classification, and pass the value-safety audit again.
Only the resulting typed set is available to the result materializer.

The placeholder is intentional: the checked-in source-bound Spark profile and
launcher preserve the v1 runner and scenario for reproducibility, but current
source admits only v2 evidence. They are not a publication path together. A
fresh optimized v2 runner/image materialization must replace the placeholder
and advance the launcher in one verified unit before the next production run.

## Reviewed live-run envelope

Raw transcript validity does not establish when, where, or under which runtime
the workflow ran. The second C2-05 admission layer therefore uses one closed
`catalog-bench/engine-result-review/v1` sidecar. The command takes only a
repository root and the review path:

```sh
cargo run -p catalog-bench-contract --locked -- engine-evidence validate-review \
  --root . \
  --review results/source/engine/<run-id>/review.json
```

For publication, the exact admitted transcripts are archived without
modification under `results/source/engine/<run-id>/transcripts`, and the sidecar
is stored beside that directory as `review.json`. It contains four sections of
operator-reviewed provenance:

1. `bundle` names a nonempty ID and title, a destination below `results/v1`, and
   the UTC instant at which the reviewed publication input was completed.
2. `run` names the shared fixture, the exact sanitized invocation
   `docker/run-spark-interoperability.sh "<run-id>"`, strictly ordered start and
   completion instants, and the observation basis for each instant.
3. `profile`, `scenario`, and the catalog-sorted `transcripts` bind normalized
   repository-relative locations to their exact lowercase SHA-256 and byte
   counts. Every transcript location must share one directory and be named
   `<catalog>.json`.
4. `environment` uses the ordinary result contract's precision-aware capture,
   while `redaction` records a completed review, its policy, and a nonempty set
   of unique excluded-data categories.

The file must be a newline-terminated regular file no larger than 1 MiB and may
not contain unknown fields. Source and output paths reject absolute paths,
traversal, duplicate separators, backslashes, URI-like forms, and control
characters. UTC timestamps accept one to nine fractional digits, validate leap
years and calendar ranges, and compare as instants rather than strings. The
reviewed operating system, architecture, and Docker network must equal the
runnable profile; container-runtime precision must be `exact`.

Crucially, `validate-review` does not accept separate profile, scenario,
fixture, evidence-directory, or catalog arguments. It resolves them from the
review, reruns the complete raw evidence validator, and correlates each claimed
path, hash, byte count, catalog, and fixture with that typed admitted set. This
prevents an operator from reviewing one run while accidentally validating
another. The validated envelope is still not a public result: it is the sole
input to the deterministic materializer.

## Deterministic result materialization

The C2-05 materializer accepts only public, independently reproducible sources:
the review sidecar and transcripts must be below `results/source`, while the
reviewed runnable profile and scenario must be below `profiles/v1` and
`scenarios/v1`. Canonical-path checks prevent a symlink from escaping the
repository. The caller supplies only the repository root and review path:

```sh
cargo run -p catalog-bench-contract --locked -- engine-import write \
  --root . \
  --review results/source/engine/<run-id>/review.json
```

The command reruns raw evidence admission and review correlation before writing
anything. It then creates, without replacing, the review-selected destination
below `results/v1` and writes:

- exact copies of the profile, scenario, review, and four transcripts below
  `source/`;
- one contract result per profile catalog;
- a manifest binding every copied or generated byte; and
- `MATRIX.md`, generated only from the validated result bundle.

Each result exposes the stock Spark component in the contract's `client` slot
and discloses the Iceberg Java connector in `adapters`. The complete launcher
interval is retained as the run interval for each catalog, with an extension
stating that its scope is the sequential four-catalog launcher. Process elapsed
time remains only in the raw transcript: result `measurements` is empty, the
manifest declares `ranking: false`, and the matrix has no rank column.

The materializer maps the thirteen recomputed workflow checks and independent
sanitization check to all fourteen scenario assertions. It applies these closed
outcome rules:

- `pass` requires every required assertion and a trusted completed process;
- failed required assertions produce `fail` with category `assertion`, while
  retaining only the failed assertion IDs and making no backend-cause or retry
  inference;
- all passing assertions with an untrusted terminal produce `fail` with category
  `harness`; and
- a pre-mutation fixture collision produces `not-tested`, with every assertion
  `not-evaluated`.

The corresponding checker is intentionally stronger than ordinary bundle
loading:

```sh
cargo run -p catalog-bench-contract --locked -- engine-import check \
  --root . \
  --review results/source/engine/<run-id>/review.json
```

It derives all expected bytes again, verifies them, reloads the complete bundle,
regenerates the matrix, and compares the exact recursive output tree. Missing,
extra, modified, symlinked, or nonregular entries fail closed. A partial failed
write is never repaired in place; the operator preserves it for diagnosis and
uses a new reviewed destination.

This repository now contains and tests that publication path. It does not yet
contain a production Spark result: only a fresh optimized four-catalog Docker
run, archived evidence, and completed human review can create that claim.

## Phase 2 unit boundaries

C2-01 owns only the common write/read/evolution contract. It intentionally does
not claim a runtime result.

C2-02 owns the reusable scenario-profile derivation and fail-closed image policy.
C2-03 owns the Spark/Iceberg production images and runnable profile above.
C2-04 owns the stock Spark process, independent REST/MinIO reconciliation,
cleanup, sanitized transcript boundary, and fresh four-catalog launcher. Its
production artifact is admitted; live four-catalog evidence remains deliberately
unpublished until complete runs and a publication bundle are validated.
C2-05 first admits the exact raw transcript set independently and then binds it
to the reviewed live-run envelope above. It now also deterministically
materializes result records, exact source copies, an immutable manifest, and the
unranked correctness matrix from only that typed validated input. The remaining
C2-05 work is the fresh optimized production run, review, and publication.
C2-06 first extracts engine-neutral process evidence and then separates common
scenario and fixture semantics from renderer-specific execution policy. It also
versions the evidence contract while replacing Spark-named runtime fields with
plan-owned neutral runtime identity. Spark remains the only implemented
execution-plan variant and production adapter; Flink and Trino are not yet
claimed.

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
