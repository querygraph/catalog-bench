# Catalog-bench interoperability contract

`catalog-bench/v1` is the durable, catalog-neutral publication boundary for the
Open Catalog Interoperability Lab. It complements the small `BenchReport` emitted
between a benchmark process and the local driver; it does not replace that fast
process-local format.

The contract has four independently versioned document kinds:

| Kind | Checked-in schema | Purpose |
|---|---|---|
| `scenario` | [`scenario.schema.json`](../schemas/v1/scenario.schema.json) | Neutral steps, prerequisites, assertions, and classification policy. |
| `profile` | [`profile.schema.json`](../schemas/v1/profile.schema.json) | Exact component/source/image pins, sanitized topology, and catalog adapter bindings. |
| `result` | [`result.schema.json`](../schemas/v1/result.schema.json) | One catalog/client/scenario execution, its outcome, assertions, measurements, environment, and evidence. |
| `manifest` | [`manifest.schema.json`](../schemas/v1/manifest.schema.json) | Immutable index and provenance for a published result bundle. |

The JSON Schemas are generated from the Rust types with Schemars configured
explicitly for JSON Schema Draft 2020-12. A test and `schemas check` compare the
parsed generated and checked-in documents exactly, so formatting changes do not
hide schema drift.

## Classification is data, not presentation

`ResultOutcome` is a closed algebraic data type:

- `pass`: the scenario ran and every required assertion passed;
- `fail`: the scenario ran and violated a required behavior or encountered an
  execution failure; category, summary, detail, retryability, and evidence are
  mandatory;
- `unsupported`: a prerequisite capability is absent, with an explicit capability
  name and explanation;
- `not-tested`: no capability conclusion is justified because execution was not
  attempted, with the blocking reason recorded.

The matrix renderer may rank only comparable `pass` records. It must display the
other three classes separately and preserve their details. A fast failing result
can retain its measurements, but those numbers do not become a valid rank.

The scenario's `strict-v1` policy is deliberately simple: unsupported is decided
from a declared prerequisite, while an attempted requirement that behaves
incorrectly is a failure. An adapter cannot relabel an observed failure as
unsupported after execution.

## Adapter completeness and no-shim semantics

Current and executable profiles define one catalog capability vocabulary and one
adapter for every catalog component. Each adapter records its exact Iceberg REST
base URL, config request, route-prefix resolution, authentication mode, optional
standard create location, and request-handling mode. The adapter's capability
coverage is an exhaustive partition: every profile capability is either scheduled
for standard-protocol exercise or declared unsupported before execution with
attribution and explanation.

`exercise-all` is the compact algebraic variant when every vocabulary entry is
scheduled. The `explicit` variant carries the exhaustive exercise/unsupported
partition only when exceptions exist, avoiding duplicated capability lists while
preserving immutable semantics.

`exercise` is not a support claim. It means evidence, rather than profile prose,
will decide whether the operation passes. An attempted failure cannot be moved to
`unsupported` afterward. The complete field semantics and current five bindings
are documented in [ADAPTERS.md](ADAPTERS.md).

All current adapters are `protocol-native`. A behavior-changing shim can be
represented only as a separately pinned connector component with an explicit
description. This disclosure prevents an experimental shim from masquerading as
a stock compatibility path; it does not make shimmed evidence comparable to the
no-shim matrix.

## Evidence and reproducibility rules

Every result repeats the readable catalog and client name/version while referring
to an immutable profile digest for full source and artifact identity. Every
profile component records one of:

- a container reference plus a scoped index, platform-manifest, or local-image
  digest and optional embedded artifact hashes;
- an immutable source revision, optional executable digest, and locked build
  settings; or
- an ecosystem package name, version, and optional package digest.

Each result embeds its actual OS, architecture, CPU, memory, limits, runtime,
network, and behaviorally relevant flags. Measurements preserve elapsed time,
sample count, ranges, arbitrary named quantiles, and counters or ratios. Semantic
validation rejects non-finite values, inverted ranges, non-monotonic quantiles,
zero-denominator ratios, duplicate identifiers, dangling evidence references,
and a `pass` that hides a failed required assertion.

Environment values that are commonly absent from legacy reports carry explicit
`exact`, `approximate`, or `unknown` precision. Approximate and unknown values
require an explanation; migration code must preserve uncertainty instead of
inventing an exact CPU model, byte count, runtime version, or limit.

A result's `run` is either one execution with timestamps and a repetition number,
or an aggregate that names its period, included and excluded repetitions, and
aggregation rule. Aggregate rows therefore cannot masquerade as individual runs,
and discarded conditioning rounds remain visible.

Artifacts are addressed by an explicit digest object. The digest covers the
artifact's exact bytes—not a reserialized JSON value—so whitespace and final
newlines are significant. Manifests identify whether evidence is a `live-run`,
`historical-import`, or `fixture`; imported legacy data can never masquerade as a
new execution.

Evidence entering a publishable result must set `sanitized: true`. The manifest
also requires a completed redaction review. Profile settings and adapter config
queries reject secret-shaped keys. Adapter URLs reject embedded credentials,
queries, and fragments. These are guardrails, not substitutes for the repository's
artifact secret scan.

### Config-probe transcripts

The C1-03 config runner produces the intermediate
`catalog-bench/config-transcript/v1` evidence shape. It records the exact profile
and scenario byte digests, selected adapter, sanitized request, allowlisted
response headers, a sanitized JSON body, raw-body byte count and—only for valid
JSON requiring no redaction—SHA-256, prefix resolution, endpoint interpretation,
every assertion, and the final probe classification. It never stores the raw
response body, OAuth client credentials, or bearer token. Secret-shaped JSON
keys and runtime credential values are recursively redacted before serialization,
and response capture is bounded to 1 MiB.

These transcripts deliberately are not `catalog-bench/v1` result documents.
Files emitted under `target/conformance-evidence` are mutable smoke diagnostics;
they become publishable only through the later result/manifest pipeline, which
must copy reviewed sanitized evidence into an immutable bundle, hash exact bytes,
record the execution environment, and pass bundle validation and secret review.

### Namespace-probe transcripts

The C1-04 namespace runner produces the intermediate
`catalog-bench/namespace-transcript/v1` evidence shape. Its typed fixture owns
two top-level namespaces and one multipart child, and the transcript records the
exact operation sequence, sanitized request and response metadata, bounded
pagination observations, cleanup outcome, and every required or optional
assertion. Fixture preflight prevents collisions with existing state, while
child-first cleanup and post-drop verification run even after an assertion
fails.

Opaque pagination tokens are treated as sensitive protocol data. Request URLs
retain only a redacted token marker, while the pagination summary retains page
and unique-namespace counts. Recursive sanitization removes secret-shaped fields
and runtime credentials, and raw response bodies are never serialized. The
summary and assertions still prove bounded traversal, uniqueness, completeness,
and loop freedom without retaining reusable opaque tokens. See the exact
five-catalog acceptance matrix and artifact identities in
[`NAMESPACE-CONFORMANCE.md`](NAMESPACE-CONFORMANCE.md).

### Table-probe transcripts

The C1-05 table runner produces the intermediate
`catalog-bench/table-transcript/v1` evidence shape. One preflighted run-owned
namespace contains two committed tables plus distinct rename, registration, and
missing-table candidates. The runner validates requested schema and properties,
nonempty immutable metadata locations, stable distinct table UUIDs, exact
isolated listings, bounded pagination, an update-and-reload metadata transition,
duplicate/missing-table/missing-namespace error envelopes, and non-purging drop.
When the adapter declares a `create_table_location`, the runner derives unique
namespace/table children, sends those standard `location` values, and also
requires the returned metadata's table location to match. Without that field it
deliberately omits `location` and exercises the catalog-managed default.

Same-namespace rename and metadata registration are standard optional
assertions. An attempted protocol failure remains a visible optional `fail`; a
profile-declared limitation is `not-evaluated` and sends no request. Neither can
change the required classification. Cleanup always reconciles all four possible
table names, verifies each absent, drops the fixture namespace, and verifies the
namespace absent. A failed collision preflight is the sole case that forbids
cleanup mutation, protecting pre-existing state.

The table runner shares authentication, config negotiation, route-prefix and
namespace-separator resolution, bounded response capture, request recording,
recursive sanitization, and opaque page-token redaction with the namespace
runner. Its smoke transcripts remain non-publishable until the immutable
result/manifest pipeline records exact artifacts and environment provenance.
See the optimized five-catalog acceptance matrix, shared-MinIO audit, and exact
artifact identities in [`TABLE-CONFORMANCE.md`](TABLE-CONFORMANCE.md).

### Commit-correctness transcripts

The C1-06 commit scenario separates deterministic correctness from the existing
throughput workload. One preflighted table first accepts matching table-UUID and
schema requirements, then advances from schema 0 to schema 1 under matching
UUID, schema, and last-field requirements. A request planned against schema 0 is
therefore provably stale without depending on scheduler timing: it must return a
spec-shaped HTTP/code 409 `CommitFailedException`, leave the current metadata
location unchanged, and apply no property update.

Idempotency remains an optional protocol capability. The runner inspects the
standard `idempotency-key-lifetime` configuration after applying config
defaults/overrides. If absent, it sends no `Idempotency-Key` and records the
three optional assertions as not evaluated. If present, it uses a valid UUIDv7
key, repeats one byte-identical commit, and requires exactly one metadata-pointer
transition. Reusing that finalized key with drifted content is an explicit
optional safety check. Raw keys are redacted from evidence, while cleanup and
the required stale-state branch remain mandatory regardless of advertisement.

This scenario does not replace the same-table contention benchmark. It proves
admission and retry semantics one operation at a time; the contention scenario
continues to measure accepted throughput, 409 rate, and non-conflict errors.
See the optimized five-catalog acceptance matrix, exact-retry findings,
shared-MinIO audit, and rejected-run analysis in
[`COMMIT-CONFORMANCE.md`](COMMIT-CONFORMANCE.md).

### Stock-PyIceberg transcripts

The C1-07 stock-client runner produces the intermediate
`catalog-bench/pyiceberg-transcript/v1` evidence shape. It loads the same profile
and adapter bindings as the Rust probes, then verifies its exact CPython,
PyIceberg, PyArrow, S3FS, OS, and architecture identity before making a catalog
request. One public `RestCatalog` workflow creates a run-owned namespace and
table, appends and scans real Arrow data, independently classifies property,
schema, delete, stale-writer recovery, delegated-access, rename, register, view,
and pagination behavior, and always reconciles owned identifiers without purge.

Required round-trip assertions determine the top-level result. Each optional
operation retains its own `pass`, `fail`, `unsupported`, or `not-evaluated`
status; a catalog or pinned-client limitation cannot erase an attempted failure.
The runner records canonical row counts, ID ranges, and SHA-256 digests instead
of raw values. Delegated access records credential categories only. Runtime
secrets, raw exceptions, response bodies, and raw rows are rejected before
exclusive, no-overwrite serialization.

These transcripts remain mutable smoke diagnostics under
`target/pyiceberg-evidence` until the immutable result/manifest pipeline records
reviewed evidence and complete environment provenance. See the accepted
five-catalog matrix, exact artifact identities, shared-MinIO object proof,
deployment corrections, and rejected diagnostics in
[`PYICEBERG-INTEROPERABILITY.md`](PYICEBERG-INTEROPERABILITY.md).

### Same-table contention scenario versions

The original
[`same-table-contention` v1](../scenarios/v1/iceberg-rest.commit.same-table-contention.json)
is retained byte-for-byte because its digest is an immutable input to the
published 2026-08-08 historical bundle. It must not be edited to describe a new
run.

The current C1-08 authority is
[`same-table-contention` v2](../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json).
It preserves the common 50-warmup, 1,000-sequential, eight-writer, six-second
workload while making previously procedural validity rules contractual:
profile-driven config/auth/routing, collision-safe fixtures, one synchronized
writer window, complete latency and outcome accounting, final-state attribution,
table-root-scoped MinIO growth, non-purging cleanup, evidence sanitization, one
discarded conditioning round, five measured rounds, rotated catalog order, and
median-with-range aggregation. The common workload omits `Idempotency-Key` for
every catalog; exact retry and content binding remain the separate C1-06
correctness scenario.

### Stock-engine interoperability transcripts

The C2-01
[`engine.iceberg.write-read-evolution`](../scenarios/v1/engine.iceberg.write-read-evolution.json)
scenario is the common Phase 2 workflow for Spark, Flink, Trino, and any later
stock engine. It uses the existing `engine` actor and records the executed engine
in a result's `client` component slot; bundle validation already accepts profile
components of kind `engine` there. No contract-shape change is required.

One scenario-owned generator yields 16 initial rows and four evolved rows. Each
engine must create the same unpartitioned format-v2 Parquet table, append and
read the initial projection, add the same optional `note` field, append and read
the evolved projection, and produce the exact canonical row counts, byte lengths,
and SHA-256 values. The harness independently correlates table identity and
metadata state through the profile's standard REST adapter and audits retained
metadata and Parquet objects under the exact returned table root in shared MinIO.

Engine-specific SQL spelling is a semantics-preserving renderer, not a
catalog-specific shim. A renderer may depend on the engine component but cannot
branch on LakeCat, Polaris, Gravitino, Lakekeeper, or another catalog. Routing,
authentication, prefix, warehouse, and object-store behavior come from the
immutable profile. Any adapter that rewrites the operation under test must be
disclosed in the result and cannot satisfy the scenario's no-shim assertion.

Both declared capabilities are required. A profile-proven engine or connector
limitation is classified as `unsupported` before fixture mutation; once an
operation is attempted, a behavioral violation is a failure. Cleanup after any
post-mutation exit uses the profile's protocol-native REST adapter with purge
disabled so an engine's destructive `DROP TABLE` spelling cannot erase the
evidence being audited. The separate Phase 2 conflict and OpenLineage scenarios
will retain their own synchronization and correlation contracts rather than
weakening this common deterministic workflow.

## Closed fields and extensions

All ordinary records and enum variants deny unknown fields. This turns misspelled
measurement, digest, or outcome fields into validation errors rather than silently
discarded evidence. Deliberate project-specific data belongs only in an explicit
`extensions` map. Custom assertion names must be namespaced, for example
`org.example/catalog-check`.

An extension cannot override a core field or change classification semantics.
Consumers that do not understand it must preserve the value and may decline a
scenario that declares the extension as a required capability.

## Versioning

`contract_version` describes document shape and semantics. A breaking shape,
classification, or validation change requires a new contract version and schema
directory. Scenario `version` changes whenever steps, prerequisites, assertions,
or their meaning changes; editorial text alone need not change it. Profiles are
immutable evidence recipes: resolving a new tag, commit, image digest, build flag,
or service setting produces a new profile artifact and digest.

A profile is either `runnable` or `draft`. Runnable profiles reject a source build
without an executable digest and a package without an artifact digest. Draft
profiles must enumerate every unresolved component and explain the gap; they are
planning inputs and cannot back a result bundle. Materializing an artifact creates
a new profile document and digest.

Writers serialize deterministically, append one newline, hash those bytes, then
create references. The bundle validator verifies exact byte lengths and SHA-256
digests, profile and scenario references, profile component identities, scenario
assertion IDs and copied required flags, result/evidence artifacts, and complete
assertion coverage for attempted outcomes. It rejects draft profiles because
unresolved artifacts cannot support published results. Per-document validation
cannot prove these cross-file relationships by itself.

## Commands

Run from the repository root:

```sh
# Detect Rust/schema drift without writing files.
cargo run -p catalog-bench-contract --locked -- schemas check

# Intentionally regenerate all four checked-in schemas.
cargo run -p catalog-bench-contract --locked -- schemas write

# Deserialize and semantically validate one file or a directory tree.
cargo run -p catalog-bench-contract --locked -- validate path/to/documents

# Detect drift between the broad candidate, audited image observations, and the
# generated runnable contention profile.
cargo run -p catalog-bench-contract --locked -- profile check-contention \
  --source-profile profiles/v1/current-2026-08-26.json \
  --materialization materializations/v1/contention-2026-08-27.json \
  --output profiles/v1/contention-2026-08-27.json

# Verify exact bytes and every cross-document link in one result bundle.
cargo run -p catalog-bench-contract --locked -- bundle validate \
  --manifest results/v1/2026-08-27/manifest.json

# Recompute the historical JSON records from the hash-pinned source TSVs.
cargo run -p catalog-bench-contract --locked -- historical-import check --root .

# Recompute the current production records, manifest, and matrix from the
# hash-pinned C110 transcript plus reviewed environment/failure sidecar.
cargo run -p catalog-bench-contract --locked -- contention-import check --root .

# Detect drift in the human matrix generated from the validated records.
cargo run -p catalog-bench-contract --locked -- matrix check \
  --manifest results/v1/2026-08-27/manifest.json \
  --output results/v1/2026-08-27/MATRIX.md
```

`validate` recurses through directories and examines `.json` files. Schema files
themselves are inputs to `schemas check`, not contract documents. The historical
importer also verifies the source hashes, exact round/catalog dimensions, summary
arithmetic, request-rate arithmetic, expected and observed MinIO growth, and
legacy rank fields before emitting v1 records. `matrix check` first runs full
bundle validation and ranks only `pass` outcomes; non-pass measurements remain
visible but unranked.

The C110 contention importer deserializes the production transcript through the
same closed ADTs used by the runner, reconstructs the scenario-derived schedule,
and reruns the benchmark aggregation and tie-breaking policy. It requires exact
agreement with the transcript's aggregates, ranking, and sweep classification;
verifies profile, scenario, runner, runtime, sanitization, and evidence digests;
checks the reviewed environment and HTTP error totals; and requires causal
failure coverage for exactly the failed catalogs. It emits distributions for
failed rows as diagnostics but the result outcome keeps them unranked. Tampering
with either source, a result, a manifest reference, or the generated matrix is
covered by external tests.

The contention profile materialization sidecar is a strict generator
input rather than a fifth `catalog-bench/v1` document kind; its source digest,
closed fields, exact image set, labels, platforms, and executable identities are
validated before the ordinary runnable profile is rendered and validated.
The generator core is scenario-policy-driven: each wrapper owns an exhaustive
component and image set plus required in-image artifact media types, while one
implementation performs source-byte binding, topology narrowing, local-image
projection, readiness derivation, and deterministic serialization. It narrows
catalog adapters with retained catalog components and rejects duplicate or
unselected image policy entries. Scenario policies may additionally require
exact immutable labels, such as a base platform digest or connector coordinate;
duplicate, empty, or mismatched label requirements fail closed. The contention
wrapper still reproduces the accepted C110 profile byte-for-byte; Phase 2 engine
profiles reuse the core with their own explicit policies rather than copying the
validation pipeline.
