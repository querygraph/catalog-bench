# Changelog

## Unreleased

- C1-05 Gravitino storage correction: aligned the pinned 1.3.0 container's
  Compose environment with its `GRAVITINO_ICEBERG_REST_*` rewrite contract, so
  the declared SQLite JDBC backend, S3 warehouse, S3FileIO, MinIO endpoint, and
  path-style credentials replace the image's `/tmp`/memory defaults. Added a
  deployment regression test and operator diagnostics for proving the effective
  rewritten configuration before accepting shared-storage evidence.

- C1-05 final runner provenance: advanced the draft conformance-runner component
  to `catalog-bench@621cc4b`, whose table probe sends and verifies the profile's
  explicit shared-storage root. The production executable remains unresolved in
  the draft profile until C1-09 materializes immutable artifacts.

- C1-05 shared-storage correction: the table runner now consumes an adapter's
  validated `create_table_location` as a fixture root, derives unique
  namespace/table child locations, sends them on every create attempt, and
  verifies the catalog preserves each requested table location. Adapters without
  an explicit root continue to exercise their configured catalog default.

- C1-05 LakeCat provenance pin: advanced the draft profile and current-profile
  report to the exact pushed table-lifecycle implementation
  `lakecat@762527c7` (`v0.3.0-31-g762527c7`). The pin includes register and
  rename support plus compatible no-current-snapshot commit evidence; C1-09
  still owns immutable artifact resolution and public result publication.

- C1-05 provenance pin: advanced only the draft conformance-runner component to
  the independently reviewed table-runner revision `catalog-bench@efbce26`.
  Its stable Rust 1.97.1, fat-LTO, single-codegen-unit production recipe remains
  unresolved until the optimized Docker artifact is built and hashed; C1-09
  still owns conversion of reviewed smoke evidence into immutable results.

- Catalog community C1-05 runner: implemented a strict Iceberg REST table
  lifecycle probe with run-owned namespace preflight, committed two-table
  create/list/load, exact isolated pagination, immutable property update, three
  spec-shaped errors, same-namespace rename, non-purging drop, metadata
  registration, complete candidate reconciliation, and sanitized no-overwrite
  evidence. Shared routing negotiation keeps config/auth/prefix/separator policy
  identical across probes; 15 adversarial table tests cover optional limitations
  and failures, collisions, metadata drift, pagination defects, response bounds,
  OAuth secrecy, and cleanup after failed assertions.

- Catalog community C1-05 contract: added a neutral, versioned Iceberg REST
  table-behavior scenario with isolated namespace ownership, two-table
  create/list/load/update/drop coverage, bounded pagination, optional standard
  rename and register operations, exact duplicate/missing-resource error shapes,
  full candidate reconciliation, and sanitized no-shim evidence policy.

- Catalog community C1-05 preparation: generalized the namespace probe's HTTP
  operation recorder, typed observation facts, response-shape checks, and
  Iceberg error validation into one reusable evidence engine. Existing public
  namespace type names remain aliases with byte-identical serialization, while
  subsequent probes inherit the same bounds, sanitization, and failure model.

- Catalog community C1-05 preparation: extracted the Iceberg REST namespace
  identifier, separator negotiation, fixture validation, and prefix-aware route
  construction into shared conformance primitives. The namespace probe retains
  its exact scenario and transcript contract while the table lifecycle probe can
  reuse one routing implementation instead of cloning protocol-sensitive code.

- Catalog community C1-04 acceptance: documented the exact optimized
  same-Docker runner and LakeCat artifacts, profile/scenario/transcript hashes,
  five-catalog required/optional matrix, repaired LakeCat defects, Nessie's
  missing-parent HTTP 200, Polaris's optional property-update HTTP 409,
  cleanup/sanitization guarantees, reproduction workflow, and the explicit
  C1-09 publication boundary.

- C1-04 provenance pin: advanced the draft current profile to the independently
  verified namespace-runner revision `catalog-bench@1f4e640` and corrected
  LakeCat namespace implementation `lakecat@42b2f34b`. Both source builds retain
  the exact stable Rust 1.97.1 production recipe; C1-09 still owns resolved
  executable/image artifacts and conversion of smoke transcripts into a
  publishable immutable bundle.

- Catalog community C1-04 runner: added a strict Iceberg REST namespace
  lifecycle probe covering isolated create/list/load, multipart hierarchy,
  property update, duplicate and missing-parent errors, bounded pagination, and
  child-first cleanup. Refactored shared target, authentication, transport,
  evidence, and specification primitives out of the config runner; added
  recursively sanitized no-overwrite transcripts, explicit optional-operation
  classification, adversarial mock-server coverage, and a production CLI that
  keeps protocol failures as evidence instead of losing them as process errors.

- C1-03 provenance pin: bound the production commit driver and conformance
  runner to `catalog-bench@feb803f8`, LakeCat to its independently verified
  endpoint-correction revision `09dd7ee3`, and modeled the conformance runner as
  its own unresolved source-build component and service. Corrected the candidate
  profile's previously future-dated resolution timestamp; it remains `draft`
  until C1-09 materializes and hashes every listed artifact.

- Catalog community C1-03: added a strict, catalog-neutral Iceberg REST config
  negotiation runner with anonymous/OAuth2 authentication, exact profile and
  scenario projection, bounded and recursively sanitized HTTP evidence, config
  map/media/prefix/endpoint assertions, predeclared unsupported classification,
  and overwrite-safe production CLI output. Added exact Apache Iceberg 1.11.0
  OpenAPI provenance, portable OAuth environment bindings, production-optimized
  same-Docker Rust builds, typed Polaris reconciliation, generic catalog
  readiness gates, comprehensive Rust/Go tests, and operator documentation.
  Live smoke transcripts remain non-publishable until C1-09 wraps reviewed
  evidence in immutable result bundles.

- Catalog community C1-02: added a typed, schema-backed catalog adapter contract
  with exact Iceberg REST config/prefix/auth routing, exhaustive 27-capability
  coverage, protocol-native versus behavior-changing-shim disclosure, secret and
  endpoint drift rejection, and complete current-profile bindings for LakeCat,
  Polaris, Gravitino, Lakekeeper, and Nessie; preserved historical profile bytes,
  regenerated affected schemas, and documented the no-shim semantics and gates.

- Phase 1 infrastructure: made the benchmark Compose project own its Docker
  network, exact source-built MinIO release, idempotently initialized warehouse
  bucket, and state volumes. Added digest-pinned Lakekeeper 0.13.3 and PostgreSQL
  17.11 services with migration, process-health, management-bootstrap, warehouse,
  and isolated-state readiness gates; typed/tested MinIO and Lakekeeper setup
  helpers that reconcile current state and fail on configuration drift; and
  current operations documentation. The final public benchmark artifact
  pipeline remains explicitly assigned to C1-09.
- Contract test portability: embedded checked-in profiles, scenarios, and schemas
  in the integration-test binary so a shared Cargo target directory cannot reuse
  stale absolute paths from a removed clean worktree.
- Documentation quality: escaped the write-data example URI so workspace
  Rustdoc builds are warning-free under `-D warnings`.
- Historical evidence: added a deterministic importer that hash-checks and
  recomputes the 2026-08-08 raw TSV evidence into four typed aggregate result
  records and an immutable bundle manifest. Added bundle-wide digest, identity,
  scenario, assertion, and evidence validation plus a generated concurrent
  matrix that ranks only `pass` outcomes and preserves Nessie's diagnostic
  measurements as an unranked `fail`.
- Result provenance: modeled single executions and multi-round aggregates as
  distinct run variants with explicit included/excluded repetitions and rules.
- Phase 0 pinsets: added a runnable reconstruction of the 2026-08-08 Linux ARM64
  commit environment, an explicitly draft 2026-08-26 catalog/client/engine
  profile, and a neutral versioned same-table contention scenario.
- Component taxonomy: added an explicit connector kind for engine/catalog runtime
  artifacts such as Apache Iceberg Java bundles.
- Evidence fidelity: environment values now encode exact, approximate, or unknown
  precision, allowing historical imports to retain incomplete hardware/runtime
  capture without fabricated values.
- Build provenance: generalized component build options and compiler flags so
  Rust, Go, C++, Java, and other toolchains share one neutral recipe shape.
- Profiles: distinguished runnable profiles from draft pinsets. Runnable profiles
  now reject unresolved source-build or package artifacts; drafts must enumerate
  every unresolved component and cannot silently look executable.
- Provenance: normalized source and build identity at the component boundary so
  source-built container images retain both their revision/build recipe and their
  scoped image plus embedded-executable digests.
- Contracts: added the catalog-neutral `catalog-bench/v1` scenario, profile,
  result, and bundle-manifest ADTs; checked-in Draft 2020-12 JSON Schemas; strict
  semantic validation and evidence-sanitization gates; and a stable-Rust CLI
  that regenerates, drift-checks, and validates contract documents. Tests now
  live outside production modules.
- Reproducibility: replaced three ambient `../../../sail` path dependencies with
  one immutable `querygraph/sail@bddb1706` workspace dependency. A standalone
  checkout now resolves the same Foyer object-store implementation regardless of
  neighboring directories, and `--locked` checks no longer depend on local Sail
  state.
