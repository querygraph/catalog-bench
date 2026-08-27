# Changelog

## Unreleased

- C1-08 contention round executor: run collision-safe setup, baseline MinIO
  audit, exact warmup and sequential phases, and barrier-synchronized timed
  writers through injected catalog and object-store ports. Every request that
  starts before the deadline now completes and is classified; measured phases
  reuse the setup UUID without hidden table loads; accepted identities cross
  the evidence boundary only as SHA-256 values; final table state and metadata
  growth fail closed; and every post-mutation exit performs verified non-purging
  cleanup. Separate workflow tests cover a passing contended round, no-mutation
  fixture collisions, ambiguous setup cleanup, concurrent request errors,
  failed object audits, metadata undercount, identity redaction, and the absence
  of setup I/O from measured latency.

- C1-08 REST and object-store ports: bind each run-owned fixture to precomputed
  standard Iceberg REST routes outside measured commit latency; require a
  committed format-v2 table snapshot; send only `assert-table-uuid` and one
  unique set-properties update; classify HTTP 200, HTTP 409, and bounded
  explicit failures separately; and hard-code non-purging cleanup without an
  arbitrary-header escape hatch. Added a credential-redacting MinIO auditor
  that recursively consumes every paginated object-list result under the exact
  returned table root, counts only `.metadata.json` objects, totals bytes, and
  verifies the transcript-referenced pointer. Separate integration tests cover
  request shape, route shape, location drift, format drift, oversized bodies,
  nested metadata, sibling exclusion, missing objects, bucket drift, path
  escape, and non-metadata pointers.

- Shared profile-driven catalog runtime: let performance runners reuse the
  conformance suite's tested OAuth2, config negotiation, static/negotiated/
  unprefixed routing, and namespace encoding through a clone-cheap session.
  Standard JSON requests deliberately expose no arbitrary-header hook, retain
  bearer tokens and response bodies only in non-serializable state, redact and
  bound failure details, and either privately collect or allocation-efficiently
  drain every response under the common one-MiB limit. Added anonymous, OAuth,
  routing, credential-secrecy, no-idempotency-header, private-body, bad-config,
  and oversized-response integration coverage without changing existing probe
  behavior.

- C1-08 contention benchmark core: add a typed, canonical scenario/profile
  boundary; balanced rotate-left conditioning and measured-round planning;
  collision-safe per-catalog/per-round fixtures; deterministic finite latency,
  throughput, quantile, median, and range statistics; and complete
  accepted/conflict/error accounting. Raw request identities now live only in
  redacted, non-serializable in-memory types, while final-state evidence retains
  validated SHA-256 values. Duplicate identities, malformed hashes, unaccounted
  requests, regressed metadata counts, zero elapsed time, non-finite samples,
  behavior-changing shims, and shared-object-store drift all fail closed in
  focused integration tests kept outside the implementation modules.

- Catalog community C1-08 contention contract: preserve the historical v1
  scenario bytes while adding a strict v2 authority for profile-driven routing,
  collision-safe fixtures, synchronized writers, complete request and latency
  accounting, final-state attribution, table-root-scoped MinIO growth,
  non-purging cleanup, sanitized no-overwrite evidence, rotated conditioning
  and measured rounds, and median-with-range aggregation. The common workload
  explicitly omits asymmetric idempotency headers. Added focused contract tests
  and corrected the documented current capability count.

- Catalog community C1-07 acceptance: document the exact commit-built stock
  PyIceberg runtime and production LakeCat artifact, five-catalog required and
  optional matrix, four exact row-state digests, all 135 retained metadata,
  manifest, and Parquet objects in shared MinIO, delegated-credential category
  boundaries, complete cleanup and sanitization audit, catalog deployment
  corrections, rejected diagnostics, reproduction workflow, and the C1-09
  publication boundary.

- C1-07 catalog data-plane reconciliation: make Nessie's client-visible S3
  endpoint resolve to shared MinIO inside the benchmark network, enable
  Gravitino's documented `s3-secret-key` credential provider, and extend the
  typed Polaris bootstrap to idempotently grant and verify
  `CATALOG_MANAGE_CONTENT` on `catalog_admin` while enabling MinIO-backed STS
  credential vending with an explicit fixture role. Omit PyIceberg 0.11.1's
  optional `s3.force-virtual-addressing` flag because its stock S3FS adapter
  treats the non-empty string `false` as enabled. Added deterministic grant
  creation/no-op/failure tests and static regressions for the same-Docker
  Nessie and effective Gravitino configuration boundaries.

- C1-07 stock-runtime completeness: select PyIceberg's public no-op auth manager
  for anonymous adapters instead of its legacy `Bearer None` fallback, and add
  profile-pinned S3FS 2026.7.0 plus all exact transitive wheels so
  catalog-selected `FsspecFileIO` remains a stock supported path. Runtime
  identity, transcript provenance, contracts, tests, and profile documentation
  now cover both object-store data planes.

- C1-07 live-smoke corrections: construct Arrow batches with the scenario's
  required `id` nullability instead of relying on nullable inference, and make
  embedded-secret rejection inspect evidence values while comparing map keys
  exactly so short fixture credentials cannot collide with safe schema field
  names. Added regressions for both representation boundaries.

- Catalog community C1-07 reproducible client image: build CPython 3.13.15 from
  the profile's Linux ARM64 child manifest, install all 41
  PyIceberg/PyArrow/S3FS
  distributions from exact wheel hashes, and run the stock-client oracle as an
  unprivileged read-only Compose service on the catalogs' shared Docker network
  and MinIO. Added exact five-catalog startup, readiness, smoke-matrix,
  classification, cleanup, security, and lock-maintenance documentation, plus a
  deployment regression test that binds image, lock, profile, and Compose
  invariants together.

- Catalog community C1-07 stock-client runner: execute the pinned PyIceberg
  namespace/table round trip, real Arrow append and exact scan, independent
  property/schema/delete/conflict/delegation/rename/register classifications,
  explicit client-level view and pagination limitations, conservative fixture
  reconciliation, and immutable value-sanitized transcripts across all five
  protocol-native adapters. Strict contract loading rejects workload drift and
  behavior-changing shims; deterministic fakes cover successful and refusing
  paths without replacing the production stock client.

- Catalog community C1-07 contract: pin the stock PyIceberg runtime and Arrow
  data plane, split optional client operations into explicit capabilities, and
  define a no-shim five-catalog workflow whose evidence distinguishes pass,
  fail, client/catalog unsupported, and dependency-not-evaluated outcomes.

- LakeCat canonical provenance repair: repin every current-profile and
  conformance milestone to its reachable commit after a privacy-only history
  rewrite. Verified `Cargo.toml`, `Cargo.lock`, and the complete `crates/` tree
  are source-identical across each rewritten milestone, rebuilt the exact
  C1-06 LakeCat pin with the production recipe, and reran the five-catalog
  commit matrix plus all 16 direct MinIO object checks.

- Catalog community C1-06 acceptance: document the exact stable-Rust,
  production-optimized commit-correctness runner, five-catalog required and
  config-gated optional matrix, direct audit of all 16 transcript-referenced
  metadata objects in shared MinIO, complete cleanup and sanitization evidence,
  Lakekeeper's and Nessie's error-envelope mismatches, Lakekeeper's exact-replay
  success and content-binding defect, rejected runner diagnostics, reproduction
  workflow, and the C1-09 publication boundary.

- C1-06 optional-branch independence: permit advertised idempotency checks after
  the required final-state reload proves the complete baseline unchanged, even
  when the stale response's status/type envelope fails its separate required
  assertion. Unsafe or mutated final state still suppresses every optional
  request.

- C1-06 successful-transition projection: compare the scenario-owned
  `catalog-bench.*` and `c1-06.*` properties exactly while treating unrelated
  catalog-managed metadata properties as opaque across admitted commits. Exact
  replay and rejected stale/content-drift checks still compare the complete
  property map, so this permits legitimate values such as Nessie's changing
  commit ID without weakening atomicity.

- C1-06 operator guidance: document the optimized Docker invocation for commit
  correctness, its deterministic required branch, config-gated optional UUIDv7
  replay checks, collision and cleanup guarantees, and the distinction between
  mutable smoke transcripts and publishable result bundles.

- Catalog community C1-06 runner: implement a strict Iceberg REST commit
  correctness probe with matching requirement admission, a deterministic stale
  schema conflict and atomicity proof, config-gated UUIDv7 exact replay and
  content-binding checks, full fixture reconciliation, and typed idempotency
  handling that can send raw keys without serializing them into evidence.

- C1-06 protocol preparation: extract committed-table request construction,
  profile-root location derivation, namespace response validation, and generic
  Iceberg metadata/schema snapshots into one reusable conformance module. The
  C1-05 runner retains its exact scenario and evidence shape while commit
  correctness can reuse the same protocol parser instead of cloning it.

- Catalog community C1-06 contract: define a neutral Iceberg REST commit
  correctness scenario that proves valid requirement admission, a deterministic
  stale-schema 409 with no mutation, UUIDv7 exact-retry behavior when advertised,
  same-key content-drift rejection, complete fixture reconciliation, and
  sanitized evidence without turning optional idempotency support into a hidden
  required capability.

- Catalog community C1-05 acceptance: documented the exact stable-Rust,
  production-optimized table-conformance runner and LakeCat artifact,
  five-catalog required/optional matrix, direct audit of all 15 referenced
  metadata objects in shared MinIO, complete cleanup and sanitization evidence,
  LakeCat's repaired no-snapshot rename defect, Gravitino's repaired deployment
  defaults, Nessie's narrow missing-namespace mismatch, rejected exploratory
  evidence, reproduction workflow, and the C1-09 publication boundary.

- C1-05 Gravitino state initialization: added a least-privilege one-shot that
  prepares only Gravitino's named state volume for the image's UID 1000 before
  the catalog starts. Fresh SQLite-backed deployments no longer fail with a
  root-owned volume, and the catalog process itself remains unprivileged.

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
  `lakecat@ef94b550` (`v0.3.0-32-gef94b550`). The pin includes register and
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
  LakeCat namespace implementation `lakecat@c821a0dc`. Both source builds retain
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
  endpoint-correction revision `10d98cbe`, and modeled the conformance runner as
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
