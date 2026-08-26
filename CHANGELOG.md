# Changelog

## Unreleased

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
