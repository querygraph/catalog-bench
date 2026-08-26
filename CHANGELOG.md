# Changelog

## Unreleased

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
