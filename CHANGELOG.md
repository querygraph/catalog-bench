# Changelog

## Unreleased

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
