# Catalog-bench interoperability contract

`catalog-bench/v1` is the durable, catalog-neutral publication boundary for the
Open Catalog Interoperability Lab. It complements the small `BenchReport` emitted
between a benchmark process and the local driver; it does not replace that fast
process-local format.

The contract has four independently versioned document kinds:

| Kind | Checked-in schema | Purpose |
|---|---|---|
| `scenario` | [`scenario.schema.json`](../schemas/v1/scenario.schema.json) | Neutral steps, prerequisites, assertions, and classification policy. |
| `profile` | [`profile.schema.json`](../schemas/v1/profile.schema.json) | Exact component/source/image pins and sanitized topology. |
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

## Evidence and reproducibility rules

Every result repeats the readable catalog and client name/version while referring
to an immutable profile digest for full source and artifact identity. Every
profile component records one of:

- a container reference plus index and optional platform digest;
- an immutable source revision, executable digest, and locked build settings; or
- an ecosystem package name, version, and optional package digest.

Each result embeds its actual OS, architecture, CPU, memory, limits, runtime,
network, and behaviorally relevant flags. Measurements preserve elapsed time,
sample count, ranges, arbitrary named quantiles, and counters or ratios. Semantic
validation rejects non-finite values, inverted ranges, non-monotonic quantiles,
zero-denominator ratios, duplicate identifiers, dangling evidence references,
and a `pass` that hides a failed required assertion.

Artifacts are addressed by an explicit digest object. The digest covers the
artifact's exact bytes—not a reserialized JSON value—so whitespace and final
newlines are significant. Manifests identify whether evidence is a `live-run`,
`historical-import`, or `fixture`; imported legacy data can never masquerade as a
new execution.

Evidence entering a publishable result must set `sanitized: true`. The manifest
also requires a completed redaction review. Profile settings reject secret-shaped
keys; this is a guardrail, not a substitute for the repository's artifact secret
scan.

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

Writers should serialize deterministically, append one newline, hash those bytes,
then create references. A bundle-level validator must additionally verify
referenced bytes, profile component bindings, scenario assertion IDs, and copied
required flags; per-document validation cannot prove those cross-file
relationships by itself.

## Commands

Run from the repository root:

```sh
# Detect Rust/schema drift without writing files.
cargo run -p catalog-bench-contract --locked -- schemas check

# Intentionally regenerate all four checked-in schemas.
cargo run -p catalog-bench-contract --locked -- schemas write

# Deserialize and semantically validate one file or a directory tree.
cargo run -p catalog-bench-contract --locked -- validate path/to/documents
```

`validate` recurses through directories and examines `.json` files. Schema files
themselves are inputs to `schemas check`, not contract documents.
