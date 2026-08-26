# Iceberg REST Namespace Conformance

This document records the C1-04 namespace-behavior acceptance run on
2026-08-26. It is a conformance matrix, not a latency or throughput ranking.
The runner applies the same typed Iceberg REST workflow to every catalog and
classifies required and optional behavior separately.

The checked-in authority is
[`iceberg-rest.namespace.behavior`](../scenarios/v1/iceberg-rest.namespace.behavior.json).
Its protocol source is the
[Apache Iceberg 1.11 REST OpenAPI](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/open-api/rest-catalog-open-api.yaml).

## What the scenario proves

Each invocation creates a run-owned fixture consisting of two top-level
namespaces and one multipart child. Before the first mutation, all three
identifiers must return spec-shaped 404 responses; if any fixture already
exists, the runner refuses every mutation and therefore cannot delete someone
else's data.

The workflow then proves:

- exact create, top-level list, and load round trips;
- stored create properties and the optional property-update operation;
- multipart encoding with the negotiated namespace separator;
- immediate-child hierarchy rather than flattened descendants;
- duplicate create as HTTP 409 with an Iceberg error envelope;
- missing-parent list as HTTP 404 with an Iceberg error envelope;
- bounded pagination from an explicit empty token, including token-loop,
  duplicate, loss, page-size, and maximum-page checks;
- the OpenAPI-permitted unpaginated fallback when a server ignores pagination;
- child-first drop and post-drop 404 verification even after an earlier
  assertion fails; and
- recursive transcript sanitization without persisted credentials, bearer
  tokens, page tokens, or raw response bodies.

There are twelve required behavior assertions and one optional
`namespace-properties-updated` assertion. An optional failure remains visible
but does not turn an otherwise conformant required workflow into a failure.

## Exact execution identity

All measured processes ran on `catalog-bench-net`; no catalog request crossed
the host network. The conformance runner and LakeCat were fully optimized Linux
ARM64 executables built inside Docker from read-only committed source checkouts.

| Item | Exact identity |
| --- | --- |
| Candidate profile | SHA-256 `db90aba01066ab2bcfc4843915c70020c53ffbe29f86ae25cb5fb553f531f286` |
| Namespace scenario | SHA-256 `0cd6262c9bda87ac217e8fc618cf3138ddabe6ca89aac94ee05628a67729b7ac` |
| Runner source | `catalog-bench@1f4e640566906ded6aa0589d52351eb1c32788f0` |
| Runner executable | SHA-256 `6a81806f955924dd2961bc6bfe68fab97cd24d302a50532d6410bccbf9c0f78e` |
| LakeCat source | `lakecat@42b2f34b85d7cbcce1b36d4008211075b6c51593` |
| LakeCat executable | SHA-256 `5a6a867c0e3923505f107d418f2a3cc327fd7fa73566b9ac89af77dc588ab839` |
| LakeCat runtime image | `lakecat-service@sha256:33dfed34779cd601cf8b98b30dde49d0f363020b0daac8f27baa35756e118691` |
| Rust toolchain | `rustc 1.97.1 (8bab26f4f 2026-07-14)`; `cargo 1.97.1 (c980f4866 2026-06-30)` |
| Production profile | `opt-level=3`, fat LTO, one codegen unit, stripped symbols, aborting panics, disabled incremental compilation, `-Dwarnings`, `-Ctarget-cpu=native`, `-j1` |
| LakeCat features | `turso-local,sail-local` |

The comparison catalog images were the profile-pinned artifacts:

| Catalog | Version | Image digest |
| --- | --- | --- |
| Apache Gravitino | 1.3.0 | `sha256:80136ae753ee77735153fc1482a389018f8c2638a54f453cb96967c7194584c7` |
| Lakekeeper | 0.13.3 | `sha256:db2ba6168eb107f22242fb7f2edc4016fa35e57bdcc606894e809c418e32e8dc` |
| Apache Nessie | 0.108.4 | `sha256:c0f42874c810f28ac30fc991e979c1b8cf5a2cbfa94212086cdddeae49629517` |
| Apache Polaris | 1.7.0 | `sha256:3495f67f38cca33892a045f7dd3f46eb52387f0fd52d4145538a772fd8aedad7` |

LakeCat's optimized build completed in 25 minutes 42 seconds. The Docker
daemon's registry-metadata resolver was unresponsive during this session, so
the exact already-local Rust 1.97.1 layer and Cargo source caches were reused in
an isolated builder container instead of restarting or pruning Docker. Package
resolution, compilation, linking, runtime packaging, and all probes still ran
inside Docker. The canonical equivalent recipe remains
[`docker/lakecat/Dockerfile`](../docker/lakecat/Dockerfile).

## Required-behavior matrix

| Catalog | Required result | Optional property update | Pagination observation | Sanitized transcript SHA-256 |
| --- | --- | --- | --- | --- |
| LakeCat | **pass** | pass | 13 pages, 13 unique top-level namespaces | `f344244b1b0a586728e37126725b0fa0be9729a01a3af832018bcb8403a4b854` |
| Apache Gravitino | **pass** | pass | 2 pages, 2 unique namespaces | `80ac2d1ffd244fa7546516c19324c34ddf2a6e01b113b5ba1d014dcb00f2956c` |
| Lakekeeper | **pass** | pass | 3 pages, 2 unique namespaces, including terminal traversal | `2eb9bdd1d704a981b0cde73fdaf7154f2cc5f3d414e3bde8eee795f312c75ada` |
| Apache Polaris | **pass** | fail: HTTP 409 instead of 200 | standards-permitted unpaginated fallback, 2 unique namespaces | `4d77732a6dbc801ea70c7c486ced8e03aed24e27226ba929e6317c171c93e88a` |
| Apache Nessie | **fail** | pass | 2 pages, 2 unique namespaces | `f5813af7285ead3ea5947a62d8d99dea79e83fb97974ccd9985114cd35c45eab` |

Every invocation passed fixture isolation, cleanup, and transcript-sanitization
checks. The page counts describe the complete top-level catalog state observed
by a one-item page request, not fixture counts and not performance measurements.
LakeCat's existing durable test database contained more unrelated top-level
namespaces than the fresh comparison stores; the runner required exact
no-loss/no-duplication traversal and removed only its three run-owned fixtures.

## LakeCat defect and repair

The exploratory pre-fix LakeCat transcript
`b520f308debbdfc80b3ecb17053de47113c7923b9a5a3eff38f144e2e5db9506`
failed required behavior for two related reasons:

1. the decoded `%1F` unit separator was treated as an unsupported character
   inside one namespace component instead of separating a multipart identifier;
2. `parent` was ignored, so the list operation could not prove immediate-child
   hierarchy or missing-parent semantics.

The accepted LakeCat revision adds a dedicated REST namespace codec, stable
pagination, parent existence checks, durable properties, exact 422 handling for
overlapping update/removal keys, and governed/redacted replay evidence. Memory
storage now keeps namespace identity and properties in one atomic map. Turso
uses a transactional side table; an old namespace row without a property row
loads as an empty property map and is lazily materialized on its first update.

The post-fix transcript `f344244b...a4b854` passes every required and optional
assertion. The production-feature test gate also passed 457 service tests and
193 Turso-store tests, including legacy-row migration and outbox-drain coverage.

## Why Nessie fails

Nessie completes fixture creation, hierarchy, property update, pagination,
duplicate rejection, and cleanup. Its one required failure is narrow and
deterministic: listing under a run-owned absent parent returns HTTP 200 with an
empty namespace page. Apache Iceberg 1.11 specifies HTTP 404 for a missing
`parent` query parameter. The runner therefore reports:

```text
missing-parent-error-spec-shaped: HTTP 200 is not in [404]
```

This finding is unrelated to Nessie's historical concurrent-commit HTTP 500
failure. C1-04 does not infer a performance result from namespace conformance.

## Why Polaris still passes required behavior

Polaris returns a valid required namespace lifecycle, hierarchy, duplicate,
missing-parent, cleanup, and unpaginated listing workflow. Its property-update
request returns HTTP 409 instead of the expected 200. The scenario and profile
classify namespace property update as optional because Iceberg explicitly says
servers are not required to support namespace properties. The failed optional
assertion is retained in evidence; it is not silently converted to pass and it
does not invalidate the required result.

## Reproduction

Build and start the optimized conformance services, then choose a fresh fixture
suffix and a new output file:

```sh
docker compose --profile conformance build lakecat conformance
docker compose --profile conformance up --detach --force-recreate lakecat
docker compose --profile conformance run --rm conformance namespace \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/iceberg-rest.namespace.behavior.json \
  --catalog lakecat \
  --fixture-id review_lakecat_01 \
  --output /evidence/review-lakecat-01.json
```

Use the same binary and scenario with `gravitino`, `lakekeeper`, `nessie`, or
`polaris`. Polaris also needs the profile-declared OAuth environment variables.
Exit 0 means all required assertions passed; exit 2 means the transcript was
successfully written with `fail` or `unsupported`; exit 1 is an invocation,
contract, or I/O failure.

## Deliberate non-claims and follow-up

- These transcripts are ignored smoke evidence, not checked-in
  `catalog-bench/v1` result records. C1-09 owns immutable bundle materialization,
  environment capture, manual redaction review, secret scanning, and site/report
  generation.
- C1-04 records behavior only. It does not change the generated concurrent
  throughput ranking and does not compare latency, CPU, or memory.
- The fixture grammar is intentionally portable across the five catalogs. It
  does not yet test arbitrary punctuation or the pre-existing LakeCat internal
  ambiguity between a literal dotted component and a dot-joined multipart
  storage path. That broader identifier-key migration remains explicit LakeCat
  conformance debt rather than being hidden by this pass.
