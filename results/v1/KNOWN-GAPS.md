# Catalog Bench Known Gaps

This page is generated from validated non-pass results and assertion outcomes. Absence from this page is not a claim of untested support.

## Apache Nessie — 0.108.4

- Bundle: `commit-2026-08-08`
- Scenario: `iceberg-rest.commit.same-table-contention` v1
- Outcome: `fail`
- Assertion gaps:
  - `zero-request-errors` (required): 97 non-conflict request errors occurred across all five measured rounds.

## Lakekeeper — 0.13.3

- Bundle: `contention-2026-08-27-c110`
- Scenario: `iceberg-rest.commit.same-table-contention` v2
- Outcome: `fail`
- Assertion gaps:
  - `zero-request-errors` (required): 58 non-conflict request errors occurred across conditioning and measured repetitions.

## Apache Nessie — 0.108.4

- Bundle: `contention-2026-08-27-c110`
- Scenario: `iceberg-rest.commit.same-table-contention` v2
- Outcome: `fail`
- Assertion gaps:
  - `zero-request-errors` (required): 106 non-conflict request errors occurred across conditioning and measured repetitions.

## Apache Gravitino — 1.3.0

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `client.pyiceberg.interoperability` v1
- Outcome: `pass`
- Assertion gaps:
  - `view-lifecycle-classified` (optional): unsupported by client: PyIceberg 0.11.1 exposes list/drop/exists helpers but no public create_view or load_view API, so a stock-client lifecycle cannot be sent.
  - `pagination-classified` (optional): unsupported by client: PyIceberg 0.11.1 parses next-page-token fields but its public list_namespaces and list_tables methods neither accept page controls nor traverse returned tokens.

## LakeCat — 0.3.0-42-g962f43cb

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `client.pyiceberg.interoperability` v1
- Outcome: `pass`
- Assertion gaps:
  - `credential-vending-classified` (optional): unsupported by catalog: table response config supplied no delegated credential category; the common workflow used fixed fixture credentials
  - `view-lifecycle-classified` (optional): unsupported by client: PyIceberg 0.11.1 exposes list/drop/exists helpers but no public create_view or load_view API, so a stock-client lifecycle cannot be sent.
  - `pagination-classified` (optional): unsupported by client: PyIceberg 0.11.1 parses next-page-token fields but its public list_namespaces and list_tables methods neither accept page controls nor traverse returned tokens.

## Lakekeeper — 0.13.3

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `client.pyiceberg.interoperability` v1
- Outcome: `pass`
- Assertion gaps:
  - `view-lifecycle-classified` (optional): unsupported by client: PyIceberg 0.11.1 exposes list/drop/exists helpers but no public create_view or load_view API, so a stock-client lifecycle cannot be sent.
  - `pagination-classified` (optional): unsupported by client: PyIceberg 0.11.1 parses next-page-token fields but its public list_namespaces and list_tables methods neither accept page controls nor traverse returned tokens.

## Apache Nessie — 0.108.4

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `client.pyiceberg.interoperability` v1
- Outcome: `pass`
- Assertion gaps:
  - `credential-vending-classified` (optional): unsupported by catalog: table response config supplied no delegated credential category; the common workflow used fixed fixture credentials
  - `view-lifecycle-classified` (optional): unsupported by client: PyIceberg 0.11.1 exposes list/drop/exists helpers but no public create_view or load_view API, so a stock-client lifecycle cannot be sent.
  - `pagination-classified` (optional): unsupported by client: PyIceberg 0.11.1 parses next-page-token fields but its public list_namespaces and list_tables methods neither accept page controls nor traverse returned tokens.

## Apache Polaris — 1.7.0

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `client.pyiceberg.interoperability` v1
- Outcome: `pass`
- Assertion gaps:
  - `view-lifecycle-classified` (optional): unsupported by client: PyIceberg 0.11.1 exposes list/drop/exists helpers but no public create_view or load_view API, so a stock-client lifecycle cannot be sent.
  - `pagination-classified` (optional): unsupported by client: PyIceberg 0.11.1 parses next-page-token fields but its public list_namespaces and list_tables methods neither accept page controls nor traverse returned tokens.

## Apache Gravitino — 1.3.0

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.commit.correctness` v1
- Outcome: `pass`
- Assertion gaps:
  - `idempotency-support-advertised` (optional): config does not advertise idempotency-key-lifetime
  - `exact-request-replayed-once` (optional): config does not advertise idempotency-key-lifetime
  - `idempotency-content-drift-rejected` (optional): config does not advertise idempotency-key-lifetime

## LakeCat — 0.3.0-42-g962f43cb

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.commit.correctness` v1
- Outcome: `pass`
- Assertion gaps:
  - `idempotency-support-advertised` (optional): config does not advertise idempotency-key-lifetime
  - `exact-request-replayed-once` (optional): config does not advertise idempotency-key-lifetime
  - `idempotency-content-drift-rejected` (optional): config does not advertise idempotency-key-lifetime

## Lakekeeper — 0.13.3

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.commit.correctness` v1
- Outcome: `fail`
- Assertion gaps:
  - `stale-requirement-rejected-atomically` (required): error type `CatalogCommitConflicts` does not match `CommitFailedException`
  - `idempotency-content-drift-rejected` (optional): HTTP 200 is not in [409]

## Apache Nessie — 0.108.4

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.commit.correctness` v1
- Outcome: `fail`
- Assertion gaps:
  - `stale-requirement-rejected-atomically` (required): error type is empty
  - `idempotency-support-advertised` (optional): config does not advertise idempotency-key-lifetime
  - `exact-request-replayed-once` (optional): config does not advertise idempotency-key-lifetime
  - `idempotency-content-drift-rejected` (optional): config does not advertise idempotency-key-lifetime

## Apache Polaris — 1.7.0

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.commit.correctness` v1
- Outcome: `pass`
- Assertion gaps:
  - `idempotency-support-advertised` (optional): config does not advertise idempotency-key-lifetime
  - `exact-request-replayed-once` (optional): config does not advertise idempotency-key-lifetime
  - `idempotency-content-drift-rejected` (optional): config does not advertise idempotency-key-lifetime

## Apache Polaris — 1.7.0

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.config.negotiation` v1
- Outcome: `fail`
- Assertion gaps:
  - `endpoint-advertisement-valid` (required): `GET polaris/v1/{prefix}/namespaces/{namespace}/generic-tables` is not an Apache Iceberg 1.11.0 REST endpoint

## Apache Nessie — 0.108.4

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.namespace.behavior` v1
- Outcome: `fail`
- Assertion gaps:
  - `missing-parent-error-spec-shaped` (required): HTTP 200 is not in [404]

## Apache Polaris — 1.7.0

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.namespace.behavior` v1
- Outcome: `pass`
- Assertion gaps:
  - `namespace-properties-updated` (optional): HTTP 409 is not in [200]

## Apache Nessie — 0.108.4

- Bundle: `phase1-behavior-2026-08-28`
- Scenario: `iceberg-rest.table.behavior` v1
- Outcome: `fail`
- Assertion gaps:
  - `missing-namespace-error-spec-shaped` (required): HTTP 200 is not in [404]

