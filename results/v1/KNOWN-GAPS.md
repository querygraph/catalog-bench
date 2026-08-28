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

