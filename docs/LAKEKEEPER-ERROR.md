# Why Lakekeeper's result is an unranked `fail`

Lakekeeper 0.13.3 entered the same production benchmark as every other catalog,
completed every round, and remains unranked because every round returned
non-conflict HTTP 503 errors. This is not a LakeCat/Turso diagnosis: the pinned
Lakekeeper deployment uses PostgreSQL 17.11 for its private catalog state, and
its own logs attribute the 503 responses to PostgreSQL deadlocks.

## What happened

The C110 sweep ran one conditioning and five measured repetitions. In all six,
Lakekeeper passed fixture isolation, setup, warmup, sequential accounting,
latency completeness, total request accounting, concurrent progress, final-state
attribution, MinIO metadata growth, and cleanup. Only the required
`zero-request-errors` assertion failed.

The complete transcript records:

| Scope | Attempts | Accepted | HTTP 409 | HTTP 503 |
|---|---:|---:|---:|---:|
| Conditioning | 43 | 27 | 5 | 11 |
| Five measured rounds | 75 | 25 | 3 | 47 |
| Total | 118 | 52 | 8 | 58 |

The measured row retains a 123.066/s median sequential accepted throughput and a
0.539/s diagnostic concurrent accepted throughput. Those numbers are real, but
they cannot be ranked: the benchmark ranks progress only after every required
correctness assertion passes.

## The failure, precisely

The reviewed Lakekeeper log reports a `CatalogBackendError` with HTTP 503:
PostgreSQL returned `deadlock detected at line 1130` while Lakekeeper was
setting table properties and committing table changes. A subsequent warning
records that the operation still failed after its retry attempts.

The matching PostgreSQL log identifies two transactions each waiting for a
`ShareLock` held by the other. Both execute the same statement shape:

1. build a `new_props` common table expression;
2. delete obsolete rows from `table_properties`; and
3. insert current rows with `ON CONFLICT ... DO UPDATE`.

PostgreSQL observed the deadlock while inserting an index tuple in
`table_properties`. The publication keeps this causal statement shape while
removing process, transaction, warehouse, table, request, and error identifiers
that are unnecessary to understand or reproduce the result.

## Why this is an error rather than a conflict

The workload intentionally creates stale same-table writers. A catalog can
correctly reject one as HTTP 409 after its optimistic requirement loses a race.
That is a measured conflict. An HTTP 503 says the catalog backend could not
complete the operation; it is neither an accepted commit nor a valid stale-state
classification.

Relabeling the 503 responses as conflicts would hide an operational failure and
make implementations with backend errors appear equivalent to implementations
that fail stale writes closed. The result contract therefore preserves separate
accepted, 409, and error counts, and `zero-request-errors` is mandatory.

## Why `retryable: false` does not deny transient deadlocks

An individual PostgreSQL deadlock can be transient and may be safe for a server
to retry inside a transaction boundary. The published result's `retryable`
field describes the aggregate benchmark classification, not PostgreSQL's error
class. The same failure recurred in all six rounds after Lakekeeper's own retry
path, so replaying this unchanged evidence cannot turn it into a pass. A fresh
run against a fix or materially different supported configuration is required.

## What would restore a rank

Lakekeeper re-enters the numeric ranking as soon as the same pinned protocol
completes:

- one conditioning and five measured rounds;
- eight concurrent writers for six seconds per round;
- zero non-conflict request errors;
- complete request and latency accounting;
- final-state attribution to an accepted request;
- sufficient MinIO metadata-object growth; and
- successful non-purging cleanup.

No benchmark exception or catalog-specific response mapping is needed. A fixed
release simply earns its rank from its measured concurrent accepted throughput.

## Evidence and reproduction

The [generated matrix](../results/v1/2026-08-27/MATRIX.md) is derived from
[Lakekeeper's typed result](../results/v1/2026-08-27/lakekeeper.json). Exact
round data is in the
[sanitized transcript](../results/contention-2026-08-27-transcript.json), and
the minimal reviewed log attribution is in the
[review sidecar](../results/contention-2026-08-27-review.json). Their hashes and
byte sizes are bound by the
[immutable manifest](../results/v1/2026-08-27/manifest.json).

```sh
cargo run -p catalog-bench-contract --locked -- contention-import check --root .
```

That check verifies the transcript and review hashes, independently reconstructs
the 30-round schedule and rank order, regenerates every result, validates all
cross-document links, and rejects a stale matrix.
