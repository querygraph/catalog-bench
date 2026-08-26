# Why Nessie's result is an unranked `fail`

Apache Nessie posts the fastest raw successful concurrent throughput in the
final sweep — and its row carries no rank. Its outcome is `fail` because in every
measured round the server returned HTTP 500 responses, and a round with request
errors is not rank-eligible. This document is the complete account: what failed,
how we know it is the server and not the harness, why the result is `fail` rather
than "disqualified", why an earlier public row looked healthy, and what would
restore a numeric rank.

## What happened

In all five measured rounds of the 2026-08-08 final sweep, Nessie 0.108.4
returned a total of **97 HTTP 500 responses** (median error rate 0.366%)
alongside 190.0/s of successful commits. The benchmark's validity rule —
declared before the sweep, applied identically to every catalog — is that a
numeric rank requires **zero request errors in every measured round**. LakeCat,
Polaris, and Gravitino each completed 5/5 valid rounds with zero errors;
Nessie completed 0/5.

An HTTP 500 is not a conflict. A 409 conflict is the catalog *working*: a
stale writer correctly losing compare-and-swap. A 500 is the catalog
*failing*: the request neither succeeded nor was correctly refused, and the
client cannot know what state the server is in. Counting 500s as conflicts
would inflate the correctness story; counting them as successes would inflate
throughput; ignoring them — as an earlier driver version did — hides them
entirely. The only honest treatment is the one applied: report the raw
numbers, count the errors, and withhold the rank.

## The failure, precisely

Server logs across the rounds consistently identify a Quarkus
`ContextNotActiveException`: Nessie's asynchronous catalog work accesses
request-scoped state — `ObjectIO` / `S3ClientSupplier`, or
`SecurityIdentityProxy` — after the originating HTTP request's context has
been torn down. The relevant producers are explicitly `@RequestScoped` in
[Nessie 0.108.4's source](https://github.com/projectnessie/nessie/blob/nessie-0.108.4/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java):
an async `CompletableFuture` outliving its request races the container's
context teardown.

This is not a version artifact or a tuning artifact of our stack:

- Guarded preflights reproduced the same failure on **0.107.5, 0.107.6, and
  0.108.4** — three releases, including the one an earlier public row used.
- Reducing the async task pool from ten threads to one *reduced but did not
  eliminate* the failures; setting the task minimum delay to zero did not
  eliminate them either.
- The failure is load-sensitive, which is why a benchmark surfaced it: eight
  concurrent writers give the async work ample opportunity to outlive its
  request.

No patched or unreleased Nessie build is substituted in the public table; the
row reflects the latest official release at the time of the run.

## Why `fail`, not the legacy "DQ"

"Disqualified" implies a rules violation by the contestant. That framing is
wrong twice. First, Nessie did not break a benchmark rule — its *server
errored under load*, which is a measured result like any other, and arguably
the most operationally important one in the table. Second, "DQ" invites the
misreading that the row was excluded by judgment call. It was not: the
exclusion is mechanical (errors > 0 in a measured round), declared before the
sweep, and applied uniformly. The v1 contract uses a closed four-way outcome:
`pass`, `fail`, `unsupported`, or `not-tested`. `Fail` states the fact: this
scenario was attempted and a required assertion failed. The row's numbers and
speed remain real, but its speed cannot be ranked against passing rows.

The preserved legacy summary says `DQ`; the historical importer verifies that
field but does not perpetuate its ambiguous presentation label. The generated
matrix derives the stricter `fail` classification from the assertion evidence.

The row sits at the **bottom** of the table for the same reason. Sorting it
first by raw throughput — even flagged — rewards the failure mode: a reader
scanning the table sees Nessie on top and a footnote they may not read. A
ranking is a claim, and the top row is its loudest word; a row with zero
valid rounds has not earned it.

## Why an earlier public row looked healthy

The [previous public Nessie row](https://github.com/querygraph/catalog-bench/blob/9f3fc71e7815763dcc8987a89b6a36f61e59727c/RESULTS.md#commit-path-results)
(0.107.5) appeared error-free because the old concurrent worker
[deliberately discarded](https://github.com/querygraph/catalog-bench/blob/9f3fc71e7815763dcc8987a89b6a36f61e59727c/crates/commit/src/main.rs#L302-L310)
every request failure that was neither an accepted commit nor a 409:

```rust
Err(_) => { /* transient; keep going */ }
```

Those failures never reached the report, never moved the conflict rate, and
never failed the process. "The benchmark completed" meant only that enough
requests succeeded to produce a throughput number — not that Nessie returned
zero 500s. The old run kept no error counter, so its true failure count is
unrecoverable. The strict driver reproduced the same request-context failure
on the same 0.107.5 image, so the decisive change between the two publications
is **observability and validity, not a Nessie regression**: errors that were
silently dropped are now counted, and they void a round.

## What this is not

- **Not a claim that Nessie is slow.** Its 190.0/s median counts only
  successful commits and remains the fastest raw concurrent value measured.
- **Not a claim that Nessie always fails.** Sequential warmup and measurement
  phases completed; the failure concentrates under concurrent load.
- **Not a permanent verdict.** The moment a Nessie release survives the same
  protocol — five measured rounds, eight writers, zero request errors, MinIO
  object audit — its row re-enters the ranking at whatever position its
  numbers earn. We would genuinely like to publish that row: an error-free
  Nessie would be a serious contender.

## And why Unity Catalog OSS is not in the table at all

Nessie *entered and errored*. Unity Catalog OSS could not enter: there is a
difference between a contender that failed and a system with no way onto the
track. The contract renders that difference as an attempted `fail` result versus
an `unsupported` or `not-tested` result when there is no valid operation to run.

This benchmark measures exactly one axis: the **commit path** — an external
client asking the catalog to advance a table pointer over Iceberg REST.
Released Unity OSS (latest **0.5.0**) serves its Iceberg REST endpoint
(`/api/2.1/unity-catalog/iceberg`) **read-only**: it has no external
`updateTable` / `set-properties` commit handler, so a commit benchmark has
nothing to exercise. It is not slow at committing; it does not expose
committing. Write endpoints exist only in the **unmerged draft PR
[#1618](https://github.com/unitycatalog/unitycatalog/pull/1618)** ("Implement
Iceberg REST catalog write endpoints"), targeting an unreleased 0.6.0 — and
this suite benchmarks released, official images, for the same reason no
patched Nessie build is substituted above.

Two clarifications to head off misreadings:

- **Databricks-hosted Unity Catalog does have Iceberg REST writes.** That is a
  separate hosted product, not the Docker-deployable OSS server this suite
  measures; putting it in a table of self-hosted open-source catalogs on one
  shared MinIO would compare unlike things.
- **This is not a permanent exclusion.** The compose file already carries a
  `unity` profile, and the README records the exact one-liner to run the
  moment a write-capable build ships. When 0.6.0 (or a merged #1618) lands,
  Unity's row enters the same protocol as everyone else and earns whatever
  position its numbers deserve.

## Reproduce it

The full protocol, immutable image digests, binary hashes, build commands, and
per-run evidence (including the error counts) are in
[RESULTS.md](../RESULTS.md); the canonical generated ranking, typed result
records, and immutable manifest are under
[`results/v1/2026-08-08/`](../results/v1/2026-08-08/), while the preserved raw
TSVs remain directly under [`results/`](../results/). The stack is
Docker-composed; one command reruns the sweep against the official Nessie image.
