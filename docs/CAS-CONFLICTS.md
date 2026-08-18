# Understanding LakeCat's CAS Conflict Rate

## Short answer

LakeCat's 85.42% conflict rate in the concurrent commit benchmark is primarily
the expected result of eight writers racing to update the same Iceberg table.
It is not evidence that Turso is returning failures for 85% of requests.

The benchmark reports an HTTP 409 when a request reaches LakeCat with an
expected metadata pointer or table version that another writer has already
advanced. That is LakeCat's optimistic compare-and-swap (CAS) contract working
as designed. Turso's lower-level MVCC contention is retried inside LakeCat; if
it escaped that retry boundary, the benchmark would count it as a request error,
not as a conflict. LakeCat had a 0% request-error rate in every measured round.

## What the benchmark measures

The final public sweep is recorded in [RESULTS.md](../RESULTS.md) and the
[median summary](../results/commit-2026-08-08-summary.tsv). Each catalog run:

1. creates a fresh namespace and table;
2. performs 50 unmeasured warm-up commits;
3. performs 1,000 measured sequential commits; and
4. starts eight tasks that continuously commit to that one table for six
   seconds.

All eight tasks use the same table UUID, but they generate independent updates
and idempotency scopes. The driver classifies outcomes in
[`crates/commit/src/main.rs`](../crates/commit/src/main.rs):

- an accepted commit increments `ok`;
- HTTP 409 increments `conflict`; and
- every other failure increments `errors`.

The published conflict rate is:

```text
conflicts / (accepted commits + conflicts)
```

Errors are deliberately excluded from that denominator and reported
separately. This distinction prevents a backend failure from looking like valid
optimistic-concurrency behavior.

The measured LakeCat medians were:

| Metric | Result |
| --- | ---: |
| Successful concurrent commits | 153.0/s |
| Conflict rate | 85.42% |
| Request-error rate | 0% |
| Request errors across measured rounds | 0 |

## Why eight same-table writers conflict so often

An Iceberg commit is conditional on the table state from which it was planned.
For a simplified pointer sequence, suppose all writers initially observe
metadata pointer `P0`:

```text
writer A: read P0 ─ prepare metadata A ─ CAS P0 → P1 ─ accepted
writer B: read P0 ─ prepare metadata B ─ CAS P0 → P2 ─ 409 (P0 is stale)
writer C: read P0 ─ prepare metadata C ─ CAS P0 → P3 ─ 409 (P0 is stale)
...
```

LakeCat performs metadata preparation and the MinIO/S3 object write before the
short final catalog transaction. This preserves parallel object-store work, but
it also means several requests can legitimately arrive at the transaction with
the same expected predecessor. The first request advances the pointer; the
others must not silently overwrite it.

In a perfectly synchronized group of eight contenders, one winner and seven
stale writers would produce a 7/8, or 87.5%, conflict rate. The benchmark is a
continuous loop rather than synchronized batches, so writers drift in and out
of overlap. Its 85.42% median is consistent with roughly that contention shape;
it is not itself proof of groups of exactly eight.

The high rate is therefore also evidence that LakeCat enforces a strict pointer
CAS. Treating those stale requests as successful would lose updates.

## Where Turso is involved

Turso is LakeCat's durable catalog-state store. It holds the table pointer,
version, pointer log, audit event, transactional outbox event, and idempotency
result. The Iceberg `metadata.json` objects measured by this benchmark are still
written to the shared MinIO instance.

At the benchmarked LakeCat revision
[`3cca8d1c`](https://github.com/querygraph/lakecat/tree/3cca8d1c749fcf1c7cbd30661ba2bd4805b256d3),
the Turso path has two distinct kinds of concurrency outcome:

### 1. Physical MVCC contention: retry internally

[`write_txn`](https://github.com/querygraph/lakecat/blob/3cca8d1c749fcf1c7cbd30661ba2bd4805b256d3/crates/lakecat-store/src/turso_store/mod.rs#L130)
uses `journal_mode=mvcc` and `BEGIN CONCURRENT`. It retries these transient
outcomes with capped exponential backoff:

- `Busy`;
- `BusySnapshot`;
- `Write-write conflict`; and
- `Commit dependency aborted`.

The retry budget is eight attempts. A keyed weak-reference mutex covers only
the final transaction for one table, preventing a continuous stream of
same-table writers from exhausting that budget. Different tables use different
gates, and metadata preparation remains outside the gate.

These retries are an implementation detail below the HTTP response. If the
budget were exhausted and the Turso failure escaped, the benchmark would record
an error (normally an HTTP 500), not a 409. The final LakeCat sweep's zero errors
shows that no such failure escaped. It does not tell us how many internal Turso
retries occurred; measuring that requires an explicit retry counter or trace.

### 2. Logical table-state conflict: return 409

After any safe physical retry, LakeCat re-runs the transaction body on a fresh
snapshot. The commit checks the expected metadata location and uses a
conditional table update guarded by both the prior version and prior pointer.
See
[`commit_table_transaction`](https://github.com/querygraph/lakecat/blob/3cca8d1c749fcf1c7cbd30661ba2bd4805b256d3/crates/lakecat-store/src/turso_store/mod.rs#L2197).

If another writer has advanced the table, the requirement check or conditional
update fails and LakeCat returns a terminal logical `Conflict` (HTTP 409). That
outcome must not be retried blindly inside the database transaction because the
request was planned from stale Iceberg metadata.

Turso can affect timing and therefore the amount of overlap, but it is not the
semantic source of the reported 409s. The stale pointer/version is.

## Why other catalogs can show fewer 409s

The concurrent result combines acceptance policy with execution speed. A lower
409 rate can mean several different things:

- the catalog serializes a larger portion of the commit path;
- the client or server transparently refreshes and rebases an update;
- its protocol adapter uses different conflict semantics; or
- requests overlap less because each attempt takes longer.

It does not automatically mean that the catalog's state store handles
concurrency better. This is why the ranking reports accepted throughput,
conflict rate, and non-conflict errors separately. Nessie's HTTP 500s, for
example, are errors and make its row ineligible; they are not reclassified as
conflicts.

## How to isolate Turso performance from CAS policy

The existing same-table phase should remain: it is useful evidence that stale
writes fail closed and that physical contention does not leak as errors. A
complete concurrency picture should add complementary measurements:

1. **Distinct-table writers.** Give each writer its own table. This removes the
   intentional pointer hotspot and measures Turso MVCC, connection pooling, and
   shared catalog-page contention more directly.
2. **Attempted throughput.** Report accepted plus conflicting attempts per
   second alongside accepted commits per second. This shows the cost of
   detecting losers without pretending they committed.
3. **Internal retry telemetry.** Count Turso retry reasons and attempts, plus
   time spent waiting for the per-table gate. Zero HTTP errors alone cannot
   distinguish a conflict-free transaction from one that succeeded on its
   eighth attempt.
4. **Client-retry throughput.** On 409, reload the table, re-plan a safe update,
   and retry within a bounded end-to-end budget. This measures user-visible
   progress for retry-capable workloads.
5. **Synchronized contention groups.** Optionally release a fixed number of
   writers from a barrier. This makes winner/loser ratios easier to interpret
   than a continuous open loop.

These should be reported as separate scenarios, not merged into one score.

## Can LakeCat reduce the 409 rate?

Yes, but each option changes the semantics or the concurrency shape:

- **Rebase commutative updates.** Operations such as independent property
  changes can sometimes be re-applied to fresh metadata after a 409. This must
  re-run Iceberg validation and metadata generation, preserve idempotency, and
  clean up any uncommitted metadata object. It is not safe for every update.
- **Move the per-table gate earlier.** Locking before table load and metadata
  preparation would prevent most stale plans, but it would also serialize S3
  work and increase same-table latency. It trades conflicts for queuing rather
  than increasing actual parallel commit capacity.
- **Retry in the client.** This keeps generic catalog semantics simple and lets
  the caller decide whether an operation is safe to re-plan, at the cost of an
  extra round trip and metadata object work.
- **Weaken or remove the CAS.** This would lower the reported conflict rate by
  allowing lost updates. It is not a valid optimization.

The current design chooses parallel preparation, a short serialized final
transaction, strict CAS, and explicit 409s. For the benchmark's deliberately
pathological same-table workload, a high logical conflict rate is the expected
cost of that choice. Turso should be evaluated with the distinct-table and
internal-retry measurements above, not inferred from the 409 percentage alone.
