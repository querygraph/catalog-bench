# Iceberg REST same-table commit contention

This document describes the current `catalog-bench-commit` performance runner.
It is an execution contract, not a catalog-specific tuning recipe and not a
claim that a draft-profile smoke run is publishable.

The authority is the versioned
[`iceberg-rest.commit.same-table-contention` v2 scenario](../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json)
plus the selected [current profile](../profiles/v1/current-2026-08-26.json).
The runner compiles the canonical scenario and rejects behaviorally meaningful
drift before network access.

## Comparison boundary

The benchmark isolates the Iceberg catalog commit transaction. Every request:

1. targets one freshly created format-v2 table;
2. requires the setup table UUID with standard `assert-table-uuid`;
3. applies one standard `set-properties` update with a unique value; and
4. receives exactly one classification: accepted HTTP 200, conflict HTTP 409,
   or a bounded explicit error.

There are no data files, query engines, catalog-specific commit suffixes,
arbitrary headers, client retries, or behavior-changing proxies in this
scenario. In particular, the common request omits `Idempotency-Key`: asymmetric
optional behavior cannot alter one catalog's measured path.

The common create request also sets the standard Iceberg properties
`write.metadata.delete-after-commit.enabled=false` and
`write.metadata.previous-versions-max=100000`. This makes the final object-count
invariant meaningful: a catalog cannot invoke its ordinary old-metadata
retention policy during the evidence window and then appear to have persisted
fewer accepted commits. These are table properties, not catalog-specific
configuration, and every implementation receives the same values.

All five catalogs write Iceberg metadata into the same `s3://warehouse` MinIO
bucket. Their private state stores remain catalog-specific because advancing a
catalog pointer is the system under comparison. The object audit prevents a
catalog from acknowledging commits without persisting the corresponding
Iceberg metadata objects.

## Fixed workload and fair order

Each catalog receives one conditioning round and five measured rounds. Every
round contains:

- 50 warmup commits in series;
- 1,000 measured commits in series;
- eight concurrent writers;
- one shared six-second writer window; and
- one final state and shared-MinIO audit.

The catalog list rotates left once per repetition. Consequently, across the five
measured rounds each catalog occupies every execution position exactly once.
Conditioning evidence is retained but excluded from numeric aggregation.

The concurrent workers wait at a barrier, then receive one deadline through a
shared start signal. A writer samples the deadline immediately before each
request. Every request that starts in the window is allowed to complete, even if
its response arrives after the nominal six seconds; phase elapsed time includes
that tail. No task is cancelled to make throughput look better.

## Isolation and cleanup

Fixture names are derived from the scenario prefix, catalog ID, caller-supplied
fixture ID, and repetition. Before mutation, the runner proves the exact
namespace absent.

- A preflight collision produces a distinct `fixture-collision` round. The
  runner sends no mutation and no cleanup request because it cannot claim
  ownership.
- Once a mutation is attempted after an absent preflight, cleanup always runs,
  including after an ambiguous response or failed assertion.
- Cleanup drops the table with `purgeRequested=false`, proves the table absent,
  drops the namespace, and proves the namespace absent.

Non-purging cleanup is intentional. It leaves run-owned metadata objects in
MinIO for independent evidence inspection while removing catalog-visible test
identifiers.

## Required round checks

A round passes only when all of these checks pass:

1. the fixture was absent before mutation;
2. namespace/table setup and baseline object audit succeeded;
3. every warmup request was accepted with no conflict or error;
4. every sequential request was accepted with no conflict or error;
5. sequential latency has one finite sample per request and monotonic
   p50/p95/p99/maximum values;
6. concurrent attempts equal accepted plus conflicts plus errors;
7. concurrent latency has one sample per attempt;
8. no concurrent request error occurred;
9. at least one concurrent commit was accepted;
10. final table UUID and location match setup, the final property belongs to an
    accepted request, and the metadata pointer remains inside the setup root;
11. metadata-object growth covers every accepted warmup, sequential, and
    concurrent commit under the fixed no-delete retention properties, and the
    exact final pointer exists; and
12. non-purging cleanup proves both table and namespace absent.

HTTP 409 is a valid measured conflict, not a request error. Timeouts, transport
failures, oversized responses, malformed responses, and unexpected statuses are
errors and make the round fail. This distinction is central when interpreting a
high conflict rate; see [Understanding LakeCat's CAS conflict rate](CAS-CONFLICTS.md).

## Aggregation and full ranking

A catalog is rankable only if its conditioning round and all five measured
rounds pass. There is no best-effort subset, outlier deletion, or replacement
round.

For each measured scalar, the transcript reports the median and full observed
minimum–maximum range. It retains:

- sequential p50, p95, and p99 latency;
- sequential accepted throughput;
- concurrent p50, p95, and p99 all-outcome latency;
- concurrent attempted and accepted throughput;
- concurrent conflict and error rates;
- concurrent attempt, acceptance, and conflict counts; and
- persisted metadata-object growth.

The primary ranking score is median concurrent **accepted** commits per second.
Attempted throughput remains visible but does not treat rejected work as
progress. Equal scores use lower median sequential p50 latency, then ascending
catalog ID, as deterministic tie-breakers.

The ranking contains every catalog. Passing catalogs receive numeric ranks;
failed catalogs appear after them as `not-ranked` with round tallies and retain
their complete per-round evidence. A fast failed row is never silently removed
or assigned a numeric rank.

## Transcript safety

The output format is `catalog-bench/contention-transcript/v1`. It records the
exact profile/scenario SHA-256 values, runner source and runtime identity,
profile-driven negotiation evidence, all 30 outcomes, aggregates, ranking, and
sanitization receipt.

Raw bearer tokens, OAuth client credentials, MinIO credentials, response bytes,
and request identities cannot cross the evidence boundary:

- credentials and tokens remain in non-serializable runtime state;
- response bodies are bounded and represented only as sanitized parsed facts or
  permitted hashes;
- raw request identities exist only while constructing a request;
- final-state attribution stores only a SHA-256 digest; and
- immediately before writing, the runner recursively audits serialized values
  for every runtime secret and every deterministic raw request-ID prefix.

Map keys are schema vocabulary and are not compared with short fixture secrets;
all serialized values are inspected, including embedded strings. Output uses
create-new semantics and is never overwritten.

## Production Docker execution

The benchmark process does not run on the host. Compose resolves the exact
public runner source commit as its Docker context, then builds the checked-in
`docker/bench.Dockerfile` from the profile-pinned Rust 1.97.1 image using locked
dependencies, optimization level 3, fat LTO, one codegen unit, native CPU
features, stripped symbols, disabled incremental compilation, and aborting
panics. Warnings are fatal. The slim runtime is read-only, drops all Linux
capabilities, and receives only the compiled executables and CA roots.

The image embeds source revision
`e5345a260a42148aa5cd1044fb3f43acfc2232d2` at compile time. Before reading
credentials or contacting a service, the runner requires that revision and the
observed Linux/ARM64 runtime to match the selected profile.

Use the fail-closed launcher with a globally new, scenario-safe run ID. It
validates the merged Compose contract, rejects pre-existing output, containers,
or state volumes with that ID, safely stops a recognized prior benchmark project
without deleting its volumes, builds the exact production sources, and lets the
`bench` service wait for all five protocol-level readiness gates. An unmanaged
container or unknown Compose project on the benchmark network aborts the run:

```sh
docker/run-contention.sh "c108_$(date -u +%m%d%H%M%S)"
```

The default host destination is `target/commit-evidence`. Set
`CATALOG_BENCH_COMMIT_EVIDENCE_DIR` before Compose to bind another output
directory. The profile and scenario mounts are read-only; only `/evidence` is
writable. `docker-compose.clean.yml` maps the run ID to new LakeCat/Turso,
Lakekeeper/PostgreSQL, Gravitino/SQLite, and MinIO volumes. It intentionally
retains those volumes after the process exits so the measured state remains
available for diagnosis; use a different run ID for every rerun.

Exit codes are:

- `0`: all catalogs passed every round and the complete transcript was written;
- `2`: the complete transcript was written, but at least one catalog is
  unranked; and
- `1`: invocation, contract, runner provenance, internal harness, sanitization,
  or evidence-write failure.

The checked-in current profile is still `draft`. A transcript produced from it
is smoke evidence, even though the executable uses the final optimization
recipe. Publication requires C1-09 to hash the exact executable and image,
materialize those identities in a runnable profile, validate the same bytes
inside the runner container, and place the transcript in a manifest-backed
immutable result bundle.
