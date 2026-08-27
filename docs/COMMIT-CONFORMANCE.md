# Iceberg REST Commit Correctness Conformance

This document records the C1-06 commit-correctness acceptance run on
2026-08-26. It is a deterministic catalog conformance matrix, not a latency,
throughput, or conflict-rate ranking. One production-optimized stable-Rust
runner exercised LakeCat, Apache Polaris, Apache Gravitino, Lakekeeper, and
Apache Nessie from the same Docker network. Every catalog stored Iceberg
metadata in the same local MinIO `warehouse` bucket.

The checked-in behavioral authority is
[`iceberg-rest.commit.correctness`](../scenarios/v1/iceberg-rest.commit.correctness.json).
Its protocol source is the
[Apache Iceberg 1.11 REST OpenAPI](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/open-api/rest-catalog-open-api.yaml).
The OpenAPI defines update-table requirement validation before updates and
identifies a requirement conflict as HTTP 409 `CommitFailedException`.

## Accepted result

LakeCat, Apache Gravitino, and Apache Polaris pass all 10 required assertions.
Lakekeeper and Apache Nessie each pass 9 of 10. Both reject the deterministic
stale requirement with HTTP/code 409 and preserve the complete current state;
their sole required mismatch is the Iceberg error `type`. Lakekeeper returns
`CatalogCommitConflicts`, while Nessie returns an empty type instead of
`CommitFailedException`.

Lakekeeper is the only tested catalog whose resolved config advertises
`idempotency-key-lifetime`. Its exact same-body/same-key retry passes and causes
only one metadata-pointer transition. Its same-key/different-body request does
not satisfy the optional content-binding assertion: Lakekeeper returns the
cached original HTTP 200 response rather than a conflict. The reload proves the
drifted value was not applied.

| Catalog | Required | Stale requirement | Idempotency advertisement | Exact retry | Content drift | Sanitized transcript SHA-256 |
| --- | ---: | --- | --- | --- | --- | --- |
| LakeCat | **pass, 10/10** | pass: 409 `CommitFailedException`; state unchanged | not advertised | not evaluated | not evaluated | `fe827bc9d315311fa6881580a9a7c55adcae2d22d9abec87939b8947eab1b4a3` |
| Apache Gravitino | **pass, 10/10** | pass: 409 `CommitFailedException`; state unchanged | not advertised | not evaluated | not evaluated | `1cf2d5759d71a076491dc4ccb86be7aa6b718316dcae14f8364f79795fb69bf7` |
| Apache Polaris | **pass, 10/10** | pass: 409 `CommitFailedException`; state unchanged | not advertised | not evaluated | not evaluated | `ca5419aa8de66bba918775ffb6817beb830ca9731258242f4d7ca154c6a9db10` |
| Lakekeeper | **fail, 9/10** | fail: 409 `CatalogCommitConflicts`; state unchanged | pass: `/overrides/idempotency-key-lifetime = PT30M` | pass | fail: cached 200; state unchanged | `daee0c1405f72355070a01085fd5ddc3f16d4f2091e3cab7a8e9659b742b7728` |
| Apache Nessie | **fail, 9/10** | fail: 409 with empty type; state unchanged | not advertised | not evaluated | not evaluated | `eeb654907fa64f0d132a5314555c9a8f7d3ddd4cb816dd1ddcc3ec7240a8fdd8` |

An optional failure does not alter the required classification. Conversely,
returning the right status and preserving state does not excuse a required
error-envelope mismatch. The matrix reports both dimensions independently.

## What the required branch proves

Every invocation derives one conservative namespace and one table from a fresh,
run-owned fixture ID. A spec-shaped namespace 404 must prove the fixture absent
before any mutation. If preflight cannot establish ownership, the runner refuses
to create or delete anything under that name.

The 10 required assertions prove:

1. authentication completes without persisting a credential or token;
2. config negotiation returns valid JSON and resolves the standard route prefix
   and namespace separator;
3. fixture preflight observes a spec-shaped `NoSuchNamespaceException`;
4. namespace and committed-table creation preserve the exact run-owned identity,
   a nonempty table UUID and metadata location, schema 0, and initial properties;
5. matching table-UUID and current-schema requirements admit one property
   transition and advance the metadata pointer exactly once;
6. matching table-UUID, schema-ID, and last-field-ID requirements admit schema 1,
   add field 2, and advance the metadata pointer exactly once;
7. a request still asserting schema 0 returns HTTP/code 409
   `CommitFailedException` and leaves UUID, metadata location, current schema,
   last field, and the complete property map unchanged;
8. an independent final reload proves schema 1 and the admitted state property
   remain current while the rejected stale property is absent;
9. non-purging cleanup drops the exact table and namespace and proves both
   spec-shaped absent afterward; and
10. the persisted transcript contains no OAuth credential, bearer token, cookie,
    raw idempotency key, secret-shaped response value, or raw response body.

Successful commits compare the scenario-owned `catalog-bench.*` and `c1-06.*`
property projection exactly. This permits an implementation to maintain its own
unrelated metadata, such as Nessie's changing `nessie.commit.id`, without hiding
scenario drift. Rejected stale requests, exact retries, and rejected content
drift compare the complete normalized state, including all catalog-managed
properties, because those operations must have no second effect.

The stale request is deterministic rather than scheduler-dependent. The runner
loads schema 0, advances the table to schema 1, and only then submits the request
planned against schema 0. This proves stale-plan rejection and metadata-pointer
atomicity one transition at a time. It does not infer correctness from an
aggregate conflict rate.

## Optional idempotency branch

Idempotency is exercised only when the resolved config contains a nonempty
standard `idempotency-key-lifetime` at the top level or in the effective
defaults/overrides maps. When it is absent, the runner sends no
`Idempotency-Key` header and records all three optional assertions as
`not-evaluated`.

When advertised, the runner creates a valid UUIDv7 value that deliberately has
no serialization or display implementation. The raw value may cross the HTTP
boundary, but the operation recorder receives only `<redacted>`. The runner then:

1. sends one property commit with the UUIDv7 key and reloads the table;
2. sends the exact same serialized body with the same key and requires an
   equivalent success response plus no second metadata-pointer transition; and
3. reuses the finalized key with different content, requires a spec-shaped HTTP
   409, and reloads the table to prove the drifted content had no effect.

The optional branch is gated by exact required final state, not by the stale
error-envelope assertion. This distinction allowed Lakekeeper's advertised
retry behavior to be measured safely even though its stale response uses the
wrong required error type. Any UUID, schema, metadata-pointer, or property drift
still suppresses every optional mutation.

## Lakekeeper findings

Lakekeeper 0.13.3 correctly enforces the stale schema requirement. Its response
has HTTP status 409, body code 409, a nonempty explanation, and no state change.
The mismatch is narrowly the body type:

```text
expected: CommitFailedException
observed: CatalogCommitConflicts
```

The required final reload preserves metadata object `00002`, schema 1, last
column 2, and the exact property map. The stale `c1-06.stale` property is absent.

Lakekeeper advertises a 30-minute idempotency lifetime. The first keyed commit
advances from required object `00002` to optional object `00003`. Replaying the
identical body and key returns the byte-equivalent successful body and remains
on `00003`, so exact replay passes. Reusing the key with a different property
value returns that same cached successful representation instead of HTTP 409.
The subsequent load still remains on `00003` with
`c1-06.retry = accepted-once`; `drifted-must-not-apply` never becomes current.
This is a content-binding response defect, not silent catalog-state mutation.

## Nessie finding

Apache Nessie 0.108.4 also correctly rejects the stale schema requirement and
preserves the complete state. Its response is:

```text
HTTP status: 409
body code: 409
body message: Requirement failed: current schema changed: expected 1 != 0
body type: <empty>
```

The pinned Iceberg contract requires `CommitFailedException`, so an empty type
cannot be reinterpreted as pass. Nessie does not advertise
`idempotency-key-lifetime`; the runner therefore sends no idempotency header and
makes no retry-support claim.

This result is unrelated to Nessie's historical concurrent benchmark errors.
The generated 2026-08-08 matrix leaves Nessie unranked because that separate
eight-writer workload observed 97 HTTP 500 request errors. The history and the
older driver's missing error accounting are documented in
[`RESULTS.md`](../RESULTS.md#why-nessie-appeared-to-pass-previously). C1-06 is a
single-transition protocol proof and makes no throughput claim.

## Exact execution identity

All catalog requests originated from container
`92a7704b3a490f6b650ec639f631f7ecba0e8dbb4f42551721e771276b700108`
on the `catalog-bench-net` bridge. The source checkout was mounted read-only,
the Cargo target was a Docker-managed writable volume, and only the evidence
directory was writable on the host. No catalog request crossed a host-published
port.

| Item | Exact identity |
| --- | --- |
| Accepted runner source | `catalog-bench@f07242219b5ef889507e288ed8f0d23ff4701ef9` |
| Candidate profile | SHA-256 `2a428c2bb6ce31eae626d0abcb82db101e9165c5497185111b84288012fbe96d` |
| Commit scenario | SHA-256 `7df567363927001aa25e55c607f60feb63b2fe5442d82d800d298d87e8bc886d` |
| Pinned Iceberg OpenAPI source | SHA-256 `80d2ec83a70eeff6e7194853f8791c17cceb14610fae6a0e6afdd2921806ee4a` |
| Runner executable | SHA-256 `243f16e0f2f375113df2516eb593b36d6a736cf3f25a76055409bd8b5e96391f`; 3,805,952 bytes |
| Runner build environment | Linux ARM64 `rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97` |
| Rust toolchain | `rustc 1.97.1 (8bab26f4f 2026-07-14)`; `cargo 1.97.1 (c980f4866 2026-06-30)`; LLVM 22.1.6 |
| Production profile | `opt-level=3`, fat LTO, one codegen unit, stripped symbols, aborting panics, no debug or incremental compilation, `-Dwarnings`, `-Ctarget-cpu=native`, `--locked`, `-j1` |
| LakeCat source | `lakecat@ef94b5508e94554f51f4764af932cbb819ae3e41` (`0.3.0-32-gef94b550`) |
| LakeCat executable | SHA-256 `0d74e70378f73a9f59eb402cc342e037b29995a3587fc20d2c27f857c671dbaa`; 19,560,096 bytes |
| LakeCat runtime image | local Linux ARM64 image `sha256:7d1eab5295e46e7df06ee14ef807f71fe8e678cc7fa167ead4c4b85a177761e1`; 60,016,569 bytes |
| LakeCat production features | `turso-local,sail-local`; Sail source `bddb1706ba2308e5029d47f04f03121236edbfa6`; Turso `0.7.0-pre.10` |
| Shared MinIO | `RELEASE.2025-10-15T17-29-55Z`, source `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`, local image `sha256:28c9405d4591b7803c8cf79afcef6a32f8fe9964982e5075babcb6a1c7ddecdb` |

LakeCat's public branch underwent a privacy-only history rewrite after the
first smoke matrix. Before rerunning acceptance, an isolated pre/post-rewrite
comparison proved `Cargo.toml`, `Cargo.lock`, and every file under `crates/`
source-identical at the endpoint, namespace, no-snapshot, and final table
milestones. The profile, executable, image, transcripts, and MinIO audit above
all come from the reachable canonical source pin; no result in the accepted
matrix depends on an obsolete commit identifier.

The comparison catalog images were the profile-pinned Linux ARM64 artifacts:

| Catalog | Version | Image index digest | ARM64 platform digest |
| --- | --- | --- | --- |
| Apache Polaris | 1.7.0 | `sha256:3495f67f38cca33892a045f7dd3f46eb52387f0fd52d4145538a772fd8aedad7` | `sha256:53022013a54121d6f81a130b80df85e2c3c1961c592c39e7e3e2353db2ab7acf` |
| Apache Gravitino | 1.3.0 | `sha256:80136ae753ee77735153fc1482a389018f8c2638a54f453cb96967c7194584c7` | `sha256:01cf367b77f91652da6c545ad5253d94c11f4e3dd71c5442863eaa330d8a1088` |
| Lakekeeper | 0.13.3 | `sha256:db2ba6168eb107f22242fb7f2edc4016fa35e57bdcc606894e809c418e32e8dc` | `sha256:ba9424131ff088e8eb5263dbdf66e63c2aec0e71687971673ca37a97389394f2` |
| Apache Nessie | 0.108.4 | `sha256:c0f42874c810f28ac30fc991e979c1b8cf5a2cbfa94212086cdddeae49629517` | `sha256:10d751690c54c837d687437e1cb269f61b8d2ca541277639d623f495b408fe9c` |

Catalog state remained private: LakeCat used file-backed Turso in its own named
volume, Gravitino used file-backed SQLite JDBC in its own named volume,
Lakekeeper used its dedicated PostgreSQL 17.11 service and volume, and Polaris
and Nessie retained their profile-declared isolated state. Only Iceberg metadata
storage was shared.

### Production build fallback

The clean-source Compose image path was retried after the history rewrite.
Docker Desktop's registry resolver again stalled while resolving the already
local Dockerfile frontend; the catalog services and source checkout were
healthy, and the stalled pull was interrupted without accepting an artifact.
The exact runner executable had already been rebuilt inside the running,
digest-pinned Rust 1.97.1 Linux ARM64 container on the same Docker network:

```sh
CARGO_INCREMENTAL=0 \
CARGO_PROFILE_RELEASE_OPT_LEVEL=3 \
CARGO_PROFILE_RELEASE_LTO=fat \
CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
CARGO_PROFILE_RELEASE_DEBUG=false \
CARGO_PROFILE_RELEASE_STRIP=symbols \
CARGO_PROFILE_RELEASE_PANIC=abort \
RUSTFLAGS='-Dwarnings -Ctarget-cpu=native' \
cargo build --locked --release -p catalog-bench-conformance -j1
```

The canonical LakeCat checkout was separately mounted read-only at exact commit
`ef94b5508e94554f51f4764af932cbb819ae3e41`. The same toolchain performed a
real rebuild of the changed LakeCat crate fingerprints and the fat-LTO final
link:

```sh
CARGO_INCREMENTAL=0 \
CARGO_PROFILE_RELEASE_OPT_LEVEL=3 \
CARGO_PROFILE_RELEASE_LTO=fat \
CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
CARGO_PROFILE_RELEASE_DEBUG=false \
CARGO_PROFILE_RELEASE_STRIP=symbols \
CARGO_PROFILE_RELEASE_PANIC=abort \
RUSTFLAGS='-Dwarnings -Ctarget-cpu=native' \
cargo build --locked --release -p lakecat-service --bin lakecat-service \
  --features turso-local,sail-local -j1
```

The resulting executable was installed into the previously verified slim Linux
ARM64 runtime layer entirely within Docker, labeled with the canonical source
revision, and accepted only after its real `/catalog/v1/config` health check
passed. The five probes then invoked the exact hashed runner with `docker exec`
from the same container and bridge against that image. No host-built or debug
binary was substituted. The executable digests, source revisions, toolchain,
build flags, profile digest, scenario digest, and every transcript digest above
are the acceptance identity. C1-09 subsequently materialized a fresh immutable
runner image and runnable profile for the separate production contention run.

## Shared-MinIO proof

The final transcripts reference 16 distinct metadata objects: three each for
LakeCat, Gravitino, Polaris, and Nessie, plus four for Lakekeeper because its
advertised optional retry branch admitted one additional transition. The first
three objects are table creation, current-requirement admission, and schema
admission. A rejected stale commit creates no new current pointer.

An independent audit ran
`minio/mc:RELEASE.2025-05-21T01-59-54Z@sha256:09f93f534cde415d192bb6084dd0e0ddd1715fb602f8a922ad121fd2bf0f8b44`
on `catalog-bench-net`. It resolved every distinct transcript location directly
against `http://minio:9000`: **16 of 16 `mc stat --json` checks succeeded**.

| Catalog | Audited objects | Final accepted metadata object | Bytes | MinIO ETag |
| --- | ---: | --- | ---: | --- |
| LakeCat | 3 | `s3://warehouse/lakecat/cb_c106_lakecat_c106r_lakecat_826d/commit_correctness/metadata/00000001787780831686-10098518-9ce7-4fc2-864e-b73702653fb9.metadata.json` | 1,278 | `3fd02c39afcea465d2a50da65d839015` |
| Apache Gravitino | 3 | `s3://warehouse/cb_c106_gravitino_c106r_gravitino_826d/commit_correctness/metadata/00002-8116a73b-ed2d-4865-847e-2b27794200ea.metadata.json` | 1,313 | `3a2918fb9e779d3b2bca052546b3e89c` |
| Apache Polaris | 3 | `s3://warehouse/bench/cb_c106_polaris_c106r_polaris_826d/commit_correctness/metadata/00002-94d29601-d3aa-442e-8606-7603188a4b04.metadata.json` | 1,365 | `3756b60eba2480e603dcd55e4d626817` |
| Apache Nessie | 3 | `s3://warehouse/cb_c106_nessie_c106r_nessie_826d/commit_correctness_507c7f77-b56b-4e6a-a9c3-614f63782df6/metadata/00000-7fdc0eb2-cdd5-4088-a405-fd2fd65b9d01.metadata.json` | 985 | `29efbc33d2259219cb569a8ad780d745` |
| Lakekeeper | 4 | `s3://warehouse/lakekeeper/01a0400a-9fa7-7ab0-af45-f79a1ac61327/metadata/00003-01a0400a-a159-7d42-b128-5d6365963b03.gz.metadata.json` | 504 | `8027815c9b34014ac6e97c851db34f16` |

Lakekeeper's required final pointer is its audited `00002` object; the table
shows `00003` because that is the one additional effect accepted by the optional
first keyed request. Exact replay and content drift both remain on `00003`.

## Rejected diagnostics and runner corrections

Two complete five-catalog matrices preceded acceptance. They remain ignored
local diagnostics and do not support the matrix above.

### Catalog-managed properties

The first runner at `catalog-bench@9be9375` compared every property across
successful commits. Nessie legitimately changes its own `nessie.commit.id`, so
the runner falsely reported that the requested `c1-06.state` transition was not
exact. Revision `6a78afd` introduced the scenario-owned projection described
above while preserving complete-state comparisons wherever no effect is
allowed.

The rejected transcript hashes were:

| Catalog | SHA-256 |
| --- | --- |
| Apache Gravitino | `b2049ac34ef63f0d30673704684be896fcf3eb4d6d0f8c11f0aa040a3c7cfab8` |
| LakeCat | `7588ceeefc981671c46d3f64f0c9973f628550494348adbee6ba852c19f46152` |
| Lakekeeper | `eb7d3b3d4a11b8dfa3859e86c6047621d6e5468e4076554ea051b0e2da7f76e6` |
| Apache Nessie | `922d562db17fa5130b2f63a39d3727ca1be84a57aaf67d0bebf465df1a0804e5` |
| Apache Polaris | `2570f6683ebd120bfb9ec33572209f4ce4a8e2834192083be13f56426576128d` |

### Independent optional evidence

The corrected `6a78afd` runner still skipped every optional operation after any
required assertion failed. That was unnecessarily strict for Lakekeeper: an
error-type mismatch does not make a byte-for-byte unchanged final state unsafe.
Revision `f072422` gates optional mutation on the independent exact final-state
fact. A regression test proves the optional branch can pass after a wrong-type
required failure, while any actual state mutation continues to suppress it.

The second rejected transcript hashes were:

| Catalog | SHA-256 |
| --- | --- |
| Apache Gravitino | `343daa02e90c09425e382a323a2ef5dc9a94673813140594ebc128f2623c6bf1` |
| LakeCat | `b0e0207ad6b8d24a199a923912be4ffab7c7f438bd30509b9710e6612e0cc617` |
| Lakekeeper | `6af4ef7fc323645340efb169a01e1a5c855abbcbee72c12edbb29345e4b83408` |
| Apache Nessie | `c448907c12cb23d5f38b5674c3b2ef2a028d4b57bdbb1c99e6a3341a674f2f1c` |
| Apache Polaris | `a3feef5bfea635e0aa2ad2ea181f46068f446ab23bcabb3a21f6fcb88963cdad` |

Neither correction changes the scenario, catalog request contract, required
error rule, or accepted catalog state. They remove runner false negatives and
make safe optional evidence independently observable.

## Sanitization and cleanup audit

Every final invocation contains 21 operation slots, including explicit skipped
records for non-advertised optional work. Every catalog reports
`required-final-state-exact: pass`, `commit-fixture-clean: pass`, and
`transcript-sanitized: pass`. The exact cleanup response sequence is identical:

```text
cleanup-drop-table                 attempted 204
cleanup-verify-table-absent        attempted 404
cleanup-drop-namespace             attempted 204
cleanup-verify-namespace-absent    attempted 404
```

Cleanup uses `purgeRequested=false`, which preserves metadata objects for the
independent MinIO audit while proving all catalog entries absent. It runs after
passing and failing assertions. Failed collision preflight remains the only path
that forbids cleanup mutation.

All five transcripts report `raw_secrets_persisted: false` and
`raw_response_body_persisted: false`. Recursive sanitization recorded 39
redactions for Lakekeeper's vended storage credentials and three idempotency
headers, 18 for Polaris's OAuth-backed responses, and none for the three
anonymous catalogs. A direct structural check proved every persisted
`authorization` or `idempotency-key` value equals `<redacted>`. Lakekeeper's
three keyed operation records each contain exactly that marker. A separate
literal scan found no benchmark credential value or bearer token.

## Reproduction

Build and start the exact catalog topology using [`DOCKER.md`](../DOCKER.md).
Require every selected one-shot readiness gate to exit zero, and bootstrap
LakeCat's governed `s3://warehouse/lakecat` storage profile as documented in the
C1-05 report. Choose a new fixture ID and output file for every invocation.

From the conformance container, the LakeCat probe is:

```sh
docker compose --profile conformance run --rm conformance commit \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/iceberg-rest.commit.correctness.json \
  --catalog lakecat \
  --fixture-id review_lakecat_commit_01 \
  --output /evidence/review-lakecat-commit-01.json
```

Repeat with `gravitino`, `lakekeeper`, `nessie`, and `polaris`, using a distinct
fixture and output path for each. Exit 0 means all required assertions passed;
exit 2 means the sanitized transcript was written with `fail` or `unsupported`;
exit 1 is an invocation, contract, or I/O failure. The CLI refuses to overwrite
evidence or mutate a colliding fixture.

## Deliberate non-claims and publication boundary

- These exact transcripts live under ignored `target/conformance-evidence` and
  are reviewed acceptance smoke evidence, not checked-in `catalog-bench/v1`
  result records.
- This report's candidate profile remains `draft`: its conformance-runner source pin
  predates the accepted C1-06 implementation, and source-built artifacts are not
  represented by immutable executable/image digests in that smoke profile.
- C1-09 later rebuilt the production contention artifacts, created the runnable
  profile, and published the reviewed
  [C110 result bundle](../results/v1/2026-08-27/manifest.json). That bundle does
  not retroactively convert these operation-level smoke transcripts into result
  records or change this C1-06 correctness matrix.
- C1-06 changes no concurrent ranking and makes no claim about throughput,
  latency, variance, RSS, recovery from ambiguous writes, multi-writer
  serializability, authorization, views, or stock-client interoperability.
  C1-07 next owns the stock PyIceberg matrix.
