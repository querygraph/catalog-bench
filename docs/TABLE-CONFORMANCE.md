# Iceberg REST Table Conformance

This document records the C1-05 table-behavior acceptance run on 2026-08-26.
It is a catalog conformance matrix, not a latency or throughput ranking. The
same typed Iceberg REST workflow ran against LakeCat, Apache Polaris, Apache
Gravitino, Lakekeeper, and Apache Nessie from one Docker network and one
production-optimized runner. Every catalog wrote its Iceberg metadata to the
same local MinIO `warehouse` bucket.

The checked-in behavioral authority is
[`iceberg-rest.table.behavior`](../scenarios/v1/iceberg-rest.table.behavior.json).
Its protocol source is the
[Apache Iceberg 1.11 REST OpenAPI](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/open-api/rest-catalog-open-api.yaml).

## Accepted result

Four catalogs pass all 15 required assertions. Apache Nessie passes 14 of 15:
its only required mismatch is returning HTTP 200 with an empty page when tables
are listed under an absent namespace, where the pinned Iceberg contract requires
a spec-shaped HTTP 404. All five catalogs pass both optional standard operations,
same-namespace rename and metadata registration.

| Catalog | Required assertions | Rename | Register | Pagination | Sanitized transcript SHA-256 |
| --- | ---: | --- | --- | --- | --- |
| LakeCat | **pass, 15/15** | pass | pass | standards-permitted unpaginated fallback; 2 tables | `202b6fcffcb1cb832f0eb818b34454c956d777b6eee7d44445c8126ca365a0b9` |
| Apache Gravitino | **pass, 15/15** | pass | pass | 2 pages; 2 unique tables | `941deab4facf307b50c5e6bf3edcf2311dd4b644762441d4a546edc35117f379` |
| Lakekeeper | **pass, 15/15** | pass | pass | 3 pages including terminal traversal; 2 unique tables | `c336b88e0aa6382f0d6c13818567554684ae868f98a1d297d4c3d9a6548aa004` |
| Apache Polaris | **pass, 15/15** | pass | pass | standards-permitted unpaginated fallback; 2 tables | `79e0fbead68feb142de7c3ce3d145560c831bcba937a9a15e43f352f32f63ac0` |
| Apache Nessie | **fail, 14/15** | pass | pass | 2 pages; 2 unique tables | `8019de1556f3bcedd7de2471c74acf8b518dd10c0ecd5888b31d1a69c163fda1` |

The page counts are protocol observations, not performance measurements. An
unpaginated fallback is allowed by the scenario only when the response returns
the complete unique result and no continuation token.

## What the scenario proves

Each invocation derives one conservative, run-owned namespace and five table
candidates from a unique fixture ID. A spec-shaped 404 must prove the namespace
absent before any mutation. If that preflight fails, the runner refuses to
mutate or clean up, so it cannot delete pre-existing catalog state.

The 15 required assertions prove:

1. authentication completes without persisting credentials or tokens;
2. config negotiation resolves the standard route prefix and namespace
   separator;
3. fixture preflight observes a spec-shaped `NoSuchNamespaceException`;
4. the exact fixture namespace is created;
5. two committed table creates return distinct UUIDs, requested schemas and
   properties, nonempty metadata locations, and the requested table location
   when the adapter declares one;
6. ordinary listing contains both identifiers exactly once;
7. loading both tables preserves UUID, metadata location, schema, properties,
   and table location;
8. bounded pagination is complete, unique, loop-free, and lossless, or the
   permitted unpaginated fallback is complete;
9. one `set-properties` plus one `remove-properties` commit advances the
   immutable metadata location while preserving UUID, schema, unmentioned
   properties, and table location;
10. duplicate create returns HTTP/code 409 with a nonempty Iceberg
    `AlreadyExistsException` envelope;
11. absent-table load returns HTTP/code 404 with a nonempty
    `NoSuchTableException` envelope;
12. absent-namespace table listing returns HTTP/code 404 with a nonempty
    `NoSuchNamespaceException` envelope;
13. non-purging drop returns 204 and the dropped table subsequently loads as a
    spec-shaped 404;
14. every source, rename destination, dropped sibling, and registration
    candidate is reconciled and absent before the namespace is dropped and
    verified absent; and
15. the persisted transcript contains no credential, bearer token, cookie,
    opaque page token, secret-shaped response value, or raw response body.

The two optional assertions are still exercised rather than inferred from a
feature list. Same-namespace rename must return 204, leave the source absent,
and preserve destination UUID and metadata location. Registration must reuse
the dropped sibling's retained metadata without overwrite, then load the new
name with the exact source UUID and metadata location. An optional protocol
failure remains visible in evidence but does not change the required result.

Cleanup runs after both passing and failing assertions. It uses
`purgeRequested=false`, which is why metadata objects remain available for the
independent MinIO audit after every catalog entry and fixture namespace has been
proved absent.

## Exact execution identity

All catalog requests ran on the `catalog-bench-net` Docker bridge. The runner's
source checkout was mounted read-only, its Cargo target lived in a Docker named
volume, and its evidence directory was the only writable host bind. No catalog
request crossed a host-published port.

| Item | Exact identity |
| --- | --- |
| Acceptance checkout | `catalog-bench@99971e8a84f116646bd05eb48728b4982b5a4444` |
| Runner implementation pin | `catalog-bench@621cc4bbc80169547c497b6829a4982e20f24e58` |
| Candidate profile | SHA-256 `a8d86ab535ac84780ad3694775deec7ae74556ccdf4ed9bf65f97335a18edf52` |
| Table scenario | SHA-256 `50237ef4dfefb2e3f58f0cca3d6a0550c6b7d08a3cceccf4ecc68d5a606fe6e9` |
| Runner executable | SHA-256 `e2f1d622640a3dc987322c185a2ff369f6612780ed62ae57651f2c57bbcfb3a7`; 3,609,344 bytes |
| Runner build image | `rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`; ARM64 manifest `sha256:6e957ef098dcc77d33e310261e4ed5843bb108d5c3b5dc2b476cbc8b6caf53fa` |
| Rust toolchain | `rustc 1.97.1 (8bab26f4f 2026-07-14)`; `cargo 1.97.1 (c980f4866 2026-06-30)`; LLVM 22.1.6 |
| Production profile | `opt-level=3`, fat LTO, one codegen unit, stripped symbols, aborting panics, no debug or incremental compilation, `-Dwarnings`, `-Ctarget-cpu=native`, `--locked`, `-j1` |
| LakeCat source | `lakecat@762527c7d27730dd789cf41b1cdee021ab712aef` (`0.3.0-31-g762527c7`) |
| LakeCat executable | SHA-256 `70bc7d84b5c08a9addf52848edec4c0746f65a2680074d1c606dd2889ae60abd`; 19,560,096 bytes |
| LakeCat runtime image | local Linux ARM64 image `sha256:3936e3576bfee378e2fde0227a4a1f9f2eb6b75322291feb3b67b4fd87ae23f6`; 60,017,816 bytes |
| LakeCat production features | `turso-local,sail-local`; Sail source `bddb1706ba2308e5029d47f04f03121236edbfa6`; Turso `0.7.0-pre.10` |
| Shared MinIO | `RELEASE.2025-10-15T17-29-55Z`, source `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`, local image `sha256:28c9405d4591b7803c8cf79afcef6a32f8fe9964982e5075babcb6a1c7ddecdb` |

The acceptance checkout is newer than the runner implementation pin because
the later commits only pin profile provenance and repair/document deployment
configuration. The final production build was rerun from the clean
`99971e8a...` checkout and remained byte-identical to the executable built from
the pinned runner implementation.

The comparison catalog images were the profile-pinned Linux ARM64 artifacts:

| Catalog | Version | Image index digest | ARM64 platform digest |
| --- | --- | --- | --- |
| Apache Polaris | 1.7.0 | `sha256:3495f67f38cca33892a045f7dd3f46eb52387f0fd52d4145538a772fd8aedad7` | `sha256:53022013a54121d6f81a130b80df85e2c3c1961c592c39e7e3e2353db2ab7acf` |
| Apache Gravitino | 1.3.0 | `sha256:80136ae753ee77735153fc1482a389018f8c2638a54f453cb96967c7194584c7` | `sha256:01cf367b77f91652da6c545ad5253d94c11f4e3dd71c5442863eaa330d8a1088` |
| Lakekeeper | 0.13.3 | `sha256:db2ba6168eb107f22242fb7f2edc4016fa35e57bdcc606894e809c418e32e8dc` | `sha256:ba9424131ff088e8eb5263dbdf66e63c2aec0e71687971673ca37a97389394f2` |
| Apache Nessie | 0.108.4 | `sha256:c0f42874c810f28ac30fc991e979c1b8cf5a2cbfa94212086cdddeae49629517` | `sha256:10d751690c54c837d687437e1cb269f61b8d2ca541277639d623f495b408fe9c` |

Catalog state remained private: LakeCat used file-backed Turso in its own named
volume, Gravitino used file-backed SQLite JDBC in its own named volume,
Lakekeeper used its dedicated PostgreSQL 17.11 process/database/volume, and the
pinned Polaris and Nessie services retained their profile-declared isolated
state. Only Iceberg metadata storage was shared.

## Shared-MinIO proof

LakeCat's fresh state volume was bootstrapped with one exact governed S3 profile
for `s3://warehouse/lakecat`. The standard create request then carried this
derived location:

```text
s3://warehouse/lakecat/cb_c105_lakecat_c105s_lakecat_826/primary
```

The create response preserved that table location and returned this immutable
metadata object:

```text
s3://warehouse/lakecat/cb_c105_lakecat_c105s_lakecat_826/primary/metadata/00000-7c643e01-d092-4605-bfd4-17bcd14c7aa2.metadata.json
```

The other four adapters intentionally omitted a create location and tested each
catalog's configured warehouse default. Every response nevertheless resolved
to the same `s3://warehouse` bucket:

| Catalog | Representative primary metadata object | Bytes | MinIO ETag |
| --- | --- | ---: | --- |
| LakeCat | `s3://warehouse/lakecat/cb_c105_lakecat_c105s_lakecat_826/primary/metadata/00000-7c643e01-d092-4605-bfd4-17bcd14c7aa2.metadata.json` | 955 | `ee66c7614f249e7897ade8d7e9d150a9` |
| Apache Gravitino | `s3://warehouse/cb_c105_gravitino_c105s_gravitino_826/primary/metadata/00000-b4b670f0-d90c-4dfe-a877-dfa2202ef656.metadata.json` | 782 | `f056a019c19281898fe75d3c8a2a7d8c` |
| Lakekeeper | `s3://warehouse/lakekeeper/01a03ef5-1582-7c42-bffb-ef90d41eb9a8/metadata/00000-01a03ef5-1582-7c42-bffb-efaddbba7cf5.gz.metadata.json` | 341 | `10df923f8da20a28bfe5e1f45b4669e1` |
| Apache Polaris | `s3://warehouse/bench/cb_c105_polaris_c105s_polaris_826/primary/metadata/00000-abcd6ad2-fb4a-4a7f-878f-141c4785096b.metadata.json` | 830 | `4f266a534b0c73b1243204aaa19eaca8` |
| Apache Nessie | `s3://warehouse/cb_c105_nessie_c105s_nessie_826/primary_6c9abcde-699b-415a-8f4a-9220c5c4b25c/metadata/00000-f293c730-b149-425b-b6ac-e5925d00cd9d.metadata.json` | 829 | `e77baef2fbfa9380987a572f7a85b373` |

The independent audit used local
`minio/mc:RELEASE.2025-05-21T01-59-54Z@sha256:09f93f534cde415d192bb6084dd0e0ddd1715fb602f8a922ad121fd2bf0f8b44`
on `catalog-bench-net`. It resolved and statted every distinct metadata location
referenced by every final transcript: three per catalog and 15 of 15 total.
Those three are the original primary metadata, the updated primary metadata,
and the sibling metadata reused by registration.

## Defects found before acceptance

### LakeCat no-snapshot rename failure

The first optimized LakeCat probe at source `c1abd976` passed all 15 required
assertions and registration, but optional rename returned HTTP 500. Its
transcript SHA-256 is
`6bdf4237bede510da22b718d880048fe9bb36b5b7df83a5dc15821a336429b90`.
The exact response was:

```text
internal error: table commit record snapshot id must be non-negative
```

Iceberg legitimately represents a newly created table with no current snapshot
as `current-snapshot-id: -1`. LakeCat had copied that wire sentinel into its
unsigned durable commit evidence. Rename later revalidated the history and
rejected the negative value. LakeCat revision `335f94ef` now normalizes the
no-snapshot sentinel to zero at the evidence boundary, decodes legacy serialized
`-1` as zero, rejects every other negative value, and validates the complete
transition before mutating either memory or Turso state. Revision `762527c7`
contains that repair and the regenerated book artifacts.

### Runner silently omitted the declared LakeCat location

The repaired LakeCat then passed all 17 assertions in transcript
`ae757976fdce33564c233cd7139b944e1cdd8e405df5d750a9158074ce4ef28b`.
That diagnostic was still rejected because its create request omitted the
profile's `create_table_location`; LakeCat selected
`file:///tmp/lakecat/...`. Behavioral success was not enough to prove the
required shared object-store topology.

Runner revision `621cc4bb` makes the adapter location a typed fixture root,
derives unique namespace/table URI children, sends the standard `location` on
primary, sibling, and duplicate creates, and requires every create, load,
update, rename, and register response to preserve the expected table location.
A dedicated adversarial test proves response-location drift fails while cleanup
still completes. Catalog-managed adapters continue to omit `location` by
contract.

### Gravitino silently retained memory and `/tmp` defaults

The first shared-storage matrix was also rejected as a set. LakeCat and the
other three warehouse-managed catalogs returned S3 locations, but Gravitino
returned `/tmp/...`. Inspection of the pinned image's own
`bin/rewrite_config.py` showed that version 1.3.0 reads only
`GRAVITINO_ICEBERG_REST_*`; the shorter Compose names were ignored. The effective
configuration therefore remained `catalog-backend = memory` and
`warehouse = /tmp`, contradicting the profile.

For traceability, that rejected matrix's transcript hashes were LakeCat
`bb3776861d3a462c2532a59b2b5f3199e56dd75d436924950db3233c35c0c76a`,
Gravitino
`d60203c3ac83d1f4412ac69f33f5163da629757b1fdbb61b2b196fce42403421`,
Lakekeeper
`e8017d0f183fbc9004d9fdae31113467dddee12c7080b353c7dedfd6c6b8676c`,
Polaris
`336f4fff044c581fbbff652016742ebef3466348966bf741ede9cd543cc8ef36`,
and Nessie
`848aeab1ada5911df8a2673ca043e02ccba40255954c85ccbcac05ba70c46740`.
Their behavior classifications do not override the failed storage-topology
precondition.

Catalog-bench revision `75c95cf` corrected the exact JDBC, URI, warehouse,
S3FileIO, MinIO, region, credential, and path-style bindings and added a
deployment regression test. A truly fresh named volume then exposed a second
honest failure: the upstream image runs as UID 1000, but Docker created the
volume root-owned. Revision `99971e8` added an idempotent root-only one-shot that
assigns only that private directory to UID 1000 and exits. The long-running
catalog remains unprivileged. Final effective config negotiation returned HTTP
200 with file-backed SQLite and `s3://warehouse/`; all three transcript-referenced
Gravitino metadata objects were then found in MinIO.

These rejected transcripts remain ignored local diagnostics. None is promoted
as accepted evidence, and none is used to make a public result claim.

## Why Nessie fails this scenario

Nessie creates, lists, loads, updates, paginates, rejects duplicate tables,
rejects missing tables, renames, drops, registers retained metadata, cleans up,
and stores all referenced metadata in MinIO. Its sole required failure is:

```text
missing-namespace-error-spec-shaped: HTTP 200 is not in [404]
```

The server returns an empty table page for a run-owned absent namespace. The
pinned Iceberg OpenAPI declares 404 for that operation, so the runner cannot
reinterpret the response as pass. This is closely analogous to C1-04's narrow
Nessie mismatch for listing namespaces under an absent parent.

It is not the historical concurrent-commit failure. In the generated 2026-08-08
concurrent matrix, Nessie is unranked because 97 HTTP 500 request errors occurred
across five measured rounds. That older workload and its Quarkus request-context
failure are documented in
[`RESULTS.md`](../RESULTS.md#why-nessie-appeared-to-pass-previously). C1-05 makes
no inference from table conformance to throughput, latency, or concurrency.

## Sanitization and cleanup audit

Every final invocation reports both `table-fixture-clean: pass` and
`transcript-sanitized: pass`. The runner recursively redacted OAuth credentials,
authorization headers, Lakekeeper-vended storage credentials, secret-shaped
response fields, and opaque pagination tokens before serialization. Raw bodies
were hashed and bounded but never persisted. Every transcript reports
`raw_secrets_persisted: false` and `raw_response_body_persisted: false`; a
separate literal scan for benchmark passwords, AWS credential names, and bearer
tokens also passed.

Cleanup explicitly attempted every possible source and destination name,
accepted only successful drop or already-absent outcomes, loaded each candidate
to prove a spec-shaped 404, dropped the namespace, and loaded the namespace to
prove its final 404. This completed for Nessie even after its earlier required
assertion failed.

## Reproduction

Build production artifacts and start the pinned catalogs using the instructions
in [`DOCKER.md`](../DOCKER.md). In particular, require all one-shot readiness
gates to exit zero and verify Gravitino's effective non-secret settings before a
run. Bootstrap LakeCat's scoped storage profile from inside Docker:

```sh
docker compose exec --no-TTY lakecat curl --fail --silent --show-error \
  --request PUT \
  --header 'content-type: application/json' \
  --data '{
    "location-prefix":"s3://warehouse/lakecat",
    "provider":"s3",
    "issuance-mode":"governed-read-required",
    "public-config":{
      "catalog-bench.phase":"C1-05",
      "catalog-bench.storage":"shared-minio"
    }
  }' \
  http://localhost:8181/management/v1/warehouses/local/storage-profiles/c105-minio
```

Choose a fresh fixture ID and a new output path for each catalog:

```sh
docker compose --profile conformance run --rm conformance table \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/iceberg-rest.table.behavior.json \
  --catalog lakecat \
  --fixture-id review_lakecat_table_01 \
  --output /evidence/review-lakecat-table-01.json
```

Repeat with `gravitino`, `lakekeeper`, `nessie`, and `polaris`. Exit 0 means all
required assertions passed. Exit 2 means the sanitized transcript was written
with `fail` or `unsupported`; exit 1 is an invocation, contract, or I/O failure.
The CLI refuses to overwrite an evidence file or mutate a colliding fixture.

## Deliberate non-claims and publication boundary

- These exact transcripts live under ignored `target/conformance-evidence` and
  are acceptance smoke evidence, not checked-in `catalog-bench/v1` results.
- The current profile remains `draft` because source-built executables are not
  yet represented as immutable digest-resolved profile artifacts.
- C1-09 owns rebuilding all final production artifacts, immutable result and
  manifest materialization, complete environment capture, manual redaction
  review, secret scanning, generated matrices/reports, and adversari.al
  publication.
- C1-05 changes no concurrent ranking and makes no claim about throughput,
  latency, memory, authorization, views, stale commits, retry behavior, or
  idempotency. Commit semantics begin in C1-06.
