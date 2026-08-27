# Stock PyIceberg Interoperability

This document records the C1-07 stock-client acceptance run on 2026-08-26.
One unmodified PyIceberg 0.11.1 `RestCatalog` workflow ran against LakeCat,
Apache Polaris, Apache Gravitino, Lakekeeper, and Apache Nessie from the same
Linux ARM64 container, on one Docker network, with one shared local MinIO
service. No proxy, request rewrite, filesystem patch, or catalog-specific shim
sat between the client and any catalog.

This is an interoperability result, not a latency or throughput ranking. The
checked-in behavioral authority is
[`client.pyiceberg.interoperability`](../scenarios/v1/client.pyiceberg.interoperability.json).
Catalog operations use the
[Apache Iceberg REST protocol](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/open-api/rest-catalog-open-api.yaml),
while data operations use PyIceberg's public table, Arrow, and file-I/O APIs.

## Accepted result

All five catalogs pass all eight required assertions. Every catalog also passes
property update, schema evolution, row delete, deterministic stale-writer
recovery, same-namespace rename, and non-purging drop/register through the stock
client.

| Catalog | Required result | Properties | Schema | Delete | Conflict | Delegated access | Rename | Register |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- |
| LakeCat | **pass, 8/8** | pass | pass | pass | pass | unsupported by catalog response | pass | pass |
| Apache Polaris | **pass, 8/8** | pass | pass | pass | pass | pass: key pair + session token | pass | pass |
| Apache Gravitino | **pass, 8/8** | pass | pass | pass | pass | pass: key pair | pass | pass |
| Lakekeeper | **pass, 8/8** | pass | pass | pass | pass | pass: key pair + session token | pass | pass |
| Apache Nessie | **pass, 8/8** | pass | pass | pass | pass | unsupported by catalog response | pass | pass |

`unsupported` is not a failed attempt. LakeCat and Nessie did not place a
delegated credential category in the loaded table config, so their successful
object I/O used the common fixed benchmark credentials. Gravitino, Lakekeeper,
and Polaris returned the categories shown above, and the same table workflow
proved those configurations could read and write objects. Evidence records only
categories; it never records a credential value.

Two other optional capabilities are explicitly unsupported by the pinned
client for every catalog:

- PyIceberg 0.11.1 exposes some view helpers but no public `create_view` and
  `load_view` lifecycle with which to run the scenario.
- Its public `list_namespaces` and `list_tables` methods neither accept page
  controls nor traverse returned page tokens.

Those are client-level findings. They make no claim about whether an individual
catalog implements the corresponding REST endpoints.

The exact sanitized transcript identities are:

| Catalog | Passed operations | Unsupported operations | Transcript SHA-256 |
| --- | ---: | ---: | --- |
| LakeCat | 14 | 3 | `bfe8896f6422146c16cabce5dbd7220d7fecc98c00c488e155f1e839a462ec90` |
| Apache Polaris | 15 | 2 | `a5c57fd120062278bbf3021f14fa4dc4689ac7645278cafcdcc3871eb7fcea3b` |
| Apache Gravitino | 15 | 2 | `83e486ac0d6a52cc00b88de67dd3156efc3d4ef44f7a9812a2d800ec388399fc` |
| Lakekeeper | 15 | 2 | `82b3ed74372ec7d98a7d424fc4f17bbde28a5519c883e6679463bb355e2255d4` |
| Apache Nessie | 14 | 3 | `c3aaaae7e98e004fef863d6ad8630d3bd8d968c7721140e82de967fa5844d90a` |

No invocation has a failed operation. The operation counts include runtime,
initialization, fixture, cleanup, and sanitization records in addition to the
assertion-bearing catalog operations.

## What the workflow proves

Each invocation derives one run-owned namespace and three possible table names
from fixture `c107_08262354`. It refuses all mutation unless the namespace is
absent, then uses only public PyIceberg APIs to prove:

1. the observed Python, PyIceberg, PyArrow, S3FS, OS, and architecture exactly
   match the profile;
2. anonymous or OAuth2 client-credentials initialization and REST config
   negotiation complete without a behavior-changing shim;
3. namespace create, list, property load, table create, table list, and table
   load preserve the requested state;
4. a required `id` field and two optional fields round-trip through a real
   Arrow append and an independent table scan;
5. one transaction updates and removes the exact requested properties;
6. schema evolution adds an optional column and an evolved append preserves
   both old and new rows;
7. `id < 4` deletion leaves exactly the expected rows;
8. two independently loaded table handles create a deterministic stale writer:
   writer A commits, writer B receives `CommitFailedException`, then one refresh
   and one retry admit writer B exactly once;
9. returned table configuration is classified by credential category without
   serializing its values;
10. rename preserves the complete row digest while leaving exactly one active
    identifier;
11. non-purging drop followed by `register_table` preserves the exact metadata
    location and row digest; and
12. cleanup reconciles all three possible names without purge, drops the owned
    namespace, and proves every catalog identifier absent.

The data-plane checkpoints are identical across all five catalogs:

| Checkpoint | Rows | ID range | Canonical row SHA-256 |
| --- | ---: | --- | --- |
| Initial append and scan | 32 | 0–31 | `be39436d6b4ca628516f75079ee5e2d80cf414d4f573ac948bbdb68755344cba` |
| Evolved-schema append and scan | 40 | 0–39 | `bbd08c1df9f17f418d320e1e0ac3fd6dd312470cb352a7de4812797671b032dc` |
| Delete and scan | 36 | 4–39 | `3dc90f603d82135840c832a3f2bb3ee9e34a097d226b9351a5bf640cc4f34450` |
| Conflict, refresh, retry, and scan | 44 | 4–47 | `b6e4fe2403a212aed3a2d0482dc1146a61a210391882c9ef14b934d736cc5cda` |

Rename and registration both preserve the final 44-row digest. The transcript
stores count, range, and canonical hash rather than raw row values.

## Exact execution identity

The accepted client image was rebuilt from the clean, already-pushed
`catalog-bench` checkout and the acceptance matrix ran before this report was
added. All catalog and S3 requests originated inside `catalog-bench-net`; no
request used a host-published catalog or MinIO port.

| Item | Exact identity |
| --- | --- |
| Accepted runner source | `catalog-bench@f2f66ee45574a64d1e76330e95e7aa551c3a148b` |
| Candidate profile | SHA-256 `82c691ed8e44fbe514bd8d586a3606c260ee76e2cb5a5a944d3ae65487bc5395` |
| PyIceberg scenario | SHA-256 `d2a6f01cfb6c39a38a10f9a5aaa3d5ef86dd45e02bcc2aad54889c3bb26adfc3` |
| Fixture | `c107_08262354`; one fresh catalog-qualified namespace per adapter |
| Client image | local Linux ARM64 image `sha256:b7bd67aaab38ff0c2e0d1c7fd957aabf6b2b472973303593e41b69043a2db521`; 132,188,605 bytes |
| Python base | `python:3.13.15-slim-bookworm@sha256:c45a22ea000adfd9cda29364bbe7edd23001ce5cc2ad15857cfbf7766943b9ca`; ARM64 manifest `sha256:e424b523c9296fdef9d2533c368facee1dc45be4c1f8e1555f90c4feac439594` |
| Observed runtime | CPython 3.13.15; PyIceberg 0.11.1; PyArrow 25.0.1; S3FS 2026.7.0; Linux/aarch64 |
| Dependency lock | 41 exact binary distributions; [`requirements.lock`](../clients/pyiceberg/requirements.lock) SHA-256 `bc5f8fd3fea03116139da649d70db230f7a3577d4746fd9aed0e40150d689508` |
| PyIceberg wheel | SHA-256 `ddb360da76c62c7c23ec3da40e1af48e6712a563905fea2d1a8911ff7a3b6c4d` |
| PyArrow wheel | SHA-256 `44a9120ce5bd81936b8ab9a88076e3fd47c2c6838e0e43630fed83626aca81d9` |
| S3FS wheel | SHA-256 `64edf3c01ebffab1eec38ff9c09eefbf86a3db14c87d248f795da0e7b801d698` |
| LakeCat source | `lakecat@ef94b5508e94554f51f4764af932cbb819ae3e41`; `turso-local,sail-local` |
| LakeCat executable | SHA-256 `0d74e70378f73a9f59eb402cc342e037b29995a3587fc20d2c27f857c671dbaa`; 19,560,096 bytes |
| LakeCat runtime image | local optimized Linux ARM64 image `sha256:7d1eab5295e46e7df06ee14ef807f71fe8e678cc7fa167ead4c4b85a177761e1`; 60,016,569 bytes |
| MinIO server | `RELEASE.2025-10-15T17-29-55Z`, source `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`, Go 1.24.8 Linux/ARM64, running image `sha256:28c9405d4591b7803c8cf79afcef6a32f8fe9964982e5075babcb6a1c7ddecdb` |
| Setup helper image | clean rebuilt Linux ARM64 image `sha256:1e549bc4d29cb9cef0855a7338c98881885ef35f8c343d9320e401b5b896049d`; 50,687,348 bytes |
| Docker runtime | Docker Engine 29.4.3, API 1.54, Linux/ARM64 kernel 6.12.76-linuxkit; Compose 5.1.3 |

The LakeCat binary is the same production artifact accepted for C1-06:
optimization level 3, fat LTO, one codegen unit, stripped symbols, aborting
panics, disabled incremental compilation, `-Dwarnings`, and native CPU target
features. The Python client image uses only hash-locked wheels and has no
runtime compilation or dependency resolution.

The comparison catalog images are the profile-pinned Linux ARM64 artifacts:

| Catalog | Version | Image index digest | ARM64 platform digest |
| --- | --- | --- | --- |
| Apache Polaris | 1.7.0 | `sha256:3495f67f38cca33892a045f7dd3f46eb52387f0fd52d4145538a772fd8aedad7` | `sha256:53022013a54121d6f81a130b80df85e2c3c1961c592c39e7e3e2353db2ab7acf` |
| Apache Gravitino | 1.3.0 | `sha256:80136ae753ee77735153fc1482a389018f8c2638a54f453cb96967c7194584c7` | `sha256:01cf367b77f91652da6c545ad5253d94c11f4e3dd71c5442863eaa330d8a1088` |
| Lakekeeper | 0.13.3 | `sha256:db2ba6168eb107f22242fb7f2edc4016fa35e57bdcc606894e809c418e32e8dc` | `sha256:ba9424131ff088e8eb5263dbdf66e63c2aec0e71687971673ca37a97389394f2` |
| Apache Nessie | 0.108.4 | `sha256:c0f42874c810f28ac30fc991e979c1b8cf5a2cbfa94212086cdddeae49629517` | `sha256:10d751690c54c837d687437e1cb269f61b8d2ca541277639d623f495b408fe9c` |

Catalog-private state remained isolated: LakeCat used its Turso volume,
Gravitino its SQLite volume, Lakekeeper its dedicated PostgreSQL database,
Polaris its own service state, and Nessie its version store. Every Iceberg
metadata, manifest, and Parquet object used the same `warehouse` bucket in the
same MinIO container.

## Shared-MinIO proof

Cleanup deliberately used `purge_requested=False`, leaving immutable object
evidence after every catalog entry and namespace had been proved absent. A
separate audit container ran
`minio/mc:RELEASE.2025-05-21T01-59-54Z@sha256:09f93f534cde415d192bb6084dd0e0ddd1715fb602f8a922ad121fd2bf0f8b44`
on `catalog-bench-net` and listed each accepted table root directly through
`http://minio:9000`.

Every catalog retained the same object shape: eight Iceberg metadata files,
thirteen Avro manifest or manifest-list files, and six Parquet files. The audit
found 135 of 135 objects and 20 of 20 distinct metadata locations named by the
transcripts.

| Catalog | Objects | Bytes | Metadata | Avro | Parquet | Transcript locations found |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| LakeCat | 27 | 79,842 | 8 | 13 | 6 | 4/4 |
| Apache Polaris | 27 | 76,922 | 8 | 13 | 6 | 4/4 |
| Apache Gravitino | 27 | 76,294 | 8 | 13 | 6 | 4/4 |
| Lakekeeper | 27 | 59,122 | 8 | 13 | 6 | 4/4 |
| Apache Nessie | 27 | 66,307 | 8 | 13 | 6 | 4/4 |
| **Total** | **135** | **358,487** | **40** | **65** | **30** | **20/20** |

The registration checkpoint preserved these final metadata objects:

| Catalog | Registered metadata object | Bytes | MinIO ETag |
| --- | --- | ---: | --- |
| LakeCat | `s3://warehouse/lakecat/cb_c107_lakecat_c107_08262354/events/metadata/00000001787788501973-fbaa6fd8-9ece-4543-b6e6-257027264a50.metadata.json` | 6,062 | `81f47cfb163be475309f3e8a709bb322` |
| Apache Polaris | `s3://warehouse/bench/cb_c107_polaris_c107_08262354/events/metadata/00007-6f3970ce-64c8-4107-b2eb-f1993ef3a2af.metadata.json` | 5,571 | `ff1a57e7fbed4b7297bea05b2ff45a2e` |
| Apache Gravitino | `s3://warehouse/cb_c107_gravitino_c107_08262354/events/metadata/00007-f81262b2-6a9d-4d4f-bdf8-78d8ca29b991.metadata.json` | 5,468 | `0d2cefd2f27c577e9904d72a62e5008e` |
| Lakekeeper | `s3://warehouse/lakekeeper/01a0407f-aa3c-75f0-8774-95f6b4929421/metadata/00007-01a0407f-ac32-7f22-9750-f305825ba441.gz.metadata.json` | 1,233 | `e42c9d75dec37c56227022a9291fac53` |
| Apache Nessie | `s3://warehouse/cb_c107_nessie_c107_08262354/events_c5bed37f-c918-4a04-83fb-f8e508c60688/metadata/00000-168a725b-80d6-4fa6-999b-6454b9037135.metadata.json` | 1,868 | `179a5bfade9bddcef15a7c44f47dd8d0` |

Object counts and bytes are correctness evidence, not performance
measurements. Different metadata compression and naming strategies are allowed.

## Deployment findings and corrections

The accepted result required correcting real configuration and representation
boundaries. None of the corrections changes requests in flight.

### Stock client and Arrow

PyIceberg 0.11.1's legacy anonymous-auth fallback constructs an invalid
`Bearer None` header when neither a credential nor token is set. Anonymous
adapters now select PyIceberg's public
[`NoopAuthManager`](https://github.com/apache/iceberg-python/blob/8dee48a8e0218353f706133ed035334869a7ee12/pyiceberg/catalog/rest/auth.py#L52-L56),
which omits the header.

The first Arrow append inferred `id` as nullable even though the Iceberg schema
requires it. The runner now constructs the Arrow schema with exact Iceberg
nullability before calling the same public append API. S3FS 2026.7.0 and its
transitive dependencies were also added to the exact wheel lock because a REST
config response can select PyIceberg's public `FsspecFileIO` at runtime.

### Apache Gravitino

After anonymous auth was corrected, Gravitino could create metadata but failed
the table response because its S3 location had no credential provider. The
Compose binding now sets the documented `s3-secret-key` provider in Gravitino's
[`GRAVITINO_ICEBERG_REST_*` configuration](https://github.com/apache/gravitino/blob/v1.3.0/docs/iceberg-rest-service.md).
The accepted table config reports a key-pair category, and object I/O succeeds
through that stock configuration.

### Apache Polaris

Polaris authentication and catalog creation were healthy, but the principal
role initially lacked content privileges. The typed setup helper now reads the
built-in `catalog_admin` role, adds `CATALOG_MANAGE_CONTENT` only when absent,
and reads the role again before succeeding. Exact no-op, write failure,
verification failure, and concurrent-create paths have deterministic tests.

PyIceberg's REST session
[requests `vended-credentials` by default](https://github.com/apache/iceberg-python/blob/8dee48a8e0218353f706133ed035334869a7ee12/pyiceberg/catalog/rest/__init__.py#L773).
Polaris therefore also needed a usable STS path rather than
`stsUnavailable=true`. Following its pinned
[S3-compatible storage configuration](https://github.com/apache/polaris/blob/4ac2f059d1cce149453d0a5f1ff1dff980ec97cc/site/content/in-dev/unreleased/configuration/configuring-polaris-for-production/configuring-aws-s3-cloud-storage-specific.md),
the catalog now names the shared MinIO STS endpoint and a fixture role ARN,
uses path-style S3, and leaves STS available. MinIO's `AssumeRole` response was
verified independently before acceptance. Polaris then returned key-pair and
session-token categories, and the stock object I/O path completed.

### Apache Nessie and S3FS

Nessie's catalog and client-visible S3 endpoints now both use
`http://minio:9000`, which is resolvable from the shared Docker network. A
container-local `127.0.0.1` would point back at the client or catalog container,
not MinIO.

The pinned PyIceberg
[`FsspecFileIO` checks string truthiness](https://github.com/apache/iceberg-python/blob/8dee48a8e0218353f706133ed035334869a7ee12/pyiceberg/io/fsspec.py#L203-L204)
for `s3.force-virtual-addressing`. Supplying the string `false` therefore
changed a path-style request into a virtual host such as `warehouse.minio`. The
runner omits that optional property, allowing S3FS to use path-style requests
for the custom endpoint. This is stock configuration, not a DNS or filesystem
workaround.

## Rejected diagnostics

Every exploratory output used a unique, non-overwritable local directory. None
is an accepted result or a publication artifact.

| Fixture | Observed result | Correction before the next accepted run |
| --- | --- | --- |
| `c107_08261901` | LakeCat append failed with `ValueError` | Preserve required Iceberg nullability in the Arrow schema. |
| `c107_08261908` | LakeCat and Lakekeeper passed; Gravitino auth, Nessie S3 runtime, and Polaris authorization failed | Select no-op anonymous auth, lock S3FS, and grant catalog content management. |
| `c107_08261918` | Gravitino lacked an S3 credential provider; Nessie targeted an unreachable virtual host; Polaris still lacked the effective content path | Configure Gravitino's provider, correct same-network S3 behavior, and verify the Polaris grant. |
| `c107_08262338` | LakeCat, Gravitino, and Lakekeeper passed; Nessie still used virtual-host addressing and Polaris rejected unavailable STS vending | Omit the false-string force flag and enable MinIO-backed Polaris STS. |
| `c107_08262358` | All five passed | Rejected only for provenance: the final Go constant/test refactor was committed after the helper image build. |
| `c107_08262354` | **All five passed** | Accepted after clean client/helper rebuilds from pushed source. |

The apparently decreasing final fixture suffix is only an identifier. Acceptance
is determined by source and artifact identity, not lexical fixture order.

## Sanitization and cleanup audit

Every transcript contains 17 operation slots and all eight required assertions
pass. LakeCat and Nessie contain 14 `pass` plus three `unsupported` operations;
the other three catalogs contain 15 `pass` plus two `unsupported` operations.
There are no failed or silently omitted operations.

All five cleanup records prove:

- `purge_requested: false`;
- the one remaining registered table was dropped;
- all three possible table candidates are absent; and
- the run-owned namespace is absent.

All five sanitization records prove raw rows, response bodies, exception
messages, secrets, and delegated credential values were not persisted. Failure
evidence is limited to an exception class and a fixed runner-owned explanation;
row evidence is limited to count, range, and digest. A separate literal scan
found no benchmark password, credential pair, or forbidden secret value in any
accepted transcript.

The evidence writer uses exclusive creation and `fsync`; rerunning with the same
output path fails rather than replacing evidence. A fixture collision suppresses
both ordinary mutation and cleanup, so the runner cannot delete a namespace it
does not own.

## Verification

The accepted implementation passed these independent gates before the final
matrix:

- `gofmt`, module-tidiness, `go vet`, and all MinIO/setup-helper Go tests;
- all 16 PyIceberg unit tests inside the exact client image;
- Ruff formatting and lint checks for the Python runner;
- all-profile Compose rendering;
- workspace Rust formatting, tests, and documentation tests;
- generated-schema equality and semantic validation of every checked-in
  profile, scenario, and result; and
- the five-catalog transcript, cleanup, sanitization, credential-category,
  exact-row, hash, and direct-MinIO audits recorded above.

The Go helper tests include Polaris create/grant, exact no-op with extra
server-managed grants, failed write, failed verification, concurrent grant, and
STS-setting validation. Deployment regressions bind the pinned client image and
lock, anonymous auth behavior, same-Docker Nessie endpoints, Gravitino provider,
and Polaris STS environment to the checked-in profile.

## Reproduction

The complete startup and readiness procedure is in
[`DOCKER.md`](../DOCKER.md#stock-pyiceberg-interoperability). After every gate
has exited successfully, the exact acceptance invocation is:

```sh
profiles=(
  --profile lakekeeper
  --profile nessie
  --profile polaris
  --profile gravitino
  --profile pyiceberg
)

fixture_id=c107_08262354
docker compose "${profiles[@]}" run --rm --no-deps pyiceberg matrix \
  --profile /contracts/profiles/v1/current-2026-08-26.json \
  --scenario /contracts/scenarios/v1/client.pyiceberg.interoperability.json \
  --fixture-id "$fixture_id" \
  --output-dir "/evidence/$fixture_id"
```

Use a new fixture and output directory for a new run; the literal acceptance ID
above is intentionally no longer reusable. Exit 0 means every catalog passed
all required assertions. Exit 2 means sanitized transcripts were written but at
least one required result did not pass. Exit 1 means invocation, contract, or
evidence persistence failed.

## Deliberate non-claims and publication boundary

- C1-07 measures no duration, latency, throughput, or resource consumption and
  changes no public performance ranking.
- Client-level view and pagination limitations are not catalog support claims.
- A catalog response without delegated credentials does not prove the catalog
  can never vend them; it describes this exact version and configured route.
- A local image ID is exact for this Docker daemon but is not a distributable
  registry digest.
- Files under `target/pyiceberg-evidence` are ignored, mutable smoke evidence.
  They are not checked-in `catalog-bench/v1` results.

C1-09 later materialized immutable production contention artifacts, produced a
runnable profile, captured the execution environment, and published the reviewed
[C110 result bundle](../results/v1/2026-08-27/manifest.json) after exact-byte,
bundle, and redaction checks. This report remains the reviewed C1-07 stock-client
acceptance record: that later contention publication does not turn its ignored
PyIceberg smoke transcripts into result records or add performance claims here.
