# Stock PyIceberg interoperability runner

This directory contains the C1-07 stock-client oracle. It runs an unmodified
`pyiceberg.catalog.rest.RestCatalog` against each profile adapter and records
what the public client can actually do. It is intentionally separate from the
Rust protocol conformance runners: no compatibility proxy, request rewrite, or
catalog-specific behavior shim sits between PyIceberg and a catalog.

## Reproducible runtime

The production image is defined by
[`docker/pyiceberg.Dockerfile`](../../docker/pyiceberg.Dockerfile) and built for
Linux ARM64. Its complete runtime is fixed in three layers:

- CPython 3.13.15 comes from the profile-recorded Linux ARM64 child manifest,
  not from a mutable tag selection.
- PyIceberg 0.11.1, PyArrow 25.0.1, and S3FS 2026.7.0 are imported at startup
  and must equal the scenario and profile before any network request or
  mutation occurs. S3FS is part of the stock runtime because a catalog may
  select PyIceberg's public `FsspecFileIO` through its config response.
- [`requirements.lock`](requirements.lock) pins all 41 direct and transitive
  distributions and the exact Linux ARM64 or platform-independent wheel hash.
  The image uses `pip --require-hashes --only-binary=:all:`; an incomplete lock,
  wrong wheel, or source-build fallback fails the build.

The image runs as UID/GID 65534. Compose adds a read-only root filesystem,
drops every Linux capability, enables `no-new-privileges`, and grants write
access only to the evidence bind mount and a bounded temporary filesystem.

## Workflow and classifications

[`workflow.py`](catalog_bench_pyiceberg/workflow.py) performs one deterministic,
run-owned workflow in scenario order:

1. prove the exact runtime;
2. initialize stock `RestCatalog`, including OAuth config negotiation or the
   public `noop` auth manager for anonymous adapters;
3. prove the fixture namespace absent before mutation;
4. create, list, and reload its namespace and table;
5. append a real Arrow batch and scan every canonical row exactly once;
6. independently exercise property updates, schema evolution, row deletes, a
   stale-writer conflict with one refresh/retry, credential vending, rename,
   and non-purging drop/register;
7. classify view lifecycle and explicit pagination as pinned-client
   limitations because PyIceberg 0.11.1 lacks the required public APIs;
8. reconcile all possible table identifiers without purge, drop the owned
   namespace, and prove the catalog fixture absent.

Required operations determine the top-level strict-v1 outcome. An optional
failure stays visible but does not turn a successful required round trip into a
failure. `unsupported` is emitted only for a profile-declared prerequisite or
when the stock client declines an optional operation before a successful
mutation. A skipped dependent operation is `not-evaluated`, never silently
counted as support or failure.

PyIceberg 0.11.1's legacy auth fallback constructs `Bearer None` when neither a
credential nor token is configured. Some anonymous servers ignore that invalid
header, while others correctly reject it. Anonymous profile adapters therefore
select PyIceberg's built-in `NoopAuthManager` through the public `auth.type`
configuration. This omits authorization entirely and is stock-client
configuration, not a request shim.

The runner also deliberately omits `s3.force-virtual-addressing` for shared
MinIO. PyIceberg 0.11.1's stock `FsspecFileIO` checks whether that property has a
non-empty string instead of parsing its Boolean value, so even the string
`false` forces requests to `warehouse.minio`. Omitting the optional force flag
lets S3FS use path-style requests for the custom `http://minio:9000` endpoint.
This is ordinary stock-client configuration; no filesystem or DNS behavior is
patched.

Registration is deliberately last among mutating optional operations. The
public client checks server-advertised endpoint support inside `register_table`,
so the runner cannot preflight it through a private API. It first drops with
`purge_requested=False`, attempts the stock registration call, then reconciles
all candidate identifiers. This can leave retained data and metadata objects in
MinIO by design; it does not leave a live catalog fixture.

## Evidence safety

Transcripts contain component identities, contract hashes, operation status,
fixed explanations, exception classes, counts, ID ranges, and canonical SHA-256
digests. They never contain raw rows, response bodies, exception messages,
OAuth tokens, object-store keys, or credential values. Before serialization,
the evidence builder recursively rejects any configured secret even when it is
embedded inside a larger evidence value, and rejects it as an exact map key.
The key comparison avoids false positives when deliberately simple fixture
values such as `secret` occur inside safe schema field names. Output uses
exclusive creation and refuses to overwrite an existing transcript.

Use a fresh lowercase fixture ID and output path for every invocation. A
preflight collision proves ownership unsafe, suppresses all mutation and
cleanup, and produces a required failure rather than deleting an existing
namespace.

## Commands

The container entry point exposes two commands:

```text
catalog-bench-pyiceberg probe  # one --catalog and one --output
catalog-bench-pyiceberg matrix # every profile adapter into a new --output-dir
```

Canonical same-Docker startup and matrix commands are in
[`DOCKER.md`](../../DOCKER.md#stock-pyiceberg-interoperability). Files written
under `target/pyiceberg-evidence` are mutable smoke diagnostics. They are not
public results until the repository's publication phase wraps reviewed bytes in
an immutable result bundle and records the production image identity. The
accepted five-catalog C1-07 matrix, exact row and MinIO proofs, artifact
identities, and deployment findings are recorded in
[`PYICEBERG-INTEROPERABILITY.md`](../../docs/PYICEBERG-INTEROPERABILITY.md).

## Verification

Fast tests inject deterministic catalog doubles at the catalog factory boundary;
they verify orchestration, conflict recovery, cleanup, strict contract loading,
classification, no-overwrite persistence, and secret rejection without
pretending to be interoperability evidence. Run the same tests in the exact
production runtime with:

```sh
docker run --rm \
  --volume "$PWD:/repo:ro" \
  --workdir /repo \
  --env PYTHONPATH=/repo/clients/pyiceberg \
  --entrypoint python \
  catalog-bench-pyiceberg:0.11.1-python3.13.15 \
  -m unittest discover -s clients/pyiceberg/tests -v
```

When changing a dependency, update the current profile and scenario from
upstream primary sources, resolve wheels specifically for CPython 3.13 on Linux
ARM64, verify every downloaded byte against the proposed lock, rebuild without
cache, inspect the imported runtime identity, rerun these tests, and obtain new
live catalog evidence. Never broaden a hash to make an unexpected artifact pass.
