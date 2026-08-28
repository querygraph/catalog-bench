# Deterministic Fault Injection

Phase 3 begins with a benchmark-owned HTTP fault boundary rather than Docker
timing guesses. The proxy forwards the original method, headers, query, host,
and body but retains none of those private values in evidence. A rule matches
an uppercase method, path fragment, first occurrence, bounded injection count,
phase, and disconnect action. Evidence contains the rule identity, phase,
match number, method, a SHA-256 of the escaped path, and—only after upstream
completion—the observed status.

## Persistence semantics

- `before-upstream` closes the client connection without forwarding the
  request. Direct state must prove non-persistence.
- `after-upstream` forwards the request, consumes the full upstream response,
  records its status, then closes the client connection before returning any
  response. Direct state determines whether the operation was accepted; the
  client outcome alone is ambiguous.

Rules carry an injection count from 1 through 1000. This lets a test bound and
observe automatic client retries rather than accidentally allowing the first
retry through. The proxy has separate data and control listeners. The Compose
overlay keeps both on `catalog-bench-net` and publishes control ports only on
host loopback.

The overlay provides one proxy for shared MinIO and one REST proxy for each
Phase 3 catalog: LakeCat, Polaris, Gravitino, and Lakekeeper. It rewrites the
catalogs' configured S3 endpoints only in the overlay. The ordinary correctness
profiles and their immutable results remain unchanged.

## Reproduction

The accepted Linux ARM64 profile is
`profiles/v1/object-faults-2026-08-28.json`; the neutral scenario is
`scenarios/v1/object-store.metadata-persistence-faults.json`. Run a fresh proof:

```sh
docker/run-object-faults.sh objfault_local
```

The runner rejects existing output, state volumes, or a reused Compose project.
It starts the exact source-built MinIO/proxy/probe image, waits for bucket
initialization, runs both signed S3 probes, verifies the JSON relationship,
hashes each raw artifact, removes both fixture objects, and removes its project
and volumes. Generated smoke evidence stays under `target/`.

## Accepted implementation evidence

Fresh run `objfault_0828a` was independently read and accepted at
`catalog-bench@18287a5332ccedf473a700ce46dac8e6f11a855f`. The immutable source
evidence is under `results/source/faults/objfault_0828a/`.

| Case | Client result | Upstream evidence | Direct object observation |
| --- | --- | --- | --- |
| Before upstream | disconnected | no upstream status | absent |
| After upstream | disconnected | HTTP 200 | present |

Both cases used content hash
`sha256:9c6a95372144d03bb2f58ebf9dc3049576560f9b6811787e3eb3baec95f02f61`.
The before artifact hash is
`sha256:ea10949f042a8963f3d1b2ca50ccb0702355945befc9df40a05517714b7669a1`;
the after artifact hash is
`sha256:7f7d9b106d22b0bd0404190ed9b07281bbca592e6df7aeeaca10b49e885d34dd`.
The reviewed summary hash is
`sha256:350c5a2e9d2c61b6e7b19c51bc0d306d5a24fecd8f0fc1259265caee14cb3faf`.

This closes the deterministic network/object-store injection substrate only.
It does not claim catalog recovery, restart safety, backup/restore behavior, or
relative performance. Those require the subsequent catalog-specific scenarios.
