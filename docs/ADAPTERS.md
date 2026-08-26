# Catalog adapter contract

Catalog adapters are immutable data in a versioned execution profile, not
catalog-specific branches hidden in the benchmark. The Phase 1 candidate profile
binds LakeCat, Apache Polaris, Apache Gravitino, Lakekeeper, and Apache Nessie to
the same Apache Iceberg REST operation vocabulary and the same Docker network.

The protocol source is Apache Iceberg 1.11.0's
[`rest-catalog-open-api.yaml`](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/open-api/rest-catalog-open-api.yaml),
pinned by tag and source revision in the current profile. That specification
defines `/v1/config`, optional endpoint advertisement, warehouse selection,
namespace and table operations, error responses, commit requirements, pagination,
OAuth2, and idempotency-key behavior. Scenario documents remain the authority for
the exact requests and assertions exercised by a run.

## What an adapter may do

An adapter supplies only deployment routing and authentication:

- the catalog component and `iceberg-rest-v1` protocol;
- a Docker-network base URL;
- the relative `/v1/config` request and sanitized warehouse query;
- an unprefixed, static, or config-negotiated `{prefix}` path binding;
- anonymous or OAuth2 client-credentials authentication without secret values;
- an optional standard `createTable.location`; and
- an exhaustive capability disposition.

It does not rewrite request bodies, response bodies, statuses, errors, metadata,
or endpoint advertisements. Every current adapter has
`request_handling.kind = protocol-native`.

The contract can represent a `behavior-changing-shim` for separately labeled
experiments, but only by naming an immutable profile component of kind
`connector` and explaining the mutation. Such a result is not a no-shim
compatibility result, and its adapter component must also be disclosed in the
result record.

## Current candidate bindings

| Catalog | Base URL | Config selection | Route prefix | Authentication | Standard create location | Handling |
| --- | --- | --- | --- | --- | --- | --- |
| LakeCat | `http://lakecat:8181/catalog` | `/v1/config` | unprefixed | anonymous | `s3://warehouse/lakecat` | protocol-native |
| Apache Polaris | `http://polaris:8181/api/catalog` | `/v1/config` | static `bench` | OAuth2 client credentials at `/v1/oauth/tokens`, scope `PRINCIPAL_ROLE:ALL` | catalog-managed | protocol-native |
| Apache Gravitino | `http://gravitino:9001/iceberg` | `/v1/config` | unprefixed | anonymous | catalog-managed | protocol-native |
| Lakekeeper | `http://lakekeeper:8181/catalog` | `/v1/config?warehouse=bench` | negotiated from `/defaults/prefix` | anonymous under the pinned allow-all fixture | catalog-managed | protocol-native |
| Apache Nessie | `http://nessie:19120/iceberg` | `/v1/config` | static `main` | anonymous | catalog-managed | protocol-native |

These are container-network addresses. Host port mappings are diagnostic access,
not benchmark adapter endpoints.

## Capability semantics

The profile defines each capability once and requires every catalog adapter to
classify every capability exactly once:

- `exercise-all` is the canonical DRY form when every vocabulary entry should be
  exercised;
- `exercise` means the harness will send the standard request and let assertions
  determine `pass` or `fail`. It is not a claim that the catalog supports or will
  pass the operation; it appears inside `explicit` coverage when exceptions exist.
- `unsupported` is a pre-execution conclusion with catalog-or-adapter attribution,
  an explanation, and an optional upstream reference. It must not be inferred
  after an attempted operation fails.

The current candidate deliberately places all 27 Phase 1 capabilities in the
`exercise-all` disposition. C1-03 through C1-08 will provide operation evidence;
if an optional operation is proven absent before execution, a new profile
revision will move it to an `explicit` exercise/unsupported partition with its
evidence rather than converting an observed failure in presentation code.

The vocabulary covers:

- config negotiation, endpoint advertisement, and warehouse/prefix routing;
- namespace create, list, load, property update, drop, hierarchy, pagination,
  duplicate handling, and missing-parent handling;
- table create, list, load, register, rename, update, drop, and spec-shaped
  errors;
- set-properties commits, requirement enforcement, stale-pointer rejection,
  exact retry, and idempotency-key content binding;
- the no-shim PyIceberg round trip; and
- same-table concurrent contention with conflicts and request errors preserved.

## Validation invariants

Semantic validation rejects a profile when:

- a non-historical profile omits a catalog component's adapter;
- an adapter references a non-catalog component or has no unique service binding;
- the adapter base URL differs from the corresponding service endpoint;
- a URL contains credentials, query text, a fragment, or an ambiguous trailing
  slash;
- config or authentication routing is malformed or contains a secret-shaped key;
- a static prefix is not one path segment, or a negotiated prefix does not point
  to the standard config `prefix` property;
- a capability is missing, duplicated, both exercised and unsupported, or not
  defined by the profile; or
- a behavior-changing shim is not a separately pinned connector component.

Historical reproduction evidence predates this optional profile section. Its
exact profile bytes remain untouched and valid; the stricter completeness rule
applies to current candidate, conformance, performance, and fault-injection
profiles, and to any historical profile that opts into adapter declarations.

Run the static gates from the repository root:

```sh
cargo run -p catalog-bench-contract --locked -- schemas check
cargo run -p catalog-bench-contract --locked -- validate profiles/v1
cargo test -p catalog-bench-common --test contract --locked
```

Static adapter validation is not behavioral conformance. Live config transcripts,
operation assertions, classification, and sanitized evidence begin in C1-03.
