# Stock Engine Interoperability

Phase 2 compares interoperability, not query-engine speed. The first authority is
the versioned
[`engine.iceberg.write-read-evolution`](../scenarios/v1/engine.iceberg.write-read-evolution.json)
scenario. A pinned stock engine must execute one identical semantic workflow
through a profile-selected Iceberg REST catalog and the shared MinIO data plane.
Spark is implemented first; Flink and Trino must consume the same scenario and
produce the same evidence contract.

This document defines the boundary before runtime code exists. A checked-in
scenario says what success means. A later runnable profile says exactly which
artifacts and topology execute it. Only sanitized transcripts and validated
result bundles say what actually happened.

## Common workflow

Every engine execution must:

1. verify the immutable engine and connector artifacts plus Linux/ARM64 runtime;
2. initialize the stock Iceberg connector from the selected catalog adapter;
3. prove its run-owned namespace absent before any mutation;
4. create and list that namespace through the engine;
5. create and reload one unpartitioned format-v2 Parquet table;
6. insert 16 deterministic rows and read their exact canonical projection;
7. add one optional string field named `note` without changing existing field
   identity;
8. insert four evolved rows and read all 20 rows under the evolved schema;
9. independently correlate the engine observation with standard Iceberg REST
   table state;
10. audit current metadata and data objects under the exact table root in the
    profile's shared MinIO;
11. reconcile all owned catalog identifiers without purge; and
12. write a bounded, sanitized transcript with no raw rows or secret values.

All 14 assertions are required. A known inability of a pinned engine or
connector is `unsupported` only when immutable profile evidence classifies it
before mutation and no substitute request is sent. An attempted operation that
violates an assertion is `fail`. An unavailable environment is `not-tested`.
These states are never collapsed into one another.

## Deterministic data oracle

Rows use integer and string semantics shared by all selected engines. For an ID
`i`:

- `category` is `category-` followed by `i mod 4`;
- `amount_cents` is `100 × i + 7`;
- evolved rows add `note = evolved-` followed by `i`.

The initial batch contains IDs 0–15. The evolved batch contains IDs 16–19. A
read is sorted by `id`, projected in scenario-declared column order, encoded as
one compact RFC 8259 JSON array per UTF-8 line, and terminated by a final line
feed. The checked-in identities are:

| Checkpoint | Rows | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| Initial projection | 16 | 346 | `e78b526d7e757090a9a90c80802c2a543cbf8166cfac6d6ed48c618926e85a15` |
| Evolved projection | 20 | 570 | `b2af6f475851e07d1ace3706d8867530c13dd5938bee90cfcc62d3939e01bea2` |

A focused Rust test regenerates both streams from the scenario parameters and
checks count, byte length, and digest. Runtime renderers consume those same
parameters; they must not carry a second engine-local row fixture.

## What may vary

The engines spell equivalent operations differently. Spark uses its DataSource
V2 Iceberg catalog, Flink creates an Iceberg catalog through its Table API, and
Trino configures its Iceberg connector. Their SQL types and namespace commands
also differ. A small renderer may translate neutral Iceberg types and operations
to one engine's stock public syntax.

That renderer is engine-specific but catalog-neutral. Given one engine and the
same semantic input, it must generate the same operation for LakeCat, Polaris,
Gravitino, and Lakekeeper. It cannot inspect a catalog ID to change SQL, skip an
assertion, rewrite a REST request, patch engine files after startup, or replace a
stock connector operation with harness HTTP. Engine and connector components are
recorded in the result; every behavior-changing adapter is disclosed separately.

Catalog-specific routing is data, not code. The existing profile adapter owns:

- REST base URL and config route;
- anonymous or OAuth2 client-credentials authentication;
- static, negotiated, or absent route prefix;
- optional standard create-table location;
- warehouse and shared-object-store topology; and
- capability limitations with upstream attribution.

Secret values enter only as runtime environment values and never enter a profile,
scenario, command transcript, query text, or result artifact.

## Independent evidence

An engine reporting query success is necessary but insufficient. The runner also
loads the table through the profile's already-tested protocol-native REST adapter
and compares table UUID, current metadata location, format version, schema,
snapshot count, and scenario-owned properties. It then traverses only the exact
returned table root in shared MinIO and requires:

- the current metadata object;
- at least four retained metadata objects, corresponding to create, first append,
  schema evolution, and second append; and
- at least two Parquet objects, one or more from each logical append.

The scenario disables metadata deletion and sets a large previous-version limit
at table creation. Cleanup uses standard Iceberg REST with purge disabled after
evidence capture. This avoids engine-specific `DROP TABLE` purge semantics and
keeps immutable metadata and data-plane evidence available for review.

## Pinned upstream behavior

The candidate profile pins Apache Iceberg Java 1.11.0 at
`6976e020b894f6a6777704df2b8c4458cb291ae9`. Its exact documentation defines:

- [Spark REST catalog configuration](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/spark-configuration.md#catalogs),
  [SQL writes](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/spark-writes.md#insert-into),
  and [additive `ALTER TABLE` evolution](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/spark-ddl.md#alter-table--add-column);
- [Flink REST catalog configuration](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/flink.md#rest-catalog)
  and [stock `INSERT INTO` writes](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/docs/docs/flink-writes.md#insert-into); and
- the [Iceberg REST OpenAPI](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/open-api/rest-catalog-open-api.yaml)
  used for independent state and cleanup evidence.

Trino 483 is pinned at `50b0b50b75abd47f830b7805ee1b51716eb4065e`.
Its exact source documentation defines the
[REST catalog properties](https://github.com/trinodb/trino/blob/50b0b50b75abd47f830b7805ee1b51716eb4065e/docs/src/main/sphinx/object-storage/metastores.md#rest-catalog)
and the [Iceberg connector's namespace, table, insert, read, and
additive-column operations](https://github.com/trinodb/trino/blob/50b0b50b75abd47f830b7805ee1b51716eb4065e/docs/src/main/sphinx/connector/iceberg.md).
These pinned sources, rather than moving `latest` pages, govern the runtime
implementation.

## Phase 2 unit boundaries

C2-01 owns only the common write/read/evolution contract. It intentionally does
not claim a runtime result.

The following independently committed units will:

1. materialize the exact Spark runtime and profile, including every Iceberg and
   object-store JAR digest;
2. implement one profile/scenario-driven Spark runner and sanitized transcript;
3. execute Spark against LakeCat, Polaris, Gravitino, and Lakekeeper;
4. add Flink and Trino renderers against the same scenario;
5. define a separate deterministic conflict scenario with an honest
   synchronization boundary; and
6. define OpenLineage correlation only for engines whose pinned integrations can
   emit and identify the required events.

Each unit updates the changelog, runs focused tests plus contract/schema checks,
passes `git diff --check`, and is committed and pushed before the next unit.
