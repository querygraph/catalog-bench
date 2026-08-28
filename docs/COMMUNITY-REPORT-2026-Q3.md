# Catalog community report — 2026 Q3

LakeCat, Polaris 1.7.0, Gravitino 1.3.0, and Lakekeeper 0.13.3 were compared as
peers through public protocols. The reviewed results are unranked correctness,
recovery, migration, and semantic-supply-chain evidence; they are not a
performance leaderboard.

Stock Spark 4.1.3, Flink 2.1.3, Trino 483, and DuckDB 1.5.3 each completed the
common Iceberg REST write/read/evolution contract across the four catalogs.
Recovery work separately covers request/response loss, restart behavior,
outbox replay, cold state restore, peer metadata-pointer migration, and
HadoopCatalog registration. Catalog configuration differences and explicit
non-claims remain in each reviewed bundle.

The semantic track pins Apache Ossie at
`1d9ebcea2932d3381c0840cc8304f0850d366509`. Run `tpcds_0828g` creates five
physical TPC-DS tables, policy-binds and CAS-publishes the exact model, drains a
stable graph anchor and OpenLineage receipt, evaluates five representative
answers, and binds the result to seven proof bases. Deliberate physical, model,
policy, graph, lineage, and artifact drift is rejected.

The upstream Polaris converter’s 45 Java tests pass. A live TPC-DS round trip
preserves one model, five datasets, and 31 fields, while explicitly losing four
relationships, five metrics, AI context, and the two source model extensions.
That verified loss report motivates a focused Apache Ossie report-contract
proposal rather than a false lossless-interchange claim.

The release index is `results/v1/2026-q3-community/manifest.json`. Known gaps
remain generated at `results/v1/KNOWN-GAPS.md`; reproduction is described in
`docs/COMMUNITY-REPRODUCTION.md`.
