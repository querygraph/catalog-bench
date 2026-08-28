# Catalog Bench Published Bundles

This page is generated from validated immutable manifests. Smoke evidence under `target/` is not included.

| Bundle | Created | Provenance | Scenarios | Results | Pass | Non-pass |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| [2026-08-08 same-table commit ranking](2026-08-08/manifest.json) | 2026-08-26T18:00:00-04:00 | historical-import | 1 | 4 | 3 | 1 |
| [2026-08-27 production same-table contention ranking](2026-08-27/manifest.json) | 2026-08-27T05:24:26Z | live-run | 1 | 5 | 3 | 2 |
| [2026-08-28 Phase 1 catalog behavior and stock-client interoperability](2026-08-28-phase1/manifest.json) | 2026-08-28T04:48:00Z | live-run | 5 | 25 | 20 | 5 |
| [Stock Flink 2.1.3 Iceberg REST interoperability across LakeCat, Polaris, Gravitino, and Lakekeeper](flink-v2-65f0a4c3-2026-08-28/manifest.json) | 2026-08-28T06:37:54Z | live-run | 1 | 4 | 4 | 0 |
| [Stock Spark 4.1.3 Iceberg REST interoperability across LakeCat, Polaris, Gravitino, and Lakekeeper](spark-v2-65f0a4c3-2026-08-28/manifest.json) | 2026-08-28T05:49:08Z | live-run | 1 | 4 | 4 | 0 |
| [Stock Trino 483 Iceberg REST interoperability across LakeCat, Polaris, Gravitino, and Lakekeeper](trino-v2-b424f778-2026-08-28/manifest.json) | 2026-08-28T08:58:44Z | live-run | 1 | 4 | 4 | 0 |

Regenerate and verify with `./publish-results.sh smoke`; use `./publish-results.sh full` to recompute every source-backed checked-in bundle first.
