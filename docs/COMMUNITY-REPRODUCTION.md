# Community evidence reproduction

Prerequisites are Docker with Compose, Git, stable Rust, Python `uv`, and enough
space for the pinned stock-engine images. Validate the historical behavioral
bundles with the smoke/full commands in the repository README.

Reproduce the semantic path from clean LakeCat, QueryGraph, and catalog-bench
checkouts at their revisions recorded in
`results/source/semantic/tpcds_0828g/review.json`:

```bash
docker/run-querygraph-tpcds-fixture.sh tpcds_<unique-id>
```

Compare the resulting `summary.json` content hash and proof bases with the
reviewed evidence. The command refuses reused output, checksum-fetches Ossie,
uses run-owned state, and cleans its containers and volumes. Validate the Q3
index by recomputing SHA-256 for every path in
`results/v1/2026-q3-community/manifest.json`. Run the repository’s existing
structured/literal secret scan before publication.
