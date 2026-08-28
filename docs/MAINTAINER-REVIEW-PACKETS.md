# Maintainer review packets

These packets provide an evidence-linked review opportunity. They were
dispatched at the public issues linked below; no project response or endorsement
is claimed until a maintainer comments on a public issue or review.

## LakeCat

Review issue: <https://github.com/querygraph/lakecat/issues/4>

Please review the stock-engine matrices, recovery evidence, and TPC-DS semantic
proof indexed by `results/v1/2026-q3-community/manifest.json`. Corrections to
LakeCat configuration, claims, or known gaps will be retained verbatim in the
next versioned feedback ledger.

## Apache Polaris

Review issue: <https://github.com/apache/polaris/issues/5403>

Please review the Polaris 1.7.0 stock-engine/recovery results and the pinned
Apache Ossie converter loss report. In particular, confirm the scope of the
ephemeral restart result and the observed TPC-DS retention/loss counts. This is
not a performance comparison.

## Apache Gravitino

Review issue: <https://github.com/apache/gravitino/issues/12719>

Please review the Gravitino 1.3.0 stock-engine and restart/cold-restore evidence,
including the SQLite topology description. Corrections to deployment scope or
behavioral interpretation are requested.

## Lakekeeper

Review issue: <https://github.com/lakekeeper/lakekeeper/issues/2002>

Please review the Lakekeeper 0.13.3 stock-engine, restart, cold-restore, and
metadata-pointer migration evidence. In particular, confirm the documented
scope of advertised idempotency and bounded gzip registration behavior.

## Correction protocol

Every packet links the immutable evidence index and known-gaps page. Accepted
corrections are appended with the public source URL, reviewer identity as
published, affected artifact hash, disposition, and the new bundle version;
historical evidence is never rewritten.
