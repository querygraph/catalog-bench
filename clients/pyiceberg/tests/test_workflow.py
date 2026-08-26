from __future__ import annotations

import unittest
from pathlib import Path

from catalog_bench_pyiceberg.contracts import load_contracts
from catalog_bench_pyiceberg.evidence import RuntimeIdentity, build_transcript
from catalog_bench_pyiceberg.model import Status
from catalog_bench_pyiceberg.workflow import run_probe
from support import FakeCatalogFactory

ROOT = Path(__file__).resolve().parents[3]
PROFILE = ROOT / "profiles/v1/current-2026-08-26.json"
SCENARIO = ROOT / "scenarios/v1/client.pyiceberg.interoperability.json"
RUNTIME = RuntimeIdentity(
    python="3.13.15",
    pyiceberg="0.11.1",
    pyarrow="25.0.1",
    s3fs="2026.7.0",
    operating_system="Linux",
    architecture="aarch64",
)
ENVIRONMENT = {
    "CATALOG_BENCH_S3_ACCESS_KEY_ID": "fixture-access-value",
    "CATALOG_BENCH_S3_SECRET_ACCESS_KEY": "fixture-secret-value",
    "CATALOG_BENCH_POLARIS_CLIENT_ID": "fixture-client-value",
    "CATALOG_BENCH_POLARIS_CLIENT_SECRET": "fixture-oauth-value",
}


class WorkflowTests(unittest.TestCase):
    def test_full_workflow_passes_for_every_profile_adapter(self) -> None:
        for catalog in ("lakecat", "polaris", "gravitino", "lakekeeper", "nessie"):
            with self.subTest(catalog=catalog):
                contracts = load_contracts(PROFILE, SCENARIO, catalog)
                factory = FakeCatalogFactory()
                run = run_probe(
                    contracts,
                    "unit",
                    getenv=ENVIRONMENT.get,
                    runtime=RUNTIME,
                    catalog_factory=factory,
                )
                transcript = build_transcript(
                    contracts,
                    run.runtime,
                    run.fixture,
                    run.operations,
                    forbidden_values=run.forbidden_values,
                )

                self.assertEqual(transcript["classification"], {"status": "pass"})
                by_id = {operation.id: operation for operation in run.operations}
                self.assertEqual(by_id["append-scan"].status, Status.PASS)
                self.assertEqual(by_id["recover-conflict"].status, Status.PASS)
                self.assertEqual(by_id["register-table"].status, Status.PASS)
                self.assertEqual(by_id["classify-views"].status, Status.UNSUPPORTED)
                self.assertEqual(
                    by_id["classify-pagination"].status, Status.UNSUPPORTED
                )
                self.assertEqual(by_id["cleanup-fixture"].status, Status.PASS)
                self.assertTrue(factory.catalogs[0].closed)
                self.assertNotIn(
                    "s3.force-virtual-addressing", factory.catalogs[0].properties
                )
                if contracts.adapter["authentication"]["kind"] == "anonymous":
                    self.assertEqual(
                        factory.catalogs[0].properties["auth"], {"type": "noop"}
                    )
                self.assertFalse(factory.catalogs[0].tables)
                self.assertFalse(factory.catalogs[0].namespaces)

    def test_runtime_mismatch_prevents_network_and_mutation(self) -> None:
        contracts = load_contracts(PROFILE, SCENARIO, "lakecat")
        factory = FakeCatalogFactory()
        runtime = RuntimeIdentity(
            python="3.13.14",
            pyiceberg="0.11.1",
            pyarrow="25.0.1",
            s3fs="2026.7.0",
            operating_system="Linux",
            architecture="aarch64",
        )

        run = run_probe(
            contracts,
            "mismatch",
            getenv=ENVIRONMENT.get,
            runtime=runtime,
            catalog_factory=factory,
        )

        self.assertEqual(run.operations[0].status, Status.FAIL)
        self.assertFalse(factory.catalogs)


if __name__ == "__main__":
    unittest.main()
