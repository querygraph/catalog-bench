from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from catalog_bench_pyiceberg.contracts import (
    ContractError,
    load_contracts,
    profile_catalog_ids,
    validate_fixture_id,
)

ROOT = Path(__file__).resolve().parents[3]
PROFILE = ROOT / "profiles/v1/current-2026-08-26.json"
SCENARIO = ROOT / "scenarios/v1/client.pyiceberg.interoperability.json"


class ContractTests(unittest.TestCase):
    def test_every_catalog_resolves_against_the_stock_client_scenario(self) -> None:
        for catalog in ("lakecat", "polaris", "gravitino", "lakekeeper", "nessie"):
            with self.subTest(catalog=catalog):
                contracts = load_contracts(PROFILE, SCENARIO, catalog)
                self.assertEqual(contracts.client_component["version"], "0.11.1")
                self.assertEqual(contracts.python_component["version"], "3.13.15")
                self.assertEqual(contracts.arrow_component["version"], "25.0.1")

    def test_matrix_catalog_order_comes_from_the_strict_profile_loader(self) -> None:
        self.assertEqual(
            profile_catalog_ids(PROFILE),
            ("lakecat", "polaris", "gravitino", "lakekeeper", "nessie"),
        )

    def test_known_client_limitations_are_defined_once(self) -> None:
        contracts = load_contracts(PROFILE, SCENARIO, "lakecat")

        for capability in (
            "client.pyiceberg.view-lifecycle",
            "client.pyiceberg.pagination",
        ):
            limitation = contracts.known_client_limitation(capability)
            self.assertIsNotNone(limitation)
            self.assertEqual(limitation.attributed_to, "client")

    def test_fixture_id_validation_is_strict(self) -> None:
        for valid in ("a", "c107_42", "a" * 24):
            validate_fixture_id(valid)
        for invalid in ("", "A", "dash-not-allowed", "a" * 25, "space here"):
            with self.subTest(value=invalid), self.assertRaises(ContractError):
                validate_fixture_id(invalid)

    def test_behavior_changing_shim_is_rejected(self) -> None:
        profile = json.loads(PROFILE.read_text())
        profile["catalog_adapters"][0]["request_handling"] = {
            "kind": "behavior-changing-shim",
            "component": "catalog-bench-conformance",
            "description": "test",
        }
        with tempfile.TemporaryDirectory() as directory:
            modified = Path(directory) / "profile.json"
            modified.write_text(json.dumps(profile))
            with self.assertRaisesRegex(ContractError, "refuses behavior-changing"):
                load_contracts(modified, SCENARIO, "lakecat")

    def test_duplicate_matrix_adapter_is_rejected(self) -> None:
        profile = json.loads(PROFILE.read_text())
        profile["catalog_adapters"].append(profile["catalog_adapters"][0])
        with tempfile.TemporaryDirectory() as directory:
            modified = Path(directory) / "profile.json"
            modified.write_text(json.dumps(profile))
            with self.assertRaisesRegex(ContractError, "must be unique"):
                profile_catalog_ids(modified)

    def test_workload_drift_is_rejected_before_execution(self) -> None:
        scenario = json.loads(SCENARIO.read_text())
        scenario["parameters"]["delete_filter"] = "id < 5"
        with tempfile.TemporaryDirectory() as directory:
            modified = Path(directory) / "scenario.json"
            modified.write_text(json.dumps(scenario))
            with self.assertRaisesRegex(ContractError, "workload.*drifted"):
                load_contracts(PROFILE, modified, "lakecat")


if __name__ == "__main__":
    unittest.main()
