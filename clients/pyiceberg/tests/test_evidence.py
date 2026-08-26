from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from catalog_bench_pyiceberg.contracts import STEP_CAPABILITIES, load_contracts
from catalog_bench_pyiceberg.evidence import (
    FixtureIdentity,
    RuntimeIdentity,
    build_transcript,
    write_new,
)
from catalog_bench_pyiceberg.model import OperationResult

ROOT = Path(__file__).resolve().parents[3]
PROFILE = ROOT / "profiles/v1/current-2026-08-26.json"
SCENARIO = ROOT / "scenarios/v1/client.pyiceberg.interoperability.json"


class EvidenceTests(unittest.TestCase):
    def test_write_new_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.json"
            write_new(path, b"first\n")
            with self.assertRaisesRegex(FileExistsError, "refusing to overwrite"):
                write_new(path, b"second\n")
            self.assertEqual(path.read_bytes(), b"first\n")

    def test_secret_fragment_embedded_in_an_observation_is_rejected(self) -> None:
        contracts = load_contracts(PROFILE, SCENARIO, "lakecat")
        operations = tuple(
            OperationResult.passed(
                step,
                capability,
                {"unsafe": "prefix-sensitive-value-suffix"}
                if step == "verify-client-runtime"
                else None,
            )
            for step, capability in STEP_CAPABILITIES.items()
        )
        runtime = RuntimeIdentity("3.13.15", "0.11.1", "25.0.1", "Linux", "aarch64")
        fixture = FixtureIdentity("unit", ("unit",), ("events",))

        with self.assertRaisesRegex(ValueError, "sensitive value"):
            build_transcript(
                contracts,
                runtime,
                fixture,
                operations,
                forbidden_values=("sensitive-value",),
            )


if __name__ == "__main__":
    unittest.main()
