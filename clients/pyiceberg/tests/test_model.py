from __future__ import annotations

import unittest

from catalog_bench_pyiceberg.model import (
    AssertionContract,
    Limitation,
    OperationResult,
    SafeFailure,
    assertion_evaluations,
    classify,
)


class ModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertions = (
            AssertionContract("required", "required-step", True),
            AssertionContract("optional", "optional-step", False),
        )

    def test_optional_unsupported_does_not_fail_top_level(self) -> None:
        operations = (
            OperationResult.passed("required-step", "required-capability"),
            OperationResult.unsupported(
                "optional-step",
                "optional-capability",
                Limitation("client", "method absent"),
            ),
        )

        self.assertEqual(classify(self.assertions, operations), {"status": "pass"})
        evaluations = assertion_evaluations(self.assertions, operations)
        self.assertEqual(evaluations[1]["outcome"]["status"], "not-evaluated")

    def test_required_unsupported_remains_distinct_from_failure(self) -> None:
        operations = (
            OperationResult.unsupported(
                "required-step",
                "required-capability",
                Limitation("catalog", "not offered"),
            ),
            OperationResult.passed("optional-step", "optional-capability"),
        )

        result = classify(self.assertions, operations)
        self.assertEqual(result["status"], "unsupported")
        self.assertEqual(result["attributed_to"], "catalog")

    def test_required_failure_is_fail(self) -> None:
        operations = (
            OperationResult.failed(
                "required-step",
                "required-capability",
                SafeFailure("request", "ExampleError", "fixed explanation"),
            ),
            OperationResult.passed("optional-step", "optional-capability"),
        )

        self.assertEqual(classify(self.assertions, operations)["status"], "fail")

    def test_status_detail_invariants_are_enforced(self) -> None:
        with self.assertRaises(ValueError):
            OperationResult(
                id="bad",
                capability=None,
                status=OperationResult.passed("x", None).status,
                reason="not allowed for pass",
            )


if __name__ == "__main__":
    unittest.main()
