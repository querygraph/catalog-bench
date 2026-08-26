"""Immutable evidence types and classification rules for the PyIceberg probe."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Status(str, Enum):
    """One operation's evidence disposition."""

    PASS = "pass"
    FAIL = "fail"
    UNSUPPORTED = "unsupported"
    NOT_EVALUATED = "not-evaluated"


@dataclass(frozen=True)
class SafeFailure:
    """Failure evidence that deliberately omits the raw exception message."""

    category: str
    exception_class: str
    explanation: str

    @classmethod
    def from_exception(
        cls, error: BaseException, *, category: str, explanation: str
    ) -> SafeFailure:
        error_type = type(error)
        return cls(
            category=category,
            exception_class=f"{error_type.__module__}.{error_type.__qualname__}",
            explanation=explanation,
        )

    def as_json(self) -> dict[str, str]:
        return {
            "category": self.category,
            "exception_class": self.exception_class,
            "explanation": self.explanation,
        }


@dataclass(frozen=True)
class Limitation:
    """A capability not sent by the client, with explicit attribution."""

    attributed_to: str
    explanation: str
    upstream_reference: str | None = None

    def as_json(self) -> dict[str, str]:
        value = {
            "attributed_to": self.attributed_to,
            "explanation": self.explanation,
        }
        if self.upstream_reference is not None:
            value["upstream_reference"] = self.upstream_reference
        return value


@dataclass(frozen=True)
class OperationResult:
    """One scenario step's complete, value-sanitized outcome."""

    id: str
    capability: str | None
    status: Status
    observations: Mapping[str, Any] = field(default_factory=dict)
    failure: SafeFailure | None = None
    limitation: Limitation | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        expected = {
            Status.PASS: (False, False, False),
            Status.FAIL: (True, False, False),
            Status.UNSUPPORTED: (False, True, False),
            Status.NOT_EVALUATED: (False, False, True),
        }[self.status]
        actual = (
            self.failure is not None,
            self.limitation is not None,
            self.reason is not None,
        )
        if actual != expected:
            raise ValueError(
                f"operation {self.id!r} has invalid detail fields for {self.status.value}"
            )

    @classmethod
    def passed(
        cls,
        operation_id: str,
        capability: str | None,
        observations: Mapping[str, Any] | None = None,
    ) -> OperationResult:
        return cls(
            id=operation_id,
            capability=capability,
            status=Status.PASS,
            observations=observations or {},
        )

    @classmethod
    def failed(
        cls,
        operation_id: str,
        capability: str | None,
        failure: SafeFailure,
        observations: Mapping[str, Any] | None = None,
    ) -> OperationResult:
        return cls(
            id=operation_id,
            capability=capability,
            status=Status.FAIL,
            observations=observations or {},
            failure=failure,
        )

    @classmethod
    def unsupported(
        cls,
        operation_id: str,
        capability: str,
        limitation: Limitation,
    ) -> OperationResult:
        return cls(
            id=operation_id,
            capability=capability,
            status=Status.UNSUPPORTED,
            limitation=limitation,
        )

    @classmethod
    def not_evaluated(
        cls, operation_id: str, capability: str | None, reason: str
    ) -> OperationResult:
        return cls(
            id=operation_id,
            capability=capability,
            status=Status.NOT_EVALUATED,
            reason=reason,
        )

    def as_json(self) -> dict[str, Any]:
        value: dict[str, Any] = {"id": self.id, "status": self.status.value}
        if self.capability is not None:
            value["capability"] = self.capability
        if self.observations:
            value["observations"] = dict(self.observations)
        if self.failure is not None:
            value["failure"] = self.failure.as_json()
        if self.limitation is not None:
            value["limitation"] = self.limitation.as_json()
        if self.reason is not None:
            value["reason"] = self.reason
        return value


@dataclass(frozen=True)
class AssertionContract:
    id: str
    step: str
    required: bool


def assertion_evaluations(
    assertions: Sequence[AssertionContract], operations: Sequence[OperationResult]
) -> list[dict[str, Any]]:
    """Project operation facts onto the checked-in assertion contract."""

    by_id = {operation.id: operation for operation in operations}
    if len(by_id) != len(operations):
        raise ValueError("operation IDs must be unique")

    evaluations = []
    for assertion in assertions:
        operation = by_id.get(assertion.step)
        if operation is None:
            raise ValueError(
                f"assertion {assertion.id!r} references missing operation {assertion.step!r}"
            )
        if operation.status is Status.PASS:
            outcome = {"status": "pass"}
        elif operation.status is Status.FAIL:
            assert operation.failure is not None
            outcome = {
                "status": "fail",
                "explanation": operation.failure.explanation,
            }
        elif operation.status is Status.UNSUPPORTED:
            assert operation.limitation is not None
            outcome = {
                "status": "not-evaluated",
                "reason": (
                    f"unsupported by {operation.limitation.attributed_to}: "
                    f"{operation.limitation.explanation}"
                ),
            }
        else:
            assert operation.reason is not None
            outcome = {"status": "not-evaluated", "reason": operation.reason}
        evaluations.append(
            {
                "assertion": assertion.id,
                "required": assertion.required,
                "outcome": outcome,
            }
        )
    return evaluations


def classify(
    assertions: Sequence[AssertionContract], operations: Sequence[OperationResult]
) -> dict[str, str]:
    """Apply strict-v1 classification without hiding optional diagnostics."""

    by_id = {operation.id: operation for operation in operations}
    for assertion in assertions:
        if not assertion.required:
            continue
        operation = by_id[assertion.step]
        if operation.status is Status.UNSUPPORTED:
            assert operation.capability is not None
            assert operation.limitation is not None
            return {
                "status": "unsupported",
                "capability": operation.capability,
                "attributed_to": operation.limitation.attributed_to,
                "explanation": operation.limitation.explanation,
            }
    failed = [
        assertion.id
        for assertion in assertions
        if assertion.required and by_id[assertion.step].status is not Status.PASS
    ]
    if failed:
        return {
            "status": "fail",
            "summary": f"{len(failed)} required assertion(s) did not pass",
        }
    return {"status": "pass"}
