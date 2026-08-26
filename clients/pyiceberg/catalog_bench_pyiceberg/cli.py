"""Command-line boundary for one probe or the complete profile matrix."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .contracts import (
    ContractError,
    load_contracts,
    profile_catalog_ids,
    validate_fixture_id,
)
from .evidence import (
    RuntimeIdentity,
    build_transcript,
    encode_transcript,
    sha256_hex,
    write_new,
)
from .workflow import detect_runtime, run_probe


@dataclass(frozen=True)
class _WrittenProbe:
    catalog: str
    classification: str
    path: Path
    sha256: str


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="catalog-bench-pyiceberg",
        description="Run pinned, no-shim stock PyIceberg interoperability probes",
    )
    commands = root.add_subparsers(dest="command", required=True)

    probe = commands.add_parser("probe", help="run one catalog adapter")
    _common_arguments(probe)
    probe.add_argument("--catalog", required=True)
    probe.add_argument("--output", required=True, type=Path)

    matrix = commands.add_parser("matrix", help="run every profile catalog adapter")
    _common_arguments(matrix)
    matrix.add_argument("--output-dir", required=True, type=Path)
    return root


def _common_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--profile", required=True, type=Path)
    command.add_argument("--scenario", required=True, type=Path)
    command.add_argument("--fixture-id", required=True)


def main(arguments: Sequence[str] | None = None) -> int:
    args = parser().parse_args(arguments)
    try:
        validate_fixture_id(args.fixture_id)
        runtime = detect_runtime()
        if args.command == "probe":
            written = _run_one(
                profile=args.profile,
                scenario=args.scenario,
                catalog=args.catalog,
                fixture_id=args.fixture_id,
                output=args.output,
                runtime=runtime,
            )
            _print_written(written)
            return 0 if written.classification == "pass" else 2

        args.output_dir.mkdir(parents=True, exist_ok=False)
        catalogs = profile_catalog_ids(args.profile)
        written = []
        for catalog in catalogs:
            result = _run_one(
                profile=args.profile,
                scenario=args.scenario,
                catalog=catalog,
                fixture_id=args.fixture_id,
                output=args.output_dir / f"pyiceberg-{catalog}-{args.fixture_id}.json",
                runtime=runtime,
            )
            written.append(result)
            _print_written(result)
        passed = sum(result.classification == "pass" for result in written)
        print(f"matrix complete: {passed}/{len(written)} catalog(s) passed")
        return 0 if passed == len(written) else 2
    except (ContractError, FileExistsError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except Exception as error:  # noqa: BLE001 - never print a raw message
        error_type = type(error)
        print(
            "error: unexpected runner failure "
            f"({error_type.__module__}.{error_type.__qualname__})",
            file=sys.stderr,
        )
        return 1


def _run_one(
    *,
    profile: Path,
    scenario: Path,
    catalog: str,
    fixture_id: str,
    output: Path,
    runtime: RuntimeIdentity,
) -> _WrittenProbe:
    contracts = load_contracts(profile, scenario, catalog)
    run = run_probe(contracts, fixture_id, runtime=runtime)
    transcript = build_transcript(
        contracts,
        run.runtime,
        run.fixture,
        run.operations,
        forbidden_values=run.forbidden_values,
    )
    encoded = encode_transcript(transcript)
    write_new(output, encoded)
    return _WrittenProbe(
        catalog=catalog,
        classification=transcript["classification"]["status"],
        path=output,
        sha256=sha256_hex(encoded),
    )


def _print_written(result: _WrittenProbe) -> None:
    print(
        f"wrote {result.path} (sha256={result.sha256}, "
        f"catalog={result.catalog}, classification={result.classification})"
    )
