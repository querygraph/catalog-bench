#!/usr/bin/env python3
"""Protocol-native Iceberg REST recovery probe with sanitized JSON evidence."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import random
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class HttpOutcome:
    kind: str
    status: int | None


class Client:
    def __init__(self, base: str, bearer: str | None = None) -> None:
        self.base = base.rstrip("/")
        self.bearer = bearer

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[HttpOutcome, dict[str, Any] | None]:
        payload = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        request_headers = {"Accept": "application/json"}
        if payload is not None:
            request_headers["Content-Type"] = "application/json"
        if self.bearer:
            request_headers["Authorization"] = f"Bearer {self.bearer}"
        request_headers.update(headers or {})
        request = urllib.request.Request(
            self.base + path, data=payload, headers=request_headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                data = response.read(1 << 20)
                decoded = json.loads(data) if data.strip() else None
                return HttpOutcome("response", response.status), decoded
        except urllib.error.HTTPError as error:
            error.read(1 << 20)
            return HttpOutcome("response", error.code), None
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError):
            return HttpOutcome("disconnected", None), None


def uuid7() -> str:
    value = (int(time.time() * 1000) << 80) | (0x7 << 76) | random.getrandbits(76)
    value = (value & ~(0b11 << 62)) | (0b10 << 62)
    return str(uuid.UUID(int=value))


def oauth_token(base: str, client_id: str, client_secret: str, scope: str) -> str:
    payload = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        }
    ).encode()
    request = urllib.request.Request(
        base.rstrip("/") + "/v1/oauth/tokens",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.loads(response.read(1 << 20))
    token = body.get("access_token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("OAuth2 response omitted access_token")
    return token


def wait_for_oauth_token(
    base: str, client_id: str, client_secret: str, scope: str, timeout: float = 90
) -> str:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return oauth_token(base, client_id, client_secret, scope)
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as error:
            last = error
            time.sleep(0.5)
    raise RuntimeError("OAuth endpoint did not recover after restart") from last


def configure_fault(control: Client, rule: dict[str, Any]) -> None:
    outcome, _ = control.request("PUT", "/v1/rule", rule)
    if outcome.status != 200:
        raise RuntimeError(f"fault rule configuration returned {outcome}")


def wait_for_fault_event(control: Client, timeout: float = 30) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        outcome, state = control.request("GET", "/v1/state")
        events = state.get("events", []) if isinstance(state, dict) else []
        if outcome.status == 200 and events:
            return state
        time.sleep(0.1)
    raise RuntimeError("timed out waiting for in-flight fault gate")


def wait_for_table_state(
    client: Client, path: str, timeout: float = 90
) -> tuple[HttpOutcome, dict[str, Any] | None]:
    deadline = time.monotonic() + timeout
    last = HttpOutcome("disconnected", None)
    while time.monotonic() < deadline:
        last, document = client.request("GET", path)
        if last.status == 200 and isinstance(document, dict):
            return last, document
        if last.status == 404:
            return last, None
        time.sleep(0.5)
    raise RuntimeError(f"catalog did not become observable after restart: {last}")


def restart_catalog(args: argparse.Namespace) -> None:
    environment = os.environ.copy()
    environment["CATALOG_BENCH_RUN_ID"] = args.run_id
    subprocess.run(
        [
            "docker", "compose",
            "--project-directory", args.repository_root,
            "--file", os.path.join(args.repository_root, "docker-compose.yml"),
            "--file", os.path.join(args.repository_root, "docker-compose.clean.yml"),
            "--file", os.path.join(args.repository_root, "docker-compose.fault.yml"),
            "restart", "--timeout", "2", args.restart_service,
        ],
        check=True,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=60,
    )


def config_and_prefix(client: Client, warehouse: str, static_prefix: str | None) -> tuple[dict[str, Any], str]:
    query = "?" + urllib.parse.urlencode({"warehouse": warehouse}) if warehouse else ""
    outcome, config = client.request("GET", "/v1/config" + query)
    if outcome.status != 200 or not isinstance(config, dict):
        raise RuntimeError(f"config negotiation failed: {outcome}")
    prefix = static_prefix
    if prefix is None:
        defaults = config.get("defaults", {})
        candidate = defaults.get("prefix") if isinstance(defaults, dict) else None
        prefix = candidate if isinstance(candidate, str) and candidate else ""
    return config, ("/v1/" + prefix if prefix else "/v1")


def create_body(table: str, location: str | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": table,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}],
        },
        "partition-spec": {"spec-id": 0, "fields": []},
        "write-order": {"order-id": 0, "fields": []},
        "properties": {"catalog-bench.owner": "phase3-recovery"},
    }
    if location:
        body["location"] = location
    return body


def table_metadata(document: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise RuntimeError("table response is not an object")
    metadata = document.get("metadata")
    if not isinstance(metadata, dict):
        raise RuntimeError("table response omitted metadata")
    return metadata


def property_value(document: dict[str, Any] | None, name: str) -> str | None:
    properties = table_metadata(document).get("properties", {})
    return properties.get(name) if isinstance(properties, dict) else None


def commit_body(metadata: dict[str, Any], name: str, value: str) -> dict[str, Any]:
    return {
        "requirements": [
            {"type": "assert-table-uuid", "uuid": metadata["table-uuid"]},
            {"type": "assert-current-schema-id", "current-schema-id": metadata["current-schema-id"]},
        ],
        "updates": [{"action": "set-properties", "updates": {name: value}}],
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    bearer = None
    if args.oauth:
        bearer = oauth_token(
            args.proxy_base,
            os.environ[args.oauth_client_id_env],
            os.environ[args.oauth_client_secret_env],
            args.oauth_scope,
        )
    proxy = Client(args.proxy_base, bearer)
    direct = Client(args.direct_base, bearer)
    control = Client(args.control_base)
    config, root = config_and_prefix(proxy, args.warehouse, args.static_prefix)
    direct_config, direct_root = config_and_prefix(direct, args.warehouse, args.static_prefix)
    if root != direct_root or config.get("defaults") != direct_config.get("defaults"):
        raise RuntimeError("proxy and direct config routing disagree")

    namespace = f"cb_c302_{args.catalog.replace('-', '_')}_{args.fixture_id}"
    table = "recovery"
    namespace_path = root + "/namespaces/" + urllib.parse.quote(namespace, safe="")
    collection_path = namespace_path + "/tables"
    table_path = collection_path + "/" + table
    direct.request("DELETE", table_path)
    direct.request("DELETE", namespace_path)
    outcome, _ = direct.request("POST", root + "/namespaces", {"namespace": [namespace]})
    if outcome.status != 200:
        raise RuntimeError(f"namespace create failed: {outcome}")
    location = args.location.rstrip("/") + "/" + namespace + "/" + table if args.location else None
    outcome, created = direct.request("POST", collection_path, create_body(table, location))
    if outcome.status != 200:
        raise RuntimeError(f"table create failed: {outcome}")
    initial = table_metadata(created)

    cases: dict[str, Any] = {}
    for phase, property_name in (
        ("before-upstream", "c3-02.before"),
        ("after-upstream", "c3-02.after"),
    ):
        body = commit_body(initial, property_name, "accepted")
        idempotency = uuid7() if advertised_idempotency(config) else None
        headers = {"Idempotency-Key": idempotency} if idempotency else {}
        configure_fault(
            control,
            {
                "id": "commit-" + phase.removesuffix("-upstream"),
                "method": "POST",
                "path_contains": table_path,
                "occurrence": 1,
                "injections": 1,
                "phase": phase,
                "action": "disconnect",
            },
        )
        first, _ = proxy.request("POST", table_path, body, headers)
        if first.kind != "disconnected":
            raise RuntimeError(f"{phase} request was not disconnected: {first}")
        loaded_status, loaded = direct.request("GET", table_path)
        if loaded_status.status != 200:
            raise RuntimeError(f"{phase} reconciliation load failed: {loaded_status}")
        observed = property_value(loaded, property_name)
        expected_before_retry = None if phase == "before-upstream" else "accepted"
        if observed != expected_before_retry:
            raise RuntimeError(f"{phase} observed property {observed!r}, want {expected_before_retry!r}")

        retry, _ = proxy.request("POST", table_path, body, headers)
        if phase == "before-upstream" and retry.status != 200:
            raise RuntimeError(f"before-upstream exact retry failed: {retry}")
        if phase == "after-upstream":
            allowed = {200} if idempotency else {200, 409}
            if retry.status not in allowed:
                raise RuntimeError(f"after-upstream recovery retry returned {retry}, want {allowed}")
        final_status, final = direct.request("GET", table_path)
        if final_status.status != 200 or property_value(final, property_name) != "accepted":
            raise RuntimeError(f"{phase} final state is not accepted exactly once")

        drift_status = None
        drift_mutated = False
        if idempotency:
            drift = commit_body(table_metadata(final), property_name, "drift-must-not-apply")
            drift_outcome, _ = proxy.request("POST", table_path, drift, headers)
            drift_status = drift_outcome.status
            _, after_drift = direct.request("GET", table_path)
            drift_mutated = property_value(after_drift, property_name) != "accepted"

        state_outcome, state = control.request("GET", "/v1/state")
        if state_outcome.status != 200 or not isinstance(state, dict):
            raise RuntimeError("fault state read failed")
        cases[phase.replace("-", "_")] = {
            "client_disconnected": True,
            "observed_before_retry": observed,
            "retry_status": retry.status,
            "idempotency_advertised": idempotency is not None,
            "idempotency_key_sha256": sha256(idempotency) if idempotency else None,
            "drift_status": drift_status,
            "drift_mutated": drift_mutated,
            "final_property": "accepted",
            "fault_events": state.get("events", []),
        }
        initial = table_metadata(final)

    property_name = "c3-02.restart"
    body = commit_body(initial, property_name, "accepted")
    configure_fault(
        control,
        {
            "id": "commit-restart",
            "method": "POST",
            "path_contains": table_path,
            "occurrence": 1,
            "injections": 1,
            "phase": "during-upstream",
            "action": "pause-request-body",
        },
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(proxy.request, "POST", table_path, body)
        gate_state = wait_for_fault_event(control)
        restart_catalog(args)
        release, _ = control.request("POST", "/v1/release")
        if release.status != 200:
            raise RuntimeError(f"in-flight gate release failed: {release}")
        interrupted, _ = pending.result(timeout=30)
    if args.oauth:
        bearer = wait_for_oauth_token(
            args.direct_base,
            os.environ[args.oauth_client_id_env],
            os.environ[args.oauth_client_secret_env],
            args.oauth_scope,
        )
        proxy.bearer = bearer
        direct.bearer = bearer
    restarted_status, after_restart = wait_for_table_state(direct, table_path)
    durable_fixture_present = restarted_status.status == 200
    observed = property_value(after_restart, property_name) if durable_fixture_present else None
    if durable_fixture_present and observed is not None:
        raise RuntimeError(f"partial in-flight request mutated state before exact retry: {observed!r}")
    retry, _ = proxy.request("POST", table_path, body)
    final_status, final = wait_for_table_state(direct, table_path)
    final_property = property_value(final, property_name) if final_status.status == 200 else None
    if durable_fixture_present and (retry.status != 200 or final_property != "accepted"):
        raise RuntimeError("restart exact retry did not reach accepted state")
    cases["restart_during_commit"] = {
        "request_outcome": asdict(interrupted),
        "durable_fixture_present": durable_fixture_present,
        "observed_before_retry": observed,
        "retry_status": retry.status,
        "final_property": final_property,
        "fault_events": gate_state.get("events", []),
    }

    drop, _ = direct.request("DELETE", table_path)
    drop_namespace, _ = direct.request("DELETE", namespace_path)
    if drop.status not in {200, 204, 404} or drop_namespace.status not in {200, 204, 404}:
        raise RuntimeError(f"cleanup failed: table={drop} namespace={drop_namespace}")

    return {
        "schema_version": "catalog-bench.catalog-recovery-probe.v2",
        "catalog": args.catalog,
        "fixture_id": args.fixture_id,
        "routing": {"prefix_sha256": sha256(root), "oauth": args.oauth},
        "cases": cases,
        "cleanup": {"table_dropped": True, "namespace_dropped": True},
    }


def advertised_idempotency(config: dict[str, Any]) -> bool:
    values = [config.get("idempotency-key-lifetime")]
    for name in ("defaults", "overrides"):
        mapping = config.get(name, {})
        values.append(mapping.get("idempotency-key-lifetime") if isinstance(mapping, dict) else None)
    return any(isinstance(value, str) and value.strip() for value in values)


def sha256(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--fixture-id", required=True)
    parser.add_argument("--proxy-base", required=True)
    parser.add_argument("--direct-base", required=True)
    parser.add_argument("--control-base", required=True)
    parser.add_argument("--warehouse", default="")
    parser.add_argument("--static-prefix")
    parser.add_argument("--location")
    parser.add_argument("--repository-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--restart-service", required=True)
    parser.add_argument("--oauth", action="store_true")
    parser.add_argument("--oauth-client-id-env", default="CATALOG_BENCH_POLARIS_CLIENT_ID")
    parser.add_argument("--oauth-client-secret-env", default="CATALOG_BENCH_POLARIS_CLIENT_SECRET")
    parser.add_argument("--oauth-scope", default="PRINCIPAL_ROLE:ALL")
    args = parser.parse_args()
    if re.fullmatch(r"[a-z0-9][a-z0-9_]{0,23}", args.fixture_id) is None:
        parser.error("fixture-id must be 1-24 lowercase ASCII letters, digits, or underscores")
    for name in ("proxy_base", "direct_base", "control_base"):
        parsed = urllib.parse.urlparse(getattr(args, name))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.query or parsed.fragment:
            parser.error(f"{name.replace('_', '-')} must be an absolute credential-free HTTP(S) URL")
    if os.path.abspath(args.repository_root) != args.repository_root:
        parser.error("repository-root must be absolute")
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*", args.restart_service) is None:
        parser.error("restart-service must be a Compose service name")
    if args.run_id != args.fixture_id:
        parser.error("run-id must equal fixture-id")
    return args


def main() -> None:
    result = run(parse_args())
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
