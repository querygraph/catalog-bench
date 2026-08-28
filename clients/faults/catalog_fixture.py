#!/usr/bin/env python3
"""Create, verify, or remove a standard Iceberg REST backup fixture."""

from __future__ import annotations

import argparse
import json
import os
import urllib.parse

from catalog_recovery import Client, config_and_prefix, create_body, oauth_token, sha256, table_metadata


def main() -> None:
    args = parse_args()
    bearer = None
    if args.oauth:
        bearer = oauth_token(
            args.base,
            os.environ["CATALOG_BENCH_POLARIS_CLIENT_ID"],
            os.environ["CATALOG_BENCH_POLARIS_CLIENT_SECRET"],
            "PRINCIPAL_ROLE:ALL",
        )
    client = Client(args.base, bearer)
    _, root = config_and_prefix(client, args.warehouse, args.static_prefix)
    namespace = f"cb_c304_{args.catalog}_{args.fixture_id}"
    namespace_path = root + "/namespaces/" + urllib.parse.quote(namespace, safe="")
    table_path = namespace_path + "/tables/backup"
    if args.operation == "create":
        client.request("DELETE", table_path)
        client.request("DELETE", namespace_path)
        outcome, _ = client.request("POST", root + "/namespaces", {"namespace": [namespace]})
        if outcome.status != 200:
            raise RuntimeError(f"namespace create failed: {outcome}")
        location = args.location.rstrip("/") + "/" + namespace + "/backup" if args.location else None
        outcome, document = client.request("POST", namespace_path + "/tables", create_body("backup", location))
        if outcome.status != 200:
            raise RuntimeError(f"table create failed: {outcome}")
        metadata = table_metadata(document)
        print(json.dumps({"catalog": args.catalog, "fixture": sha256(namespace), "table_uuid": metadata["table-uuid"], "metadata_location": sha256(document["metadata-location"])}))
    elif args.operation == "verify":
        outcome, document = client.request("GET", table_path)
        result = {"catalog": args.catalog, "status": outcome.status, "restored": outcome.status == 200}
        if outcome.status == 200:
            metadata = table_metadata(document)
            result.update({"table_uuid": metadata["table-uuid"], "metadata_location": sha256(document["metadata-location"])})
        print(json.dumps(result))
    else:
        table, _ = client.request("DELETE", table_path)
        namespace_outcome, _ = client.request("DELETE", namespace_path)
        print(json.dumps({"catalog": args.catalog, "table_status": table.status, "namespace_status": namespace_outcome.status}))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("create", "verify", "drop"))
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--fixture-id", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--warehouse", default="")
    parser.add_argument("--static-prefix")
    parser.add_argument("--location")
    parser.add_argument("--oauth", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    main()
