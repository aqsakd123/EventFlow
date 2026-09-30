"""Black-box matrix for OFF and VERSION_LSN read routing policies."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone

from test_scenarios import Scenario, ScenarioFailure, headers


def header(result, name: str) -> str | None:
    mapping = result.headers if hasattr(result, "headers") else result
    return next((value for key, value in mapping.items() if key.lower() == name.lower()), None)


def future_lsn(lsn: str) -> str:
    major, minor = (int(part, 16) for part in lsn.split("/"))
    value = (major << 32) + minor + 0x1000000
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


def run(args: argparse.Namespace) -> None:
    scenario = Scenario(args.base_url, "docker", 10000, "eventflow", args.compose_file)
    actor = headers("routing-matrix-organizer", "routing-matrix-workspace", "ORGANIZER")
    starts = datetime.now(timezone.utc) + timedelta(days=2)
    payload = {
        "title": "read-consistency-routing-matrix",
        "description": "routing policy acceptance fixture",
        "startsAt": starts.isoformat().replace("+00:00", "Z"),
        "endsAt": (starts + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "timezone": "UTC",
        "capacity": 1,
    }

    created_response = scenario.request("POST", "/api/v1/events", actor, payload)
    created = scenario.expect(created_response, 201, "matrix create")
    event_id = created["id"]
    published_response = scenario.request("POST", f"/api/v1/events/{event_id}/publish", actor)
    published = scenario.expect(published_response, 200, "matrix publish")
    receipt_lsn = header(published_response, "X-EventFlow-Commit-LSN")
    receipt_version = header(published_response, "X-EventFlow-Entity-Version")
    scope = header(published_response, "X-EventFlow-Consistency-Scope")
    entity_key = header(published_response, "X-EventFlow-Entity-Key")
    if not receipt_lsn or receipt_version is None or scope != "event" or entity_key != event_id:
        raise ScenarioFailure(f"publish did not return a scoped consistency receipt: {published_response.headers}")

    # The list endpoint is explicitly OFF: causal headers must be ignored.
    list_response = scenario.request(
        "GET",
        "/api/v1/events",
        {**actor, "X-EventFlow-Min-LSN": future_lsn(receipt_lsn), "X-EventFlow-Min-Version": "999999"},
    )
    scenario.expect(list_response, 200, "OFF list read")
    if not (header(list_response, "X-EventFlow-DB-Route") or "").startswith("replica-"):
        raise ScenarioFailure(f"OFF list unexpectedly used primary: {list_response.headers}")

    causal_headers = {
        **actor,
        "X-EventFlow-Min-LSN": receipt_lsn,
        "X-EventFlow-Min-Version": receipt_version,
        "X-EventFlow-Consistency-Scope": scope,
        "X-EventFlow-Entity-Key": entity_key,
    }
    replica_headers = None
    for _ in range(30):
        result = scenario.request("GET", f"/api/v1/events/{event_id}", causal_headers)
        scenario.expect(result, 200, "caught-up causal read")
        route = header(result, "X-EventFlow-DB-Route") or ""
        if route.startswith("replica-"):
            replica_headers = result.headers
            break
        time.sleep(0.5)
    if replica_headers is None:
        raise ScenarioFailure("replica never became eligible for the committed LSN")

    primary_response = scenario.request(
        "GET",
        f"/api/v1/events/{event_id}",
        {
            **causal_headers,
            "X-EventFlow-Min-LSN": future_lsn(receipt_lsn),
        },
    )
    scenario.expect(primary_response, 200, "uncaught-up causal read")
    if header(primary_response, "X-EventFlow-DB-Route") != "primary":
        raise ScenarioFailure(f"uncaught-up threshold was served by replica: {primary_response.headers}")

    mismatched_scope = scenario.request(
        "GET",
        f"/api/v1/events/{event_id}",
        {**causal_headers, "X-EventFlow-Consistency-Scope": "other-feature"},
    )
    scenario.expect(mismatched_scope, 200, "scope mismatch read")
    if header(mismatched_scope, "X-EventFlow-DB-Route") != "primary":
        raise ScenarioFailure(f"scope mismatch was served by replica: {mismatched_scope.headers}")
    print(json.dumps({
        "status": "PASS",
        "checks": [
            "OFF list ignores causal headers and routes to replica",
            "VERSION_LSN routes scoped receipt to replica",
            "higher LSN threshold routes to primary",
            "scope mismatch falls back to primary",
        ],
        "receiptLsn": receipt_lsn,
        "replicaRoute": header(replica_headers, "X-EventFlow-DB-Route"),
        "primaryRoute": header(primary_response, "X-EventFlow-DB-Route"),
    }, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:28181")
    parser.add_argument("--compose-file", default="docker-compose.consistency.yml")
    try:
        run(parser.parse_args())
    except (ScenarioFailure, OSError) as error:
        print(f"READ CONSISTENCY MATRIX FAILED: {error}", file=sys.stderr)
        sys.exit(1)
