"""Black-box HA routing and reconciliation scenario for docker-compose.consistency.yml."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class ScenarioFailure(AssertionError):
    pass


def request(method: str, url: str, headers: dict[str, str], body: Any = None) -> tuple[int, Any, dict[str, str]]:
    encoded = None if body is None else json.dumps(body).encode("utf-8")
    actual = {"Accept": "application/json", **headers}
    if encoded is not None:
        actual["Content-Type"] = "application/json"
    try:
        with urlopen(Request(url, data=encoded, headers=actual, method=method), timeout=15) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None, {
                key.lower(): value for key, value in response.headers.items()
            }
    except HTTPError as error:
        raw = error.read()
        return error.code, json.loads(raw) if raw else None, {
            key.lower(): value for key, value in error.headers.items()
        }
    except (URLError, TimeoutError) as error:
        raise ScenarioFailure(f"{method} {url} transport failure: {error}") from error


def expect(result: tuple[int, Any, dict[str, str]], status: int, label: str) -> tuple[Any, dict[str, str]]:
    actual, body, headers = result
    if actual != status:
        raise ScenarioFailure(f"{label}: expected HTTP {status}, got {actual}: {body}")
    return body, headers


def command(compose_file: str, *args: str) -> None:
    result = subprocess.run(
        ["docker", "compose", "-f", compose_file, *args],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise ScenarioFailure(f"docker compose {' '.join(args)} failed: {result.stderr.strip()}")


def wait_for(operation: Any, label: str, timeout: float = 45) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = operation()
            if last:
                return last
        except (ScenarioFailure, URLError):
            pass
        time.sleep(0.5)
    raise ScenarioFailure(f"timeout waiting for {label}; last={last!r}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway-url", default="http://localhost:28181")
    parser.add_argument("--event-url", default="http://localhost:28081")
    parser.add_argument("--registration-url", default="http://localhost:28082")
    parser.add_argument("--compose-file", default="docker-compose.consistency.yml")
    parser.add_argument("--reconciliation-key", default="local-reconciliation-key")
    args = parser.parse_args()

    session = f"ha-{uuid.uuid4()}"
    actor = {
        "X-User-Id": "ha-organizer",
        "X-Workspace-Id": "ha-workspace",
        "X-Roles": "ORGANIZER",
        "X-EventFlow-Consistency-Session": session,
    }
    internal = {
        "X-EventFlow-Reconciliation-Key": args.reconciliation_key,
        "X-Workspace-Id": "ha-workspace",
    }
    starts = datetime.now(timezone.utc) + timedelta(hours=2)
    create_body = {
        "title": "HA routing event",
        "description": "before partition",
        "startsAt": starts.isoformat(),
        "endsAt": (starts + timedelta(hours=2)).isoformat(),
        "timezone": "UTC",
        "capacity": 10,
    }

    event, receipt = expect(
        request("POST", args.gateway_url + "/api/v1/events", actor, create_body),
        201,
        "create event",
    )
    event_id = event["id"]
    lsn = receipt.get("x-eventflow-commit-lsn")
    version = receipt.get("x-eventflow-entity-version")
    if not lsn or version is None:
        raise ScenarioFailure(f"write did not return LSN/version receipt: {receipt}")

    unauthenticated = request(
        "GET", f"{args.event_url}/internal/reconciliation/events/{event_id}", {}
    )
    if unauthenticated[0] != 401:
        raise ScenarioFailure(f"internal endpoint accepted unauthenticated request: {unauthenticated}")

    version_only = {
        **actor,
        "X-EventFlow-Consistency-Session": f"version-only-{uuid.uuid4()}",
        "X-EventFlow-Min-Version": version,
    }
    _, version_only_headers = expect(
        request("GET", f"{args.gateway_url}/api/v1/events/{event_id}", version_only),
        200,
        "version-only causal read",
    )
    if version_only_headers.get("x-eventflow-db-route") != "primary":
        raise ScenarioFailure(f"incomplete causal hint did not conservatively use primary: {version_only_headers}")

    causal = {
        **actor,
        "X-EventFlow-Min-LSN": lsn,
        "X-EventFlow-Min-Version": version,
        "X-EventFlow-Consistency-Scope": receipt["x-eventflow-consistency-scope"],
        "X-EventFlow-Entity-Key": receipt["x-eventflow-entity-key"],
    }
    _, immediate_headers = expect(
        request("GET", f"{args.gateway_url}/api/v1/events/{event_id}", causal),
        200,
        "causal read",
    )
    if immediate_headers.get("x-eventflow-db-route") not in {"primary", "replica-1", "replica-2", "replica-3"}:
        raise ScenarioFailure(f"causal read returned an unknown route: {immediate_headers}")

    time.sleep(3.5)
    _, replica_headers = expect(
        request("GET", f"{args.gateway_url}/api/v1/events/{event_id}", causal),
        200,
        "post-pin causal read",
    )
    if not replica_headers.get("x-eventflow-db-route", "").startswith("replica-"):
        raise ScenarioFailure(f"caught-up replica was not selected: {replica_headers}")

    # All replicas unavailable must degrade to the primary, never to a stale error.
    replicas = ("event-db-replica-1", "event-db-replica-2", "event-db-replica-3")
    command(args.compose_file, "stop", *replicas)
    try:
        _, fallback_headers = expect(
            request("GET", f"{args.gateway_url}/api/v1/events/{event_id}", causal),
            200,
            "replica outage fallback",
        )
        if fallback_headers.get("x-eventflow-db-route") != "primary":
            raise ScenarioFailure(f"replica outage did not fall back to primary: {fallback_headers}")
    finally:
        command(args.compose_file, "start", *replicas)

    published, _ = expect(
        request("POST", f"{args.gateway_url}/api/v1/events/{event_id}/publish", actor),
        200,
        "publish event",
    )

    def projection_ready() -> bool:
        status, _, _ = request(
            "PATCH",
            f"{args.registration_url}/internal/reconciliation/events/{event_id}/chaos",
            internal,
            {},
        )
        return status == 200

    wait_for(projection_ready, "registration projection")

    # Rabbit outage is the local data-flow partition. Both databases can now
    # accept independent writes to the shared coordination fields.
    command(args.compose_file, "stop", "rabbitmq")
    try:
        source_only, _ = expect(
            request("POST", args.gateway_url + "/api/v1/events", actor, {
                **create_body,
                "title": "Source-only event during partition",
            }),
            201,
            "create source-only event",
        )
        update_body = {
            "title": published["title"],
            "description": "event-side write during partition",
            "startsAt": published["startsAt"],
            "endsAt": published["endsAt"],
            "timezone": published["timezone"],
            "capacity": 8,
            "version": published["version"],
        }
        updated, _ = expect(
            request("PATCH", f"{args.gateway_url}/api/v1/events/{event_id}", actor, update_body),
            200,
            "event-side capacity update",
        )
        expect(
            request("POST", f"{args.gateway_url}/api/v1/events/{event_id}/cancel", actor),
            200,
            "event-side terminal transition",
        )
        time.sleep(0.02)
        expect(
            request(
                "PATCH",
                f"{args.registration_url}/internal/reconciliation/events/{event_id}/chaos",
                internal,
                {"capacity": 12, "status": "ENDED", "registrationOpen": False},
            ),
            200,
            "projection-side divergent write",
        )

        report, _ = expect(
            request("POST", args.registration_url + "/internal/reconciliation/run", internal, {}),
            200,
            "reconciliation run",
        )
        if report["failed"] or report["quarantined"] or report["resolved"] < 1:
            raise ScenarioFailure(f"unexpected reconciliation report: {report}")

        source, _ = expect(
            request("GET", f"{args.event_url}/internal/reconciliation/events/{event_id}", internal),
            200,
            "source after reconciliation",
        )
        projection, _ = expect(
            request(
                "PATCH",
                f"{args.registration_url}/internal/reconciliation/events/{event_id}/chaos",
                internal,
                {},
            ),
            200,
            "projection after reconciliation",
        )
        for field in ("status", "registrationOpen", "capacity", "confirmedCount"):
            if source[field] != projection[field]:
                raise ScenarioFailure(f"branches did not converge for {field}: {source} != {projection}")
        if source["status"] != "CANCELLED":
            raise ScenarioFailure(f"terminal source state was resurrected: {source}")
        if source["capacity"] != 12:
            raise ScenarioFailure(f"same-field LWW did not select later projection write: {source}")

        expect(
            request(
                "PATCH",
                f"{args.registration_url}/internal/reconciliation/events/{source_only['id']}/chaos",
                internal,
                {},
            ),
            200,
            "source-only projection repaired",
        )

        regression = request(
            "PATCH",
            f"{args.registration_url}/internal/reconciliation/events/{event_id}/chaos",
            internal,
            {"status": "PUBLISHED", "registrationOpen": True},
        )
        if regression[0] != 409:
            raise ScenarioFailure(f"terminal state regression was accepted: {regression}")

        conflicts, _ = expect(
            request("GET", args.registration_url + "/internal/reconciliation/conflicts", internal),
            200,
            "conflict audit",
        )
        fields = {row["field_name"] for row in conflicts}
        if not {"capacity", "status"}.issubset(fields):
            raise ScenarioFailure(f"missing conflict audit rows: {conflicts}")
    finally:
        command(args.compose_file, "start", "rabbitmq")

    print(json.dumps({"status": "PASS", "eventId": event_id, "route": replica_headers, "report": report}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
