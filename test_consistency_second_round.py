"""Focused second-round checks for reconciliation fairness and projection CAS state."""

from __future__ import annotations

import json
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


COMPOSE = "docker-compose.consistency.yml"
GATEWAY = "http://localhost:28181"
EVENT = "http://localhost:28081"
REGISTRATION = "http://localhost:28082"
KEY = "local-reconciliation-key"


class CheckFailure(AssertionError):
    pass


def request(method: str, url: str, headers: dict[str, str], body: object = None) -> tuple[int, object, dict[str, str]]:
    encoded = None if body is None else json.dumps(body).encode("utf-8")
    actual = {"Accept": "application/json", **headers}
    if encoded is not None:
        actual["Content-Type"] = "application/json"
    try:
        with urlopen(Request(url, data=encoded, headers=actual, method=method), timeout=20) as response:
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
        raise CheckFailure(f"{method} {url} transport failure: {error}") from error


def expect(result: tuple[int, object, dict[str, str]], status: int, label: str) -> tuple[object, dict[str, str]]:
    actual, body, headers = result
    if actual != status:
        raise CheckFailure(f"{label}: expected HTTP {status}, got {actual}: {body}")
    return body, headers


def compose(*args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, *args],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise CheckFailure(f"docker compose {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def psql(service: str, database: str, sql: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, "exec", "-T", service,
         "psql", "-U", "eventflow", "-d", database, "-At", "-c", sql],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise CheckFailure(f"psql failed: {result.stderr.strip()}")
    return result.stdout.strip()


def main() -> int:
    workspace = f"second-round-{uuid.uuid4()}"
    actor = {
        "X-User-Id": "second-round-organizer",
        "X-Workspace-Id": workspace,
        "X-Roles": "ORGANIZER",
        "X-EventFlow-Consistency-Session": f"second-round-{uuid.uuid4()}",
    }
    internal = {
        "X-EventFlow-Reconciliation-Key": KEY,
        "X-Workspace-Id": workspace,
    }

    event_id = "00000000-0000-0000-0000-000000000000"
    auth_statuses = [
        request("GET", f"{EVENT}/internal/reconciliation/events/{event_id}", {})[0],
        request("GET", f"{EVENT}/internal/reconciliation/events/{event_id}",
                {"X-EventFlow-Reconciliation-Key": "wrong"})[0],
        request("GET", f"{REGISTRATION}/internal/reconciliation/conflicts", {})[0],
        request("GET", f"{REGISTRATION}/internal/reconciliation/conflicts",
                {"X-EventFlow-Reconciliation-Key": "wrong", "X-Workspace-Id": workspace})[0],
    ]
    if auth_statuses != [401, 401, 401, 401]:
        raise CheckFailure(f"internal auth statuses: {auth_statuses}")

    bindings = compose("ps", "-a")
    for binding in ("127.0.0.1:28081->8081/tcp", "127.0.0.1:28082->8082/tcp"):
        if binding not in bindings:
            raise CheckFailure(f"missing localhost binding {binding}: {bindings}")

    starts = datetime.now(timezone.utc) + timedelta(hours=2)
    event_body = {
        "title": "Second-round route probe",
        "description": "bounded verification",
        "startsAt": starts.isoformat(),
        "endsAt": (starts + timedelta(hours=2)).isoformat(),
        "timezone": "UTC",
        "capacity": 10,
    }
    created, receipt = expect(
        request("POST", f"{GATEWAY}/api/v1/events", actor, event_body), 201, "route probe create"
    )
    version_headers = {
        **actor,
        "X-EventFlow-Consistency-Session": f"version-only-{uuid.uuid4()}",
        "X-EventFlow-Min-Version": str(created["version"]),
    }
    _, route_headers = expect(
        request("GET", f"{GATEWAY}/api/v1/events/{created['id']}", version_headers),
        200,
        "version-only read",
    )
    if route_headers.get("x-eventflow-db-route") != "primary":
        raise CheckFailure(f"version-only route: {route_headers}")

    main_event_id = "a9741dca-f670-4153-9bac-ac50c236cde3"
    main_internal = {
        "X-EventFlow-Reconciliation-Key": KEY,
        "X-Workspace-Id": "ha-workspace",
    }
    source, _ = expect(
        request("GET", f"{EVENT}/internal/reconciliation/events/{main_event_id}", main_internal),
        200,
        "main source convergence",
    )
    projection, _ = expect(
        request("PATCH", f"{REGISTRATION}/internal/reconciliation/events/{main_event_id}/chaos",
                main_internal, {}),
        200,
        "main projection convergence",
    )
    for field in ("status", "registrationOpen", "capacity", "confirmedCount"):
        if source[field] != projection[field]:
            raise CheckFailure(f"main event diverged for {field}: {source} != {projection}")
    if source["status"] != "CANCELLED":
        raise CheckFailure(f"main terminal state changed: {source}")
    conflicts, _ = expect(
        request("GET", f"{REGISTRATION}/internal/reconciliation/conflicts", main_internal),
        200,
        "main conflict audit",
    )
    if not {"capacity", "status"}.issubset({row["field_name"] for row in conflicts}):
        raise CheckFailure(f"main conflict audit incomplete: {conflicts}")
    regression = request(
        "PATCH", f"{REGISTRATION}/internal/reconciliation/events/{main_event_id}/chaos",
        main_internal, {"status": "PUBLISHED", "registrationOpen": True},
    )
    if regression[0] != 409:
        raise CheckFailure(f"terminal regression status: {regression}")

    fairness_workspace = f"{workspace}-fairness"
    fairness_actor = {**actor, "X-Workspace-Id": fairness_workspace}
    fairness_internal = {**internal, "X-Workspace-Id": fairness_workspace}
    compose("stop", "rabbitmq")
    try:
        first = None
        for index in range(201):
            body = {**event_body, "title": f"Fairness source-only {index:03d}"}
            event, _ = expect(
                request("POST", f"{GATEWAY}/api/v1/events", fairness_actor, body),
                201,
                f"source-only create {index}",
            )
            if first is None:
                first = event

        first_report, _ = expect(
            request("POST", f"{REGISTRATION}/internal/reconciliation/run", fairness_internal, {}),
            200,
            "fairness first page",
        )
        second_report, _ = expect(
            request("POST", f"{REGISTRATION}/internal/reconciliation/run", fairness_internal, {}),
            200,
            "fairness second page",
        )
        count = int(psql(
            "registration-db", "eventflow_registration",
            f"SELECT count(*) FROM event_projections WHERE workspace_id = '{fairness_workspace}'",
        ))
        if first_report != {"scanned": 200, "resolved": 200, "conflicts": 0,
                            "quarantined": 0, "failed": 0}:
            raise CheckFailure(f"fairness first report: {first_report}")
        if second_report["scanned"] != 201 or second_report["resolved"] != 1 or second_report["failed"] != 0:
            raise CheckFailure(f"fairness second report: {second_report}")
        if count != 201:
            raise CheckFailure(f"source-only projection count: {count}")

        first_id = first["id"]
        expect(
            request("PATCH", f"{REGISTRATION}/internal/reconciliation/events/{first_id}/chaos",
                    fairness_internal, {"capacity": 12}),
            200,
            "projection-side revision write",
        )
        before = psql(
            "registration-db", "eventflow_registration",
            f"SELECT event_version || '|' || local_revision FROM event_projections WHERE event_id = '{first_id}'",
        )
        if before != "0|1":
            raise CheckFailure(f"unexpected pre-CAS state: {before}")
        third_report, _ = expect(
            request("POST", f"{REGISTRATION}/internal/reconciliation/run", fairness_internal, {}),
            200,
            "projection CAS apply",
        )
        after = psql(
            "registration-db", "eventflow_registration",
            f"SELECT event_version || '|' || local_revision FROM event_projections WHERE event_id = '{first_id}'",
        )
        if after != "1|2":
            raise CheckFailure(f"unexpected post-CAS state: {after}")
        if third_report["failed"] != 0 or third_report["resolved"] < 1:
            raise CheckFailure(f"projection CAS report: {third_report}")

        cas_probe = psql(
            "registration-db", "eventflow_registration",
            f"UPDATE event_projections SET capacity = capacity "
            f"WHERE event_id = '{first_id}' AND event_version = 999999 AND local_revision = 999999",
        )
        if cas_probe != "UPDATE 0":
            raise CheckFailure(f"stale CAS probe changed rows: {cas_probe}")
    finally:
        compose("start", "rabbitmq")

    print(json.dumps({
        "status": "PASS",
        "auth": auth_statuses,
        "version_only_route": route_headers.get("x-eventflow-db-route"),
        "bindings": ["127.0.0.1:28081->8081/tcp", "127.0.0.1:28082->8082/tcp"],
        "fairness": {"first": first_report, "second": second_report, "projection_count": count},
        "cas": {"before": before, "after": after, "stale_probe": cas_probe, "apply": third_report},
        "terminal": {"status": source["status"], "conflict_fields": ["capacity", "status"]},
        "receipt_version": receipt.get("x-eventflow-entity-version"),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (CheckFailure, subprocess.SubprocessError) as error:
        print(f"SECOND-ROUND CHECK FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
