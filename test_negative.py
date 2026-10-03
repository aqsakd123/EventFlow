"""Black-box negative security, media, internal-auth and quarantine checks."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

from test_scenarios import (
    Scenario,
    ScenarioFailure,
    create_event,
    headers,
    publish_and_wait_projection,
    register_when_ready,
)


def expect_error(scenario: Scenario, result, status: int, code: str, label: str) -> None:
    scenario.expect_error(result, status, code, label)


def upload(url: str, content: bytes, content_type: str) -> None:
    request = Request(url, data=content, headers={"Content-Type": content_type}, method="PUT")
    with urlopen(request, timeout=15) as response:
        if response.status not in (200, 201):
            raise ScenarioFailure(f"presigned upload failed: HTTP {response.status}")


def host_reachable_signed_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.hostname == "localstack":
        return urlunsplit((parsed.scheme, "localhost:4566", parsed.path, parsed.query, parsed.fragment))
    return url


def run(args: argparse.Namespace) -> None:
    scenario = Scenario(args.gateway_url, "docker", 10000, "eventflow", args.compose_file)
    organizer = headers("negative-organizer", "negative-workspace", "ORGANIZER")
    participant = headers("negative-participant", "negative-workspace", "PARTICIPANT")
    other_tenant = headers("negative-other", "other-workspace", "ORGANIZER")
    staff = headers("negative-staff", "negative-workspace", "CHECKIN_STAFF")
    starts = datetime.now(timezone.utc) + timedelta(days=2)
    auth_probe = {
        "title": "auth-probe",
        "description": "negative test",
        "startsAt": starts.isoformat().replace("+00:00", "Z"),
        "endsAt": (starts + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "timezone": "UTC",
        "capacity": 1,
    }

    # Authentication, role and tenant boundaries.
    expect_error(
        scenario,
        scenario.request("POST", "/api/v1/events", {}, auth_probe),
        401,
        "AUTHENTICATION_REQUIRED",
        "missing identity",
    )
    expect_error(
        scenario,
        scenario.request("POST", "/api/v1/events", participant, auth_probe),
        403,
        "ORGANIZER_REQUIRED",
        "participant cannot create event",
    )
    expect_error(
        scenario,
        scenario.request("GET", "/api/v1/events", {"X-User-Id": "no-workspace", "X-Roles": "PARTICIPANT"}),
        403,
        "WORKSPACE_REQUIRED",
        "workspace is required",
    )

    invalid = scenario.request("POST", "/api/v1/events", organizer, {
        "title": "",
        "description": "invalid",
        "startsAt": "not-an-instant",
        "endsAt": "not-an-instant",
        "timezone": "UTC",
        "capacity": 0,
    })
    expect_error(scenario, invalid, 400, "VALIDATION_ERROR", "invalid event payload")

    event = create_event(scenario, organizer, "negative-media-security", 1)
    event_id = event["id"]
    publish_and_wait_projection(scenario, event_id, organizer)
    scenario.wait_for(
        lambda: scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'")
        if scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'") == "PUBLISHED" else None,
        "negative test projection",
    )

    expect_error(
        scenario,
        scenario.request("GET", f"/api/v1/events/{event_id}", other_tenant),
        403,
        "TENANT_ACCESS_DENIED",
        "cross-tenant event read",
    )
    expect_error(
        scenario,
        scenario.request("POST", f"/api/v1/events/{event_id}/media/upload-session", other_tenant, {
            "fileName": "cross-tenant.png", "contentType": "image/png", "size": 4,
        }),
        403,
        "TENANT_ACCESS_DENIED",
        "cross-tenant media upload",
    )
    expect_error(
        scenario,
        scenario.request("POST", f"/api/v1/events/{event_id}/registrations", participant),
        400,
        "IDEMPOTENCY_KEY_REQUIRED",
        "missing idempotency key",
    )
    expect_error(
        scenario,
        scenario.request("POST", f"/api/v1/events/{event_id}/check-ins", participant, {"participantId": participant["X-User-Id"]}),
        403,
        "CHECKIN_STAFF_REQUIRED",
        "participant cannot check in",
    )
    expect_error(
        scenario,
        scenario.request("GET", f"/api/v1/events/{event_id}/attendance", participant),
        403,
        "ATTENDANCE_ACCESS_DENIED",
        "participant cannot read attendance",
    )

    # Media validation and object metadata mismatch.
    expect_error(
        scenario,
        scenario.request("POST", f"/api/v1/events/{event_id}/media/upload-session", organizer, {
            "fileName": "bad.txt", "contentType": "text/plain", "size": 4,
        }),
        400,
        "MEDIA_NOT_ALLOWED",
        "unsupported media type",
    )
    expect_error(
        scenario,
        scenario.request("POST", f"/api/v1/events/{event_id}/media/upload-session", organizer, {
            "fileName": "too-large.png", "contentType": "image/png", "size": 5 * 1024 * 1024 + 1,
        }),
        400,
        "MEDIA_NOT_ALLOWED",
        "oversized media",
    )

    missing_object = scenario.expect(
        scenario.request("POST", f"/api/v1/events/{event_id}/media/upload-session", organizer, {
            "fileName": "missing.png", "contentType": "image/png", "size": 4,
        }),
        201,
        "create missing-object media session",
    )
    expect_error(
        scenario,
        scenario.request("POST", f"/api/v1/events/{event_id}/media/{missing_object['mediaId']}/finalize", organizer),
        409,
        "MEDIA_NOT_READY",
        "finalize before upload",
    )
    missing_state = scenario.db_query("event", f"SELECT state FROM event_media WHERE id = '{missing_object['mediaId']}'")
    if missing_state != "REJECTED":
        raise ScenarioFailure(f"missing object was not rejected in DB: {missing_state!r}")

    mismatch = scenario.expect(
        scenario.request("POST", f"/api/v1/events/{event_id}/media/upload-session", organizer, {
            "fileName": "mismatch.png", "contentType": "image/png", "size": 4,
        }),
        201,
        "create mismatch media session",
    )
    upload(host_reachable_signed_url(mismatch["uploadUrl"]), b"bad", "image/png")
    rejected = scenario.expect(
        scenario.request("POST", f"/api/v1/events/{event_id}/media/{mismatch['mediaId']}/finalize", organizer),
        200,
        "finalize metadata mismatch",
    )
    if rejected.get("state") != "REJECTED" or rejected.get("actualSize") != 3:
        raise ScenarioFailure(f"metadata mismatch was not rejected: {rejected}")

    # Internal reconciliation endpoints reject missing and incorrect keys.
    internal = {"X-Workspace-Id": "negative-workspace"}
    for base_url, path, label in (
        (args.event_url, f"/internal/reconciliation/events/{event_id}", "event internal auth"),
        (args.registration_url, f"/internal/reconciliation/events/{event_id}/chaos", "registration internal auth"),
    ):
        result = scenario.request_absolute("GET" if base_url == args.event_url else "PATCH", base_url + path, internal, {} if base_url != args.event_url else None)
        if result.status != 401:
            raise ScenarioFailure(f"{label} accepted missing key: {result.status} {result.body}")
        wrong = {**internal, "X-EventFlow-Reconciliation-Key": "wrong-key"}
        result = scenario.request_absolute("GET" if base_url == args.event_url else "PATCH", base_url + path, wrong, {} if base_url != args.event_url else None)
        if result.status != 401:
            raise ScenarioFailure(f"{label} accepted wrong key: {result.status} {result.body}")

    # Quarantine fixture: a direct DB-only corruption makes capacity lower than confirmed_count.
    quarantine_event = create_event(scenario, organizer, "negative-quarantine", 1)
    quarantine_id = quarantine_event["id"]
    publish_and_wait_projection(scenario, quarantine_id, organizer)
    register_when_ready(scenario, quarantine_id, participant, f"negative-quarantine-{uuid.uuid4().hex}")
    quarantine_sql = "SET search_path TO registration_service; ALTER TABLE event_projections DROP CONSTRAINT event_projections_capacity_check; UPDATE event_projections SET capacity = 0, sync_meta = jsonb_set(sync_meta, '{capacity}', to_jsonb('9999999999999:0:negative-test'::text), true), local_revision = local_revision + 1, updated_at = now() WHERE event_id = '" + quarantine_id + "'"
    restore_sql = "SET search_path TO registration_service; UPDATE event_projections SET capacity = 1, local_revision = local_revision + 1, updated_at = now() WHERE event_id = '" + quarantine_id + "'; ALTER TABLE event_projections ADD CONSTRAINT event_projections_capacity_check CHECK (capacity > 0)"
    scenario.command([
        "docker", "compose", "-f", args.compose_file, "exec", "-T", "event-db-primary", "psql",
        "-U", "eventflow", "-d", "eventflow", "-v", "ON_ERROR_STOP=1", "-c", quarantine_sql,
    ])
    try:
        reconciliation_key = args.reconciliation_key
        report = scenario.expect(
            scenario.request_absolute(
                "POST",
                args.registration_url + "/internal/reconciliation/run",
                {"X-EventFlow-Reconciliation-Key": reconciliation_key, "X-Workspace-Id": "negative-workspace"},
                {},
            ),
            200,
            "quarantine reconciliation",
        )
        if report.get("quarantined", 0) < 1 or report.get("failed", 0) != 0:
            raise ScenarioFailure(f"invalid projection was not quarantined: {report}")
        quarantined_state = scenario.db_query("registration", f"SELECT capacity || ':' || confirmed_count FROM event_projections WHERE event_id = '{quarantine_id}'")
        if quarantined_state != "0:1":
            raise ScenarioFailure(f"quarantine changed invalid projection: {quarantined_state!r}")
    finally:
        scenario.command([
            "docker", "compose", "-f", args.compose_file, "exec", "-T", "event-db-primary", "psql",
            "-U", "eventflow", "-d", "eventflow", "-v", "ON_ERROR_STOP=1", "-c", restore_sql,
        ])
    print(json.dumps({
        "status": "PASS",
        "checks": [
            "authentication, role and tenant boundary errors",
            "validation and idempotency errors",
            "media type, size, missing object and metadata mismatch rejection",
            "internal reconciliation missing/wrong key rejection",
            "capacity-below-confirmed quarantine without applying corrupt state",
        ],
    }, indent=2))


def request_absolute(self, method: str, url: str, headers: dict[str, str], body=None):
    """Use Scenario's existing parser for a service URL outside the gateway."""
    encoded = None if body is None else json.dumps(body).encode("utf-8")
    request_headers = {"Accept": "application/json", **headers}
    if encoded is not None:
        request_headers["Content-Type"] = "application/json"
    try:
        with urlopen(Request(url, data=encoded, headers=request_headers, method=method), timeout=15) as response:
            from test_scenarios import HttpResult, decode_body, elapsed_ms
            return HttpResult(response.status, decode_body(response.read()), 0, dict(response.headers.items()))
    except Exception as error:
        from urllib.error import HTTPError
        if not isinstance(error, HTTPError):
            raise ScenarioFailure(f"{method} {url} transport failure: {error}") from error
        from test_scenarios import HttpResult, decode_body
        return HttpResult(error.code, decode_body(error.read()), 0, dict(error.headers.items()))


Scenario.request_absolute = request_absolute


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway-url", default="http://localhost:28181")
    parser.add_argument("--event-url", default="http://localhost:28081")
    parser.add_argument("--registration-url", default="http://localhost:28082")
    parser.add_argument("--compose-file", default="docker-compose.consistency.yml")
    parser.add_argument("--reconciliation-key", default="local-reconciliation-key")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except (ScenarioFailure, OSError) as error:
        print(f"NEGATIVE SCENARIO FAILED: {error}", file=sys.stderr)
        sys.exit(1)
