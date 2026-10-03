"""Black-box EventFlow verification for a live Docker Compose or Kubernetes lab.

The script uses only the Python standard library. It checks HTTP status/body,
request latency, RabbitMQ-backed registration projection, Kafka-backed analytics,
and persisted rows in both service databases.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


@dataclass
class HttpResult:
    status: int
    body: Any
    elapsed_ms: float
    headers: dict[str, str]


class ScenarioFailure(AssertionError):
    pass


class Scenario:
    def __init__(self, base_url: str, runtime: str, latency_budget_ms: int, kube_context: str, compose_file: str = "docker-compose.consistency.yml"):
        self.base_url = base_url.rstrip("/")
        self.runtime = runtime
        self.latency_budget_ms = latency_budget_ms
        self.kube_context = kube_context
        self.compose_file = compose_file
        self.results: list[HttpResult] = []

    def request(self, method: str, path: str, headers: dict[str, str], body: Any = None) -> HttpResult:
        encoded = None
        request_headers = {"Accept": "application/json", **headers}
        if body is not None:
            encoded = json.dumps(body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, data=encoded, headers=request_headers, method=method)
        started = perf_counter()
        try:
            with urlopen(request, timeout=15) as response:
                raw = response.read()
                result = HttpResult(response.status, decode_body(raw), elapsed_ms(started), dict(response.headers.items()))
        except HTTPError as error:
            raw = error.read()
            result = HttpResult(error.code, decode_body(raw), elapsed_ms(started), dict(error.headers.items()))
        except (URLError, TimeoutError) as error:
            raise ScenarioFailure(f"{method} {path} transport failure: {error}") from error
        self.results.append(result)
        if result.elapsed_ms > self.latency_budget_ms:
            raise ScenarioFailure(f"{method} {path} latency {result.elapsed_ms:.1f}ms > {self.latency_budget_ms}ms")
        return result

    def expect(self, result: HttpResult, status: int, message: str) -> Any:
        if result.status != status:
            raise ScenarioFailure(f"{message}: expected HTTP {status}, got {result.status}: {result.body}")
        if not isinstance(result.body, (dict, list)):
            raise ScenarioFailure(f"{message}: expected JSON body, got {result.body!r}")
        return result.body

    def expect_error(self, result: HttpResult, status: int, code: str, message: str) -> None:
        body = self.expect(result, status, message)
        if not isinstance(body, dict) or body.get("code") != code:
            raise ScenarioFailure(f"{message}: expected error code {code}, got {body}")

    def wait_for(self, operation: Callable[[], Any], description: str, timeout_s: float = 30.0) -> Any:
        deadline = time.monotonic() + timeout_s
        last: Any = None
        while time.monotonic() < deadline:
            last = operation()
            if last:
                return last
            time.sleep(0.5)
        raise ScenarioFailure(f"Timed out waiting for {description}; last={last!r}")

    def db_query(self, database: str, sql: str) -> str:
        schema = "event_service" if database == "event" else "registration_service"
        scoped_sql = f"SET search_path TO {schema}; {sql}"
        if self.runtime == "docker":
            command = ["docker", "compose", "-f", self.compose_file, "exec", "-T", "event-db-primary",
                       "psql", "-U", "eventflow", "-d", "eventflow", "-tAc", scoped_sql]
        else:
            pod = self.command(["kubectl", "--context", self.kube_context, "--insecure-skip-tls-verify=true",
                                "-n", "eventflow", "get", "pods", "-l", "app=shared-db-primary",
                                "-o", "jsonpath={.items[0].metadata.name}"])
            command = ["kubectl", "--context", self.kube_context, "--insecure-skip-tls-verify=true",
                       "-n", "eventflow", "exec", pod, "--", "psql", "-U", "eventflow", "-d", "eventflow", "-tAc", scoped_sql]
        return self.command(command).strip()

    @staticmethod
    def command(command: list[str]) -> str:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=30)
        if completed.returncode != 0:
            raise ScenarioFailure(f"command failed ({completed.returncode}): {' '.join(command)}\n{completed.stderr.strip()}")
        return completed.stdout


def decode_body(raw: bytes) -> Any:
    if not raw:
        return None
    text = raw.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def elapsed_ms(started: float) -> float:
    return (perf_counter() - started) * 1000


def instant_value(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def headers(user: str, workspace: str, roles: str) -> dict[str, str]:
    return {"X-User-Id": user, "X-Workspace-Id": workspace, "X-Roles": roles}


def create_event(scenario: Scenario, organizer: dict[str, str], title: str, capacity: int) -> dict[str, Any]:
    starts = datetime.now(timezone.utc) + timedelta(days=2)
    payload = {
        "title": title,
        "description": "live black-box scenario",
        "startsAt": starts.isoformat().replace("+00:00", "Z"),
        "endsAt": (starts + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
        "timezone": "Asia/Ho_Chi_Minh",
        "capacity": capacity,
    }
    return scenario.expect(scenario.request("POST", "/api/v1/events", organizer, payload), 201, "create event")


def create_expired_event(scenario: Scenario, organizer: dict[str, str], title: str) -> dict[str, Any]:
    ends = datetime.now(timezone.utc) - timedelta(hours=1)
    payload = {
        "title": title,
        "description": "expired live black-box scenario",
        "startsAt": (ends - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "endsAt": ends.isoformat().replace("+00:00", "Z"),
        "timezone": "Asia/Ho_Chi_Minh",
        "capacity": 1,
    }
    return scenario.expect(scenario.request("POST", "/api/v1/events", organizer, payload), 201, "create expired event")


def publish_and_wait_projection(scenario: Scenario, event_id: str, organizer: dict[str, str]) -> dict[str, Any]:
    published = scenario.expect(scenario.request("POST", f"/api/v1/events/{event_id}/publish", organizer), 200, "publish event")
    if published.get("status") != "PUBLISHED" or published.get("registrationOpen") is not True:
        raise ScenarioFailure(f"publish output is not active: {published}")
    return published


def register_when_ready(scenario: Scenario, event_id: str, participant: dict[str, str], key: str) -> dict[str, Any]:
    result: HttpResult | None = None
    for _ in range(60):
        result = scenario.request("POST", f"/api/v1/events/{event_id}/registrations",
                                  {**participant, "Idempotency-Key": key})
        if result.status == 201:
            return scenario.expect(result, 201, "register participant")
        if result.status == 404 and isinstance(result.body, dict) and result.body.get("code") == "EVENT_PROJECTION_NOT_READY":
            time.sleep(0.5)
            continue
        raise ScenarioFailure(f"registration failed before projection became ready: {result.status} {result.body}")
    raise ScenarioFailure(f"registration projection did not become ready: {result.body if result else None}")


def assert_db_state(scenario: Scenario, event_id: str, first_key: str, second_event_id: str,
                    concurrent_event_id: str, concurrent_capacity: int) -> None:
    event_row = scenario.db_query("event", f"SELECT status || ':' || capacity || ':' || confirmed_count FROM events WHERE id = '{event_id}'")
    if event_row != "PUBLISHED:1:0":
        raise ScenarioFailure(f"event DB state mismatch: {event_row!r}")

    projection = scenario.db_query("registration", f"SELECT event_status || ':' || confirmed_count FROM event_projections WHERE event_id = '{event_id}'")
    if projection != "PUBLISHED:1":
        raise ScenarioFailure(f"registration projection mismatch: {projection!r}")

    registrations = scenario.db_query("registration", f"SELECT count(*) FROM registrations WHERE event_id = '{event_id}' AND status = 'CONFIRMED'")
    if registrations != "1":
        raise ScenarioFailure(f"confirmed registration count mismatch: {registrations!r}")

    attendance = scenario.db_query("registration", f"SELECT count(*) FROM attendance WHERE event_id = '{event_id}'")
    if attendance != "1":
        raise ScenarioFailure(f"attendance count mismatch: {attendance!r}")

    idem = scenario.db_query("registration", f"SELECT state FROM idempotency_keys WHERE participant_id = 'participant-1' AND idempotency_key = '{first_key}'")
    if idem != "SUCCEEDED":
        raise ScenarioFailure(f"idempotency state mismatch: {idem!r}")

    outbox = scenario.db_query("event", f"SELECT count(*) FROM outbox_messages WHERE aggregate_id = '{event_id}' AND status = 'SENT'")
    if int(outbox) < 4:
        raise ScenarioFailure(f"event outbox was not fully relayed: {outbox!r}")

    analytics = scenario.wait_for(
        lambda: scenario.db_query("registration", f"SELECT count(*) FROM analytics_event_ledger WHERE workspace_id = 'workspace-1' AND message_id IS NOT NULL")
        if int(scenario.db_query("registration", "SELECT count(*) FROM analytics_event_ledger WHERE workspace_id = 'workspace-1'")) >= 4 else None,
        "Kafka analytics ledger",
    )
    if int(analytics) < 4:
        raise ScenarioFailure(f"analytics ledger count mismatch: {analytics!r}")

    second_projection = scenario.db_query("registration", f"SELECT event_status || ':' || confirmed_count FROM event_projections WHERE event_id = '{second_event_id}'")
    if second_projection != "PUBLISHED:1":
        raise ScenarioFailure(f"second event projection mismatch: {second_projection!r}")

    concurrent_projection = scenario.db_query("registration", f"SELECT event_status || ':' || confirmed_count FROM event_projections WHERE event_id = '{concurrent_event_id}'")
    if concurrent_projection != f"PUBLISHED:{concurrent_capacity}":
        raise ScenarioFailure(f"concurrent RSVP projection mismatch: {concurrent_projection!r}")
    concurrent_rows = scenario.db_query("registration", f"SELECT count(*) FROM registrations WHERE event_id = '{concurrent_event_id}' AND status = 'CONFIRMED'")
    if concurrent_rows != str(concurrent_capacity):
        raise ScenarioFailure(f"concurrent RSVP row count mismatch: {concurrent_rows!r}")


def run_media_scenario(scenario: Scenario, event_id: str, organizer: dict[str, str]) -> None:
    upload = scenario.expect(scenario.request("POST", f"/api/v1/events/{event_id}/media/upload-session", organizer,
                                                {"fileName": "scenario.png", "contentType": "image/png", "size": 4}),
                             201, "create upload session")
    if not upload.get("uploadUrl") or not upload.get("objectKey"):
        raise ScenarioFailure(f"upload session missing signed URL/key: {upload}")
    signed_url = upload["uploadUrl"]
    parsed = urlsplit(signed_url)
    if parsed.hostname == "localstack":
        signed_url = urlunsplit((parsed.scheme, "localhost:4566", parsed.path, parsed.query, parsed.fragment))
    request = Request(signed_url, data=b"demo", headers={"Content-Type": "image/png"}, method="PUT")
    started = perf_counter()
    try:
        with urlopen(request, timeout=15) as response:
            upload_elapsed = elapsed_ms(started)
            if response.status not in (200, 204):
                raise ScenarioFailure(f"presigned PUT returned HTTP {response.status}")
    except URLError as error:
        raise ScenarioFailure(f"presigned PUT failed for {signed_url}: {error}") from error
    if upload_elapsed > scenario.latency_budget_ms:
        raise ScenarioFailure(f"presigned PUT latency {upload_elapsed:.1f}ms > {scenario.latency_budget_ms}ms")
    final = scenario.expect(scenario.request("POST", f"/api/v1/events/{event_id}/media/{upload['mediaId']}/finalize", organizer),
                            200, "finalize media")
    if final.get("state") != "READY" or final.get("actualSize") != 4 or final.get("contentType") != "image/png":
        raise ScenarioFailure(f"media finalization mismatch: {final}")


def run(args: argparse.Namespace) -> None:
    scenario = Scenario(args.base_url, args.runtime, args.latency_budget_ms, args.kube_context, args.compose_file)
    organizer = headers("organizer-1", "workspace-1", "ORGANIZER")
    participant = headers("participant-1", "workspace-1", "PARTICIPANT")
    second_participant = headers("participant-2", "workspace-1", "PARTICIPANT")
    staff = headers("staff-1", "workspace-1", "CHECKIN_STAFF")
    other_tenant = headers("user-b", "workspace-2", "ORGANIZER")
    suffix = uuid.uuid4().hex[:10]
    first_key = f"scenario-{suffix}-first"

    health_result: HttpResult | None = None
    last_health_error: str | None = None
    for _ in range(60):
        try:
            health_result = scenario.request("GET", "/actuator/health", {})
            if health_result.status == 200:
                break
            last_health_error = f"HTTP {health_result.status}: {health_result.body}"
        except ScenarioFailure as error:
            last_health_error = str(error)
        time.sleep(1)
    if health_result is None or health_result.status != 200:
        raise ScenarioFailure(f"gateway health did not become ready: {last_health_error}")
    health = scenario.expect(health_result, 200, "gateway health")
    if health.get("status") != "UP":
        raise ScenarioFailure(f"gateway health is not UP: {health}")

    invalid = scenario.request("POST", "/api/v1/events", organizer, {
        "title": "invalid-capacity",
        "startsAt": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat().replace("+00:00", "Z"),
        "endsAt": (datetime.now(timezone.utc) + timedelta(days=2, hours=1)).isoformat().replace("+00:00", "Z"),
        "timezone": "Asia/Ho_Chi_Minh",
        "capacity": 0,
    })
    scenario.expect_error(invalid, 400, "VALIDATION_ERROR", "invalid request validation")

    first = create_event(scenario, organizer, f"live-scenario-{suffix}-one", 1)
    event_id = first["id"]
    if first.get("status") != "DRAFT" or first.get("confirmedCount") != 0:
        raise ScenarioFailure(f"create output mismatch: {first}")
    scenario.expect(scenario.request("GET", f"/api/v1/events/{event_id}", organizer), 200, "get event")
    scenario.expect(scenario.request("GET", "/api/v1/events", organizer), 200, "list events")
    draft_list = scenario.expect(scenario.request("GET", "/api/v1/events", participant), 200, "participant list hides draft")
    if any(item.get("id") == event_id for item in draft_list):
        raise ScenarioFailure(f"participant list exposed draft event {event_id}: {draft_list}")
    scenario.expect_error(scenario.request("GET", f"/api/v1/events/{event_id}", other_tenant), 403,
                          "TENANT_ACCESS_DENIED", "cross-tenant event access")
    published = publish_and_wait_projection(scenario, event_id, organizer)
    scenario.wait_for(
        lambda: scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'")
        if scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'") == "PUBLISHED" else None,
        "first event projection",
    )

    scenario.expect_error(scenario.request("POST", f"/api/v1/events/{event_id}/registrations",
                                           {**other_tenant, "Idempotency-Key": f"scenario-{suffix}-tenant"}), 403,
                          "TENANT_ACCESS_DENIED", "cross-tenant RSVP")
    confirmed = register_when_ready(scenario, event_id, participant, first_key)
    if confirmed.get("status") != "CONFIRMED" or confirmed.get("participantId") != "participant-1":
        raise ScenarioFailure(f"registration output mismatch: {confirmed}")
    duplicate = scenario.expect(scenario.request("POST", f"/api/v1/events/{event_id}/registrations",
                                                  {**participant, "Idempotency-Key": first_key}), 201, "idempotent registration")
    if duplicate.get("registrationId") != confirmed.get("registrationId"):
        raise ScenarioFailure(f"idempotency returned another registration: {confirmed} vs {duplicate}")
    mine = scenario.expect(scenario.request("GET", f"/api/v1/events/{event_id}/registrations/me", participant), 200, "get mine")
    if mine.get("registrationId") != confirmed.get("registrationId") or mine.get("checkedIn") is not False:
        raise ScenarioFailure(f"get mine mismatch: {mine}")

    updated = scenario.expect(scenario.request("PATCH", f"/api/v1/events/{event_id}", organizer, {
        "title": first["title"] + "-updated",
        "description": first["description"],
        "startsAt": first["startsAt"],
        "endsAt": first["endsAt"],
        "timezone": first["timezone"],
        "capacity": first["capacity"],
        "version": published["version"],
    }), 200, "update published event")
    if updated.get("version") != published.get("version", 0) + 1:
        raise ScenarioFailure(f"event version did not advance: {published} -> {updated}")
    scenario.wait_for(
        lambda: scenario.db_query("registration", f"SELECT event_version || ':' || confirmed_count FROM event_projections WHERE event_id = '{event_id}'")
        if scenario.db_query("registration", f"SELECT event_version FROM event_projections WHERE event_id = '{event_id}'") == str(updated["version"]) else None,
        "updated event projection preserving confirmed count",
    )
    full = scenario.request("POST", f"/api/v1/events/{event_id}/registrations",
                            {**second_participant, "Idempotency-Key": f"scenario-{suffix}-full"})
    scenario.expect_error(full, 409, "EVENT_FULL", "capacity enforcement")

    checked = scenario.expect(scenario.request("POST", f"/api/v1/events/{event_id}/check-ins", staff,
                                                {"participantId": "participant-1"}), 200, "check-in")
    if checked.get("status") != "CHECKED_IN":
        raise ScenarioFailure(f"check-in output mismatch: {checked}")
    repeated_checkin = scenario.expect(scenario.request("POST", f"/api/v1/events/{event_id}/check-ins", staff,
                                                        {"participantId": "participant-1"}), 200, "idempotent check-in")
    checked_delta = abs(instant_value(repeated_checkin["checkedInAt"]) - instant_value(checked["checkedInAt"]))
    if checked_delta > timedelta(milliseconds=1):
        raise ScenarioFailure(f"repeated check-in changed timestamp: {checked} vs {repeated_checkin}")
    attendance = scenario.expect(scenario.request("GET", f"/api/v1/events/{event_id}/attendance", staff), 200, "attendance")
    if len(attendance) != 1 or attendance[0].get("status") != "CHECKED_IN":
        raise ScenarioFailure(f"attendance output mismatch: {attendance}")
    cancel_after_checkin = scenario.request("DELETE", f"/api/v1/events/{event_id}/registrations/me", participant)
    scenario.expect_error(cancel_after_checkin, 409, "ALREADY_CHECKED_IN", "cancel after check-in")

    second = create_event(scenario, organizer, f"live-scenario-{suffix}-two", 2)
    second_event_id = second["id"]
    publish_and_wait_projection(scenario, second_event_id, organizer)
    second_registration = register_when_ready(scenario, second_event_id, second_participant, f"scenario-{suffix}-second")
    if second_registration.get("status") != "CONFIRMED":
        raise ScenarioFailure(f"second registration mismatch: {second_registration}")
    cancelled = scenario.expect(scenario.request("DELETE", f"/api/v1/events/{second_event_id}/registrations/me", second_participant),
                                200, "cancel registration")
    if cancelled.get("status") != "CANCELLED":
        raise ScenarioFailure(f"cancel output mismatch: {cancelled}")
    reactivated = register_when_ready(scenario, second_event_id, second_participant, f"scenario-{suffix}-reactivated")
    if reactivated.get("status") != "CONFIRMED" or reactivated.get("registrationId") != second_registration.get("registrationId"):
        raise ScenarioFailure(f"re-registration did not reuse the registration row: {second_registration} -> {reactivated}")
    reused = scenario.request("POST", f"/api/v1/events/{second_event_id}/registrations",
                              {**participant, "Idempotency-Key": first_key})
    scenario.expect_error(reused, 409, "IDEMPOTENCY_KEY_REUSED", "idempotency key reuse across events")

    concurrent_capacity = args.rsvp_capacity
    concurrent_clients = args.rsvp_concurrency
    if concurrent_clients <= concurrent_capacity:
        raise ScenarioFailure("--rsvp-concurrency must be greater than --rsvp-capacity")
    concurrent = create_event(scenario, organizer, f"live-scenario-{suffix}-concurrent", concurrent_capacity)
    concurrent_event_id = concurrent["id"]
    publish_and_wait_projection(scenario, concurrent_event_id, organizer)
    scenario.wait_for(
        lambda: scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{concurrent_event_id}'")
        if scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{concurrent_event_id}'") == "PUBLISHED" else None,
        "concurrent event projection",
    )

    def concurrent_rsvp(index: int) -> HttpResult:
        user = headers(f"load-participant-{index}", "workspace-1", "PARTICIPANT")
        return scenario.request("POST", f"/api/v1/events/{concurrent_event_id}/registrations",
                                {**user, "Idempotency-Key": f"scenario-{suffix}-load-{index}"})

    with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
        concurrent_results = list(executor.map(concurrent_rsvp, range(concurrent_clients)))
    successful = [result for result in concurrent_results if result.status == 201]
    full_results = [result for result in concurrent_results if result.status == 409]
    if len(successful) != concurrent_capacity or len(full_results) != concurrent_clients - concurrent_capacity:
        raise ScenarioFailure(f"concurrent RSVP status distribution mismatch: {[result.status for result in concurrent_results]}")
    for result in full_results:
        scenario.expect_error(result, 409, "EVENT_FULL", "concurrent capacity rejection")

    expired = create_expired_event(scenario, organizer, f"live-scenario-{suffix}-expired")
    expired_event_id = expired["id"]
    publish_and_wait_projection(scenario, expired_event_id, organizer)
    scenario.wait_for(
        lambda: scenario.db_query("event", f"SELECT status FROM events WHERE id = '{expired_event_id}'")
        if scenario.db_query("event", f"SELECT status FROM events WHERE id = '{expired_event_id}'") == "ENDED" else None,
        "expired event lifecycle",
    )
    scenario.wait_for(
        lambda: scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{expired_event_id}'")
        if scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{expired_event_id}'") == "ENDED" else None,
        "ended event projection",
    )
    scenario.expect_error(scenario.request("POST", f"/api/v1/events/{expired_event_id}/registrations",
                                           {**participant, "Idempotency-Key": f"scenario-{suffix}-expired"}), 409,
                          "REGISTRATION_CLOSED", "expired event registration")

    if args.runtime == "docker" and not args.skip_media:
        run_media_scenario(scenario, event_id, organizer)

    assert_db_state(scenario, event_id, first_key, second_event_id, concurrent_event_id, concurrent_capacity)
    latencies = sorted(result.elapsed_ms for result in scenario.results)
    p50 = latencies[len(latencies) // 2]
    print(json.dumps({
        "status": "PASS",
        "runtime": args.runtime,
        "requests": len(scenario.results),
        "latencyMs": {"p50": round(p50, 1), "max": round(max(latencies), 1), "budget": args.latency_budget_ms},
        "eventId": event_id,
        "secondEventId": second_event_id,
        "mediaChecked": args.runtime == "docker" and not args.skip_media,
        "dbChecked": True,
        "rabbitProjectionChecked": True,
        "kafkaAnalyticsChecked": True,
        "concurrentRsvp": {"clients": concurrent_clients, "capacity": concurrent_capacity,
                           "confirmed": len(successful), "full": len(full_results)},
    }, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:28181")
    parser.add_argument("--runtime", choices=("docker", "k8s"), default="docker")
    parser.add_argument("--kube-context", default="eventflow")
    parser.add_argument("--compose-file", default="docker-compose.consistency.yml")
    parser.add_argument("--latency-budget-ms", type=int, default=10000)
    parser.add_argument("--rsvp-concurrency", type=int, default=101)
    parser.add_argument("--rsvp-capacity", type=int, default=100)
    parser.add_argument("--skip-media", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except (ScenarioFailure, HTTPError, URLError, subprocess.SubprocessError) as error:
        print(f"SCENARIO FAILED: {error}", file=sys.stderr)
        sys.exit(1)
