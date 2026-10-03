"""Degraded-dependency drill for the local EventFlow Docker stack."""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class DegradeFailure(AssertionError):
    pass


BASE_URL = "http://localhost:28181"
COMPOSE_FILE = os.environ.get("COMPOSE_FILE", "docker-compose.consistency.yml")
ORGANIZER = {"X-User-Id": "degrade-organizer", "X-Workspace-Id": "degrade-workspace", "X-Roles": "ORGANIZER"}
PARTICIPANT = {"X-User-Id": "degrade-participant", "X-Workspace-Id": "degrade-workspace", "X-Roles": "PARTICIPANT"}


def request(method: str, path: str, headers: dict[str, str], body: Any = None) -> tuple[int, Any]:
    encoded = None if body is None else json.dumps(body).encode("utf-8")
    request_headers = {"Accept": "application/json", **headers}
    if encoded is not None:
        request_headers["Content-Type"] = "application/json"
    try:
        with urlopen(Request(BASE_URL + path, data=encoded, headers=request_headers, method=method), timeout=15) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except HTTPError as error:
        raw = error.read()
        return error.code, json.loads(raw) if raw else None
    except URLError as error:
        raise DegradeFailure(f"{method} {path} transport failure: {error}") from error


def command(args: list[str], timeout: int = 30) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise DegradeFailure(f"command failed ({result.returncode}): {' '.join(args)}\n{result.stderr.strip()}")
    return result.stdout.strip()


def db_query(database: str, sql: str) -> str:
    schema = "event_service" if database == "event" else "registration_service"
    return command([
        "docker", "compose", "-f", COMPOSE_FILE, "exec", "-T", "event-db-primary", "psql",
        "-U", "eventflow", "-d", "eventflow", "-tAc", f"SET search_path TO {schema}; {sql}",
    ])


def analytics_snapshot() -> str:
    return db_query("registration", "SELECT count(*) FROM analytics_event_ledger")


def wait_for(operation: Any, description: str, timeout_s: float = 45.0) -> str:
    deadline = time.monotonic() + timeout_s
    last = ""
    while time.monotonic() < deadline:
        last = str(operation())
        if last:
            return last
        time.sleep(1)
    raise DegradeFailure(f"timed out waiting for {description}; last={last!r}")


def wait_for_unsent(event_id: str, channel: str) -> str:
    return wait_for(
        lambda: db_query("event", f"SELECT status FROM outbox_messages WHERE aggregate_id = '{event_id}' AND channel = '{channel}' ORDER BY created_at DESC LIMIT 1")
        if db_query("event", f"SELECT status FROM outbox_messages WHERE aggregate_id = '{event_id}' AND channel = '{channel}' ORDER BY created_at DESC LIMIT 1") in ("PENDING", "PROCESSING") else "",
        f"{channel} outbox unsent while dependency is down",
    )


def wait_for_outbox_idle(channel: str, timeout_s: float = 60.0) -> None:
    def idle() -> str:
        pending = db_query(
            "event",
            f"SELECT count(*) FROM outbox_messages WHERE channel = '{channel}' AND status IN ('PENDING', 'PROCESSING')",
        )
        return "idle" if pending == "0" else ""

    wait_for(idle, f"{channel} outbox to drain before failure injection", timeout_s)


def wait_for_analytics_stable(duration_s: float = 3.0) -> str:
    baseline = analytics_snapshot()
    deadline = time.monotonic() + duration_s
    while time.monotonic() < deadline:
        time.sleep(0.5)
        current = analytics_snapshot()
        if current != baseline:
            baseline = current
            deadline = time.monotonic() + duration_s
    return baseline


def expect(status: int, body: Any, wanted: int, message: str) -> dict[str, Any]:
    if status != wanted or not isinstance(body, dict):
        raise DegradeFailure(f"{message}: expected {wanted}, got {status}: {body}")
    return body


def expect_code(status: int, body: Any, wanted_status: int, wanted_code: str, message: str) -> None:
    if status != wanted_status or not isinstance(body, dict) or body.get("code") != wanted_code:
        raise DegradeFailure(f"{message}: expected {wanted_status}/{wanted_code}, got {status}: {body}")


def create_and_publish(title: str) -> str:
    starts = datetime.now(timezone.utc) + timedelta(days=2)
    status, body = request("POST", "/api/v1/events", ORGANIZER, {
        "title": title,
        "description": "dependency degradation drill",
        "startsAt": starts.isoformat().replace("+00:00", "Z"),
        "endsAt": (starts + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "timezone": "Asia/Ho_Chi_Minh",
        "capacity": 2,
    })
    event = expect(status, body, 201, "create degraded event")
    event_id = str(event["id"])
    published_status, published = request("POST", f"/api/v1/events/{event_id}/publish", ORGANIZER)
    published_body = expect(published_status, published, 200, "publish degraded event")
    if published_body.get("status") != "PUBLISHED":
        raise DegradeFailure(f"publish did not return PUBLISHED: {published_body}")
    return event_id


def rsvp(event_id: str) -> tuple[int, Any]:
    return request("POST", f"/api/v1/events/{event_id}/registrations",
                   {**PARTICIPANT, "Idempotency-Key": f"degrade-{uuid.uuid4().hex}"})


def rabbit_degrade() -> dict[str, Any]:
    command(["docker", "compose", "-f", COMPOSE_FILE, "stop", "rabbitmq"])
    try:
        event_id = create_and_publish("rabbit-degrade-" + uuid.uuid4().hex[:8])
        unsent = wait_for_unsent(event_id, "RABBIT")
        status, body = rsvp(event_id)
        expect_code(status, body, 404, "EVENT_PROJECTION_NOT_READY", "RSVP during Rabbit outage")
    finally:
        command(["docker", "compose", "-f", COMPOSE_FILE, "start", "rabbitmq"])
    projection = wait_for(
        lambda: db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'")
        if db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'") == "PUBLISHED" else "",
        "Rabbit projection recovery",
    )
    status, body = rsvp(event_id)
    confirmed = expect(status, body, 201, "RSVP after Rabbit recovery")
    return {
        "outboxStatusWhileDown": unsent,
        "projectionAfterRecovery": projection,
        "rsvpAfterRecovery": confirmed.get("status"),
        "guarantee": "fail-safe before projection; eventual recovery after Rabbit restart",
    }


def kafka_degrade() -> dict[str, Any]:
    wait_for_outbox_idle("KAFKA")
    wait_for_analytics_stable()
    baseline = int(db_query("registration", "SELECT count(*) FROM analytics_event_ledger"))
    command(["docker", "compose", "-f", COMPOSE_FILE, "stop", "kafka"])
    try:
        event_id = create_and_publish("kafka-degrade-" + uuid.uuid4().hex[:8])
        unsent = wait_for_unsent(event_id, "KAFKA")
        wait_for(
            lambda: db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'")
            if db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'") == "PUBLISHED" else "",
            "Rabbit projection while Kafka is down",
        )
        status, body = rsvp(event_id)
        confirmed = expect(status, body, 201, "critical RSVP while Kafka is down")
        analytics_while_down = int(db_query("registration", "SELECT count(*) FROM analytics_event_ledger"))
        if analytics_while_down != baseline:
            raise DegradeFailure(f"Kafka-down analytics changed unexpectedly: {baseline} -> {analytics_while_down}")
    finally:
        command(["docker", "compose", "-f", COMPOSE_FILE, "start", "kafka"])
    wait_for(
        lambda: command([
            "docker", "compose", "-f", COMPOSE_FILE, "exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092",
            "--describe", "--topic", "eventflow.domain-events",
        ]),
        "Kafka broker recovery",
        timeout_s=60,
    )
    sent = wait_for(
        lambda: db_query("event", f"SELECT count(*) FROM outbox_messages WHERE aggregate_id = '{event_id}' AND channel = 'KAFKA' AND status = 'SENT'")
        if int(db_query("event", f"SELECT count(*) FROM outbox_messages WHERE aggregate_id = '{event_id}' AND channel = 'KAFKA' AND status = 'SENT'")) >= 1 else "",
        "Kafka outbox recovery",
        timeout_s=60,
    )
    analytics_after = wait_for(
        lambda: db_query("registration", "SELECT count(*) FROM analytics_event_ledger")
        if int(db_query("registration", "SELECT count(*) FROM analytics_event_ledger")) > baseline else "",
        "Kafka analytics recovery",
        timeout_s=60,
    )
    return {
        "outboxStatusWhileDown": unsent,
        "rsvpWhileDown": confirmed.get("status"),
        "outboxSentAfterRecovery": int(sent),
        "analyticsBefore": baseline,
        "analyticsAfterRecovery": int(analytics_after),
        "guarantee": "Kafka is off the RSVP critical path; outbox catches up after broker recovery",
    }


def main() -> None:
    print(json.dumps({
        "status": "PASS",
        "rabbit": rabbit_degrade(),
        "kafka": kafka_degrade(),
    }, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (DegradeFailure, HTTPError, URLError, subprocess.SubprocessError) as error:
        print(f"DEGRADE DRILL FAILED: {error}")
        raise SystemExit(1)
