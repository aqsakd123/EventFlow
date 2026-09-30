"""Local failure drills for RabbitMQ DLQ and Kafka restart/replay.

Run only while the temporary EventFlow Docker Compose stack is up. The drills
touch only the EventFlow local Rabbit queue and registration analytics tables.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import shutil
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class DrillFailure(AssertionError):
    pass

COMPOSE_FILE = os.environ.get("COMPOSE_FILE", "docker-compose.consistency.yml")


def management_request(base_url: str, user: str, password: str, method: str,
                       path: str, payload: Any = None) -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": "Basic " + base64.b64encode(
            f"{user}:{password}".encode("utf-8")
        ).decode("ascii"),
    }
    if method != "DELETE":
        headers["Accept"] = "application/json"
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = Request(base_url.rstrip("/") + path, data=body, method=method,
                      headers=headers)
    try:
        with urlopen(request, timeout=15) as response:
            raw = response.read()
            return json.loads(raw) if raw else None
    except (HTTPError, URLError) as error:
        detail = error.read().decode("utf-8", "replace") if isinstance(error, HTTPError) else str(error)
        raise DrillFailure(f"Rabbit management {method} {path} failed: {detail}") from error


def run_command(command: list[str], timeout: int = 120) -> str:
    if command[:2] == ["docker", "compose"]:
        command = command[:2] + ["-f", COMPOSE_FILE] + command[2:]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if completed.returncode != 0:
        raise DrillFailure(f"command failed ({completed.returncode}): {' '.join(command)}\n{completed.stderr.strip()}")
    return completed.stdout.strip()


def rabbit_queue(base_url: str, user: str, password: str, queue: str) -> dict[str, Any]:
    return management_request(base_url, user, password, "GET", f"/api/queues/%2F/{queue}")


def wait_for_dlq(base_url: str, user: str, password: str, queue: str, timeout_s: float = 20.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = rabbit_queue(base_url, user, password, queue)
        if int(last.get("messages_ready", 0)) >= 1:
            return last
        time.sleep(0.5)
    raise DrillFailure(f"DLQ did not receive the poison message: {last}")


def rabbit_dlq_drill(args: argparse.Namespace) -> dict[str, Any]:
    queue = "eventflow.registration.events.dlq"
    management_request(args.rabbit_url, args.rabbit_user, args.rabbit_password, "DELETE",
                       f"/api/queues/%2F/{queue}/contents")
    published = management_request(
        args.rabbit_url, args.rabbit_user, args.rabbit_password, "POST",
        "/api/exchanges/%2F/eventflow.events/publish",
        {
            "properties": {"content_type": "application/json", "delivery_mode": 2},
            "routing_key": "EVENT_PUBLISHED",
            "payload": "{not-json",
            "payload_encoding": "string",
        },
    )
    if published.get("routed") is not True:
        raise DrillFailure(f"Rabbit poison message was not routed: {published}")
    dlq = wait_for_dlq(args.rabbit_url, args.rabbit_user, args.rabbit_password, queue)
    fetched = management_request(
        args.rabbit_url, args.rabbit_user, args.rabbit_password, "POST",
        f"/api/queues/%2F/{queue}/get",
        {"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"},
    )
    if not fetched or fetched[0].get("payload") != "{not-json":
        raise DrillFailure(f"DLQ payload mismatch: {fetched}")
    headers = fetched[0].get("properties", {}).get("headers", {})
    if not headers.get("x-death"):
        raise DrillFailure(f"DLQ message has no x-death retry evidence: {fetched[0]}")
    return {
        "published": True,
        "dlqMessagesReady": int(dlq.get("messages_ready", 0)),
        "xDeath": headers["x-death"],
    }


def analytics_snapshot() -> str:
    return run_command([
        "docker", "compose", "exec", "-T", "registration-db", "psql",
        "-U", "eventflow", "-d", "eventflow_registration", "-tAc",
        "SELECT (SELECT count(*) FROM analytics_event_ledger) || ':' || COALESCE((SELECT sum(metric_value) FROM analytics_projection), 0)",
    ])


def wait_for_snapshot(expected: str, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    last = ""
    while time.monotonic() < deadline:
        last = analytics_snapshot()
        if last == expected:
            return
        time.sleep(1)
    raise DrillFailure(f"analytics snapshot did not converge to {expected!r}; last={last!r}")


def kafka_restart_replay_drill(args: argparse.Namespace) -> dict[str, Any]:
    before = analytics_snapshot()
    if before == "0:0":
        raise DrillFailure("Kafka drill needs analytics events; run test_scenarios.py first")

    run_command(["docker", "compose", "restart", "kafka"])
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            run_command([
                "docker", "compose", "exec", "-T", "kafka",
                "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092",
                "--describe", "--topic", args.kafka_topic,
            ])
            break
        except DrillFailure:
            time.sleep(2)
    else:
        raise DrillFailure("Kafka topic did not become queryable after broker restart")
    after_restart = analytics_snapshot()
    if after_restart != before:
        raise DrillFailure(f"analytics changed/lost data after Kafka restart: before={before}, after={after_restart}")

    replay_shell = "powershell.exe" if shutil.which("powershell.exe") else "pwsh"
    replay = run_command([
        replay_shell, "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", "kafka-replay.ps1", "-ConfirmLocalReset",
    ])
    wait_for_snapshot(before)
    after_replay = analytics_snapshot()
    if after_replay != before:
        raise DrillFailure(f"Kafka replay changed projection totals: before={before}, after={after_replay}")
    return {
        "before": before,
        "afterRestart": after_restart,
        "afterReplay": after_replay,
        "replayCommand": replay,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rabbit-url", default="http://localhost:15672")
    parser.add_argument("--rabbit-user", default=os.environ.get("RABBITMQ_USERNAME", "eventflow"))
    parser.add_argument("--rabbit-password", default=os.environ.get("RABBITMQ_PASSWORD", "replace-me-rabbitmq"))
    parser.add_argument("--kafka-topic", default="eventflow.domain-events")
    args = parser.parse_args()
    if not args.rabbit_user or not args.rabbit_password:
        parser.error("set RABBITMQ_USERNAME and RABBITMQ_PASSWORD or pass both RabbitMQ options")
    return args


def main() -> None:
    args = parse_args()
    run_command(["docker", "compose", "start", "registration-service"])
    rabbit = rabbit_dlq_drill(args)
    kafka = kafka_restart_replay_drill(args)
    print(json.dumps({"status": "PASS", "rabbit": rabbit, "kafka": kafka}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (DrillFailure, HTTPError, URLError, subprocess.SubprocessError) as error:
        print(f"RESILIENCE DRILL FAILED: {error}")
        raise SystemExit(1)
