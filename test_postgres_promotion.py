"""Disposable local PostgreSQL promotion drill for the Docker topology.

This test intentionally destroys the current Compose volumes during cleanup because
the promoted replica cannot be safely reattached to the old primary in this lab.
Run only with --confirm-local-reset.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class PromotionFailure(AssertionError):
    pass


def command(args: list[str], timeout: int = 180) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise PromotionFailure(f"command failed ({result.returncode}): {' '.join(args)}\n{result.stderr.strip()}")
    return result.stdout.strip()


def compose(compose_file: str, *args: str, timeout: int = 180) -> str:
    return command(["docker", "compose", "-f", compose_file, *args], timeout)


def sql(compose_file: str, service: str, database: str, statement: str) -> str:
    return compose(
        compose_file,
        "exec", "-T", service, "psql", "-U", "eventflow", "-d", database,
        "-tAc", statement,
        timeout=60,
    ).strip()


def wait_for(operation, label: str, timeout_s: float = 120) -> str:
    deadline = time.monotonic() + timeout_s
    last = ""
    while time.monotonic() < deadline:
        try:
            last = str(operation())
            if last:
                return last
        except (PromotionFailure, OSError, subprocess.SubprocessError):
            pass
        time.sleep(2)
    raise PromotionFailure(f"timed out waiting for {label}; last={last!r}")


def health(url: str) -> bool:
    try:
        with urlopen(url, timeout=5) as response:
            return response.status == 200
    except (HTTPError, URLError, TimeoutError):
        return False


def write_event() -> tuple[int, dict]:
    starts = datetime.now(timezone.utc) + timedelta(days=2)
    body = json.dumps({
        "title": "postgres-promotion-drill",
        "description": "writer after replica promotion",
        "startsAt": starts.isoformat().replace("+00:00", "Z"),
        "endsAt": (starts + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "timezone": "UTC",
        "capacity": 1,
    }).encode("utf-8")
    request = Request(
        "http://localhost:28181/api/v1/events",
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-User-Id": "promotion-organizer",
            "X-Workspace-Id": "promotion-workspace",
            "X-Roles": "ORGANIZER",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        return error.code, json.loads(error.read())


def run(args: argparse.Namespace) -> None:
    if not args.confirm_local_reset:
        raise PromotionFailure("refusing promotion drill without --confirm-local-reset")

    override_path = os.path.join(tempfile.gettempdir(), "eventflow-promotion-override.yml")
    with open(override_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            "services:\n"
            "  event-service:\n"
            "    environment:\n"
            "      SPRING_DATASOURCE_URL: jdbc:postgresql://event-db-replica-1:5432/eventflow?currentSchema=event_service\n"
            "      EVENT_DB_READ_REPLICA_URLS: ""\n"
        )

    promoted = False
    reset_needed = False
    try:
        streaming = sql(
            args.compose_file,
            "event-db-primary",
            "eventflow",
            "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'",
        )
        if int(streaming) != 1:
            raise PromotionFailure(f"expected one streaming replica before promotion, got {streaming!r}")

        compose(args.compose_file, "stop", "event-db-primary")
        reset_needed = True
        compose(args.compose_file, "exec", "-T", "-u", "postgres", "event-db-replica-1", "pg_ctl",
                "-D", "/var/lib/postgresql/data/pgdata", "promote", timeout=60)
        wait_for(
            lambda: sql(args.compose_file, "event-db-replica-1", "eventflow", "SELECT pg_is_in_recovery()")
            if sql(args.compose_file, "event-db-replica-1", "eventflow", "SELECT pg_is_in_recovery()") == "f" else "",
            "replica-1 promotion",
        )
        promoted = True

        compose(
            args.compose_file,
            "-f", override_path,
            "up", "-d", "--no-deps", "--force-recreate", "event-service",
        )
        wait_for(lambda: "healthy" if health("http://localhost:28081/actuator/health") else "", "event-service on promoted writer")
        status, body = write_event()
        if status != 201:
            raise PromotionFailure(f"application write did not survive promotion: HTTP {status}: {body}")
        persisted = sql(
            args.compose_file,
            "event-db-replica-1",
            "eventflow",
            "SET search_path TO event_service; SELECT count(*) FROM events WHERE id = '" + body['id'] + "'",
        )
        if persisted != "1":
            raise PromotionFailure(f"promoted writer did not persist application write: {persisted!r}")

        print(json.dumps({
            "status": "PASS",
            "streamingReplicasBeforePromotion": int(streaming),
            "promotedReplica": "event-db-replica-1",
            "applicationWriteAfterPromotion": True,
            "persistedOnPromotedWriter": True,
        }, indent=2))
    finally:
        if reset_needed:
            compose(args.compose_file, "down", "-v", "--remove-orphans", timeout=240)
            compose(args.compose_file, "up", "-d", "--build", timeout=600)
            wait_for(lambda: "healthy" if health("http://localhost:28181/actuator/health") else "", "stack restoration")
        try:
            os.remove(override_path)
        except OSError:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--compose-file", default="docker-compose.consistency.yml")
    parser.add_argument("--confirm-local-reset", action="store_true")
    try:
        run(parser.parse_args())
    except (PromotionFailure, OSError, subprocess.SubprocessError) as error:
        print(f"POSTGRES PROMOTION FAILED: {error}", file=sys.stderr)
        sys.exit(1)
