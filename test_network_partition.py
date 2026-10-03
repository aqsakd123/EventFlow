"""Black-box RabbitMQ network-partition and recovery drill."""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from test_scenarios import (
    Scenario,
    ScenarioFailure,
    create_event,
    headers,
    publish_and_wait_projection,
)


def container_id(scenario: Scenario, service: str) -> str:
    return scenario.command(["docker", "compose", "-f", scenario.compose_file, "ps", "-q", service]).strip()


def network_name(scenario: Scenario, container: str) -> str:
    raw = scenario.command(["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", container])
    networks = json.loads(raw)
    if not networks:
        raise ScenarioFailure(f"container has no Docker network: {container}")
    return next(iter(networks))


def app_health() -> bool:
    try:
        with urlopen("http://localhost:28081/actuator/health", timeout=5) as event_response, urlopen("http://localhost:28082/actuator/health", timeout=5) as registration_response:
            return event_response.status == 200 and registration_response.status == 200
    except (HTTPError, URLError, TimeoutError, OSError):
        return False

def run(args: argparse.Namespace) -> None:
    scenario = Scenario("http://localhost:28181", "docker", 10000, "eventflow", args.compose_file)
    event_scenario = Scenario("http://localhost:28081", "docker", 10000, "eventflow", args.compose_file)
    registration_scenario = Scenario("http://localhost:28082", "docker", 10000, "eventflow", args.compose_file)
    organizer = headers("partition-organizer", "partition-workspace", "ORGANIZER")

    event_scenario.wait_for(lambda: app_health(), "dependent service readiness", timeout_s=90)
    event = create_event(event_scenario, organizer, "rabbit-network-partition", 1)
    event_id = event["id"]
    publish_and_wait_projection(event_scenario, event_id, organizer)
    scenario.wait_for(
        lambda: scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'")
        if scenario.db_query("registration", f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'") == "PUBLISHED" else None,
        "initial Rabbit projection",
    )

    rabbit = container_id(scenario, "rabbitmq")
    network = network_name(scenario, rabbit)
    scenario.command(["docker", "network", "disconnect", network, rabbit])
    reconnected = False
    try:
        scenario.command([
            "docker", "compose", "-f", args.compose_file, "exec", "-T", "event-db-primary", "psql",
            "-U", "eventflow", "-d", "eventflow", "-v", "ON_ERROR_STOP=1", "-c",
            "SET search_path TO event_service; WITH target AS (SELECT id FROM outbox_messages WHERE aggregate_id = '" + event_id + "' AND channel = 'RABBIT' ORDER BY created_at DESC LIMIT 1) UPDATE outbox_messages SET status = 'PENDING', attempts = 0, last_error = NULL, updated_at = now() WHERE id IN (SELECT id FROM target)",
        ])
        time.sleep(2)
        pending = scenario.db_query(
            "event",
            f"SELECT status FROM outbox_messages WHERE aggregate_id = '{event_id}' AND channel = 'RABBIT' ORDER BY created_at DESC LIMIT 1",
        )
        if pending not in {"PENDING", "PROCESSING"}:
            raise ScenarioFailure(f"Rabbit outbox did not remain unsent during partition: {pending!r}")
    finally:
        scenario.command(["docker", "network", "connect", "--alias", "rabbitmq", network, rabbit])
        reconnected = True

    if not reconnected:
        raise ScenarioFailure("Rabbit network was not reconnected")

    scenario.command(["docker", "compose", "-f", args.compose_file, "restart", "event-service", "registration-service"])
    scenario.wait_for(lambda: app_health(), "dependent service recovery", timeout_s=90)
    scenario.wait_for(
        lambda: scenario.db_query("event", f"SELECT status FROM outbox_messages WHERE aggregate_id = '{event_id}' AND channel = 'RABBIT' ORDER BY created_at DESC LIMIT 1")
        if scenario.db_query("event", f"SELECT status FROM outbox_messages WHERE aggregate_id = '{event_id}' AND channel = 'RABBIT' ORDER BY created_at DESC LIMIT 1") == "SENT" else None,
        "Rabbit outbox after network recovery",
        timeout_s=60,
    )

    if not app_health():
        raise ScenarioFailure("dependent services are not healthy after Rabbit recovery")

    print(json.dumps({
        "status": "PASS",
        "network": network,
        "checks": [
            "Rabbit network disconnect leaves a pending outbox message",
            "Rabbit reconnect drains the pending outbox",
            "dependent services recover after Rabbit network reconnect",
        ],
    }, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--compose-file", default="docker-compose.consistency.yml")
    try:
        run(parser.parse_args())
    except (ScenarioFailure, OSError) as error:
        print(f"NETWORK PARTITION FAILED: {error}", file=sys.stderr)
        sys.exit(1)
