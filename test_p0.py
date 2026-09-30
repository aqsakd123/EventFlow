"""Black-box P0 checks for stale projection safety during event lifecycle changes."""

from __future__ import annotations

import json
import sys
import time
import subprocess

from test_scenarios import (
    Scenario,
    ScenarioFailure,
    create_event,
    headers,
    publish_and_wait_projection,
    register_when_ready,
)


def wait_projection(scenario: Scenario, event_id: str, state: str) -> None:
    scenario.wait_for(
        lambda: scenario.db_query(
            "registration",
            f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'",
        ) if scenario.db_query(
            "registration",
            f"SELECT event_status FROM event_projections WHERE event_id = '{event_id}'",
        ) == state else None,
        f"projection {state}",
    )


def compose(action: str, compose_file: str = "docker-compose.consistency.yml") -> None:
    completed = subprocess.run(
        ["docker", "compose", "-f", compose_file, action, "rabbitmq"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        raise ScenarioFailure(f"docker compose {action} rabbitmq failed: {completed.stderr.strip()}")


def run() -> None:
    scenario = Scenario("http://localhost:28181", "docker", 10000, "eventflow")
    organizer = headers("p0-organizer", "workspace-1", "ORGANIZER")
    participant = headers("p0-participant", "workspace-1", "PARTICIPANT")
    staff = headers("p0-staff", "workspace-1", "CHECKIN_STAFF")

    stale_rsvp = create_event(scenario, organizer, "p0-stale-rsvp", 1)
    stale_rsvp_id = stale_rsvp["id"]
    publish_and_wait_projection(scenario, stale_rsvp_id, organizer)
    wait_projection(scenario, stale_rsvp_id, "PUBLISHED")

    compose("stop")
    try:
        scenario.expect(
            scenario.request("POST", f"/api/v1/events/{stale_rsvp_id}/cancel", organizer),
            200,
            "cancel while Rabbit is down",
        )
        blocked = scenario.request(
            "POST",
            f"/api/v1/events/{stale_rsvp_id}/registrations",
            {**participant, "Idempotency-Key": "p0-stale-rsvp-key"},
        )
        scenario.expect_error(blocked, 409, "REGISTRATION_CLOSED", "stale projection RSVP guard")
        rows = scenario.db_query(
            "registration",
            f"SELECT count(*) FROM registrations WHERE event_id = '{stale_rsvp_id}'",
        )
        if rows != "0":
            raise ScenarioFailure(f"stale projection guard created a registration: {rows}")
    finally:
        compose("start")
    wait_projection(scenario, stale_rsvp_id, "CANCELLED")

    stale_checkin = create_event(scenario, organizer, "p0-stale-checkin", 1)
    stale_checkin_id = stale_checkin["id"]
    publish_and_wait_projection(scenario, stale_checkin_id, organizer)
    wait_projection(scenario, stale_checkin_id, "PUBLISHED")
    register_when_ready(scenario, stale_checkin_id, participant, "p0-stale-checkin-rsvp")

    compose("stop")
    try:
        scenario.expect(
            scenario.request("POST", f"/api/v1/events/{stale_checkin_id}/cancel", organizer),
            200,
            "cancel check-in event while Rabbit is down",
        )
        blocked = scenario.request(
            "POST",
            f"/api/v1/events/{stale_checkin_id}/check-ins",
            staff,
            {"participantId": "p0-participant"},
        )
        scenario.expect_error(blocked, 409, "EVENT_NOT_CHECKINABLE", "stale projection check-in guard")
    finally:
        compose("start")
    wait_projection(scenario, stale_checkin_id, "CANCELLED")

    print(json.dumps({
        "status": "PASS",
        "checks": [
            "cancelled source blocks RSVP before Rabbit projection catches up",
            "cancelled source blocks check-in before Rabbit projection catches up",
            "blocked RSVP creates no registration row",
        ],
    }, indent=2))


if __name__ == "__main__":
    try:
        run()
    except (ScenarioFailure, subprocess.SubprocessError) as error:
        print(f"P0 SCENARIO FAILED: {error}", file=sys.stderr)
        sys.exit(1)
