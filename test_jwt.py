"""Real Keycloak-issued JWT black-box check for the Gateway.

The test intentionally sends only Authorization: Bearer. The Gateway must
validate the signature/issuer and derive workspace/roles from verified claims.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def decode(raw: bytes):
    return json.loads(raw.decode("utf-8")) if raw else None


def request(url: str, method: str = "GET", headers: dict[str, str] | None = None,
            body: dict | None = None, form: dict[str, str] | None = None):
    data = None
    request_headers = {"Accept": "application/json", **(headers or {})}
    if form is not None:
        data = urlencode(form).encode("utf-8")
        request_headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    try:
        started = time.perf_counter()
        with urlopen(Request(url, data=data, headers=request_headers, method=method), timeout=15) as response:
            return response.status, decode(response.read()), (time.perf_counter() - started) * 1000
    except HTTPError as error:
        return error.code, decode(error.read()), 0


def token(keycloak_url: str, username: str, password: str, client_id: str) -> str:
    status, body, _ = request(
        f"{keycloak_url.rstrip('/')}/realms/eventflow/protocol/openid-connect/token",
        method="POST",
        form={"grant_type": "password", "client_id": client_id,
              "username": username, "password": password},
    )
    if status != 200 or not isinstance(body, dict) or not body.get("access_token"):
        raise AssertionError(f"token request failed: HTTP {status}: {body}")
    return body["access_token"]


def jwt_payload(access_token: str) -> dict:
    parts = access_token.split(".")
    if len(parts) != 3:
        raise AssertionError("Keycloak response is not a compact JWT")
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    return json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))


def expect(status: int, body, expected: int, description: str):
    if status != expected:
        raise AssertionError(f"{description}: expected HTTP {expected}, got {status}: {body}")
    return body


def run(args):
    organizer = token(args.keycloak_url, "organizer", args.organizer_password, args.client_id)
    participant = token(args.keycloak_url, "participant", args.participant_password, args.client_id)
    organizer_claims = jwt_payload(organizer)
    participant_claims = jwt_payload(participant)
    if organizer_claims.get("workspace_id") != "workspace-1":
        raise AssertionError(f"workspace_id mapper missing: {organizer_claims}")
    roles = organizer_claims.get("realm_access", {}).get("roles", [])
    if "ORGANIZER" not in roles:
        raise AssertionError(f"ORGANIZER role missing from JWT: {organizer_claims}")
    if "PARTICIPANT" not in participant_claims.get("realm_access", {}).get("roles", []):
        raise AssertionError(f"PARTICIPANT role missing from JWT: {participant_claims}")

    auth = {"Authorization": f"Bearer {organizer}"}
    no_token_status, _, _ = request(f"{args.gateway_url.rstrip('/')}/api/v1/events")
    expect(no_token_status, None, 401, "request without JWT")

    starts = datetime.now(timezone.utc) + timedelta(days=2)
    event_body = {
        "title": "jwt-scenario",
        "description": "Keycloak-issued JWT",
        "startsAt": starts.isoformat().replace("+00:00", "Z"),
        "endsAt": (starts + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "timezone": "Asia/Ho_Chi_Minh",
        "capacity": 1,
    }
    status, created, create_ms = request(
        f"{args.gateway_url.rstrip('/')}/api/v1/events", "POST",
        {**auth, "X-Workspace-Id": "forged-workspace", "X-Roles": "PARTICIPANT"}, event_body)
    created = expect(status, created, 201, "JWT organizer creates event")
    if created.get("workspaceId") != "workspace-1" or created.get("organizerId") != organizer_claims.get("sub"):
        raise AssertionError(f"Gateway trusted client headers instead of JWT claims: {created}")
    event_id = created["id"]
    try:
        status, published, _ = request(
            f"{args.gateway_url.rstrip('/')}/api/v1/events/{event_id}/publish", "POST", auth)
        expect(status, published, 200, "JWT organizer publishes event")
        participant_auth = {"Authorization": f"Bearer {participant}"}
        status, visible, participant_ms = request(
            f"{args.gateway_url.rstrip('/')}/api/v1/events/{event_id}", "GET", participant_auth)
        visible = expect(status, visible, 200, "JWT participant reads published event")
        if visible.get("workspaceId") != "workspace-1":
            raise AssertionError(f"participant workspace claim was not propagated: {visible}")
    finally:
        request(f"{args.gateway_url.rstrip('/')}/api/v1/events/{event_id}/cancel", "POST", auth)

    print(json.dumps({
        "status": "PASS",
        "issuer": organizer_claims.get("iss"),
        "workspaceClaim": organizer_claims.get("workspace_id"),
        "roles": roles,
        "noTokenStatus": no_token_status,
        "createLatencyMs": round(create_ms, 1),
        "participantReadLatencyMs": round(participant_ms, 1),
        "verifiedHeaders": "Authorization only; forged identity headers ignored",
    }, indent=2))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keycloak-url", default="http://localhost:8180")
    parser.add_argument("--gateway-url", default="http://localhost:28181")
    parser.add_argument("--client-id", default="eventflow-web")
    parser.add_argument("--organizer-password", default=os.environ.get("KEYCLOAK_ORGANIZER_PASSWORD"))
    parser.add_argument("--participant-password", default=os.environ.get("KEYCLOAK_PARTICIPANT_PASSWORD"))
    args = parser.parse_args()
    if not args.organizer_password or not args.participant_password:
        parser.error("set KEYCLOAK_ORGANIZER_PASSWORD and KEYCLOAK_PARTICIPANT_PASSWORD or pass both password options")
    return args


if __name__ == "__main__":
    try:
        run(parse_args())
    except (AssertionError, HTTPError, URLError, TimeoutError) as error:
        print(f"JWT SCENARIO FAILED: {error}", file=sys.stderr)
        sys.exit(1)
