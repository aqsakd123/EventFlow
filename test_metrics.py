"""Black-box check that custom outbox/Rabbit/Kafka metrics are exposed."""

from __future__ import annotations

import json
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def get(url: str):
    with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=10) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def wait_metric(url: str, name: str):
    last = None
    for _ in range(30):
        try:
            status, body = get(url)
            if status == 200:
                if body.get("name") != name or not body.get("measurements"):
                    raise AssertionError(f"metric response malformed for {name}: {body}")
                return body
            last = f"HTTP {status}: {body}"
        except (HTTPError, URLError) as error:
            last = str(error)
        time.sleep(1)
    raise AssertionError(f"metric {name} unavailable: {last}")


def run():
    metrics = {
        "eventflow.outbox.pending": "http://localhost:28081/actuator/metrics/eventflow.outbox.pending",
        "eventflow.outbox.oldest.age.seconds": "http://localhost:28081/actuator/metrics/eventflow.outbox.oldest.age.seconds",
        "eventflow.rabbit.registration.queue.ready": "http://localhost:28082/actuator/metrics/eventflow.rabbit.registration.queue.ready",
        "eventflow.kafka.consumer.lag": "http://localhost:28082/actuator/metrics/eventflow.kafka.consumer.lag",
    }
    for name, url in metrics.items():
        wait_metric(url, name)
    print(json.dumps({"status": "PASS", "metrics": sorted(metrics)}, indent=2))


if __name__ == "__main__":
    try:
        run()
    except (AssertionError, HTTPError, URLError, TimeoutError) as error:
        print(f"METRICS SCENARIO FAILED: {error}", file=sys.stderr)
        sys.exit(1)
