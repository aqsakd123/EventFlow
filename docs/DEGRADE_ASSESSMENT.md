# EventFlow — degrade assessment

Date: 2026-09-16  
Command: `python test_degrade.py` while the temporary Docker Compose stack was running.

## Results

| Dependency failure | Observed behavior | Guarantee demonstrated |
|---|---|---|
| RabbitMQ stopped | Event outbox stayed `PENDING`; RSVP returned `EVENT_PROJECTION_NOT_READY` before the projection existed. After restart, projection became `PUBLISHED` and RSVP became `CONFIRMED`. | Fail-safe against accepting an RSVP without the local event projection; eventual recovery through the outbox. |
| Kafka stopped | Kafka outbox stayed `PENDING`; Rabbit projection and critical RSVP still worked. After restart, the outbox was sent and analytics recovered from 126 to 128 events. | Kafka is off the RSVP critical path; analytics is eventually consistent. |

## Limits

- `PROCESSING` is an observed in-flight state, not a durability or exactly-once guarantee.
- Rabbit/Kafka outage can increase outbox age and consumer lag. Metrics now expose pending,
  processing, oldest age, queue depth and consumer lag; thresholds remain deployment policy.
- Registration Service outage, LocalStack outage and real AWS failure were not injected.
- JWT/OIDC validation was tested with real Keycloak-issued tokens; browser OAuth flow remains
  intentionally outside scope.
- Single-node Kafka is a lab topology with RF=1; it does not demonstrate broker HA.
- The preflight guard reduces the stale projection window but cannot make a cross-service
  source read and registration transaction distributed-atomic.
