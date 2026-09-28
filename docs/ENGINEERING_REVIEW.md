# EventFlow — engineering review

Date: 2026-09-16  
Scope: review against PLAN_DETAIL and the pasted EventFlow/backend documents. No new
service, product feature, HA topology or AWS resource was added.

## Review decision

**PASS WITH EXPLICIT DEFERRED ITEMS.** The local learning scope has the required three
application deployables, database ownership, core state/invariants, RabbitMQ workflow,
Kafka analytics, S3 emulator flow, Docker multi-stage build and Kubernetes manifest.
The review does not claim real AWS or production HA.

Repository note: this workspace is not a Git working tree (`git rev-parse --show-toplevel`
does not resolve), so commit history was unavailable and no repository was initialized as
part of this scoped implementation. This review is therefore artifact- and test-evidence-based.

## Requirement coverage

| Area | Review result | Evidence |
|---|---|---|
| Gateway + two business services | PASS | root POM and three app modules |
| Event/registration state and tenant boundary | PASS | service rules and black-box negative cases |
| PostgreSQL ownership/migration/locking/idempotency | PASS | separate migrations and live DB assertions |
| RabbitMQ outbox/projection/retry/DLQ | PASS local | live poison-message drill and x-death evidence |
| Kafka producer/consumer/ledger/replay | PASS local | live restart/replay drill; snapshot unchanged |
| Load invariant | PASS local | 101 concurrent RSVP, capacity 100: 100 confirmed, 1 full |
| Docker/Kubernetes delivery | PASS local | no-cache Docker and fresh Minikube runs |
| Degraded behavior | PASS local | Rabbit/Kafka stop-start drill |
| Real AWS RDS/S3/EKS/IAM | NOT RUN | manual AWS phase, no credentials/resources in this scope |
| Keycloak-issued JWT/OIDC | PASS local/K8s | `test_jwt.py`; real Keycloak 25 token, issuer/role/workspace validation and forged-header rejection |
| P0 stale projection protection | PASS local | `test_p0.py`; source-state preflight blocks RSVP/check-in before Rabbit catches up |
| Backup/restore and lag metrics | PASS local/K8s | Docker/K8s PostgreSQL scripts; Actuator outbox/Rabbit/Kafka metrics |
| HA topology, AWS RPO/RTO, waitlist, AI | DEFERRED | production AWS/managed HA and product features remain outside scope |

## Findings fixed during this loop

- Made concurrent load configurable and raised the default acceptance check to 101/100.
- Added kafka-replay.ps1 with an explicit confirmation guard before clearing only local
  analytics tables and resetting the local consumer group.
- Added test_resilience.py for Rabbit poison-to-DLQ and Kafka restart/replay.
- Added test_degrade.py for fail-safe projection lag and Kafka outbox recovery.
- Corrected the replay snapshot query so multiple analytics metrics do not multiply the
  ledger count.
- Corrected Docker replay output handling and Rabbit Management request headers.
- Reconciled image tags, K8s port-forward port, and final request/latency evidence.
- Implemented Keycloak realm fixtures and JWT resource-server validation; added real JWT black-box
  checks for Docker and K8s.
- Added source-of-truth event-state preflight, outbox/queue/lag metrics, and guarded PostgreSQL
  backup/restore utilities for Docker and K8s labs.
- Fixed K8s two-replica Hikari exhaustion at 101 concurrent requests with `DB_POOL_SIZE=30`;
  added startup probes after real cold-start CrashLoop evidence.
- Fixed the degrade harness baseline race by waiting for Kafka outbox drain and stable analytics.
- Fixed K8s backup utility fail-fast behavior when kubectl cannot resolve a DB pod.

## Deliberate non-expansion

No custom Auth Service, frontend, waitlist, quorum broker cluster, EKS/RDS provisioner,
service mesh, or production exactly-once claim was introduced. These remain explicit
TODO/deferred work rather than hidden assumptions.
