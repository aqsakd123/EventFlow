# EventFlow — architecture notes

## Boundary

There are three application deployables. Identity is deliberately not a fourth custom service.

| Component | Owns | Does not own |
|---|---|---|
| Gateway | route, request/correlation IDs, OIDC switch | business rule, DB, resource authorization |
| Event Service | event, media metadata, event outbox, audit | registration tables |
| Registration Service | local event projection, registration, capacity, attendance, inbox/outbox, analytics | event source-of-truth tables |
| Keycloak/OIDC | identity, tokens, roles | EventFlow business data |

## Why one shared database

The local Compose stack uses one PostgreSQL database with two schemas: `event_service` and `registration_service`. This keeps table ownership and Flyway history separate while fitting the free-tier topology of one writer and one physical read replica. Registration never joins Event Service tables.

## Messaging split

- RabbitMQ: `Event Service -> eventflow.events -> eventflow.registration.events`. It carries lifecycle delivery needed to maintain the local projection. The listener ACK happens after the DB method returns; retry is bounded and final rejection goes to DLQ.
- Kafka: outbox relay publishes versioned domain envelopes to `eventflow.domain-events`. The analytics consumer writes an event ledger before updating counters; duplicate messages do not increment the metric twice.
- PostgreSQL to broker is not one atomic transaction. Outbox relay is intentionally at-least-once; `messageId`/inbox/ledger provide idempotent internal effects.

## Core invariant

Registration Service locks the projection row and executes:

```sql
UPDATE event_projections
SET confirmed_count = confirmed_count + 1
WHERE event_id = ?
  AND event_status = 'PUBLISHED'
  AND registration_open = true
  AND confirmed_count < capacity;
```

Exactly one successful update can claim the last seat after the projection row lock. This must be demonstrated with the concurrent test/load step in the runbook, not inferred from a happy-path request.

## Security boundary

The Gateway removes client-supplied identity headers when a verified JWT is present and derives them from JWT claims. Business services still re-check workspace ownership and role. Direct service exposure is for local learning only; production should use private service networking, OIDC validation and IAM role-based AWS access.

The Keycloak 25 dev realm is imported from `infra/keycloak/eventflow-realm.json`. With
`OIDC_ENABLED=true`, Spring Security validates issuer, signature, expiry and JWKS; the
`workspace_id` mapper and `realm_access.roles` become trusted internal identity context. The
black-box check intentionally sends forged identity headers and accepts only the JWT claims.

Before RSVP/check-in, Registration Service reads the event source state through its private
internal endpoint and compares the source version with the local Rabbit projection. Cancelled or
ended source state is rejected even if the async projection is stale. A race after the preflight
read is still possible; this is a bounded stale-window reduction, not a distributed transaction.

Operational signals are exposed through Actuator: outbox pending/processing/oldest age, Rabbit
registration queue ready depth, and Kafka consumer lag. PostgreSQL dump/restore utilities live in
`ops/backup`; restore is guarded by an explicit confirmation switch.

## Known lab limits

The consistency lab uses one physical read replica for the shared database. Event reads may route
to the replica with causal fallback; Registration writes and capacity/check-in invariants stay on
the primary. The reconciliation worker converges the
existing events and event_projections copies with per-field HLC metadata; see
CONSISTENCY_RECONCILIATION.md. Lab chaos writes are internal, feature-flagged and never routed
through the Gateway.

- Minikube manifest uses two replicas/PDBs for the stateless app tier and startup probes for slow
  cold starts. Stateful dependencies remain one replica with `emptyDir`; it demonstrates
  scheduling, probes and service networking, not HA or durable storage.
- Kafka is one KRaft broker locally. It does not prove RF3/minISR2 behavior.
- LocalStack is an S3 emulator, not AWS IAM/S3 evidence.
