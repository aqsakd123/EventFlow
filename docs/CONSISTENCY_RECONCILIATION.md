# EventFlow — read consistency and reconciliation

Date: 2026-09-21
Status: implemented in code; AWS remains **NOT RUN**

## Why this shape

The repository already has two related records:

~~~text
event_db.events (source)
        |
        | transactional outbox -> RabbitMQ
        v
registration_db.event_projections (workflow copy + confirmed_count)
~~~

The implementation reuses this real Event → Registration flow. It does not add an artificial
order/shipment table and does not make registrations active-active. Making seat allocation
multi-writer would permit two partitioned zones to sell the same last seat; no later merge can
undo that external promise safely.

The topology is therefore:

- shared_db: one PostgreSQL writer and exactly one PostgreSQL 16 physical read replica; Event and Registration use separate schemas.
- registration_db: one invariant-critical writer.
- Reconciliation: application-level convergence between events and event_projections.
  A lab-only projection writer creates controlled divergence while the Rabbit link is down.
- Three deployable types remain unchanged. The worker lives in registration-service.

The LLM council had one material dissent: promote an isolated Event replica and reconcile two
Event timelines. That models disaster recovery more literally, but requires promotion, fencing
and destructive reseeding. The selected design is smaller, exercises the requested merge rules
on existing business data, and has a direct Docker scenario. A physical split-brain drill can be
added later without changing the merge metadata.

## Primary/read-replica routing

Only safe Event query endpoints use EventReadRouter:

- GET /api/v1/events
- GET /api/v1/events/{eventId}

Mutations, Flyway, outbox, schedulers and internal invariant reads keep the normal writer
JdbcTemplate. In particular, /internal/events/{id}/registration-state always reads the writer
because it is the fail-safe check before RSVP/check-in.

Configure one read-replica endpoint:

~~~text
EVENT_DB_READ_REPLICA_URLS=jdbc:postgresql://shared-db-replica:5432/eventflow?currentSchema=event_service
~~~

For every candidate the router checks:

~~~sql
current_setting('server_version_num')::int / 10000 = 16
pg_is_in_recovery() = true
pg_last_wal_replay_lsn() >= requested commit LSN
~~~

The healthy eligible replica is selected; when it is unavailable or behind, reads fall back to the primary. Wrong major version, promoted replica,
connection failure, invalid causal header, or insufficient replay LSN falls back to the writer.
Responses expose X-EventFlow-DB-Route and, for replica reads, X-EventFlow-Replay-LSN. Metric
eventflow.database.read.routes records target and bounded fallback reason; it does not label user
or raw LSN.

### Explicit read-routing policy

The policy is selected server-side with @ReadConsistency; client headers never enable a stronger
mode by themselves. Resolution is method annotation, then controller-class annotation, then the
default OFF.

~~~java
@GetMapping("/{eventId}")
@ReadConsistency(
    mode = ReadRoutingMode.VERSION_LSN,
    scope = "event",
    requireEntityVersion = true
)
~~~

The three modes are:

| Mode | Behavior |
|---|---|
| OFF | Ignore causal LSN/version/pin headers. Use a healthy replica and accept replica staleness. |
| VERSION_LSN | Require a valid X-EventFlow-Min-LSN; for single-entity reads this endpoint also requires entity version. A replica is eligible only after replaying at least that LSN and the returned row version is sufficient. Missing/invalid LSN, version or scope/entity receipt mismatch falls back to primary. |
| PRIMARY_PIN | Honor the scoped short-lived primary pin created by a write receipt. While the pin is active, route to primary; after expiry, normal replica eligibility applies. |

Current Event endpoints deliberately demonstrate two policies:

- list GET /api/v1/events: OFF;
- detail GET /api/v1/events/{eventId}: VERSION_LSN, scope event, entity version required.

A write receipt contains:

~~~text
X-EventFlow-Commit-LSN
X-EventFlow-Entity-Version
X-EventFlow-Consistency-Scope
X-EventFlow-Entity-Key
X-EventFlow-Write-Pin-Until
~~~

For VERSION_LSN, echo the first four values as:

~~~text
X-EventFlow-Min-LSN: <commit LSN>
X-EventFlow-Min-Version: <entity version>
X-EventFlow-Consistency-Scope: <same scope>
X-EventFlow-Entity-Key: <same entity key>
~~~

The service transaction commits before X-EventFlow-Commit-LSN is read. A single-resource read
also checks the returned row version and falls back to the writer if it is behind or the row has
not replayed yet. The default write pin is 3 seconds. The pin header is clamped to at most 3
seconds from the receiving server clock; it is a routing hint, not authorization. Application
nodes therefore need normal clock synchronization.

This is read-your-write routing, not a promise that asynchronous replication lag is always below
3 seconds. The local lab uses raw echo headers; production deployments should replace them with
a signed/scoped receipt or trusted gateway context before allowing cross-node clients to supply
causal thresholds.

## Reconciliation model

Migration V2 adds sync_meta JSON to both copies. Registration projections also receive a
local_revision changed by every message, RSVP/cancel, chaos write, and reconciliation apply;
the worker CASes this revision so it cannot overwrite a concurrent registration count.
last_reconciled_at drives fair scheduling. The JSON stores a Hybrid Logical Clock (HLC) per
shared field:

~~~json
{
  "status": "1789850000000:0:event-zone-a",
  "registrationOpen": "1789850000000:0:event-zone-a",
  "capacity": "1789850000010:0:event-zone-a",
  "confirmedCount": "1789850000020:0:registration-zone-b"
}
~~~

Event mutations stamp status, registrationOpen, and capacity in the same transaction as the
event/outbox write. RSVP/cancel stamps confirmedCount in the Registration transaction. Rabbit
messages carry the original Event clocks; the consumer does not generate a new timestamp on
delivery. A late message therefore cannot win merely because it arrived later.

EventProjectionReconciliationWorker performs:

1. **Detect** — fairly rotate local rows by last_reconciled_at and keyset-page Event source IDs.
   The union detects both divergence and source events whose projection was never created.
2. **Merge** — retain changes to independent fields. For a conflicting scalar field, select the
   greatest HLC (physicalMillis, logicalCounter, siteId) and record FIELD_LWW.
3. **Protect state** — DRAFT → PUBLISHED → terminal; ENDED and CANCELLED never return to a
   non-terminal state. A source terminal wins a competing terminal because it is the event
   authority. Status is deliberately not generic LWW.
4. **Protect capacity** — quarantine, rather than clamp or discard registrations, when merged
   capacity is below confirmedCount.
5. **Apply** — send a compare-and-set request to Event Service, then CAS both event_version and
   local_revision on the projection. A deterministic merge_id makes retries idempotent. There is
   no distributed transaction; a crash or competing Rabbit/RSVP write is repaired by the next run.
6. **Audit** — persist same-field conflicts idempotently before remote/local apply. A competing
   Rabbit delivery therefore cannot erase the observed conflict. Bounded counters cover resolved,
   conflict, quarantined, and failed work.

The Event apply increments events.version and writes EVENT_RECONCILED to Rabbit and Kafka
outboxes. The Rabbit consumer remains version-gated and idempotent.

### Internal lab endpoints

These endpoints are not routed by API Gateway:

~~~text
POST  registration-service/internal/reconciliation/run
GET   registration-service/internal/reconciliation/conflicts
PATCH registration-service/internal/reconciliation/events/{id}/chaos
GET   event-service/internal/reconciliation/events/{id}
GET   event-service/internal/reconciliation/events/ids
POST  event-service/internal/reconciliation/events/{id}/apply
~~~

All reconciliation endpoints are off by default. The chaos writer additionally requires
EVENT_RECONCILIATION_CHAOS_WRITES_ENABLED=true. It still refuses capacity below confirmed count
and terminal-state regression. Every endpoint requires X-EventFlow-Reconciliation-Key; requests
that read or mutate tenant data also require X-Workspace-Id and verify the scope. The local
Compose secret is lab-only, and direct service ports bind to 127.0.0.1. Production must source
the key from a secret manager, rotate it, and keep /internal/** on private service networking.

## Docker scenario

The new manifest is separate from the historical Compose file that was deliberately removed:

~~~powershell
docker compose -f docker-compose.consistency.yml up -d --build
python test_consistency_reconciliation.py
docker compose -f docker-compose.consistency.yml down -v --remove-orphans
~~~

docker-compose.consistency.yml uses official PostgreSQL 16.4. Replicas bootstrap with
pg_basebackup -R and remain hot standbys. The scenario proves:

- immediate read goes to primary during the 3-second pin;
- after the pin, an LSN-caught-up replica serves the read;
- loss of the read replica falls back to primary;
- an incomplete version-only causal hint uses the writer;
- unauthenticated internal reconciliation calls are rejected;
- stopping Rabbit creates a data-flow partition;
- source-side enumeration repairs an event whose projection was never delivered;
- Event and Projection perform independent and same-field writes;
- reconciliation performs merge + HLC LWW, preserves terminal state, audits conflicts and
  converges both databases;
- a post-convergence terminal regression is rejected.

The scenario is intentionally application-level. It does not claim that a PostgreSQL standby can
remain writable while still replaying the old primary timeline.

## AWS path — manual, not executed

For the read-scaled Event database:

1. Create an RDS for PostgreSQL 16 Multi-AZ writer.
2. Add one read replica and give the application its direct endpoint so it can check
   each replay LSN. Multi-AZ standby is not counted as a query replica.
3. Run Flyway only against the writer. Use private subnets/security groups, TLS
   sslmode=verify-full, separate read/write roles, and Secrets Manager-managed credentials,
   encryption, PITR, and alarms for replica lag/fallback rate.
4. Canary the single endpoint, then enable it. A promoted replica returns
   pg_is_in_recovery() = false and is automatically excluded from the reader pool.

RDS PostgreSQL read replicas use asynchronous native streaming replication, so the application
must retain writer fallback:
[AWS read-replica configuration](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.Replication.ReadReplicas.Configuration.html).

IAM database authentication is not implemented by the static Hikari password configuration.
If selected later, add an AWS JDBC/token-refresh integration; IAM auth tokens expire after
15 minutes:
[AWS IAM DB authentication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

If true multi-writer RDS becomes a product requirement, evaluate pgactive on supported RDS
PostgreSQL versions. AWS documents row conflicts, LWW/custom resolution, and recommends avoiding
conflicts by using one writer or independent data subsets:
[AWS pgactive](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.pgactive.html),
[active-active conflicts](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.pgactive.actact.replication.html).
Do not put seat allocation into that conflict domain.

Aurora Global Database write forwarding still commits changes on the primary cluster; it is not
two independent writers:
[Aurora global write forwarding](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-write-forwarding-apg.html).

AWS resources, failover, TLS/IAM and RPO/RTO evidence remain **NOT RUN**.
