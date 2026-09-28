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

- event_db: one writer and exactly three PostgreSQL 16 physical read replicas.
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

Configure exactly three comma-separated endpoints:

~~~text
EVENT_DB_READ_REPLICA_URLS=jdbc:postgresql://r1:5432/eventflow_event,jdbc:postgresql://r2:5432/eventflow_event,jdbc:postgresql://r3:5432/eventflow_event
~~~

For every candidate the router checks:

~~~sql
current_setting('server_version_num')::int / 10000 = 16
pg_is_in_recovery() = true
pg_last_wal_replay_lsn() >= requested commit LSN
~~~

Healthy eligible replicas are selected round-robin. Wrong major version, promoted replica,
connection failure, invalid causal header, or insufficient replay LSN falls back to the writer.
Responses expose X-EventFlow-DB-Route and, for replica reads, X-EventFlow-Replay-LSN. Metric
eventflow.database.read.routes records target and bounded fallback reason; it does not label user
or raw LSN.

### Version + LSN causal contract

Successful Event writes query pg_current_wal_flush_lsn() only after the service transaction has
returned and committed. They return:

~~~text
X-EventFlow-Commit-LSN
X-EventFlow-Entity-Version
X-EventFlow-Write-Pin-Until
~~~

The client echoes them as:

~~~text
X-EventFlow-Min-LSN: <commit LSN>
X-EventFlow-Min-Version: <entity version>
X-EventFlow-Write-Pin-Until: <received value>
~~~

For the first **3 seconds** (Duration.ofSeconds(3)), the read is pinned to the writer. At exactly
3 seconds the pin has expired. The router may then use only a replica whose replay LSN has reached
the requested LSN. A single-resource read also checks the returned row version and falls back to
the writer if it is behind or the row has not replayed yet. Supplying only one member of the
LSN/version pair is treated conservatively and routes to the writer.

The server keeps a short in-memory session pin and also accepts the echoed receipt so the behavior
survives application load balancing. The pin header is clamped to at most 3 seconds from the
receiving server clock. It is a routing hint, not authorization; a forged value can at most force
a short writer read. Application nodes therefore need normal clock synchronization.

This is read-your-write routing, not a promise that asynchronous replication lag is always below
3 seconds.

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
- loss of all three replicas falls back to primary;
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
2. Add three read replicas and give the application each direct replica endpoint so it can check
   each replay LSN. Multi-AZ standby is not counted as a query replica.
3. Run Flyway only against the writer. Use private subnets/security groups, TLS
   sslmode=verify-full, separate read/write roles, and Secrets Manager-managed credentials,
   encryption, PITR, and alarms for replica lag/fallback rate.
4. Canary one endpoint, then enable all three. A promoted replica returns
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
