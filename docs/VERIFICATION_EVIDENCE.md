# EventFlow - verification evidence

Date: 2026-09-16
Scope: real local Docker and Minikube runs, no application build cache.

## Independent replacement test session — 2026-09-21

Scope: fresh local `docker-compose.consistency.yml` stack; AWS was **NOT RUN**. No compile or
test fix was needed in this round. Added and ran
`test_consistency_second_round.py`; the only worktree changes from this round are that focused
check and this evidence entry.

1. Build and Compose startup
   - `mvn -B -gs maven-settings.xml clean verify`: PASS (`BUILD SUCCESS`). Four-module reactor:
     event-service 4 tests, registration-service 10 tests, API gateway 0 tests; 14 Java tests
     total, 0 failures/errors/skips.
   - `docker compose -f docker-compose.consistency.yml config`: PASS.
   - `docker compose -f docker-compose.consistency.yml up -d --build`: PASS. The three Event
     replicas, both PostgreSQL databases, Kafka, RabbitMQ and LocalStack became healthy; all
     three application containers started. Runtime service bindings were
     `127.0.0.1:28081->8081/tcp`, `127.0.0.1:28082->8082/tcp`, and gateway
     `0.0.0.0:28181->8080/tcp`.

2. Required black-box scenarios
   - `python test_consistency_reconciliation.py`: PASS. Event
     `a9741dca-f670-4153-9bac-ac50c236cde3`; post-pin causal read selected `replica-1` with
     replay LSN `0/5090D10`; report was
     `scanned=2,resolved=2,conflicts=2,quarantined=0,failed=0`.
   - The scenario runtime-verified missing-key auth rejection, version-only causal routing to
     primary, write pinning, caught-up replica routing, all-replica primary fallback,
     source-only projection repair, terminal convergence/non-regression, and conflict audit.
   - PowerShell equivalent of `COMPOSE_FILE=docker-compose.consistency.yml python test_p0.py`:
     PASS. It verified cancelled-source guards for stale-projection RSVP and check-in, and
     that the blocked RSVP created no registration row.

3. Focused second-round check
   - `python test_consistency_second_round.py`: PASS.
   - Runtime auth statuses were `[401, 401, 401, 401]` for missing/wrong keys on both internal
     services. Version-only read route was `primary`; the main event remained `CANCELLED`, its
     source/projection fields converged, the conflict audit contained `capacity` and `status`,
     and terminal regression returned HTTP 409.
   - Fairness: first scoped run
     `{"scanned":200,"resolved":200,"conflicts":0,"quarantined":0,"failed":0}`; second scoped
     run `{"scanned":201,"resolved":1,"conflicts":0,"quarantined":0,"failed":0}`; projection
     count reached exactly 201.
   - CAS/revision runtime check: projection-side mutation state was `event_version|local_revision
     = 0|1`; reconciliation advanced it to `1|2` with
     `{"scanned":200,"resolved":1,"conflicts":1,"quarantined":0,"failed":0}`; stale expected
     values matched no row (`UPDATE 0`).
   - Source inspection: `EventProjectionReconciliationWorker` updates require both
     `event_version = expected` and `local_revision = expected` and increment
     `local_revision`; `EventReconciliationService` applies source changes with a version CAS;
     `markAttempted` changes only scheduling metadata. These SQL/code claims are source-inspected,
     while the state transitions and no-match probe above are runtime-tested.

4. Scoped cleanup
   - `docker compose -f docker-compose.consistency.yml down -v --remove-orphans`: PASS. Removed
     only the consistency project's 10 containers, five declared volumes, and
     `eventflow-consistency_default`.
   - Verification commands
     `docker compose -f docker-compose.consistency.yml ps -a`,
     `docker volume ls --filter name=eventflow-consistency`, and
     `docker network ls --filter name=eventflow-consistency` returned no matching resources.
     No unrelated Docker resources were touched.

## Consistency reconciliation second-round verification — 2026-09-21

Scope: fresh local `docker-compose.consistency.yml` stack; all claims below are local runtime or source-inspection evidence, not AWS evidence.

1. Build and Compose startup
   - `mvn -B -gs maven-settings.xml clean verify`: PASS (`BUILD SUCCESS`). Four-module reactor; event-service ran 4 tests and registration-service ran 10 tests, with 0 failures/errors (14 Java tests total). API gateway had no tests.
   - `docker compose -f docker-compose.consistency.yml config`: PASS.
   - `docker compose -f docker-compose.consistency.yml up -d --build`: PASS. All declared dependencies became healthy and all three application containers started.

2. Runtime scenarios [runtime verified]
   - `python test_consistency_reconciliation.py`: PASS; event `895a54aa-e9f3-4ce5-a289-0310ce912755`; caught-up read selected `replica-3` with replay LSN `0/609C8D0`; report `scanned=4,resolved=2,conflicts=2,quarantined=0,failed=0`.
   - The scenario verified version-only causal reads route to primary, write-pin routing, replica catch-up, all-replica outage fallback, source-only projection creation, divergent source/projection reconciliation, terminal-state non-regression, conflict audit fields, and source/projection field convergence.
   - `COMPOSE_FILE=docker-compose.consistency.yml python test_p0.py`: PASS. It verified source cancellation blocks stale-projection RSVP and check-in, and the blocked RSVP creates no registration row.

3. Focused safety and consistency checks
   - Internal reconciliation authentication [runtime verified]: missing and wrong keys returned HTTP 401 for both event-service and registration-service internal endpoints (`401/401/401/401`).
   - Direct service exposure [runtime/config verified]: Docker reported `8081/tcp -> 127.0.0.1:28081` and `8082/tcp -> 127.0.0.1:28082`; the gateway remains the externally published API port.
   - `local_revision` [runtime verified and source inspected]: chaos mutation advanced `0→1`; reconciliation apply advanced `1→2`; RSVP/cancel advanced `0→1→2`; Rabbit projection delivery advanced `2→3`. Every production projection mutation SQL path inspected (`EventProjectionConsumer`, `RegistrationApplicationService`, `ReconciliationChaosService`, and `EventProjectionReconciliationWorker`) increments `local_revision`; initial inserts start at 0 via the migration default or explicit worker value. `markAttempted` changes only reconciliation bookkeeping and does not increment it.
   - Dual local CAS [source inspected, then exercised]: worker SQL updates require both `event_version = expected` and `local_revision = expected`, while the update increments `local_revision`. A temporary isolated trigger/live-delivery race caused one run to return `failed=1`; the conflict count remained persisted at 2, the fixture was removed, and the retry converged source/projection to `DRAFT|12|2` with `failed=0`.
   - Conflict ordering and durability [source inspected and runtime exercised]: `recordConflicts` precedes remote apply and local apply in the worker. The controlled CAS-loss run persisted the audit row even though the local apply failed, and the subsequent retry retained both conflict rows.
   - 201+ pagination/starvation [runtime verified]: after inserting exactly 201 isolated source-only rows with Rabbit stopped, the first scoped run reported `scanned=200,resolved=200,failed=0`; the second reported `scanned=201,resolved=1,failed=0`; registration DB count reached 201 projections. The 201st row was therefore processed after the first 200.
   - Lifecycle/update locking [source inspected]: event update/publish/cancel and reconciliation source apply load the event with `FOR UPDATE`; lifecycle expiry also locks candidate event rows. The Maven suite passed, but no separate high-contention stress benchmark was claimed.
   - Irreversible terminal convergence [runtime verified]: the main scenario ended with source and projection converged to the terminal source state (`CANCELLED`, registration closed), and a later terminal regression request returned HTTP 409.

4. Scoped cleanup
   - `docker compose -f docker-compose.consistency.yml down -v --remove-orphans`: PASS. Removed only the consistency stack containers, five declared project volumes, and `eventflow-consistency_default` network.
   - Verification: `docker compose -f docker-compose.consistency.yml ps -a`, `docker volume ls --filter name=eventflow-consistency`, and `docker network ls --filter name=eventflow-consistency` returned no matching resources. No unrelated Docker resources were touched.

## Consistency reconciliation verification — 2026-09-21

Scope: fresh local stack from `docker-compose.consistency.yml`; AWS resources were not used.

1. Build and static Compose validation
   - `mvn -B -gs maven-settings.xml clean verify`: PASS (`BUILD SUCCESS`). The reactor completed all four modules; event-service ran 3 tests and registration-service ran 7 tests, with 0 failures/errors.
   - `docker compose -f docker-compose.consistency.yml config`: PASS.
   - `docker compose -f docker-compose.consistency.yml up -d --build`: PASS. Primary, three read replicas, registration DB, Kafka, RabbitMQ and LocalStack were healthy; event-service, registration-service and gateway were up on ports 28081, 28082 and 28181.

2. Consistency and reconciliation scenario
   - Command: `python test_consistency_reconciliation.py`
   - Result: PASS; event `b3a6951b-6202-4317-b2dc-97b3c94c21b9`.
   - Causal read evidence: `X-EventFlow-DB-Route=replica-3`, `X-EventFlow-Replay-LSN=0/609B780` after the write pin expired.
   - Reconciliation report: `scanned=1`, `resolved=1`, `conflicts=2`, `quarantined=0`, `failed=0`.
   - Convergence: source and projection both ended at `status=CANCELLED`, `registrationOpen=false`, `capacity=12`, `confirmedCount=0`, `version=4`.
   - Conflict audit: `status` resolved as `TERMINAL_SOURCE_WINS` (`CANCELLED` over `ENDED`); `capacity` resolved as `FIELD_LWW` with winner `12` over `8`.
   - Replica evidence after the run: primary `pg_is_in_recovery=false`, flush LSN `0/60A2850`; replicas 1/2/3 each `pg_is_in_recovery=true`, replay LSN `0/60A2850`.

3. Related regression
   - Command: `COMPOSE_FILE=docker-compose.consistency.yml python test_p0.py`
   - Result: PASS. It verified source cancellation blocks stale-projection RSVP and check-in, and the blocked RSVP created no registration row.

4. Minimal fixes found during execution
   - Added explicit Spring constructor selection for the production constructors of both `HybridLogicalClock` beans, `EventReadRouter`, and `ConsistencyPinRegistry`; this fixed the observed `No default constructor found` startup failures.
   - Corrected the LocalStack healthcheck to test S3 `running`; the previous `available` probe left app containers in `Created` even though LocalStack was ready.
   - Increased only the scenario wait from 3.1s to 3.5s to avoid a measured sub-100ms race with the existing 3-second write pin.


5. Scoped cleanup
   - Command: `docker compose -f docker-compose.consistency.yml down -v --remove-orphans`
   - Result: PASS. The declared containers, five project volumes and `eventflow-consistency_default` network were removed.
   - Verification: `docker compose ... ps -a`, `docker volume ls --filter name=eventflow-consistency`, and `docker network ls --filter name=eventflow-consistency` returned no resources.

## Final loop

1. Maven
   - Command: mvn -B -gs maven-settings.xml clean verify
   - Result: BUILD SUCCESS
   - Tests: StateRulesTest 2; CapacityInvariantTest 1; EventStateClientTest 2; total 5.

2. Docker cold runtime
   - Build: docker compose build --pull --no-cache --progress=plain
   - Runtime: fresh disposable Compose volumes; `docker compose up -d --force-recreate
     --renew-anon-volumes` after the no-cache build.
   - Scenario: python test_scenarios.py --base-url http://localhost:28181 --runtime docker --rsvp-concurrency 101 --rsvp-capacity 100
   - Result: PASS; 133 requests; p50 401.4 ms; max 765.7 ms.
   - Checks: API status/body/error codes, latency, tenant isolation, draft visibility,
     idempotency, DB rows, Rabbit projection, Kafka analytics ledger, 101-way RSVP/capacity-100
     distribution (100 confirmed, 1 full), check-in, lifecycle END, and LocalStack S3 PUT/HEAD/finalize.

3. K8s cold runtime
   - Profile: fresh eventflow; Kubernetes v1.29.2; Docker driver; 4 CPU; 6144 MB.
   - Images: three final images built with docker build --pull --no-cache and loaded
     with minikube image load.
   - Manifest: infra/kubernetes/all.yaml applied with explicit eventflow context.
   - Kafka: __consumer_offsets and eventflow.domain-events exist; domain topic is
     one partition, RF 1, ISR 1.
   - Scenario: python test_scenarios.py --base-url http://localhost:29080 --runtime k8s
     --kube-context eventflow --rsvp-concurrency 101 --rsvp-capacity 100
   - Result: PASS; 132 requests; p50 4366.0 ms; max 6795.8 ms; 100 confirmed, 1 full.
   - Final pods: 12 pods including two replicas each for the three stateless apps; all 1/1
     Running after startup probes.
   - Init containers wait for event-db and registration-db before Flyway.

## Fixes found by real execution

- Cross-tenant RSVP could run before Rabbit projection; the scenario now waits for projection
  and the service returns TENANT_ACCESS_DENIED after projection.
- Published event capacity update now validates against registration projection before commit.
- Single-node Kafka needed RF=1 internal-topic settings; without them analytics group timed out.
- S3 presigned URL needed path-style addressing for LocalStack host replacement.
- Expired published events are moved to ENDED by the lifecycle scheduler and registration closes.
- Scenario retry handles app startup transport disconnects and checks timestamp idempotency
  at sub-millisecond database/JSON precision.
- K8s app deployments now wait for PostgreSQL with init containers.
- Keycloak fixture required complete user profile fields (`email`, `firstName`, `lastName`,
  `requiredActions=[]`) for direct-grant dev users; hostname is a valid URL for Keycloak 25.
- K8s two-replica load initially exposed Hikari exhaustion (5 HTTP 500s); `DB_POOL_SIZE=30`
  and startup probes fixed the actual failure/cold-start behavior, then 101/100 passed.
- Degrade baseline initially raced with a prior Kafka outbox catch-up; the harness now waits
  for KAFKA outbox drain and a stable analytics count before stopping Kafka.

## Rabbit/Kafka failure drills

- Command: python test_resilience.py
- Rabbit: PASS; malformed payload reached the DLQ with x-death count 1.
- Kafka: PASS; current rerun analytics snapshot was 118:118 before broker restart, after
  restart and after replay from the earliest topic offset.

## Degrade assessment

- Command: python test_degrade.py
- Rabbit outage: PASS; outbox was PENDING, RSVP failed safe before projection, then
  projection and RSVP recovered after Rabbit restart.
- Kafka outage: PASS; outbox was PENDING, RSVP stayed CONFIRMED, and analytics
  recovered from 126 to 128 after Kafka restart.

## Follow-up implementation evidence

- JWT/Keycloak Docker: PASS with Keycloak 25.0.6. `test_jwt.py` received signed tokens,
  checked `iss`, `workspace_id` and realm role claims, got 401 without a token, and proved
  forged identity headers were ignored. Gateway create/read latency was 285.5/20.5 ms.
- JWT/Keycloak K8s: PASS through Keycloak/Gateway port-forwards 28180/29080. Create/read
  latency was 478.2/24.4 ms; both Gateway replicas had `OIDC_ENABLED=true`.
- P0 source guard: PASS. `test_p0.py` stopped RabbitMQ, cancelled at event source, and
  verified RSVP/check-in were rejected before the async projection caught up; no registration
  row was written for the blocked RSVP.
- Metrics: PASS on Docker and K8s. `test_metrics.py` found outbox pending/oldest-age,
  Rabbit ready depth and Kafka consumer lag metrics.
- PostgreSQL backup/restore: PASS on Docker and K8s disposable databases. Plain SQL dumps
  used `--clean --if-exists`; restore used `psql -v ON_ERROR_STOP=1` and required
  `-ConfirmRestore`.
- Rabbit poison/DLQ and Kafka restart/replay: PASS. The Rabbit message reached DLQ with
  `x-death`; Kafka analytics snapshot remained 118:118 through restart and replay.

## Cleanup result

After the final engineering loop evidence was recorded and cleanup completed:
- Compose containers, project volumes and network: removed.
- EventFlow app image tags 0.1.0: removed.
- Namespace and Minikube profile eventflow: removed.
- EventFlow kube context/cluster/user entries were already absent after profile deletion.
- Root docker-compose.yml: removed as requested.
- test_scenarios.py, test_resilience.py, test_degrade.py and kafka-replay.ps1 remain source-only;
  temporary Python cache and Maven target directories were removed.
- secure-intake profile and unrelated localstack/secure-intake containers were not removed.

## Not claimed

Real AWS RDS/S3/EKS/IAM deployment, managed-broker HA/RF3/minISR2, AWS RPO/RTO and CI/CD
were not run. Local Keycloak JWT/OIDC and local Docker/K8s backup/restore were verified above.
