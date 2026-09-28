# EventFlow - code versus AWS/manual work

Updated: 2026-09-16

This file is the boundary for the requested learning exercise. Code that can
run locally is implemented and tested here. AWS account/IAM/CICD/CLI work is
intentionally manual and is not claimed as completed.

## Implemented in this workspace

| Concern | Code/config | How to verify |
|---|---|---|
| JWT validation | apps/api-gateway/src/main/java/com/eventflow/gateway/SecurityConfig.java | OIDC_ENABLED=true makes the Gateway validate issuer, signature, expiry and JWKS; verified with Keycloak 25 on Docker and K8s |
| JWT claim propagation | apps/api-gateway/src/main/java/com/eventflow/gateway/GatewayFilters.java | sub, workspace_id and realm_access.roles become verified internal headers |
| Local Keycloak realm | infra/keycloak/eventflow-realm.json and infra/kubernetes/all.yaml | Import into Keycloak 25; users organizer, participant, checkin are dev-only fixtures with complete profile fields |
| JWT black-box scenario | test_jwt.py | Gets a real Keycloak token, calls Gateway with Bearer only, checks forged headers are ignored |
| Stale projection guard | InternalEventStateController.java, EventStateClient.java | test_p0.py cancels with Rabbit stopped and expects RSVP/check-in rejection |
| Outbox health/age | metrics/OutboxMetrics.java, metrics/OutboxHealthIndicator.java | /actuator/metrics/eventflow.outbox.pending and /actuator/health |
| Rabbit queue depth | registration/metrics/RabbitQueueMetrics.java | /actuator/metrics/eventflow.rabbit.registration.queue.ready |
| Kafka consumer lag | registration/metrics/KafkaLagMetrics.java | /actuator/metrics/eventflow.kafka.consumer.lag; -1 means broker/group unavailable |
| Local PostgreSQL backup/restore | ops/backup/backup-postgres.ps1, restore-postgres.ps1 | Docker and K8s replay passed; dumps use `--clean --if-exists`, restore requires `-ConfirmRestore` and `ON_ERROR_STOP=1` |
| K8s stateless HA behavior | infra/kubernetes/all.yaml | App replicas=2, PDB minAvailable=1, startupProbe, graceful shutdown and `DB_POOL_SIZE=30`; stateful dependencies remain single-node lab components |

OAuth authorization-code/browser flow is outside this exercise. The JWT
scenario uses Keycloak's direct-grant endpoint only to obtain a lab token;
the Gateway still performs real JWT resource-server validation.

## Manual AWS work not performed

1. Create an AWS account boundary, budget alarm, IAM least-privilege roles and
   OIDC/IRSA trust. Never copy local development or test credentials.
2. Build each image with the multi-stage Dockerfiles, tag by immutable commit
   SHA, authenticate to ECR, push, and deploy to EKS.
3. Replace local PostgreSQL with two RDS databases or an approved schema
   boundary. Enable encryption, backups, PITR, retention and restore testing.
   Code touchpoints are the datasource URLs/credentials in each service's
   `application.yml`, Flyway migrations under `src/main/resources/db/migration`,
   and the local `ops/backup` scripts; validate schema ownership and run restore
   smoke checks after cutover.
4. Replace LocalStack with a private S3 bucket, Block Public Access, a task
   role limited to the tenant key prefix, lifecycle cleanup and CloudTrail.
   Code touchpoints are `S3StorageClient`, the event media upload-session and
   finalize methods, and `S3_ENDPOINT`/bucket/region configuration; no Java
   business flow change should be needed for an AWS SDK endpoint swap.
5. Replace single-node Rabbit/Kafka with Amazon MQ/MSK or approved managed
   equivalents. Configure quorum/replication, minimum ISR, TLS, auth and
   retention. Code touchpoints are the Rabbit listener/relay and DLQ
   declarations, Kafka producer/consumer properties, topic/group names, and
   `KafkaLagMetrics`; replay/lag acceptance must be rerun against the managed
   endpoints.
6. Run the CI/CD pipeline, migration promotion, rollback and secret rotation
   from the chosen AWS tooling. Build the existing multi-stage Dockerfiles,
   push immutable SHA tags to ECR, update EKS manifests/Secrets, and verify the
   Gateway, event-service and registration-service rollout. These are not
   simulated by Maven or Minikube.

## HA, backup/restore and RPO/RTO boundary

The application code is stateless at the Gateway and uses idempotent outbox
relays, readiness/liveness/startup probes and graceful shutdown. Kubernetes app
replicas/PDBs can protect the stateless tier, but a one-node Minikube lab with
emptyDir PostgreSQL, one Rabbit node and one Kafka broker cannot prove HA.

RPO/RTO are operational measurements, not Java methods:

- RPO: record the timestamp of the latest successful RDS snapshot/PITR point
  and compare it with the incident cut-off. The local backup script provides
  a repeatable dump timestamp, not an AWS RPO guarantee.
- RTO: measure from restore initiation until both services pass Flyway,
  /actuator/health/readiness, API smoke, outbox drain and data-count checks.
- Repeat the exercise for a single database, broker, application deployment
  and complete-region recovery. Record expected/actual RPO/RTO in the AWS
  incident/runbook repository.

AWS CloudWatch/Prometheus alerting must consume the implemented metrics:
outbox oldest age, pending/processing counts, Rabbit ready messages and Kafka
consumer lag. Alert thresholds are environment policy and are intentionally
not hardcoded as a production claim here.

Local verification status: Keycloak JWT, P0 source-state guard, metrics, Docker/K8s
backup/restore and the 101/100 acceptance load are PASS. AWS account/IAM/RDS/S3/EKS,
managed broker HA, CI/CD and production RPO/RTO remain manual and NOT RUN.
