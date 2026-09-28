# EventFlow - Docker -> Minikube -> AWS manual runbook

Verified: 2026-09-16
Scope: local learning lab, not production.

## 1. Stack and limitations

Deployables: api-gateway:8080, event-service:8081, registration-service:8082.
Event Service owns event PostgreSQL. Registration Service owns registration PostgreSQL.
Event outbox -> RabbitMQ registration projection. Event/registration outbox -> Kafka analytics.
LocalStack emulates S3. Keycloak 25 is included as identity infrastructure; OIDC is opt-in in the
local manifest, and real Keycloak-issued JWT validation is verified by `test_jwt.py`.

Local identity headers:
- X-User-Id
- X-Workspace-Id
- X-Roles: OWNER, ORGANIZER, PARTICIPANT, CHECKIN_STAFF

Kafka is one KRaft broker. A single broker must set:
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
Without these, __consumer_offsets cannot be created and the analytics group times out.

## 2. Prerequisites

~~~powershell
docker --version
docker compose version
java -version
mvn -version
minikube version --short
kubectl version --client
docker info
~~~

Versions used: Java 21, Spring Boot 3.3.5, PostgreSQL 16.4, RabbitMQ 3.13.7,
Kafka 3.8.1, LocalStack 3.8.1, Keycloak 25.0.6, Kubernetes v1.29.2.
This workspace uses Maven HTTP mirror only as a local truststore/proxy workaround.

## 3. LocalStack S3 emulator

~~~powershell
docker run -d --name eventflow-localstack -p 4567:4566 -e SERVICES=s3 -e AWS_DEFAULT_REGION=us-east-1 localstack/localstack:3.8.1
for ($i = 0; $i -lt 30; $i++) {
  try { Invoke-RestMethod http://localhost:4567/_localstack/health | Out-Null; break }
  catch { Start-Sleep -Seconds 2 }
}
docker exec eventflow-localstack awslocal s3 mb s3://eventflow-media
docker exec eventflow-localstack awslocal s3api put-public-access-block --bucket eventflow-media --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
docker exec eventflow-localstack awslocal s3api get-public-access-block --bucket eventflow-media
~~~

Expected: bucket exists and all four flags are true. Stop this standalone container before
starting the Compose LocalStack service:

~~~powershell
docker stop eventflow-localstack
docker rm eventflow-localstack
~~~

## 4. Maven and Docker

~~~powershell
mvn -B -gs maven-settings.xml clean verify
~~~

Expected: three-module reactor, BUILD SUCCESS, 5 unit tests pass (including source-state guard
client tests).

The root docker-compose.yml was temporary for this verification and is deleted during cleanup.
To replay Docker manually, recreate services named event-db, registration-db, rabbitmq, kafka,
localstack, keycloak, event-service, registration-service and api-gateway. Use ports
15432, 15433, 15674, 15673, 19092, 4567 and 28181. Use Kafka RF=1 settings from section 1.
Do not run the commands below until that manifest has been recreated.

~~~powershell
docker compose build --pull --no-cache --progress=plain
docker compose up -d --force-recreate --renew-anon-volumes
docker compose ps
Invoke-RestMethod http://localhost:28181/actuator/health
Invoke-RestMethod http://localhost:28181/actuator/health/readiness
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic eventflow.domain-events
python test_scenarios.py --base-url http://localhost:28181 --runtime docker --rsvp-concurrency 101 --rsvp-capacity 100
~~~

test_scenarios.py checks HTTP body/error code and latency, both databases, Rabbit projection,
Kafka analytics ledger, tenant isolation, idempotency, check-in, lifecycle, 101 concurrent
RSVP requests against capacity 100, and LocalStack S3 PUT/HEAD/finalize.

Run the local failure and degrade drills while this stack is up:

~~~powershell
python test_resilience.py
python test_degrade.py
~~~

The drills publish one malformed Rabbit message, restart Kafka, reset/rebuild the local
analytics projection, and stop/start Rabbit/Kafka. They are local destructive tests for
the EventFlow queues/tables only.

### Docker OIDC/JWT mode

The Compose manifest imports `infra/keycloak/eventflow-realm.json` and exposes Keycloak on
`http://localhost:8180`. Dev-only user passwords come from `KEYCLOAK_ORGANIZER_PASSWORD` and `KEYCLOAK_PARTICIPANT_PASSWORD` in `.env`
and `KEYCLOAK_CHECKIN_PASSWORD`. Enable the Gateway resource server and recreate only that service:

~~~powershell
$env:OIDC_ENABLED="true"
$env:OIDC_ISSUER_URI="http://localhost:8180/realms/eventflow"
$env:OIDC_JWK_SET_URI="http://keycloak:8080/realms/eventflow/protocol/openid-connect/certs"
docker compose up -d --force-recreate api-gateway
python test_jwt.py --keycloak-url http://localhost:8180 --gateway-url http://localhost:28181
~~~

Expected: token request 200, no-token request 401, role/workspace claims are propagated, and
forged `X-Workspace-Id`/`X-Roles` headers are ignored. OAuth browser authorization-code flow is
intentionally outside this learning scope.

### Metrics and PostgreSQL backup/restore

Verify the real Actuator metrics while the stack is up:

~~~powershell
python test_metrics.py --event-url http://localhost:28081 --registration-url http://localhost:28082
~~~

The check covers `eventflow.outbox.pending`, `eventflow.outbox.oldest.age.seconds`,
`eventflow.rabbit.registration.queue.ready` and `eventflow.kafka.consumer.lag`. A lag/depth value
of `-1` means the dependency was unavailable during refresh; health reports database query failure
separately.

Backups are plain SQL with `--clean --if-exists`; restore is destructive and requires an explicit
switch. Stop application writers before restore:

~~~powershell
$eventDump = Join-Path (Get-Location) ".tmp-event.sql"
$registrationDump = Join-Path (Get-Location) ".tmp-registration.sql"
pwsh -NoProfile -File .\ops\backup\backup-postgres.ps1 -Database event -Runtime docker -OutputPath $eventDump
pwsh -NoProfile -File .\ops\backup\backup-postgres.ps1 -Database registration -Runtime docker -OutputPath $registrationDump
docker compose stop api-gateway event-service registration-service
pwsh -NoProfile -File .\ops\backup\restore-postgres.ps1 -Database event -Runtime docker -InputPath $eventDump -ConfirmRestore
pwsh -NoProfile -File .\ops\backup\restore-postgres.ps1 -Database registration -Runtime docker -InputPath $registrationDump -ConfirmRestore
docker compose start event-service registration-service api-gateway
~~~

Delete temporary SQL files after inspection. This proves local replay, not an AWS RPO/RTO
guarantee.

## 5. Minikube/Kubernetes

~~~powershell
minikube start --profile eventflow --driver=docker --kubernetes-version=v1.29.2 --cpus=4 --memory=6144
kubectl --context eventflow --insecure-skip-tls-verify=true get nodes
docker build --pull --no-cache -t eventflow/api-gateway:0.1.0 -f apps/api-gateway/Dockerfile .
docker build --pull --no-cache -t eventflow/event-service:0.1.0 -f apps/event-service/Dockerfile .
docker build --pull --no-cache -t eventflow/registration-service:0.1.0 -f apps/registration-service/Dockerfile .
minikube image load --profile eventflow eventflow/api-gateway:0.1.0
minikube image load --profile eventflow eventflow/event-service:0.1.0
minikube image load --profile eventflow eventflow/registration-service:0.1.0
# Render the template with values from .env before applying; do not apply all.yaml directly.
# Example in Git Bash/WSL:
set -a; . ./.env; set +a
envsubst < infra/kubernetes/all.yaml | kubectl --context eventflow --insecure-skip-tls-verify=true apply -f -
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow wait --for=condition=available deployment --all --timeout=180s
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow get pods
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow exec deployment/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow exec deployment/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic eventflow.domain-events
~~~

The manifest uses imagePullPolicy Never for app images and init containers waiting for
PostgreSQL before Flyway. The stateless app tier has two replicas, PDB minAvailable=1,
`DB_POOL_SIZE=30`, and startup probes for slow cold starts. Expected final state: 12 pods
(six dependency/stateful pods plus six app replicas), all 1/1 Running after startup.

In another terminal:

~~~powershell
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow port-forward service/api-gateway 29080:8080
~~~

Then run:

~~~powershell
python test_scenarios.py --base-url http://localhost:29080 --runtime k8s --kube-context eventflow --skip-media --rsvp-concurrency 101 --rsvp-capacity 100
~~~

K8s skips media because the local manifest does not provide a host-reachable presigned URL.
Docker scenario covers S3 fully. If kubectl reports an x509 CA error on this lab host, use
the exact eventflow context plus insecure-skip-tls-verify shown above; never use this for
shared or production clusters.

### Minikube OIDC/JWT and metrics

The manifest defaults to header mode so the core learning scenario remains simple. Enable
OIDC explicitly and restart both Gateway replicas:

~~~powershell
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow patch configmap eventflow-config --type merge -p '{"data":{"OIDC_ENABLED":"true"}}'
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow rollout restart deployment/api-gateway
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow rollout status deployment/api-gateway --timeout=180s
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow port-forward service/keycloak 28180:8080
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow port-forward service/api-gateway 29080:8080
python test_jwt.py --keycloak-url http://localhost:28180 --gateway-url http://localhost:29080
~~~

For metrics, use two additional terminals:

~~~powershell
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow port-forward service/event-service 29081:8081
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow port-forward service/registration-service 29082:8082
python test_metrics.py --event-url http://localhost:29081 --registration-url http://localhost:29082
~~~

### Minikube backup/restore

The scripts use `kubectl exec` and require `-InsecureSkipTlsVerify` only for this local
certificate setup. Scale writers to zero before replaying a dump; restore requires
`-ConfirmRestore`:

~~~powershell
$eventDump = Join-Path (Get-Location) ".tmp-k8s-event.sql"
$registrationDump = Join-Path (Get-Location) ".tmp-k8s-registration.sql"
pwsh -NoProfile -File .\ops\backup\backup-postgres.ps1 -Database event -Runtime k8s -KubeContext eventflow -InsecureSkipTlsVerify -OutputPath $eventDump
pwsh -NoProfile -File .\ops\backup\backup-postgres.ps1 -Database registration -Runtime k8s -KubeContext eventflow -InsecureSkipTlsVerify -OutputPath $registrationDump
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow scale deployment/event-service deployment/registration-service deployment/api-gateway --replicas=0
pwsh -NoProfile -File .\ops\backup\restore-postgres.ps1 -Database event -Runtime k8s -KubeContext eventflow -InsecureSkipTlsVerify -InputPath $eventDump -ConfirmRestore
pwsh -NoProfile -File .\ops\backup\restore-postgres.ps1 -Database registration -Runtime k8s -KubeContext eventflow -InsecureSkipTlsVerify -InputPath $registrationDump -ConfirmRestore
kubectl --context eventflow --insecure-skip-tls-verify=true -n eventflow scale deployment/event-service deployment/registration-service deployment/api-gateway --replicas=2
~~~

These DBs use `emptyDir` in Minikube. This is a replay test, not durable backup or AWS RPO/RTO.

## 6. Cleanup to code-only

Run cleanup before deleting docker-compose.yml and stop every port-forward with Ctrl+C:

~~~powershell
docker compose down --remove-orphans
docker image rm eventflow/api-gateway:0.1.0 eventflow/event-service:0.1.0 eventflow/registration-service:0.1.0 -f 2>$null
kubectl --context eventflow --insecure-skip-tls-verify=true delete namespace eventflow --ignore-not-found
minikube delete --profile eventflow
kubectl config delete-context eventflow 2>$null
kubectl config delete-cluster eventflow 2>$null
kubectl config delete-user eventflow 2>$null
Remove-Item -LiteralPath .\docker-compose.yml -Force
~~~

Verify:

~~~powershell
docker ps -a --filter 'name=eventflow'
docker network ls --format '{{.Name}}' | Select-String 'eventflow'
docker volume ls --format '{{.Name}}' | Select-String 'eventflow'
docker image ls --format '{{.Repository}}:{{.Tag}}' | Select-String '^eventflow/'
minikube profile list
kubectl config get-contexts
Test-Path .\docker-compose.yml
~~~

If this was a disposable Compose project, remove only the exact volumes shown by
`docker volume ls` after confirming their Compose project prefix. Expected: no EventFlow
containers, network, volumes, app tags, namespace, profile or context;
Test-Path is False. Do not delete secure-intake, standalone localstack, or unrelated resources.

## 7. Evidence from the real no-cache loop

- Maven clean verify: BUILD SUCCESS; 5 tests passed.
- Docker: build --pull --no-cache, fresh volumes; 133 requests, p50 401.4 ms,
  max 765.7 ms; HTTP contracts, DB, Rabbit, Kafka, 101 concurrent RSVP/capacity 100,
  lifecycle and S3 passed. Distribution: 100 confirmed, 1 EVENT_FULL.
- Docker Keycloak JWT: PASS; issuer/workspace/role claims, no-token 401 and forged identity
  headers ignored; create 285.5 ms, participant read 20.5 ms.
- Docker P0 guard/metrics/backup-restore: PASS; source cancel blocked RSVP/check-in before
  Rabbit projection, four required metrics returned, and both DB dumps restored with ON_ERROR_STOP=1.
- Kafka: __consumer_offsets exists; domain topic partition 1, RF 1, ISR 1.
- Minikube: fresh eventflow profile, v1.29.2; 12 pods (app replicas 2/2) available after
  startup probes; 132 requests, p50 4366.0 ms, max 6795.8 ms; DB, Rabbit, Kafka and 101 concurrent
  RSVP/capacity 100 passed. Media is intentionally skipped in this local manifest.
- Minikube Keycloak JWT: PASS; issuer/workspace/role claims, no-token 401 and forged identity
  headers ignored; create 478.2 ms, participant read 24.4 ms. Metrics and K8s DB backup/restore PASS.
- Rabbit poison/retry/DLQ injection: PASS; malformed payload reached DLQ with x-death.
- Kafka broker restart/replay: PASS; analytics snapshot stayed 118:118 before/after restart/replay.
- Degrade drill: PASS; Rabbit outage was fail-safe before projection and recovered; Kafka
  outage kept RSVP available and outbox/analytics caught up after recovery. The harness was
  fixed to drain outbox and stabilize analytics before taking the Kafka baseline.
- AWS RDS/S3/EKS/IAM, managed broker HA, AWS CI/CD and AWS RPO/RTO: NOT RUN; see
  `docs/AWS_MANUAL_GAPS.md` for exact manual steps and code touchpoints.
