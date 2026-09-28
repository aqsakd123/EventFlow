# EventFlow — TODO

Trạng thái: VERIFIED LOCAL/K8S — engineering loop PASS; runtime cleanup hoàn tất; AWS production/HA topology vẫn deferred
Ngày bắt đầu: 2026-09-15

## P0 — Bootstrap và contract

- [x] Tạo `PLAN_DETAIL.md`.
- [x] Tạo `TODO.md`.
- [x] Tạo Maven multi-module với 3 deployable: gateway, event-service, registration-service.
- [x] Chốt database ownership và migration riêng cho từng service.
- [x] Chốt API/event envelope và error code.
- [x] Chốt local identity header và OIDC production config.

## P0 — Core business

- [x] Event CRUD, publish, cancel, lifecycle validation.
- [x] Capacity không giảm dưới confirmed count.
- [x] Registration projection từ event event qua RabbitMQ.
- [x] RSVP transaction với row lock/conditional update.
- [x] Idempotency key và duplicate registration protection.
- [x] Cancel, check-in và audit/idempotent behavior.
- [x] Negative tenant authorization tests.
- [x] Source-of-truth preflight cho RSVP/check-in để chặn stale Rabbit projection sau cancel/end; bounded consistency được ghi rõ.
- [x] Acceptance load test: 101 clients đồng thời, capacity 100, đúng 100 `201`, 1 `EVENT_FULL`, không oversell trên Docker và K8s.

## P0 — Engineering loop còn mở

- [x] Code review theo đúng PLAN_DETAIL và document gốc; xác nhận không thiếu requirement và không mở rộng scope.
- [x] RabbitMQ poison-message: retry hữu hạn, chuyển DLQ, kiểm tra payload/header và cleanup.
- [x] Kafka broker restart: restart broker local, xác nhận analytics không mất dữ liệu.
- [x] Kafka replay: chạy kafka-replay.ps1, xóa/rebuild analytics projection và xác nhận tổng ledger không đổi.
- [x] Test scenario tải 101 concurrent/capacity 100 trên stack cold Docker và K8s; ghi latency và DB result.
- [x] Đánh giá degrade: Rabbit/Kafka unavailable, retry/outbox behavior, API fail-safe/không oversell và giới hạn guarantee.
- [x] Sửa lệch evidence: request count, port K8s, Keycloak hostname và startup timing (chỉ giữ command đã chạy).
- [x] Review/fix test-harness race: baseline Kafka chỉ chụp sau khi outbox drain và analytics stable.
- [x] Cleanup sau vòng test: không còn container/volume/profile/target/cache EventFlow; workspace ở trạng thái code/docs-only.

## P1 — AWS emulator/media

- [x] LocalStack service và private S3 bucket init.
- [x] Presigned upload session.
- [x] Finalize kiểm tra object tồn tại, size/content type/key tenant.
- [x] READY/REJECTED state và cleanup note.

## P1 — Messaging

- [x] Event outbox trong cùng transaction.
- [x] Relay RabbitMQ với durable exchange/queue, confirm, retry và DLQ.
- [x] Registration inbox/deduplication và ACK sau commit.
- [x] Kafka domain-events producer.
- [x] Kafka analytics consumer và analytics ledger trên single-node Kafka; topic/internal offset topic đã kiểm tra live.
- [x] Kafka replay command (kafka-replay.ps1, local projection reset guarded by -ConfirmLocalReset).
- [x] Metric/health cho outbox, Rabbit queue depth và Kafka consumer lag; `-1` biểu thị unavailable.

## P1 — Delivery

- [x] Multi-stage Dockerfile cho từng service.
- [x] Docker Compose local stack: PostgreSQL, RabbitMQ, Kafka, LocalStack, Keycloak, app.
- [x] Kubernetes manifests: ConfigMap/Secret/Deployment/Service (Ingress để deferred cho AWS/EKS).
- [x] Probes, resource requests/limits, graceful shutdown.
- [x] K8s app replicas/PDB, datasource pool sizing cho 101 concurrent và startupProbe chống cold-start CrashLoop.
- [x] Minikube image build/load và rollout smoke test.
- [x] `docs/DOCKER_TO_K8S_RUNBOOK.md` ghi command/evidence thực tế.

## P2 — Verification/evidence

- [x] Maven clean test/verify: BUILD SUCCESS, tổng 5 unit tests.
- [x] Docker cold build/runtime: create → publish → RSVP → cancel → check-in.
- [x] 403/cross-tenant evidence, malformed request validation, participant draft filtering.
- [x] Rabbit retry/DLQ evidence.
- [x] Kafka restart/replay evidence.
- [x] LocalStack S3 evidence.
- [x] Minikube rollout/readiness evidence: 12 pods (6 stateful/dependency + 6 app replicas), all Ready sau startupProbe.
- [x] test_scenarios.py: Docker 133 requests (p50 401.4 ms, max 765.7 ms), K8s 132 requests (p50 4366.0 ms, max 6795.8 ms); API/DB/Rabbit/Kafka/concurrency/lifecycle pass.
- [x] test_jwt.py: Keycloak-issued JWT pass trên Docker và K8s; no-token 401, forged identity headers ignored.
- [x] test_p0.py và test_metrics.py: stale-source guard và metrics/lag pass trên Docker.
- [x] test_resilience.py/test_degrade.py: Rabbit DLQ, Kafka restart/replay và dependency degrade pass.
- [x] PostgreSQL backup/restore scripts pass trên Docker và K8s disposable DB.
- [x] Cleanup Docker/Minikube và kiểm tra resource prefix `eventflow`: đã xóa runtime, volumes, images, Minikube profile và build artifacts.

## Deferred — không chặn local/K8s learning loop

- [x] Keycloak realm + real JWT end-to-end trong Keycloak 25 container (dev fixture; OAuth browser authorization-code flow không thuộc scope).
- [ ] Waitlist.
- [x] PostgreSQL backup/restore utility + local Docker/K8s replay.
- [ ] AWS RPO/RTO measurement, PITR/restore drill và acceptance target.
- [ ] RabbitMQ quorum 3-node lab.
- [ ] Kafka RF3/minISR2 lab.
- [ ] PostgreSQL HA/sharding lab.
- [ ] AWS RDS/S3/EKS/IAM thật.
- [ ] AWS CI/CD, secret rotation, migration promotion/rollback và managed broker setup.
- [ ] AI read-only assistant/evals.

## Definition of evidence

Mỗi mục runtime phải ghi một trong: `PROPOSED`, `IMPLEMENTED`, `VERIFIED LOCAL`, `VERIFIED K8S`, `VERIFIED AWS`, `NOT RUN`. Kèm ngày, version, fixture, command, expected/actual và giới hạn guarantee.
