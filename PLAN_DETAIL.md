# EventFlow — PLAN_DETAIL

Ngày: 2026-09-15  
Trạng thái ban đầu: PROPOSED  
Trạng thái phiên 2026-09-16: IMPLEMENTED/VERIFIED LOCAL và VERIFIED K8S; load/failure/degrade,
JWT/Keycloak, metrics và backup/restore loop đã PASS; cleanup runtime cuối đã hoàn tất.
AWS production resources và HA topology vẫn NOT RUN theo boundary.

## 1. Mục tiêu phiên này

Xây một microservice backend nhỏ nhưng có chiều sâu để học và thực hành:

- API Gateway.
- Event Service.
- Registration Service.
- PostgreSQL transaction và migration.
- RabbitMQ cho workflow/event delivery.
- Kafka cho immutable stream và analytics projection.
- LocalStack làm AWS emulator cho S3.
- Docker multi-stage build.
- Kubernetes/Minikube deployment.
- Runbook manual từ Docker đến Kubernetes/AWS.

Không xây custom Auth Service. Identity infrastructure là Keycloak/OIDC; Gateway validate issuer,
signature, expiry, JWKS và role claims khi bật OIDC. Local profile dùng identity headers có ghi rõ
trong runbook; Keycloak 25 dev realm đã được chạy end-to-end bằng JWT thật.

## 2. Boundary cuối

```text
Client
  |
  v
API Gateway :8080
  |---------------------> Event Service :8081 ---- event_db
  |                       |
  |                       +--> Outbox -> RabbitMQ
  |                       +--> Kafka domain-events
  |
  +---------------------> Registration Service :8082 -- registration_db
                          ^
                          +------ RabbitMQ event projection
                          +------ Kafka analytics projection

Keycloak/OIDC: identity infrastructure (không sở hữu business data)
LocalStack: S3 emulator, bucket private trong flow local
```

Mỗi service sở hữu database/schema riêng. Registration Service không đọc bảng của Event Service.

## 3. Business contract

### Actor và tenant

- `OWNER`/`ORGANIZER`: tạo, sửa, publish, cancel event trong workspace được cấp quyền.
- `PARTICIPANT`: RSVP, cancel trước check-in, xem registration của mình.
- `CHECKIN_STAFF`: check-in event được phân công.
- `workspace_id` lấy từ identity header/JWT đã xác minh; không tin giá trị client gửi trong body.

### Event state

```text
DRAFT -> PUBLISHED -> ENDED
  \-----------------> CANCELLED
PUBLISHED ---------> CANCELLED
```

- Chỉ event `PUBLISHED`, chưa hết hạn đăng ký và chưa đầy mới nhận RSVP.
- Cancel event chặn ngay RSVP/check-in; notification/event delivery là async.
- Không giảm capacity dưới số confirmed hiện tại.
- Thời gian lưu UTC; giữ timezone gốc.

### Registration/check-in

```text
CONFIRMED <-> CANCELLED
CONFIRMED -> attendance CHECKED_IN
```

- Một participant chỉ có một registration hiện tại cho một event.
- RSVP/cancel dùng transaction và row lock để không oversell.
- Check-in chỉ cho registration `CONFIRMED`; quét lặp trả kết quả cũ.
- Participant không tự cancel sau check-in.
- Cùng idempotency key với payload khác trả conflict.

### File state

```text
PENDING_UPLOAD -> VALIDATING -> READY
                             \-> REJECTED
PENDING_UPLOAD --------------> EXPIRED
READY -> DELETING -> DELETED
```

Chỉ object đã xác minh mới là `READY` và được serve. Object key do server sinh theo tenant/event/file.

## 4. Flow bắt buộc

### Core synchronous flow

```text
Create event
  -> Publish event
  -> Event Service commit event + outbox trong cùng transaction
  -> RabbitMQ consumer cập nhật event projection
  -> RSVP (lock projection, increment confirmed_count có điều kiện)
  -> Cancel (lock registration/projection, decrement đúng một lần)
  -> Check-in (unique attendance, idempotent)
```

RSVP/check-in còn preflight event source qua `GET /internal/events/{id}/registration-state`.
Nếu RabbitMQ projection chưa bắt kịp source version, request fail-safe với
`EVENT_PROJECTION_NOT_READY`; điều này thu hẹp stale window nhưng không tuyên bố distributed
atomic transaction giữa hai service.

### Async flow

```text
Business transaction
  -> outbox row
  -> relay
  -> RabbitMQ workflow queue (retry/DLQ)
  -> Registration projection
  -> Kafka domain-events topic
  -> analytics projection/replay
```

Không tuyên bố transaction nguyên tử giữa PostgreSQL, RabbitMQ và Kafka. Consumer phải idempotent vì crash sau commit nhưng trước ACK/offset commit có thể tạo delivery lặp.

### Media flow

```text
upload-session -> presigned PUT -> finalize -> HEAD/metadata validation -> READY
```

LocalStack mô phỏng S3. AWS thật cần private bucket, IAM role/temporary credentials, Block Public Access và lifecycle cleanup.

## 5. Stack/version chốt

- Java 21.
- Maven 3.9+.
- Spring Boot 3.3.5.
- Spring Cloud 2023.0.3 (Gateway).
- PostgreSQL 16.
- RabbitMQ 3.13 management.
- Kafka 3.8 KRaft single-node local profile.
- LocalStack 3.8.
- Keycloak 25 (OIDC infrastructure, optional khi chạy local flow).
- Kubernetes API tương thích Minikube/kubectl 1.25+.

Image tag local phải cố định theo commit/release; không dùng `latest` trong lab cần tái hiện.

## 6. Tiêu chí nghiệm thu

### Code

- `mvn verify` chạy được từ root.
- Core API có validation và error code ổn định.
- Event lifecycle đúng state machine.
- 100+ RSVP concurrent vào capacity 100 không có hơn 100 confirmed.
- Unique registration, idempotent cancel/check-in.
- Tenant A không đọc/sửa dữ liệu tenant B.
- Migration, health/readiness/liveness, graceful shutdown.
- Outbox, inbox/deduplication, Rabbit retry/DLQ và Kafka projection/replay có code/config tối thiểu.
- Keycloak-issued JWT qua Gateway có thể kiểm tra bằng `test_jwt.py`; local direct grant chỉ là
  fixture để lấy token, OAuth authorization-code/browser flow không thuộc scope.
- Outbox age/pending, Rabbit ready depth và Kafka consumer lag có actuator metrics/health.
- Backup/restore PostgreSQL có utility cho Docker và K8s lab, với restore guard bắt buộc.

### Runtime evidence

- LocalStack bucket tạo được và object access private theo emulator.
- Docker images build bằng multi-stage Dockerfile.
- Docker smoke flow qua Gateway chạy được nếu daemon/network sẵn sàng.
- Minikube deploy/rollout/readiness/smoke chạy được nếu Docker daemon và profile cho phép.
- Trạng thái nào không chạy được phải ghi `NOT RUN` cùng lỗi thật, không ghi VERIFIED.

## 7. Ghi chú triển khai và cleanup

1. Start AWS emulator trước: LocalStack qua Docker, tạo S3 bucket.
2. Build/test Maven.
3. Build images và chạy Docker Compose smoke test.
4. Build/load images vào Minikube, apply manifests, kiểm tra rollout và flow.
5. Ghi command, version, expected/actual, lỗi inject và recovery vào `docs/DOCKER_TO_K8S_RUNBOOK.md`.
6. Sau khi ghi runbook: dừng port-forward, xóa runtime/container/image/volume lab do project tạo;
   `minikube delete --profile eventflow` và kiểm tra không còn resource project.

Các lệnh cleanup chỉ nhắm resource có tên prefix `eventflow`; không xóa container/image/profile ngoài phạm vi project.

## 8. Non-goals phiên này

- Không payment, waitlist, notification provider thật.
- Không custom Auth Service hoặc OAuth authorization-code UI; JWT resource-server validation và
  Keycloak dev realm là phần đã implement.
- Không Kafka/Rabbit/PostgreSQL HA production.
- Không AWS RPO/RTO guarantee; chỉ có local backup/restore utility và hướng dẫn đo AWS thủ công.
- Không EKS/RDS/Amazon MQ/MSK provision tự động.
- Không autonomous AI.
- Không tuyên bố production-ready hay exactly-once external side effect.

## 9. Definition of Done

Project có đúng ba application deployables; core flow và negative/concurrency cases có test; local runtime và Minikube được kiểm chứng ở mức có thể; runbook manual có đủ lệnh từ Docker đến K8s/AWS và cleanup; mọi giới hạn, phần chưa chạy và giả định được ghi rõ.
