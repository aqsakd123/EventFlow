# EventFlow

EventFlow là backend microservice để học và thực hành event lifecycle đa tenant, RSVP không
oversell, outbox/messaging, S3-compatible media, OIDC và vận hành Docker/Kubernetes. Đây là
**learning lab** được kiểm chứng ở local/Minikube; không phải tuyên bố production-ready hoặc AWS
đã triển khai.

## Bắt đầu từ đâu

| Nếu bạn muốn… | Đọc/chạy… |
|---|---|
| Hiểu phạm vi nghiệp vụ, acceptance criteria và non-goals | [PLAN_DETAIL.md](PLAN_DETAIL.md) |
| Kiểm tra build nhanh | Xem [Build/test Java](#1-buildtest-java) |
| Chạy topology Docker hiện hành | Xem [Consistency lab](#2-chay-manifest-dang-co-trong-repository) |
| Hiểu ownership, messaging và invariant | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) |
| Tìm tài liệu theo chủ đề | Xem [Bản đồ tài liệu](#ban-do-tai-lieu) |
| Biết phần nào chưa làm/chưa verify | Xem [Trạng thái và giới hạn](#trang-thai-va-gioi-han) |

## Trạng thái nhanh

| Phạm vi | Trạng thái | Nguồn xác minh / lưu ý |
|---|---|---|
| Ba application deployable, business flow, test Maven | **VERIFIED LOCAL** | Lần evidence mới nhất trong repo: 2026-09-21; xem [VERIFICATION_EVIDENCE](docs/VERIFICATION_EVIDENCE.md) |
| Consistency: shared primary + 1 read replica, causal read, reconciliation | **VERIFIED LOCAL** | Chạy bằng [docker-compose.consistency.yml](docker-compose.consistency.yml) |
| Docker/Minikube core flow, JWT, metric, backup/restore | **VERIFIED LOCAL** / **VERIFIED K8S** trong các vòng evidence trước | Xem [runbook](docs/DOCKER_TO_K8S_RUNBOOK.md); một số Docker command ở đó là historical |
| AWS RDS/S3/EKS/IAM, managed-broker HA, CI/CD, RPO/RTO | **NOT RUN** / deferred | Xem [AWS_MANUAL_GAPS](docs/AWS_MANUAL_GAPS.md) và [TODO](TODO.md) |

VERIFIED luôn mô tả một lần chạy có ghi evidence, không phải guarantee cho môi trường khác.

## Hệ thống này làm gì?

~~~text
                                  Keycloak / OIDC
                              (identity, không sở hữu data)
                                           |
Client --> API Gateway :8080 ------------+------------------+
             | routing, correlation, JWT                    |
             v                                                v
 Event Service :8081                                  Registration Service :8082
 - event lifecycle                                    - local event projection
 - shared PostgreSQL: event_service schema            - shared PostgreSQL: registration_service schema
 - media metadata / S3                                - RSVP, capacity, check-in
 - outbox                                              - inbox/outbox, analytics
       |       \                                           ^
       |        +--> Kafka: domain-events / analytics      |
       +--> RabbitMQ: event projection --------------------+
                    (retry + DLQ)

LocalStack / S3-compatible bucket <--- presigned upload --- Event Service
~~~

Project chỉ có ba application deployable:

| Thành phần | Trách nhiệm | Không sở hữu |
|---|---|---|
| api-gateway | Route, request/correlation ID, chuyển đổi local-header/OIDC JWT | Business rule, database, resource authorization |
| event-service | Event, lifecycle, media metadata, event outbox, audit, Event DB | Registration tables |
| registration-service | Event projection, RSVP/capacity, idempotency, check-in, inbox/outbox, analytics | Event source-of-truth tables |
| Keycloak | Identity, token và role claim | Dữ liệu EventFlow |

Các boundary chi tiết, lý do tách schema và giới hạn lab nằm trong
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Invariant và mô hình dữ liệu quan trọng

- Event Service và Registration Service sở hữu schema riêng trong cùng một PostgreSQL database; Registration Service không
  join trực tiếp bảng của Event Service.
- PostgreSQL transaction là business truth. Mutation ghi outbox trong cùng transaction; relay tới
  RabbitMQ/Kafka là at-least-once, vì vậy consumer deduplicate theo messageId.
- RSVP dùng local event projection, row lock và conditional update confirmed_count < capacity;
  đây là điểm bảo vệ không oversell.
- Trước RSVP/check-in, Registration Service đọc private source state từ Event Service để giảm
  stale window khi Rabbit projection chưa cập nhật. Đây không phải distributed transaction.
- RabbitMQ là workflow delivery (retry/DLQ); Kafka chỉ phục vụ stream/analytics, không nằm trên
  RSVP critical path.
- Contract của domain event: [contracts/events/domain-event-envelope.json](contracts/events/domain-event-envelope.json).

## Yêu cầu môi trường

| Mục đích | Cần có |
|---|---|
| Build/test Java | JDK 21, Maven 3.9+ (không có Maven Wrapper) |
| Consistency lab | Docker Engine và Docker Compose v2 |
| Test harness | Python 3.9+; các script hiện dùng standard library, không có requirements.txt |
| Backup/replay script | PowerShell 7 (pwsh) |
| Kubernetes lab | Docker, Minikube và kubectl; xem runbook để có profile eventflow |

maven-settings.xml cấu hình HTTP mirror Huaweicloud do giới hạn truststore/proxy của lab. Nó là
workaround bootstrap cục bộ, không nên được xem là cấu hình CI/production mặc định.

Trước khi chạy Compose hoặc render manifest Kubernetes, copy `.env.example` thành `.env` và thay các giá trị local. File `.env` đã được ignore và không được commit.

## Build và consistency lab Docker hiện hành

### 1. Build/test Java

~~~powershell
mvn -B -gs maven-settings.xml clean verify
~~~

### 2. Chạy manifest đang có trong repository

Manifest thực thi hiện hành là [docker-compose.consistency.yml](docker-compose.consistency.yml).
Nó chạy một shared PostgreSQL primary + đúng một physical read replica, hai schema logic (`event_service` và `registration_service`), RabbitMQ, Kafka, LocalStack và
ba application service. Keycloak **không** có trong manifest này; Gateway chạy local header mode
với OIDC_ENABLED=false.

~~~powershell
Copy-Item .env.example .env  # chỉnh giá trị local trước khi chạy
docker compose -f docker-compose.consistency.yml config
docker compose -f docker-compose.consistency.yml up -d --build
python test_consistency_reconciliation.py
python test_consistency_second_round.py
~~~

Hai scenario trên chủ động dừng/khởi động Rabbit hoặc replica và tạo dữ liệu fixture. Chỉ chạy
trong Docker lab disposable, không dùng với dữ liệu cần giữ.

| Host endpoint của consistency lab | Giá trị |
|---|---|
| Gateway public | http://localhost:28181 |
| Event Service (loopback-only) | http://127.0.0.1:28081 |
| Registration Service (loopback-only) | http://127.0.0.1:28082 |
| Shared PostgreSQL primary / read replica | 55432 / 55433 |
| Logical schemas | `event_service` / `registration_service` |
| RabbitMQ AMQP / management | 5672 / 15672 |
| Kafka / LocalStack | 9092 / 4566 |

Dọn lab này sẽ xóa volume của riêng Compose project:

~~~powershell
docker compose -f docker-compose.consistency.yml down -v --remove-orphans
~~~

Đường chạy, 3-second writer pinning, LSN/version causal header, writer fallback và HLC-based
reconciliation được mô tả tại [docs/CONSISTENCY_RECONCILIATION.md](docs/CONSISTENCY_RECONCILIATION.md).

### Docker runbook historical và JWT

Không có docker-compose.yml active ở root. File Compose cũ có Keycloak đã được cố ý xóa sau vòng
verification và chỉ còn dạng archive tại
[docs/DOCKER_COMPOSE_MANIFEST.md](docs/DOCKER_COMPOSE_MANIFEST.md). Vì vậy, **không chạy**
docker compose up không kèm -f, hoặc test_jwt.py theo Docker, trên consistency manifest hiện hành:
manifest đó không có Keycloak.

[docs/DOCKER_TO_K8S_RUNBOOK.md](docs/DOCKER_TO_K8S_RUNBOOK.md) vẫn là nguồn tham khảo cho
Minikube, OIDC historical, metric, failure drill, backup/restore và cleanup. Phần Docker trong
runbook giả định manifest archive đã được phục hồi; đọc prerequisite trước khi copy lệnh.

## API công khai

Mọi public API đi qua Gateway. DTO/validation chi tiết hiện nằm trong
apps/event-service/.../api/EventDtos.java và
apps/registration-service/.../api/RegistrationDtos.java; project chưa có OpenAPI document riêng.

| Nhóm | Endpoint |
|---|---|
| Event | POST, GET /api/v1/events; GET, PATCH /api/v1/events/{eventId} |
| Lifecycle | POST /api/v1/events/{eventId}/publish; POST /api/v1/events/{eventId}/cancel |
| Media | POST /api/v1/events/{eventId}/media/upload-session; POST /api/v1/events/{eventId}/media/{mediaId}/finalize |
| Registration | POST /api/v1/events/{eventId}/registrations; GET, DELETE /api/v1/events/{eventId}/registrations/me |
| Attendance | POST /api/v1/events/{eventId}/check-ins; GET /api/v1/events/{eventId}/attendance |
| Network egress/weather | GET /api/v1/network/weather |

`GET /api/v1/network/weather` calls the configured geolocation service from the Gateway,
so the returned IP/city represent the server's observed public egress (the AWS NAT Gateway
in the AWS deployment). It then calls Open-Meteo for current weather at those coordinates.
The upstream URLs and request timeout can be overridden with `PUBLIC_GEOLOCATION_URL`,
`PUBLIC_WEATHER_URL`, and `PUBLIC_WEATHER_TIMEOUT`.

POST .../registrations yêu cầu Idempotency-Key. Các endpoint /internal/** là private
service-to-service/reconciliation-lab API, không phải public API và không nên expose qua Gateway.

### Identity và phân quyền

Local-only header mode dùng:

~~~text
X-User-Id: organizer-1
X-Workspace-Id: workspace-1
X-Roles: ORGANIZER
~~~

Các role gồm OWNER, ORGANIZER, PARTICIPANT và CHECKIN_STAFF. Không dùng header này làm
authentication production. Khi bật OIDC_ENABLED=true, Gateway phải validate issuer, signature,
expiry và JWKS của JWT; identity header client gửi sẽ bị bỏ và tạo lại từ verified claims. Khi
triển khai thật, giữ service private phía sau Gateway, dùng secret manager/IAM và không dùng lab
credential/Keycloak fixture.

## Cấu trúc repository

| Path | Nội dung |
|---|---|
| [apps/api-gateway](apps/api-gateway) | Spring Cloud Gateway, route và security/filter |
| [apps/event-service](apps/event-service) | Event lifecycle, media, outbox, read routing/reconciliation source |
| [apps/registration-service](apps/registration-service) | Projection, RSVP, capacity, check-in, inbox/outbox, analytics/reconciliation worker |
| [contracts/events](contracts/events) | JSON schema cho domain-event envelope |
| [infra/kubernetes/all.yaml](infra/kubernetes/all.yaml) | Manifest Minikube/Kubernetes |
| [infra/keycloak](infra/keycloak) | Dev realm Keycloak |
| [infra/localstack](infra/localstack) | Bucket init cho LocalStack |
| [infra/postgres](infra/postgres) | Primary/replica bootstrap cho consistency lab |
| [ops/backup](ops/backup) | Backup/restore PostgreSQL có guard xác nhận |
| [docker-compose.consistency.yml](docker-compose.consistency.yml) | Manifest Docker active cho consistency/reconciliation lab |
| test_*.py, kafka-replay.ps1 | Black-box/failure/metrics harness và local replay tool |
| [docs](docs) | Thiết kế, runbook và evidence — xem bảng bên dưới |

## Test, evidence và vận hành

| Công cụ | Khi nào dùng | Lưu ý |
|---|---|---|
| mvn -B -gs maven-settings.xml clean verify | Build và Java test | Command an toàn nhất để kiểm tra nhanh source hiện tại |
| test_consistency_reconciliation.py | Consistency Compose active | Scenario destructive đối với lab: dừng replica/Rabbit và tạo fixture |
| test_read_consistency_matrix.py | Consistency Compose active | Ma trận OFF/VERSION_LSN, LSN threshold và scope mismatch fallback |
| test_consistency_second_round.py | Consistency Compose active | Tạo 201 fixture rows và kiểm tra auth/CAS/fairness |
| test_p0.py | Source-state guard trên consistency lab | Set COMPOSE_FILE = docker-compose.consistency.yml trước khi chạy |
| test_metrics.py | Metric endpoint Docker hiện hành | Dùng default 28081/28082; các flag URL trong runbook historical hiện không được script parse |
| test_jwt.py | JWT qua Keycloak | Dùng K8s hoặc restored historical Compose; không chạy trực tiếp với manifest consistency |
| test_scenarios.py, test_negative.py, test_degrade.py | Consistency Compose active | E2E, negative security/media/quarantine và dependency degrade |
| test_resilience.py, kafka-replay.ps1 | Failure/replay drill | Cần đọc precondition; thao tác queue/Kafka/analytics có thể destructive |

Ví dụ chạy source-state guard với PowerShell:

~~~powershell
$env:COMPOSE_FILE = 'docker-compose.consistency.yml'
python test_p0.py
~~~

Backup/restore có ConfirmRestore, dùng SQL clean/if-exists và phải dừng writer trước khi restore.
kafka-replay.ps1 yêu cầu ConfirmLocalReset trước khi truncate analytics/reset consumer group. Các
script này chỉ dành cho lab; xem [runbook](docs/DOCKER_TO_K8S_RUNBOOK.md) và
[AWS boundary](docs/AWS_MANUAL_GAPS.md) trước khi dùng. Event DB backup/restore Docker trong
runbook historical hiện chưa khớp trực tiếp service name event-db-primary của consistency lab.

## Bản đồ tài liệu

| Path | Nội dung | Cách dùng |
|---|---|---|
| [PLAN_DETAIL.md](PLAN_DETAIL.md) | Scope, business contract, acceptance criteria, non-goals, Definition of Done | Nguồn yêu cầu chính |
| [TODO.md](TODO.md) | Tiến độ, deferred work và định nghĩa mức evidence | Theo dõi backlog/trạng thái |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Boundary, ownership, messaging, invariant, security và lab limits | Đọc trước khi sửa design |
| [docs/CONSISTENCY_RECONCILIATION.md](docs/CONSISTENCY_RECONCILIATION.md) | 1 shared read replica, causal LSN/version reads, HLC merge, active Docker scenario, AWS boundary | Tài liệu hiện hành cho consistency lab |
| [docs/DOCKER_TO_K8S_RUNBOOK.md](docs/DOCKER_TO_K8S_RUNBOOK.md) | Docker/Minikube, OIDC, metrics, backup/restore, cleanup | Reference vận hành; Docker section là historical nếu không phục hồi manifest cũ |
| [docs/DOCKER_COMPOSE_MANIFEST.md](docs/DOCKER_COMPOSE_MANIFEST.md) | Nội dung docker-compose.yml cũ | Archive/manual replay, không phải runtime default |
| [docs/VERIFICATION_EVIDENCE.md](docs/VERIFICATION_EVIDENCE.md) | Lệnh và kết quả các vòng verification local/K8s | Evidence theo ngày; mục 2026-09-21 là summary mới nhất của consistency lab |
| [docs/ENGINEERING_REVIEW.md](docs/ENGINEERING_REVIEW.md) | Review decision, coverage, fix đã làm và deliberate non-expansion | Kiểm tra phạm vi review |
| [docs/DEGRADE_ASSESSMENT.md](docs/DEGRADE_ASSESSMENT.md) | Rabbit/Kafka outage behavior và limit | Historical local failure evidence |
| [docs/LOCALSTACK_EVIDENCE.md](docs/LOCALSTACK_EVIDENCE.md) | LocalStack/S3 emulator evidence | Không thay thế AWS IAM/S3 evidence |
| [docs/AWS_MANUAL_GAPS.md](docs/AWS_MANUAL_GAPS.md) | Code đã có so với AWS/HA/CI-CD/RPO-RTO chưa chạy | Bắt buộc đọc trước production planning |
| [contracts/events/domain-event-envelope.json](contracts/events/domain-event-envelope.json) | Schema envelope event bất đồng bộ | Contract cho producer/consumer |
| [infra/kubernetes/all.yaml](infra/kubernetes/all.yaml) | ConfigMap/Secret/Deployment/Service và dependency local | Deploy Minikube; không phải EKS production |
| [ops/backup](ops/backup) | PostgreSQL dump/restore cho lab | Có guard xác nhận; restore destructive |
| [docker-compose.consistency.yml](docker-compose.consistency.yml) | Topology primary/replica + reconciliation hiện hành | Dùng kèm test_consistency_*.py |

## Trạng thái và giới hạn

- Không có custom Auth Service; Keycloak chỉ là identity infrastructure/dev fixture.
- Không tuyên bố exactly-once external side effect hoặc distributed atomic transaction giữa
  PostgreSQL, RabbitMQ và Kafka.
- Kafka/Rabbit/PostgreSQL HA production, AWS RDS/S3/EKS/IAM, CI/CD, secret rotation và AWS
  RPO/RTO vẫn chưa được chạy. Kubernetes lab có hai replica cho stateless app tier, nhưng stateful
  dependency là single-node và database dùng emptyDir.
- Consistency lab chạy trên cả Compose và Kubernetes với một shared primary + một read replica; đây vẫn là lab topology, không phải HA production.
- `.env.example` liệt kê biến runtime local; `.env` không được commit. Bảng configuration canonical và OpenAPI request-response reference vẫn chưa có.
- Các Docker script/tài liệu historical và manifest consistency không hoàn toàn cùng topology.
  README này đánh dấu rõ đường chạy active; trước khi chuẩn hóa automation, nên tạo một manifest
  full-stack active (nếu cần Keycloak) hoặc cập nhật script theo service/port hiện tại.

Các deferred item cụ thể và mức evidence được duy trì trong [TODO.md](TODO.md). Đừng nâng một
claim từ VERIFIED LOCAL lên production capability nếu chưa có evidence tương ứng.

## GitHub Actions CI/CD

`.github/workflows/build-push.yml` authenticates to AWS with GitHub OIDC, builds the three
service images with immutable commit-SHA tags, and pushes them to ECR. It runs on pushes to
`main` and checks for new commits daily at 08:00 Asia/Ho_Chi_Minh; a scheduled run skips
when no commit was made in the previous 24 hours. ECR repositories and the
`github-ecr-push` OIDC role must be provisioned separately.