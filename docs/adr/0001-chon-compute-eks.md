# ADR-0001: Chọn EKS làm nền compute cho EventFlow

- Trạng thái: Accepted
- Ngày: 2026-09-28
- Người quyết định: <tên>

## Bối cảnh

EventFlow gồm 3 service Java 21 / Spring Boot 3.3.5 (api-gateway, event-service,
registration-service), chạy liên tục, giao tiếp qua RabbitMQ (outbox/workflow/DLQ)
và Kafka (analytics), dùng PostgreSQL, Keycloak/OIDC, S3. Môi trường local đã chạy
bằng Docker Compose và Minikube với manifest Kubernetes. Cần scale độc lập từng
service, rolling update không downtime, health probe, cấu hình/secret tách khỏi image.

## Cách ra quyết định: nhìn vào đặc tính workload

| Đặc tính workload EventFlow | Hệ quả với lựa chọn compute |
|---|---|
| 3 service Java/Spring Boot chạy liên tục (long-running) | Lambda kém phù hợp (cold start JVM, giới hạn 15 phút) |
| api-gateway là Spring Cloud Gateway, giữ kết nối liên tục | Cần process luôn chạy, không phải hàm theo sự kiện |
| Consumer RabbitMQ, producer Kafka chạy nền | Cần process luôn chạy và kết nối dài |
| Đã có manifest Kubernetes (Minikube) | EKS tái sử dụng; ECS phải viết lại Task Definition |
| Cần scale độc lập từng service, rolling update, health probe | Cả ECS và EKS đáp ứng, EC2 phải tự làm |
| Nhóm đã học K8s | Chi phí học thấp với EKS |

## Các phương án

1. **EC2 tự quản:** kiểm soát cao nhưng phải tự làm deploy, scale, patch, health check.
2. **ECS (Fargate/EC2):** vận hành nhẹ hơn EKS, nhưng phải viết lại toàn bộ manifest
   sang Task Definition và gắn chặt vào AWS.
3. **EKS:** dùng lại manifest/Helm đã có từ Minikube, HPA, probe, rolling update sẵn.
4. **Lambda:** cold start JVM, giới hạn thời gian chạy, không hợp Spring Cloud Gateway
   và consumer chạy dài.

## Quyết định

Chọn EKS (managed node group) cho 3 service. Lambda chỉ dùng cho tác vụ phụ theo
sự kiện nếu cần sau này. RDS, S3, Amazon MQ/MSK là dịch vụ managed ngoài cluster.

## Hệ quả

**Tích cực:** tái dùng manifest Minikube; scale/rollout chuẩn K8s; ít phụ thuộc AWS-specific.

**Tiêu cực:** phí control plane cố định; phải quản lý node group và nâng cấp phiên bản
K8s định kỳ; độ phức tạp vận hành cao hơn ECS.

**Giảm thiểu:** dùng managed node group, bật cluster autoscaler/Karpenter về sau,
tắt/xoá cluster khi học xong để tránh tốn phí.

## Xem xét lại khi

Đội nhỏ đến mức chi phí vận hành K8s vượt lợi ích (khi đó cân nhắc ECS Fargate),
hoặc khi số service tăng mạnh và cần service mesh.

## 2.2. So sánh bốn lựa chọn

| Tiêu chí | EC2 | ECS | EKS | Lambda |
|---|---|---|---|---|
| Đơn vị triển khai | VM | Task/Service | Pod/Deployment | Function |
| Bạn phải quản lý | OS, patch, scale, deploy | Cluster (nhẹ), task definition | Node group, upgrade K8s, add-on | Gần như không |
| Phù hợp workload | Ứng dụng cũ, cần kiểm soát OS | Container đơn giản, ít phụ thuộc K8s | Nhiều microservice, cần hệ sinh thái K8s | Hàm ngắn, theo sự kiện |
| Chi phí cố định | Theo instance | Không có phí control plane | Phí control plane EKS (khoảng 0,10 USD/giờ) cộng node | Trả theo lượt gọi |
| Portability | Thấp | Gắn chặt AWS | Cao (K8s chuẩn) | Gắn chặt AWS |
| Độ phức tạp vận hành | Cao | Thấp | Cao | Thấp |

Lambda vẫn hợp cho các việc phụ (xử lý ảnh khi upload lên S3, cron nhẹ), nhưng không
dùng làm compute chính cho ba service này.