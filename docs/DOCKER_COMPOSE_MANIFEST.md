# EventFlow temporary Docker Compose manifest

This is archival text, not an active compose file. The root docker-compose.yml was
deleted after verification. To replay the Docker lab manually, copy the YAML below to
the workspace root as docker-compose.yml, then follow DOCKER_TO_K8S_RUNBOOK.md.

~~~yaml
services:
  event-db:
    image: postgres:16.4
    environment:
      POSTGRES_DB: eventflow_event
      POSTGRES_USER: eventflow
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "15432:5432"
    volumes:
      - eventflow_event_db_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U eventflow -d eventflow_event"]
      interval: 5s
      timeout: 5s
      retries: 20

  registration-db:
    image: postgres:16.4
    environment:
      POSTGRES_DB: eventflow_registration
      POSTGRES_USER: eventflow
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "15433:5432"
    volumes:
      - eventflow_registration_db_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U eventflow -d eventflow_registration"]
      interval: 5s
      timeout: 5s
      retries: 20

  rabbitmq:
    image: rabbitmq:3.13.7-management
    environment:
      RABBITMQ_DEFAULT_USER: eventflow
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASSWORD}
    ports:
      - "15674:5672"
      - "15673:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 5s
      retries: 20

  kafka:
    image: apache/kafka:3.8.1
    environment:
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093"
      KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
    ports:
      - "19092:9092"
    volumes:
      - eventflow_kafka_data:/var/lib/kafka/data

  localstack:
    image: localstack/localstack:3.8.1
    environment:
      SERVICES: s3
      AWS_DEFAULT_REGION: us-east-1
    ports:
      - "4567:4566"
    volumes:
      - "./infra/localstack/init:/etc/localstack/init/ready.d:ro"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:4566/_localstack/health >/dev/null"]
      interval: 5s
      timeout: 5s
      retries: 30

  keycloak:
    image: quay.io/keycloak/keycloak:25.0.6
    command: ["start-dev", "--http-port=8080", "--import-realm"]
    environment:
      KC_BOOTSTRAP_ADMIN_USERNAME: admin
      KC_BOOTSTRAP_ADMIN_PASSWORD: ${KEYCLOAK_ADMIN_PASSWORD}
      KC_HEALTH_ENABLED: "true"
      KC_HOSTNAME: http://localhost:8180
    ports:
      - "8180:8080"
    volumes:
      - "./infra/keycloak:/opt/keycloak/data/import:ro"

  event-service:
    image: eventflow/event-service:0.1.0
    build:
      context: .
      dockerfile: apps/event-service/Dockerfile
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://event-db:5432/eventflow_event
      SPRING_DATASOURCE_USERNAME: eventflow
      SPRING_DATASOURCE_PASSWORD: ${POSTGRES_PASSWORD}
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: "5672"
      RABBITMQ_USERNAME: eventflow
      RABBITMQ_PASSWORD: ${RABBITMQ_PASSWORD}
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: eventflow.domain-events
      REGISTRATION_SERVICE_URL: http://registration-service:8082
      S3_ENDPOINT: http://localstack:4566
      S3_BUCKET: eventflow-media
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    ports:
      - "28081:8081"
    depends_on:
      event-db:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
      localstack:
        condition: service_healthy
      kafka:
        condition: service_started

  registration-service:
    image: eventflow/registration-service:0.1.0
    build:
      context: .
      dockerfile: apps/registration-service/Dockerfile
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://registration-db:5432/eventflow_registration
      SPRING_DATASOURCE_USERNAME: eventflow
      SPRING_DATASOURCE_PASSWORD: ${POSTGRES_PASSWORD}
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: "5672"
      RABBITMQ_USERNAME: eventflow
      RABBITMQ_PASSWORD: ${RABBITMQ_PASSWORD}
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: eventflow.domain-events
      AWS_REGION: us-east-1
      EVENT_SERVICE_URL: http://event-service:8081
    ports:
      - "28082:8082"
    depends_on:
      registration-db:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
      kafka:
        condition: service_started

  api-gateway:
    image: eventflow/api-gateway:0.1.0
    build:
      context: .
      dockerfile: apps/api-gateway/Dockerfile
    ports:
      - "28181:8080"
    environment:
      REGISTRATION_SERVICE_URL: http://registration-service:8082
      EVENT_SERVICE_URL: http://event-service:8081
      OIDC_ENABLED: "${OIDC_ENABLED:-false}"
      OIDC_ISSUER_URI: "${OIDC_ISSUER_URI:-http://localhost:8180/realms/eventflow}"
      OIDC_JWK_SET_URI: "${OIDC_JWK_SET_URI:-http://keycloak:8080/realms/eventflow/protocol/openid-connect/certs}"
    depends_on:
      event-service:
        condition: service_started
      registration-service:
        condition: service_started
      keycloak:
        condition: service_started

volumes:
  eventflow_event_db_data:
  eventflow_registration_db_data:
  eventflow_kafka_data:
~~~

After a manual replay, remove the reconstructed file only after:
docker compose down -v --remove-orphans and the checks in the runbook pass.
