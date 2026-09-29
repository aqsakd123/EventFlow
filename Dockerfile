# syntax=docker/dockerfile:1.7
# Build with: docker build --build-arg SERVICE=apps/api-gateway --build-arg PORT=8080 -t eventflow/api-gateway:local .

FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Keep dependency metadata in an earlier layer for better rebuild caching.
COPY pom.xml maven-settings.xml ./
COPY apps/api-gateway/pom.xml apps/api-gateway/pom.xml
COPY apps/event-service/pom.xml apps/event-service/pom.xml
COPY apps/registration-service/pom.xml apps/registration-service/pom.xml

ARG SERVICE
COPY apps/api-gateway/src apps/api-gateway/src
COPY apps/event-service/src apps/event-service/src
COPY apps/registration-service/src apps/registration-service/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -gs maven-settings.xml -pl "${SERVICE}" -am package -DskipTests

FROM eclipse-temurin:21-jre-jammy AS runtime
ARG SERVICE
ARG PORT=8080
WORKDIR /app
RUN useradd -r -u 1001 app
COPY --from=build /workspace/${SERVICE}/target/app.jar /app/app.jar
USER 1001
EXPOSE ${PORT}
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]