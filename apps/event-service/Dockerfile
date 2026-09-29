FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml maven-settings.xml ./
COPY apps/api-gateway/pom.xml apps/api-gateway/pom.xml
COPY apps/event-service/pom.xml apps/event-service/pom.xml
COPY apps/registration-service/pom.xml apps/registration-service/pom.xml
COPY apps/event-service/src apps/event-service/src
RUN mvn -B -gs maven-settings.xml -pl apps/event-service -am package -DskipTests

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /workspace/apps/event-service/target/app.jar app.jar
USER 10001
EXPOSE 8081
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
