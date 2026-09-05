FROM openjdk:26-ea-jdk-slim AS builder
WORKDIR /app

COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
RUN chmod +x ./gradlew && ./gradlew dependencies --no-daemon || true

COPY src ./src
RUN ./gradlew bootJar --no-daemon -x test

FROM openjdk:26-ea-jdk-slim
WORKDIR /app

COPY --from=builder /app/build/libs/connector-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 5556

ENTRYPOINT ["java", "-XX:+UseZGC", "-jar", "app.jar"]
