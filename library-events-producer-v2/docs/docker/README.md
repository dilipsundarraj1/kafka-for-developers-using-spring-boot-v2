# Docker Guide

## Why this approach is useful

- **Faster image builds**: Gradle work stays outside Docker, so image creation is just a copy + package step.
- **Smaller runtime image**: The container only needs JRE + your app JAR (no Gradle cache or source code).
- **Clear separation of concerns**: Build concerns stay with Gradle; runtime concerns stay with Docker.
- **Easier debugging**: You can verify the JAR locally before containerizing, which narrows down failures quickly.
- **CI/CD friendly**: Pipelines can reuse the same JAR artifact for testing, scanning, and image creation.

This guide uses the current `Dockerfile` approach:

1. Build the Spring Boot JAR on your machine with Gradle.
2. Build a Docker image that copies the generated JAR.
3. Run the container.

## Prerequisites

- Docker running locally
- Java and Gradle wrapper available (`./gradlew`)

## 1) Build the app JAR

Run from the project root:

```bash
./gradlew clean build
```

This creates JARs under `build/libs/`.

## 2) Build the Docker image

```bash
docker build -t library-events-producer:v1 .
```

The `Dockerfile` copies `build/libs/*-SNAPSHOT.jar` into `/app/app.jar`.

## 3) Run the container

```bash
docker run --name library-events-producer -p 8080:8080 library-events-producer:v1
```

If port `8080` is already in use, map another host port:

```bash
docker run --name library-events-producer -p 18080:8080 library-events-producer:v1
```

## 4) Verify the app is running

```bash
curl -i http://localhost:8080/swagger-ui.html
```

If you mapped to `18080`, use `http://localhost:18080/swagger-ui.html`.

## 5) Stop and remove the container

```bash
docker stop library-events-producer
docker rm library-events-producer
```

## Optional: run with Kafka bootstrap override

If you want to set Kafka broker at runtime:

```bash
docker run --name library-events-producer \
  -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=dev \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  library-events-producer:v1
```

Adjust the Kafka address/port for your environment.

