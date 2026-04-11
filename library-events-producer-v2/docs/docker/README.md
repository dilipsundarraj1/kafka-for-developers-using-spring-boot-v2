# Docker Guide

<!-- TOC -->
* [Docker Guide](#docker-guide)
  * [Why this approach is useful](#why-this-approach-is-useful)
  * [Prerequisites](#prerequisites)
  * [Prompt to generate the Dockerfile](#prompt-to-generate-the-dockerfile)
  * [1) Build the app JAR](#1-build-the-app-jar)
  * [2) Build the Docker image](#2-build-the-docker-image)
  * [3) Run the container](#3-run-the-container)
    * [Why `host.docker.internal` and not `localhost`?](#why-hostdockerinternal-and-not-localhost)
  * [4) Verify the app is running](#4-verify-the-app-is-running)
  * [5) Stop and remove the container](#5-stop-and-remove-the-container)
  * [Optional: use a different Kafka broker address](#optional-use-a-different-kafka-broker-address)
  * [Run the same Docker image in different environments](#run-the-same-docker-image-in-different-environments)
    * [Run in `stage`](#run-in-stage)
    * [Run in `prod`](#run-in-prod)
    * [Key idea](#key-idea)
<!-- TOC -->

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

## Prompt to generate the Dockerfile

Use this prompt if you want an AI assistant to generate the same kind of `Dockerfile` used in this project:

```text
Generate a Dockerfile for my Spring Boot application with these requirements:

- Do not build the app inside Docker.
- I will run `./gradlew clean build` outside Docker first.
- The Dockerfile should copy the generated Spring Boot jar from `build/libs/` into the image.
- Use a runtime-only base image: `eclipse-temurin:25-jre`.
- Set the working directory to `/app`.
- Copy the executable jar to `/app/app.jar`.
- Expose port `8080`.
- Start the app with: `java -jar /app/app.jar`.
- Use an `ARG JAR_FILE=build/libs/*-SNAPSHOT.jar` so the jar path can be overridden if needed.
- Keep the Dockerfile simple and production-friendly.

Return only the Dockerfile content.
```

Expected result:

```dockerfile
FROM eclipse-temurin:25-jre
WORKDIR /app

ARG JAR_FILE=build/libs/*-SNAPSHOT.jar
COPY ${JAR_FILE} /app/app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

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

> Note: the project `.dockerignore` is configured to exclude most of `build/` but still include the executable Spring Boot JAR from `build/libs/`. That keeps the Docker context smaller while still allowing `docker build` to succeed after `./gradlew clean build`.

## 3) Run the container

```bash
docker run --name library-events-producer \
  -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=dev \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092 \
  library-events-producer:v1
```

If port `8080` is already in use, map another host port:

```bash
docker run --name library-events-producer \
  -p 18080:8080 \
  -e SPRING_PROFILES_ACTIVE=dev \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092 \
  library-events-producer:v1
```

This uses the Docker-friendly Kafka listener from your `compose.yaml`.

### Why `host.docker.internal` and not `localhost`?

When your Spring Boot app runs **inside a Docker container**, `localhost` means:

- the **container itself**
- **not your Mac host machine**
- and **not the Kafka container started separately by `docker compose`**

So if you pass:

```bash
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

the app container tries to connect to Kafka **inside itself**, which fails.

`host.docker.internal` is a special Docker hostname that lets a container reach services exposed by your host environment.

In your setup:

- Kafka is started from `compose.yaml`
- Kafka exposes a Docker-friendly listener on `host.docker.internal:29092`
- your app container can reach Kafka through that address

Quick comparison:

- `localhost:9092` -> correct when the app runs directly on your machine
- `host.docker.internal:29092` -> correct when the app runs in Docker and Kafka is exposed from the host/compose setup
- `kafka1:19092` -> correct when both app and Kafka run in the same Docker network

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

## Optional: use a different Kafka broker address

If your Kafka broker is not the one from the current `compose.yaml`, change only this environment variable:

```bash
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=<your-broker-host>:<your-broker-port>
```

Examples:

```bash
# App runs on your machine and Kafka is exposed on localhost
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# App runs in Docker and Kafka is exposed with the Docker listener from compose.yaml
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092

# App and Kafka run in the same Docker network
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka1:19092
```

## Run the same Docker image in different environments

You can reuse the **same image** and switch behavior only through runtime environment variables.

### Run in `stage`

```bash
docker run --name library-events-producer-stage \
  -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=stage \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=stage-broker1:9092,stage-broker2:9092 \
  library-events-producer:v1
```

### Run in `prod`

```bash
docker run --name library-events-producer-prod \
  -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=prod-broker1:9092,prod-broker2:9092,prod-broker3:9092 \
  library-events-producer:v1
```

### Key idea

Build once:

```bash
docker build -t library-events-producer:v1 .
```

Run anywhere by changing only:

- `SPRING_PROFILES_ACTIVE`
- `SPRING_KAFKA_BOOTSTRAP_SERVERS`

