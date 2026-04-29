# Library Events Consumer

Spring Boot 4 / Java 25 Kafka consumer for the `library-events` topic.

It consumes JSON events, maps DTOs to JPA entities, and persists them to PostgreSQL with Flyway-managed schema.

## What this app does

- Listens to Kafka topic: `library-events`
- Deserializes payloads to `LibraryEventDto`
- Persists `LibraryEvent` and `Book` entities to PostgreSQL
- Supports retry + recovery mode (`failure-table`, `dlt`, `both`)
- Exposes REST APIs for querying and managing persisted data
- Exposes Actuator health probes for liveness and readiness

## Tech stack

- Java 25
- Spring Boot 4.0.3
- Spring Kafka
- Spring Data JPA
- PostgreSQL
- Flyway
- Testcontainers

## Project structure (high level)

- `src/main/java/com/learnkafka/consumer` - Kafka listener
- `src/main/java/com/learnkafka/service` - event processing transaction flow
- `src/main/java/com/learnkafka/domain` - JPA entities
- `src/main/java/com/learnkafka/dto` - DTO records
- `src/main/java/com/learnkafka/mapper` - DTO to entity mapping
- `src/main/resources/db/migration` - Flyway migrations
- `docs/` - PRD, implementation plan, and course notes

## Prerequisites

- Java 25
- Docker Desktop (for local PostgreSQL)
- Kafka broker reachable from the app

## Configuration

Main config file: `src/main/resources/application.yml`

Common runtime environment variables:

- `SPRING_DATASOURCE_URL` (example: `jdbc:postgresql://host.docker.internal:5432/mydatabase`)
- `SPRING_DATASOURCE_USERNAME` (example: `myuser`)
- `SPRING_DATASOURCE_PASSWORD` (example: `secret`)
- `SPRING_KAFKA_BOOTSTRAP_SERVERS` (example: `host.docker.internal:29092`)

## Run locally

```bash
./gradlew clean build
./gradlew bootRun
```

## Run with Docker image

```bash
./gradlew clean build
docker build -t library-events-consumer:v1 .
docker run --name library-events-consumer \
  -p 8081:8081 \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/mydatabase \
  -e SPRING_DATASOURCE_USERNAME=myuser \
  -e SPRING_DATASOURCE_PASSWORD=secret \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092 \
  library-events-consumer:v1
```

## Health probes (liveness + readiness)

This app has Actuator probes enabled in `src/main/resources/application.yml`.

Probe endpoints:

- Liveness: `GET /actuator/health/liveness`
- Readiness: `GET /actuator/health/readiness`
- Additional probe paths (enabled):
  - `GET /livez`
  - `GET /readyz`

Quick check:

```bash
curl -i http://localhost:8081/actuator/health/liveness
curl -i http://localhost:8081/actuator/health/readiness
curl -i http://localhost:8081/livez
curl -i http://localhost:8081/readyz
```

## REST endpoints

- `GET /v1/library-events`
- `GET /v1/library-events/{id}`
- `POST /v1/library-events`
- `PUT /v1/library-events/{id}`
- `GET /v1/books`
- `GET /v1/books/{id}`
- `POST /v1/books`
- `PUT /v1/books/{id}`
- `DELETE /v1/books/{id}`

## Tests

```bash
./gradlew test
```

## Notes

- Schema changes should always be done via new Flyway migrations.
- `ddl-auto` is intentionally `none`.
- Detailed error-handling walkthrough is covered later in the course materials.

