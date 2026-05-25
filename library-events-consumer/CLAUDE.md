nhn# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spring Boot Kafka consumer application that processes library event messages. Demonstrates enterprise error handling with retry logic, dead-letter topics, and failure recovery using JPA persistence.

## Build & Test Commands

```bash
# Build
./gradlew build

# Run all tests (unit + integration)
./gradlew test

# Run a specific test class
./gradlew test --tests "com.learnkafka.consumer.LibraryEventsConsumerIntegrationTest"

# Run a specific test method
./gradlew test --tests "com.learnkafka.consumer.LibraryEventsConsumerIntegrationTest.publishNewLibraryEvent"

# Run application (default local profile)
./gradlew bootRun

# Run with specific profile
./gradlew bootRun --args='--spring.profiles.active=nonprod'
```

## Tech Stack

- Java 25, Spring Boot 3.1.0, Spring Kafka
- H2 in-memory database with Spring Data JPA
- Gradle 7.6.1
- Lombok for boilerplate reduction
- JUnit 5 with EmbeddedKafka for integration tests

## Architecture

### Kafka Consumer Flow

1. `LibraryEventsConsumer` receives messages from `library-events` topic (consumer group: `library-events-listener-group`)
2. `LibraryEventsService` deserializes JSON to `LibraryEvent` entity and processes it
3. NEW events are saved directly; UPDATE events require existing event validation
4. Error handling routes failures:
   - Recoverable errors (RecoverableDataAccessException) → `library-events.RETRY` topic
   - Non-recoverable errors (IllegalArgumentException) → `library-events.DLT` (dead letter topic)

### Key Components

- **LibraryEventsConsumerConfig** (`config/`): Configures Kafka listener container factory with concurrency=3, error handlers with FixedBackOff(1000ms, 2 retries), and DeadLetterPublishingRecoverer
- **LibraryEventsService** (`service/`): Business logic for processing events and persisting to H2
- **FailureService** (`service/`): Persists failed records for later retry
- **RetryScheduler** (`scheduler/`): Scheduled task (currently disabled) for reprocessing failed records

### Entities

- `LibraryEvent`: Main event with ID, type (NEW/UPDATE), and associated Book
- `Book`: Book details with bidirectional relationship to LibraryEvent
- `FailureRecord`: Stores failed message metadata for retry processing

## Configuration

Application runs on port 8081. Key properties in `application.yml`:

- `topics.retry`: Retry topic name (default: `library-events.RETRY`)
- `topics.dlt`: Dead letter topic name (default: `library-events.DLT`)
- `libraryListener.startup` / `retryListener.startup`: Enable/disable consumers

Profiles: `local` (default), `nonprod` (with SSL), `prod`

## Testing

Tests are in separate source directories:
- `src/test/java/unit/` - Unit tests
- `src/test/java/intg/` - Integration tests using EmbeddedKafka

Integration tests use `@EmbeddedKafka` annotation with topics: `library-events`, `library-events.RETRY`, `library-events.DLT` (3 partitions each). SpyBean is used on consumers and services for verification.
