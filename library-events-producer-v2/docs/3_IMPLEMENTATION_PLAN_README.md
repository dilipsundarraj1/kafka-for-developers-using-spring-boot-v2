# Implementation Plan: Library Events Producer API

This plan translates the PRD into executable engineering steps.

## Checklist
1. Domain model and validation
2. API layer (POST and PUT)
3. Kafka publishing
4. Retry strategy
5. Serialization
6. Tests
7. Observability
8. Docs

## 1. Domain Model and Validation
- Create models: `LibraryEvent`, `Book`, `LibraryEventType`.
- Add validation annotations (`@NotNull`, `@NotBlank`) for required fields.
- Enforce `libraryEventId` required for UPDATE.
- Enforce POST requires `libraryEventType` = ADD.

## 2. API Layer
- Create controller `LibraryEventsController` with:
  - `POST /v1/library-events` -> 201 Created.
  - `PUT /v1/library-events` -> 200 OK.
- Add exception handling for validation errors and server errors.

## 3. Kafka Publishing
- Configure Kafka producer properties in `application.properties`.
- Implement `LibraryEventProducer` using `KafkaTemplate`.
- Topic name: `library-events`.
- Use `libraryEventId` as message key when present.

## 4. Retry Strategy
- Add retry for Kafka publish failures (e.g., Spring Retry or KafkaTemplate retries).
- Return 500 after retries are exhausted.
- Log retry attempts and final failures.

## 5. Serialization
- Use JSON serialization for event payloads (Jackson).
- Keep schema consistent with `docs/PRD.md`.

## 6. Tests
- Unit tests for validation rules and controller responses.
- Producer tests with Kafka test utilities.
- Optional integration tests for POST/PUT and Kafka publish.

## 7. Observability
- Log request receipt and publish success/failure.
- Optional: basic metrics for counts and latency.

## 8. Documentation
- Keep `docs/PRD.md` current.
- Optionally add API examples to `README.md`.

