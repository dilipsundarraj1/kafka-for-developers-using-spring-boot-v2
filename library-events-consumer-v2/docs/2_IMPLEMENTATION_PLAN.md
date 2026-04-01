# Implementation Plan
## Library Events Consumer

## Table of Contents

- [1. Objective](#1-objective)
- [2. Planning Assumptions](#2-planning-assumptions)
- [3. Execution-Order Implementation Roadmap](#3-execution-order-implementation-roadmap)
  - [Step 1: Kafka Consumer + Configuration](#step-1-kafka-consumer--configuration--start-here)
  - [Step 2: DTO + Deserialization](#step-2-dto--deserialization)
  - [Step 3: Kafka Under the Hood](#step-3-kafka-under-the-hood)
  - [Step 4: StringDeserializer vs JsonDeserializer](#step-4-stringdeserializer-vs-jsondeserializer)
  - [Step 5: Consumer Groups and Consumer Offset Management](#step-5-consumer-groups-and-consumer-offset-management)
  - [Step 6: Tasks - Business Logic](#step-6-tasks---business-logic)
  - [Step 7: Integration Test to Ensure Save is Working](#step-7-integration-test-to-ensure-save-is-working)
- [4. Testing Strategy](#4-testing-strategy)
  - [4.1 Unit Tests](#41-unit-tests)
  - [4.2 Integration Tests](#42-integration-tests)
  - [4.3 Repository Tests](#43-repository-tests)
  - [4.4 Minimum Acceptance Test Matrix](#44-minimum-acceptance-test-matrix)
- [5. Execution Sequence Summary](#5-execution-sequence-summary)
- [6. Implementation Checklist](#6-implementation-checklist)
- [7. Definition of Done](#7-definition-of-done)

---

## 1. Objective
Implement a Kafka consumer for topic `library-events` that:
- Inserts `LibraryEvent` + `Book` for `ADD`
- Updates existing `LibraryEvent` for `UPDATE`
- Handles validation, retry, and failure routing based on `docs/PRD.md`

## 2. Planning Assumptions
- Input messages are JSON payloads with `eventType`, `libraryEventId` (for `UPDATE`), and `book`.
- PostgreSQL is the persistence store.
- Spring Kafka + Spring Data JPA are used.
- Error handling options from PRD section 11 are in scope (retry + DLT recommended).

## 3. Execution-Order Implementation Roadmap

> **Guiding principle:** Build the consumer-first pipeline incrementally — get messages
> flowing, then deserialize them, then persist, then add business rules. Each step
> produces a runnable, testable application.

### Step 1: Kafka Consumer + Configuration ✦ START HERE
Path: `src/main/java/com/learnkafka/consumer`, `src/main/java/com/learnkafka/config`, `src/main/resources/application.yml`

#### Goal
Stand up a working Kafka listener that reads raw messages from `library-events` and logs them. No deserialization, no DB — just prove connectivity.

#### Modules
- `LibraryEventsConsumer`
- `LibraryEventsConsumerConfig` (basic factory only)
- Kafka consumer properties in `application.yml`

#### Tasks
1. Configure Kafka consumer properties in `application.yml`:
   - `spring.kafka.consumer.bootstrap-servers`
   - `spring.kafka.consumer.group-id=library-events-listener-group`
   - `spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.IntegerDeserializer`
   - `spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer`
   - `spring.kafka.consumer.auto-offset-reset=latest`
2. Create `LibraryEventsConsumerConfig` with a `ConcurrentKafkaListenerContainerFactory` bean (default error handler for now).
3. Create `LibraryEventsConsumer` class annotated with `@Component`.
4. Add `@KafkaListener(topics = "library-events")` method.
5. Accept message as `ConsumerRecord<Integer, String>`.
6. Log full Kafka metadata: topic, partition, offset, key, value.
7. **No service delegation yet** — the listener just logs the raw payload.

#### Deliverables
- A running consumer that connects to Kafka and logs every message from `library-events`.
- Kafka consumer properties externalized.
- Basic container factory configuration.

#### Acceptance Criteria
- Application starts without errors and joins the consumer group.
- Publishing a test message to `library-events` produces a log line with topic, partition, offset, key, and value.
- No DB or DTO code is required at this stage.

---

### Step 2: DTO + Deserialization
Path: `src/main/java/com/learnkafka/dto`

#### Goal
Deserialize the raw JSON string received in Step 1 into typed DTO objects. Validate structure. No persistence yet.

#### Modules
- `LibraryEventDto` (Java record)
- `BookDto` (Java record)

#### Tasks
1. Create `BookDto` record with fields: `bookId` (Integer), `bookName` (String), `bookAuthor` (String).
2. Create `LibraryEventDto` record with fields: `libraryEventId` (Integer), `eventType` (EventType — reuse existing enum from domain), `book` (BookDto).
3. Keep DTOs free of JPA annotations — strict separation from persistence model.
4. Add basic bean validation annotations on DTOs (lightweight, not full business rules):
   - `@NotNull` on `eventType` and `book` in `LibraryEventDto`.
   - `@NotBlank` on `bookName`, `bookAuthor` in `BookDto`.
   - `@NotNull` on `bookId` in `BookDto`.
5. Create a stub `LibraryEventService` with method `processEvent(ConsumerRecord<Integer, String>)` that:
   - Deserializes JSON value to `LibraryEventDto` using `ObjectMapper`.
   - Logs the deserialized DTO.
   - Does **not** persist anything yet.
6. Update `LibraryEventsConsumer` to delegate to `LibraryEventService.processEvent()`.

#### Deliverables
- Typed DTO records that deserialize from Kafka JSON payloads.
- Consumer now delegates to service; service deserializes and logs.
- Bean validation annotations ready for later enforcement.

#### Acceptance Criteria
- Sending a valid JSON message to `library-events` produces a log line showing the deserialized `LibraryEventDto`.
- Malformed JSON causes a `JsonProcessingException` (logged, not swallowed).
- DTOs have no JPA dependency.

---

### Step 3: Kafka Under the Hood
Path: `docs/3_Kafka_Consumer_Under_the_hood.md`, `src/main/java/com/learnkafka/consumer`, `src/main/java/com/learnkafka/config`

#### Goal
Understand how the Spring Kafka consumer works under the hood before adding advanced behavior.

#### Modules
- Kafka poll loop lifecycle
- Listener container threading and partition assignment
- Rebalance flow and record processing guarantees
- Manual acknowledgment behavior in this project

#### Tasks
1. Study `docs/3_Kafka_Consumer_Under_the_hood.md` and map concepts to current code.
2. Trace how `@KafkaListener` receives records and how container threads are created.
3. Trace record flow from poll loop to listener invocation and exception propagation.
4. Identify integration points for acknowledgment/error handling to be formalized in Step 5 and Step 6.
5. Identify hook points for retry/error handling to be implemented in Step 6.

#### Deliverables
- Clear mental model of consumer lifecycle: poll -> dispatch -> process -> error handling.
- Project-specific notes linking Kafka internals to `LibraryEventsConsumer` and config.

#### Acceptance Criteria
- Team can explain partition assignment, rebalance impact, and listener dispatch semantics in this codebase.
- Ownership of manual acknowledgment/commit verification is deferred to Step 5.

---

### Step 4: StringDeserializer vs JsonDeserializer
Path: `docs/4_STRING_VS_JSON_DESERIALIZER.md`, `src/main/resources/application.yml`, `src/test/resources/application.yml`

#### Goal
Decide and implement the right deserializer strategy for this consumer (`JsonDeserializer` with DTO mapping) and understand trade-offs vs `StringDeserializer`.

#### Modules
- `StringDeserializer` flow (raw payload)
- `JsonDeserializer` flow (typed DTO)
- Trusted packages and type mapping
- Producer/consumer class-name mismatch handling

#### Tasks
1. Review `docs/4_STRING_VS_JSON_DESERIALIZER.md` and compare both deserializer approaches.
2. Keep `IntegerDeserializer` for keys and `JsonDeserializer` for values in consumer config.
3. Validate JSON deserializer properties in `application.yml`:
   - `spring.json.trusted.packages`
   - `spring.json.value.default.type`
   - `spring.json.type.mapping`
4. Verify test profile keeps equivalent deserializer settings.
5. Capture migration note: why project moved from `ConsumerRecord<Integer, String>` to `ConsumerRecord<Integer, LibraryEventDto>`.

#### Deliverables
- Finalized deserializer strategy decision for project standards.
- Working consumer deserialization into `LibraryEventDto`.

#### Acceptance Criteria
- Valid JSON payloads deserialize into DTOs without manual `ObjectMapper` parsing in listener code.
- Type mapping handles producer type headers correctly.

---

### Step 5: Consumer Groups and Consumer Offset Management
Path: `docs/5_CONSUMER_CONCEPTS_HANDS_ON.md`, `src/main/java/com/learnkafka/config`, `src/main/resources/application.yml`, `src/test/resources/application.yml`

#### Goal
Configure and validate consumer-group behavior and offset management so the consumer is predictable across restarts, failures, and scale-out.

#### Modules
- Consumer group ID and partition ownership
- `auto-offset-reset` (`latest` vs `earliest`)
- Manual acknowledgment and commit timing
- Restart/replay behavior

#### Tasks
1. Review `docs/5_CONSUMER_CONCEPTS_HANDS_ON.md` and map concepts to project config.
2. Confirm `spring.kafka.consumer.group-id` strategy for local and test environments.
3. Keep `auto-offset-reset=latest` for app runtime and override to `earliest` in integration tests where needed.
4. Verify manual commit semantics with `AckMode.MANUAL` and explicit `acknowledge()` call.
5. Document expected behavior for:
   - app restart
   - new consumer joining same group
   - rebalance while processing

#### Deliverables
- Group/offset policy documented and implemented in config.
- Predictable commit behavior for normal and test flows.

#### Acceptance Criteria
- Consumer joins group and claims partitions as expected.
- Offset behavior is understood and validated for both `latest` and `earliest` scenarios.

---

### Step 6: Tasks - Business Logic
Path: `src/main/java/com/learnkafka/service`, `src/main/java/com/learnkafka/dto`, `src/main/java/com/learnkafka/config`, `src/main/resources/db/migration`

#### Goal
Add full business logic: `ADD`/`UPDATE` branching, conditional validation, exception classification, and Kafka error handling with retry + DLT. Any schema changes required for new business rules are delivered as new Flyway versioned migrations.

> **Schema change rule:** If this step requires new columns, indexes, or constraints, create a new Flyway migration (e.g., `V3__add_status_column.sql`). Never modify existing migrations (`V1`, `V2`) and never use `ddl-auto: create/update`.

#### Modules
- `LibraryEventService` (full implementation)
- `LibraryEventMapper` (add `updateEntity()`)
- `LibraryEventsConsumerConfig` (error handler + retry + DLT)
- New Flyway migrations (if schema changes are needed for business logic)

#### Tasks — Schema Changes (if needed)
1. If new columns or tables are required (e.g., a `status` column, a `failed_event` table for custom recovery):
   - Create a new migration file: `src/main/resources/db/migration/V{N}__{description}.sql`.
   - Check existing migrations to determine the next version number (currently `V2` is the latest).
   - **Never edit** `V1__init_schema.sql` or `V2__add_audit_columns.sql` — they are already applied.
   - Update JPA entities to match the new schema (add fields, getters, setters, `@Column` annotations).
   - Verify Flyway applies the migration on startup before testing.

#### Tasks — Business Logic
2. Implement event-type branching in `LibraryEventService.processEvent()`:
   - `ADD`: map DTO to new entity, persist via repository (persistence layer already in place).
   - `UPDATE`: fetch existing `LibraryEvent` by ID; apply updates from DTO via mapper; save.
3. Add `updateEntity(LibraryEventDto dto, LibraryEvent existing)` to `LibraryEventMapper`.
4. Implement update-not-found policy: throw `IllegalArgumentException` with descriptive message.

#### Tasks — Validation
5. Enforce conditional validations in service:
   - `UPDATE` requires non-null `libraryEventId` → reject with `IllegalArgumentException`.
   - `book` must be present for both `ADD` and `UPDATE`.
6. Validate DTO using bean validation (`Validator`) or manual checks in service.
7. Classify exceptions:
   - **Non-retryable:** `IllegalArgumentException`, `JsonProcessingException` (bad data, will never succeed).
   - **Retryable:** all others (transient DB errors, network issues).

#### Tasks — Error Handling & Retry
8. Update `LibraryEventsConsumerConfig`:
   - Configure `DefaultErrorHandler` with `FixedBackOff` or `ExponentialBackOff` (3 attempts, `1s`/`2s`/`4s`).
   - Register non-retryable exception classes.
   - Configure `DeadLetterPublishingRecoverer` for DLT routing to `library-events.DLT`.
   - Ensure offset commits only after success or DLT handoff.
9. Ensure `@Transactional` boundaries prevent partial writes on failure.
10. *(Optional)* If persisting failed events to a `failed_event` table for custom recovery:
    - Create `V3__create_failed_event_table.sql` with columns: `id`, `topic`, `partition`, `offset_val`, `key`, `value`, `error_message`, `status`, `created_at`.
    - Create `FailedEvent` entity + `FailedEventRepository`.
    - Implement `ConsumerRecordRecoverer` that persists to this table.

#### Deliverables
- Full `ADD` + `UPDATE` service implementation.
- Conditional validation with deterministic rejection.
- Exception classification driving retry vs DLT behavior.
- Error handler with backoff, retry, and dead-letter routing.
- Any new Flyway migrations for schema changes required by business logic.

#### Acceptance Criteria
- `ADD` event inserts `LibraryEvent` + `Book` in DB.
- `UPDATE` event with valid ID updates existing record.
- `UPDATE` event with non-existent ID is rejected (non-retryable).
- `UPDATE` event with null `libraryEventId` is rejected (non-retryable).
- Malformed JSON is rejected (non-retryable → DLT).
- Transient DB failure triggers retry with backoff.
- Exhausted retries route to `library-events.DLT`.
- Any new schema changes are delivered as Flyway migrations (not Hibernate DDL).
- `flyway_schema_history` table shows all migrations applied in order.

---

### Step 7: Integration Test to Ensure Save is Working
Path: `src/test/java/com/learnkafka/consumer`, `src/test/java/com/learnkafka/service`

#### Goal
Verify that the save flow works end-to-end and at service level.

#### Tasks
1. Add/maintain consumer integration tests (Embedded Kafka + Testcontainers PostgreSQL):
   - produce `ADD` event to `library-events`
   - assert `LibraryEvent` + `Book` persisted with FK and audit fields
2. Add/maintain service integration tests (no Kafka broker):
   - build `ConsumerRecord<Integer, LibraryEventDto>` directly
   - call `libraryEventService.processEvent()` and assert DB state
3. Ensure test cleanup order in `@BeforeEach`:
   - delete `bookRepository` first, then `libraryEventRepository`
4. Keep test Flyway config active (`ddl-auto: none`, Flyway enabled).

#### Deliverables
- Integration tests that prove save path correctness.
- Stable repeatable test setup with Flyway-managed schema.

#### Acceptance Criteria
- Tests confirm parent/child rows are persisted correctly for `ADD` flow.
- Tests fail on broken mapping/order/FK behavior.

---

## 4. Testing Strategy

### 4.1 Unit Tests
Path: `src/test/java/com/learnkafka/service`

- `ADD` event → successful insert
- `UPDATE` event → successful update
- `UPDATE` event → not found behavior
- Missing `book` → validation failure
- Missing `libraryEventId` on `UPDATE` → validation failure
- Exception classification (retryable vs non-retryable)

### 4.2 Integration Tests
Path: `src/test/java/com/learnkafka/consumer`

- Consume `ADD` from Kafka → persisted in DB
- Consume `UPDATE` from Kafka → updated in DB
- Invalid payload → routed to error flow (DLT/logged)
- DB transient failure → retry policy triggered

### 4.3 Repository Tests
Path: `src/test/java/com/learnkafka/repository`

- Save `LibraryEvent` with `Book` → both persisted
- Find by ID → returns correct entity
- Update existing entity → fields updated
- Cascade behavior validated

### 4.4 Minimum Acceptance Test Matrix
| # | Scenario | Expected Outcome |
|---|----------|-----------------|
| 1 | Valid `ADD` event | `LibraryEvent` + `Book` inserted in DB |
| 2 | Valid `UPDATE` (existing ID) | `LibraryEvent` updated in DB |
| 3 | `UPDATE` with non-existent ID | Reject + log error |
| 4 | Invalid/malformed payload | Non-retryable error path |
| 5 | DB transient error | Retries then success or DLT |
| 6 | Duplicate `ADD` | Policy-defined behavior |

---

## 5. Execution Sequence Summary

| Step | What | Key Outcome |
|------|------|-------------|
| **1** | **Kafka Consumer + Config** | Raw messages logged from `library-events` topic |
| **2** | **DTO + Deserialization** | JSON → typed `LibraryEventDto`; consumer delegates to service |
| **3** | **Kafka Under the Hood** | Consumer internals understood: poll loop, rebalance, listener dispatch |
| **4** | **StringDeserializer vs JsonDeserializer** | Deserializer strategy finalized and DTO deserialization validated |
| **5** | **Consumer Groups and Consumer Offset Management** | Group behavior and offset semantics configured and validated |
| **6** | **Tasks - Business Logic** | ADD/UPDATE branching, validation, retry, DLT |
| **7** | **Integration Test to Ensure Save is Working** | Save path verified with integration tests |

> **Rationale:** This outside-in order lets you verify each layer independently.
> Step 1 proves Kafka connectivity. Step 2 proves deserialization. Step 3 proves
> Kafka internals. Step 4 proves deserializer strategy. Step 5 proves group/offset
> behavior. Step 6 adds business rules. Step 7 locks in save behavior via integration tests.

---

## 6. Implementation Checklist

### Step 1: Kafka Consumer + Configuration
- [ ] Configure Kafka consumer properties (`bootstrap-servers`, `group-id`, deserializers, `auto-offset-reset`)
- [ ] Create `LibraryEventsConsumerConfig` with container factory bean
- [ ] Create `LibraryEventsConsumer` with `@KafkaListener`
- [ ] Log raw `ConsumerRecord` metadata (topic, partition, offset, key, value)
- [ ] Verify consumer joins group and receives messages

### Step 2: DTO + Deserialization
- [x] Create `BookDto` record ✅
- [x] Create `LibraryEventDto` record ✅
- [x] Add bean validation annotations on DTOs ✅
- [x] Create stub `LibraryEventService.processEvent()` — deserialize + log ✅
- [x] Update consumer to delegate to service ✅
- [ ] Verify deserialization of valid JSON payloads

### Step 3: Kafka Under the Hood
- [ ] Read `docs/3_Kafka_Consumer_Under_the_hood.md`
- [ ] Map poll/dispatch/rebalance concepts to `LibraryEventsConsumer` and container config
- [ ] Map listener invocation flow and exception propagation hooks in current code
- [ ] Identify hook points for retry/error handling

### Step 4: StringDeserializer vs JsonDeserializer
- [ ] Read `docs/4_STRING_VS_JSON_DESERIALIZER.md`
- [ ] Validate `JsonDeserializer` config in `application.yml`
- [ ] Validate test deserializer config in `src/test/resources/application.yml`
- [ ] Document why DTO-based `JsonDeserializer` is preferred in this project

### Step 5: Consumer Groups and Consumer Offset Management
- [ ] Read `docs/5_CONSUMER_CONCEPTS_HANDS_ON.md`
- [ ] Validate `group-id` strategy and environment overrides
- [ ] Validate `auto-offset-reset` usage (`latest` app, `earliest` integration tests)
- [ ] Verify manual acknowledgment and offset commit timing
- [ ] Document restart/rebalance behavior expectations

### Step 6: Tasks - Business Logic
- [ ] Create new Flyway migration(s) if schema changes are needed (e.g., `V3__create_failed_event_table.sql`)
- [ ] **Never edit** existing migrations (`V1`, `V2`) — only add new versioned files
- [ ] Implement `UPDATE` path in service (fetch → update → save)
- [ ] Add `updateEntity()` to `LibraryEventMapper`
- [ ] Add conditional validations (`libraryEventId` required for `UPDATE`, `book` required always)
- [ ] Implement update-not-found policy (reject + log)
- [ ] Classify exceptions (retryable vs non-retryable)
- [ ] Configure retry/backoff in `LibraryEventsConsumerConfig`
- [ ] Configure `DeadLetterPublishingRecoverer` for DLT routing
- [ ] *(Optional)* Create `FailedEvent` entity + `FailedEventRepository` backed by `V3` migration
- [ ] Finalize decision table options from `docs/PRD.md` section 11.10

### Step 7: Integration Test to Ensure Save is Working
- [ ] Add/verify consumer integration test for `ADD` save flow (Embedded Kafka + Testcontainers)
- [ ] Add/verify service integration test for `processEvent()` save flow (manual `ConsumerRecord`)
- [ ] Assert `LibraryEvent` + `Book` row counts and FK relationship
- [ ] Assert audit columns are populated (`createdAt`, `updatedAt`)
- [ ] Keep cleanup order in `@BeforeEach` (book first, then library_event)

### Post-Implementation
- [ ] Add unit tests for service
- [ ] Add integration tests for consumer
- [ ] Add repository tests
- [ ] Add structured logging and metrics
- [ ] Document replay/runbook basics

## 7. Definition of Done
- Consumer reads from `library-events` topic.
- `ADD` inserts `LibraryEvent` + `Book` into PostgreSQL.
- `UPDATE` updates existing event based on agreed not-found policy.
- Error handling path is implemented for invalid events and DB issues.
- Tests pass for core success and failure scenarios.
- Operational guidance exists for retries, DLT, and replay.

