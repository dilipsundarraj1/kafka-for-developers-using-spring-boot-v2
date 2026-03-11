# Implementation Plan
## Library Events Consumer

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
Path: `src/main/java/com/learnkafka/consumer`, `src/main/java/com/learnkafka/config`, `src/main/resources/application.properties`

#### Goal
Stand up a working Kafka listener that reads raw messages from `library-events` and logs them. No deserialization, no DB — just prove connectivity.

#### Modules
- `LibraryEventsConsumer`
- `LibraryEventsConsumerConfig` (basic factory only)
- Kafka consumer properties in `application.properties`

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

### Step 3: Domain Model + Repository + DB Persistence
Path: `src/main/java/com/learnkafka/domain`, `src/main/java/com/learnkafka/repository`, `src/main/java/com/learnkafka/dto`, `src/main/resources/application.properties`

#### Goal
Wire persistence end-to-end: map DTOs to JPA entities and save them to PostgreSQL. Initially handle only the `ADD` event type (simple insert).

#### Pre-existing (from Layer 1)
- `EventType` enum — ✅ already created
- `Book` entity — ✅ already created
- `LibraryEvent` entity — ✅ already created

#### Modules
- `LibraryEventRepository`
- `LibraryEventMapper`
- JPA/datasource configuration in `application.properties`

#### Tasks
1. Configure JPA/datasource properties in `application.properties`:
   - `spring.datasource.url`, `spring.datasource.username`, `spring.datasource.password`
   - `spring.jpa.hibernate.ddl-auto=update`
   - `spring.jpa.show-sql=true` (development)
   - `spring.jpa.properties.hibernate.format_sql=true`
2. Create `LibraryEventRepository extends JpaRepository<LibraryEvent, Integer>`.
3. Create `LibraryEventMapper` utility class with:
   - `toEntity(LibraryEventDto dto)` → new `LibraryEvent` + `Book` entities.
   - `toBookEntity(BookDto dto)` → new `Book` entity.
4. Update `LibraryEventService.processEvent()` to:
   - Deserialize JSON to `LibraryEventDto` (already done in Step 2).
   - Map DTO → entity via `LibraryEventMapper.toEntity()`.
   - Save entity via `LibraryEventRepository.save()`.
   - Add `@Transactional` annotation.
5. Verify cascade: saving `LibraryEvent` also persists `Book` (via `CascadeType.ALL`).
6. Verify entity scan picks up `com.learnkafka.domain` package.

#### Deliverables
- End-to-end flow: Kafka message → DTO → Entity → PostgreSQL.
- Repository interface for `LibraryEvent`.
- Mapper with `toEntity()` method.
- Working DB connectivity.

#### Acceptance Criteria
- Sending an `ADD` event to `library-events` inserts both `LibraryEvent` and `Book` rows in PostgreSQL.
- `LibraryEvent` and `Book` tables are auto-created on startup.
- App starts and connects to DB without errors.

---

### Step 4: Business Logic, Validation, and Error Handling
Path: `src/main/java/com/learnkafka/service`, `src/main/java/com/learnkafka/dto`, `src/main/java/com/learnkafka/config`

#### Goal
Add full business logic: `ADD`/`UPDATE` branching, conditional validation, exception classification, and Kafka error handling with retry + DLT.

#### Modules
- `LibraryEventService` (full implementation)
- `LibraryEventMapper` (add `updateEntity()`)
- `LibraryEventsConsumerConfig` (error handler + retry + DLT)

#### Tasks — Business Logic
1. Implement event-type branching in `LibraryEventService.processEvent()`:
   - `ADD`: map DTO to new entity, persist via repository (already working from Step 3).
   - `UPDATE`: fetch existing `LibraryEvent` by ID; apply updates from DTO via mapper; save.
2. Add `updateEntity(LibraryEventDto dto, LibraryEvent existing)` to `LibraryEventMapper`.
3. Implement update-not-found policy: throw `IllegalArgumentException` with descriptive message.

#### Tasks — Validation
4. Enforce conditional validations in service:
   - `UPDATE` requires non-null `libraryEventId` → reject with `IllegalArgumentException`.
   - `book` must be present for both `ADD` and `UPDATE`.
5. Validate DTO using bean validation (`Validator`) or manual checks in service.
6. Classify exceptions:
   - **Non-retryable:** `IllegalArgumentException`, `JsonProcessingException` (bad data, will never succeed).
   - **Retryable:** all others (transient DB errors, network issues).

#### Tasks — Error Handling & Retry
7. Update `LibraryEventsConsumerConfig`:
   - Configure `DefaultErrorHandler` with `FixedBackOff` or `ExponentialBackOff` (3 attempts, `1s`/`2s`/`4s`).
   - Register non-retryable exception classes.
   - Configure `DeadLetterPublishingRecoverer` for DLT routing to `library-events.DLT`.
   - Ensure offset commits only after success or DLT handoff.
8. Ensure `@Transactional` boundaries prevent partial writes on failure.

#### Deliverables
- Full `ADD` + `UPDATE` service implementation.
- Conditional validation with deterministic rejection.
- Exception classification driving retry vs DLT behavior.
- Error handler with backoff, retry, and dead-letter routing.

#### Acceptance Criteria
- `ADD` event inserts `LibraryEvent` + `Book` in DB.
- `UPDATE` event with valid ID updates existing record.
- `UPDATE` event with non-existent ID is rejected (non-retryable).
- `UPDATE` event with null `libraryEventId` is rejected (non-retryable).
- Malformed JSON is rejected (non-retryable → DLT).
- Transient DB failure triggers retry with backoff.
- Exhausted retries route to `library-events.DLT`.

---

## 4. Cross-Cutting Implementation

### 4.1 Observability
#### Tasks
1. Add structured logs for event lifecycle (received, processing, persisted, failed).
2. Log Kafka metadata on every event (topic, partition, offset, key).
3. Add metrics (Micrometer) for:
   - processed success/failure count
   - retry attempts
   - DLT publish count
   - processing latency
4. Define alert thresholds (consumer lag, DLT spikes, retry exhaustion).

---

## 5. Testing Strategy

### 5.1 Unit Tests
Path: `src/test/java/com/learnkafka/service`

- `ADD` event → successful insert
- `UPDATE` event → successful update
- `UPDATE` event → not found behavior
- Missing `book` → validation failure
- Missing `libraryEventId` on `UPDATE` → validation failure
- Exception classification (retryable vs non-retryable)

### 5.2 Integration Tests
Path: `src/test/java/com/learnkafka/consumer`

- Consume `ADD` from Kafka → persisted in DB
- Consume `UPDATE` from Kafka → updated in DB
- Invalid payload → routed to error flow (DLT/logged)
- DB transient failure → retry policy triggered

### 5.3 Repository Tests
Path: `src/test/java/com/learnkafka/repository`

- Save `LibraryEvent` with `Book` → both persisted
- Find by ID → returns correct entity
- Update existing entity → fields updated
- Cascade behavior validated

### 5.4 Minimum Acceptance Test Matrix
| # | Scenario | Expected Outcome |
|---|----------|-----------------|
| 1 | Valid `ADD` event | `LibraryEvent` + `Book` inserted in DB |
| 2 | Valid `UPDATE` (existing ID) | `LibraryEvent` updated in DB |
| 3 | `UPDATE` with non-existent ID | Reject + log error |
| 4 | Invalid/malformed payload | Non-retryable error path |
| 5 | DB transient error | Retries then success or DLT |
| 6 | Duplicate `ADD` | Policy-defined behavior |

---

## 6. Execution Sequence Summary

| Step | What | Key Outcome |
|------|------|-------------|
| **1** | **Kafka Consumer + Config** | Raw messages logged from `library-events` topic |
| **2** | **DTO + Deserialization** | JSON → typed `LibraryEventDto`; consumer delegates to service |
| **3** | **Domain Model + Repository + DB Save** | DTO → Entity → PostgreSQL (ADD flow works end-to-end) |
| **4** | **Business Logic + Validation + Error Handling** | ADD/UPDATE branching, validation, retry, DLT |
| 5 | Unit + Integration Tests | Validate all paths |
| 6 | Observability + Runbook | Production readiness |

> **Rationale:** This outside-in order lets you verify each layer independently.
> Step 1 proves Kafka connectivity. Step 2 proves deserialization. Step 3 proves
> persistence. Step 4 adds the business rules on top of a known-working pipeline.

---

## 7. Implementation Checklist

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

### Step 3: Domain Model + Repository + DB Save
- [x] Create `EventType` enum ✅
- [x] Create `Book` entity ✅
- [x] Create `LibraryEvent` entity with `Book` relationship ✅
- [ ] Configure JPA/datasource properties for PostgreSQL
- [ ] Create `LibraryEventRepository`
- [ ] Create `LibraryEventMapper` with `toEntity()` + `toBookEntity()`
- [ ] Update service to map DTO → entity and save via repository
- [ ] Verify `ADD` event persists `LibraryEvent` + `Book` in DB

### Step 4: Business Logic + Validation + Error Handling
- [ ] Implement `UPDATE` path in service (fetch → update → save)
- [ ] Add `updateEntity()` to `LibraryEventMapper`
- [ ] Add conditional validations (`libraryEventId` required for `UPDATE`, `book` required always)
- [ ] Implement update-not-found policy (reject + log)
- [ ] Classify exceptions (retryable vs non-retryable)
- [ ] Configure retry/backoff in `LibraryEventsConsumerConfig`
- [ ] Configure `DeadLetterPublishingRecoverer` for DLT routing
- [ ] Finalize decision table options from `docs/PRD.md` section 11.10

### Post-Implementation
- [ ] Add unit tests for service
- [ ] Add integration tests for consumer
- [ ] Add repository tests
- [ ] Add structured logging and metrics
- [ ] Document replay/runbook basics

## 8. Definition of Done
- Consumer reads from `library-events` topic.
- `ADD` inserts `LibraryEvent` + `Book` into PostgreSQL.
- `UPDATE` updates existing event based on agreed not-found policy.
- Error handling path is implemented for invalid events and DB issues.
- Tests pass for core success and failure scenarios.
- Operational guidance exists for retries, DLT, and replay.

