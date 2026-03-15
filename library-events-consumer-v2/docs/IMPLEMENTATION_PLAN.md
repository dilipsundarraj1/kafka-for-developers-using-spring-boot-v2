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
Path: `src/main/java/com/learnkafka/domain`, `src/main/java/com/learnkafka/repository`, `src/main/java/com/learnkafka/dto`, `src/main/resources/application.yml`

#### Goal
Wire persistence end-to-end: map DTOs to JPA entities and save them to PostgreSQL. Initially handle only the `ADD` event type (simple insert).

#### Pre-existing (from Layer 1)
- `LibraryEventType` enum (renamed from `EventType` to match producer payload) — ✅ already created
- `Book` entity — ✅ already created
- `LibraryEvent` entity — ✅ already created

#### Modules
- `LibraryEventRepository`
- `BookRepository`
- `LibraryEventMapper`
- JPA/datasource configuration in `application.yml`

#### Tasks
1. Configure JPA/datasource properties in `application.yml`:
   - `spring.datasource.url`, `spring.datasource.username`, `spring.datasource.password` (matching `compose.yaml` credentials)
   - `spring.jpa.hibernate.ddl-auto=create` (use `create` on first run to generate correct IDENTITY columns, then switch to `update`)
   - `spring.jpa.show-sql=true` (development)
   - `spring.jpa.properties.hibernate.format_sql=true`
2. Create `LibraryEventRepository extends JpaRepository<LibraryEvent, Integer>`.
3. Create `BookRepository extends JpaRepository<Book, Integer>` — needed because `Book` has a producer-provided ID and must be saved explicitly before `LibraryEvent`.
4. Create `LibraryEventMapper` utility class with:
   - `toEntity(LibraryEventDto dto)` → new `LibraryEvent` + `Book` entities.
   - `toBookEntity(BookDto dto)` → new `Book` entity.
   - **Do NOT set** `book.setLibraryEvent(libraryEvent)` in the mapper — the bidirectional back-reference must be set in the service after both entities are persisted.
5. Update `LibraryEventService.processEvent()` to:
   - Extract `LibraryEventDto` from `ConsumerRecord` (already deserialized by `JsonDeserializer`).
   - Map DTO → entity via `LibraryEventMapper.toEntity()`.
   - Save `LibraryEvent` first via `libraryEventRepository.save()` (DB generates the ID via `@GeneratedValue(IDENTITY)`).
   - Create `Book` entity, set the FK (`book.setLibraryEvent(savedEvent)`), then save via `bookRepository.save()`.
   - Set bidirectional back-reference (`savedEvent.setBook(savedBook)`) for in-memory consistency.
   - Add `@Transactional` annotation.
6. Verify entity scan picks up `com.learnkafka.domain` package.

#### Key Design Decisions (from error fixes)

##### ID Generation Strategy
- **`LibraryEvent.libraryEventId`**: `@Id @GeneratedValue(strategy = GenerationType.IDENTITY)` — producer sends `null` for `ADD` events; DB auto-generates the ID.
- **`Book.bookId`**: `@Id @NotNull` — producer provides the `bookId` (e.g., `1`); no `@GeneratedValue`.

##### Cascade Strategy
- **`LibraryEvent.book`**: `@OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})` — `LibraryEvent` is the **inverse side** (no `@JoinColumn`).
- **`Book.libraryEvent`**: `@OneToOne @JoinColumn(name = "library_event_id")` — `Book` is the **owning side** (holds the FK).
- `CascadeType.ALL` on `LibraryEvent` is now safe because `LibraryEvent` is saved first (gets its DB-generated ID), then `Book` is saved with the FK.
- The FK column `library_event_id` lives in the `book` table.

##### Save Order
- `LibraryEvent` must be saved **before** `Book` because:
  - `LibraryEvent` has `@GeneratedValue(IDENTITY)` with null ID → saved first to get the DB-generated ID.
  - `Book` is the owning side with `@JoinColumn(name = "library_event_id")` → needs the `LibraryEvent` ID to write the FK.
  - `Book` has a manually-assigned ID (`bookId`) → `JpaRepository.save()` calls `merge()`.

##### Bidirectional Relationship
- `Book` is now the **owning side** with `@JoinColumn(name = "library_event_id")` — the FK lives in the `book` table.
- `LibraryEvent` is the **inverse side** with `@OneToOne(mappedBy = "libraryEvent")`.
- The owning side (`book.setLibraryEvent(savedEvent)`) must be set before saving `Book` — this is what writes the FK.
- The inverse side (`savedEvent.setBook(savedBook)`) is set after both are saved for in-memory consistency only.

##### DTO Field Name Mapping
- Producer sends `libraryEventType` in JSON; DTO record component is also named `libraryEventType` (renamed from `eventType` to match producer).
- If the DTO field name differs from the JSON key, use `@JsonProperty("libraryEventType")` on the record component.

##### DDL Auto Strategy
- Use `ddl-auto: create` on the first run to generate tables with correct IDENTITY columns.
- `ddl-auto: update` does **not** alter existing columns to add IDENTITY generation or remove NOT NULL constraints from a prior schema.
- After the first successful run, switch to `ddl-auto: update` to preserve data.

#### Deliverables
- End-to-end flow: Kafka message → `JsonDeserializer` → `LibraryEventDto` → `LibraryEventMapper` → `Book` + `LibraryEvent` entities → PostgreSQL.
- Repository interfaces for both `LibraryEvent` and `Book`.
- Mapper with `toEntity()` and `toBookEntity()` methods (no bidirectional reference setup).
- Working DB connectivity with correct schema.

#### Acceptance Criteria
- Sending an `ADD` event to `library-events` inserts both `LibraryEvent` and `Book` rows in PostgreSQL.
- `LibraryEvent` table uses an auto-generated IDENTITY primary key.
- `Book` table uses the producer-provided `bookId` as the primary key.
- `LibraryEvent` and `Book` tables are created on startup.
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
- [x] Rename `EventType` enum → `LibraryEventType` (match producer payload) ✅
- [x] Update `Book` entity — `@Id @NotNull` on `bookId`, no `@GeneratedValue` (producer-provided ID) ✅
- [x] Update `LibraryEvent` entity — `@Id @GeneratedValue(IDENTITY)` on `libraryEventId` (DB-generated for ADD) ✅
- [x] Update `LibraryEvent` — inverse side with `@OneToOne(mappedBy = "libraryEvent", cascade = ALL)` ✅
- [x] Update `Book` — owning side with `@OneToOne @JoinColumn(name = "library_event_id")` ✅
- [x] Configure JPA/datasource properties in `application.yml` (matching `compose.yaml` credentials) ✅
- [x] Set `ddl-auto: create` for initial schema generation with IDENTITY columns ✅
- [x] Create `LibraryEventRepository` ✅
- [x] Create `BookRepository` (explicit `Book` save needed due to producer-provided ID) ✅
- [x] Create `LibraryEventMapper` with `toEntity()` + `toBookEntity()` (no bidirectional ref in mapper) ✅
- [x] Update `LibraryEventDto` — rename field to `libraryEventType` to match producer JSON ✅
- [x] Update service: save `LibraryEvent` first → save `Book` with FK → set back-reference ✅
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

