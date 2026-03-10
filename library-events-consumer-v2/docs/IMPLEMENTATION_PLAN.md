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

## 3. Layer-wise Implementation Roadmap

### Layer 1: Domain Model
Path: `src/main/java/com/learnkafka/domain`

#### Modules
- `EventType` enum
- `Book` entity
- `LibraryEvent` entity

#### Tasks
1. Create `EventType` enum with values `ADD`, `UPDATE`.
2. Create `Book` entity with fields: `bookId` (PK), `bookName`, `bookAuthor`.
3. Create `LibraryEvent` entity with fields: `libraryEventId` (PK), `eventType`, `book` (relationship).
4. Add JPA annotations (`@Entity`, `@Id`, `@GeneratedValue`, `@OneToOne`, `@Enumerated`).
5. Add field-level constraints (`@NotNull`, `@NotBlank` where applicable).
6. Define cascade strategy for `LibraryEvent` -> `Book` relationship.
7. Decide ID generation strategy (`GenerationType.IDENTITY` vs producer-provided).

#### Deliverables
- Persistable domain model with correct JPA mappings.
- Cascade and relationship behavior validated.

#### Acceptance Criteria
- Entities compile and map to expected DB schema.
- Relationship between `LibraryEvent` and `Book` is correctly defined.

---

### Layer 2: DTO and Mapping
Path: `src/main/java/com/learnkafka/dto`

#### Modules
- `LibraryEventDto`
- `BookDto`
- `LibraryEventMapper`

#### Tasks
1. Create `LibraryEventDto` record/class with fields: `libraryEventId`, `eventType`, `book`.
2. Create `BookDto` record/class with fields: `bookId`, `bookName`, `bookAuthor`.
3. Add bean validation annotations on DTOs:
   - `@NotNull` on `eventType` and `book`.
   - `@NotBlank` on `bookName`, `bookAuthor`.
   - `@NotNull` on `libraryEventId` only enforced conditionally for `UPDATE` (validated in service).
4. Create `LibraryEventMapper` utility to convert:
   - `LibraryEventDto` -> `LibraryEvent` entity (for `ADD`).
   - `LibraryEventDto` -> update fields on existing `LibraryEvent` entity (for `UPDATE`).
5. Keep DTOs free of JPA annotations — strict separation from persistence model.

#### Deliverables
- Inbound payload DTOs decoupled from JPA entities.
- Mapper with clear `toEntity()` and `updateEntity()` methods.
- Bean validation annotations for early input rejection.

#### Acceptance Criteria
- DTOs deserialize correctly from Kafka JSON payloads.
- Mapper produces valid entities for both `ADD` and `UPDATE` flows.
- Validation annotations reject missing/invalid fields before service logic runs.

---

### Layer 3: Repository
Path: `src/main/java/com/learnkafka/repository`

#### Modules
- `LibraryEventRepository`
- `BookRepository` (if explicit access needed)

#### Tasks
1. Create `LibraryEventRepository extends JpaRepository<LibraryEvent, Integer>`.
2. Create `BookRepository extends JpaRepository<Book, Integer>` (optional, for direct book queries).
3. Add custom query methods if needed (e.g., existence check by ID).
4. Add indexes/unique constraints for idempotency strategy.

#### Deliverables
- Repository interfaces supporting all service-layer use cases.
- DB schema aligned to entity model.

#### Acceptance Criteria
- CRUD operations work for `LibraryEvent` and `Book`.
- Save, find-by-ID, and update flows validated via repository tests.

---

### Layer 4: Kafka Consumer
Path: `src/main/java/com/learnkafka/consumer`

#### Modules
- `LibraryEventsConsumer`

#### Tasks
1. Create `LibraryEventsConsumer` class annotated with `@Component`.
2. Add `@KafkaListener(topics = "library-events")` method.
3. Accept message payload as `ConsumerRecord<Integer, String>`.
4. Extract and log Kafka metadata: topic, partition, offset, key.
5. Delegate processing to `LibraryEventService.processEvent()`.
6. Keep listener thin — no business logic inside.
7. Wire listener error flow to central error handler config.
8. Ensure commit/ack behavior follows success or DLT handoff policy.

#### Deliverables
- Listener class with single responsibility: receive + delegate.
- Structured logs for consumption entry points.
- Integration hook to service contract (`processEvent`).

#### Risks
- Tight coupling if listener performs domain logic.
- Rework if ack mode is finalized before retry/DLT decisions.

#### Acceptance Criteria
- Messages from `library-events` are consumed.
- Listener forwards payload to service with metadata context.
- Listener-level error paths are tested.

---

### Layer 5: Service
Path: `src/main/java/com/learnkafka/service`

#### Modules
- `LibraryEventService`

#### Tasks
1. Define service method: `processEvent(ConsumerRecord<Integer, String> consumerRecord)`.
2. Deserialize JSON payload to `LibraryEventDto` using `ObjectMapper`.
3. Validate DTO (bean validation + conditional checks).
4. Map DTO to entity using `LibraryEventMapper`.
5. Implement event-type branch:
   - `ADD`: map DTO to new entity, persist `LibraryEvent` + `Book` transactionally via repository.
   - `UPDATE`: fetch existing `LibraryEvent` by ID; apply updates from DTO via mapper; save.
4. Add conditional validations:
   - `UPDATE` requires non-null `libraryEventId`.
   - `book` must be present for both `ADD` and `UPDATE`.
5. Implement update-not-found policy (reject + log error as default).
6. Add `@Transactional` boundaries to prevent partial writes.
7. Classify exceptions into retryable vs non-retryable categories.

#### Deliverables
- Service implementation with deterministic behavior for `ADD` / `UPDATE`.
- Validation and business exceptions.
- Transaction-safe persistence orchestration.

#### Risks
- Ambiguous policy for missing `UPDATE` target can cause inconsistent behavior.
- Mixing parsing + persistence in same method can reduce testability.

#### Acceptance Criteria
- `ADD` and `UPDATE` paths pass unit tests.
- Not-found and invalid payload behaviors are deterministic.
- Exception classification is usable by Kafka error handler.

---

### Layer 6: JPA Configuration and Wiring
Path: `src/main/resources/application.properties`

#### Tasks
1. Configure datasource URL, username, password for PostgreSQL.
2. Set JPA DDL auto mode (`create`, `update`, or `validate` depending on environment).
3. Enable SQL logging for development/debug.
4. Set Hibernate dialect for PostgreSQL.
5. Verify entity scan picks up `com.learnkafka.domain` package.

#### Deliverables
- Working DB connectivity from app to PostgreSQL.
- Entities auto-create or validate against DB schema.

#### Acceptance Criteria
- App starts and connects to DB without errors.
- `LibraryEvent` and `Book` tables are created/validated on startup.

---

## 4. Cross-Cutting Implementation

### 4.1 Error Handling and Retry
Path: `src/main/java/com/learnkafka/config`

#### Modules
- `LibraryEventsConsumerConfig`

#### Tasks
1. Configure `ConcurrentKafkaListenerContainerFactory` with custom error handler.
2. Use `DefaultErrorHandler` with `FixedBackOff` or `ExponentialBackOff`.
3. Set retry policy: 3 attempts, exponential backoff (`1s`, `2s`, `4s`) with jitter.
4. Mark non-retryable exceptions:
   - `IllegalArgumentException` (validation failures)
   - `JsonProcessingException` (deserialization errors)
5. Configure `DeadLetterPublishingRecoverer` for DLT routing to `library-events.DLT`.
6. Ensure offset commits only after success or DLT handoff.

### 4.2 Kafka Consumer Configuration
Path: `src/main/resources/application.properties`

#### Tasks
1. Set bootstrap servers.
2. Set consumer group ID.
3. Configure key/value deserializers (`IntegerDeserializer`, `StringDeserializer`).
4. Set auto-offset-reset policy (`latest` or `earliest`).
5. Set listener concurrency.
6. Configure ack mode (`MANUAL` if needed, or rely on default).

### 4.3 Observability
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

- `ADD` event -> successful insert
- `UPDATE` event -> successful update
- `UPDATE` event -> not found behavior
- Missing `book` -> validation failure
- Missing `libraryEventId` on `UPDATE` -> validation failure
- Exception classification (retryable vs non-retryable)

### 5.2 Integration Tests
Path: `src/test/java/com/learnkafka/consumer`

- Consume `ADD` from Kafka -> persisted in DB
- Consume `UPDATE` from Kafka -> updated in DB
- Invalid payload -> routed to error flow (DLT/logged)
- DB transient failure -> retry policy triggered

### 5.3 Repository Tests
Path: `src/test/java/com/learnkafka/repository`

- Save `LibraryEvent` with `Book` -> both persisted
- Find by ID -> returns correct entity
- Update existing entity -> fields updated
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

## 6. Recommended Execution Sequence
Even with the chosen layering, this sequence reduces rework:

| Step | Layer | Reason |
|------|-------|--------|
| 1 | Domain Model (entities + enum) | Foundation for all other layers |
| 2 | DTO + Mapping (DTOs, mapper, validation) | Decouple Kafka payload from JPA entities |
| 3 | Repository | Persistence contracts before business logic |
| 4 | Service (`ADD` then `UPDATE`) | Real transactional logic with working DB |
| 5 | Kafka Consumer | Thin listener delegates to proven service |
| 6 | Error Handler + Retry + DLT | Wired after exception types are known |
| 7 | Configuration hardening | Externalize properties, tune concurrency |
| 8 | Unit + Integration Tests | Validate all paths |
| 9 | Observability + Runbook | Production readiness |

---

## 7. Implementation Checklist
- [ ] Finalize decision table options from `docs/PRD.md` section 11.10
- [ ] Create `EventType` enum
- [ ] Create `Book` entity
- [ ] Create `LibraryEvent` entity with `Book` relationship
- [ ] Create `LibraryEventDto` and `BookDto`
- [ ] Create `LibraryEventMapper` (toEntity + updateEntity)
- [ ] Add bean validation annotations on DTOs
- [ ] Create `LibraryEventRepository`
- [ ] Create `BookRepository` (if needed)
- [ ] Implement `LibraryEventService.processEvent()`
- [ ] Implement `ADD` path in service
- [ ] Implement `UPDATE` path in service
- [ ] Add validation and exception classification
- [ ] Create `LibraryEventsConsumer` with `@KafkaListener`
- [ ] Configure retry/backoff and DLT in `LibraryEventsConsumerConfig`
- [ ] Configure Kafka consumer properties
- [ ] Configure JPA/datasource properties
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

