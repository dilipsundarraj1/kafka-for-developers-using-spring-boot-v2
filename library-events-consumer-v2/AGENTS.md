# AGENTS.md

## Project Overview

Spring Boot 4.0 / Java 25 Kafka consumer that listens to topic `library-events`, deserializes JSON into DTOs, maps to JPA entities, and persists to PostgreSQL. Schema managed by Flyway.

## Architecture & Data Flow

```
Kafka topic "library-events"
  → LibraryEventsConsumer (@KafkaListener, MANUAL ack)
    → LibraryEventService.processEvent() (@Transactional)
      → LibraryEventMapper (DTO → Entity, static utility)
        → LibraryEventRepository.save() then BookRepository.save()
```

**Key design decisions:**
- **DTO/Entity separation** — `dto/` package holds Java records (`LibraryEventDto`, `BookDto`) with bean validation; `domain/` holds JPA entities (`LibraryEvent`, `Book`). `LibraryEventMapper` is the manual static bridge; no MapStruct.
- **Bidirectional OneToOne** — `LibraryEvent.book` is `mappedBy`, `Book.libraryEvent` owns the FK. On persist, `LibraryEvent` is saved first (to get the IDENTITY-generated ID), then `Book` is saved with the FK set. The `book` field is temporarily nulled to avoid cascade issues. See `LibraryEventService.processEvent()`.
- **Book PK is producer-provided** (`@Id`, no `@GeneratedValue`); `LibraryEvent` PK is DB-generated (`IDENTITY`).
- **Kafka deserialization** uses Spring's `JsonDeserializer` with type mapping configured in `application.yml` — the producer sends `com.learnkafka.domain.LibraryEvent` but it's remapped to `com.learnkafka.dto.LibraryEventDto` on this consumer side (`spring.json.type.mapping`).
- **Manual offset commit** — `AckMode.MANUAL` in `LibraryEventsConsumerConfig`; consumer calls `acknowledgment.acknowledge()` in a `finally` block.

## Build & Test Commands

```bash
./gradlew build          # compile + test
./gradlew test           # tests only (Testcontainers auto-start PostgreSQL)
./gradlew bootRun        # run app (needs local Kafka + Postgres via compose.yaml)
docker compose up -d     # start PostgreSQL for local dev (Kafka must be external)
```

## Testing Patterns

- Integration tests use **Testcontainers** with `@SpringBootTest` + `@ImportTestcontainers`. A static `PostgreSQLContainer` with `@ServiceConnection` auto-configures the datasource — no manual URL wiring needed.
- Tests bypass Kafka entirely: they construct `ConsumerRecord<Integer, LibraryEventDto>` directly and call `libraryEventService.processEvent()`. See `LibraryEventServiceIntegrationTest.buildConsumerRecord()`.
- `@BeforeEach` deletes `bookRepository` first, then `libraryEventRepository` (FK order matters).
- Test `application.yml` sets `server.port: 0`, disables Flyway clean protection (`clean-disabled: false`), and keeps `ddl-auto: none` (Flyway owns schema).

## Schema Management

Flyway migrations live in `src/main/resources/db/migration/`. JPA `ddl-auto` is `none` — **never use `create` or `update`**; all schema changes must be new versioned migrations (`V3__description.sql`, etc.).

## Key Conventions

- **No Lombok** — entities use explicit constructors, getters, setters; DTOs are Java `record` types.
- **Audit columns** (`createdAt`, `updatedAt`) managed via JPA `@PrePersist`/`@PreUpdate` callbacks on each entity.
- **Logging** uses SLF4J (`LoggerFactory.getLogger`) — not `@Slf4j` annotation.
- **Constructor injection** everywhere (no `@Autowired` on fields).
- **Event types**: `LibraryEventType.ADD` and `LibraryEventType.UPDATE`. The DTO field is named `libraryEventType` while the entity field is `eventType`.
- Design docs live in `docs/` — consult `1_PRD.md` for requirements and `2_IMPLEMENTATION_PLAN.md` for phased delivery context.

