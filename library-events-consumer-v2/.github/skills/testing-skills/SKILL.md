---
name: testing
description: Unit and integration testing patterns for this Spring Boot 4.0 Kafka consumer project using JUnit Jupiter, Testcontainers 2.x, EmbeddedKafka, and MockMvc.
---

## About this skill

This project has three categories of tests, all under `src/test/java/com/learnkafka/`. There are no mocks — every test runs against real infrastructure (embedded Kafka broker, Testcontainers PostgreSQL).

## Technology Stack

| Component | Version | Notes |
|---|---|---|
| JUnit Jupiter | 6.x | Via `spring-boot-starter-test` (Spring Boot 4.0) |
| Spring Boot Test | 4.0.3 | `@SpringBootTest`, `@AutoConfigureMockMvc` |
| Testcontainers | 2.x | `@ImportTestcontainers` + `@ServiceConnection` (NOT the old `@Testcontainers`/`@Container`) |
| EmbeddedKafka | (managed) | `@EmbeddedKafka` from `spring-kafka-test` |
| MockMvc | (managed) | Import `AutoConfigureMockMvc` from `org.springframework.boot.webmvc.test.autoconfigure` (Spring Boot 4.0 package) |
| Jackson 3.x | (managed) | `tools.jackson.databind.ObjectMapper` (NOT `com.fasterxml.jackson`) for test JSON serialization |

## Test Dependencies (`build.gradle`)

```groovy
testImplementation 'org.springframework.boot:spring-boot-starter-data-jpa-test'
testImplementation 'org.springframework.boot:spring-boot-starter-kafka-test'
testImplementation 'org.springframework.boot:spring-boot-starter-validation-test'
testImplementation 'org.springframework.boot:spring-boot-starter-webmvc-test'
testImplementation 'org.springframework.boot:spring-boot-testcontainers'
testImplementation 'org.testcontainers:testcontainers-postgresql'
testImplementation 'org.springframework.boot:spring-boot-starter-flyway-test'
testRuntimeOnly    'org.junit.platform:junit-platform-launcher'
```

## Test Configuration

File: `src/test/resources/application.yml`

- `server.port: 0` — random port to avoid conflicts.
- `spring.flyway.clean-disabled: false` — allow Flyway clean in tests.
- `spring.jpa.hibernate.ddl-auto: none` — Flyway owns the schema; never use `create` or `update`.
- **No `spring.datasource.*`** — `@ServiceConnection` auto-wires JDBC URL/username/password from the Testcontainer.
- `spring.kafka.consumer.auto-offset-reset: latest` — overridden to `earliest` in Kafka integration tests via `@TestPropertySource`.

## Test Categories

### 1. Kafka Consumer Integration Tests (`consumer/`)

**File:** `LibraryEventsConsumerIntegrationTest.java`
**Purpose:** End-to-end test — produce to EmbeddedKafka → real consumer picks up → persists to Testcontainers PostgreSQL.

**Annotations:**
```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"library-events"},
        bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers")
@TestPropertySource(properties = {"spring.kafka.consumer.auto-offset-reset=earliest"})
@ImportTestcontainers
```

**Key patterns:**
- Declares `@ServiceConnection static PostgreSQLContainer<?>` for the database.
- `@BeforeEach` builds a `KafkaTemplate<Integer, LibraryEventDto>` using `EmbeddedKafkaBroker.getBrokersAsString()` with `IntegerSerializer` + `JsonSerializer`.
- Produces messages via `kafkaTemplate.send(...).get(10, TimeUnit.SECONDS)`.
- Uses a polling helper `waitForRecordCount(expectedCount, timeoutSeconds)` that polls `libraryEventRepository.count()` until the async consumer has persisted the expected records.
- Asserts against the real database via `libraryEventRepository.findAll()` and `bookRepository.findAll()`.

**Template:**
```java
@Test
void consumeLibraryEvent_ADD_shouldPersistLibraryEventAndBook() throws Exception {
    // given
    BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
    LibraryEventDto dto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);

    // when — produce to embedded Kafka
    kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

    // then — wait for async consumer
    waitForRecordCount(1, 10);

    List<LibraryEvent> events = libraryEventRepository.findAll();
    assertEquals(1, events.size());
    // ... assert fields, FK relationships, audit timestamps
}
```

### 2. Service Integration Tests (`service/`)

**File:** `LibraryEventServiceIntegrationTest.java`
**Purpose:** Test `LibraryEventService.processEvent()` directly — bypasses Kafka entirely, exercises DTO→Entity mapping and JPA persistence against real PostgreSQL.

**Annotations:**
```java
@SpringBootTest
@ImportTestcontainers
```

**Key patterns:**
- Constructs `ConsumerRecord<Integer, LibraryEventDto>` manually via a `buildConsumerRecord()` helper — no Kafka broker needed.
- Calls `libraryEventService.processEvent(consumerRecord)` synchronously.
- Asserts the two-step persist: `LibraryEvent` saved first (IDENTITY-generated ID), then `Book` saved with FK set.

**Helper:**
```java
private ConsumerRecord<Integer, LibraryEventDto> buildConsumerRecord(Integer key, LibraryEventDto value) {
    return new ConsumerRecord<>(
            "library-events",   // topic
            0,                  // partition
            0L,                 // offset
            key,                // key (nullable)
            value               // value
    );
}
```

**Template:**
```java
@Test
void processEvent_ADD_shouldPersistLibraryEventAndBook() {
    // given
    BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
    LibraryEventDto dto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);
    ConsumerRecord<Integer, LibraryEventDto> record = buildConsumerRecord(null, dto);

    // when
    libraryEventService.processEvent(record);

    // then
    assertEquals(1, libraryEventRepository.count());
    assertEquals(1, bookRepository.count());
    // ... assert fields, FK, audit columns
}
```

### 3. REST Controller Integration Tests (`controller/`)

**File:** `BookControllerIntegrationTest.java`
**Purpose:** Test `BookController` REST endpoints via MockMvc against real PostgreSQL.

**Annotations:**
```java
@SpringBootTest
@AutoConfigureMockMvc   // from org.springframework.boot.webmvc.test.autoconfigure
@ImportTestcontainers
```

**Key patterns:**
- Uses `MockMvc` (autowired) for HTTP assertions — `status()`, `jsonPath()`, `content()`.
- Uses Jackson 3 `tools.jackson.databind.ObjectMapper` for JSON serialization in request bodies.
- `@BeforeEach` cleans `bookRepository` (child first due to FK).
- Uses a `persistBookWithLibraryEvent()` helper to set up test data that needs a LibraryEvent FK.
- Tests all CRUD operations: GET (all, by ID, 404), POST (201, 400 validation), PUT (200, 404), DELETE (204, 404).

**Helper for test data setup:**
```java
private void persistBookWithLibraryEvent(Integer bookId, String bookName, String bookAuthor) {
    LibraryEvent libraryEvent = new LibraryEvent(null, LibraryEventType.ADD, null);
    LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

    Book book = new Book(bookId, bookName, bookAuthor);
    book.setLibraryEvent(savedEvent);
    bookRepository.save(book);
}
```

**Template:**
```java
@Test
void getBookById_shouldReturnBook() throws Exception {
    persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");

    mockMvc.perform(get("/v1/books/1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.bookId").value(1))
            .andExpect(jsonPath("$.bookName").value("Clean Code"))
            .andExpect(jsonPath("$.bookAuthor").value("Robert C. Martin"))
            .andExpect(jsonPath("$.libraryEventId").isNotEmpty())
            .andExpect(jsonPath("$.createdAt").isNotEmpty())
            .andExpect(jsonPath("$.updatedAt").isNotEmpty());
}

@Test
void createBook_shouldPersistAndReturn201() throws Exception {
    BookDto bookDto = new BookDto(10, "Domain-Driven Design", "Eric Evans");

    mockMvc.perform(post("/v1/books")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(bookDto)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.bookId").value(10))
            .andExpect(jsonPath("$.bookName").value("Domain-Driven Design"));
}
```

## Key Conventions

### Data Cleanup

- Always use `@BeforeEach` (not `@AfterEach`) — ensures a clean DB even if the previous test failed.
- Delete child entities first due to FK constraints: `bookRepository.deleteAll()` then `libraryEventRepository.deleteAll()`.

### Testcontainers Setup

- Use `@ImportTestcontainers` (Spring Boot 4.0) — NOT the old `@Testcontainers` from TC 1.x.
- Use `@ServiceConnection` on the container field — NOT `@DynamicPropertySource`.
- Container must be `static` and use the typed `PostgreSQLContainer` (not `GenericContainer`).

```java
@ServiceConnection
static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");
```

### Assertions

- Use JUnit Jupiter assertions (`assertEquals`, `assertNotNull`, `assertTrue`, `assertFalse`) — no AssertJ or Hamcrest.
- For MockMvc use `MockMvcResultMatchers` (`status()`, `jsonPath()`, `content()`).

### Naming Convention

- Test class: `{ClassName}IntegrationTest.java`
- Test method: `{methodUnderTest}_{scenario}_{expectedBehavior}()` — e.g., `processEvent_ADD_shouldPersistLibraryEventAndBook()`.

### No Mocks

- This project does **not** use Mockito or mocks. All tests run against real infrastructure.
- Kafka consumer tests use `@EmbeddedKafka`; service tests bypass Kafka by constructing `ConsumerRecord` directly.

### Jackson Version

- Test code uses Jackson 3.x: `tools.jackson.databind.ObjectMapper` — NOT `com.fasterxml.jackson.databind.ObjectMapper`.

## Running Tests

```bash
./gradlew test                           # Run all tests (Docker must be running for Testcontainers)
./gradlew test --tests "com.learnkafka.service.LibraryEventServiceIntegrationTest"   # Single class
./gradlew test --tests "*.BookControllerIntegrationTest.getBookById*"                 # Single method pattern
```

### Prerequisites

- **Docker must be running** — Testcontainers starts PostgreSQL containers automatically.
- No need to run `compose.yaml` or start Kafka externally — tests are self-contained.

## Reference

- See `docs/7_INTEGRATION_TESTING_WITH_TESTCONTAINERS.md` for detailed Testcontainers 2.x setup, lifecycle, and troubleshooting.
- See `docs/5_CONSUMER_CONCEPTS_HANDS_ON.md` for Kafka consumer concepts tested in the consumer integration test.

