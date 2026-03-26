---
name: controller
description: REST controller patterns for this Spring Boot Kafka consumer project, including CRUD endpoints and companion integration tests.
---

# Controller Skill

> **Scope:** Creating a new REST controller with full CRUD operations **and** its companion integration-test class.
> Read `AGENTS.md` first for project-wide conventions.

---

## Technology Stack

| Component             | Value                                                              |
|-----------------------|--------------------------------------------------------------------|
| Spring Boot           | 4.0.x                                                              |
| Java                  | 25                                                                 |
| Web framework         | Spring WebMVC (`spring-boot-starter-webmvc`)                       |
| Validation            | Jakarta Bean Validation (`spring-boot-starter-validation`)         |
| Persistence           | Spring Data JPA + PostgreSQL                                       |
| Schema management     | Flyway (JPA `ddl-auto: none`)                                      |
| JSON (app)            | Jackson 3 — `tools.jackson.databind.ObjectMapper`                  |
| JSON (Kafka runtime)  | Jackson 2 — `com.fasterxml.jackson` (only for Kafka serializer)    |
| Testing               | JUnit 5 + MockMvc + Testcontainers (PostgreSQL)                    |
| MockMvc import        | `org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc` (Spring Boot 4.0 package) |

---

## Files to Create Per Controller

When adding a new controller for entity `{Entity}`, create **all** of the following:

| # | File | Package / Path | Purpose |
|---|------|----------------|---------|
| 1 | `{Entity}Dto.java` | `com.learnkafka.dto` | Request DTO — Java `record` with bean-validation annotations |
| 2 | `{Entity}ResponseDto.java` | `com.learnkafka.dto` | Response DTO — Java `record`, includes audit fields + any FK IDs |
| 3 | Mapper method(s) in `LibraryEventMapper.java` | `com.learnkafka.dto` | Static `to{Entity}Entity(dto)` and `to{Entity}ResponseDto(entity)` methods |
| 4 | `{Entity}Repository.java` | `com.learnkafka.repository` | `JpaRepository<{Entity}, {IdType}>` |
| 5 | `{Entity}Service.java` | `com.learnkafka.service` | Business logic — `findAll`, `findById`, `create`, `update`, `delete` |
| 6 | `{Entity}Controller.java` | `com.learnkafka.controller` | REST endpoints under `/v1/{entities}` |
| 7 | Flyway migration | `src/main/resources/db/migration/V{N}__create_{entity}.sql` | DDL for the new table |
| 8 | `{Entity}ControllerIntegrationTest.java` | `com.learnkafka.controller` (test) | Full integration-test class |

---

## 1. Request DTO — `{Entity}Dto.java`

- Java `record` in `com.learnkafka.dto`.
- Annotate fields with `@NotNull`, `@NotBlank`, etc. from `jakarta.validation.constraints`.
- No audit fields — those are entity-only.

### Template

```java
package com.learnkafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record {Entity}Dto(
        @NotNull
        {IdType} {entityId},

        @NotBlank
        String {field1},

        @NotBlank
        String {field2}
) {
}
```

### Reference — `BookDto.java`

```java
public record BookDto(
        @NotNull Integer bookId,
        @NotBlank String bookName,
        @NotBlank String bookAuthor
) {}
```

---

## 2. Response DTO — `{Entity}ResponseDto.java`

- Java `record` in `com.learnkafka.dto`.
- Includes all fields the client should see: entity fields + FK IDs + `createdAt` / `updatedAt`.

### Template

```java
package com.learnkafka.dto;

import java.time.LocalDateTime;

public record {Entity}ResponseDto(
        {IdType} {entityId},
        String {field1},
        String {field2},
        Integer libraryEventId,      // FK reference (if applicable)
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}
```

---

## 3. Mapper — add methods to `LibraryEventMapper.java`

- All mapping is manual static methods — **no MapStruct**.
- Add two methods: `to{Entity}Entity({Entity}Dto)` and `to{Entity}ResponseDto({Entity})`.

### Template additions

```java
public static {Entity} to{Entity}Entity({Entity}Dto dto) {
    return new {Entity}(dto.{entityId}(), dto.{field1}(), dto.{field2}());
}

public static {Entity}ResponseDto to{Entity}ResponseDto({Entity} entity) {
    Integer libraryEventId = entity.getLibraryEvent() != null
            ? entity.getLibraryEvent().getLibraryEventId()
            : null;
    return new {Entity}ResponseDto(
            entity.get{EntityId}(),
            entity.get{Field1}(),
            entity.get{Field2}(),
            libraryEventId,
            entity.getCreatedAt(),
            entity.getUpdatedAt()
    );
}
```

---

## 4. Repository — `{Entity}Repository.java`

```java
package com.learnkafka.repository;

import com.learnkafka.domain.{Entity};
import org.springframework.data.jpa.repository.JpaRepository;

public interface {Entity}Repository extends JpaRepository<{Entity}, {IdType}> {
}
```

---

## 5. Service — `{Entity}Service.java`

### Conventions

- **Constructor injection** — single constructor, no `@Autowired`.
- **Logging** — `private static final Logger log = LoggerFactory.getLogger({Entity}Service.class);`
- **`@Transactional`** on mutating methods (`create`, `update`, `delete`).
- Use mapper for DTO ↔ Entity conversion.
- `delete()` must break bidirectional OneToOne references before deleting (if the entity participates in one).

### Template

```java
package com.learnkafka.service;

import com.learnkafka.domain.{Entity};
import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.dto.{Entity}ResponseDto;
import com.learnkafka.dto.LibraryEventMapper;
import com.learnkafka.repository.{Entity}Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class {Entity}Service {

    private static final Logger log = LoggerFactory.getLogger({Entity}Service.class);

    private final {Entity}Repository {entity}Repository;

    public {Entity}Service({Entity}Repository {entity}Repository) {
        this.{entity}Repository = {entity}Repository;
    }

    public List<{Entity}ResponseDto> findAll() {
        log.info("Fetching all {entities}");
        return {entity}Repository.findAll()
                .stream()
                .map(LibraryEventMapper::to{Entity}ResponseDto)
                .toList();
    }

    public Optional<{Entity}ResponseDto> findById({IdType} {entityId}) {
        log.info("Fetching {entity} with id: {}", {entityId});
        return {entity}Repository.findById({entityId})
                .map(LibraryEventMapper::to{Entity}ResponseDto);
    }

    @Transactional
    public {Entity}ResponseDto create({Entity}Dto dto) {
        log.info("Creating {entity}: {}", dto);
        {Entity} entity = LibraryEventMapper.to{Entity}Entity(dto);
        {Entity} saved = {entity}Repository.save(entity);
        log.info("Successfully created {entity}: {}", saved);
        return LibraryEventMapper.to{Entity}ResponseDto(saved);
    }

    @Transactional
    public Optional<{Entity}ResponseDto> update({IdType} {entityId}, {Entity}Dto dto) {
        log.info("Updating {entity} with id: {}", {entityId});
        return {entity}Repository.findById({entityId})
                .map(existing -> {
                    existing.set{Field1}(dto.{field1}());
                    existing.set{Field2}(dto.{field2}());
                    {Entity} updated = {entity}Repository.save(existing);
                    log.info("Successfully updated {entity}: {}", updated);
                    return LibraryEventMapper.to{Entity}ResponseDto(updated);
                });
    }

    @Transactional
    public boolean delete({IdType} {entityId}) {
        log.info("Deleting {entity} with id: {}", {entityId});
        return {entity}Repository.findById({entityId})
                .map(entity -> {
                    // Break bidirectional references if applicable
                    if (entity.getLibraryEvent() != null) {
                        entity.getLibraryEvent().set{Entity}(null);
                    }
                    {entity}Repository.delete(entity);
                    log.info("Successfully deleted {entity} with id: {}", {entityId});
                    return true;
                })
                .orElse(false);
    }
}
```

---

## 6. Controller — `{Entity}Controller.java`

### Conventions

- **`@RestController`** + **`@RequestMapping("/v1/{entities}")`**.
- **Constructor injection** of the service.
- **Logging** — `private static final Logger log = LoggerFactory.getLogger({Entity}Controller.class);`
- Use `@Valid` on `@RequestBody` params for request DTOs.
- Return `ResponseEntity<{Entity}ResponseDto>` for single-entity endpoints.
- Return `ResponseEntity<List<{Entity}ResponseDto>>` for list endpoints.
- HTTP status conventions:
  - `GET` → `200 OK`
  - `POST` → `201 Created`
  - `PUT` → `200 OK` (or `404` if not found)
  - `DELETE` → `204 No Content` (or `404` if not found)
  - Not found → `ResponseEntity.notFound().build()`

### Template

```java
package com.learnkafka.controller;

import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.dto.{Entity}ResponseDto;
import com.learnkafka.service.{Entity}Service;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/{entities}")
public class {Entity}Controller {

    private static final Logger log = LoggerFactory.getLogger({Entity}Controller.class);

    private final {Entity}Service {entity}Service;

    public {Entity}Controller({Entity}Service {entity}Service) {
        this.{entity}Service = {entity}Service;
    }

    @GetMapping
    public ResponseEntity<List<{Entity}ResponseDto>> getAll{Entities}() {
        log.info("GET /v1/{entities}");
        List<{Entity}ResponseDto> items = {entity}Service.findAll();
        return ResponseEntity.ok(items);
    }

    @GetMapping("/{entityId}")
    public ResponseEntity<{Entity}ResponseDto> get{Entity}ById(@PathVariable {IdType} {entityId}) {
        log.info("GET /v1/{entities}/{}", {entityId});
        return {entity}Service.findById({entityId})
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<{Entity}ResponseDto> create{Entity}(@RequestBody @Valid {Entity}Dto dto) {
        log.info("POST /v1/{entities} - {}", dto);
        {Entity}ResponseDto created = {entity}Service.create(dto);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PutMapping("/{entityId}")
    public ResponseEntity<{Entity}ResponseDto> update{Entity}(@PathVariable {IdType} {entityId},
                                                               @RequestBody @Valid {Entity}Dto dto) {
        log.info("PUT /v1/{entities}/{} - {}", {entityId}, dto);
        return {entity}Service.update({entityId}, dto)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{entityId}")
    public ResponseEntity<Void> delete{Entity}(@PathVariable {IdType} {entityId}) {
        log.info("DELETE /v1/{entities}/{}", {entityId});
        if ({entity}Service.delete({entityId})) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.notFound().build();
    }
}
```

### Reference — `BookController.java`

```java
@RestController
@RequestMapping("/v1/books")
public class BookController {
    private static final Logger log = LoggerFactory.getLogger(BookController.class);
    private final BookService bookService;

    public BookController(BookService bookService) {
        this.bookService = bookService;
    }

    @GetMapping
    public ResponseEntity<List<BookResponseDto>> getAllBooks() { ... }

    @GetMapping("/{bookId}")
    public ResponseEntity<BookResponseDto> getBookById(@PathVariable Integer bookId) { ... }

    @PostMapping
    public ResponseEntity<BookResponseDto> createBook(@RequestBody @Valid BookDto bookDto) { ... }

    @PutMapping("/{bookId}")
    public ResponseEntity<BookResponseDto> updateBook(@PathVariable Integer bookId,
                                                      @RequestBody @Valid BookDto bookDto) { ... }

    @DeleteMapping("/{bookId}")
    public ResponseEntity<Void> deleteBook(@PathVariable Integer bookId) { ... }
}
```

---

## 7. Flyway Migration

- File: `src/main/resources/db/migration/V{N}__create_{entity}.sql`
- **Never** use JPA `ddl-auto: create/update`. All schema changes are Flyway migrations.
- Include `created_at` and `updated_at` audit columns.
- If the entity has a FK to `library_event`, add the constraint.

### Template

```sql
CREATE TABLE {entity} (
    {entity_id}      {ID_TYPE}    PRIMARY KEY,
    {field1}         VARCHAR(255) NOT NULL,
    {field2}         VARCHAR(255) NOT NULL,
    library_event_id INTEGER,
    created_at       TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP    NOT NULL DEFAULT now(),
    CONSTRAINT fk_{entity}_library_event
        FOREIGN KEY (library_event_id)
        REFERENCES library_event (library_event_id)
);
```

---

## 8. Integration Test — `{Entity}ControllerIntegrationTest.java`

### Annotations & Setup

```java
@SpringBootTest           // Full application context
@AutoConfigureMockMvc     // From org.springframework.boot.webmvc.test.autoconfigure (Spring Boot 4.0)
@ImportTestcontainers     // Auto-start Testcontainers
```

### Key Rules

| Rule | Detail |
|------|--------|
| **Testcontainers** | Declare `@ServiceConnection static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");` |
| **MockMvc** | `@Autowired private MockMvc mockMvc;` — no manual `standaloneSetup`. |
| **ObjectMapper** | `tools.jackson.databind.ObjectMapper` (Jackson 3) — **not** `com.fasterxml.jackson`. |
| **`@BeforeEach` cleanup** | Delete child repositories first, then parent repositories (FK order). E.g., `bookRepository.deleteAll()` before `libraryEventRepository.deleteAll()`. |
| **No Kafka** | Controller tests don't need Kafka. Tests hit REST endpoints via MockMvc. |
| **Naming convention** | `{method}_{scenario}_{expectedResult}()` — e.g., `createBook_invalidPayload_shouldReturn400()`. |

### Required Imports

```java
import com.learnkafka.domain.{Entity};
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.repository.{Entity}Repository;
import com.learnkafka.repository.LibraryEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
```

### Test Method Checklist

Every controller integration-test class **must** include these test cases at minimum:

| # | Test Method | HTTP | Asserts |
|---|-------------|------|---------|
| 1 | `getAll{Entities}_shouldReturnEmptyList` | `GET /v1/{entities}` | `200`, `jsonPath("$.length()").value(0)` |
| 2 | `getAll{Entities}_shouldReturnAll{Entities}` | `GET /v1/{entities}` | `200`, `jsonPath("$.length()").value(N)` |
| 3 | `get{Entity}ById_shouldReturn{Entity}` | `GET /v1/{entities}/{id}` | `200`, assert all fields + `createdAt`/`updatedAt` not empty |
| 4 | `get{Entity}ById_notFound_shouldReturn404` | `GET /v1/{entities}/999` | `404` |
| 5 | `create{Entity}_shouldPersistAndReturn201` | `POST /v1/{entities}` | `201`, assert all fields + `createdAt`/`updatedAt` not empty |
| 6 | `create{Entity}_invalidPayload_shouldReturn400` | `POST /v1/{entities}` | `400` (send nulls/blanks) |
| 7 | `update{Entity}_shouldUpdateAndReturn200` | `PUT /v1/{entities}/{id}` | `200`, assert updated fields |
| 8 | `update{Entity}_notFound_shouldReturn404` | `PUT /v1/{entities}/999` | `404` |
| 9 | `delete{Entity}_shouldDeleteAndReturn204` | `DELETE /v1/{entities}/{id}` | `204` |
| 10 | `delete{Entity}_notFound_shouldReturn404` | `DELETE /v1/{entities}/999` | `404` |

### Helper Method

If the entity has a relationship with `LibraryEvent`, include a helper to persist test data:

```java
private void persistEntityWithLibraryEvent({IdType} {entityId}, String {field1}, String {field2}) {
    LibraryEvent libraryEvent = new LibraryEvent(null, LibraryEventType.ADD, null);
    LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

    {Entity} entity = new {Entity}({entityId}, {field1}, {field2});
    entity.setLibraryEvent(savedEvent);
    {entity}Repository.save(entity);
}
```

### Full Template

```java
package com.learnkafka.controller;

import com.learnkafka.domain.{Entity};
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.repository.{Entity}Repository;
import com.learnkafka.repository.LibraryEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ImportTestcontainers
class {Entity}ControllerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private {Entity}Repository {entity}Repository;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        {entity}Repository.deleteAll();       // child first (FK order)
        libraryEventRepository.deleteAll();   // parent second
    }

    // ── GET all ──────────────────────────────────────────────

    @Test
    void getAll{Entities}_shouldReturnEmptyList() throws Exception {
        mockMvc.perform(get("/v1/{entities}"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getAll{Entities}_shouldReturnAll{Entities}() throws Exception {
        persistEntityWithLibraryEvent(/* id1 */, /* field1 */, /* field2 */);
        persistEntityWithLibraryEvent(/* id2 */, /* field1 */, /* field2 */);

        mockMvc.perform(get("/v1/{entities}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    // ── GET by ID ────────────────────────────────────────────

    @Test
    void get{Entity}ById_shouldReturn{Entity}() throws Exception {
        persistEntityWithLibraryEvent(/* id */, /* field1 */, /* field2 */);

        mockMvc.perform(get("/v1/{entities}/{id}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.{entityId}").value(/* id */))
                .andExpect(jsonPath("$.{field1}").value(/* expected */))
                .andExpect(jsonPath("$.{field2}").value(/* expected */))
                .andExpect(jsonPath("$.libraryEventId").isNotEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void get{Entity}ById_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(get("/v1/{entities}/999"))
                .andExpect(status().isNotFound());
    }

    // ── POST (create) ────────────────────────────────────────

    @Test
    void create{Entity}_shouldPersistAndReturn201() throws Exception {
        {Entity}Dto dto = new {Entity}Dto(/* id */, /* field1 */, /* field2 */);

        mockMvc.perform(post("/v1/{entities}")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.{entityId}").value(/* id */))
                .andExpect(jsonPath("$.{field1}").value(/* expected */))
                .andExpect(jsonPath("$.{field2}").value(/* expected */))
                .andExpect(jsonPath("$.libraryEventId").isEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void create{Entity}_invalidPayload_shouldReturn400() throws Exception {
        {Entity}Dto dto = new {Entity}Dto(null, "", "");

        mockMvc.perform(post("/v1/{entities}")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isBadRequest());
    }

    // ── PUT (update) ─────────────────────────────────────────

    @Test
    void update{Entity}_shouldUpdateAndReturn200() throws Exception {
        persistEntityWithLibraryEvent(/* id */, /* field1 */, /* field2 */);
        {Entity}Dto updateDto = new {Entity}Dto(/* id */, /* updatedField1 */, /* field2 */);

        mockMvc.perform(put("/v1/{entities}/{id}")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.{field1}").value(/* updatedField1 */))
                .andExpect(jsonPath("$.{field2}").value(/* field2 */));
    }

    @Test
    void update{Entity}_notFound_shouldReturn404() throws Exception {
        {Entity}Dto updateDto = new {Entity}Dto(999, "Non-existent", "Nobody");

        mockMvc.perform(put("/v1/{entities}/999")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isNotFound());
    }

    // ── DELETE ────────────────────────────────────────────────

    @Test
    void delete{Entity}_shouldDeleteAndReturn204() throws Exception {
        persistEntityWithLibraryEvent(/* id */, /* field1 */, /* field2 */);

        mockMvc.perform(delete("/v1/{entities}/{id}"))
                .andExpect(status().isNoContent());
    }

    @Test
    void delete{Entity}_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(delete("/v1/{entities}/999"))
                .andExpect(status().isNotFound());
    }

    // ── Helper ────────────────────────────────────────────────

    private void persistEntityWithLibraryEvent({IdType} {entityId}, String {field1}, String {field2}) {
        LibraryEvent libraryEvent = new LibraryEvent(null, LibraryEventType.ADD, null);
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        {Entity} entity = new {Entity}({entityId}, {field1}, {field2});
        entity.setLibraryEvent(savedEvent);
        {entity}Repository.save(entity);
    }
}
```

---

## Placeholder Reference

Use these placeholders consistently in the templates above:

| Placeholder | Meaning | BookController example |
|-------------|---------|----------------------|
| `{Entity}` | PascalCase entity name | `Book` |
| `{entity}` | camelCase entity name | `book` |
| `{entities}` | Lowercase plural for URL path | `books` |
| `{Entities}` | PascalCase plural | `Books` |
| `{EntityId}` | PascalCase ID field | `BookId` |
| `{entityId}` | camelCase ID field | `bookId` |
| `{entity_id}` | snake_case column name | `book_id` |
| `{IdType}` | Java type of the PK | `Integer` |
| `{ID_TYPE}` | SQL type of the PK | `INTEGER` |
| `{field1}`, `{field2}` | camelCase entity fields | `bookName`, `bookAuthor` |
| `{Field1}`, `{Field2}` | PascalCase for getters/setters | `BookName`, `BookAuthor` |
| `{N}` | Next Flyway version number | `3` (check existing migrations) |

---

## Checklist Before Finishing

- [ ] All 8 files created (DTO, ResponseDTO, mapper methods, repository, service, controller, migration, integration test).
- [ ] Flyway migration version number does not conflict with existing migrations in `src/main/resources/db/migration/`.
- [ ] `@BeforeEach` deletes child repo before parent repo.
- [ ] `ObjectMapper` import is `tools.jackson.databind.ObjectMapper` (Jackson 3), not `com.fasterxml`.
- [ ] `@AutoConfigureMockMvc` import is `org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc`.
- [ ] No Lombok — entities use explicit constructors, getters, setters; DTOs are Java records.
- [ ] Logging uses `LoggerFactory.getLogger(...)`, not `@Slf4j`.
- [ ] Constructor injection everywhere — no `@Autowired` on fields.
- [ ] JPA entity has `@PrePersist` and `@PreUpdate` callbacks for `createdAt`/`updatedAt`.
- [ ] All 10 test methods are present (see Test Method Checklist).
- [ ] Run `./gradlew test` to verify all tests pass.

