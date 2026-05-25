# Integration Testing with Testcontainers

## Table of Contents

1. [Overview](#overview)
2. [Technology Stack & Versions](#technology-stack--versions)
3. [Testcontainers 2.x — What Changed from 1.x](#testcontainers-2x--what-changed-from-1x)
4. [Required Dependencies](#required-dependencies)
5. [Dependency Tree Explained](#dependency-tree-explained)
6. [Test Configuration (`application.yml`)](#test-configuration-applicationyml)
7. [Test Class Setup — Anatomy of an Integration Test](#test-class-setup--anatomy-of-an-integration-test)
8. [How `@ImportTestcontainers` + `@ServiceConnection` Work Together](#how-importtestcontainers--serviceconnection-work-together)
9. [Container Lifecycle](#container-lifecycle)
10. [Writing a Test Method](#writing-a-test-method)
11. [Common Patterns & Helpers](#common-patterns--helpers)
12. [Troubleshooting](#troubleshooting)
13. [Quick Reference — Copy-Paste Template](#quick-reference--copy-paste-template)

---

## Overview

This project uses **Testcontainers** to spin up a real **PostgreSQL** database inside a Docker container during integration tests. This gives us:

- **No mocks** — tests run against a real database with real SQL.
- **No manual setup** — no need to start `compose.yaml` before running tests.
- **Isolation** — each test run gets a fresh, disposable container.
- **CI-friendly** — works anywhere Docker is available.

The key integration is between **Spring Boot 4.x** and **Testcontainers 2.x**, which uses a different API than the older Testcontainers 1.x that most tutorials reference.

---

## Technology Stack & Versions

| Component | Version | Notes |
|---|---|---|
| Spring Boot | 4.0.3 | Uses `spring-boot-testcontainers` module |
| Testcontainers | 2.0.3 | Major rewrite from 1.x — different module names and API |
| JUnit Jupiter | 6.0.3 | Provided by `spring-boot-starter-test` |
| PostgreSQL Driver | (managed) | `org.postgresql:postgresql` — runtime dependency |
| Hibernate | 7.2.x | JPA provider via `spring-boot-starter-data-jpa` |

---

## Testcontainers 2.x — What Changed from 1.x

> ⚠️ **This is the #1 source of confusion.** Most online tutorials and AI-generated code reference TC 1.x APIs that no longer exist.

### Module Renames

| Testcontainers 1.x (old) | Testcontainers 2.x (new) | Status |
|---|---|---|
| `org.testcontainers:postgresql` | `org.testcontainers:testcontainers-postgresql` | **Renamed** |
| `org.testcontainers:junit-jupiter` | *(removed)* | **Merged into core / replaced by Spring Boot** |
| `org.testcontainers:testcontainers` | `org.testcontainers:testcontainers` | Same (core) |

### Annotation Changes

| Testcontainers 1.x (old) | Spring Boot 4.x + TC 2.x (new) |
|---|---|
| `@Testcontainers` (from `org.testcontainers.junit.jupiter`) | `@ImportTestcontainers` (from `org.springframework.boot.testcontainers.context`) |
| `@Container` (from `org.testcontainers.junit.jupiter`) | *(not needed)* — `@ImportTestcontainers` discovers container fields automatically |
| `@DynamicPropertySource` (manual property wiring) | `@ServiceConnection` (auto-wires datasource URL, username, password) |

### Class Availability

| Class | Package in TC 2.x |
|---|---|
| `PostgreSQLContainer` | `org.testcontainers.containers.PostgreSQLContainer` (from `testcontainers-postgresql` module) |
| `JdbcDatabaseContainer` | `org.testcontainers.containers.JdbcDatabaseContainer` (from `testcontainers-jdbc` — transitive via `testcontainers-postgresql`) |
| `GenericContainer` | `org.testcontainers.containers.GenericContainer` (from core `testcontainers`) |

> **Key insight:** Spring Boot's `JdbcContainerConnectionDetailsFactory` requires a `JdbcDatabaseContainer` (not a plain `GenericContainer`). `PostgreSQLContainer` extends `JdbcDatabaseContainer`, so `@ServiceConnection` can auto-detect the JDBC connection details. Using a plain `GenericContainer` with `@ServiceConnection` will fail with:
> ```
> No ConnectionDetailsFactory found for source '@ServiceConnection source for ...'
> ```

---

## Required Dependencies

Add these to `build.gradle`:

```groovy
dependencies {
    // ... production dependencies ...

    // Spring Boot's Testcontainers integration (provides @ImportTestcontainers, @ServiceConnection)
    testImplementation 'org.springframework.boot:spring-boot-testcontainers'

    // PostgreSQL container (provides PostgreSQLContainer class)
    // Transitively brings in: testcontainers-jdbc → testcontainers-database-commons → testcontainers (core)
    testImplementation 'org.testcontainers:testcontainers-postgresql'

    // JUnit platform launcher (required for test execution)
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}
```

### What NOT to add (TC 2.x)

```groovy
// ❌ These artifacts DO NOT EXIST in Testcontainers 2.x — they will FAIL to resolve
// testImplementation 'org.testcontainers:postgresql'       // old name → use testcontainers-postgresql
// testImplementation 'org.testcontainers:junit-jupiter'    // removed → use @ImportTestcontainers
```

### Transitive Dependency Tree

```
org.springframework.boot:spring-boot-testcontainers:4.0.3
└── org.testcontainers:testcontainers:2.0.3               ← core module

org.testcontainers:testcontainers-postgresql:2.0.3         ← PostgreSQLContainer
└── org.testcontainers:testcontainers-jdbc:2.0.3           ← JdbcDatabaseContainer (base class)
    └── org.testcontainers:testcontainers-database-commons:2.0.3
        └── org.testcontainers:testcontainers:2.0.3        ← core (deduplicated)
```

---

## Dependency Tree Explained

| Dependency | What It Provides | Why Needed |
|---|---|---|
| `spring-boot-testcontainers` | `@ImportTestcontainers`, `@ServiceConnection`, `ContainerConnectionDetailsFactory` SPI | Bridges Spring Boot's connection details system with Testcontainers |
| `testcontainers-postgresql` | `PostgreSQLContainer` class | Typed container that knows how to start PostgreSQL with defaults |
| `testcontainers-jdbc` (transitive) | `JdbcDatabaseContainer` base class | Required by Spring Boot's `JdbcContainerConnectionDetailsFactory` to auto-wire `spring.datasource.*` |
| `testcontainers` core (transitive) | `GenericContainer`, Docker client, lifecycle management | Foundation for all Testcontainers functionality |

---

## Test Configuration (`application.yml`)

File: `src/test/resources/application.yml`

```yaml
server:
  port: 0                              # Random port — avoids conflicts with running app

spring:
  application:
    name: library-events-consumer-test
  jpa:
    hibernate:
      ddl-auto: create                # Create schema on startup — container destruction handles cleanup
    show-sql: true                      # Log SQL for debugging
    properties:
      hibernate:
        format_sql: true
  kafka:
    consumer:
      bootstrap-servers: localhost:9092
      group-id: library-events-listener-group
      key-deserializer: org.apache.kafka.common.serialization.IntegerDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      auto-offset-reset: latest
      properties:
        spring.json.trusted.packages: com.learnkafka.dto,com.learnkafka.domain
        spring.json.value.default.type: com.learnkafka.dto.LibraryEventDto
        spring.json.type.mapping: com.learnkafka.domain.LibraryEvent:com.learnkafka.dto.LibraryEventDto
```

### Key differences from `main/resources/application.yml`

| Property | `main` (production) | `test` | Why |
|---|---|---|---|
| `server.port` | `8081` | `0` | Random port avoids conflicts |
| `spring.datasource.*` | Hardcoded `localhost:5432` | **Not present** | `@ServiceConnection` auto-configures this from the container |
| `jpa.hibernate.ddl-auto` | `create` | `create` | Same — `create-drop` is unnecessary since Testcontainers destroys the container (and all data) after tests |

> **Important:** Do NOT put `spring.datasource.url` in test `application.yml`. The `@ServiceConnection` annotation tells Spring Boot to get the JDBC URL, username, and password directly from the running Testcontainer. If you hardcode it, the container's random port will be ignored.

---

## Test Class Setup — Anatomy of an Integration Test

```java
package com.learnkafka.service;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;      // ① Replaces @Testcontainers
import org.springframework.boot.testcontainers.service.connection.ServiceConnection; // ② Auto-wires datasource
import org.testcontainers.containers.PostgreSQLContainer;                           // ③ From testcontainers-postgresql

@SpringBootTest                 // Boots full Spring application context
@ImportTestcontainers           // Scans this class for static container fields and manages their lifecycle
class LibraryEventServiceIntegrationTest {

    @ServiceConnection          // Tells Spring Boot: "get JDBC connection details from this container"
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    // ... @Autowired beans, @BeforeEach, @Test methods ...
}
```

### Line-by-line Breakdown

| # | Element | Purpose |
|---|---|---|
| ① | `@ImportTestcontainers` | Replaces the old `@Testcontainers` + `@Container` combo. Scans the annotated class (or a referenced class) for `static` fields of type `Startable` (i.e., any container) and manages their lifecycle. |
| ② | `@ServiceConnection` | Replaces `@DynamicPropertySource`. Spring Boot inspects the container type (`PostgreSQLContainer` → extends `JdbcDatabaseContainer`) and auto-creates `JdbcConnectionDetails` with the correct URL, username, and password from the running container. |
| ③ | `PostgreSQLContainer` | A typed container from `org.testcontainers:testcontainers-postgresql`. It knows the default PostgreSQL port (5432), default credentials, and includes a readiness check. |

---

## How `@ImportTestcontainers` + `@ServiceConnection` Work Together

```
┌─────────────────────────────────────────────────────────┐
│ Test starts                                             │
│                                                         │
│  1. @ImportTestcontainers scans for static container    │
│     fields annotated with @ServiceConnection            │
│                                                         │
│  2. Spring Boot starts the PostgreSQLContainer           │
│     → Docker pulls postgres:latest (if not cached)      │
│     → Starts container on a random port (e.g., 55432)   │
│     → Waits for readiness (accepts connections)         │
│                                                         │
│  3. @ServiceConnection triggers                         │
│     JdbcContainerConnectionDetailsFactory               │
│     → Reads container.getJdbcUrl()                      │
│     → Reads container.getUsername()                     │
│     → Reads container.getPassword()                     │
│     → Creates JdbcConnectionDetails bean                │
│                                                         │
│  4. Spring auto-configuration uses JdbcConnectionDetails │
│     → Configures HikariCP DataSource                    │
│     → Hibernate connects to the container's PostgreSQL  │
│     → DDL runs (create-drop) → tables created           │
│                                                         │
│  5. Tests execute against real PostgreSQL                │
│                                                         │
│  6. After all tests → container is stopped and removed  │
└─────────────────────────────────────────────────────────┘
```

### The `ConnectionDetailsFactory` Chain

```
PostgreSQLContainer (is-a JdbcDatabaseContainer)
       │
       ▼
JdbcContainerConnectionDetailsFactory
  (registered in spring-boot-jdbc via META-INF/spring.factories)
       │
       ▼
JdbcConnectionDetails
  ├── getJdbcUrl()    → "jdbc:postgresql://localhost:55432/test"
  ├── getUsername()    → "test"
  └── getPassword()   → "test"
       │
       ▼
DataSourceAutoConfiguration picks this up
       │
       ▼
HikariCP DataSource configured ✅
```

> **Why `GenericContainer` fails:** `GenericContainer` does NOT extend `JdbcDatabaseContainer`, so no `ConnectionDetailsFactory` matches it. Spring Boot throws:
> ```
> No ConnectionDetailsFactory found for source '@ServiceConnection source for ...'
> ```

---

## Container Lifecycle

| Phase | What Happens |
|---|---|
| **Before first test** | `@ImportTestcontainers` starts the container. Spring context loads, Hibernate creates schema. |
| **Between tests** | Container stays running. Use `@BeforeEach` to clean data (e.g., `repository.deleteAll()`). |
| **After all tests** | Container is stopped and removed. Docker resources are freed. |

### Data Cleanup Pattern

```java
@BeforeEach
void setUp() {
    // Delete child entities first (FK constraint order)
    bookRepository.deleteAll();
    libraryEventRepository.deleteAll();
}
```

> Use `@BeforeEach` (not `@AfterEach`) so the DB is clean even if a previous test failed.

---

## Writing a Test Method

### Pattern: Given → When → Then

```java
@Test
void processEvent_ADD_shouldPersistLibraryEventAndBook() {
    // given — build a ConsumerRecord with DTOs (simulates what Kafka delivers)
    BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
    LibraryEventDto libraryEventDto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);
    ConsumerRecord<Integer, LibraryEventDto> consumerRecord = new ConsumerRecord<>(
            "library-events", 0, 0L, null, libraryEventDto);

    // when — call the service directly (bypasses Kafka consumer)
    libraryEventService.processEvent(consumerRecord);

    // then — verify data was persisted
    var libraryEvents = libraryEventRepository.findAll();
    assertEquals(1, libraryEvents.size());

    LibraryEvent savedEvent = libraryEvents.getFirst();
    assertNotNull(savedEvent.getLibraryEventId());          // DB auto-generated
    assertEquals(LibraryEventType.ADD, savedEvent.getEventType());

    var books = bookRepository.findAll();
    assertEquals(1, books.size());
    assertEquals("Clean Code", books.getFirst().getBookName());
}
```

### Key Points

1. **We test the service layer directly** — no need to start a Kafka broker for DB persistence tests.
2. **We build `ConsumerRecord` manually** — this simulates the record the Kafka listener would receive.
3. **We assert against real DB state** — using Spring Data repositories to query what was persisted.

---

## Common Patterns & Helpers

### Helper: Build a `ConsumerRecord`

```java
private ConsumerRecord<Integer, LibraryEventDto> buildConsumerRecord(Integer key, LibraryEventDto value) {
    return new ConsumerRecord<>(
            "library-events",       // topic
            0,                      // partition
            0L,                     // offset
            key,                    // key
            value                   // value
    );
}
```

### Shared Container Across Multiple Test Classes

If you have many test classes and want to reuse the same container (faster), extract it:

```java
// src/test/java/com/learnkafka/TestContainersConfig.java
public class TestContainersConfig {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");
}
```

Then reference it from each test class:

```java
@SpringBootTest
@ImportTestcontainers(TestContainersConfig.class)   // ← points to shared container
class MyTest {
    // ...
}
```

---

## Troubleshooting

### Error: `No ConnectionDetailsFactory found for source '@ServiceConnection source for ...'`

**Cause:** Using `GenericContainer` instead of `PostgreSQLContainer`.

**Fix:** Use the typed container:
```java
// ❌ Wrong
static GenericContainer<?> postgres = new GenericContainer<>("postgres:latest");

// ✅ Correct
static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");
```

And ensure you have the dependency:
```groovy
testImplementation 'org.testcontainers:testcontainers-postgresql'   // NOT 'org.testcontainers:postgresql'
```

---

### Error: `Cannot resolve symbol 'Testcontainers'` or `Cannot resolve symbol 'Container'`

**Cause:** Using TC 1.x annotations that don't exist in TC 2.x.

**Fix:** Replace:
```java
// ❌ TC 1.x — these classes don't exist in TC 2.x
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.junit.jupiter.Container;

// ✅ Spring Boot 4.x + TC 2.x
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
```

---

### Error: `Cannot resolve symbol 'PostgreSQLContainer'`

**Cause:** Missing `testcontainers-postgresql` dependency, or IDE hasn't synced Gradle.

**Fix:**
1. Ensure `build.gradle` has: `testImplementation 'org.testcontainers:testcontainers-postgresql'`
2. Reload Gradle project in IDE (Gradle tool window → Refresh icon).
3. Verify with: `./gradlew dependencies --configuration testCompileClasspath | grep testcontainers`

---

### Error: Gradle dependency resolution failure for `org.testcontainers:postgresql` or `org.testcontainers:junit-jupiter`

**Cause:** These are TC 1.x artifact names that don't exist at version 2.x.

**Fix:** Use the new names:
```groovy
// ❌ Old (TC 1.x)
testImplementation 'org.testcontainers:postgresql'
testImplementation 'org.testcontainers:junit-jupiter'

// ✅ New (TC 2.x)
testImplementation 'org.testcontainers:testcontainers-postgresql'
// junit-jupiter is no longer needed — use @ImportTestcontainers from Spring Boot
```

---

### Tests are slow / Docker pull on every run

**Fix:** Use a specific tag instead of `latest`:
```java
static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17");
```

---

### `HikariPool - Failed to validate connection` warnings on shutdown

**Cause:** The Testcontainer is stopped before HikariCP finishes closing connections. This is cosmetic and harmless.

**Fix (optional):** These warnings can be ignored. They occur because the Docker container is destroyed before the connection pool fully drains.

---

## Quick Reference — Copy-Paste Template

### `build.gradle` (test dependencies only)

```groovy
testImplementation 'org.springframework.boot:spring-boot-testcontainers'
testImplementation 'org.testcontainers:testcontainers-postgresql'
testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
```

### `src/test/resources/application.yml` (minimal)

```yaml
server:
  port: 0
spring:
  jpa:
    hibernate:
      ddl-auto: create
  # Do NOT add spring.datasource.* — @ServiceConnection handles it
```

### Test class skeleton

```java
package com.learnkafka.service;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest
@ImportTestcontainers
class MyIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    // @Autowired your services and repositories

    // @BeforeEach → clean DB

    // @Test → given/when/then
}
```

### Run tests

```bash
./gradlew test
# or run a specific test class
./gradlew test --tests "com.learnkafka.service.LibraryEventServiceIntegrationTest"
```

### Prerequisites

- **Docker must be running** on the machine (Docker Desktop, colima, etc.)
- No need to run `compose.yaml` — Testcontainers manages its own container.

