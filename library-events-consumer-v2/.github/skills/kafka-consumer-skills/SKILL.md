---
name: kafka-consumer
description: Kafka consumer patterns for this Spring Boot project, including JSON deserialization, persistence, and integration testing.
---

# Kafka Consumer Skill

> **Scope:** Creating a new Kafka consumer that listens to a topic, deserializes JSON into DTOs, processes via a service layer, and persists to PostgreSQL — **plus** integration tests using **Embedded Kafka** and **Testcontainers (PostgreSQL)**.
> Read `AGENTS.md` first for project-wide conventions.

---

## Technology Stack

| Component                  | Value                                                                                       |
|----------------------------|---------------------------------------------------------------------------------------------|
| Spring Boot                | 4.0.x                                                                                       |
| Java                       | 25                                                                                          |
| Kafka client               | `spring-boot-starter-kafka`                                                                 |
| Kafka test                 | `spring-boot-starter-kafka-test` (provides `@EmbeddedKafka`, `EmbeddedKafkaBroker`)         |
| JSON deserialization       | `spring.kafka.consumer.value-deserializer: JsonDeserializer` + type mapping in `application.yml` |
| JSON serialization (tests) | Jackson 2 `JsonSerializer` (Kafka's serializer still uses `com.fasterxml.jackson`)          |
| JSON (app code)            | Jackson 3 — `tools.jackson.databind.ObjectMapper` — **not** `com.fasterxml`                 |
| Persistence                | Spring Data JPA + PostgreSQL                                                                |
| Schema management          | Flyway (`ddl-auto: none`)                                                                   |
| PostgreSQL (tests)         | Testcontainers — `@ImportTestcontainers` + `@ServiceConnection`                             |
| Kafka (tests)              | `@EmbeddedKafka` — in-process broker, no Docker needed                                      |
| Ack mode                   | `MANUAL` — consumer calls `acknowledgment.acknowledge()` in `finally` block                 |

---

## Files to Create Per Kafka Consumer

When adding a new Kafka consumer for topic `{topic-name}` processing `{Entity}` events:

| # | File                                | Package / Path                              | Purpose                                                  |
|---|-------------------------------------|---------------------------------------------|----------------------------------------------------------|
| 1 | `{Entity}Dto.java`                  | `com.learnkafka.dto`                        | Inbound DTO — Java `record` with bean-validation         |
| 2 | Domain entity (if new)              | `com.learnkafka.domain`                     | JPA entity with audit callbacks                          |
| 3 | Mapper method(s)                    | `com.learnkafka.dto.LibraryEventMapper`     | Static `toEntity(dto)` conversion                        |
| 4 | `{Entity}Repository.java`           | `com.learnkafka.repository`                 | `JpaRepository<{Entity}, {IdType}>`                      |
| 5 | `{Entity}Service.java`              | `com.learnkafka.service`                    | `processEvent(ConsumerRecord<K, V>)` — `@Transactional`  |
| 6 | `{Topic}Consumer.java`              | `com.learnkafka.consumer`                   | `@KafkaListener` + MANUAL ack                            |
| 7 | Consumer config (if custom)         | `com.learnkafka.config`                     | `ConcurrentKafkaListenerContainerFactory` bean            |
| 8 | `application.yml` entries           | `src/main/resources/application.yml`        | Consumer group, deserializer, type mapping                |
| 9 | Flyway migration (if new table)     | `src/main/resources/db/migration/V{N}__...` | DDL                                                      |
| 10 | `{Topic}ConsumerIntegrationTest.java` | `com.learnkafka.consumer` (test)          | End-to-end: Embedded Kafka → Consumer → DB assertions    |
| 11 | `{Entity}ServiceIntegrationTest.java` | `com.learnkafka.service` (test)           | Service-only: bypass Kafka, call `processEvent()` directly |

---

## 1. Inbound DTO — `{Entity}Dto.java`

- Java `record` in `com.learnkafka.dto`.
- Annotate with `@NotNull`, `@NotBlank`, `@Valid` (for nested records).
- This is what the `JsonDeserializer` produces on the consumer side.

### Reference — `LibraryEventDto.java`

```java
package com.learnkafka.dto;

import com.learnkafka.domain.LibraryEventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEventDto(
        Integer libraryEventId,

        @NotNull
        LibraryEventType libraryEventType,

        @NotNull
        @Valid
        BookDto book
) {
}
```

---

## 2. Consumer — `{Topic}Consumer.java`

### Conventions

- **`@Component`** — not `@Service` (the consumer is a Kafka infrastructure concern, not business logic).
- **`@KafkaListener(topics = "{topic-name}")`** on the handler method.
- **MANUAL acknowledgment** — method signature includes `Acknowledgment` parameter.
- **`acknowledgment.acknowledge()`** is called in a `finally` block — offsets are committed even if processing fails (error handling is delegated to the `DefaultErrorHandler` in the container factory).
- **Constructor injection** of the service.
- **Logging** — `private static final Logger log = LoggerFactory.getLogger(...)`.

### Template

```java
package com.learnkafka.consumer;

import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.service.{Entity}Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class {Topic}Consumer {

    private static final Logger log = LoggerFactory.getLogger({Topic}Consumer.class);

    private final {Entity}Service {entity}Service;

    public {Topic}Consumer({Entity}Service {entity}Service) {
        this.{entity}Service = {entity}Service;
    }

    @KafkaListener(topics = "{topic-name}")
    public void onMessage(ConsumerRecord<Integer, {Entity}Dto> consumerRecord,
                          Acknowledgment acknowledgment) {
        log.info("ConsumerRecord : {}", consumerRecord);
        try {
            {entity}Service.processEvent(consumerRecord);
        } finally {
            acknowledgment.acknowledge();
        }
    }
}
```

### Reference — `LibraryEventsConsumer.java`

```java
@Component
public class LibraryEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);
    private final LibraryEventService libraryEventService;

    public LibraryEventsConsumer(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord,
                          Acknowledgment acknowledgment) {
        log.info("ConsumerRecord : {}", consumerRecord);
        try {
            libraryEventService.processEvent(consumerRecord);
        } finally {
            acknowledgment.acknowledge();
        }
    }
}
```

---

## 3. Consumer Config — `{Topic}ConsumerConfig.java`

Only create a new config class if the new consumer requires a **different** container factory (e.g., different ack mode, concurrency, error handler). If the existing `LibraryEventsConsumerConfig` works, reuse it.

### Conventions

- **`@Configuration`** + **`@EnableKafka`**.
- Bean returns `KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>>`.
- Default ack mode is **`MANUAL`** — set via `factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL)`.

### Template

```java
package com.learnkafka.config;

import com.learnkafka.dto.{Entity}Dto;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@EnableKafka
public class {Topic}ConsumerConfig {

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, {Entity}Dto>>
            kafkaListenerContainerFactory(ConsumerFactory<Integer, {Entity}Dto> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<Integer, {Entity}Dto>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
```

### Reference — `LibraryEventsConsumerConfig.java`

```java
@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, LibraryEventDto>>
            kafkaListenerContainerFactory(ConsumerFactory<Integer, LibraryEventDto> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
```

---

## 4. Service — `{Entity}Service.java`

### Conventions

- **`@Service`** annotation.
- **`@Transactional`** on `processEvent()`.
- Accepts `ConsumerRecord<K, V>` — extracts the DTO via `consumerRecord.value()`.
- Uses `LibraryEventMapper` for DTO → Entity conversion.
- **Save order matters** for bidirectional OneToOne: save the parent entity first (to get the DB-generated ID), then save the child entity with the FK set. Temporarily null out the child reference to avoid cascade issues on the first save.
- **Constructor injection** — no `@Autowired`.
- **Logging** — `LoggerFactory.getLogger(...)`.

### Template

```java
package com.learnkafka.service;

import com.learnkafka.domain.{ChildEntity};
import com.learnkafka.domain.{ParentEntity};
import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.dto.LibraryEventMapper;
import com.learnkafka.repository.{ChildEntity}Repository;
import com.learnkafka.repository.{ParentEntity}Repository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class {Entity}Service {

    private static final Logger log = LoggerFactory.getLogger({Entity}Service.class);

    private final {ParentEntity}Repository {parentEntity}Repository;
    private final {ChildEntity}Repository {childEntity}Repository;

    public {Entity}Service({ParentEntity}Repository {parentEntity}Repository,
                           {ChildEntity}Repository {childEntity}Repository) {
        this.{parentEntity}Repository = {parentEntity}Repository;
        this.{childEntity}Repository = {childEntity}Repository;
    }

    @Transactional
    public void processEvent(ConsumerRecord<Integer, {Entity}Dto> consumerRecord) {
        {Entity}Dto dto = consumerRecord.value();
        log.info("{Entity}Dto : {}", dto);

        {ParentEntity} parentEntity = LibraryEventMapper.toEntity(dto);

        // Save parent first — it has @GeneratedValue(IDENTITY)
        parentEntity.set{ChildEntity}(null); // detach child to avoid cascade issues
        {ParentEntity} savedParent = {parentEntity}Repository.save(parentEntity);

        // Save child with FK pointing to the persisted parent
        {ChildEntity} child = LibraryEventMapper.to{ChildEntity}Entity(dto.{child}());
        child.set{ParentEntity}(savedParent);
        {ChildEntity} savedChild = {childEntity}Repository.save(child);

        // Set bidirectional back-reference for in-memory consistency
        savedParent.set{ChildEntity}(savedChild);

        log.info("Successfully persisted : {}", savedParent);
    }
}
```

### Reference — `LibraryEventService.java`

```java
@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);
    private final LibraryEventRepository libraryEventRepository;
    private final BookRepository bookRepository;

    public LibraryEventService(LibraryEventRepository libraryEventRepository,
                               BookRepository bookRepository) {
        this.libraryEventRepository = libraryEventRepository;
        this.bookRepository = bookRepository;
    }

    @Transactional
    public void processEvent(ConsumerRecord<Integer, LibraryEventDto> consumerRecord) {
        LibraryEventDto libraryEventDto = consumerRecord.value();
        log.info("LibraryEventDto : {}", libraryEventDto);

        LibraryEvent libraryEvent = LibraryEventMapper.toEntity(libraryEventDto);

        // Save LibraryEvent first — it has @GeneratedValue(IDENTITY)
        libraryEvent.setBook(null);
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        // Save Book with FK pointing to the persisted LibraryEvent
        Book book = LibraryEventMapper.toBookEntity(libraryEventDto.book());
        book.setLibraryEvent(savedEvent);
        Book savedBook = bookRepository.save(book);

        // Set bidirectional back-reference
        savedEvent.setBook(savedBook);

        log.info("Successfully persisted the library event : {}", savedEvent);
    }
}
```

---

## 5. Kafka `application.yml` Configuration

When adding a new consumer for a new topic, add/verify these properties:

```yaml
spring:
  kafka:
    consumer:
      bootstrap-servers: localhost:9092
      group-id: {group-id}
      key-deserializer: org.apache.kafka.common.serialization.IntegerDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      auto-offset-reset: latest
      properties:
        spring.json.trusted.packages: com.learnkafka.dto,com.learnkafka.domain
        spring.json.value.default.type: com.learnkafka.dto.{Entity}Dto
        # Remap producer type to consumer DTO type (if producer sends a different class name)
        spring.json.type.mapping: {producer.fully.qualified.Class}:com.learnkafka.dto.{Entity}Dto
```

### Key Properties Explained

| Property | Purpose |
|----------|---------|
| `spring.json.trusted.packages` | Packages the `JsonDeserializer` trusts for deserialization — must include DTO and domain packages |
| `spring.json.value.default.type` | Fallback type when the `__TypeId__` header is missing — set to the consumer DTO |
| `spring.json.type.mapping` | Maps the producer's class name (in Kafka headers) to the consumer's DTO class |

### Test `application.yml` Overrides

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest   # Override to "earliest" for tests — consume from beginning
```

> **⚠️ Critical:** Integration tests using `@EmbeddedKafka` **must** override `auto-offset-reset` to `earliest`. Otherwise the consumer starts at `latest` and misses messages produced before the consumer subscribes.

---

## 6. Flyway Migration

Same rules as the controller skill — see `.github/skills/controller-skills/SKILL.md` § 7.

---

## 7. Integration Test — Consumer (Embedded Kafka End-to-End)

### Purpose

End-to-end test: produce a message to an **Embedded Kafka** topic → the `@KafkaListener` consumes it → the service persists to a **Testcontainers PostgreSQL** database → assert DB state.

### Annotations & Setup

```java
@SpringBootTest                        // Full application context (consumer auto-starts)
@EmbeddedKafka(                        // In-process Kafka broker
    partitions = 1,
    topics = {"{topic-name}"},
    bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers"
)
@TestPropertySource(properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest"   // CRITICAL — consume from beginning
})
@ImportTestcontainers                  // Auto-start Testcontainers (PostgreSQL)
```

### Key Rules

| Rule | Detail |
|------|--------|
| **`@EmbeddedKafka`** | Creates an in-process Kafka broker. `bootstrapServersProperty` must point to `spring.kafka.consumer.bootstrap-servers` so the consumer factory connects to it. |
| **`auto-offset-reset=earliest`** | **Required.** Without this, consumer starts at `latest` and misses test messages. Use `@TestPropertySource` to override. |
| **Testcontainers** | `@ServiceConnection static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");` |
| **`EmbeddedKafkaBroker`** | `@Autowired private EmbeddedKafkaBroker embeddedKafkaBroker;` — used to get `getBrokersAsString()` for the test producer. |
| **Test producer** | Create a `KafkaTemplate` in `@BeforeEach` using `DefaultKafkaProducerFactory` with `embeddedKafkaBroker.getBrokersAsString()`. |
| **Serializers** | Test producer uses `IntegerSerializer` (key) + `JsonSerializer` (value) from `org.springframework.kafka.support.serializer`. |
| **Async wait** | Consumer processing is async — use a polling helper (`waitForRecordCount`) that polls the DB until expected records appear or a timeout is reached. |
| **`@BeforeEach` cleanup** | Delete child repos first, then parent repos (FK order). |
| **Naming convention** | `consume{Entity}_{scenario}_{expected}()` — e.g., `consumeLibraryEvent_ADD_shouldPersistLibraryEventAndBook()`. |

### Required Imports

```java
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.dto.BookDto;
import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
```

### Test Producer Setup (in `@BeforeEach`)

```java
@Autowired
private EmbeddedKafkaBroker embeddedKafkaBroker;

private KafkaTemplate<Integer, {Entity}Dto> kafkaTemplate;

@BeforeEach
void setUp() {
    // FK-order cleanup
    {childEntity}Repository.deleteAll();
    {parentEntity}Repository.deleteAll();

    // Build a test-only producer connected to the embedded broker
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    var producerFactory = new DefaultKafkaProducerFactory<Integer, {Entity}Dto>(producerProps);
    kafkaTemplate = new KafkaTemplate<>(producerFactory);
}
```

### Async Wait Helper

The consumer processes messages asynchronously. Tests must poll the DB until expected records appear.

```java
/**
 * Polls the database until the expected number of records appear,
 * or fails after the given timeout in seconds.
 */
private void waitForRecordCount(long expectedCount, int timeoutSeconds) throws InterruptedException {
    for (int i = 0; i < timeoutSeconds * 10; i++) {
        if ({parentEntity}Repository.count() >= expectedCount) {
            // Small buffer for remaining DB operations (child save after parent)
            Thread.sleep(200);
            return;
        }
        Thread.sleep(100);
    }
    fail("Timed out waiting for " + expectedCount + " record(s), found "
            + {parentEntity}Repository.count());
}
```

### Test Method Checklist

Every consumer integration-test class **must** include these test cases at minimum:

| # | Test Method | Scenario | Asserts |
|---|-------------|----------|---------|
| 1 | `consume{Entity}_{EventType}_shouldPersist{Entity}` | Single message, happy path | Parent + child persisted, FK relationship set, audit columns not null |
| 2 | `consume{Entity}_{EventType}_multipleMessages_shouldPersistAll` | Two or more messages | `count()` matches, each entity retrievable by ID |
| 3 | `consume{Entity}_{OtherEventType}_shouldPersist{Entity}` | Different event type (if applicable) | Event type persisted correctly |
| 4 | `consume{Entity}_withKey_shouldPersistSuccessfully` | Message sent with explicit Kafka key | Entity persisted, FK set — proves keyed messages work |

### Full Template

```java
package com.learnkafka.consumer;

import com.learnkafka.domain.{ChildEntity};
import com.learnkafka.domain.{ParentEntity};
import com.learnkafka.domain.{EventTypeEnum};
import com.learnkafka.dto.{ChildDto};
import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.repository.{ChildEntity}Repository;
import com.learnkafka.repository.{ParentEntity}Repository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"{topic-name}"},
        bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers")
@TestPropertySource(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@ImportTestcontainers
class {Topic}ConsumerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private {ParentEntity}Repository {parentEntity}Repository;

    @Autowired
    private {ChildEntity}Repository {childEntity}Repository;

    private KafkaTemplate<Integer, {Entity}Dto> kafkaTemplate;

    @BeforeEach
    void setUp() {
        {childEntity}Repository.deleteAll();      // child first (FK order)
        {parentEntity}Repository.deleteAll();     // parent second

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        var producerFactory = new DefaultKafkaProducerFactory<Integer, {Entity}Dto>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    // ── Happy Path ───────────────────────────────────────────

    @Test
    void consume{Entity}_{EventType}_shouldPersist{ParentEntity}And{ChildEntity}() throws Exception {
        // given
        {ChildDto} childDto = new {ChildDto}(/* id */, /* field1 */, /* field2 */);
        {Entity}Dto dto = new {Entity}Dto(null, {EventTypeEnum}.ADD, childDto);

        // when — produce to embedded Kafka
        kafkaTemplate.send("{topic-name}", dto).get(10, TimeUnit.SECONDS);

        // then — wait for consumer to process and persist
        waitForRecordCount(1, 10);

        List<{ParentEntity}> parents = {parentEntity}Repository.findAll();
        assertEquals(1, parents.size());

        {ParentEntity} savedParent = parents.getFirst();
        assertNotNull(savedParent.get{ParentEntityId}());
        assertEquals({EventTypeEnum}.ADD, savedParent.getEventType());
        assertNotNull(savedParent.getCreatedAt());
        assertNotNull(savedParent.getUpdatedAt());

        List<{ChildEntity}> children = {childEntity}Repository.findAll();
        assertEquals(1, children.size());

        {ChildEntity} savedChild = children.getFirst();
        assertEquals(/* expected id */, savedChild.get{ChildEntityId}());
        assertEquals(/* expected field1 */, savedChild.get{Field1}());
        assertEquals(/* expected field2 */, savedChild.get{Field2}());
        assertNotNull(savedChild.getCreatedAt());
        assertNotNull(savedChild.getUpdatedAt());

        // Verify FK relationship
        assertNotNull(savedChild.get{ParentEntity}());
        assertEquals(savedParent.get{ParentEntityId}(),
                savedChild.get{ParentEntity}().get{ParentEntityId}());
    }

    // ── Multiple Messages ────────────────────────────────────

    @Test
    void consume{Entity}_{EventType}_multipleMessages_shouldPersistAll() throws Exception {
        // given
        {ChildDto} childDto1 = new {ChildDto}(/* id1 */, /* field1 */, /* field2 */);
        {Entity}Dto dto1 = new {Entity}Dto(null, {EventTypeEnum}.ADD, childDto1);

        {ChildDto} childDto2 = new {ChildDto}(/* id2 */, /* field1 */, /* field2 */);
        {Entity}Dto dto2 = new {Entity}Dto(null, {EventTypeEnum}.ADD, childDto2);

        // when — produce two messages
        kafkaTemplate.send("{topic-name}", dto1).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send("{topic-name}", dto2).get(10, TimeUnit.SECONDS);

        // then
        waitForRecordCount(2, 10);

        assertEquals(2, {parentEntity}Repository.count());
        assertEquals(2, {childEntity}Repository.count());

        assertTrue({childEntity}Repository.findById(/* id1 */).isPresent());
        assertTrue({childEntity}Repository.findById(/* id2 */).isPresent());
    }

    // ── Different Event Type ─────────────────────────────────

    @Test
    void consume{Entity}_{OtherEventType}_shouldPersist{ParentEntity}() throws Exception {
        // given
        {ChildDto} childDto = new {ChildDto}(/* id */, /* field1 */, /* field2 */);
        {Entity}Dto dto = new {Entity}Dto(null, {EventTypeEnum}.UPDATE, childDto);

        // when
        kafkaTemplate.send("{topic-name}", dto).get(10, TimeUnit.SECONDS);

        // then
        waitForRecordCount(1, 10);

        List<{ParentEntity}> parents = {parentEntity}Repository.findAll();
        assertEquals(1, parents.size());
        assertEquals({EventTypeEnum}.UPDATE, parents.getFirst().getEventType());
    }

    // ── With Kafka Key ───────────────────────────────────────

    @Test
    void consume{Entity}_withKey_shouldPersistSuccessfully() throws Exception {
        // given
        {ChildDto} childDto = new {ChildDto}(/* id */, /* field1 */, /* field2 */);
        {Entity}Dto dto = new {Entity}Dto(null, {EventTypeEnum}.ADD, childDto);

        // when — send with an explicit key
        kafkaTemplate.send("{topic-name}", /* key */, dto).get(10, TimeUnit.SECONDS);

        // then
        waitForRecordCount(1, 10);

        List<{ParentEntity}> parents = {parentEntity}Repository.findAll();
        assertEquals(1, parents.size());
        assertNotNull(parents.getFirst().get{ParentEntityId}());

        {ChildEntity} savedChild = {childEntity}Repository.findById(/* id */).orElse(null);
        assertNotNull(savedChild);
        assertEquals(parents.getFirst().get{ParentEntityId}(),
                savedChild.get{ParentEntity}().get{ParentEntityId}());
    }

    // ── Helper ───────────────────────────────────────────────

    private void waitForRecordCount(long expectedCount, int timeoutSeconds)
            throws InterruptedException {
        for (int i = 0; i < timeoutSeconds * 10; i++) {
            if ({parentEntity}Repository.count() >= expectedCount) {
                Thread.sleep(200); // buffer for child entity save
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for " + expectedCount + " record(s), found "
                + {parentEntity}Repository.count());
    }
}
```

### Reference — `LibraryEventsConsumerIntegrationTest.java`

```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"library-events"},
        bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers")
@TestPropertySource(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@ImportTestcontainers
class LibraryEventsConsumerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @Autowired
    private BookRepository bookRepository;

    private KafkaTemplate<Integer, LibraryEventDto> kafkaTemplate;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        var producerFactory = new DefaultKafkaProducerFactory<Integer, LibraryEventDto>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @Test
    void consumeLibraryEvent_ADD_shouldPersistLibraryEventAndBook() throws Exception {
        BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
        LibraryEventDto dto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);

        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);
        waitForRecordCount(1, 10);

        List<LibraryEvent> events = libraryEventRepository.findAll();
        assertEquals(1, events.size());
        // ... full assertions on parent + child + FK + audit columns
    }
}
```

---

## 8. Integration Test — Service (Bypass Kafka)

### Purpose

Test the service's `processEvent()` directly — no Kafka broker involved. Build a `ConsumerRecord` manually and pass it to the service. Faster, more focused, no async waiting.

### Annotations & Setup

```java
@SpringBootTest           // Full application context (JPA, Flyway, etc.)
@ImportTestcontainers     // Testcontainers for PostgreSQL
// NO @EmbeddedKafka — Kafka is not used at all
```

### Key Rules

| Rule | Detail |
|------|--------|
| **No Kafka** | No `@EmbeddedKafka`, no `EmbeddedKafkaBroker`, no `KafkaTemplate`. |
| **`ConsumerRecord` builder** | Build `ConsumerRecord<K, V>` manually using the 5-arg constructor. |
| **Synchronous** | No async wait needed — `processEvent()` is called directly and returns after DB commit. |
| **`@BeforeEach` cleanup** | Same FK-order cleanup as the consumer test. |

### ConsumerRecord Builder Helper

```java
private ConsumerRecord<Integer, {Entity}Dto> buildConsumerRecord(Integer key, {Entity}Dto value) {
    return new ConsumerRecord<>(
            "{topic-name}",    // topic
            0,                 // partition
            0L,                // offset
            key,               // key
            value              // value
    );
}
```

### Test Method Checklist

| # | Test Method | Scenario | Asserts |
|---|-------------|----------|---------|
| 1 | `processEvent_{EventType}_shouldPersist{Entity}` | Single record, happy path | Parent + child persisted, FK set, audit columns not null |
| 2 | `processEvent_{EventType}_shouldAutoGenerateId` | Verify DB-generated ID | `assertNotNull(savedParent.getId())` |
| 3 | `processEvent_{EventType}_multiple_shouldPersistAll` | Two records processed sequentially | `count()` and `findById()` assertions |

### Full Template

```java
package com.learnkafka.service;

import com.learnkafka.domain.{ChildEntity};
import com.learnkafka.domain.{ParentEntity};
import com.learnkafka.domain.{EventTypeEnum};
import com.learnkafka.dto.{ChildDto};
import com.learnkafka.dto.{Entity}Dto;
import com.learnkafka.repository.{ChildEntity}Repository;
import com.learnkafka.repository.{ParentEntity}Repository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ImportTestcontainers
class {Entity}ServiceIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private {Entity}Service {entity}Service;

    @Autowired
    private {ParentEntity}Repository {parentEntity}Repository;

    @Autowired
    private {ChildEntity}Repository {childEntity}Repository;

    @BeforeEach
    void setUp() {
        {childEntity}Repository.deleteAll();      // child first (FK order)
        {parentEntity}Repository.deleteAll();     // parent second
    }

    @Test
    void processEvent_{EventType}_shouldPersist{ParentEntity}And{ChildEntity}() {
        // given
        {ChildDto} childDto = new {ChildDto}(/* id */, /* field1 */, /* field2 */);
        {Entity}Dto dto = new {Entity}Dto(null, {EventTypeEnum}.ADD, childDto);
        ConsumerRecord<Integer, {Entity}Dto> record = buildConsumerRecord(null, dto);

        // when
        {entity}Service.processEvent(record);

        // then
        var parents = {parentEntity}Repository.findAll();
        assertEquals(1, parents.size());

        {ParentEntity} savedParent = parents.getFirst();
        assertNotNull(savedParent.get{ParentEntityId}());
        assertEquals({EventTypeEnum}.ADD, savedParent.getEventType());

        var children = {childEntity}Repository.findAll();
        assertEquals(1, children.size());

        {ChildEntity} savedChild = children.getFirst();
        assertEquals(/* expected id */, savedChild.get{ChildEntityId}());
        assertNotNull(savedChild.get{ParentEntity}());
        assertEquals(savedParent.get{ParentEntityId}(),
                savedChild.get{ParentEntity}().get{ParentEntityId}());
    }

    @Test
    void processEvent_{EventType}_shouldAutoGenerate{ParentEntity}Id() {
        // given
        {ChildDto} childDto = new {ChildDto}(/* id */, /* field1 */, /* field2 */);
        {Entity}Dto dto = new {Entity}Dto(null, {EventTypeEnum}.ADD, childDto);
        ConsumerRecord<Integer, {Entity}Dto> record = buildConsumerRecord(null, dto);

        // when
        {entity}Service.processEvent(record);

        // then
        var parents = {parentEntity}Repository.findAll();
        assertEquals(1, parents.size());
        assertNotNull(parents.getFirst().get{ParentEntityId}(),
                "{parentEntityId} should be auto-generated by the DB");
    }

    @Test
    void processEvent_{EventType}_multiple_shouldPersistAll() {
        // given
        {ChildDto} childDto1 = new {ChildDto}(/* id1 */, /* field1 */, /* field2 */);
        {Entity}Dto dto1 = new {Entity}Dto(null, {EventTypeEnum}.ADD, childDto1);

        {ChildDto} childDto2 = new {ChildDto}(/* id2 */, /* field1 */, /* field2 */);
        {Entity}Dto dto2 = new {Entity}Dto(null, {EventTypeEnum}.ADD, childDto2);

        // when
        {entity}Service.processEvent(buildConsumerRecord(null, dto1));
        {entity}Service.processEvent(buildConsumerRecord(null, dto2));

        // then
        assertEquals(2, {parentEntity}Repository.count());
        assertEquals(2, {childEntity}Repository.count());

        Optional<{ChildEntity}> child1 = {childEntity}Repository.findById(/* id1 */);
        assertTrue(child1.isPresent());

        Optional<{ChildEntity}> child2 = {childEntity}Repository.findById(/* id2 */);
        assertTrue(child2.isPresent());
    }

    private ConsumerRecord<Integer, {Entity}Dto> buildConsumerRecord(
            Integer key, {Entity}Dto value) {
        return new ConsumerRecord<>(
                "{topic-name}",  // topic
                0,               // partition
                0L,              // offset
                key,             // key
                value            // value
        );
    }
}
```

### Reference — `LibraryEventServiceIntegrationTest.java`

```java
@SpringBootTest
@ImportTestcontainers
class LibraryEventServiceIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired private LibraryEventService libraryEventService;
    @Autowired private LibraryEventRepository libraryEventRepository;
    @Autowired private BookRepository bookRepository;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
    }

    @Test
    void processEvent_ADD_shouldPersistLibraryEventAndBook() {
        BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
        LibraryEventDto dto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);
        ConsumerRecord<Integer, LibraryEventDto> record = buildConsumerRecord(null, dto);

        libraryEventService.processEvent(record);

        // ... assertions on libraryEventRepository + bookRepository
    }

    private ConsumerRecord<Integer, LibraryEventDto> buildConsumerRecord(
            Integer key, LibraryEventDto value) {
        return new ConsumerRecord<>("library-events", 0, 0L, key, value);
    }
}
```

---

## Placeholder Reference

| Placeholder | Meaning | LibraryEvents example |
|-------------|---------|----------------------|
| `{topic-name}` | Kafka topic name | `library-events` |
| `{Topic}` | PascalCase topic name (for class names) | `LibraryEvents` |
| `{Entity}` | PascalCase DTO/event name | `LibraryEvent` |
| `{entity}` | camelCase | `libraryEvent` |
| `{ParentEntity}` | JPA entity saved first (has `@GeneratedValue`) | `LibraryEvent` |
| `{parentEntity}` | camelCase | `libraryEvent` |
| `{ChildEntity}` | JPA entity saved second (has FK) | `Book` |
| `{childEntity}` | camelCase | `book` |
| `{ChildDto}` | Child DTO record | `BookDto` |
| `{EventTypeEnum}` | Enum for event types | `LibraryEventType` |
| `{N}` | Next Flyway version number | `3` |

---

## ⚠️ Common Pitfalls

| Pitfall | How to Avoid |
|---------|--------------|
| Test consumer misses messages | Set `auto-offset-reset=earliest` via `@TestPropertySource` |
| `EmbeddedKafkaBroker` not wired | Ensure `bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers"` in `@EmbeddedKafka` |
| Test fails intermittently | Use `waitForRecordCount()` with adequate timeout (10s); don't use `Thread.sleep(fixed)` |
| `JsonSerializer` import confusion | Test producer uses `org.springframework.kafka.support.serializer.JsonSerializer` (Jackson 2); app code uses Jackson 3 `tools.jackson.databind.ObjectMapper` |
| FK constraint violation in cleanup | Always delete child repo before parent repo in `@BeforeEach` |
| `kafkaTemplate.send()` appears to succeed but nothing consumed | Verify topic name in `@EmbeddedKafka(topics = {...})` matches the `@KafkaListener(topics = ...)` exactly |
| Deserialization type mismatch | Verify `spring.json.type.mapping` in `application.yml` maps the producer's class to the consumer's DTO |

---

## Checklist Before Finishing

- [ ] Consumer class uses `@Component`, not `@Service`.
- [ ] `acknowledgment.acknowledge()` is in a `finally` block.
- [ ] Consumer config sets `AckMode.MANUAL`.
- [ ] `application.yml` has `spring.json.trusted.packages`, `spring.json.value.default.type`, and `spring.json.type.mapping`.
- [ ] Consumer integration test has `@EmbeddedKafka` with correct `topics` and `bootstrapServersProperty`.
- [ ] `@TestPropertySource` overrides `auto-offset-reset` to `earliest`.
- [ ] Test producer built in `@BeforeEach` using `embeddedKafkaBroker.getBrokersAsString()`.
- [ ] `waitForRecordCount()` helper polls DB instead of fixed `Thread.sleep()`.
- [ ] Service integration test builds `ConsumerRecord` manually — no Kafka involved.
- [ ] `@BeforeEach` deletes child repo before parent repo (FK order).
- [ ] No Lombok — explicit constructors, getters, setters.
- [ ] Logging uses `LoggerFactory.getLogger(...)`, not `@Slf4j`.
- [ ] Constructor injection everywhere — no `@Autowired` on fields.
- [ ] Flyway migration created if new tables are needed.
- [ ] Run `./gradlew test` to verify all tests pass.

