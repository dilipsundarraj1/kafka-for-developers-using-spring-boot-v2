# Kafka Consumer Diagrams

---

## Section 1: @KafkaListener Flow — How Records Are Polled

### Step 1 — Application Startup

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        APPLICATION STARTUP                                  │
│                                                                             │
│  @SpringBootApplication                                                     │
│  LibraryEventsConsumerApplication                                           │
│         │                                                                   │
│         ▼                                                                   │
│  @EnableKafka (LibraryEventsConsumerConfig)                                 │
│  Scans for @KafkaListener annotations and registers them                    │
└─────────────────────────────┬───────────────────────────────────────────────┘
```

- `@SpringBootApplication` bootstraps the Spring context and triggers component scanning.
- `@EnableKafka` on `LibraryEventsConsumerConfig` registers the `KafkaListenerAnnotationBeanPostProcessor`.
- The post-processor scans all beans for `@KafkaListener` annotated methods and registers them as listener endpoints.

---

### Step 2 — Container Factory Setup

```
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONTAINER FACTORY SETUP                                  │
│                                                                             │
│  KafkaListenerContainerFactory Bean                                         │
│  (LibraryEventsConsumerConfig.kafkaListenerContainerFactory)                │
│                                                                             │
│  ┌─────────────────────────────────────────┐                                │
│  │  ConcurrentKafkaListenerContainerFactory│                                │
│  │  + ConsumerFactory<Integer,             │                                │
│  │      LibraryEventDto>                   │                                │
│  └─────────────────────────────────────────┘                                │
│                                                                             │
│  application.yml drives ConsumerFactory config:                             │
│    bootstrap-servers: localhost:9092                                        │
│    group-id:          library-events-listener-group                         │
│    key-deserializer:  IntegerDeserializer                                   │
│    value-deserializer: JsonDeserializer                                     │
│    auto-offset-reset:  latest                                               │
│    trusted.packages:   com.learnkafka.dto, com.learnkafka.domain            │
│    type.mapping:       LibraryEvent → LibraryEventDto                       │
└─────────────────────────────┬───────────────────────────────────────────────┘
```

- `ConcurrentKafkaListenerContainerFactory` is the Spring-managed factory that creates listener containers for each `@KafkaListener` endpoint.
- It wraps a `ConsumerFactory` (auto-configured by Spring Boot from `application.yml`) that supplies the raw `KafkaConsumer` instances.
- `bootstrap-servers` tells the consumer where to connect to the Kafka broker.
- `group-id` assigns this consumer to the `library-events-listener-group` consumer group, enabling load-balanced partition assignment across instances.
- `key-deserializer` / `value-deserializer` define how raw bytes from Kafka are converted back into Java objects.
- `spring.json.type.mapping` decouples the producer's `LibraryEvent` JPA entity from the consumer's `LibraryEventDto` record during deserialization.

---

### Step 3 — Concurrent Message Listener Container & Poll Loop

```
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  CONCURRENT MESSAGE LISTENER CONTAINER                      │
│                                                                             │
│  ConcurrentMessageListenerContainer                                         │
│    └── Manages one or more KafkaMessageListenerContainer threads            │
│                                                                             │
│  Each container thread runs a poll loop:                                    │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────┐               │
│  │                    POLL LOOP (background thread)         │               │
│  │                                                          │               │
│  │  ┌─────────────────────────────────────────────────┐    │               │
│  │  │  KafkaConsumer.poll(timeout)                    │    │               │
│  │  │                                                 │    │               │
│  │  │  Sends FetchRequest to Kafka Broker             │    │               │
│  │  │  Topic: "library-events"                        │    │               │
│  │  │  Partition(s): assigned by group coordinator    │    │               │
│  │  └──────────────────────┬──────────────────────────┘    │               │
│  │                         │                                │               │
│  │              Records returned?                           │               │
│  │                 YES │        NO                          │               │
│  │                     │         └──► wait, poll again      │               │
│  │                     ▼                                    │               │
│  │  ┌──────────────────────────────────────────────────┐   │               │
│  │  │  Deserialize each ConsumerRecord                 │   │               │
│  │  │    Key:   Integer  (IntegerDeserializer)         │   │               │
│  │  │    Value: JSON → LibraryEventDto (JsonDeserializer│   │               │
│  │  │           with type mapping)                     │   │               │
│  │  └──────────────────────┬───────────────────────────┘   │               │
│  └────────────────────────-│──────────────────────────────-┘               │
└────────────────────────────│────────────────────────────────────────────────┘
```

- `ConcurrentMessageListenerContainer` manages one background thread per partition (or per configured concurrency level).
- Each thread runs an infinite poll loop — it never stops until the application shuts down.
- `KafkaConsumer.poll(timeout)` sends a `FetchRequest` to the broker asking for new records from the assigned partitions of `library-events`.
- If no records are available the poll blocks for up to the timeout duration and then retries — there is no busy-waiting.
- When records arrive, each `ConsumerRecord` is deserialized: the key bytes become an `Integer` and the value JSON bytes become a `LibraryEventDto` using `JsonDeserializer` with the configured type mapping.

---

### Step 4 — @KafkaListener Method Dispatch

```
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│               @KafkaListener METHOD DISPATCH                                │
│                                                                             │
│  LibraryEventsConsumer                                                      │
│                                                                             │
│  @KafkaListener(topics = "library-events")                                  │
│  onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord)         │
│                                                                             │
│    ┌──────────────────────────────────────────────────┐                     │
│    │  ConsumerRecord contains:                        │                     │
│    │    topic     → "library-events"                  │                     │
│    │    partition → assigned partition number         │                     │
│    │    offset    → record offset                     │                     │
│    │    key       → Integer (libraryEventId)          │                     │
│    │    value     → LibraryEventDto {                 │                     │
│    │                  libraryEventId,                 │                     │
│    │                  eventType (ADD/UPDATE),         │                     │
│    │                  book { id, name, author }       │                     │
│    │                }                                 │                     │
│    └──────────────────────────────────────────────────┘                     │
│         │                                                                   │
│         ▼                                                                   │
│    log.info("ConsumerRecord: {}", consumerRecord)                           │
│         │                                                                   │
│         ▼                                                                   │
│    libraryEventService.processEvent(consumerRecord)                         │
└─────────────────────────────┬───────────────────────────────────────────────┘
```

- Spring invokes `onMessage()` once per deserialized `ConsumerRecord` — this is your entry point for business logic.
- The `ConsumerRecord` carries the full metadata: topic name, partition number, offset, key, and the deserialized `LibraryEventDto` value.
- The `offset` uniquely identifies the record's position in the partition and is used for offset commit after processing.
- `eventType` (ADD or UPDATE) inside `LibraryEventDto` tells the service what kind of operation to perform.
- The call to `libraryEventService.processEvent()` delegates to the service layer, keeping the consumer class focused solely on receiving records.

---

### Step 5 — Service Layer

```
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       SERVICE LAYER                                         │
│                                                                             │
│  LibraryEventService                                                        │
│                                                                             │
│  processEvent(ConsumerRecord<Integer, LibraryEventDto>)                     │
│    └── libraryEventDto = consumerRecord.value()                             │
│    └── log.info("LibraryEventDto: {}", libraryEventDto)                    │
│                                                                             │
│    [Future: save to DB, route by eventType ADD/UPDATE, etc.]                │
└─────────────────────────────┬───────────────────────────────────────────────┘
```

- `processEvent()` is the business logic layer — currently it extracts the `LibraryEventDto` from the record and logs it.
- `consumerRecord.value()` returns the already-deserialized `LibraryEventDto`, so no manual JSON parsing is needed here.
- This is where future logic will live: persisting to the database, branching on `eventType` (ADD vs UPDATE), publishing downstream events, etc.
- Keeping this logic in a `@Service` rather than in the consumer class makes it independently testable.

---

### Step 6 — Offset Commit

```
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     OFFSET COMMIT                                           │
│                                                                             │
│  After onMessage() returns successfully:                                    │
│    auto-commit OR manual ack commits offset back to Kafka                   │
│    (currently using default auto-commit from auto-offset-reset: latest)     │
│                                                                             │
│  Next poll() picks up from committed offset                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

- Once `onMessage()` returns without throwing an exception, Spring Kafka commits the offset for that record back to Kafka.
- The committed offset tells the broker "this consumer has successfully processed up to this point" — if the app restarts it resumes from here.
- Currently using default auto-commit, meaning Spring Kafka commits periodically in the background without explicit acknowledgement in code.
- `auto-offset-reset: latest` means if no committed offset exists (e.g. first startup) the consumer starts from the newest record, not from the beginning of the topic.
- Switching to `AckMode.MANUAL` would give full control — the offset is only committed when `Acknowledgment.acknowledge()` is explicitly called.

---

## Section 2: Kafka Consumer Auto-Configuration Flow

### Step 1 — The trigger: `spring-boot-starter-kafka`

```
build.gradle
└── implementation 'org.springframework.boot:spring-boot-starter-kafka'
        │
        └── pulls in:
              spring-kafka
              kafka-clients (Apache Kafka)
              jackson-databind (for JsonDeserializer)
```

Simply adding `spring-boot-starter-kafka` to the classpath is enough to activate all Kafka
auto-configuration. There is no need to manually instantiate a `KafkaConsumer` or register
any beans — Spring Boot detects the dependency and bootstraps everything automatically.

---

### Step 2 — Auto-configuration class chain

```
spring.factories / AutoConfiguration.imports
        │
        ▼
KafkaAutoConfiguration                         ← Spring Boot registers this automatically
        │
        ├── reads application.yml via
        │       KafkaProperties  (spring.kafka.*)
        │
        ├── creates ──► DefaultKafkaConsumerFactory<K,V>
        │                   │
        │                   ├── bootstrap-servers : localhost:9092
        │                   ├── group-id          : library-events-listener-group
        │                   ├── key-deserializer  : IntegerDeserializer
        │                   ├── value-deserializer: JsonDeserializer
        │                   └── auto-offset-reset : latest
        │
        └── creates ──► KafkaListenerEndpointRegistry
                            manages all listener containers
```

`KafkaAutoConfiguration` is the heart of Spring Boot's Kafka support. It reads every
`spring.kafka.*` property from `application.yml` and binds them into a `KafkaProperties`
object. From those properties it builds a `DefaultKafkaConsumerFactory` which is the factory
responsible for creating raw `KafkaConsumer` instances whenever a new listener container needs
one. It also creates a `KafkaListenerEndpointRegistry` that tracks and manages the lifecycle
(start/stop) of every listener container in the application.

---

### Step 3 — Your config overrides the factory

```java
// LibraryEventsConsumerConfig.java
@Bean
KafkaListenerContainerFactory<...> kafkaListenerContainerFactory(
        ConsumerFactory<Integer, LibraryEventDto> consumerFactory) {  // ← injected from auto-config
    var factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);                       // ← wraps the auto-configured factory
    return factory;
}
```

The auto-configured `DefaultKafkaConsumerFactory` is injected here as a parameter. By
declaring your own `ConcurrentKafkaListenerContainerFactory` bean you take control of how
listener containers are created — for example you can set the concurrency level (number of
threads), plug in a custom error handler, or configure an `AckMode`. Because you are wrapping
the auto-configured `ConsumerFactory`, all the deserializer and connection settings from
`application.yml` are preserved automatically.

---

### Step 4 — `application.yml` → `KafkaProperties` binding

```
application.yml                         KafkaProperties (bound fields)
─────────────────────────────────────────────────────────────────────
spring.kafka.consumer:
  bootstrap-servers: localhost:9092  →  getConsumer().getBootstrapServers()
  group-id: library-events-…        →  getConsumer().getGroupId()
  key-deserializer: Integer…        →  getConsumer().getKeyDeserializer()
  value-deserializer: Json…         →  getConsumer().getValueDeserializer()
  auto-offset-reset: latest         →  getConsumer().getAutoOffsetReset()
  properties:
    spring.json.trusted.packages    →  passed as extra consumer props
    spring.json.value.default.type  →  tells JsonDeserializer target type
    spring.json.type.mapping        →  LibraryEvent → LibraryEventDto
```

Spring Boot uses `@ConfigurationProperties(prefix = "spring.kafka")` to bind every key in
`application.yml` directly to typed fields in `KafkaProperties`. The `properties:` block is
passed through as a raw map of strings directly to the underlying `KafkaConsumer`, which
allows you to configure any native Kafka setting or Spring Kafka extension (like
`spring.json.type.mapping`) without needing a custom Java config class.

---

### Step 5 — `@EnableKafka` + `@KafkaListener` wiring

```
@EnableKafka  (LibraryEventsConsumerConfig)
      │
      └──► registers KafkaListenerAnnotationBeanPostProcessor
                  │
                  └──► scans all beans for @KafkaListener methods
                              │
                              └──► LibraryEventsConsumer.onMessage()
                                        topic    = "library-events"
                                        groupId  = (from application.yml)
                                        factory  = kafkaListenerContainerFactory
```

`@EnableKafka` registers a `KafkaListenerAnnotationBeanPostProcessor` into the Spring context.
This post-processor runs after all beans are created and scans every bean for methods annotated
with `@KafkaListener`. When it finds `LibraryEventsConsumer.onMessage()`, it reads the topic
name, resolves which container factory to use, and registers a listener endpoint. The
`KafkaListenerEndpointRegistry` then creates a `ConcurrentMessageListenerContainer` for that
endpoint and starts its internal poll loop in a background thread — making the consumer live
without any additional code from the developer.

---

### Step 6 — Complete auto-config picture

```
┌─────────────────────────────────────────────────────────────────────┐
│  spring-boot-starter-kafka on classpath                             │
│                    │                                                │
│                    ▼                                                │
│         KafkaAutoConfiguration                                      │
│                    │                                                │
│      ┌─────────────┴──────────────┐                                 │
│      ▼                            ▼                                 │
│  DefaultKafkaConsumerFactory   KafkaListenerEndpointRegistry        │
│  (reads KafkaProperties)       (manages container lifecycle)        │
│      │                                                              │
│      │ injected into                                                │
│      ▼                                                              │
│  YOUR LibraryEventsConsumerConfig                                   │
│      │                                                              │
│      └──► ConcurrentKafkaListenerContainerFactory                   │
│                    │                                                │
│                    │  @EnableKafka scans beans                      │
│                    ▼                                                │
│          LibraryEventsConsumer.onMessage()                          │
│          @KafkaListener(topics="library-events")                    │
│                    │                                                │
│                    │  container started by registry                 │
│                    ▼                                                │
│          KafkaConsumer.poll() loop running in background thread     │
│                    │                                                │
│                    ▼                                                │
│          JsonDeserializer maps LibraryEvent → LibraryEventDto       │
│          (via spring.json.type.mapping in application.yml)          │
└─────────────────────────────────────────────────────────────────────┘
```

This diagram shows the full chain from the starter dependency all the way down to the running
poll loop. Notice that `DefaultKafkaConsumerFactory` flows into your custom config, which
produces the `ConcurrentKafkaListenerContainerFactory`. The `KafkaListenerEndpointRegistry`
(created by auto-config) then uses that factory to build and start the listener container for
every `@KafkaListener` method it discovers. The end result is a continuously running poll loop
that delivers deserialized `LibraryEventDto` records to `onMessage()` with zero boilerplate.

---

### Key Takeaway

You write **zero boilerplate** for the `KafkaConsumer` itself. Spring Boot auto-configuration:

| Step | What happens |
|------|-------------|
| 1 | `spring-boot-starter-kafka` on classpath triggers `KafkaAutoConfiguration` |
| 2 | `application.yml` is bound to `KafkaProperties` |
| 3 | `DefaultKafkaConsumerFactory` is created with all deserializer settings |
| 4 | Your `LibraryEventsConsumerConfig` wraps it in `ConcurrentKafkaListenerContainerFactory` |
| 5 | `@EnableKafka` scans for `@KafkaListener` and registers endpoints |
| 6 | `KafkaListenerEndpointRegistry` starts the poll loop in a background thread |
