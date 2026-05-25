# Kafka Consumer Diagrams

## Table of Contents

- [Section 1: @KafkaListener Flow — How Records Are Polled](#section-1-kafkalistener-flow--how-records-are-polled)
  - [Step 1 — Application Startup](#step-1--application-startup)
  - [Step 2 — Container Factory Setup](#step-2--container-factory-setup)
  - [Step 3 — Concurrent Message Listener Container & Poll Loop](#step-3--concurrent-message-listener-container--poll-loop)
  - [Step 4 — @KafkaListener Method Dispatch](#step-4--kafkalistener-method-dispatch)
  - [Step 5 — Service Layer](#step-5--service-layer)
  - [Step 6 — Offset Commit](#step-6--offset-commit)
- [Section 2: Kafka Consumer Auto-Configuration Flow](#section-2-kafka-consumer-auto-configuration-flow)
  - [Step 1 — The trigger: spring-boot-starter-kafka](#step-1--the-trigger-spring-boot-starter-kafka)
  - [Step 2 — Auto-configuration class chain](#step-2--auto-configuration-class-chain)
  - [Step 3 — Your config overrides the factory](#step-3--your-config-overrides-the-factory)
  - [Step 4 — application.yml → KafkaProperties binding](#step-4--applicationyml--kafkaproperties-binding)
  - [Step 5 — @EnableKafka + @KafkaListener wiring](#step-5--enablekafka--kafkalistener-wiring)
  - [Step 6 — Complete auto-config picture](#step-6--complete-auto-config-picture)
  - [Key Takeaway](#key-takeaway)

---

## Section 1: @KafkaListener Flow — How Records Are Polled

```mermaid
flowchart TB
  subgraph S1["Step 1 - Application Startup"]
    A1["@SpringBootApplication"] --> A2["@EnableKafka in LibraryEventsConsumerConfig"]
    A2 --> A3["KafkaListenerAnnotationBeanPostProcessor registers listener endpoints"]
  end

  subgraph S2["Step 2 - Container Factory Setup"]
    B1["application.yml spring.kafka.consumer.*"] --> B2["KafkaProperties binding"]
    B2 --> B3["ConsumerFactory<Integer, LibraryEventDto>"]
    B3 --> B4["ConcurrentKafkaListenerContainerFactory"]
    B4 --> B5["AckMode.BATCH"]
  end

  subgraph S3["Step 3 - Concurrent Message Listener Container & Poll Loop"]
    C1["ConcurrentMessageListenerContainer thread"] --> C2["KafkaConsumer.poll(timeout)"]
    C2 --> C3{"Records returned?"}
    C3 -- No --> C4["Wait and poll again"] --> C2
    C3 -- Yes --> C5["Deserialize key/value\nIntegerDeserializer + JsonDeserializer\nwith type mapping"]
  end

  subgraph S4["Step 4 - @KafkaListener Method Dispatch"]
    D1["LibraryEventsConsumer.onMessage(record)"] --> D2["Log topic/partition/offset/key/value"]
    D2 --> D3["libraryEventService.processEvent(record)"]
  end

  subgraph S5["Step 5 - Service Layer"]
    E1["Extract LibraryEventDto from record.value()"] --> E2["Run business logic + DB operations"]
  end

  subgraph S6["Step 6 - Offset Commit"]
    F1["Listener finishes processing current poll batch"] --> F2["Container commits offsets for the batch"]
    F2 --> F3["Next poll continues after committed batch offsets"]
  end

  A3 --> B4
  B5 --> C1
  C5 --> D1
  D3 --> E1
  E2 --> F1
```

### Step 1 — Application Startup

- Spring Boot starts the application context.
- `@EnableKafka` activates Kafka listener infrastructure.
- Spring scans beans and registers each `@KafkaListener` endpoint.

---

### Step 2 — Container Factory Setup

- Spring binds `spring.kafka.*` properties to `KafkaProperties`.
- `ConsumerFactory<Integer, LibraryEventDto>` is built from these settings.
- `ConcurrentKafkaListenerContainerFactory` wraps the consumer factory.
- This flow assumes `AckMode.BATCH`.

---

### Step 3 — Concurrent Message Listener Container & Poll Loop

- Listener container starts background polling threads.
- Each thread continuously calls `KafkaConsumer.poll(timeout)`.
- When records are present, Spring deserializes:
  - key -> `Integer`
  - value JSON -> `LibraryEventDto` via `JsonDeserializer`

---

### Step 4 — @KafkaListener Method Dispatch

- Spring invokes `LibraryEventsConsumer.onMessage(...)` per `ConsumerRecord`.
- The method logs metadata and delegates to `libraryEventService.processEvent(record)`.

---

### Step 5 — Service Layer

- Service reads `consumerRecord.value()` (already deserialized DTO).
- Business logic executes (mapping, validation, persistence, etc.).
- Keeping this in `@Service` keeps consumer code thin and testable.

---

### Step 6 — Offset Commit

- In BATCH ack mode, offsets are committed after the records returned from a `poll()` have been processed.
- The listener method does not need an `Acknowledgment` parameter for this flow.
- Next poll resumes from the latest committed batch offsets.

---

## Section 2: Kafka Consumer Auto-Configuration Flow

```mermaid
flowchart TB
  S1["Step 1\nDependency present:\nspring-boot-starter-kafka"] --> S2

  subgraph S2["Step 2\nSpring Boot Auto-Configuration"]
    A1["KafkaAutoConfiguration"] --> A2["Bind spring.kafka.* -> KafkaProperties"]
    A2 --> A3["Create DefaultKafkaConsumerFactory<K,V>"]
    A2 --> A4["Create KafkaListenerEndpointRegistry"]
  end

  S2 --> S3

  subgraph S3["Step 3\nYour Config Wraps the Factory"]
    B1["LibraryEventsConsumerConfig.kafkaListenerContainerFactory(...)"]
    B2["Inject ConsumerFactory<Integer, LibraryEventDto>"]
    B3["Build ConcurrentKafkaListenerContainerFactory"]
    B4["Set AckMode.BATCH"]
    B1 --> B2 --> B3 --> B4
  end

  S3 --> S4

  subgraph S4["Step 4\napplication.yml -> Runtime Consumer Properties"]
    C1["bootstrap-servers"]
    C2["group-id"]
    C3["deserializers"]
    C4["auto-offset-reset"]
    C5["spring.json.* mapping"]
  end

  S4 --> S5

  subgraph S5["Step 5\n@EnableKafka + @KafkaListener Wiring"]
    D1["KafkaListenerAnnotationBeanPostProcessor"] --> D2["Find @KafkaListener methods"]
    D2 --> D3["Register endpoint: LibraryEventsConsumer.onMessage(...)"]
  end

  S5 --> S6

  subgraph S6["Step 6\nContainer Startup & Poll Loop"]
    E1["Registry starts listener container"] --> E2["KafkaConsumer.poll() loop"]
    E2 --> E3["Deserialize -> ConsumerRecord<Integer, LibraryEventDto>"]
    E3 --> E4["Invoke onMessage(record)"]
  end
```

### Step 1 — The trigger: `spring-boot-starter-kafka`

- Adding `org.springframework.boot:spring-boot-starter-kafka` to `build.gradle` is the trigger.
- Spring Boot detects it on the classpath and enables Kafka auto-configuration.
- You do not manually create `KafkaConsumer` instances.

---

### Step 2 — Auto-configuration class chain

- Spring Boot registers `KafkaAutoConfiguration`.
- It binds `spring.kafka.*` into `KafkaProperties`.
- It creates:
  - `DefaultKafkaConsumerFactory<K,V>`
  - `KafkaListenerEndpointRegistry`
- The registry manages lifecycle (start/stop) of listener containers.

---

### Step 3 — Your config overrides the factory

- `LibraryEventsConsumerConfig` defines `kafkaListenerContainerFactory(...)`.
- Spring injects the auto-configured `ConsumerFactory<Integer, LibraryEventDto>`.
- You wrap it in `ConcurrentKafkaListenerContainerFactory` and apply project-specific settings (e.g., `AckMode.BATCH`).
- Kafka connection/deserializer settings still come from `application.yml`.

---

### Step 4 — `application.yml` → `KafkaProperties` binding

- Spring uses `@ConfigurationProperties(prefix = "spring.kafka")` to bind config.
- Key consumer fields include:
  - `bootstrap-servers`
  - `group-id`
  - `key-deserializer`
  - `value-deserializer`
  - `auto-offset-reset`
- Extended JSON options (`spring.json.*`) are passed through for `JsonDeserializer` behavior.

---

### Step 5 — `@EnableKafka` + `@KafkaListener` wiring

- `@EnableKafka` registers `KafkaListenerAnnotationBeanPostProcessor`.
- It scans beans for `@KafkaListener` methods.
- `LibraryEventsConsumer.onMessage(...)` is registered as a Kafka listener endpoint.
- The endpoint is associated with the configured container factory and topic.

---

### Step 6 — Complete auto-config picture

- `KafkaListenerEndpointRegistry` starts listener containers at runtime.
- Container threads run `KafkaConsumer.poll()` continuously.
- Records are deserialized into `ConsumerRecord<Integer, LibraryEventDto>`.
- Spring dispatches each record to `LibraryEventsConsumer.onMessage(...)`, and batch commits happen after the processed poll batch completes.

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
