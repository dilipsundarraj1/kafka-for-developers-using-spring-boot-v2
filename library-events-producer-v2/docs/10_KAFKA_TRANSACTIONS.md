# Kafka Transactions in Spring Kafka

---

<!-- TOC -->
* [Kafka Transactions in Spring Kafka](#kafka-transactions-in-spring-kafka)
  * [What is a Kafka Transaction?](#what-is-a-kafka-transaction)
  * [Exactly-Once Semantics (EOS)](#exactly-once-semantics-eos)
    * [The Three Delivery Guarantees](#the-three-delivery-guarantees)
    * [The Three Layers of EOS in Kafka](#the-three-layers-of-eos-in-kafka)
      * [Layer 1 — Idempotent Producer](#layer-1--idempotent-producer)
      * [Layer 2 — Transactions](#layer-2--transactions)
      * [Layer 3 — `read_committed` Consumer](#layer-3--read_committed-consumer)
    * [How the Three Layers Combine](#how-the-three-layers-combine)
    * [EOS Summary](#eos-summary)
  * [Part 1 — Producer-Side Transactions](#part-1--producer-side-transactions)
    * [Core Concepts](#core-concepts)
      * [1. Idempotent Producer](#1-idempotent-producer)
      * [2. Transactional ID](#2-transactional-id)
      * [3. Transaction Coordinator](#3-transaction-coordinator)
    * [Producer Transaction Options in Spring Kafka](#producer-transaction-options-in-spring-kafka)
      * [Option 1 — `KafkaTransactionManager` + `@Transactional`](#option-1--kafkatransactionmanager--transactional)
      * [Option 2 — `KafkaTemplate.executeInTransaction()`](#option-2--kafkatemplateexecuteintransaction)
      * [Option 3 — `ChainedKafkaTransactionManager` (Kafka + Database)](#option-3--chainedkafkatransactionmanager-kafka--database)
    * [Producer Options Comparison](#producer-options-comparison)
    * [How to Enable Producer Transactions in This Project](#how-to-enable-producer-transactions-in-this-project)
      * [Step 1 — What Is Already in Place](#step-1--what-is-already-in-place)
      * [Step 2 — Add the Transaction ID Prefix](#step-2--add-the-transaction-id-prefix)
      * [Step 3 — Annotate the Service Methods](#step-3--annotate-the-service-methods)
  * [Is This Only a Producer Concern?](#is-this-only-a-producer-concern)
  * [Part 2 — Consumer-Side Transactions](#part-2--consumer-side-transactions)
    * [Consumer Isolation Level](#consumer-isolation-level)
    * [The Problem Without Consumer Transactions](#the-problem-without-consumer-transactions)
    * [Consume-Transform-Produce (CTP) Pattern](#consume-transform-produce-ctp-pattern)
    * [How Spring Kafka Implements CTP](#how-spring-kafka-implements-ctp)
    * [Consumer Transaction Flow Diagram](#consumer-transaction-flow-diagram)
  * [How to Enable Transactions in This Project (Summary)](#how-to-enable-transactions-in-this-project-summary)
    * [Step 4 — Set Consumer Isolation Level (Downstream Services)](#step-4--set-consumer-isolation-level-downstream-services)
  * [Does This Project Benefit from EOS?](#does-this-project-benefit-from-eos)
    * [Idempotent Producer — `enable.idempotence: true` ✅ Already enabled, genuinely useful](#idempotent-producer--enableidempotence-true--already-enabled-genuinely-useful)
    * [Transactions — `transaction-id-prefix` ❌ Not meaningful here](#transactions--transaction-id-prefix--not-meaningful-here)
    * [`read_committed` — ❌ Not applicable here](#read_committed---not-applicable-here)
    * [Summary for This Project](#summary-for-this-project)
  * [Important Considerations](#important-considerations)
    * [Performance Trade-off](#performance-trade-off)
    * [Transactional ID Must Be Stable](#transactional-id-must-be-stable)
    * [`acks=all` Is Mandatory](#acksall-is-mandatory)
    * [`enable.auto.commit` Must Be False for CTP](#enableautocommit-must-be-false-for-ctp)
    * [Avoid Mixing Transactional and Non-Transactional Sends](#avoid-mixing-transactional-and-non-transactional-sends)
      * [Test-only override used in this project](#test-only-override-used-in-this-project)
<!-- TOC -->

## What is a Kafka Transaction?

A Kafka transaction gives you **atomicity across multiple produce operations**. Either all messages in a transaction are committed and become visible to consumers, or none of them are — even if the producer crashes mid-way through.

Without transactions:
- A producer sends Message A → succeeds
- A producer sends Message B → crashes before sending
- Consumers see Message A but never Message B — **partial state**

With transactions:
- Producer begins a transaction
- Sends Message A and Message B within the transaction
- If it commits → consumers see both
- If it crashes before committing → consumers see neither

This is the foundation of **Exactly-Once Semantics (EOS)** in Kafka.

---

## Exactly-Once Semantics (EOS)

### The Three Delivery Guarantees

Before understanding EOS, it helps to understand the three levels of delivery guarantee that any messaging system can offer:

| Guarantee | What it means | Risk |
|-----------|--------------|------|
| **At-most-once** | Message is sent once — never retried | Message loss is possible |
| **At-least-once** | Message is retried until acknowledged | Duplicate messages are possible |
| **Exactly-once** | Message is processed precisely one time | No loss, no duplicates |

By default, Kafka gives you **at-least-once**. The producer retries on failure, which means a message that was already written to the broker can be written again. This is safe for many use cases, but not when duplicates cause problems — like billing events, inventory updates, or financial transactions.

**Exactly-once** is the hardest guarantee to achieve in a distributed system. Kafka achieves it not through a single feature but through **three layers working together**.

---

### The Three Layers of EOS in Kafka

Each layer closes a different gap. Remove any one of them and you fall back to at-least-once.

---

#### Layer 1 — Idempotent Producer

**What it prevents:** Duplicate messages from producer retries

**How it works:** Each message carries a sequence number — the broker deduplicates retries using the Producer ID (PID) and sequence number per partition.

**Covers:** Producer → Broker

**Scenario — Network drops after broker writes, before ack is sent back**

| | Behaviour | Outcome |
|-|-----------|---------|
| **Without idempotence** | Producer thinks send failed → retries → broker writes it again | Consumer receives the message **twice** — duplicate processing |
| **With Layer 1 (Idempotent Producer)** | Producer retries → broker sees same sequence number → deduplicates | Consumer receives the message **once** — correct |

---

#### Layer 2 — Transactions

**What it prevents:** Partial visibility when sending multiple messages

**How it works:** All messages in a transaction commit together or not at all, coordinated by the Transaction Coordinator.

**Covers:** Producer → Broker (multi-message atomicity)

**Scenario — Producer sends Message A and Message B, crashes after A is written**

| | Behaviour | Outcome |
|-|-----------|---------|
| **Without transactions** | A is written, B is never written | Consumer sees Message A but never Message B — **partial state** |
| **With Layer 2 (Transactions)** | Neither message is visible until both are committed — crash aborts both | Consumer sees **neither** — consistent state |

---

#### Layer 3 — `read_committed` Consumer

**What it prevents:** Reading messages from aborted transactions

**How it works:** Consumer only sees fully committed messages — aborted and in-flight messages are invisible.

**Covers:** Broker → Consumer

**Scenario — Producer begins a transaction, writes messages, then aborts**

| | Behaviour | Outcome |
|-|-----------|---------|
| **Without `read_committed`** | Consumer reads in-flight messages before the abort completes | Consumer processes events that should never have existed — **phantom events** |
| **With Layer 3 (`read_committed`)** | Consumer only sees fully committed messages | Aborted messages are **never visible** — no phantom events |

---

### How the Three Layers Combine

```
Producer
  │  enable.idempotence = true       ← Layer 1: no duplicates on retry
  │  transaction-id-prefix set       ← Layer 2: atomic multi-message commit
  ▼
Kafka Broker
  │  Transaction Coordinator manages COMMIT / ABORT markers
  ▼
Consumer
     isolation.level = read_committed ← Layer 3: never reads aborted messages
```

All three must be active for true end-to-end EOS. In practice:
- **Layer 1 alone** → at-least-once with deduplication at broker level
- **Layers 1 + 2** → atomic producer, but consumer can still read aborted messages
- **Layers 1 + 2 + 3** → full exactly-once end to end

---

### EOS Summary

| Layer | Gap it closes | Mechanism |
|-------|--------------|-----------|
| Idempotent producer | Duplicate messages from producer retries | Sequence numbers per partition |
| Transactions | Partial visibility of multi-message publish | All-or-nothing commit via Transaction Coordinator |
| `read_committed` consumer | Phantom events from aborted transactions | Consumer skips uncommitted and aborted messages |
| `sendOffsetsToTransaction()` | Duplicate consume-produce cycles on restart | Consumer offset and produce are one atomic unit |

---

## Part 1 — Producer-Side Transactions

### Core Concepts

#### 1. Idempotent Producer

The prerequisite for transactions. An idempotent producer ensures that **retried messages are not duplicated** on the broker side.

This project already has it enabled:

```yaml
# application.yml
spring:
  kafka:
    producer:
      properties:
        enable.idempotence: true
```

Each producer is assigned a **Producer ID (PID)** and a **sequence number** per partition. The broker deduplicates retries using these. Transactions build on top of this guarantee.

#### 2. Transactional ID

A stable, unique string assigned to a producer across restarts. The broker uses this to:
- Fence off zombie producers (old instances of the same producer that crashed and recovered)
- Track the in-progress transaction state

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: lib-events-tx-
```

Spring Kafka appends a unique suffix (e.g., `-0`, `-1`) to the prefix per producer instance, resulting in IDs like `lib-events-tx-0`.

#### 3. Transaction Coordinator

A Kafka broker component that manages transaction state. It:
- Tracks which partitions are part of a transaction
- Writes transaction markers (`COMMIT` or `ABORT`) to every partition involved
- Ensures atomicity across partitions and even across topics

---

### Producer Transaction Options in Spring Kafka

#### Option 1 — `KafkaTransactionManager` + `@Transactional`

The most common approach. Spring manages the transaction lifecycle automatically via the `@Transactional` annotation, the same way it manages database transactions with `DataSourceTransactionManager`.

**How it works:**
1. `@Transactional` intercepts the method call
2. Spring begins a Kafka transaction (`beginTransaction`)
3. Your method executes — all `kafkaTemplate.send()` calls join the transaction
4. If the method returns normally → `commitTransaction`
5. If the method throws → `abortTransaction`

**When to use:**
- Your service method sends one or more Kafka messages and you want all-or-nothing delivery
- Simple producer-only use case (no database involved)

---

**What is `KafkaTransactionManager`?**

`KafkaTransactionManager` is Spring Kafka's implementation of Spring's `PlatformTransactionManager` interface — the same abstraction used by `DataSourceTransactionManager` for JDBC and `JpaTransactionManager` for JPA. It bridges Spring's `@Transactional` machinery to Kafka's native transaction API.

When `@Transactional` fires, Spring looks up a `PlatformTransactionManager` bean and delegates begin/commit/abort to it. By registering `KafkaTransactionManager`, you teach Spring to manage Kafka transactions the same way it manages database ones — with no Kafka-specific code in your service layer.

**How `KafkaTransactionManager` gets wired up**

`KafkaTransactionManager` wraps a `ProducerFactory` that is configured with a `transactional.id`. It holds a reference to the factory so it can obtain a transactional producer and call `beginTransaction()`, `commitTransaction()`, and `abortTransaction()` on your behalf.

```
@Transactional
    │
    ▼
Spring TransactionInterceptor
    │  looks up → PlatformTransactionManager
    ▼
KafkaTransactionManager
    │  wraps → ProducerFactory (configured with transaction-id-prefix)
    ▼
KafkaProducer (transactional)
    │  communicates with → Transaction Coordinator on broker
    ▼
Kafka Broker
```

**Auto-configuration vs manual declaration**

When `spring.kafka.producer.transaction-id-prefix` is set in `application.yml`, Spring Boot's `KafkaAutoConfiguration` automatically:
1. Creates a `DefaultKafkaProducerFactory` with `transactional.id` set
2. Wraps it in a `KafkaTransactionManager` bean
3. Registers that bean as the default `PlatformTransactionManager`

You only need to declare the bean manually if you have multiple `KafkaTemplate` instances or need to customise the factory:

```java
// Only needed when auto-configuration is not sufficient
@Configuration
public class KafkaTransactionConfig {

    @Bean
    public ProducerFactory<Long, LibraryEvent> producerFactory(KafkaProperties props) {
        Map<String, Object> config = props.buildProducerProperties();
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lib-events-tx-");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTransactionManager<Long, LibraryEvent> kafkaTransactionManager(
            ProducerFactory<Long, LibraryEvent> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean
    public KafkaTemplate<Long, LibraryEvent> kafkaTemplate(
            ProducerFactory<Long, LibraryEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
```

---

**What Happens Under the Hood (Producer)**

```
LibraryEventsController
  │  POST /v1/libraryevent
  ▼
LibraryEventService.createLibraryEvent()   ← @Transactional intercepts here
  │
  ├── KafkaTransactionManager.beginTransaction()
  │     Producer sends BEGIN marker to Transaction Coordinator
  │
  ├── KafkaTemplate.send("library-events", key, libraryEvent)
  │     Message is staged — NOT yet visible to read_committed consumers
  │
  └── Method returns normally
        │
        ├── KafkaTransactionManager.commitTransaction()
        │     Transaction Coordinator writes COMMIT markers to all partitions
        │     Message is NOW visible to read_committed consumers
        │
        └── [on exception] KafkaTransactionManager.abortTransaction()
              Transaction Coordinator writes ABORT markers
              Message is NEVER visible to read_committed consumers
```

---

**Step 1 — Configure `application.yml`**

```yaml
spring:
  kafka:
    producer:
      acks: all
      transaction-id-prefix: lib-events-tx-   # triggers auto-configuration of KafkaTransactionManager
      properties:
        enable.idempotence: true
```

Spring Boot automatically registers a `KafkaTransactionManager` bean when `transaction-id-prefix` is set — no manual bean declaration required.

**Step 1b — Configure the consumer `application.yml` (downstream consumer service)**

This configuration belongs to the **consumer application** — the downstream service that reads from `library-events`. Without this, consumers will process messages from transactions that were later aborted (phantom events).

```yaml
# Consumer service — application.yml
spring:
  kafka:
    consumer:
      properties:
        isolation.level: read_committed   # only reads messages from committed transactions
```

**Step 2 — Annotate the service method with `@Transactional`**

```java
// LibraryEventService.java
import org.springframework.transaction.annotation.Transactional;

@Service
public class LibraryEventService {

    private final KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    public LibraryEventService(KafkaTemplate<Long, LibraryEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional   // Spring intercepts this → begins Kafka transaction
    public void publishEvents(List<LibraryEvent> events) {
        // All sends are inside the same transaction — all or nothing
        for (LibraryEvent event : events) {
            kafkaTemplate.send("library-events", event.libraryEventId(), event);
        }
        // Method returns normally → Spring calls commitTransaction()
        // Any exception thrown here → Spring calls abortTransaction()
    }
}
```

**Step 3 — Verify the transaction flow at runtime**

When `publishNewEvent()` is called:

```
@Transactional proxy intercepts the call
  │
  ├── KafkaTransactionManager.beginTransaction()
  │     Producer registers with Transaction Coordinator (BEGIN marker sent)
  │
  ├── kafkaTemplate.send("library-events", event1...) ← staged, not yet visible
  ├── kafkaTemplate.send("library-events", event2...) ← staged, not yet visible
  │
  └── Method returns normally
        ├── KafkaTransactionManager.commitTransaction()
        │     COMMIT markers written to both partitions
        │     Both messages become visible to read_committed consumers
        │
        └── [on any exception]
              KafkaTransactionManager.abortTransaction()
              ABORT markers written — neither message is ever visible
```

---

#### Option 2 — `KafkaTemplate.executeInTransaction()`

A programmatic alternative to `@Transactional`. You pass a lambda to `executeInTransaction()` and Spring handles begin/commit/abort around it.

**How it works:**
- Explicitly scopes a transaction to the lambda body
- Useful when you need a transaction for just one block inside a larger method that is not itself `@Transactional`

**When to use:**
- You need fine-grained control over exactly which `send()` calls are transactional
- You cannot use `@Transactional` (e.g., the method is called internally within the same class, bypassing the Spring proxy)

**Step 1 — Configure `application.yml`**

```yaml
spring:
  kafka:
    producer:
      acks: all
      transaction-id-prefix: lib-events-tx-   # required — enables transactional producer
      properties:
        enable.idempotence: true
```

**Step 1b — Configure the consumer `application.yml` (downstream consumer service)**

This configuration belongs to the **consumer application** — the downstream service that reads from `library-events`. Without this, consumers will process messages from transactions that were later aborted (phantom events).

```yaml
# Consumer service — application.yml
spring:
  kafka:
    consumer:
      properties:
        isolation.level: read_committed   # only reads messages from committed transactions
```

**Step 2 — Call `executeInTransaction()` in the service**

```java
// LibraryEventService.java
@Service
public class LibraryEventService {

    private final KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    public LibraryEventService(KafkaTemplate<Long, LibraryEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // No @Transactional on the method — transaction is scoped to the lambda only
    public void publishNewEvent(LibraryEvent event) {

        // Non-transactional work can happen here before the lambda
        log.info("Preparing to publish event: {}", event.libraryEventId());

        kafkaTemplate.executeInTransaction(ops -> {
            // Everything inside this lambda is part of one Kafka transaction
            for (LibraryEvent e : events) {
                ops.send("library-events", e.libraryEventId(), e);
            }
            return null;   // return value is passed back from executeInTransaction()
        });

        // Non-transactional work can continue here after the lambda
        log.info("Event published successfully: {}", event.libraryEventId());
    }
}
```

**Step 3 — Verify the transaction flow at runtime**

When `executeInTransaction()` is called:

```
kafkaTemplate.executeInTransaction(ops -> { ... })
  │
  ├── KafkaTransactionManager.beginTransaction()
  │     Transactional producer registers with Transaction Coordinator
  │
  ├── ops.send("library-events", event1...) ← staged inside the transaction
  ├── ops.send("library-events", event2...) ← staged inside the transaction
  │
  └── Lambda completes normally
        ├── commitTransaction()
        │     COMMIT markers written — both messages become visible
        │
        └── [on any exception thrown inside the lambda]
              abortTransaction()
              ABORT markers written — neither message is ever visible
```

**Note:** `ops` inside the lambda is a `KafkaOperations` scoped to this transaction. Do not use the outer `kafkaTemplate` reference inside the lambda — use `ops` instead to guarantee the sends are part of the same transaction.

---

#### Option 3 — `ChainedKafkaTransactionManager` (Kafka + Database)

Used when a **single operation must write to both a database and Kafka atomically**. This chains a `KafkaTransactionManager` with a `DataSourceTransactionManager` (or JPA `JpaTransactionManager`).

**How it works:**
- When `@Transactional` fires, Spring begins both a DB transaction and a Kafka transaction
- On commit: DB commits first, then Kafka commits
- On rollback: both are rolled back

**Important limitation:** This is a **best-effort** chain, not a true two-phase commit (2PC). If the DB commits but the application crashes before Kafka commits, you can have inconsistency. This is acceptable for most use cases and is called the **"last resource gambit"** pattern.

**When to use:**
- You are saving an entity to PostgreSQL and publishing a Kafka event in the same operation
- You want to avoid the dual-write problem (DB saved, Kafka failed — or vice versa)

**Step 1 — Configure `application.yml`**

```yaml
spring:
  kafka:
    producer:
      acks: all
      transaction-id-prefix: lib-events-tx-
      properties:
        enable.idempotence: true
  datasource:
    url: jdbc:postgresql://localhost:5432/librarydb
    username: libuser
    password: secret
```

**Step 2 — Declare the `ChainedKafkaTransactionManager` bean**

Spring Boot auto-configures `KafkaTransactionManager` and `JpaTransactionManager` separately. You need to wire them together manually:

```java
// KafkaTransactionConfig.java
@Configuration
public class KafkaTransactionConfig {

    // Step 2a — get the auto-configured Kafka transaction manager
    // (Spring Boot creates this automatically when transaction-id-prefix is set)

    // Step 2b — get the JPA transaction manager
    @Bean
    public JpaTransactionManager jpaTransactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }

    // Step 2c — chain them: Kafka is the "last resource" (commits second)
    @Bean
    public ChainedKafkaTransactionManager<Object, Object> chainedKafkaTransactionManager(
            KafkaTransactionManager<Long, LibraryEvent> kafkaTransactionManager,
            JpaTransactionManager jpaTransactionManager) {

        // Order matters: JPA commits first, Kafka commits second
        return new ChainedKafkaTransactionManager<>(kafkaTransactionManager, jpaTransactionManager);
    }
}
```

**Step 3 — Annotate the service method, qualifying with the chained manager**

```java
// LibraryEventService.java
@Service
public class LibraryEventService {

    private final LibraryEventRepository repository;
    private final KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    public LibraryEventService(LibraryEventRepository repository,
                               KafkaTemplate<Long, LibraryEvent> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // Use the chained manager — both DB and Kafka are in scope
    @Transactional("chainedKafkaTransactionManager")
    public LibraryEvent createLibraryEvent(LibraryEvent libraryEvent) {

        // Step A: persist to database (inside the DB transaction)
        LibraryEvent saved = repository.save(libraryEvent);

        // Step B: publish to Kafka (inside the Kafka transaction)
        kafkaTemplate.send("library-events", saved.libraryEventId(), saved);

        return saved;
        // Method returns → JPA commits first, then Kafka commits
        // Any exception → both DB and Kafka roll back
    }
}
```

**Step 4 — Verify the transaction flow at runtime**

When `createLibraryEvent()` is called:

```
@Transactional("chainedKafkaTransactionManager") proxy intercepts
  │
  ├── JpaTransactionManager.beginTransaction()      ← DB transaction opens
  ├── KafkaTransactionManager.beginTransaction()    ← Kafka transaction opens
  │
  ├── repository.save(libraryEvent)                 ← written to DB (not yet committed)
  ├── kafkaTemplate.send("library-events", ...)     ← staged in Kafka (not yet committed)
  │
  └── Method returns normally
        ├── JpaTransactionManager.commitTransaction()    ← DB record committed first
        ├── KafkaTransactionManager.commitTransaction()  ← Kafka message committed second
        │     Both operations are now durable and visible
        │
        └── [on any exception]
              JpaTransactionManager.rollbackTransaction()   ← DB write rolled back
              KafkaTransactionManager.abortTransaction()    ← Kafka message aborted
```

**Failure window (the "last resource gambit"):**
```
JPA commits ✓  →  [application crashes here]  →  Kafka never commits
  DB has the record, Kafka topic does not — inconsistency
  Mitigation: use an outbox pattern or idempotent consumers to handle this edge case
```

---

### Producer Options Comparison

| Option | Transaction Scope | Use Case |
|--------|------------------|----------|
| `@Transactional` + `KafkaTransactionManager` | Kafka only | Produce multiple messages atomically |
| `executeInTransaction()` | Kafka only (programmatic) | Fine-grained or internal method control |
| `ChainedKafkaTransactionManager` | Kafka + Database | Atomic DB write + Kafka publish |

---

### How to Enable Producer Transactions in This Project

#### Step 1 — What Is Already in Place

This project already has the two prerequisites:

```yaml
# application.yml — already configured
spring:
  kafka:
    producer:
      acks: all                         # required — transactions need full acknowledgement
      properties:
        enable.idempotence: true        # required — idempotence is the foundation of transactions
```

#### Step 2 — Add the Transaction ID Prefix

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 10
      key-serializer: org.apache.kafka.common.serialization.LongSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
      transaction-id-prefix: lib-events-tx-    # <-- add this
      properties:
        retry.backoff.ms: 1000
        enable.idempotence: true
```

Spring Boot auto-configures a `KafkaTransactionManager` bean as soon as `transaction-id-prefix` is set.

#### Step 3 — Annotate the Service Methods

```java
// LibraryEventService.java
import org.springframework.transaction.annotation.Transactional;

@Service
public class LibraryEventService {

    @Transactional                     // <-- Kafka transaction begins here
    public CompletableFuture<LibraryEvent> createLibraryEvent(LibraryEvent libraryEvent) {
        return libraryEventProducer.sendLibraryEvent(libraryEvent)
                .thenApply(_ -> libraryEvent)
                .exceptionally(ex -> {
                    throw new LibraryEventPublishException(
                            "Failed to publish library event to Kafka", ex.getCause());
                });
    }

    @Transactional                     // <-- Kafka transaction begins here
    public CompletableFuture<LibraryEvent> updateLibraryEvent(LibraryEvent libraryEvent) {
        return libraryEventProducer.sendLibraryEvent(libraryEvent)
                .thenApply(_ -> libraryEvent)
                .exceptionally(ex -> {
                    throw new LibraryEventPublishException(
                            "Failed to publish library event to Kafka", ex.getCause());
                });
    }
}
```

---

## Is This Only a Producer Concern?

No. Transactions span **both the producer and the consumer**. Each side has its own role:

| Side | Concern | Mechanism |
|------|---------|-----------|
| **Producer** | Publish messages atomically — all or nothing | `transactional.id`, `KafkaTransactionManager`, `@Transactional` |
| **Consumer** | Only read messages from committed transactions | `isolation.level = read_committed` |
| **Consumer-Producer** | Consume, process, and publish atomically as one unit | `sendOffsetsToTransaction()` + producer transaction |

The consumer side is where the picture becomes complete. Without it, a transactional producer alone does not give you full end-to-end exactly-once guarantees.

---

## Part 2 — Consumer-Side Transactions

### Consumer Isolation Level

The consumer side of the transaction story starts with **isolation level**. This controls what messages a consumer can see.

| `isolation.level` | Behaviour |
|-------------------|-----------|
| `read_uncommitted` (default) | Reads all messages — including those inside an open transaction and those from aborted transactions |
| `read_committed` | Reads only messages from **committed** transactions — aborted and in-flight messages are invisible |

```yaml
# Consumer service application.yml
spring:
  kafka:
    consumer:
      isolation-level: read_committed
```

**Why this matters:** If the producer sends a message inside a transaction and then aborts it, a `read_uncommitted` consumer will have already processed that message — a phantom event that should never have existed. A `read_committed` consumer simply never sees it.

Any consumer that must only process fully committed events should use `read_committed`. This is especially important for downstream services that react to validated events — they must never see messages from a transaction that was later aborted.

---

### The Problem Without Consumer Transactions

Consider a consumer whose job is:

1. **Consume** a message from a topic
2. **Process** it (validate, enrich, transform)
3. **Persist** to a database
4. **Publish** a result to another topic

Without consumer transactions, this flow has a critical gap:

```
Consume message
  │
  ├── Process ✓
  ├── Persist to database ✓
  ├── Publish to output topic ✓
  └── Commit consumer offset ✓  ← happens automatically

What if the app crashes AFTER publishing but BEFORE committing the offset?
→ On restart, the message is reprocessed
→ Database gets a duplicate write
→ Output topic gets a duplicate message
→ Downstream consumers process the same event twice
```

This is the **at-least-once** problem. Consumer transactions solve it.

---

### Consume-Transform-Produce (CTP) Pattern

This is the key consumer-side transaction concept. It ties the **consumer offset commit** to the **producer transaction**, so both succeed or both fail as one atomic unit.

```
Consumer reads message from input-topic
  │
  ├── Producer begins a transaction
  │
  ├── Process + Persist to database
  │
  ├── KafkaTemplate.send("output-topic", ...)    ← inside transaction
  │
  ├── sendOffsetsToTransaction(consumerOffset)   ← offset commit joins transaction
  │
  └── Transaction commits atomically:
        ├── output-topic message becomes visible
        └── Consumer offset for input-topic is advanced
            (broker records this — not the consumer group coordinator)
```

The crucial mechanism is **`sendOffsetsToTransaction()`**. Instead of committing the consumer offset to the consumer group coordinator (the normal path), the offset is written as part of the Kafka transaction. This means:

- If the transaction **commits** → message is published AND offset is advanced → no reprocessing
- If the transaction **aborts** → message is NOT published AND offset is NOT advanced → message is reprocessed → no duplicates

This is **Exactly-Once Stream Processing (EOS)** — the gold standard for stateful Kafka pipelines.

---

### How Spring Kafka Implements CTP

Spring Kafka handles `sendOffsetsToTransaction()` automatically when you combine:

1. A `@KafkaListener` method annotated with `@Transactional`
2. A `KafkaTemplate` configured with a `transaction-id-prefix`
3. `enable.auto.commit = false` on the consumer (Spring Kafka sets this by default when using `@KafkaListener`)

Spring's `KafkaMessageListenerContainer` detects the active transaction and calls `sendOffsetsToTransaction()` before committing — you do not call it manually.

```yaml
# Consumer-Producer service application.yml
spring:
  kafka:
    consumer:
      isolation-level: read_committed
      enable-auto-commit: false        # Spring Kafka manages offset commits
    producer:
      transaction-id-prefix: my-service-tx-
      acks: all
      properties:
        enable.idempotence: true
```

```java
@Component
public class EventProcessor {

    @Transactional                              // begins a Kafka producer transaction
    @KafkaListener(topics = "input-topic", groupId = "my-consumer-group")
    public void onMessage(ConsumerRecord<Long, MyEvent> record) {

        // Step 1: Process and persist
        myService.process(record.value());

        // Step 2: Publish result (inside the transaction)
        kafkaTemplate.send("output-topic", record.key(), record.value());

        // Spring Kafka automatically calls sendOffsetsToTransaction() here
        // The consumer offset for input-topic is committed AS PART OF the transaction
    }
}
```

---

### Consumer Transaction Flow Diagram

```
input-topic
  │
  │  Consumer reads record (offset not yet committed)
  ▼
@KafkaListener + @Transactional
  │
  ├── Producer transaction BEGINS
  │
  ├── process() + persist to database
  │
  ├── kafkaTemplate.send("output-topic", ...)
  │     ↳ message is STAGED — not yet visible to read_committed consumers
  │
  ├── Spring Kafka calls sendOffsetsToTransaction(currentOffset)
  │     ↳ consumer offset joins the producer transaction
  │
  └── Transaction COMMITS
        ├── output-topic message is NOW visible
        └── Consumer offset for input-topic is advanced
              ↳ on restart, this message will NOT be reprocessed

[On any exception]
  Transaction ABORTS
    ├── output-topic message is discarded — never visible
    └── Consumer offset is NOT advanced
          ↳ on restart, this message WILL be reprocessed (retry)
```

---

## How to Enable Transactions in This Project (Summary)

### Step 4 — Set Consumer Isolation Level (Downstream Services)

```yaml
# MicroService 3, 4, 5 — application.yml
spring:
  kafka:
    consumer:
      isolation-level: read_committed
      enable-auto-commit: false
```

---

## Does This Project Benefit from EOS?

This project sends a **single message per REST request**. That changes which EOS features are genuinely useful here and which are not.

### Idempotent Producer — `enable.idempotence: true` ✅ Already enabled, genuinely useful

When `KafkaTemplate.send()` times out or the network drops, the producer retries. Without idempotence, the broker may have already written the message and the retry creates a **duplicate** — the librarian's ADD or UPDATE event lands twice on the topic. With idempotence, the broker deduplicates using sequence numbers per partition, so the message lands **exactly once** even under retries.

This is the one EOS feature that directly benefits this project as-is.

### Transactions — `transaction-id-prefix` ❌ Not meaningful here

Transactions shine when you need to atomically send **multiple messages together** — all succeed or all fail. Since this project sends one message per REST call, wrapping a single send in a transaction gives no additional guarantee beyond what idempotence already provides. The cost (added latency, Transaction Coordinator overhead) outweighs any benefit.

Transactions would become meaningful here if a single librarian action needed to publish **multiple events atomically** — for example, a batch of library events where all must succeed or none should be visible.

### `read_committed` — ❌ Not applicable here

This project is a **producer**. The `read_committed` isolation level belongs on the **consumer side** — on the downstream services that read from `library-events`. It has no effect on a service that only produces.

### Summary for This Project

| EOS Feature | Useful Here? | Reason |
|-------------|-------------|--------|
| `enable.idempotence` | ✅ Yes | Prevents duplicate messages on producer retry |
| Transactions | ❌ Not yet | Single message per request — nothing to group atomically |
| `read_committed` | ❌ Not here | Producer service — applies to downstream consumers only |

---

## Important Considerations

### Performance Trade-off
Transactions add latency. The broker must write transaction markers and coordinate with the Transaction Coordinator before consumers can read messages. For high-throughput, low-latency use cases, weigh whether EOS is necessary.

### Transactional ID Must Be Stable
The `transaction-id-prefix` must remain consistent across restarts. Changing it means the broker treats the new producer as a different transactional producer and the old one becomes a zombie that will be fenced off.

### `acks=all` Is Mandatory
Transactions require `acks=all`. If you set `acks=1` or `acks=0`, the broker will reject the transactional producer registration.

### `enable.auto.commit` Must Be False for CTP
The Consume-Transform-Produce pattern requires manual offset management. Spring Kafka sets this automatically when you use `@KafkaListener`, but verify it is not overridden to `true` in your config.

### Avoid Mixing Transactional and Non-Transactional Sends
By default, once a `KafkaTemplate` is backed by a transaction-capable producer (`transaction-id-prefix`), calling `send()` outside an active transaction can throw:

`IllegalStateException: No transaction is in process ...`

Spring Kafka provides an escape hatch for mixed-mode usage:

```java
kafkaTemplate.setAllowNonTransactional(true);
```

With this flag enabled:
- If a Kafka transaction is active, sends participate in that transaction.
- If no transaction is active, sends are allowed as normal non-transactional sends.

This is useful in integration tests where the same app context may exercise both transactional and non-transactional paths.

#### Test-only override used in this project

In `LibraryEventsControllerIntegrationTest`, we use a test-scoped `KafkaTemplate` bean to avoid changing global runtime behavior:

```java
@TestConfiguration
static class TestKafkaTemplateConfig {

    @Bean
    @Primary
    KafkaTemplate<Long, LibraryEvent> kafkaTemplate(ProducerFactory<Long, LibraryEvent> producerFactory) {
        KafkaTemplate<Long, LibraryEvent> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setAllowNonTransactional(true);
        return kafkaTemplate;
    }
}
```

Recommendation:
- **Production default:** keep strict behavior (do not rely on `allowNonTransactional=true`) so missing transaction boundaries fail fast.
- **Tests/dev mixed flows:** enable `allowNonTransactional=true` in test scope when needed.
