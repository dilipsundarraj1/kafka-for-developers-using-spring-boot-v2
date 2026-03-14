# Kafka Consumer Concepts — Hands-On Guide

> **Prerequisites:** Step 2 of the implementation plan is complete. The consumer reads
> from `library-events`, deserializes via `JsonDeserializer`, and logs the
> `LibraryEventDto`. No DB persistence is needed for these exercises.

---

## Table of Contents

- [1. Consumer Groups and Rebalance — Hands On](#1-consumer-groups-and-rebalance--hands-on)
  - [What Is a Consumer Group?](#what-is-a-consumer-group)
  - [Hands-On: Observe Rebalance](#hands-on-observe-rebalance)
  - [Key Takeaways](#key-takeaways)
- [2. Default Consumer Offset Management in Spring Kafka — Hands On](#2-default-consumer-offset-management-in-spring-kafka--hands-on)
  - [What Is an Offset?](#what-is-an-offset)
  - [Spring Kafka Default Behavior](#spring-kafka-default-behavior)
  - [Hands-On: Observe Default Offset Behavior](#hands-on-observe-default-offset-behavior)
  - [`latest` vs `earliest`](#latest-vs-earliest)
- [3. Manual Consumer Offset Management — Hands On](#3-manual-consumer-offset-management--hands-on)
  - [Why Manual Offset Management?](#why-manual-offset-management)
  - [Ack Modes Reference](#ack-modes-reference)
  - [Hands-On: Switch to MANUAL Acknowledgment](#hands-on-switch-to-manual-acknowledgment)
  - [When to Use Manual Ack](#when-to-use-manual-ack)
- [4. Concurrent Consumers — Hands On](#4-concurrent-consumers--hands-on)
  - [What Is Listener Concurrency?](#what-is-listener-concurrency)
  - [Hands-On: Enable Concurrent Consumers](#hands-on-enable-concurrent-consumers)
  - [Concurrency vs Multiple Instances](#concurrency-vs-multiple-instances)
  - [The Golden Rule](#the-golden-rule)
  - [Deciding Concurrency for This Project](#deciding-concurrency-for-this-project)
- [Summary](#summary)

---

## 1. Consumer Groups and Rebalance — Hands On

### What Is a Consumer Group?

Every Kafka consumer belongs to a **consumer group** (identified by `group.id`). Kafka uses the
group to distribute partitions across consumer instances:

```
Topic: library-events (3 partitions)

Consumer Group: library-events-listener-group
┌─────────────────────────────────────────────────┐
│                                                 │
│  Consumer-1 ← Partition 0, Partition 1          │
│  Consumer-2 ← Partition 2                       │
│                                                 │
└─────────────────────────────────────────────────┘
```

**Key rules:**
- Each partition is assigned to **exactly one** consumer within a group.
- If there are more consumers than partitions, the extra consumers sit **idle**.
- If a consumer joins or leaves, Kafka **rebalances** — reassigns partitions across the remaining consumers.

### Our Current Configuration

```yaml
# application.yml
spring:
  kafka:
    consumer:
      group-id: library-events-listener-group   # ← our consumer group
```

All instances of this application that start with this `group-id` form **one consumer group**.

### Hands-On: Observe Rebalance

#### Step 1: Check topic partitions

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 --describe --topic library-events
```

Note the **partition count** (e.g., 3).

#### Step 2: Start the first consumer instance

```bash
# Terminal 1 — start the Spring Boot app
./gradlew bootRun
```

Watch the logs. You'll see something like:

```
library-events-listener-group: partitions assigned: [library-events-0, library-events-1, library-events-2]
```

**One consumer owns all 3 partitions.**

#### Step 3: Start a second consumer instance

```bash
# Terminal 2 — start another instance on a different port
SERVER_PORT=8082 ./gradlew bootRun
```

Watch the logs on **both** terminals. You'll see a **rebalance**:

```
# Terminal 1 (first instance)
library-events-listener-group: partitions revoked: [library-events-0, library-events-1, library-events-2]
library-events-listener-group: partitions assigned: [library-events-0, library-events-1]

# Terminal 2 (second instance)
library-events-listener-group: partitions assigned: [library-events-2]
```

Kafka redistributed the partitions.

#### Step 4: Stop the second instance (Ctrl+C on Terminal 2)

Watch Terminal 1 — another rebalance:

```
library-events-listener-group: partitions assigned: [library-events-0, library-events-1, library-events-2]
```

The first instance takes back all partitions.

#### Step 5: Start a consumer with a DIFFERENT group

```bash
# Terminal 2 — different group-id
SERVER_PORT=8082 SPRING_KAFKA_CONSUMER_GROUP_ID=library-events-listener-group-2 ./gradlew bootRun
```

Now **both** consumers receive **every** message (each group gets its own copy). This is the
**fan-out / broadcast** pattern.

### Key Takeaways

| Scenario | Behavior |
|----------|----------|
| 1 consumer, 3 partitions | Consumer reads all 3 partitions |
| 2 consumers, same group, 3 partitions | Partitions split (e.g., 2+1) |
| 3 consumers, same group, 3 partitions | 1 partition each — ideal |
| 4 consumers, same group, 3 partitions | 1 consumer is **idle** |
| 2 consumers, **different** groups | Both get all messages (broadcast) |
| Consumer joins/leaves | **Rebalance** — partitions redistributed |

---

## 2. Default Consumer Offset Management in Spring Kafka — Hands On

### What Is an Offset?

Every message in a Kafka partition has a sequential **offset** (0, 1, 2, ...). The consumer
group tracks the **last committed offset** per partition — this is how Kafka knows where to
resume after a restart.

```
Partition 0:  [msg-0] [msg-1] [msg-2] [msg-3] [msg-4] [msg-5]
                                        ↑
                                  committed offset = 3
                                  (next poll starts here)
```

### Spring Kafka Default Behavior

Spring Kafka uses **`AckMode.BATCH`** by default:

| Setting | Default | Meaning |
|---------|---------|---------|
| `ack-mode` | `BATCH` | Offsets are committed after **all records** from the last `poll()` are processed |
| `enable-auto-commit` | `false` (Spring sets this) | Kafka's native auto-commit is disabled; Spring manages commits |
| `auto-offset-reset` | `latest` (our config) | If no committed offset exists, start from the latest message |

This means: Spring calls `consumer.poll()`, gets a batch of records, calls your `@KafkaListener`
for each one, and **after all succeed**, commits the offsets.

### Hands-On: Observe Default Offset Behavior

#### Step 1: Produce some messages BEFORE starting the consumer

```bash
# Stop the consumer app if running
# Make sure the library-events-producer-api is running on port 8080

# Produce 3 messages using the producer API
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'

curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 2,
      "bookName": "Spring Boot in Action",
      "bookAuthor": "Craig Walls"
    }
  }'

curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 3,
      "bookName": "Effective Java",
      "bookAuthor": "Joshua Bloch"
    }
  }'
```

#### Step 2: Start the consumer with `auto-offset-reset: latest`

```bash
./gradlew bootRun
```

**Result:** The 3 messages are **NOT consumed**. With `latest`, the consumer starts
at the end of the partition when there's no committed offset.

#### Step 3: Change to `earliest` and restart

```yaml
# application.yml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest
```

**But wait** — if the group already has a committed offset, `auto-offset-reset` is **ignored**.
You need to use a **new group-id** to see the effect:

```bash
SPRING_KAFKA_CONSUMER_GROUP_ID=test-earliest-group ./gradlew bootRun
```

**Result:** All 3 messages are consumed from the beginning.

#### Step 4: Check committed offsets

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 \
  --group library-events-listener-group --describe
```

Output:

```
GROUP                             TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
library-events-listener-group     library-events   0          5               5               0
library-events-listener-group     library-events   1          3               3               0
library-events-listener-group     library-events   2          4               4               0
```

- **CURRENT-OFFSET:** Last committed offset for this group.
- **LOG-END-OFFSET:** Latest offset in the partition.
- **LAG:** How far behind the consumer is (0 = caught up).

#### Step 5: Restart the consumer — observe it resumes from committed offset

```bash
./gradlew bootRun
```

No old messages are re-consumed. The consumer picks up from where it left off.

### `latest` vs `earliest`

| `auto-offset-reset` | When it applies | Behavior |
|---------------------|-----------------|----------|
| `latest` | **Only** when no committed offset exists for the group | Skip all existing messages, consume only new ones |
| `earliest` | **Only** when no committed offset exists for the group | Consume from the very beginning of the partition |
| *(either)* | When a committed offset **exists** | **Ignored** — resumes from committed offset |

---

## 3. Manual Consumer Offset Management — Hands On

### Why Manual Offset Management?

With the default `BATCH` ack mode, offsets are committed **after all records in a batch succeed**.
But what if you want finer control?

- Commit after **each record** (at-least-once, record-level granularity).
- Commit **only after successful DB persistence** (avoid data loss).
- **Don't commit** if processing fails (force re-delivery).

Spring Kafka provides `AckMode.MANUAL` and `AckMode.MANUAL_IMMEDIATE` for this.

### Ack Modes Reference

| AckMode | Behavior |
|---------|----------|
| `BATCH` (default) | Commit after all records from `poll()` are processed |
| `RECORD` | Commit after **each** record is processed |
| `MANUAL` | You call `acknowledgment.acknowledge()` — committed on next `poll()` |
| `MANUAL_IMMEDIATE` | You call `acknowledgment.acknowledge()` — committed **immediately** |

### Hands-On: Switch to MANUAL Acknowledgment

#### Step 1: Update the container factory to use MANUAL ack mode

```java
// LibraryEventsConsumerConfig.java

@Bean
KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, LibraryEventDto>> kafkaListenerContainerFactory(
        ConsumerFactory<Integer, LibraryEventDto> consumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    return factory;
}
```

#### Step 2: Update the listener to accept `Acknowledgment`

```java
// LibraryEventsConsumer.java

@KafkaListener(topics = "library-events")
public void onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord,
                      Acknowledgment acknowledgment) {
    log.info("ConsumerRecord : {}", consumerRecord);
    libraryEventService.processEvent(consumerRecord);
    acknowledgment.acknowledge();  // ← manually commit offset
}
```

#### Step 3: Test — observe that offsets are committed only when you call `acknowledge()`

Send a message via the producer API:

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

- Start the app, send a message → offset committed (check with `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --group library-events-listener-group --describe`).
- Comment out `acknowledgment.acknowledge()`, restart, send a message → offset is **NOT committed**.
- Restart the app again → the same message is **re-delivered** (offset wasn't committed).

#### Step 4: Revert to default (BATCH) for this project

For our project, `BATCH` mode is sufficient because:
- The error handler + retry + DLT pipeline handles failures.
- We don't need per-record commit granularity.

**Revert the changes** — remove the `setAckMode(MANUAL)` line and the `Acknowledgment` parameter.

### When to Use Manual Ack

| Use Case | Recommended AckMode |
|----------|---------------------|
| Simple consumer, error handler manages retries | `BATCH` (default) |
| Must commit only after confirmed DB write | `MANUAL` or `MANUAL_IMMEDIATE` |
| High-throughput, occasional failures acceptable | `BATCH` |
| Exactly-once semantics required | `MANUAL` + idempotent writes |

---

## 4. Concurrent Consumers — Hands On

### What Is Listener Concurrency?

By default, Spring Kafka creates **one** `KafkaConsumer` thread per `@KafkaListener`. To
parallelize consumption within a single application instance, you set **concurrency** — this
creates multiple consumer threads in the **same consumer group**.

```
concurrency = 3, topic has 3 partitions

Application Instance (single JVM)
┌──────────────────────────────────────────────────┐
│                                                  │
│  Thread-0 (KafkaConsumer) ← Partition 0          │
│  Thread-1 (KafkaConsumer) ← Partition 1          │
│  Thread-2 (KafkaConsumer) ← Partition 2          │
│                                                  │
└──────────────────────────────────────────────────┘
```

Each thread is an independent `KafkaConsumer` that owns one or more partitions.

### Hands-On: Enable Concurrent Consumers

#### Step 1: Set concurrency in the container factory

```java
// LibraryEventsConsumerConfig.java

@Bean
KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, LibraryEventDto>> kafkaListenerContainerFactory(
        ConsumerFactory<Integer, LibraryEventDto> consumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(3);  // ← 3 consumer threads
    return factory;
}
```

#### Step 2: Start the app and observe thread assignment

```bash
./gradlew bootRun
```

In the logs, you'll see **three** consumers joining the group:

```
[consumer-0-C-1] Assigned: [library-events-0]
[consumer-1-C-1] Assigned: [library-events-1]
[consumer-2-C-1] Assigned: [library-events-2]
```

#### Step 3: Produce messages and observe parallel processing

Send several messages via the producer API and watch the logs:

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'

curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 2,
      "bookName": "Spring Boot in Action",
      "bookAuthor": "Craig Walls"
    }
  }'

curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 3,
      "bookName": "Effective Java",
      "bookAuthor": "Joshua Bloch"
    }
  }'
```

Messages from different partitions are processed by **different threads** concurrently:

```
[consumer-0-C-1] ConsumerRecord : ... partition=0 ...
[consumer-2-C-1] ConsumerRecord : ... partition=2 ...
[consumer-1-C-1] ConsumerRecord : ... partition=1 ...
```

#### Step 4: Try concurrency > partition count

Set `factory.setConcurrency(5)` with only 3 partitions:

```
[consumer-0-C-1] Assigned: [library-events-0]
[consumer-1-C-1] Assigned: [library-events-1]
[consumer-2-C-1] Assigned: [library-events-2]
[consumer-3-C-1] Assigned: []   ← IDLE, no partitions
[consumer-4-C-1] Assigned: []   ← IDLE, no partitions
```

**2 threads are wasted.** Concurrency should be ≤ partition count.

#### Alternative: Set via application.yml

Instead of setting concurrency in Java config, you can use a property:

```yaml
# application.yml
spring:
  kafka:
    listener:
      concurrency: 3
```

And in the config, read it automatically (Spring Boot auto-configuration handles this if you
don't define your own factory bean). If you define a custom factory, wire it explicitly:

```java
@Bean
KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, LibraryEventDto>> kafkaListenerContainerFactory(
        ConsumerFactory<Integer, LibraryEventDto> consumerFactory,
        @Value("${spring.kafka.listener.concurrency:1}") int concurrency) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(concurrency);
    return factory;
}
```

### Concurrency vs Multiple Instances

| Approach | How It Works | When to Use |
|----------|-------------|-------------|
| `concurrency=3` in one instance | 3 threads, 1 JVM | Single machine, moderate throughput |
| 3 separate app instances, `concurrency=1` each | 3 JVMs, 1 thread each | Distributed deployment, high availability |
| 3 instances × `concurrency=3` | 9 consumers total, but only 3 partitions → 6 idle | Wasteful — don't do this |

### The Golden Rule

> **Total consumers (across all instances) should equal the partition count for optimal throughput.**
>
> `total consumers = instances × concurrency ≤ partition count`

### Deciding Concurrency for This Project

| Topic Partitions | Deployment | Recommended Concurrency |
|------------------|-----------|-------------------------|
| 3 | 1 instance (dev) | `concurrency: 3` |
| 3 | 3 instances (prod) | `concurrency: 1` |
| 6 | 2 instances (prod) | `concurrency: 3` |

---

## Summary

| Concept | Key Setting | Our Project Default |
|---------|------------|---------------------|
| Consumer Groups | `spring.kafka.consumer.group-id` | `library-events-listener-group` |
| Offset Reset | `spring.kafka.consumer.auto-offset-reset` | `latest` |
| Ack Mode | `ContainerProperties.AckMode` | `BATCH` (Spring default) |
| Concurrency | `factory.setConcurrency(n)` | `1` (default, configurable) |

> **Next step:** Proceed to Step 3 of the implementation plan — Domain Model + Repository + DB Persistence.

