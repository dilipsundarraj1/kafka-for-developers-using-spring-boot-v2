# Kafka Consumer Error Handling, Retry & Recovery

This reference doc is for students implementing consumer error handling strategies in the Library Events Consumer project.

It keeps the same technical content, but organizes it in an implementation-first format.

## Table of Contents

- [How to Use This Reference](#how-to-use-this-reference)
- [Error Handling Dependency Flow](#error-handling-dependency-flow)
- [Mapping to Current Project](#mapping-to-current-project)
- [Exploring Current Consumer Behavior](#exploring-current-consumer-behavior)
  - [Step 1 — Start the Console Producer](#step-1--start-the-console-producer)
  - [Step 2 — Publish an Invalid Message](#step-2--publish-an-invalid-message)
  - [Step 3 — Observed Consumer Behavior](#step-3--observed-consumer-behavior)
- [Part 1: Spring Kafka Error Handling Infrastructure](#part-1-spring-kafka-error-handling-infrastructure)
  - [1) Spring Kafka Error Handling Architecture](#1-spring-kafka-error-handling-architecture)
  - [2) DefaultErrorHandler](#2-defaulterrorhandler)
  - [3) Retry with BackOff Strategies](#3-retry-with-backoff-strategies)
  - [4) RetryListener](#4-retrylistener)
- [Part 2: Error Classification](#part-2-error-classification)
  - [5) Types of Errors in a Kafka Consumer](#5-types-of-errors-in-a-kafka-consumer)
  - [6) Classifying Retryable vs Non-Retryable Exceptions](#6-classifying-retryable-vs-non-retryable-exceptions)
    - [FixedBackOff](#fixedbackoff)
    - [ExponentialBackOff](#exponentialbackoff)
    - [Which One to Use?](#which-one-to-use)
- [Part 3: Recovery Strategies](#part-3-recovery-strategies)
  - [Overview](#overview)
  - [7) Dead Letter Topic (DLT)](#7-dead-letter-topic-dlt)
  - [8) Custom Recovery Strategies](#8-custom-recovery-strategies)
    - [Log and Skip](#log-and-skip)
    - [Persist to a Failure Table](#persist-to-a-failure-table)
    - [Publish to DLT + Persist](#publish-to-dlt--persist)
  - [Bonus) Retrying Failed Records from the Database](#bonus-retrying-failed-records-from-the-database)
- [Part 4: Wiring It Together](#part-4-wiring-it-together)
  - [9) Manual Acknowledgment and Error Handling](#9-manual-acknowledgment-and-error-handling)
  - [10) Full Configuration: LibraryEventsConsumerConfig.java](#10-full-configuration-libraryeventsconsumerconfigjava)
  - [11) End-to-End Flow with Error Handling](#11-end-to-end-flow-with-error-handling)
- [Part 5: Testing Error Handling](#part-5-testing-error-handling)
  - [12) Retry and Recovery in Tests](#12-retry-and-recovery-in-tests)
- [Suggested Implementation Order](#suggested-implementation-order)
- [Implementation Checklist](#implementation-checklist)

---

## How to Use This Reference

Use this in sequence while implementing:

1. Understand the Spring Kafka error handling architecture — how `DefaultErrorHandler`, `BackOff`, and `RecoveryCallback` fit together (Part 1, Section 1).
2. Configure `DefaultErrorHandler` with the right `BackOff` strategy and wire it into `ConcurrentKafkaListenerContainerFactory` (Part 1, Sections 2–3).
3. Add a `RetryListener` for observability — log each delivery attempt as it happens (Part 1, Section 4).
4. Classify your exceptions — decide which errors are retryable and which go straight to recovery (Part 2, Sections 5–6).
5. Wire in a `DeadLetterPublishingRecoverer` and choose a recovery strategy (Part 3, Sections 7–8).
6. Understand how manual acknowledgment interacts with the error handler, then wire everything together (Part 4, Sections 9–11).
7. Validate behavior with retry- and DLT-focused integration tests (Part 5, Section 12).

---

## Error Handling Dependency Flow

```text
Exception thrown in @KafkaListener
          |
          v
DefaultErrorHandler  ──────────────────────────────────────────────
          |                                                         |
          | Is it non-retryable?                                   |
          | (IllegalArgumentException, DataIntegrityViolation...)  |
          |                                                         |
         YES                                                        NO
          |                                                         |
          v                                                         v
Skip retries immediately                          BackOff (FixedBackOff / ExponentialBackOff)
          |                                                         |
          |                                              Retry 1 → Retry 2 → Retry 3
          |                                                         |
          |                                              All retries exhausted?
          |                                                         |
          └──────────────────────┬──────────────────────────────────┘
                                 v
                     RecoveryCallback (ConsumerRecordRecoverer)
                                 |
              ┌──────────────────┼──────────────────┐
              v                  v                   v
       Log and Skip     DeadLetterPublishing    Persist to DB
                           Recoverer                  +
                                |               Publish to DLT
                                v
                    library-events.DLT
                                |
                                v
                    Offset committed by ErrorHandler
                    Next message consumed — no partition block
```

---

## Mapping to Current Project

| Concern | Current State | Action Needed |
|---|---|---|
| Exception classification | No classification — all exceptions treated the same | Add `addNotRetryableExceptions(...)` for permanent failures |
| Retry on failure | None — first failure is final | Configure `DefaultErrorHandler` with `FixedBackOff` |
| Dead letter handling | None — failed messages are lost silently | Wire `DeadLetterPublishingRecoverer` |
| Acknowledgment | Success path only — `acknowledge()` called after `processEvent()` succeeds | Already correct; ensure `DefaultErrorHandler` is wired so exceptions propagate to it |
| Retry observability | None | Add `RetryListener` to log each retry attempt |
| Testing retries | Not tested | Add `@SpyBean` tests with simulated failures |

---

## Exploring Current Consumer Behavior

This section demonstrates what happens when an incompatible message is published to `library-events` — one that cannot be deserialized into `LibraryEventDto`.

### Step 1 — Start the Console Producer

```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:19092 \
  --topic library-events
```

### Step 2 — Publish an Invalid Message

At the `>` prompt, paste an UPDATE event with a null `libraryEventId` (invalid — UPDATE requires a non-null ID).

> **Important:** `kafka-console-producer` sends each line as a separate message. Always paste JSON as a single line, otherwise each line becomes its own message and causes a `SerializationException`. Also note the field name must be `libraryEventType` (not `eventType`) to match the `LibraryEventDto` record.

```
{"libraryEventId":null,"libraryEventType":"UPDATE","book":{"bookId":1,"bookName":"Kafka: The Definitive Guide","bookAuthor":"Neha Narkhede"}}
```

**Observed error log (DefaultErrorHandler):**

```
INFO  c.l.consumer.LibraryEventsConsumer - ConsumerRecord : ConsumerRecord(topic = library-events, partition = 0, offset = 71, value = LibraryEventDto[libraryEventId=null, libraryEventType=UPDATE, book=BookDto[bookId=1, bookName=Kafka: The Definitive Guide, bookAuthor=Neha Narkhede]])
INFO  c.l.service.LibraryEventService   - LibraryEventDto : LibraryEventDto[libraryEventId=null, libraryEventType=UPDATE, ...]
ERROR o.s.kafka.listener.DefaultErrorHandler - Backoff FixedBackOffExecution[interval=0, currentAttempts=10, maxAttempts=9] exhausted for library-events-0@71

org.springframework.kafka.listener.ListenerExecutionFailedException: Listener method threw exception
```

Key observations:
- The message **deserializes successfully** — the consumer and service both log it before the error
- `currentAttempts=10, maxAttempts=9` — the `IllegalArgumentException` thrown by the validation **is retryable**, so `DefaultErrorHandler` exhausts all 9 retries before giving up
- Contrast with `hello world`: deserialization errors skip retries entirely (`maxAttempts=0`); application-level exceptions go through the full retry cycle

At the `>` prompt, type a plain string that is not valid JSON:

```
> hello world
```

Press `Ctrl+C` to exit the producer.

**Observed error log (DefaultErrorHandler):**

```
ERROR o.s.kafka.listener.DefaultErrorHandler - Backoff FixedBackOffExecution[interval=0, currentAttempts=1, maxAttempts=0] exhausted for library-events-0@18

org.springframework.kafka.listener.ListenerExecutionFailedException: Listener failed
Caused by: org.springframework.kafka.support.serializer.DeserializationException: failed to deserialize
Caused by: org.apache.kafka.common.errors.SerializationException: Can't deserialize data from topic [library-events]
Caused by: com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'hello': was expecting
  (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
  at [Source: UNKNOWN; line: 1, column: 6]
```

Key observations:
- `maxAttempts=0` — no retries; `DefaultErrorHandler` skips the backoff loop entirely for deserialization errors
- The failure bubbles up as a `DeserializationException` wrapping a `SerializationException` wrapping a `JsonParseException`

---

## Part 1: Spring Kafka Error Handling Infrastructure

### 1) Spring Kafka Error Handling Architecture

**What**
- Spring Kafka's error handling stack sits between the Kafka container and the listener method. Exceptions thrown by the listener are caught by `DefaultErrorHandler`, which orchestrates retries and recovery without blocking the partition.

**How it works internally**
```
Kafka Broker
     |
     v
KafkaMessageListenerContainer
     |
     | calls listener method
     v
@KafkaListener method (LibraryEventsConsumer.onMessage)
     |
     | throws exception
     v
DefaultErrorHandler                        ← configured on the container factory
     |
     |── Is this exception non-retryable?
     |       YES → skip retries → go to RecoveryCallback immediately
     |       NO  → retry with BackOff
     |
     |── Retries exhausted?
     |       YES → RecoveryCallback (e.g., DeadLetterPublishingRecoverer)
     |       NO  → retry after backoff delay
     v
RecoveryCallback
     |── DeadLetterPublishingRecoverer → publish to <topic>.DLT
     |── Custom recovery → log, persist to failure table, alert
```

The key component is `DefaultErrorHandler`, which replaced the older `SeekToCurrentErrorHandler` in Spring Kafka 2.8+.

**Why it matters**
- Without `DefaultErrorHandler`, an unhandled exception from the listener propagates up to the container, which logs it and moves on — the message is silently dropped. `DefaultErrorHandler` ensures no message is lost without a deliberate recovery decision.

---

### 2) DefaultErrorHandler

**What**
- `DefaultErrorHandler` is a `CommonErrorHandler` implementation that catches exceptions thrown by the listener method, applies the configured backoff and retry logic, and delegates to a `RecoveryCallback` when retries are exhausted.

**How it works internally**
1. Catches exceptions thrown by the listener method.
2. Checks if the exception is classified as non-retryable — if so, immediately invokes the recovery callback.
3. If retryable, waits for the configured `BackOff` interval and retries the same record.
4. After all retry attempts are exhausted, invokes the recovery callback.
5. After recovery, acknowledges the offset and moves to the next record — **the partition is not blocked**.

**How to configure it**

`DefaultErrorHandler` is registered on the `ConcurrentKafkaListenerContainerFactory` bean in `LibraryEventsConsumerConfig`:

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>
        kafkaListenerContainerFactory(
            ConsumerFactory<Integer, LibraryEventDto> consumerFactory,
            DefaultErrorHandler errorHandler) {

    ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    factory.setCommonErrorHandler(errorHandler);   // ← register here

    return factory;
}
```

**Why it matters**
- This is the central orchestrator of the consumer error handling strategy. All retry and recovery flows pass through it.

---

### 3) Retry with BackOff Strategies

**What**
- `BackOff` determines how long `DefaultErrorHandler` waits between retry attempts. The right choice depends on the nature of the transient failure — short DB blip vs. prolonged downstream outage.

#### FixedBackOff

Retries at a fixed interval — same wait time between every attempt.

```java
// Retry up to 3 times, wait 1 second between each attempt
FixedBackOff fixedBackOff = new FixedBackOff(1000L, 3L);
//                                             ↑       ↑
//                                          interval  maxAttempts
DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, fixedBackOff);
```

**Retry timeline:**
```
Message fails at T=0
  ↓ wait 1s
Retry 1 at T=1s   → fails
  ↓ wait 1s
Retry 2 at T=2s   → fails
  ↓ wait 1s
Retry 3 at T=3s   → fails
  ↓
Recovery invoked  → publish to DLT
```

**When to use:** Simple scenarios where the issue is expected to resolve quickly (e.g., short DB connection blip).

#### ExponentialBackOff

Retries with exponentially increasing wait times — avoids hammering a struggling downstream system.

```java
ExponentialBackOff exponentialBackOff = new ExponentialBackOff();
exponentialBackOff.setInitialInterval(1_000L);   // start at 1 second
exponentialBackOff.setMultiplier(2.0);            // double each time
exponentialBackOff.setMaxInterval(10_000L);       // cap at 10 seconds
exponentialBackOff.setMaxElapsedTime(30_000L);    // stop after 30 seconds total

DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, exponentialBackOff);
```

**Retry timeline:**
```
Message fails at T=0
  ↓ wait 1s
Retry 1 at T=1s   → fails
  ↓ wait 2s
Retry 2 at T=3s   → fails
  ↓ wait 4s
Retry 3 at T=7s   → fails
  ↓ wait 8s
Retry 4 at T=15s  → fails
  ↓ wait 10s (capped)
Retry 5 at T=25s  → fails
  ↓ 30s total elapsed
Recovery invoked  → publish to DLT
```

**When to use:** Database or downstream service is under load — exponential backoff gives it progressively more time to recover without flooding it.

#### Which One to Use?

| Scenario | Recommended BackOff |
|----------|-------------------|
| Short transient DB timeouts | `FixedBackOff(1000, 3)` — 3 retries, 1s apart |
| DB under load / connection pool exhausted | `ExponentialBackOff` — back off progressively |
| External service call (circuit breaker candidate) | `ExponentialBackOff` with max interval cap |
| Unit testing retry behavior | `FixedBackOff(0, 2)` — no wait, 2 retries |

**Common pitfall**
- Using `FixedBackOff(0, 3)` in production means three retries with zero delay — the recovering database is immediately flooded before it has a chance to stabilize. Always use at least `1000ms` in non-test environments.

**Why it matters**
- The BackOff strategy determines whether the consumer acts as a good citizen toward downstream systems or hammers them under failure conditions.

For this project, `FixedBackOff` with 3 retries at 1 second is a reasonable starting point.

#### Wiring BackOff → DefaultErrorHandler → ConcurrentKafkaListenerContainerFactory

Once the `BackOff` is chosen, it flows through `DefaultErrorHandler` and is registered on the container factory:

```java
@Bean
public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {

    // Step 1 — choose a BackOff strategy
    FixedBackOff fixedBackOff = new FixedBackOff(1_000L, 3L);
    //                                             ↑        ↑
    //                                          interval  maxAttempts

    // Step 2 — build DefaultErrorHandler with the backoff and recoverer
    DefaultErrorHandler errorHandler =
            new DefaultErrorHandler(recoverer, fixedBackOff);

    return errorHandler;
}

@Bean
public ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>
        kafkaListenerContainerFactory(
            ConsumerFactory<Integer, LibraryEventDto> consumerFactory,
            DefaultErrorHandler errorHandler) {           // ← injected from above

    ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    factory.setCommonErrorHandler(errorHandler);          // ← Step 3: registered here

    return factory;
}
```

The same pattern applies when switching to `ExponentialBackOff` — only Step 1 changes:

```java
// Step 1 (ExponentialBackOff variant) — swap in here; Steps 2 and 3 are unchanged
ExponentialBackOff exponentialBackOff = new ExponentialBackOff();
exponentialBackOff.setInitialInterval(1_000L);
exponentialBackOff.setMultiplier(2.0);
exponentialBackOff.setMaxInterval(10_000L);
exponentialBackOff.setMaxElapsedTime(30_000L);

DefaultErrorHandler errorHandler =
        new DefaultErrorHandler(recoverer, exponentialBackOff);
```

---

### 4) RetryListener

**What**
- `RetryListener` is a callback interface on `DefaultErrorHandler` that fires on every failed delivery attempt. It gives you a hook to log, meter, or alert on each retry in real time — without modifying the listener or service.

**How it works internally**
- `DefaultErrorHandler` calls `RetryListener.failedDelivery(record, exception, deliveryAttempt)` each time a delivery attempt fails (including the first attempt before any backoff).
- `deliveryAttempt` is 1-based — so `1` means the first failure (original delivery), `2` means after the first retry, and so on.
- `RetryListener.recovered(record, exception)` fires when recovery succeeds (e.g., the record was published to DLT).
- `RetryListener.recoveryFailed(record, original, failure)` fires when the recoverer itself throws — receives both the original listener exception and the recoverer exception; the offset still advances, but the failure is logged.

**Retry attempt vs delivery attempt numbering**

```
Message received
    ↓
Attempt 1 (original) → fails → failedDelivery(record, ex, deliveryAttempt=1)
    ↓ wait 1s (FixedBackOff)
Attempt 2 (retry 1)  → fails → failedDelivery(record, ex, deliveryAttempt=2)
    ↓ wait 1s
Attempt 3 (retry 2)  → fails → failedDelivery(record, ex, deliveryAttempt=3)
    ↓ wait 1s
Attempt 4 (retry 3)  → fails → failedDelivery(record, ex, deliveryAttempt=4)
    ↓
All retries exhausted → recoverer invoked → recovered(record, ex)
```

**⚠️ Why you see "Delivery attempt 1" only once for non-retryable exceptions**
- When the thrown exception matches `addNotRetryableExceptions(...)`, `DefaultErrorHandler` **skips the backoff loop entirely** — `failedDelivery` is called once (attempt 1) and the recoverer is invoked immediately. No attempt 2, 3, or 4.
- This is correct behavior — it confirms the exception was classified as non-retryable and went straight to recovery.

**How to register a RetryListener (lambda — single callback):**
```java
errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
    log.warn("Delivery attempt {} failed. Topic={}, Partition={}, Offset={}, Error={}",
             deliveryAttempt,
             record.topic(), record.partition(), record.offset(),
             ex.getMessage())
);
```

**Full RetryListener with all three callbacks:**
```java
errorHandler.setRetryListeners(new RetryListener() {

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
        log.warn("Delivery attempt {} failed. Topic={}, Partition={}, Offset={}, Error={}",
                 deliveryAttempt,
                 record.topic(), record.partition(), record.offset(),
                 ex.getMessage());
    }

    @Override
    public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
        log.info("Record recovered after retries. Topic={}, Partition={}, Offset={}",
                 record.topic(), record.partition(), record.offset());
    }

    @Override
    public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
        log.error("Record recovery failed. Topic={}, Partition={}, Offset={}, OriginalError={}, RecoveryError={}",
                  record.topic(), record.partition(), record.offset(),
                  original.getMessage(), failure.getMessage());
    }
});
```

**Common pitfall**
- Using a lambda registers only `failedDelivery`. If you need `recovered` or `recoveryFailed` callbacks, implement the full `RetryListener` interface as shown above.

**Why it matters**
- Without a `RetryListener`, retries happen silently. In production, you need to know when a message is being retried, how many times, and whether recovery succeeded — before it becomes an incident.

---

## Part 2: Error Classification

### 5) Types of Errors in a Kafka Consumer

**What**
- Consumer failures fall into two categories: transient (retryable) and permanent (non-retryable). Getting this classification right determines whether a retry wastes time or recovers successfully.

**Retryable Errors**

These are **transient failures** — the same message may succeed if tried again after a short delay:

| Error | Reason |
|-------|--------|
| `TransientDataAccessException` | Temporary DB lock or timeout |
| `RecoverableDataAccessException` | Transient DB connectivity issue |
| `SocketTimeoutException` | Network hiccup to the database |
| `JpaSystemException` (wrapping transient causes) | JPA-level transient failures |
| Custom application exceptions marked as retryable | Business logic that may succeed on retry |

**Non-Retryable Errors**

These are **permanent failures** — retrying will never succeed. Routing these to a DLT immediately (without wasting retry attempts) is the right strategy:

| Error | Reason |
|-------|--------|
| `IllegalArgumentException` | Invalid message content — will always fail |
| `NullPointerException` | Programming error or malformed payload |
| `DataIntegrityViolationException` | Duplicate key — retrying will always fail |
| `JsonProcessingException` | Malformed JSON in the message value |
| `InvalidFormatException` | Invalid enum value or type mismatch in payload |
| Custom domain exceptions marked as non-retryable | Business rule violations |

**Common pitfall**
- Treating all exceptions as retryable causes the consumer to retry a duplicate-key insert 3 times before sending it to the DLT — wasting 3 seconds of backoff time and retrying something that can never succeed. Classify `DataIntegrityViolationException` as non-retryable from the start.

**Why it matters**
- Correct classification prevents both silent message loss (no retry on transient failures) and wasted retry attempts (retrying permanent failures). It is the foundation of the entire error handling strategy.

---

### 6) Classifying Retryable vs Non-Retryable Exceptions

**What**
- `DefaultErrorHandler` provides two methods to register exception classifications. Use these to tell the error handler which exceptions should bypass retries entirely.

**Key configs**
- `addNotRetryableExceptions(Class<?>...)` — named exceptions skip all retries and go straight to recovery.
- `addRetryableExceptions(Class<?>...)` — only named exceptions trigger retry; everything else goes straight to recovery (strict allowlist mode).

**How it works internally**
- By default, `DefaultErrorHandler` considers all exceptions retryable unless told otherwise.
- When an exception is thrown by the listener, `DefaultErrorHandler` checks its exception classification map before deciding whether to start the backoff loop or invoke recovery immediately.
- `addNotRetryableExceptions` is the permissive approach: most things retry, specific exceptions do not.
- `addRetryableExceptions` is the restrictive approach: only named exceptions retry, all others go straight to DLT. Use this when you want tight control over what is allowed to retry.

**Recommended approach for this project — `addNotRetryableExceptions`:**
```java
errorHandler.addNotRetryableExceptions(
    IllegalArgumentException.class,        // bad payload — will never succeed
    NullPointerException.class,            // programming error
    DataIntegrityViolationException.class  // duplicate key — will always fail
);
```

**Common pitfall**
- `NullPointerException` is retryable by default in `DefaultErrorHandler`. If a malformed message causes an NPE in `processEvent()`, it will be retried 3 times before going to the DLT — always add it to the non-retryable list.

**Why it matters**
- Without explicit classification, every error — including ones that can never recover — wastes retry time and clogs the backoff queue.

---

## Part 3: Recovery Strategies

### Overview

Retries handle **transient** failures — the expectation is that the same message will eventually succeed if tried again. But retries are not infinite. When all retry attempts are exhausted, the consumer must make a deliberate decision: what happens to this message now?

Without a recovery strategy, the answer is silent loss. Spring Kafka's `DefaultErrorHandler` catches the exception, logs it, advances the offset, and moves on. The message is gone — no trace, no way to replay it, no alert. In any system where messages have business value, this is unacceptable.

Recovery is the answer to that problem. It is a last-resort handler — a `ConsumerRecordRecoverer` — that is invoked exactly once after all retries are exhausted. Its job is to ensure the failed record is **not silently dropped** but instead handled in a way that preserves it for inspection, replay, or auditing.

The right recovery strategy depends on the nature of your system:
- If the downstream is Kafka-native (another consumer can reprocess the DLT), use a **Dead Letter Topic**.
- If you need human-visible failure tracking and scheduler-based replay, use a **failure table**.
- In production systems where both visibility and replay matter, use **both**.

There are four general recovery patterns in Kafka consumer applications:

| Strategy | What happens | When to use |
|---|---|---|
| **Dead Letter Topic (DLT)** | Failed record is published to `<topic>.DLT` with failure metadata headers | Default choice — gives you Kafka-native replay and a full audit trail |
| **Log and Skip** | Log the failure and advance the offset | Non-critical events (metrics, logs) where occasional loss is acceptable |
| **Persist to Failure Table** | Save failed record to a DB table for inspection and manual replay | When you need operational visibility and replay via admin/scheduler |
| **DLT + Persist** | Publish to DLT *and* save to DB | Production systems needing both Kafka-based replay and operational dashboards |

In this project, recovery is **configurable** via `app.kafka.recovery.mode`. The supported modes map directly to those four patterns:

| Mode | What happens | When to use |
|---|---|---|
| **`failure-table`** (default) | Save failed record to `failure_record` with status `OPEN` | Current teaching default for scheduler-based retry flow |
| **`dlt`** | Publish failed record to `<topic>.DLT` with failure metadata headers | Kafka-native replay and audit trail |
| **`both`** | Persist to `failure_record` and publish to DLT | Demonstrate/operate both recovery paths together |
| **Custom lambda** | Any custom side effect (log/alert/persist elsewhere) | Advanced or domain-specific recovery behavior |

**Key contract:** the recoverer is called once after all retry attempts are exhausted. After it returns (or throws), the offset advances and the partition unblocks. If recovery code throws, the failure is reported via `RetryListener.recoveryFailed(...)`.

**Recovery mode used by this codebase**

```yaml
app:
  kafka:
    recovery:
      mode: failure-table # options: failure-table | dlt | both
```

- `failure-table` (default): `FailureRecordService.saveFailureRecord(...)`
- `dlt`: `DeadLetterPublishingRecoverer.accept(...)`
- `both`: executes both paths in sequence

---

### 7) Dead Letter Topic (DLT)

**What**
- A Dead Letter Topic (DLT) is a separate Kafka topic where messages that could not be processed — even after all retry attempts — are published for later inspection, reprocessing, or alerting.
- In this project, DLT is enabled when `app.kafka.recovery.mode` is `dlt` or `both`.

```
library-events          ← original topic (messages consumed here)
library-events.DLT      ← dead letter topic (failed messages land here)
```

**How it works internally**
- Spring Kafka provides `DeadLetterPublishingRecoverer` — a `ConsumerRecordRecoverer` that:
  1. Takes the failed `ConsumerRecord`
  2. Publishes it to `<original-topic>.DLT` using a `KafkaTemplate`
  3. Includes the original headers plus additional failure metadata headers:
     - `kafka_dlt-exception-fqcn` — fully qualified exception class name
     - `kafka_dlt-exception-message` — exception message
     - `kafka_dlt-exception-stacktrace` — full stack trace
     - `kafka_dlt-original-topic` — source topic
     - `kafka_dlt-original-partition` — source partition
     - `kafka_dlt-original-offset` — source offset

**DLT Topic Naming Convention**

| Original Topic | DLT Topic |
|---------------|-----------|
| `library-events` | `library-events.DLT` |
| `order-events` | `order-events.DLT` |

Spring Kafka follows the `<topic>.DLT` convention by default. You can override this.

**Create the DLT topic via Docker**

Run this command against the running `kafka1` container (matches the `docker-compose-multi-broker.yml` setup in this project):

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --create --topic library-events.DLT --partitions 3 --replication-factor 3
```

Verify it was created:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic library-events.DLT
```

> `kafka1:19092` is the internal listener address used inside the Docker network (see `KAFKA_ADVERTISED_LISTENERS` in `docker-compose-multi-broker.yml`). `localhost:9092` is only reachable from the host machine, not from within the container.

**Configuration in this project (DLT-capable):**
```java
@Bean
public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
        KafkaTemplate<Integer, Object> dltKafkaTemplate) {
    return new DeadLetterPublishingRecoverer(
            dltKafkaTemplate,
            (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())
    );
}
```

**Custom DLT topic or partition routing:**
```java
@Bean
public DeadLetterPublishingRecoverer recoverer(KafkaTemplate<Integer, LibraryEventDto> kafkaTemplate) {
    return new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        //                       ↑
        //                 custom destination resolver
    );
}
```

**Consume from the DLT via console:**

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
  --topic library-events.DLT \
  --from-beginning
```

```
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
--topic library-events.DLT \
--property print.headers=true \
--property print.key=true \
--property print.timestamp=true \
--property key.separator=" | "
```

**Common pitfall**
- The DLT topic must exist on the broker before the consumer starts — or `auto.create.topics.enable=true` must be set. If the DLT topic doesn't exist and auto-creation is off, the `DeadLetterPublishingRecoverer` will throw a `TopicAuthorizationException` or `UnknownTopicOrPartitionException` at recovery time, and the original message will still be lost.

**Why it matters**
- The DLT gives you an audit trail of every failed message, the ability to reprocess failures manually, and prevents silent message loss. Nothing is dropped without a record.

---

### 8) Custom Recovery Strategies

**What**
- `DeadLetterPublishingRecoverer` is one recovery strategy. Beyond DLT, there are two more common patterns worth knowing. Each is a standalone `ConsumerRecordRecoverer` lambda you can wire directly into `DefaultErrorHandler`.

#### Log and Skip

The simplest recovery — log the failure and move on. Use only for non-critical use cases where message loss is acceptable:

```java
ConsumerRecordRecoverer logAndSkip = (record, exception) -> {
    log.error("Recovery: skipping failed record. Topic={}, Partition={}, Offset={}, Exception={}",
              record.topic(), record.partition(), record.offset(), exception.getMessage());
};

DefaultErrorHandler errorHandler = new DefaultErrorHandler(logAndSkip, fixedBackOff);
```

**When to use:** Metrics events or logging records where the occasional loss under extreme failure is acceptable.

#### Persist to a Failure Table

Persist failed messages to a database table for inspection and manual reprocessing:

```java
ConsumerRecordRecoverer persistToFailureTable = (record, exception) -> {
    log.error("Recovery: persisting failed record to failure table. Offset={}", record.offset());
    failureRecordService.saveFailureRecord(record, exception);
};

DefaultErrorHandler errorHandler = new DefaultErrorHandler(persistToFailureTable, fixedBackOff);
```

**When to use:** When you need operational visibility and the ability to replay specific failed records via an admin endpoint or scheduled job.

#### Publish to DLT + Persist

Combine both — publish to DLT for Kafka-based reprocessing and persist for visibility in your operational database:

```java
ConsumerRecordRecoverer dltAndPersist = (record, exception) -> {
    // 1. publish to DLT
    deadLetterPublishingRecoverer.accept(record, exception);

    // 2. persist to failure table for operational visibility
    failureRecordService.saveFailureRecord(record, exception);

    log.error("Recovery: published to DLT and persisted to failure table. Offset={}",
              record.offset());
};

DefaultErrorHandler errorHandler = new DefaultErrorHandler(dltAndPersist, fixedBackOff);
```

**When to use:** Production systems where you need both Kafka-based replay capability and operational dashboards showing failure counts and details.

**Common pitfall**
- If the database write fails (e.g., the DB is down), the recovery itself throws an exception. Wrap the persistence call in a try/catch so that a DB failure during recovery does not prevent the offset from advancing and block the partition.

#### Wiring It Together with a Mode-Driven Recoverer

Once you've understood each strategy individually, a clean way to make the recovery behavior configurable is a mode-driven `ConsumerRecordRecoverer` bean that selects the right path at startup time based on a property:

```yaml
app:
  kafka:
    recovery:
      mode: failure-table   # options: failure-table | dlt | both
```

```java
@Bean
public ConsumerRecordRecoverer consumerRecordRecoverer(
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {

    RecoveryMode mode = RecoveryMode.from(recoveryMode);

    return (record, exception) -> {
        switch (mode) {
            case DLT ->
                deadLetterPublishingRecoverer.accept(record, exception);
            case FAILURE_TABLE ->
                failureRecordService.saveFailureRecord(record, exception);
            case BOTH -> {
                failureRecordService.saveFailureRecord(record, exception);
                deadLetterPublishingRecoverer.accept(record, exception);
            }
        }
    };
}
```

Each `case` corresponds directly to one of the three strategies above. Switching between them requires only a property change — no code change.

**Why it matters**
- The recovery strategy is the last line of defense. Choosing the right one determines whether failed messages disappear silently, accumulate in a DLT for replay, or are visible in your operational database. The mode-driven approach lets you start simple and add strategies without restructuring the error handler wiring.

---

### Bonus) Retrying Failed Records from the Database

#### Context: why this goes beyond our app

- In this application, the downstream work after consuming a message is a **database write** (save or update a `LibraryEvent` row).
- In real-world services, that downstream step is often a **REST API call** to another service.
- If the downstream service is temporarily unavailable, processing fails and the record is stored in `failure_record` with status `OPEN`.
- The scheduled retry process keeps attempting the record until the downstream service recovers and processing succeeds.
- On success, the record is marked `FIXED`, and the message is effectively delivered.
- Core value of this pattern: **transient downstream failures become retryable, not permanent losses**.

---

#### How it works — step by step

```
Step 1 — Message consumed from Kafka
─────────────────────────────────────────────────────────────────────
LibraryEventsConsumer.onMessage()
  → libraryEventService.processEvent(record)
      → downstream call throws (DB constraint, REST service down, etc.)
  → exception propagates to DefaultErrorHandler

Step 2 — DefaultErrorHandler retries (FixedBackOff: 1s × 3 attempts)
─────────────────────────────────────────────────────────────────────
  Attempt 1 → fails
  Attempt 2 → fails
  Attempt 3 → fails
  All retries exhausted → recoverer called

Step 3 — Recoverer handles the failure (mode-dependent)
─────────────────────────────────────────────────────────────────────
  ConsumerRecordRecoverer
    → mode=failure-table (default): save to failure_record (status='OPEN')
    → mode=dlt: publish to library-events.DLT
    → mode=both: save to failure_record + publish to DLT
    → Offset committed → partition unblocked → next message consumed

Step 4 — Scheduler wakes up every 10 seconds
─────────────────────────────────────────────────────────────────────
  LibraryEventsScheduler.retryFailedRecords()
    → FailureRecordService.retryFailedRecords()
        → SELECT * FROM failure_record WHERE status = 'OPEN'
        → For each OPEN record:
            → Deserialize errorRecord JSON back to LibraryEventDto
            → Reconstruct ConsumerRecord (same topic/partition/offset/key/value)
            → libraryEventService.processEvent(consumerRecord)
                → SUCCESS  → UPDATE failure_record SET status = 'FIXED'
                → FAILURE  → log error, leave status = 'OPEN' (retried next cycle)
```

---

#### The three classes involved

**`FailureRecord` (entity)**

| Column | Type | Notes |
|---|---|---|
| `topic` | String | Original Kafka topic |
| `key_value` | Integer | Message key |
| `error_record` | TEXT | Full JSON of the failed payload |
| `partition` | Integer | Original partition |
| `offset_value` | Long | Original offset |
| `exception` | TEXT | Exception message at time of failure |
| `status` | String | `OPEN` → not yet fixed, `FIXED` → successfully retried |
| `created_at` | LocalDateTime | Set once at insert |
| `updated_at` | LocalDateTime | Updated on every status change |

**`FailureRecordService`**

```java
// Called by the recoverer — saves the failed record
public void saveFailureRecord(ConsumerRecord<Integer, LibraryEventDto> record,
                               Exception exception) { ... }

// Called by the scheduler — finds all OPEN records and replays them
public void retryFailedRecords() {
    List<FailureRecord> openRecords = failureRecordRepository.findAllByStatus(OPEN);

    openRecords.forEach(failureRecord -> {
        try {
            LibraryEventDto dto = objectMapper.readValue(
                    failureRecord.getErrorRecord(), LibraryEventDto.class);

            ConsumerRecord<Integer, LibraryEventDto> consumerRecord =
                    new ConsumerRecord<>(
                            failureRecord.getTopic(),
                            failureRecord.getPartition(),
                            failureRecord.getOffsetValue(),
                            failureRecord.getKeyValue(),
                            dto
                    );

            libraryEventService.processEvent(consumerRecord); // replay

            failureRecord.setStatus(FIXED);
            failureRecordRepository.save(failureRecord);      // mark done

        } catch (Exception e) {
            log.error("Retry failed for id={}: {}", failureRecord.getId(), e.getMessage());
            // status stays OPEN — will be retried next cycle
        }
    });
}
```

**`LibraryEventsScheduler`**

```java
@Component
public class LibraryEventsScheduler {

    private final FailureRecordService failureRecordService;

    // Runs every 10 seconds — retries all OPEN failure records
    @Scheduled(fixedRateString = "${retry.scheduler.fixed-rate:10000}")
    public void retryFailedRecords() {
        log.info("Scheduler: starting retry of OPEN failure records");
        failureRecordService.retryFailedRecords();
        log.info("Scheduler: completed retry run");
    }
}
```

Enable scheduling in your main class or any `@Configuration`:
```java
@EnableScheduling
```

---

#### Why the status field matters

```
OPEN   → failure was recorded, not yet successfully retried
FIXED  → retry succeeded — downstream processed the event

Nothing is deleted. The table is a permanent audit log.
You can query it at any time:

  SELECT * FROM failure_record WHERE status = 'OPEN';   -- backlog
  SELECT * FROM failure_record WHERE status = 'FIXED';  -- resolved
```

---

#### Common pitfall — retrying non-retryable errors

If a record failed due to a permanent error (e.g. `IllegalArgumentException` — bad payload), the scheduler will retry it forever and it will never move to `FIXED`. Distinguish permanent vs. transient failures at the recoverer level:

```java
ConsumerRecordRecoverer recoverer = (record, exception) -> {
    if (exception instanceof IllegalArgumentException) {
        // permanent — save with status DEAD, not OPEN
        failureRecordService.saveWithStatus(record, exception, "DEAD");
    } else {
        // transient — save as OPEN so the scheduler retries it
        failureRecordService.saveFailureRecord(record, exception);
    }
};
```

The scheduler query then only picks up `OPEN` records and never wastes cycles on permanently bad payloads.

---

## Part 4: Wiring It Together

### 9) Manual Acknowledgment and Error Handling

**What**
- With `MANUAL` acknowledgment mode, the listener controls when offsets are committed. `acknowledge()` must only be called on the success path so that exceptions propagate to `DefaultErrorHandler` for retry and recovery.

**How it works internally**
- When `acknowledge()` is called only on the success path, an exception from `processEvent()` propagates to `DefaultErrorHandler`. The error handler redelivers the same record for retries, and commits the offset only after recovery completes.
- If `acknowledge()` were called in a `finally` block, the offset would be committed immediately — even if `processEvent()` threw an exception. The error handler would never see it and the message would be permanently lost.

**Current implementation (correct):**
```java
@KafkaListener(topics = "library-events")
public void onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord,
                      Acknowledgment acknowledgment) {
    log.info("ConsumerRecord : {}", consumerRecord);
    libraryEventService.processEvent(consumerRecord);
    // Only acknowledge on success — on exception, DefaultErrorHandler takes over:
    // it retries with FixedBackOff, then persists to failure_record table on exhaustion.
    acknowledgment.acknowledge();
}
```

**How the offset advances on failure:**
```text
onMessage() called
    ↓
processEvent() throws TransientDataAccessException
    ↓
Exception propagates to DefaultErrorHandler  ← no acknowledge yet
    ↓
Retry 1 (after 1s backoff) → succeeds
    ↓
onMessage() calls acknowledgment.acknowledge()  ← offset committed ✓
```

When the listener throws an exception, `DefaultErrorHandler` intercepts it. The offset is **not** committed until either:
- The retry succeeds → `acknowledge()` is called by the listener
- All retries are exhausted → `DefaultErrorHandler` acknowledges after recovery (DLT publish)

**Common pitfall**
- Since any exception from `processEvent()` propagates directly to `DefaultErrorHandler`, make sure the error handler is correctly wired on the container factory — otherwise exceptions will propagate to the container and be swallowed silently.

**Why it matters**
- This ensures no message is ever lost — either it succeeds and the offset moves forward, or it goes to the DLT and the offset moves forward.

---

### 10) Full Configuration: LibraryEventsConsumerConfig.java

The complete `LibraryEventsConsumerConfig` wires together all the pieces covered in Parts 1–3: a dedicated DLT producer, the mode-driven recoverer, retry/backoff, non-retryable exception classification, and a full `RetryListener`.

```java
@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private static final Logger log =
            LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

    private final FailureRecordService failureRecordService;
    private final String recoveryMode;
    private final String bootstrapServers;

    public LibraryEventsConsumerConfig(
            FailureRecordService failureRecordService,
            @Value("${app.kafka.recovery.mode:failure-table}") String recoveryMode,
            @Value("${spring.kafka.consumer.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        this.failureRecordService = failureRecordService;
        this.recoveryMode = recoveryMode;
        this.bootstrapServers = bootstrapServers;
    }

    // ── DLT Producer ─────────────────────────────────────────────────────────
    // A dedicated producer factory and KafkaTemplate for publishing to the DLT.
    // Uses Object as the value type so it can carry any failed record payload.

    @Bean
    public ProducerFactory<Integer, Object> dltProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<Integer, Object> dltKafkaTemplate(
            ProducerFactory<Integer, Object> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    // ── Dead Letter Publishing Recoverer ─────────────────────────────────────
    // Routes failed records to <original-topic>.DLT on the same partition.
    // Adds exception metadata headers automatically.

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<Integer, Object> dltKafkaTemplate) {
        return new DeadLetterPublishingRecoverer(
                dltKafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        );
    }

    // ── Mode-Driven Recoverer ─────────────────────────────────────────────────
    // Selects the recovery path at startup based on app.kafka.recovery.mode.
    // Delegates to the appropriate strategy: persist, DLT, or both.

    @Bean
    public ConsumerRecordRecoverer consumerRecordRecoverer(
            DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {

        RecoveryMode mode = RecoveryMode.from(recoveryMode);
        log.info("Kafka recovery mode: {}", mode);

        return (record, exception) -> {
            switch (mode) {
                case DLT ->
                    publishToDlt(record, exception, deadLetterPublishingRecoverer);
                case FAILURE_TABLE ->
                    persistFailureRecord(record, exception);
                case BOTH -> {
                    persistFailureRecord(record, exception);
                    publishToDlt(record, exception, deadLetterPublishingRecoverer);
                }
            }
        };
    }

    // ── Default Error Handler ─────────────────────────────────────────────────
    // Retry 3 times with 1-second fixed backoff.
    // Non-retryable exceptions skip retries and go straight to the recoverer.
    // RetryListener covers the full delivery lifecycle: attempt, recovered, recoveryFailed.

    @Bean
    public DefaultErrorHandler errorHandler(ConsumerRecordRecoverer consumerRecordRecoverer) {

        var fixedBackOff = new FixedBackOff(1_000L, 3L);

        var errorHandler = new DefaultErrorHandler(consumerRecordRecoverer, fixedBackOff);

        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,         // malformed JSON / type mismatch
                IllegalArgumentException.class,         // bad payload — will never succeed
                DataIntegrityViolationException.class   // duplicate key — always fails
        );

        errorHandler.setRetryListeners(new RetryListener() {

            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Delivery attempt {} failed. Topic={}, Partition={}, Offset={}, Error={}",
                        deliveryAttempt,
                        record.topic(), record.partition(), record.offset(),
                        ex.getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.info("Record recovered after retries. Topic={}, Partition={}, Offset={}",
                        record.topic(), record.partition(), record.offset());
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.error("Record recovery failed. Topic={}, Partition={}, Offset={}, OriginalError={}, RecoveryError={}",
                        record.topic(), record.partition(), record.offset(),
                        original.getMessage(), failure.getMessage());
            }
        });

        return errorHandler;
    }

    // ── Container Factory ─────────────────────────────────────────────────────

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, LibraryEventDto>>
            kafkaListenerContainerFactory(
                ConsumerFactory<Integer, LibraryEventDto> consumerFactory,
                DefaultErrorHandler errorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    // ── Private Helpers ───────────────────────────────────────────────────────

    private void persistFailureRecord(ConsumerRecord<?, ?> record, Exception exception) {
        log.error("All retries exhausted. Persisting failed record to failure_record table. "
                        + "Topic={}, Partition={}, Offset={}, Exception={}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());

        //noinspection unchecked
        failureRecordService.saveFailureRecord(
                (ConsumerRecord<Integer, LibraryEventDto>) record, exception);
    }

    private void publishToDlt(
            ConsumerRecord<?, ?> record,
            Exception exception,
            DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
        log.error("All retries exhausted. Publishing failed record to DLT. "
                        + "Topic={}, Partition={}, Offset={}, Exception={}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());
        deadLetterPublishingRecoverer.accept(record, exception);
    }

    // ── Recovery Mode Enum ────────────────────────────────────────────────────

    private enum RecoveryMode {
        FAILURE_TABLE, DLT, BOTH;

        private static RecoveryMode from(String value) {
            String normalized = value == null ? "" : value.trim().toUpperCase().replace('-', '_');
            return switch (normalized) {
                case "FAILURE_TABLE" -> FAILURE_TABLE;
                case "DLT"           -> DLT;
                case "BOTH"          -> BOTH;
                default -> throw new IllegalArgumentException(
                        "Invalid app.kafka.recovery.mode: " + value
                        + " (expected: failure-table, dlt, both)");
            };
        }
    }
}
```

**Why each component earns its place**

| Component | Without it | With it |
|---|---|---|
| `dltProducerFactory` + `dltKafkaTemplate` | Consumer's serializer config used for DLT — type mismatch on Object payloads | Dedicated producer with `JsonSerializer` for Object; no type conflict |
| `DeadLetterPublishingRecoverer` | Failed messages disappear — no audit trail | Failed records land in `<topic>.DLT` with full exception headers |
| `ConsumerRecordRecoverer` (mode-driven) | Single hard-coded recovery path | Recovery path configurable via `app.kafka.recovery.mode` — no code change needed |
| `RecoveryMode` enum | String comparison scattered in switch — typo-prone | Normalized at startup; invalid config fails fast with a clear message |
| `DefaultErrorHandler` | Exceptions swallowed silently — message lost | Retry + recovery orchestrated automatically |
| `FixedBackOff(1000, 3)` | No retry — first failure is final | 3 retry attempts with 1s breathing room for transient failures |
| `addNotRetryableExceptions` | Permanent failures waste 3 retry cycles before recovery | `DeserializationException`, `IllegalArgumentException`, `DataIntegrityViolationException` go straight to recovery |
| `RetryListener` (full interface) | Retries happen silently | Every attempt, recovery, and recovery failure is logged — observable in production |

---

### 11) End-to-End Flow with Error Handling

```
Kafka Broker: library-events
          |
          v
LibraryEventsConsumer.onMessage()
          |
          v
libraryEventService.processEvent()
          |
    ┌─────┴──────────────────────────────────────────────────────┐
    │ SUCCESS                                                     │ FAILURE
    v                                                             v
acknowledgment.acknowledge()                          Exception thrown
Offset committed                                             |
Next message consumed                                        v
                                              DefaultErrorHandler intercepts
                                                             |
                                              ┌──────────────┴───────────────────┐
                                              │ Non-Retryable?                   │ Retryable?
                                              │ (IllegalArgumentException,        │ (TransientDataAccessException,
                                              │  DataIntegrityViolationException) │  SocketTimeoutException)
                                              v                                   v
                                     Skip to Recovery             Wait (FixedBackOff: 1s)
                                              │                        Retry attempt 1
                                              │                             |
                                              │                        Retry attempt 2
                                              │                             |
                                              │                        Retry attempt 3
                                              │                             |
                                              │                    All retries exhausted
                                              │                             |
                                              └──────────────┬─────────────┘
                                                             v
                                             DeadLetterPublishingRecoverer
                                                             |
                                                             v
                                              Publish to library-events.DLT
                                              (with exception headers attached)
                                                             |
                                                             v
                                              Offset committed by ErrorHandler
                                              Next message consumed — no partition block
```

---

## Part 5: Testing Error Handling

### 12) Retry and Recovery in Tests

**What**
- Testing retry and recovery behavior requires injecting `@SpyBean` on the consumer or service to simulate failures, verifying retry count, and verifying DLT message delivery.

**Key setup**
```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"library-events", "library-events.DLT"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=library-events-consumer-test"
})
class LibraryEventsConsumerIntegrationTest {

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumer;

    @SpyBean
    LibraryEventService libraryEventService;

    @Autowired
    KafkaTemplate<Integer, LibraryEventDto> kafkaTemplate;
}
```

**What to test**
- Happy path: a valid event is consumed, persisted, and the offset advances.
- Retryable failure: a `RecoverableDataAccessException` causes 3 retry attempts (4 total invocations: 1 original + 3 retries), then the record goes to DLT.
- Non-retryable failure: an `IllegalArgumentException` triggers exactly 1 invocation — no retries — and the record goes to DLT immediately.
- DLT delivery: a `@KafkaListener` on `library-events.DLT` confirms the failed record arrived with the correct headers.

**Example — retryable exception retries 3 times:**
```java
@Test
void onMessage_retryableException_shouldRetryThreeTimes() throws Exception {
    // given
    BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
    LibraryEventDto dto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);

    // simulate transient failure on every call
    doThrow(new RecoverableDataAccessException("Simulated DB timeout"))
            .when(libraryEventService).processEvent(any());

    // when
    kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

    // then — 1 original + 3 retries = 4 total invocations
    await().atMost(Duration.ofSeconds(10))
           .untilAsserted(() ->
               verify(libraryEventService, times(4)).processEvent(any())
           );
}
```

**Example — non-retryable exception goes to DLT immediately:**
```java
@Test
void onMessage_nonRetryableException_shouldGoToDLTImmediately() throws Exception {
    // given
    BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
    LibraryEventDto dto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);

    doThrow(new IllegalArgumentException("Invalid event type"))
            .when(libraryEventService).processEvent(any());

    // when
    kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

    // then — no retries, goes straight to DLT — exactly 1 invocation
    await().atMost(Duration.ofSeconds(5))
           .untilAsserted(() ->
               verify(libraryEventService, times(1)).processEvent(any())
           );
    // verify DLT consumer received the message
}
```

**Common pitfall**
- `@SpyBean` wraps the real bean with a Mockito spy — it calls the real method unless stubbed with `doThrow(...)`. Using `when(...).thenThrow(...)` syntax (vs `doThrow(...).when(...)`) can cause the real method to execute once before the stub kicks in. Always use `doThrow` with `@SpyBean`.

**Why it matters**
- Without tests that inject failures, you have no proof the retry and DLT paths actually work. Retry and recovery bugs are invisible in happy-path tests.

---

## Suggested Implementation Order

1. **Classify exceptions** — decide which are retryable vs non-retryable for your domain.
2. **Configure `FixedBackOff`** — start simple with 3 retries at 1 second.
3. **Add `RetryListener`** — log each delivery attempt for real-time observability.
4. **Wire `DeadLetterPublishingRecoverer`** — DLT is the safety net for all unrecoverable failures.
5. **Register `DefaultErrorHandler`** — connect backoff, retry listener, and recoverer to the container factory.
6. **Add `addNotRetryableExceptions`** — exclude permanent failures from retry.
7. **Write failure-injection tests** — verify retry count and DLT delivery with `@SpyBean`.

---

## Implementation Checklist

- [ ] Confirm `acknowledgment.acknowledge()` is on the success path only in `onMessage()`.
- [ ] Identify which exceptions in your domain are retryable vs non-retryable.
- [ ] Configure `FixedBackOff` (or `ExponentialBackOff`) with appropriate interval and max attempts.
- [ ] Create a `DeadLetterPublishingRecoverer` bean wired to a `KafkaTemplate`.
- [ ] Create a `DefaultErrorHandler` bean with the backoff and recoverer.
- [ ] Register `addNotRetryableExceptions` for permanent failure types.
- [ ] Add a `RetryListener` to log each retry attempt.
- [ ] Register the `DefaultErrorHandler` on the `ConcurrentKafkaListenerContainerFactory`.
- [ ] Write integration tests that inject `RecoverableDataAccessException` and verify 4 total invocations.
- [ ] Write integration tests that inject `IllegalArgumentException` and verify 1 total invocation + DLT delivery.
