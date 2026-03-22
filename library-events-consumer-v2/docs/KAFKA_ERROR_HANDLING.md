# Kafka Consumer Error Handling, Retry & Recovery

## Table of Contents

- [Introduction](#introduction)
- [Current State of the Consumer](#current-state-of-the-consumer)
- [Types of Errors in a Kafka Consumer](#types-of-errors-in-a-kafka-consumer)
  - [Retryable Errors](#retryable-errors)
  - [Non-Retryable Errors](#non-retryable-errors)
- [Spring Kafka Error Handling Architecture](#spring-kafka-error-handling-architecture)
- [DefaultErrorHandler](#defaulterrorhandler)
  - [What it Does](#what-it-does)
  - [How to Configure It](#how-to-configure-it)
- [Retry with BackOff Strategies](#retry-with-backoff-strategies)
  - [FixedBackOff](#fixedbackoff)
  - [ExponentialBackOff](#exponentialbackoff)
  - [Which One to Use?](#which-one-to-use)
- [Classifying Retryable vs Non-Retryable Exceptions](#classifying-retryable-vs-non-retryable-exceptions)
- [Dead Letter Topic (DLT)](#dead-letter-topic-dlt)
  - [What is a DLT?](#what-is-a-dlt)
  - [How Spring Kafka Implements DLT](#how-spring-kafka-implements-dlt)
  - [DeadLetterPublishingRecoverer](#deadletterpublishingrecoverer)
  - [DLT Topic Naming Convention](#dlt-topic-naming-convention)
- [Custom Recovery Strategies](#custom-recovery-strategies)
  - [Log and Skip](#log-and-skip)
  - [Persist to a Failure Table](#persist-to-a-failure-table)
  - [Publish to DLT + Persist](#publish-to-dlt--persist)
- [Full Configuration: LibraryEventsConsumerConfig.java](#full-configuration-libraryeventsconsumerconfigjava)
- [How Manual Acknowledgment Interacts with Error Handling](#how-manual-acknowledgment-interacts-with-error-handling)
- [End-to-End Flow with Error Handling](#end-to-end-flow-with-error-handling)
- [Retry and Recovery in Tests](#retry-and-recovery-in-tests)
- [Summary of Strategies](#summary-of-strategies)

---

## Introduction

A Kafka consumer that processes messages and persists them to a database will inevitably encounter failures — transient database timeouts, malformed payloads, constraint violations, and downstream service unavailability. Without a deliberate error handling strategy, one bad message can halt the entire consumer, block the partition, and cause lag to accumulate indefinitely.

Spring Kafka provides a layered error handling system that enables:

1. **Retrying** transient failures automatically with configurable backoff
2. **Classifying** exceptions — some should be retried, some should not
3. **Recovering** messages that cannot be processed after all retries are exhausted — typically by sending them to a Dead Letter Topic (DLT)
4. **Continuing** processing of subsequent messages so a single bad record does not block the partition

This document covers how to implement all of these for the `library-events-consumer-v2` project.

---

## Current State of the Consumer

The consumer currently uses **MANUAL acknowledgment mode**, which gives explicit control over when offsets are committed.

**`LibraryEventsConsumer.java`** — current state:
```java
@KafkaListener(topics = "library-events")
public void onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord,
                      Acknowledgment acknowledgment) {
    try {
        libraryEventService.processEvent(consumerRecord);
    } finally {
        acknowledgment.acknowledge();
    }
}
```

**Problems with this approach:**

| Problem | Consequence |
|---------|------------|
| `acknowledgment.acknowledge()` is called in `finally` — even on exception | The offset is committed even when processing fails. The message is lost silently. |
| No retry logic | A transient DB timeout fails the message permanently on first attempt |
| No exception classification | A malformed payload (never recoverable) is treated the same as a timeout (retryable) |
| No dead letter handling | Failed messages disappear with no audit trail |

The goal is to move from this to a proper error handling strategy while preserving manual acknowledgment control.

---

## Types of Errors in a Kafka Consumer

Before configuring retry, classify which errors should be retried and which should not.

### Retryable Errors

These are **transient failures** — the same message may succeed if tried again after a short delay:

| Error | Reason |
|-------|--------|
| `TransientDataAccessException` | Temporary DB lock or timeout |
| `RecoverableDataAccessException` | Transient DB connectivity issue |
| `SocketTimeoutException` | Network hiccup to the database |
| `JpaSystemException` (wrapping transient causes) | JPA-level transient failures |
| Custom application exceptions marked as retryable | Business logic that may succeed on retry |

### Non-Retryable Errors

These are **permanent failures** — retrying will never succeed. Routing these to a DLT immediately (without wasting retry attempts) is the right strategy:

| Error | Reason |
|-------|--------|
| `IllegalArgumentException` | Invalid message content — will always fail |
| `NullPointerException` | Programming error or malformed payload |
| `DataIntegrityViolationException` | Duplicate key — retrying will always fail |
| `JsonProcessingException` | Malformed JSON in the message value |
| `InvalidFormatException` | Invalid enum value or type mismatch in payload |
| Custom domain exceptions marked as non-retryable | Business rule violations |

---

## Spring Kafka Error Handling Architecture

Spring Kafka's error handling stack (as of Spring Kafka 3.x / Spring Boot 4.x):

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

---

## DefaultErrorHandler

### What it Does

`DefaultErrorHandler` is a `CommonErrorHandler` implementation that:

1. Catches exceptions thrown by the listener method
2. Checks if the exception is classified as non-retryable — if so, immediately invokes the recovery callback
3. If retryable, waits for the configured `BackOff` interval and retries the same record
4. After all retry attempts are exhausted, invokes the recovery callback
5. After recovery, acknowledges the offset and moves to the next record — **the partition is not blocked**

### How to Configure It

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

---

## Retry with BackOff Strategies

BackOff determines how long to wait between retry attempts.

### FixedBackOff

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

### ExponentialBackOff

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

### Which One to Use?

| Scenario | Recommended BackOff |
|----------|-------------------|
| Short transient DB timeouts | `FixedBackOff(1000, 3)` — 3 retries, 1s apart |
| DB under load / connection pool exhausted | `ExponentialBackOff` — back off progressively |
| External service call (circuit breaker candidate) | `ExponentialBackOff` with max interval cap |
| Unit testing retry behavior | `FixedBackOff(0, 2)` — no wait, 2 retries |

For this project, `FixedBackOff` with 3 retries at 1 second is a reasonable starting point.

---

## Classifying Retryable vs Non-Retryable Exceptions

`DefaultErrorHandler` has two methods for exception classification:

**`addNotRetryableExceptions(Class<?>...)`** — these exceptions skip retries entirely and go straight to recovery:

```java
errorHandler.addNotRetryableExceptions(
    IllegalArgumentException.class,
    NullPointerException.class
);
```

**`addRetryableExceptions(Class<?>...)`** — only these exceptions trigger retry (all others go straight to recovery):

```java
errorHandler.addRetryableExceptions(
    RecoverableDataAccessException.class
);
```

> Use `addNotRetryableExceptions` when you want most exceptions to retry by default, but exclude specific ones.
> Use `addRetryableExceptions` when you want a strict allowlist — only named exceptions retry; everything else goes straight to DLT.

For this project, the recommended approach is to use `addNotRetryableExceptions` — opt specific exceptions out of retry:

```java
errorHandler.addNotRetryableExceptions(
    IllegalArgumentException.class,        // bad payload — will never succeed
    NullPointerException.class,            // programming error
    DataIntegrityViolationException.class  // duplicate key — will always fail
);
```

---

## Dead Letter Topic (DLT)

### What is a DLT?

A Dead Letter Topic (DLT) is a separate Kafka topic where messages that could not be processed — even after all retry attempts — are published for later inspection, reprocessing, or alerting.

```
library-events          ← original topic (messages consumed here)
library-events.DLT      ← dead letter topic (failed messages land here)
```

The DLT gives you:
- An audit trail of every failed message
- The ability to inspect and reprocess failures manually
- Prevention of message loss — nothing is silently dropped
- Separation of failed messages from the main processing flow

### How Spring Kafka Implements DLT

Spring Kafka provides `DeadLetterPublishingRecoverer` — a `ConsumerRecordRecoverer` that:

1. Takes the failed `ConsumerRecord`
2. Publishes it to `<original-topic>.DLT` using a `KafkaTemplate`
3. Includes the original headers plus additional failure metadata headers:
   - `kafka_dlt-exception-fqcn` — fully qualified exception class name
   - `kafka_dlt-exception-message` — exception message
   - `kafka_dlt-exception-stacktrace` — full stack trace
   - `kafka_dlt-original-topic` — source topic
   - `kafka_dlt-original-partition` — source partition
   - `kafka_dlt-original-offset` — source offset

### DeadLetterPublishingRecoverer

```java
@Bean
public DeadLetterPublishingRecoverer recoverer(KafkaTemplate<Integer, LibraryEventDto> kafkaTemplate) {
    return new DeadLetterPublishingRecoverer(kafkaTemplate);
    // Publishes to library-events.DLT by default
}
```

To customize the DLT topic name or partition routing:

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

### DLT Topic Naming Convention

| Original Topic | DLT Topic |
|---------------|-----------|
| `library-events` | `library-events.DLT` |
| `order-events` | `order-events.DLT` |

Spring Kafka follows the `<topic>.DLT` convention by default. You can override this.

---

## Custom Recovery Strategies

`DeadLetterPublishingRecoverer` is the most common recovery strategy, but you can implement any behavior using a `ConsumerRecordRecoverer` lambda or class.

### Log and Skip

The simplest recovery — log the failure and move on. Use only for non-critical use cases where message loss is acceptable:

```java
ConsumerRecordRecoverer logAndSkip = (record, exception) -> {
    log.error("Recovery: skipping failed record. Topic={}, Partition={}, Offset={}, Exception={}",
              record.topic(), record.partition(), record.offset(), exception.getMessage());
};
```

### Persist to a Failure Table

Persist failed messages to a database table for inspection and manual reprocessing:

```java
ConsumerRecordRecoverer persistToFailureTable = (record, exception) -> {
    log.error("Recovery: persisting failed record to failure table. Offset={}", record.offset());
    FailedEvent failedEvent = new FailedEvent(
        record.topic(),
        record.partition(),
        record.offset(),
        record.value().toString(),
        exception.getMessage(),
        LocalDateTime.now()
    );
    failedEventRepository.save(failedEvent);
};
```

### Publish to DLT + Persist

Combine both — publish to DLT for Kafka-based reprocessing and persist for visibility in your operational database:

```java
ConsumerRecordRecoverer dltAndPersist = (record, exception) -> {
    // 1. publish to DLT
    deadLetterPublishingRecoverer.accept(record, exception);

    // 2. persist to failure table for operational visibility
    FailedEvent failedEvent = new FailedEvent(...);
    failedEventRepository.save(failedEvent);

    log.error("Recovery: published to DLT and persisted to failure table. Offset={}",
              record.offset());
};
```

---

## Full Configuration: LibraryEventsConsumerConfig.java

Here is the complete updated `LibraryEventsConsumerConfig` with retry, DLT, and non-retryable exception classification:

```java
@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private static final Logger log =
            LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

    // ── Dead Letter Publishing Recoverer ────────────────────────────────────
    // Publishes failed records to library-events.DLT after all retries are exhausted

    @Bean
    public DeadLetterPublishingRecoverer recoverer(
            KafkaTemplate<Integer, LibraryEventDto> kafkaTemplate) {

        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Recovery: publishing failed record to DLT. "
                            + "Topic={}, Partition={}, Offset={}, Exception={}",
                            record.topic(), record.partition(),
                            record.offset(), ex.getMessage());

                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                }
        );
    }

    // ── Default Error Handler ────────────────────────────────────────────────
    // Retry up to 3 times with 1-second fixed backoff
    // Non-retryable exceptions bypass retries and go straight to DLT

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {

        // Retry 3 times, wait 1 second between each attempt
        FixedBackOff fixedBackOff = new FixedBackOff(1_000L, 3L);

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, fixedBackOff);

        // These exceptions skip retries entirely — send straight to DLT
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,         // bad payload
                NullPointerException.class,             // programming error
                DataIntegrityViolationException.class   // duplicate key — always fails
        );

        // Log each retry attempt for observability
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for record. Topic={}, Partition={}, Offset={}, Error={}",
                        deliveryAttempt,
                        record.topic(), record.partition(), record.offset(),
                        ex.getMessage())
        );

        return errorHandler;
    }

    // ── Container Factory ────────────────────────────────────────────────────

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>
            kafkaListenerContainerFactory(
                ConsumerFactory<Integer, LibraryEventDto> consumerFactory,
                DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
```

---

## How Manual Acknowledgment Interacts with Error Handling

The current consumer acknowledges in `finally` — which means it acknowledges even on failure. This must be changed to allow `DefaultErrorHandler` to control offset management during retries.

**Current (incorrect with error handler):**

```java
@KafkaListener(topics = "library-events")
public void onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord,
                      Acknowledgment acknowledgment) {
    try {
        libraryEventService.processEvent(consumerRecord);
    } finally {
        acknowledgment.acknowledge();   // ← called even on failure — message is lost
    }
}
```

**Updated (correct with error handler):**

```java
@KafkaListener(topics = "library-events")
public void onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord,
                      Acknowledgment acknowledgment) {
    libraryEventService.processEvent(consumerRecord);
    acknowledgment.acknowledge();   // ← only called on success
    // On exception: DefaultErrorHandler takes over — retries, then recovers
}
```

When the listener throws an exception, `DefaultErrorHandler` intercepts it. The offset is **not** committed until either:
- The retry succeeds → `acknowledge()` is called by the listener
- All retries are exhausted → `DefaultErrorHandler` acknowledges after recovery (DLT publish)

This ensures no message is ever lost — either it succeeds and the offset moves forward, or it goes to the DLT and the offset moves forward.

---

## End-to-End Flow with Error Handling

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

## Retry and Recovery in Tests

Testing retry and recovery behavior requires:
- Injecting `SpyBean` on the consumer or service to simulate failures
- Verifying retry count with `CountDownLatch` or `verify(..., times(n))`
- Verifying DLT message delivery via a separate `@KafkaListener` on the DLT topic

**Example — testing that a retryable exception retries 3 times:**

```java
@SpyBean
LibraryEventsConsumer libraryEventsConsumer;

@SpyBean
LibraryEventService libraryEventService;

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
    CountDownLatch latch = new CountDownLatch(4);
    await().atMost(Duration.ofSeconds(10))
           .untilAsserted(() ->
               verify(libraryEventService, times(4)).processEvent(any())
           );
}

@Test
void onMessage_nonRetryableException_shouldGoToDLTImmediately() throws Exception {
    // given
    BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
    LibraryEventDto dto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);

    doThrow(new IllegalArgumentException("Invalid event type"))
            .when(libraryEventService).processEvent(any());

    // when
    kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

    // then — no retries, goes straight to DLT
    await().atMost(Duration.ofSeconds(5))
           .untilAsserted(() ->
               verify(libraryEventService, times(1)).processEvent(any())
           );
    // verify DLT consumer received the message
}
```

---

## Summary of Strategies

| Strategy | When to Use | Spring Kafka Component |
|----------|------------|----------------------|
| **Fixed Retry** | Short transient errors (DB timeout, network blip) | `FixedBackOff` + `DefaultErrorHandler` |
| **Exponential Retry** | Downstream under load — back off progressively | `ExponentialBackOff` + `DefaultErrorHandler` |
| **Non-Retryable Classification** | Bad payload, constraint violations — never will succeed | `errorHandler.addNotRetryableExceptions(...)` |
| **Dead Letter Topic** | Full audit trail, no message loss, reprocessing support | `DeadLetterPublishingRecoverer` |
| **Log and Skip** | Non-critical events where loss is acceptable | Custom `ConsumerRecordRecoverer` lambda |
| **Persist to Failure Table** | Operational visibility into failed messages | Custom `ConsumerRecordRecoverer` + repository |

### The Rule

> **Retryable exceptions** → retry with backoff → DLT on exhaustion
> **Non-retryable exceptions** → skip retries → DLT immediately
> **DLT** → always — never silently drop a message

This gives you maximum resilience: transient failures recover automatically, permanent failures are captured for inspection, and the consumer never blocks a partition waiting forever on a message it can never process.
