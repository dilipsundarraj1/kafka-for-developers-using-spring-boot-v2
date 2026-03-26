# Reliable Kafka Producer Reference

This reference doc is for students implementing reliable producer strategies in the Library Events Producer project.

It keeps the same technical content, but organizes it in an implementation-first format.

## Table of Contents

- [How to Use This Reference](#how-to-use-this-reference)
- [Part 1: Producer Reliability Configuration](#part-1-producer-reliability-configuration)
  - [1) Acknowledgment Modes (`acks`)](#1-acknowledgment-modes-acks)
  - [2) Retries and Retry Backoff](#2-retries-and-retry-backoff)
  - [3) Idempotent Producer (`enable.idempotence`)](#3-idempotent-producer-enableidempotence)
  - [4) `min.insync.replicas` (Broker/Topic Config)](#4-mininsyncreplicas-brokertopic-config)
  - [5) `max.in.flight.requests.per.connection`](#5-maxinflightrequestsperconnection)
  - [6) Producer Timeouts](#6-producer-timeouts)
  - [7) Recommended Reliable Producer Configuration (Summary)](#7-recommended-reliable-producer-configuration-summary)
  - [8) Configuring the Reliable Producer in Spring Boot (Hands-On)](#8-configuring-the-reliable-producer-in-spring-boot-hands-on)
- [Part 2: Application-Level Error Handling & Retry](#part-2-application-level-error-handling--retry)
  - [9) Handling Retriable vs Non-Retriable Errors](#9-handling-retriable-vs-non-retriable-errors)
  - [10) Application-Level Retry (Spring Retry / Custom Logic)](#10-application-level-retry-spring-retry--custom-logic)
  - [11) Error Handling in Callbacks / CompletableFuture](#11-error-handling-in-callbacks--completablefuture)
- [Part 3: Reliability Testing](#part-3-reliability-testing)
  - [12a) Unit Test Hints](#12a-unit-test-hints)
  - [12b) Integration Test Hints](#12b-integration-test-hints)
- [Topic Dependency Flow](#topic-dependency-flow)
- [Mapping to Current Project](#mapping-to-current-project)
- [Suggested Implementation Order](#suggested-implementation-order)
- [Implementation Checklist](#implementation-checklist)

---

## How to Use This Reference

Use this in sequence while implementing:

1. Configure producer reliability (`acks`, retries, idempotence, timeouts).
2. Align broker/topic settings (`min.insync.replicas`, replication factor).
3. Implement callback and application-level error handling.
4. Validate behavior with reliability-focused tests.

---

## Part 1: Producer Reliability Configuration

These settings are applied in `application.yml` (or via `KafkaProducerConfig`) and form the foundation of a reliable producer. They control how the producer communicates with the broker, how it handles transient failures at the Kafka protocol level, and how it avoids data loss or duplication.

### 1) Acknowledgment Modes (`acks`)

**What**
- The `acks` producer configuration controls how many broker replicas must acknowledge a write before the producer considers it successful.

**Values**
- `acks=0` - Fire and forget; no acknowledgment (fastest, least reliable).
- `acks=1` - Leader acknowledgment only (default); message is written to the leader's log.
- `acks=all` (`-1`) - All in-sync replicas (ISR) must acknowledge (slowest, most reliable).

**Why it matters**
- `acks=all` is required for a reliable producer. Without it, data can be lost if the leader crashes before replicating.

**Spring Boot config**
```yaml
spring:
  kafka:
    producer:
      acks: all
```

---

### 2) Retries and Retry Backoff

**What**
- When a transient error occurs (for example, `NOT_LEADER_FOR_PARTITION`, network timeout), the producer can automatically retry sending the message.

**Key configs**
- `retries` - Number of retry attempts (default: `2147483647` in modern Kafka clients, effectively infinite).
- `retry.backoff.ms` - Delay between retries (default: `100ms`).
- `delivery.timeout.ms` - Upper bound on total time for a send (including retries). Default: `120000ms` (2 minutes).

**Examples of transient broker/network failures**
- `NOT_LEADER_FOR_PARTITION` - The broker the producer sent to is no longer the leader for that partition (for example, after a leader election due to broker restart or crash). A retry will discover the new leader via metadata refresh.
- `REQUEST_TIMED_OUT` - The broker did not respond within `request.timeout.ms`. Could be caused by a temporary GC pause, disk I/O spike, or network congestion.
- `NETWORK_EXCEPTION` - A TCP-level failure such as a broken connection, DNS resolution failure, or temporary network partition between the producer and the broker.
- `NotEnoughReplicasException` - The broker cannot satisfy `min.insync.replicas` because one or more replicas are temporarily out of sync (for example, a follower broker is restarting). Once the replica catches up, the retry succeeds.
- `LEADER_NOT_AVAILABLE` - A new topic/partition was just created or a leader election is in progress. Metadata will be refreshed and the retry will find the new leader.
- `UNKNOWN_TOPIC_OR_PARTITION` - The broker's metadata cache has not caught up yet (for example, topic was just created). A retry after metadata refresh resolves it.
- `CORRUPT_MESSAGE` (CRC check failure) - Rare; caused by transient data corruption during network transmission. A retry sends a fresh copy.

**Why it matters**
- Retries handle these transient broker/network failures transparently - the producer recovers automatically without application intervention.

**Spring Boot config**
```yaml
spring:
  kafka:
    producer:
      retries: 10
      properties:
        retry.backoff.ms: 1000
        delivery.timeout.ms: 120000
```

---

### 3) Idempotent Producer (`enable.idempotence`)

**What**
- Ensures that retries do not result in duplicate messages. The broker deduplicates based on the producer ID and sequence number.

**Key config**
- `enable.idempotence=true` (default since Kafka 3.0+).

**Implicit requirements**
- When idempotence is enabled, `acks` is forced to `all`, `retries` is set to `Integer.MAX_VALUE`, and `max.in.flight.requests.per.connection <= 5`.

**Why it matters**
- Retries can cause duplicates without idempotence. This guarantees exactly-once per partition semantics at the producer level.

**Spring Boot config**
```yaml
spring:
  kafka:
    producer:
      properties:
        enable.idempotence: true
```

---

### 4) `min.insync.replicas` (Broker/Topic Config)

**What**
- A broker- or topic-level setting that defines the minimum number of in-sync replicas that must acknowledge a write when `acks=all`.

**Typical value**
- `min.insync.replicas=2` (with replication factor of 3).

**Why it matters**
- Even with `acks=all`, if only 1 replica is in-sync, the message is effectively only persisted once.
- Setting `"min.insync.replicas=2"` ensures at least 2 copies exist before acknowledging.

**Failure behavior**
- If ISR count drops below `min.insync.replicas`, the broker returns `NotEnoughReplicasException` and the producer retries or fails - this is desired because it prevents under-replicated writes.

---

### 5) `max.in.flight.requests.per.connection`

**What**
- Controls how many unacknowledged requests the producer will send on a single connection before blocking.

**Default**
- `5`.

**Why it matters**
- With `max.in.flight.requests.per.connection > 1` and retries enabled (without idempotence), messages can arrive out of order at the broker.
- With idempotence enabled, Kafka guarantees ordering even with up to 5 in-flight requests.
- Set to `1` for strict ordering without idempotence.

**Spring Boot config**
```yaml
spring:
  kafka:
    producer:
      properties:
        max.in.flight.requests.per.connection: 5
```

---

### 6) Producer Timeouts

**Key configs**
- `delivery.timeout.ms` - Total time for a message to be sent and acknowledged (includes retries). Default: `120000ms`.
- `request.timeout.ms` - Time the producer waits for a response from the broker for a single request. Default: `30000ms`.
- `linger.ms` - Time the producer waits to accumulate a batch before sending. Default: `0ms`.
- `max.block.ms` - Time the `send()` call blocks waiting for buffer space or metadata. Default: `60000ms`.

**Relationship**
- `delivery.timeout.ms >= linger.ms + request.timeout.ms`.

**Why it matters**
- Misconfigured timeouts can cause premature failures or excessively long waits.

---

### 7) Recommended Reliable Producer Configuration (Summary)

The "gold standard" configuration for a reliable Kafka producer:

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 10
      key-serializer: org.apache.kafka.common.serialization.IntegerSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        retry.backoff.ms: 1000
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
        linger.ms: 0
```

Combined with broker/topic settings:
```
replication.factor=3
min.insync.replicas=2
```

---

### 8) Configuring the Reliable Producer in Spring Boot (Hands-On)

- Walk through updating `application.yml` with the reliable config.
- Demonstrate the behavior difference between `acks=1` and `acks=all`.
- Show how `min.insync.replicas` interacts with `acks=all`.
- Show producer logs when retries happen.

---

## Part 2: Application-Level Error Handling & Retry

Once the config-level reliability is in place, the next layer is application-level error handling. This group covers how to classify errors (retriable vs non-retriable), how to manually retry via `CompletableFuture` chaining, and how to implement callbacks that respond to producer failures.

### 9) Handling Retriable vs Non-Retriable Errors

**Retriable errors**
- Transient failures where a retry is likely to succeed.
- `NOT_LEADER_FOR_PARTITION`
- `REQUEST_TIMED_OUT`
- `NETWORK_EXCEPTION`
- `NotEnoughReplicasException`

**Non-retriable errors**
- Permanent failures where retrying will not help.
- `MESSAGE_TOO_LARGE`
- `SERIALIZATION_ERROR`
- `AUTHORIZATION_FAILED`
- `TOPIC_AUTHORIZATION_FAILED`

**Application-level handling**
- In `LibraryEventProducer`, the `whenComplete` callback or `try/catch` (synchronous) should differentiate between these and take appropriate action (for example, log, alert, send to DLQ).

**Code example — classify error in `whenComplete`**
```java
// In LibraryEventProducer
future.whenComplete((result, ex) -> {
    if (ex != null) {
        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
        if (cause instanceof RetriableException) {
            logger.warn("Retriable error — Kafka built-in retries will handle this. key={}", key, ex);
        } else {
            logger.error("Non-retriable error — escalating. key={} event={}", key, libraryEvent, ex);
            // send to DLQ, raise an alert, or return a failure response
        }
        return;
    }
    logger.info("Published library event. topic={} partition={} offset={} key={}",
            result.getRecordMetadata().topic(),
            result.getRecordMetadata().partition(),
            result.getRecordMetadata().offset(),
            key);
});
```

> `RetriableException` is from `org.apache.kafka.common.errors.RetriableException`. All Kafka retriable errors extend it, so a single `instanceof` check covers the full retriable category.

---

### 10) Application-Level Retry (Spring Retry / Custom Logic)

**What**
- In addition to Kafka's built-in producer retries, you can add application-level retry at the controller/service layer.

**Use case**
- When `send()` future completes exceptionally (for example, after all Kafka retries are exhausted), you may want to retry the entire operation or send to a fallback.

**Options**
- Spring Retry (`@Retryable` annotation).
- Manual retry with `CompletableFuture` chaining.
- Circuit breaker pattern (Resilience4j).

**Why it matters**
- Kafka retries only handle broker-level transient errors.
- Application-level retry can handle broader failure scenarios (for example, serialization retry after fix, timeout-based backoff).

**Code example — manual retry with `CompletableFuture` chaining**
```java
// In LibraryEventProducer
public CompletableFuture<SendResult<Integer, LibraryEvent>> sendLibraryEventWithRetry(
        LibraryEvent libraryEvent, int attemptsLeft) {

    Integer key = libraryEvent.libraryEventId();

    return sendLibraryEvent(libraryEvent)
            .exceptionallyCompose(ex -> {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                if (attemptsLeft > 0 && cause instanceof RetriableException) {
                    logger.warn("Retrying after retriable error. attemptsLeft={} key={}", attemptsLeft, key);
                    return sendLibraryEventWithRetry(libraryEvent, attemptsLeft - 1);
                }
                logger.error("Exhausted retries or non-retriable error. key={}", key, ex);
                return CompletableFuture.failedFuture(ex);
            });
}
```

**Code example — Spring Retry with `@Retryable`**

Add dependency to `pom.xml`:
```xml
<dependency>
    <groupId>org.springframework.retry</groupId>
    <artifactId>spring-retry</artifactId>
</dependency>
```

Enable in your main application class or config:
```java
@EnableRetry
@SpringBootApplication
public class LibraryEventsProducerApplication { ... }
```

Annotate the send method:
```java
@Retryable(
    retryFor = {RetriableException.class},
    maxAttempts = 3,
    backoff = @Backoff(delay = 1000, multiplier = 2)
)
public CompletableFuture<SendResult<Integer, LibraryEvent>> sendLibraryEvent(LibraryEvent libraryEvent) {
    // existing send logic
}

@Recover
public CompletableFuture<SendResult<Integer, LibraryEvent>> recoverSend(
        RetriableException ex, LibraryEvent libraryEvent) {
    logger.error("All retries exhausted. Sending to fallback. event={}", libraryEvent, ex);
    // send to DLQ or return a failure signal
    return CompletableFuture.failedFuture(ex);
}
```

---

### 11) Error Handling in Callbacks / CompletableFuture

**Async approach** (`whenComplete`) — matches current `LibraryEventProducer`
```java
// In LibraryEventProducer.sendLibraryEvent()
future.whenComplete((result, ex) -> {
    if (ex != null) {
        logger.error("Failed to publish library event. key={} event={}", key, libraryEvent, ex);
        return;
    }
    logger.info(
            "Published library event. topic={} partition={} offset={} key={} event={}",
            result.getRecordMetadata().topic(),
            result.getRecordMetadata().partition(),
            result.getRecordMetadata().offset(),
            key,
            libraryEvent);
});
```

**Sync approach** (`.get()`) — matches current `LibraryEventProducer.sendLibraryEventSynchronous()`
```java
try {
    SendResult<Integer, LibraryEvent> result =
            key == null
                    ? kafkaTemplate.send(topicName, libraryEvent).get()
                    : kafkaTemplate.send(topicName, key, libraryEvent).get();

    logger.info("Published library event synchronously. topic={} partition={} offset={} key={}",
            result.getRecordMetadata().topic(),
            result.getRecordMetadata().partition(),
            result.getRecordMetadata().offset(),
            key);

    return result;
} catch (ExecutionException ex) {
    logger.error("Failed to publish library event synchronously. key={} event={}", key, libraryEvent, ex);
    throw ex;
} catch (InterruptedException ex) {
    Thread.currentThread().interrupt();
    throw ex;
}
```

**Why it matters**
- Unhandled exceptions in callbacks silently drop errors. Every producer must have explicit error handling.

---

## Part 3: Reliability Testing

With config and error-handling code in place, this part focuses on proving correctness. Tests are split into two separate concerns: unit tests that verify error classification logic in isolation, and integration tests that verify the full produce-to-Kafka lifecycle using a real (embedded) broker.

---

### 12a) Unit Test Hints

Unit tests use `@ExtendWith(MockitoExtension.class)` and mock `KafkaTemplate`. No Spring context is started. They run fast and are focused on the error-handling logic inside `LibraryEventProducer`.

**What to test**
- Happy path: `send()` with a null key calls `kafkaTemplate.send(topic, event)`; with a non-null key calls `kafkaTemplate.send(topic, key, event)`.
- Failure path: when `KafkaTemplate.send()` returns a failed future, the returned `CompletableFuture` completes exceptionally.
- Error classification: verify that a `NetworkException` cause `isInstanceOf(RetriableException.class)` and a `RecordTooLargeException` cause `isNotInstanceOf(RetriableException.class)`.
- Synchronous path: `sendLibraryEventSynchronous()` rethrows the underlying exception on failure.

**Skeleton**
```java
@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @Mock
    KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;

    @InjectMocks
    LibraryEventProducer producer;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(producer, "topicName", "library-events");
    }

    // --- Happy path ---

    @Test
    void sendLibraryEvent_withNullKey_callsSendWithoutKey() {
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, someBook());
        when(kafkaTemplate.send(eq("library-events"), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(buildSendResult(null, event)));

        CompletableFuture<SendResult<Integer, LibraryEvent>> future = producer.sendLibraryEvent(event);

        assertThat(future.isDone()).isTrue();
        verify(kafkaTemplate).send("library-events", event);
    }

    // --- Failure path — retriable ---

    @Test
    void sendLibraryEvent_withNetworkException_causeIsRetriable() {
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, someBook());
        CompletableFuture<SendResult<Integer, LibraryEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new NetworkException("broker unreachable"));
        when(kafkaTemplate.send(eq("library-events"), eq(event))).thenReturn(failed);

        CompletableFuture<SendResult<Integer, LibraryEvent>> future = producer.sendLibraryEvent(event);

        assertThat(future.isCompletedExceptionally()).isTrue();
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown.getCause())
                .isInstanceOf(NetworkException.class)
                .isInstanceOf(RetriableException.class);
    }

    // --- Failure path — non-retriable ---

    @Test
    void sendLibraryEvent_withRecordTooLargeException_causeIsNotRetriable() {
        LibraryEvent event = new LibraryEvent(null, LibraryEventType.ADD, someBook());
        CompletableFuture<SendResult<Integer, LibraryEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RecordTooLargeException("message too large"));
        when(kafkaTemplate.send(eq("library-events"), eq(event))).thenReturn(failed);

        CompletableFuture<SendResult<Integer, LibraryEvent>> future = producer.sendLibraryEvent(event);

        assertThat(future.isCompletedExceptionally()).isTrue();
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown.getCause())
                .isInstanceOf(RecordTooLargeException.class)
                .isNotInstanceOf(RetriableException.class);
    }
}
```

> See `LibraryEventProducerTest` in `src/test` for the full implementation.

---

### 12b) Integration Test Hints

Integration tests use `@SpringBootTest` + `@EmbeddedKafka`. The full Spring context starts with a real (in-process) Kafka broker. No mocking — the actual `KafkaTemplate` sends to the embedded broker.

**What to test**
- HTTP response: POST returns `201 Created`; PUT returns `202 Accepted`.
- Validation rejections: null book, blank book name, wrong event type all return `400 Bad Request`.
- Kafka message delivery: after a successful POST or PUT, a consumer reading from the embedded broker finds the record with the correct key and payload.

**Key setup**
```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = "library-events")
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "library.events.topic=library-events"
})
class LibraryEventsControllerIntegrationTest {

    @Autowired MockMvc mockMvc;
    @Autowired EmbeddedKafkaBroker embeddedKafkaBroker;
}
```

**Kafka message delivery hint — what the consumer setup looks like**
```java
// 1. Send the event via HTTP
mockMvc.perform(post("/v1/library-events")
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(event)))
        .andExpect(status().isCreated());

// 2. Create a consumer that reads from the beginning of the topic
//    Use a unique group ID per test so each consumer starts at offset 0
Consumer<Integer, String> consumer = createTestConsumer("verify-post-" + System.nanoTime());
embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "library-events");

// 3. Poll in a loop until the record is found or the timeout expires
//    Use unique book content per test to identify the right record
ConsumerRecord<Integer, String> found = waitForRecord(consumer, "My Unique Book Title", Duration.ofSeconds(5));
assertThat(found).isNotNull();
assertThat(found.key()).isNull();             // ADD event — no key
assertThat(found.value()).contains("\"ADD\"");
consumer.close();
```

**Why `earliest` + unique content?**
- The producer send is async — the HTTP 201 can return before the record reaches the broker.
- Reading from `earliest` ensures the record is found even if it arrives slightly after the consumer starts.
- Using unique book titles per test avoids false positives from records produced by other tests in the same topic.

**`waitForRecord` helper pattern**
```java
private ConsumerRecord<Integer, String> waitForRecord(
        Consumer<Integer, String> consumer, String contentContains, Duration timeout) {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<Integer, String> record : records) {
            if (record.value().contains(contentContains)) {
                return record;
            }
        }
    }
    return null;  // timed out — test will fail on assertThat(found).isNotNull()
}
```

> See `LibraryEventsControllerIntegrationTest` in `src/test` for the full implementation.

---

## Topic Dependency Flow

```text
acks=all  ----------------------\
                                v
min.insync.replicas --> Durable Writes (no data loss)
                                |
retries + retry.backoff.ms -----|
                                v
enable.idempotence ------> No Duplicates from Retries
                                |
max.in.flight.requests ---------|
                                v
                     Ordered + Deduplicated Messages
                                |
delivery.timeout.ms ----------- |
                                v
Error Handling (Callback) --> Graceful Failure / DLQ / Alert
```

---

## Mapping to Current Project

| Topic | Current State | Action Needed |
|---|---|---|
| `acks` | Not explicitly set (defaults to `1`) | Set to `all` |
| `retries` | Not explicitly set (defaults vary) | Explicitly configure |
| `enable.idempotence` | Not set | Enable explicitly |
| `min.insync.replicas` | Not configured | Configure on topic/broker |
| `max.in.flight.requests` | Not set | Confirm default `5` with idempotence |
| Error handling | Basic `whenComplete` callback | Enhance with retriable vs non-retriable logic |
| Application-level retry | Not implemented | Add Spring Retry or custom logic |
| Testing reliability | Basic tests exist | Add failure-injection tests |

---

## Suggested Implementation Order

1. `acks` - Start here; this is the foundation of producer reliability.
2. `min.insync.replicas` - Pair with `acks=all`.
3. Retries and retry backoff - Define behavior for transient failures.
4. Idempotent producer - Prevent duplicates from retries.
5. `max.in.flight.requests` - Confirm ordering guarantees.
6. Producer timeouts - Tune timing behavior.
7. Recommended config - Consolidate final producer settings.
8. Retriable vs non-retriable errors - Implement correct handling paths.
9. Error handling in callbacks - Implement application-level response.
10. Application-level retry - Add resilience beyond Kafka built-in retries.
11. Testing reliability - Prove behavior under failure modes.
12. Hands-on walkthrough - Apply all settings to Library Events Producer.

---

## Implementation Checklist

- [ ] Set `acks=all`.
- [ ] Configure `retries`, `retry.backoff.ms`, and `delivery.timeout.ms`.
- [ ] Enable `enable.idempotence=true`.
- [ ] Validate topic/broker replication strategy (`replication.factor=3`, `min.insync.replicas=2`).
- [ ] Confirm `max.in.flight.requests.per.connection` aligns with ordering needs.
- [ ] Implement retriable vs non-retriable error handling.
- [ ] Add callback/synchronous error handling paths.
- [ ] Add reliability tests and failure-injection scenarios.
