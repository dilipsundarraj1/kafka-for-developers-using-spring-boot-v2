# Reliable Kafka Producer Reference

This reference doc is for students implementing reliable producer strategies in the Library Events Producer project.

It keeps the same technical content, but organizes it in an implementation-first format.

## Table of Contents

- [How to Use This Reference](#how-to-use-this-reference)
- [Reliability Strategy Reference](#reliability-strategy-reference)
  - [1) Acknowledgment Modes (`acks`)](#1-acknowledgment-modes-acks)
  - [2) Retries and Retry Backoff](#2-retries-and-retry-backoff)
  - [3) Idempotent Producer (`enable.idempotence`)](#3-idempotent-producer-enableidempotence)
  - [4) `min.insync.replicas` (Broker/Topic Config)](#4-mininsyncreplicas-brokertopic-config)
  - [5) `max.in.flight.requests.per.connection`](#5-maxinflightrequestsperconnection)
  - [6) Handling Retriable vs Non-Retriable Errors](#6-handling-retriable-vs-non-retriable-errors)
  - [7) Producer Timeouts](#7-producer-timeouts)
  - [8) Application-Level Retry (Spring Retry / Custom Logic)](#8-application-level-retry-spring-retry--custom-logic)
  - [9) Error Handling in Callbacks / CompletableFuture](#9-error-handling-in-callbacks--completablefuture)
  - [10) Recommended Reliable Producer Configuration (Summary)](#10-recommended-reliable-producer-configuration-summary)
  - [11) Testing Reliability](#11-testing-reliability)
  - [12) Configuring the Reliable Producer in Spring Boot (Hands-On)](#12-configuring-the-reliable-producer-in-spring-boot-hands-on)
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

## Reliability Strategy Reference

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
- Setting `min.insync.replicas=2` ensures at least 2 copies exist before acknowledging.

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

### 6) Handling Retriable vs Non-Retriable Errors

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

---

### 7) Producer Timeouts

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

### 8) Application-Level Retry (Spring Retry / Custom Logic)

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

---

### 9) Error Handling in Callbacks / CompletableFuture

**Async approach** (`whenComplete`)
```java
future.whenComplete((result, ex) -> {
    if (ex != null) {
        // Log, alert, send to DLQ
    } else {
        // Log success with metadata
    }
});
```

**Sync approach** (`.get()`)
```java
try {
    SendResult<Integer, LibraryEvent> result = kafkaTemplate.send(...).get();
} catch (ExecutionException ex) {
    // Handle Kafka errors
} catch (InterruptedException ex) {
    Thread.currentThread().interrupt();
}
```

**Why it matters**
- Unhandled exceptions in callbacks silently drop errors. Every producer must have explicit error handling.

---

### 10) Recommended Reliable Producer Configuration (Summary)

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

### 11) Testing Reliability

- Unit tests: Mock `KafkaTemplate` to simulate `send()` failures and verify error handling logic.
- Integration tests: Use `EmbeddedKafka` to test actual produce-and-consume flows.
- Failure injection: Simulate broker unavailability, slow networks, and serialization errors to validate retry behavior and error handling.

---

### 12) Configuring the Reliable Producer in Spring Boot (Hands-On)

- Walk through updating `application.yml` with the reliable config.
- Demonstrate the behavior difference between `acks=1` and `acks=all`.
- Show how `min.insync.replicas` interacts with `acks=all`.
- Show producer logs when retries happen.

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
6. Retriable vs non-retriable errors - Implement correct handling paths.
7. Producer timeouts - Tune timing behavior.
8. Error handling in callbacks - Implement application-level response.
9. Application-level retry - Add resilience beyond Kafka built-in retries.
10. Recommended config - Consolidate final producer settings.
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
