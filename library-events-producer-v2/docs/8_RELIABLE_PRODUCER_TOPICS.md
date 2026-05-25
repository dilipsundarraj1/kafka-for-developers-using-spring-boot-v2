# Reliable Kafka Producer Reference

This reference doc is for students implementing reliable producer strategies in the Library Events Producer project.

It keeps the same technical content, but organizes it in an implementation-first format.

## Table of Contents

- [How to Use This Reference](#how-to-use-this-reference)
- [Topic Dependency Flow](#topic-dependency-flow)
- [Mapping to Current Project](#mapping-to-current-project)
- [Part 1: Producer Reliability Configuration](#part-1-producer-reliability-configuration)
  - [1) Acknowledgment Modes (`acks`)](#1-acknowledgment-modes-acks)
    - [1.1) `min.insync.replicas` (Broker/Topic Config)](#11-mininsyncreplicas-brokertopic-config)
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

## Topic Dependency Flow

```text
acks=-1  ----------------------\
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
| `acks` | Not explicitly set (defaults to `-1` / `all` since Kafka 3.0+) | Already set to `all`; no change needed |
| `retries` | Not explicitly set (defaults vary) | Explicitly configure |
| `enable.idempotence` | Not set | Enable explicitly |
| `min.insync.replicas` | Not configured | Configure on topic/broker |
| `max.in.flight.requests` | Not set | Confirm default `5` with idempotence |
| Error handling | Basic `whenComplete` callback | Enhance with retriable vs non-retriable logic |
| Application-level retry | Not implemented | Add Spring Retry or custom logic |
| Testing reliability | Basic tests exist | Add failure-injection tests |

---

## Part 1: Producer Reliability Configuration

These settings are applied in `application.yml` (or via `KafkaProducerConfig`) and form the foundation of a reliable producer. They control how the producer communicates with the broker, how it handles transient failures at the Kafka protocol level, and how it avoids data loss or duplication.

### 1) Acknowledgment Modes (`acks`)

**What**
- The `acks` producer configuration controls how many broker replicas must acknowledge a write before the producer considers it successful.

**Values**
- `acks=0` - Fire and forget; no acknowledgment (fastest, least reliable).
- `acks=1` - Leader acknowledgment only; message is written to the leader's log.
- `acks=-1` (`all`) - All in-sync replicas (ISR) must acknowledge (slowest, most reliable). **This is the default since Kafka 3.0+ / Spring Boot 3+.**

**How it works internally**
- When the producer calls `send()`, the message is placed in an internal buffer and then sent to the partition leader on the broker.
- With `acks=1`, the leader writes the message to its local log and immediately sends an acknowledgment back to the producer. The followers replicate asynchronously — if the leader crashes before replication completes, the message is lost.
- With `acks=-1`, the leader waits until all replicas in the ISR have written the message to their logs before acknowledging. This guarantees the message survives a leader failure because at least one follower has the message.
- The ISR (In-Sync Replicas) is the set of replicas that are fully caught up with the leader. A replica falls out of the ISR if it lags behind by more than `replica.lag.time.max.ms`.

**Data loss scenario with `acks=1`**
```text
Producer → sends message M1 → Leader (Broker 1) writes to log → ACK sent to producer ✓
                                      ↓ replication in progress...
                               Broker 1 crashes before replication completes
                               Broker 2 elected as new leader (does NOT have M1)
                               M1 is permanently lost
```

**No data loss with `acks=-1`**
```text
Producer → sends message M1 → Leader (Broker 1) writes to log
                               Follower (Broker 2) writes to log
                               Follower (Broker 3) writes to log
                               All ISR replicas confirmed → ACK sent to producer ✓
                               Broker 1 crashes
                               Broker 2 elected as new leader (already has M1)
                               M1 is safe
```

**Trade-offs**

| Setting | Throughput | Latency | Durability | Example Use Case |
|---|---|---|---|---|
| `acks=0` | Highest | Lowest | None — messages can be lost | Retail: logging every product page view or homepage impression during a flash sale — losing a few view counts is acceptable, and throughput must keep up with thousands of events per second |
| `acks=1` | High | Low | Partial — leader crash can lose data | Clickstream or user activity tracking where losing a small number of events under failure is tolerable |
| `acks=-1` | Lower | Higher | Full — survives leader failure | Financial transactions, order events, or any domain where every message must be durably persisted |

**Common pitfall**
- Setting `acks=-1` alone is not enough. If `min.insync.replicas=1`, the broker only requires one replica (the leader itself) to acknowledge. You must set `min.insync.replicas=2` alongside `acks=-1` to get true durability (see Section 1.1).

#### 1.1) `min.insync.replicas` (Broker/Topic Config)

`min.insync.replicas` is a broker/topic-level safety gate used with `acks=-1`.

- Recommended for production with RF=3: `min.insync.replicas=2`
- Effect: the leader rejects writes if fewer than 2 replicas are in ISR
- Failure mode when ISR drops below 2: producer sees `NotEnoughReplicasException` and retries

**Safe combinations**

| Replication Factor | `min.insync.replicas` | Broker failures tolerated | Notes |
|---|---|---|---|
| 3 | 2 | 1 | Recommended for production |
| 3 | 3 | 0 | Maximum durability, zero fault tolerance |
| 3 | 1 | 2 | Same as `acks=1` — not truly safe |
| 1 | 1 | 0 | Development only |

**Failure scenario**
```text
Cluster: 3 brokers, replication.factor=3, min.insync.replicas=2

Broker 2 and Broker 3 restart simultaneously → ISR = {Broker 1} (size=1)
Producer sends M1 with acks=-1
Broker 1 checks: ISR size (1) < min.insync.replicas (2) → NotEnoughReplicasException
Producer retries after retry.backoff.ms
Broker 2 recovers → ISR = {Broker 1, Broker 2} (size=2)
Producer retries again → write succeeds ✓
```

**Topic setup to enforce durability (`replication.factor=3`, `min.insync.replicas=2`)**

Use the command below to update `library-events` and enforce `min.insync.replicas=2`:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --create --if-not-exists --topic library-events --partitions 3 --replication-factor 3

docker exec kafka1 kafka-configs --bootstrap-server kafka1:19092 --entity-type topics --entity-name library-events --alter --add-config min.insync.replicas=2
```

**Why it matters**
- `acks=-1` is required for a reliable producer. Without it, data can be lost if the leader crashes before replicating.

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

**How it works internally**
- When `send()` fails with a retriable error, the producer does not immediately return a failure to the application. Instead, it waits `retry.backoff.ms` and re-sends the same message to the broker.
- The retry loop continues until either the send succeeds, the `retries` count is exhausted, or `delivery.timeout.ms` is exceeded — whichever comes first.
- Between retries, the producer refreshes its metadata to discover the new leader for the partition.
- The retry is transparent to the application — the `CompletableFuture` returned by `kafkaTemplate.send()` only completes (successfully or exceptionally) after all retries are finished.

#### 2.1 Transient Broker/Network Errors (First Failure Scenario)

**Topic setup for transient-failure testing (`min.insync.replicas=2`)**

Use the command below to update `library-events` and enforce `min.insync.replicas=2`:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --create --if-not-exists --topic library-events --partitions 3 --replication-factor 3

docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --alter --topic library-events \
  --config min.insync.replicas=2
```

**Retry timeline (transient error)**
```text
t=0ms     send() called — broker returns NOT_LEADER_FOR_PARTITION
t=1000ms  retry 1 — broker still in leader election
t=2000ms  retry 2 — new leader elected, message accepted → ACK ✓

Total time: ~2000ms  (well within delivery.timeout.ms=120000ms)
```

**Examples of transient broker/network failures**
- `NOT_LEADER_FOR_PARTITION` - The broker the producer sent to is no longer the leader for that partition (for example, after a leader election due to broker restart or crash). A retry will discover the new leader via metadata refresh.
- `REQUEST_TIMED_OUT` - The broker did not respond within `request.timeout.ms`. Could be caused by a temporary GC pause, disk I/O spike, or network congestion.
- `NETWORK_EXCEPTION` - A TCP-level failure such as a broken connection, DNS resolution failure, or temporary network partition between the producer and the broker.
- `NotEnoughReplicasException` - The broker cannot satisfy `min.insync.replicas` because one or more replicas are temporarily out of sync (for example, a follower broker is restarting). Once the replica catches up, the retry succeeds.
- `LEADER_NOT_AVAILABLE` - A new topic/partition was just created or a leader election is in progress. Metadata will be refreshed and the retry will find the new leader.
- `UNKNOWN_TOPIC_OR_PARTITION` - The broker's metadata cache has not caught up yet (for example, topic was just created). A retry after metadata refresh resolves it.
- `CORRUPT_MESSAGE` (CRC check failure) - Rare; caused by transient data corruption during network transmission. A retry sends a fresh copy.

**Common pitfall**
- Setting `retries` to a high number without also setting `delivery.timeout.ms` appropriately can cause a message to be retried for up to 2 minutes (the default). In high-throughput systems, this can cause the producer buffer to fill up and back-pressure the application.
- Setting `retry.backoff.ms` too low (for example, `100ms`) during a prolonged broker outage floods the broker with retry requests before it has recovered. A value of `1000ms` is a safer default.

**Why it matters**
- Retries handle transient broker/network failures transparently — the producer recovers automatically without application intervention.

#### 2.2 Cluster Down Behavior (Follow-up Scenario)

When the entire Kafka cluster is down, the producer cannot fetch metadata for the target topic.

**What you will see**
- Repeating background warnings such as:
  - `Bootstrap broker localhost:9092 (id: -1 ...) disconnected`
  - `Node -1 disconnected`
- Request-thread failure after metadata wait expires:
  - `org.apache.kafka.common.errors.TimeoutException: Topic library-events not present in metadata after 60000 ms.`

**Why this happens**
- `id=-1` is the bootstrap placeholder node used before the producer learns real broker IDs from metadata.
- While the cluster is down, metadata refresh fails repeatedly in the background.
- `send()` blocks while waiting for metadata up to `max.block.ms` (default `60000ms`), then fails fast for that request.
- Background reconnect attempts continue after the request fails; the producer is still trying to recover for future sends.

**Cluster-down timeline**
```text
t=0ms        send() called
t=0..60000ms metadata fetch retries continue; bootstrap node (-1) disconnect warnings repeat
t=60000ms    max.block.ms reached -> TimeoutException (topic not present in metadata)
t>60000ms    background network thread keeps reconnecting until broker returns
```

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

**How it works internally**
- When idempotence is enabled, the broker assigns each producer a unique **Producer ID (PID)**.
- Every message the producer sends includes a **sequence number** that increments per partition.
- If the producer retries a message (because an ACK was lost in transit), the broker detects the duplicate via the PID + sequence number combination and silently discards it — the consumer never sees it twice.
- Without idempotence, a lost ACK causes the producer to re-send, and the broker writes the message a second time since it has no way to detect the duplicate.

**Duplicate scenario without idempotence**
```text
Producer sends M1 (seq=1) → Broker writes M1 → ACK sent → ACK lost in network
Producer times out → retries M1                → Broker writes M1 again (duplicate!)
Consumer receives M1 twice ✗
```

**No duplicate with idempotence**
```text
Producer sends M1 (PID=42, seq=1) → Broker writes M1 → ACK sent → ACK lost in network
Producer times out → retries M1 (PID=42, seq=1)
Broker sees PID=42, seq=1 already written → discards duplicate → sends ACK ✓
Consumer receives M1 once ✓
```

**Implicit requirements**
- When idempotence is enabled, Kafka automatically enforces: `acks=-1`, `retries=Integer.MAX_VALUE`, and `max.in.flight.requests.per.connection <= 5`. If you set conflicting values, Kafka throws a `ConfigException` at startup.

**Common pitfall**
- Idempotence is **per-session only**. If the producer restarts, it gets a new PID. A message sent just before restart and retried after restart can still be duplicated. For cross-session exactly-once guarantees, Kafka Transactions are required.

**Why it matters**
- Retries can cause duplicates without idempotence. This guarantees exactly-once per partition semantics at the producer level within a single producer session.

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

This topic is intentionally covered with `acks` in `Section 1.1` so durability settings stay together.

Quick command reference:

```bash
kafka-topics.sh --alter --topic library-events \
  --config min.insync.replicas=2 \
  --bootstrap-server localhost:9092
```

---

### 5) `max.in.flight.requests.per.connection`

**What**
- Controls how many unacknowledged requests the producer will send on a single connection before blocking.

**Default**
- `5`.

**How it works internally**
- The producer can pipeline multiple batches to the broker without waiting for an ACK for each one. This improves throughput by keeping the network pipe full.
- If `max.in.flight.requests.per.connection=5`, the producer can have 5 batches in transit simultaneously per broker connection.
- Without idempotence, if batch 1 fails and is retried after batch 2 has already been sent and acknowledged, the broker ends up writing them out of order: M2, M1.
- With idempotence enabled, the broker uses the PID and sequence numbers to detect and reject out-of-order writes, enforcing correct ordering even with up to 5 in-flight requests.

**Message reordering scenario (without idempotence)**
```text
max.in.flight.requests=2, retries enabled, idempotence disabled

Producer sends batch [M1] and [M2] simultaneously
Broker ACKs [M2] but [M1] fails (network blip)
Producer retries [M1]
Broker writes: M2 → M1  ✗  (wrong order)
```

**With idempotence (ordering guaranteed)**
```text
max.in.flight.requests=5, idempotence enabled

Producer sends batches with sequence numbers: M1(seq=1), M2(seq=2), ...M5(seq=5)
M1 fails → producer retries M1(seq=1)
Broker sees seq=1 is next expected → writes M1 first, then continues ✓
```

**When to set it to `1`**
- If you need strict ordering AND cannot use idempotence (for example, older Kafka versions), set `max.in.flight.requests.per.connection=1`. This forces the producer to wait for an ACK before sending the next batch, eliminating reordering at the cost of throughput.

**Why it matters**
- With idempotence enabled, the safe and recommended value is `5` — it gives good throughput while guaranteeing order and no duplicates.

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

**What**
- A set of configs that control how long the producer waits at different stages of the send pipeline before giving up or retrying.

**Key configs**
- `delivery.timeout.ms` - Total time for a message to be sent and acknowledged (includes retries). Default: `120000ms`.
- `request.timeout.ms` - Time the producer waits for a response from the broker for a single request attempt. Default: `30000ms`.
- `linger.ms` - Time the producer waits to accumulate a batch before sending. Default: `0ms`.
- `max.block.ms` - Time the `send()` call blocks waiting for buffer space or metadata. Default: `60000ms`.

**How they relate — the send pipeline**
```text
send() called
    │
    ├─► max.block.ms — wait for buffer space and metadata fetch
    │
    ▼
Message batched in producer buffer
    │
    ├─► linger.ms — wait to accumulate more messages into the batch
    │
    ▼
Batch sent to broker
    │
    ├─► request.timeout.ms — wait for broker ACK for this attempt
    │         │
    │         └─► if timeout: retry after retry.backoff.ms
    │
    ▼
ACK received  ─────────────────────────────────────────────────────────┐
    │                                                                    │
    └─► delivery.timeout.ms — outer deadline covering ALL of the above ◄┘
```

**Relationship constraint**
- `delivery.timeout.ms >= linger.ms + request.timeout.ms`
- Violating this causes Kafka to throw a `ConfigException` at startup.

**Common pitfalls**
- Setting `delivery.timeout.ms` too low causes messages to expire before Kafka's built-in retries have a chance to recover from a transient failure.
- Setting `linger.ms` too high (for example, `100ms`) improves batch compression and throughput but adds latency to every message — only appropriate for high-throughput, latency-tolerant pipelines.
- Setting `max.block.ms` too low causes `send()` to throw `TimeoutException` if the broker metadata is briefly unavailable at startup, before the broker is ready.

**Recommended values for the Library Events Producer**

| Config | Recommended | Reason |
|---|---|---|
| `delivery.timeout.ms` | `120000` | 2 min outer cap; enough for 10 retries with 1s backoff |
| `request.timeout.ms` | `30000` | Wait up to 30s for a single broker response |
| `linger.ms` | `0` | Low-latency API — send immediately |
| `max.block.ms` | `60000` | Block up to 60s for metadata on startup |

**Spring Boot config**
```yaml
spring:
  kafka:
    producer:
      properties:
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
        linger.ms: 0
```

---

### 7) Recommended Reliable Producer Configuration (Summary)

The "gold standard" configuration for a reliable Kafka producer, combining all the settings from Sections 1–6:

```yaml
spring:
  kafka:
    producer:
      acks: all                                                          # Section 1 — require all ISR replicas to ACK
      retries: 10                                                        # Section 2 — retry up to 10 times on transient errors
      key-serializer: org.apache.kafka.common.serialization.IntegerSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
      properties:
        enable.idempotence: true                                         # Section 3 — prevent duplicates from retries
        max.in.flight.requests.per.connection: 5                         # Section 5 — safe with idempotence; maintains order
        retry.backoff.ms: 1000                                           # Section 2 — wait 1s between retries
        delivery.timeout.ms: 120000                                      # Section 6 — 2 min outer deadline
        request.timeout.ms: 30000                                        # Section 6 — 30s per broker request
        linger.ms: 0                                                     # Section 6 — send immediately, no batching delay
```

Combined with broker/topic settings (not in `application.yml`):
```properties
replication.factor=3          # 3 copies of each partition across brokers
min.insync.replicas=2         # Section 1.1 — at least 2 replicas must ACK before writing
```

**Why each setting earns its place**

| Setting | Without it | With it |
|---|---|---|
| `acks=-1` | Message lost if leader crashes before replication | Message survives leader failure |
| `retries=10` | Transient failures surface as errors to the app | App recovers automatically |
| `enable.idempotence` | Retries produce duplicate messages | Exactly-once per partition per session |
| `min.insync.replicas=2` | `acks=-1` satisfied by 1 replica (no real safety) | Requires 2 copies before ACK |
| `max.in.flight=5` | Must set to 1 for ordering without idempotence | Safe at 5 with idempotence |
| `retry.backoff.ms=1000` | Retries hammer recovering broker immediately | Gives broker time to recover |
| `delivery.timeout.ms=120000` | Retries may expire before recovery completes | Enough headroom for 10 retries |

---

### 8) Configuring the Reliable Producer in Spring Boot (Hands-On)

**Step 1 — Update `application.yml`**

Add the full reliable producer config under `spring.kafka.producer`:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      retries: 10
      key-serializer: org.apache.kafka.common.serialization.IntegerSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        retry.backoff.ms: 1000
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
        linger.ms: 0
```

**Step 2 — Verify config is applied at startup**

Spring Boot logs the effective Kafka producer config at `DEBUG` level. Enable it temporarily to confirm:

```yaml
logging:
  level:
    org.apache.kafka.clients.producer: DEBUG
```

Look for lines like:
```
ProducerConfig values:
  acks = all
  enable.idempotence = true
  retries = 10
  ...
```

**Step 3 — Observe `acks=1` vs `acks=-1` behavior**

- With `acks=1`: `send()` completes as soon as the leader writes to its log. Under a rolling broker restart, you may occasionally see messages lost without any error.
- With `acks=-1`: `send()` only completes after all ISR replicas acknowledge. Under a rolling restart, the producer may briefly see `NotEnoughReplicasException` and retry — but no messages are lost.

**Step 4 — Observe retry logs**

When a broker is temporarily unavailable, you will see log lines like:

```
WARN  o.a.k.c.p.i.Sender - [Producer ...] Got error produce response with correlation id 5
      on topic-partition library-events-0, retrying (9 attempts left). Error: NOT_LEADER_FOR_PARTITION
```

After the retry succeeds:
```
INFO  c.l.producer.LibraryEventProducer - Published library event.
      topic=library-events partition=0 offset=101 key=42
```

**Step 5 — Set `min.insync.replicas` on the topic**

This cannot be set in `application.yml` — use the Kafka CLI or your Docker Compose setup:
```bash
kafka-topics.sh --alter --topic library-events \
  --config min.insync.replicas=2 \
  --bootstrap-server localhost:9092
```

Verify:
```bash
kafka-topics.sh --describe --topic library-events --bootstrap-server localhost:9092
```

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

---

### 10) Application-Level Error Handling

**What**
- In addition to Kafka's built-in producer retries, the application layer should handle errors that surface after all Kafka retries are exhausted — such as logging, alerting, or routing to a fallback.

**Why it matters**
- Kafka retries only handle broker-level transient errors. Once those retries are exhausted, the failed future or exception reaches your code. Without explicit handling, the error is silently swallowed.

---

Implementation examples for async `whenComplete` and sync `.get()` with `try/catch` are documented in **Section 11: Error Handling in Callbacks / CompletableFuture**.

---

### What To Do With a Failed Record

Once all Kafka retries are exhausted, the error surfaces to your application. At that point you have two real options. This course will log the failed record — both alerting options below are out of scope here but are shown so you know what to implement in production.

---

#### Option 1 — Save the Failed Record to a Database

Persist the failed event to a database so it can be inspected, replayed, or dead-lettered later.

**Implementation steps**

1. **Add a `FailedEvent` entity** to your data model:

```java
@Entity
@Table(name = "failed_events")
public class FailedEvent {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Integer eventKey;

    @Column(columnDefinition = "TEXT")
    private String eventPayload;   // serialize LibraryEvent to JSON

    private String errorMessage;
    private String errorType;      // "RETRIABLE" or "NON_RETRIABLE"
    private LocalDateTime failedAt;
    private String status;         // "PENDING_REPLAY", "REPLAYED", "DEAD_LETTERED"
}
```

2. **Inject a `FailedEventRepository`** (Spring Data JPA) into `LibraryEventProducer`.

3. **Persist inside the error handler** (`whenComplete` or `catch` block):

```java
// whenComplete callback
if (ex != null) {
    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
    String errorType = (cause instanceof RetriableException) ? "RETRIABLE" : "NON_RETRIABLE";

    // --- Option 1: Save to DB ---
    FailedEvent failedEvent = new FailedEvent();
    failedEvent.setEventKey(key);
    failedEvent.setEventPayload(objectMapper.writeValueAsString(libraryEvent));
    failedEvent.setErrorMessage(cause.getMessage());
    failedEvent.setErrorType(errorType);
    failedEvent.setFailedAt(LocalDateTime.now());
    failedEvent.setStatus("PENDING_REPLAY");
    failedEventRepository.save(failedEvent);

    logger.error("Failed event saved to DB for replay. key={} errorType={}", key, errorType, ex);
}
```

4. **Replay later** by querying for `status = 'PENDING_REPLAY'` records (for example, via a scheduled job or an admin endpoint) and re-calling `sendLibraryEvent()`.

---

#### Option 2 — Send an Alert to a Notification Channel (Slack / Grafana)

Push an alert to your team's monitoring system so on-call engineers are notified immediately.

**Option 2a — Slack (via Incoming Webhook)**

1. Create an Incoming Webhook URL in your Slack workspace (Slack App → Incoming Webhooks).
2. Add the webhook URL to `application.yml`:

```yaml
alerts:
  slack:
    webhook-url: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

3. Send an HTTP POST from the error handler:

```java
// In the error handler block
String slackMessage = String.format(
    ":red_circle: *Kafka producer failure*%n"
    + "Topic: `%s`  Key: `%s`%n"
    + "Error: `%s`%n"
    + "Event: `%s`",
    topicName, key, cause.getMessage(),
    objectMapper.writeValueAsString(libraryEvent));

String payload = objectMapper.writeValueAsString(Map.of("text", slackMessage));

HttpClient.newHttpClient().sendAsync(
    HttpRequest.newBuilder()
        .uri(URI.create(slackWebhookUrl))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload))
        .build(),
    HttpResponse.BodyHandlers.discarding());
```

**Option 2b — Grafana (via Alertmanager / annotation API)**

1. Record a Grafana annotation on the dashboard for every producer failure:

```java
// POST to Grafana annotations API
String annotationBody = objectMapper.writeValueAsString(Map.of(
    "dashboardId", 42,                       // your dashboard ID
    "panelId",     7,                        // your panel ID
    "time",        Instant.now().toEpochMilli(),
    "tags",        List.of("kafka", "producer-failure"),
    "text",        "Producer failed: key=" + key + " error=" + cause.getMessage()
));

HttpClient.newHttpClient().sendAsync(
    HttpRequest.newBuilder()
        .uri(URI.create("http://grafana:3000/api/annotations"))
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer " + grafanaApiToken)
        .POST(HttpRequest.BodyPublishers.ofString(annotationBody))
        .build(),
    HttpResponse.BodyHandlers.discarding());
```

2. Alternatively, expose a Micrometer counter (`producer.failures`) and configure a Grafana alert rule on that metric — no code change needed in the error handler beyond incrementing the counter.

---

> **In this course** we will only log the failed record. Persisting to a database and sending Slack/Grafana alerts are production concerns that are out of scope for this course. The patterns above are provided so you know exactly what to plug in when you need them.

```java
// What we do in this course — log the failed record
if (ex != null) {
    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
    if (cause instanceof RetriableException) {
        logger.error("Retriable error exhausted all retries. key={} event={}", key, libraryEvent, ex);
    } else {
        logger.error("Non-retriable error. key={} event={}", key, libraryEvent, ex);
    }
}
```

---

### 11) Error Handling in Callbacks / CompletableFuture

**Async approach — `whenComplete`**

Used when calling `sendLibraryEvent()`. The callback fires after Kafka's internal retries are finished, whether the send succeeded or failed.

```java
// In LibraryEventProducer.sendLibraryEvent()
public CompletableFuture<SendResult<Integer, LibraryEvent>> sendLibraryEvent(LibraryEvent libraryEvent) {
    Integer key = libraryEvent.libraryEventId();
    CompletableFuture<SendResult<Integer, LibraryEvent>> future =
            key == null
                    ? kafkaTemplate.send(topicName, libraryEvent)
                    : kafkaTemplate.send(topicName, key, libraryEvent);

    future.whenComplete((result, ex) -> {
        if (ex != null) {
            // All Kafka retries exhausted — handle at application level
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof RetriableException) {
                // Retriable but retries exhausted — alert or send to DLQ
                logger.error("Retriable error exhausted all retries. key={} event={}", key, libraryEvent, ex);
            } else {
                // Non-retriable — log and escalate immediately
                logger.error("Non-retriable error. key={} event={}", key, libraryEvent, ex);
            }
            return;
        }
        logger.info("Published library event. topic={} partition={} offset={} key={} event={}",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset(),
                key,
                libraryEvent);
    });

    return future;
}
```

---

**Sync approach — `.get()` with `try/catch`**

Used when calling `sendLibraryEventSynchronous()`. Errors are caught at the call site, giving the caller control over how to respond.

```java
// In LibraryEventProducer.sendLibraryEventSynchronous()
public SendResult<Integer, LibraryEvent> sendLibraryEventSynchronous(LibraryEvent libraryEvent)
        throws Exception {
    Integer key = libraryEvent.libraryEventId();
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
        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
        if (cause instanceof RetriableException) {
            // Retriable but retries exhausted — alert or send to DLQ
            logger.error("Retriable error exhausted all retries. key={} event={}", key, libraryEvent, ex);
        } else {
            // Non-retriable — log and escalate immediately
            logger.error("Non-retriable error. key={} event={}", key, libraryEvent, ex);
        }
        throw ex;
    } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw ex;
    }
}
```

**Why it matters**
- Unhandled exceptions in callbacks silently drop errors. Every producer must have explicit error handling.

---

**Bonus — Spring Retry with `@Retryable`**

Spring Retry adds a declarative retry layer on top of the method call — useful when you want to retry the entire `send()` operation at the application level, independent of Kafka's built-in retries.

Add the dependency to `build.gradle`:
```groovy
implementation 'org.springframework.retry:spring-retry'
```

Enable on the main application class:
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
    logger.error("All Spring Retry attempts exhausted. event={}", libraryEvent, ex);
    // send to DLQ or surface an error response
    return CompletableFuture.failedFuture(ex);
}
```

> `@Recover` is called automatically when all `@Retryable` attempts are exhausted. The method signature must match the return type and include the exception as the first parameter.

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

## Suggested Implementation Order

1. `acks` - Start here; this is the foundation of producer reliability.
2. `min.insync.replicas` - Pair with `acks=-1`.
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

- [ ] Set `acks=-1`.
- [ ] Configure `retries`, `retry.backoff.ms`, and `delivery.timeout.ms`.
- [ ] Enable `enable.idempotence=true`.
- [ ] Validate topic/broker replication strategy (`replication.factor=3`, `min.insync.replicas=2`).
- [ ] Confirm `max.in.flight.requests.per.connection` aligns with ordering needs.
- [ ] Implement retriable vs non-retriable error handling.
- [ ] Add callback/synchronous error handling paths.
- [ ] Add reliability tests and failure-injection scenarios.
