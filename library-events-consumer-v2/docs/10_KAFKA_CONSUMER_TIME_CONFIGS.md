# Kafka Consumer Time-Related Configurations

## Overview

Kafka consumers depend on several time-based settings that control how long the consumer
waits, how often it checks in with the broker, and how aggressively it retries on failure.
This document covers each config independently first, then shows how they all connect.

---

## Table of Contents

- [1. Broker-Side Timing (Consumer ↔ Coordinator)](#1-broker-side-timing-consumer--coordinator)
    - [heartbeat.interval.ms](#heartbeatintervalms)
    - [session.timeout.ms](#sessiontimeoutms)
    - [max.poll.interval.ms](#maxpollintervalms)
- [2. Fetch Timing (Consumer ↔ Broker)](#2-fetch-timing-consumer--broker)
    - [fetch.min.bytes](#fetchminbytes)
    - [fetch.max.wait.ms](#fetchmaxwaitms)
    - [fetch.max.bytes](#fetchmaxbytes)
    - [max.poll.records](#maxpollrecords)
- [3. Offset Commit Timing](#3-offset-commit-timing)
    - [auto.commit.interval.ms](#autocommitintervalms)
- [4. Connection Timing](#4-connection-timing)
    - [request.timeout.ms](#requesttimeoutms)
    - [connections.max.idle.ms](#connectionsmaxidlems)
    - [reconnect.backoff.ms](#reconnectbackoffms)
    - [reconnect.backoff.max.ms](#reconnectbackoffmaxms)
- [5. How They Relate to Each Other](#5-how-they-relate-to-each-other)
- [6. Current Project Settings](#6-current-project-settings)
- [7. Quick Reference Table](#7-quick-reference-table)

---

## 1. Broker-Side Timing (Consumer ↔ Coordinator)

> **What is the Coordinator?**
> The "Coordinator" here refers to the **group coordinator** — a specific broker in the
> Kafka cluster elected to manage a consumer group. It receives heartbeats, tracks session
> timeouts, and triggers rebalances when a consumer joins, leaves, or is declared dead. It
> also tracks committed offsets for the group via the internal `__consumer_offsets` topic.
> Every consumer group has exactly one broker acting as its group coordinator at any given
> time, determined by which partition of `__consumer_offsets` the group maps to.

These three settings all relate to the same question: **how does the group coordinator
know if a consumer is still alive?** They each answer a different part of that question
and are covered independently below before being connected in
[Section 5](#5-how-they-relate-to-each-other).

---

### `heartbeat.interval.ms`

| | |
|---|---|
| **Default** | `3000` ms (3 s) |
| **Where set** | `spring.kafka.consumer.properties.heartbeat.interval.ms` |

A heartbeat is a small "I'm alive" signal the consumer sends to the **group coordinator**
(a designated broker). This config controls how frequently that signal is sent.

Heartbeats serve one purpose only: **keeping the consumer's session alive**. They do not
fetch records, commit offsets, or carry any payload.

```yaml
spring:
  kafka:
    consumer:
      properties:
        heartbeat.interval.ms: 3000
```

#### How heartbeats are sent in Spring Kafka

Spring Kafka itself does **not** send heartbeats. It delegates entirely to the underlying
`kafka-clients` library (`KafkaConsumer`), which uses a dedicated **`HeartbeatThread`**
inside `ConsumerCoordinator`. This thread runs on its own schedule, completely
independent of your listener code.

```
┌─────────────────────────────────────────────────────────────────────┐
│  Spring Kafka ConcurrentMessageListenerContainer                    │
│                                                                     │
│  ┌─────────────────────────────────┐                                │
│  │  Listener Thread (your code)    │                                │
│  │  poll() → process records       │                                │
│  └─────────────────────────────────┘                                │
│                                                                     │
│  ┌─────────────────────────────────┐                                │
│  │  HeartbeatThread (Kafka client) │                                │
│  │  sends HeartbeatRequest to the  │  ← fires every                 │
│  │  group coordinator broker       │    heartbeat.interval.ms       │
│  └─────────────────────────────────┘                                │
└─────────────────────────────────────────────────────────────────────┘
```

The `HeartbeatThread` does not wait for `poll()` to be called — it ticks on its own
regardless of what the listener thread is doing.

---

### `session.timeout.ms`

| | |
|---|---|
| **Default** | `45000` ms (45 s) |
| **Where set** | `spring.kafka.consumer.properties.session.timeout.ms` |

This is the broker's patience window. It defines the maximum time the group coordinator
will wait **without receiving a heartbeat** before it concludes the consumer is dead.

When the session expires the coordinator:
1. Removes the consumer from the group.
2. Triggers a **rebalance** to redistribute its partitions to other consumers.

```yaml
spring:
  kafka:
    consumer:
      properties:
        session.timeout.ms: 45000
```

> Must be within the broker's `group.min.session.timeout.ms` /
> `group.max.session.timeout.ms` bounds (typically 6 s – 1800 s).

---

### `max.poll.interval.ms`

| | |
|---|---|
| **Default** | `300000` ms (5 min) |
| **Where set** | `spring.kafka.consumer.properties.max.poll.interval.ms` |

This is the maximum time allowed between two consecutive `poll()` calls. If the listener
takes too long to process a batch and does not call `poll()` again within this window,
the broker considers the consumer **stuck** and triggers a rebalance — even if heartbeats
are still arriving normally.

```yaml
spring:
  kafka:
    consumer:
      properties:
        max.poll.interval.ms: 300000   # increase if processing is slow
```

> **Most common cause of unexpected rebalances.** If your listener does heavy DB work or
> calls slow external services, lower `max.poll.records` so each batch finishes faster,
> or increase this value to give more processing time.

---

## 2. Fetch Timing (Consumer ↔ Broker)

These settings control how the consumer batches records from the broker and how long it
waits when the topic is sparse.

### `fetch.min.bytes`

| | |
|---|---|
| **Default** | `1` byte |
| **Where set** | `spring.kafka.consumer.properties.fetch.min.bytes` |

The minimum amount of data the broker must have available before responding to a fetch
request. Setting this higher reduces the number of fetch requests on low-traffic topics
at the cost of slightly higher latency.

```yaml
spring:
  kafka:
    consumer:
      properties:
        fetch.min.bytes: 1
```

---

### `fetch.max.wait.ms`

| | |
|---|---|
| **Default** | `500` ms |
| **Where set** | `spring.kafka.consumer.properties.fetch.max.wait.ms` |

How long the broker waits for `fetch.min.bytes` to accumulate before returning a
response. The broker responds after **either** the data threshold or this timeout is
reached, whichever comes first.

```yaml
spring:
  kafka:
    consumer:
      properties:
        fetch.max.wait.ms: 500
```

---

### `fetch.max.bytes`

| | |
|---|---|
| **Default** | `52428800` bytes (50 MB) |
| **Where set** | `spring.kafka.consumer.properties.fetch.max.bytes` |

The maximum total bytes the broker returns across all partitions in a single fetch
response. Useful for capping memory use on high-throughput consumers.

---

### `max.poll.records`

| | |
|---|---|
| **Default** | `500` |
| **Where set** | `spring.kafka.consumer.max-poll-records` |

The maximum number of records returned in a single `poll()`. This directly controls how
much work lands in a single listener invocation.

```yaml
spring:
  kafka:
    consumer:
      max-poll-records: 100   # reduce if processing per record is expensive
```

---

> **Summary**
> When the consumer calls `poll()`, it sends a fetch request to the broker. The broker
> will not respond until either `fetch.min.bytes` of data is available **or**
> `fetch.max.wait.ms` has elapsed — whichever comes first. The response is capped at
> `fetch.max.bytes` total. Once the records arrive, `poll()` returns at most
> `max.poll.records` of them to the listener.
>
> Together these four settings form the **fetch pipeline**:
>
> ```
> poll() called
>   │
>   ├─ fetch request sent to broker
>   │
>   │  broker waits until:
>   │    data ≥ fetch.min.bytes   ← accumulate enough data (throughput)
>   │    OR
>   │    fetch.max.wait.ms elapsed ← don't wait forever (latency)
>   │
>   ├─ broker responds (capped at fetch.max.bytes)
>   │
>   └─ poll() returns up to max.poll.records to the listener
> ```
>
> The key trade-off: raising `fetch.min.bytes` and `fetch.max.wait.ms` improves
> throughput by batching more records per fetch at the cost of higher latency.
> Lowering them gives lower latency but more frequent round-trips to the broker.

---

## 3. Offset Commit Timing

### `auto.commit.interval.ms`

| | |
|---|---|
| **Default** | `5000` ms (5 s) |
| **Where set** | `spring.kafka.consumer.properties.auto.commit.interval.ms` |

How often offsets are automatically committed to the broker when
`enable.auto.commit=true`. Only relevant when auto-commit is enabled.

> **This project uses `AckMode.BATCH`** (default, set explicitly in `LibraryEventsConsumerConfig`),
> so auto-commit is effectively disabled. Offsets are committed automatically by Spring Kafka
> after all records from a single `poll()` batch have been processed. This gives reliable
> at-least-once delivery without requiring explicit `Acknowledgment.acknowledge()` calls.

---

## 4. Connection Timing

### `request.timeout.ms`

| | |
|---|---|
| **Default** | `30000` ms (30 s) |
| **Where set** | `spring.kafka.consumer.properties.request.timeout.ms` |

How long the client waits for a response from the broker before considering the request
failed. Covers fetch requests, metadata requests, and offset commits.

---

### `connections.max.idle.ms`

| | |
|---|---|
| **Default** | `540000` ms (9 min) |
| **Where set** | `spring.kafka.consumer.properties.connections.max.idle.ms` |

Idle TCP connections to brokers are closed after this duration. Lower this if you want
connections torn down faster; raise it on stable, high-throughput consumers to avoid
reconnect overhead.

---

### `reconnect.backoff.ms`

| | |
|---|---|
| **Default** | `50` ms |
| **Where set** | `spring.kafka.consumer.properties.reconnect.backoff.ms` |

Initial backoff time before attempting to reconnect to a broker after a connection
failure.

---

### `reconnect.backoff.max.ms`

| | |
|---|---|
| **Default** | `1000` ms (1 s) |
| **Where set** | `spring.kafka.consumer.properties.reconnect.backoff.max.ms` |

Maximum backoff for reconnection attempts. Reconnect delay doubles each attempt up to
this ceiling.

---

## 5. How They Relate to Each Other

Now that each config is clear individually, here is how they connect during a normal
poll cycle.

### The two-thread model

There are always two independent threads active inside the Kafka consumer:

```
  LISTENER THREAD                        HEARTBEAT THREAD
  ───────────────                        ────────────────
  poll()                                 (runs on its own tick)
    │                                           │
    ├─ sends fetch request to broker            ├─ every heartbeat.interval.ms:
    │                                           │   sends HeartbeatRequest
    ├─ broker responds (fetch.max.wait.ms)      │   to group coordinator
    │                                           │
    ├─ up to max.poll.records returned          │
    │                                           │
    ├─ listener processes records               │   broker expects heartbeat
    │   (must complete within                   │   within session.timeout.ms
    │    max.poll.interval.ms)                  │
    │                                           │
  poll() again ...                             (keeps ticking regardless)
```

### What each timer guards

| Timer | Owned by | Guards against |
|---|---|---|
| `heartbeat.interval.ms` | HeartbeatThread | Controls how often the "I'm alive" signal is sent |
| `session.timeout.ms` | Group coordinator (broker) | Detects **dead consumers** — process crash, network loss |
| `max.poll.interval.ms` | Group coordinator (broker) | Detects **stuck consumers** — alive but processing too slowly |

### The critical insight

Because heartbeats are sent by a **separate thread**, a consumer can be simultaneously:
- Sending heartbeats ✓ → `session.timeout.ms` is satisfied
- Not calling `poll()` ✗ → `max.poll.interval.ms` is violated → **rebalance fires anyway**

This is the most common source of unexpected rebalances in Spring Kafka. The consumer
looks alive to itself (heartbeats are flowing), but the broker kicks it out because
`poll()` was not called in time.

### The constraint chain

```
heartbeat.interval.ms  <  session.timeout.ms  <  max.poll.interval.ms
        3 s                      45 s                    300 s
```

- `heartbeat.interval.ms` must be less than `session.timeout.ms` so that multiple
  heartbeats are sent within a single session window (typically set to 1/3 of
  `session.timeout.ms`).
- `session.timeout.ms` must be less than `max.poll.interval.ms` so that a slow-but-alive
  consumer is not killed by the session timer before the poll timer catches it.

Violating this chain causes either premature session expiry or the broker not detecting
a truly dead consumer fast enough.

### Practical implication for this project

`AckMode.BATCH` is used in `LibraryEventsConsumerConfig`. The full sequence per batch is:

1. Container calls `poll()` — bounded by `max.poll.interval.ms`.
2. Up to `max.poll.records` records are returned — after fetching, broker waited at most `fetch.max.wait.ms`.
3. `LibraryEventsConsumer.onMessage()` processes each record.
4. On failure: `DefaultErrorHandler` retries with `FixedBackOff` (3× at 1 s intervals).
5. On exhaustion: recoverer writes to `failure_record` table or DLT.
6. After the batch completes, Spring Kafka commits offsets automatically (BATCH mode).
7. Throughout all of this, `HeartbeatThread` is ticking every `heartbeat.interval.ms`.

If step 3–5 takes longer than `max.poll.interval.ms` in total, a rebalance fires. The
lever to fix this is **`max.poll.records`** (fewer records per batch = faster finish),
not `session.timeout.ms`.

---

## 6. Current Project Settings

| Config | Value | Source |
|---|---|---|
| `session.timeout.ms` | `45000` (default) | Kafka default |
| `heartbeat.interval.ms` | `3000` (default) | Kafka default |
| `max.poll.interval.ms` | `300000` (default) | Kafka default |
| `auto-offset-reset` | `latest` | `application.yml` |
| `enable.auto.commit` | `false` (implicit) | `AckMode.BATCH` in config |

---

## 7. Quick Reference Table

| Config | Layer | Default | Tune When |
|---|---|---|---|
| `heartbeat.interval.ms` | Kafka client | 3 s | Always keep at ~1/3 of `session.timeout.ms` |
| `session.timeout.ms` | Broker/Consumer | 45 s | Consumer runs on flaky network |
| `max.poll.interval.ms` | Broker/Consumer | 300 s | Processing per batch is slow |
| `max.poll.records` | Consumer | 500 | Need to reduce per-batch processing time |
| `fetch.min.bytes` | Consumer/Broker | 1 B | Want fewer fetch round-trips on low traffic |
| `fetch.max.wait.ms` | Consumer/Broker | 500 ms | Want lower latency on sparse topics |
| `auto.commit.interval.ms` | Consumer | 5 s | Only relevant when auto-commit is on |
| `request.timeout.ms` | Client/Broker | 30 s | Slow brokers or high-latency networks |
| `reconnect.backoff.ms` | Client | 50 ms | Broker flap recovery aggressiveness |
| `reconnect.backoff.max.ms` | Client | 1 s | Cap for reconnect backoff growth |
