# Introduction to Unit and Integration Testing

## Table of Contents

1. [Why Testing Matters](#why-testing-matters)
   - [The Real Cost of Not Testing](#the-real-cost-of-not-testing)
   - [The Cost of Bugs Grows Over Time](#the-cost-of-bugs-grows-over-time)
   - [Testing in a Kafka-Based System](#testing-in-a-kafka-based-system)
   - [What Teams With Good Test Coverage Experience](#what-teams-with-good-test-coverage-experience)
2. [Unit Testing](#unit-testing)
   - [What Is a Unit Test?](#what-is-a-unit-test)
   - [Key Characteristics](#key-characteristics)
   - [Example](#example-spring-boot--junit-6)
   - [What to Mock](#what-to-mock)
   - [Visual: How Unit Testing Works](#visual-how-unit-testing-works)
3. [Integration Testing](#integration-testing)
   - [What Is an Integration Test?](#what-is-an-integration-test)
   - [Key Characteristics](#key-characteristics-1)
   - [Example](#example-spring-boot--embedded-kafka)
   - [Visual: How Integration Testing Works](#visual-how-integration-testing-works)
4. [Unit vs Integration Testing — Side by Side](#unit-vs-integration-testing--side-by-side)
   - [Visual: What Gets Mocked vs What Is Real](#visual-what-gets-mocked-vs-what-is-real)
5. [Request Flow in a Kafka Application](#request-flow-in-a-kafka-application)
6. [The Testing Pyramid](#the-testing-pyramid)
   - [Test Execution Speed Comparison](#test-execution-speed-comparison)
7. [Best Practices](#best-practices)
8. [Summary](#summary)

---

## Why Testing Matters

Testing is not optional — it is a core engineering discipline that separates professional software from fragile prototypes.

### The Real Cost of Not Testing

Consider a real-world scenario: your team is building a **library events service** that publishes messages to Kafka whenever a book is added or updated. The system works fine in development. You ship it.

Three weeks later, a colleague refactors the `LibraryEventService` to add a new field to the Kafka message. They don't realize that the consumer downstream depends on the exact message structure. There are no tests. The change looks clean. It gets merged.

In production, the consumer silently starts failing to deserialize messages. Books go unindexed. No alerts fire. Users report stale search results days later.

**The bug cost:**
- 2 days to diagnose (no test pointed at the contract)
- 1 day to hotfix and redeploy
- Data inconsistency requiring a manual reconciliation job
- Lost user trust

A single integration test covering the producer-consumer message contract would have caught this in seconds during the CI pipeline.

---

### The Cost of Bugs Grows Over Time

```
Cost to fix a bug:

  Requirements  │█  $1
  Development   │███  $10
  Testing (QA)  │█████████  $100
  Production    │████████████████████████  $1,000+

                └──────────────────────────────▶ time
```

The later a bug is found, the more it costs — in time, money, and reputation.

---

### Testing in a Kafka-Based System

Kafka introduces asynchronous, distributed communication. This makes bugs especially hard to trace without tests:

- A producer sends a malformed message → the consumer crashes silently
- A topic name is misconfigured → messages are published to the wrong topic
- A serializer changes → consumers cannot deserialize old messages

Tests give you a safety net at every layer:

| Layer | Risk Without Tests | Test Type |
|---|---|---|
| Producer logic | Wrong message structure published | Unit Test |
| REST → Kafka flow | Message never reaches broker | Integration Test |
| Consumer logic | Records processed incorrectly | Unit Test |
| Consumer → DB flow | Data silently not persisted | Integration Test |

---

### What Teams With Good Test Coverage Experience

- **Faster onboarding** — new developers run the tests, understand the system behavior, and make changes confidently within days
- **Fearless refactoring** — upgrade Spring Boot, swap a library, restructure a class — tests tell you immediately if something broke
- **Shorter review cycles** — PRs with passing tests require less manual inspection
- **Reliable CI/CD** — automated pipelines catch regressions before they reach production

**Without tests, you:**
- Cannot confidently refactor or upgrade dependencies
- Discover bugs in production, where the cost is highest
- Slow down over time as fear of breaking things accumulates
- Rely on manual QA, which doesn't scale

**With tests, you:**
- Catch bugs early, when they're cheapest to fix
- Refactor with confidence
- Document behavior through executable specifications
- Enable continuous integration and deployment (CI/CD)

---

## Unit Testing

### Visual: How Unit Testing Works

```
┌─────────────────────────────────────────────────────────────┐
│                        UNIT TEST                            │
│                                                             │
│   ┌──────────┐      ┌──────────────────┐                   │
│   │  Input   │─────▶│   Your Method /  │─────▶  Assert     │
│   │  (given) │      │   Class (SUT)    │        Result      │
│   └──────────┘      └──────────────────┘                   │
│                              │                              │
│                    ┌─────────▼─────────┐                   │
│                    │   Dependencies    │                    │
│                    │  🚫 Kafka         │  ← All MOCKED      │
│                    │  🚫 Database      │                    │
│                    │  🚫 REST API      │                    │
│                    └───────────────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

### What Is a Unit Test?

A **unit test** validates a single, isolated piece of logic — typically a method or class — without involving external dependencies like databases, message brokers, or HTTP clients.

### Key Characteristics

| Property | Description |
|---|---|
| **Fast** | Runs in milliseconds; no I/O |
| **Isolated** | Dependencies are mocked or stubbed |
| **Deterministic** | Same input always gives same output |
| **Focused** | Tests one thing only |

### Example (Spring Boot / JUnit 6)

```java
@ExtendWith(MockitoExtension.class)
class LibraryEventServiceTest {

    @InjectMocks
    private LibraryEventService libraryEventService;

    @Mock
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Test
    void publishLibraryEvent_success() throws Exception {
        // given
        LibraryEvent event = TestUtil.libraryEventRecord();
        when(kafkaTemplate.send(any(ProducerRecord.class)))
            .thenReturn(mock(CompletableFuture.class));

        // when
        libraryEventService.sendLibraryEvent(event);

        // then
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }
}
```

### What to Mock

- External services (Kafka, databases, REST APIs)
- System clocks, random generators
- File system or network I/O

---

## Integration Testing

### Visual: How Integration Testing Works

```
┌─────────────────────────────────────────────────────────────┐
│                    INTEGRATION TEST                          │
│                                                             │
│  ┌──────────┐   ┌───────────┐   ┌──────────┐              │
│  │  HTTP    │──▶│Controller │──▶│ Service  │              │
│  │  Request │   └───────────┘   └────┬─────┘              │
│  └──────────┘                        │                     │
│                               ┌──────▼──────┐             │
│                               │  Embedded   │             │
│                               │   Kafka     │  ← REAL      │
│                               │  (in-memory)│             │
│                               └─────────────┘             │
│                                                             │
│   Full Spring context loaded ✅   Real I/O ✅              │
└─────────────────────────────────────────────────────────────┘
```

### What Is an Integration Test?

An **integration test** validates that multiple components work correctly **together** — including real (or embedded) infrastructure like Kafka brokers, databases, or web servers.

### Key Characteristics

| Property | Description |
|---|---|
| **Realistic** | Uses real or embedded infrastructure |
| **Slower** | Involves I/O and startup time |
| **Broader scope** | Covers component interactions |
| **Fewer in count** | Complement, not replace, unit tests |

### Example (Spring Boot + Embedded Kafka)

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {
    "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class LibraryEventsControllerIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void postLibraryEvent_success() {
        // given
        LibraryEvent event = TestUtil.libraryEventRecord();
        HttpEntity<LibraryEvent> request = new HttpEntity<>(event, headers());

        // when
        ResponseEntity<LibraryEvent> response = restTemplate
            .exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);

        // then
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
    }
}
```

---

## Unit vs Integration Testing — Side by Side

### Visual: What Gets Mocked vs What Is Real

```
                UNIT TEST              INTEGRATION TEST
              ┌───────────┐           ┌───────────┐
  Controller  │   REAL    │           │   REAL    │
              ├───────────┤           ├───────────┤
  Service     │   REAL    │           │   REAL    │
              ├───────────┤           ├───────────┤
  Kafka       │  MOCKED   │           │ EMBEDDED  │
              ├───────────┤           ├───────────┤
  Database    │  MOCKED   │           │ EMBEDDED  │
              ├───────────┤           ├───────────┤
  HTTP Client │  MOCKED   │           │ TEST REST │
              └───────────┘           └───────────┘
```

| Aspect | Unit Test | Integration Test |
|---|---|---|
| **Speed** | Very fast (ms) | Slower (seconds) |
| **Dependencies** | Mocked | Real or embedded |
| **Scope** | Single class/method | Multiple components |
| **Failure diagnosis** | Easy — narrow scope | Harder — more moving parts |
| **Confidence level** | Logic correctness | System correctness |
| **Quantity** | Many | Fewer |

---

## Request Flow in a Kafka Application

```
Unit Tests cover each box individually:

  ┌────────────┐     ┌────────────┐     ┌────────────┐
  │ Controller │     │  Service   │     │   Kafka    │
  │            │────▶│            │────▶│  Producer  │
  │  (REST)    │     │  (Logic)   │     │            │
  └────────────┘     └────────────┘     └────────────┘
       ↑                  ↑                   ↑
  [Unit Test]        [Unit Test]         [Unit Test]
  Mock Service       Mock Kafka          Mock Template


Integration Tests cover the entire flow end-to-end:

  ┌────────────┐     ┌────────────┐     ┌──────────────┐
  │ Controller │────▶│  Service   │────▶│ EmbeddedKafka│
  │  (REST)    │     │  (Logic)   │     │   (real I/O) │
  └────────────┘     └────────────┘     └──────────────┘
  ◀─────────────────────────────────────────────────────▶
                  [Integration Test]
```

---

## The Testing Pyramid

```
                        ▲
                       /|\
                      / | \
                     /  |  \
                    / E2E   \
                   /  Tests  \          2–5 tests
                  /___________\         Slow ⏱⏱⏱
                 /             \
                / Integration   \
               /    Tests        \     10–20 tests
              /___________________\    Medium ⏱⏱
             /                     \
            /      Unit Tests        \
           /                          \ 100+ tests
          /____________________________\ Fast ⏱
```

### Test Execution Speed Comparison

```
Unit Test         ██ 5ms
Integration Test  ████████████████████ 2,000ms
E2E Test          ████████████████████████████████████ 10,000ms

                  └─────────────────────────────────────▶ time
```

---

## Best Practices

1. **Follow AAA** — Arrange, Act, Assert
2. **One assertion per test** — keep tests focused
3. **Name tests clearly** — `methodName_scenario_expectedResult`
4. **Don't test frameworks** — test your code, not Spring or Kafka internals
5. **Run tests in CI** — tests that don't run automatically don't get fixed

---

## Summary

| | Unit Test | Integration Test |
|---|---|---|
| **Goal** | Validate logic in isolation | Validate component collaboration |
| **Tools** | JUnit, Mockito | Spring Boot Test, EmbeddedKafka |
| **When to write** | Always, for every business method | For critical integration points |

> Testing is not about proving code works — it's about **building confidence** that it continues to work as the system evolves.
