# Introduction to Unit and Integration Testing

## Why Testing Matters

Testing is not optional — it is a core engineering discipline that separates professional software from fragile prototypes.

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

### What Is a Unit Test?

A **unit test** validates a single, isolated piece of logic — typically a method or class — without involving external dependencies like databases, message brokers, or HTTP clients.

### Key Characteristics

| Property | Description |
|---|---|
| **Fast** | Runs in milliseconds; no I/O |
| **Isolated** | Dependencies are mocked or stubbed |
| **Deterministic** | Same input always gives same output |
| **Focused** | Tests one thing only |

### Example (Spring Boot / JUnit 5)

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

---

## Integration Testing

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

---

## Unit vs Integration Testing — Side by Side

| Aspect | Unit Test | Integration Test |
|---|---|---|
| **Speed** | Very fast (ms) | Slower (seconds) |
| **Dependencies** | Mocked | Real or embedded |
| **Scope** | Single class/method | Multiple components |
| **Failure diagnosis** | Easy — narrow scope | Harder — more moving parts |
| **Confidence level** | Logic correctness | System correctness |
| **Quantity** | Many | Fewer |

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
