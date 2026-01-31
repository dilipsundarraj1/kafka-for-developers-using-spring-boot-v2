# Library Events Producer - Project Context

## Project Overview
Spring Boot 4.0.2 Kafka producer application using Java 25 and Gradle 8.13.

## Test Structure

### Directory Layout
```
src/test/java/
├── unit/com/learnkafka/          # Fast unit tests (mocked dependencies)
│   ├── controller/LibraryEventControllerUnitTest.java
│   ├── producer/LibraryEventProducerUnitTest.java
│   └── util/TestUtil.java
└── intg/com/learnkafka/          # Integration tests (full app context)
    └── controller/
        ├── LibraryEventsControllerIntegrationTest.java
        └── LibraryEventsControllerIntegrationTestApproach2.java
```

### Test Frameworks
- JUnit 5 (Jupiter)
- Mockito (`@Mock`, `@Spy`, `@InjectMocks`, `@MockitoBean`)
- Spring Boot Test (`@WebMvcTest`, `@SpringBootTest`, `MockMvc`, `TestRestTemplate`)
- Spring Boot Docker Compose (auto-starts Kafka from `compose.yaml`)
- Spring Kafka Test (`KafkaTestUtils`)

### Unit Test Patterns
- Controller: `@WebMvcTest` + `MockMvc` + `@MockitoBean`
- Producer: `@ExtendWith(MockitoExtension.class)` + `ReflectionTestUtils.setField()` for `@Value` injection

### Integration Test Patterns
- **Approach 1 (Docker Compose Kafka):** `@SpringBootTest` + real Kafka from `compose.yaml` + consumer verification
- **Approach 2 (Mocked Kafka):** Full Spring Boot + mocked `KafkaTemplate` + `Mockito.verify()`

### Docker Compose Integration Testing

The project uses Spring Boot Docker Compose support for integration tests:

1. `compose.yaml` defines KRaft-mode Kafka (no ZooKeeper)
2. Spring Boot auto-starts containers when tests run
3. Tests create Kafka consumer to verify messages
4. Test config is separated from main app config using profiles

**Key files:**
- `compose.yaml` - Kafka container config
- `src/test/resources/application-test.yml` - Test-specific Docker Compose settings
- `src/main/resources/application.yml` - Main app config (unchanged for tests)

**Integration test pattern:**
```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("test")
public class LibraryEventsControllerIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS,
            ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs).createConsumer();
        consumer.subscribe(List.of("library-events"));
    }
}
```

## Test Coverage Gaps (To Implement)

| Component | What to Add | Priority |
|-----------|-------------|----------|
| `LibraryEventProducer.sendLibraryEvent()` | Unit test for first approach method | High |
| `LibraryEventProducer.sendLibraryEventSynchronous()` | Unit test for sync method | High |
| Error handlers (`handleFailure`, `handleSuccess`) | Failure scenario tests | High |
| `LibraryEventControllerAdvice` | Direct unit test (currently only indirect) | Medium |
| Kafka serialization errors | JSON failure tests | Medium |
| Timeout scenarios | Synchronous timeout test | Medium |
| `AutoCreateConfig` | Topic creation verification | Low |

### Recommended Tests to Write

**In `LibraryEventProducerUnitTest.java`:**
```java
@Test void sendLibraryEvent_success()
@Test void sendLibraryEventSynchronous_success()
@Test void sendLibraryEvent_failure()  // Test handleFailure is called
@Test void sendLibraryEventSynchronous_timeout()
```

**New file `LibraryEventControllerAdviceUnitTest.java`:**
- Test multiple field errors formatting
- Test sorting of error messages

**Failure scenario pattern:**
```java
@Test
void sendLibraryEvent_Approach2_failure() {
    var future = CompletableFuture.<SendResult<Integer, String>>failedFuture(
        new RuntimeException("Kafka unavailable"));
    when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
    // Assert exception is handled
}
```
