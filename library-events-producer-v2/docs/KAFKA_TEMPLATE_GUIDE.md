# Kafka Template Guide

## Overview

`KafkaTemplate` is a Spring Framework class that provides a simple abstraction for sending messages to Apache Kafka topics. It's the primary tool used in Spring Kafka applications to publish messages from your application to Kafka brokers.

In the Library Events Producer, `KafkaTemplate` is used to publish library events to the `library-events` Kafka topic.

## What is KafkaTemplate?

`KafkaTemplate` is a Spring Kafka component that:
- Simplifies Kafka producer configuration and management
- Provides methods to send messages synchronously and asynchronously
- Handles serialization of message payloads
- Supports callbacks for success and error handling
- Manages connection pooling and resource cleanup

### Key Characteristics
- **Thread-safe**: Can be safely used across multiple threads
- **Configured via Spring Boot properties**: Easy configuration through `application.yml`
- **Automatic retry handling**: Built-in retry mechanisms for failed sends
- **Type-safe**: Can be parameterized with generic types for type safety

## How KafkaTemplate Works

### Basic Flow
```
Application Code
    ↓
KafkaTemplate.send(topic, message)
    ↓
Serialization (converts Java object to bytes)
    ↓
Kafka Producer
    ↓
Kafka Broker
    ↓
Topic Partition
```

### Message Sending Process
1. **Message Creation**: Application creates a message object
2. **Serialization**: KafkaTemplate serializes the message to bytes
3. **Producer Metadata**: Kafka producer gathers broker metadata
4. **Batching & Buffering**: Messages are batched for efficiency
5. **Network Send**: Messages are sent to the Kafka broker
6. **Acknowledgment**: Broker acknowledges receipt
7. **Callback Execution**: Success or error callbacks are triggered

## KafkaTemplate in Library Events Producer

### Configuration

In `application.yml`:
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.IntegerSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

### Producer Implementation

```java
@Component
public class LibraryEventProducer {
    
    @Autowired
    private KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;
    
    @Value("${library.events.topic}")
    private String topic;
    
    public void sendLibraryEvent(LibraryEvent event) throws JsonProcessingException {
        // Send message with event ID as key and event as value
        kafkaTemplate.send(topic, event.getLibraryEventId(), event);
    }
}
```

### Key Components

| Component | Purpose |
|-----------|---------|
| `KafkaTemplate<Integer, LibraryEvent>` | Generic type specifies key type (Integer) and value type (LibraryEvent) |
| `bootstrap-servers` | Kafka broker address for initial connection |
| `key-serializer` | Converts Integer keys to bytes |
| `value-serializer` | Converts LibraryEvent objects to JSON bytes |

## Common KafkaTemplate Methods

### 1. Synchronous Send (Blocking)
```java
// Returns a ListenableFuture that blocks until message is sent
SendResult<Integer, LibraryEvent> result = 
    kafkaTemplate.send(topic, event).get(3, TimeUnit.SECONDS);
```

**Pros:**
- Guarantees message delivery before method returns
- Simplest to implement
- Easy error handling

**Cons:**
- Blocks the calling thread
- Lower throughput
- Can cause performance issues under high load

### 2. Asynchronous Send (Non-blocking)
```java
// Returns a ListenableFuture immediately
ListenableFuture<SendResult<Integer, LibraryEvent>> future = 
    kafkaTemplate.send(topic, event);
```

**Pros:**
- Non-blocking
- Higher throughput
- Better performance

**Cons:**
- Must handle success/error callbacks
- More complex error handling

### 3. Send with Callbacks
```java
ListenableFuture<SendResult<Integer, LibraryEvent>> future = 
    kafkaTemplate.send(topic, key, event);

future.addCallback(
    new ListenableFutureCallback<SendResult<Integer, LibraryEvent>>() {
        @Override
        public void onSuccess(SendResult<Integer, LibraryEvent> result) {
            // Handle success
            log.info("Message sent successfully: {}", result.getProducerRecord());
        }
        
        @Override
        public void onFailure(Throwable ex) {
            // Handle failure
            log.error("Failed to send message", ex);
        }
    }
);
```

### 4. Send with Topic, Key, and Value
```java
// Topic: "library-events"
// Key: 1 (libraryEventId)
// Value: LibraryEvent object
kafkaTemplate.send(topic, 1, libraryEvent);
```

## Message Key and Value

### Key (Partition Determinant)
- **Purpose**: Used to determine which partition the message goes to
- **Format**: Integer (libraryEventId in this project)
- **Behavior**: Messages with the same key always go to the same partition
- **Use Case**: Ensures events for the same library item stay ordered

### Value (Actual Message)
- **Purpose**: The actual data being published
- **Format**: LibraryEvent object (serialized to JSON)
- **Content**: Library event details including book information

### Example
```java
// Message key: 123 (libraryEventId)
// Message value: {"libraryEventId": 123, "libraryEventType": "ADD", "book": {...}}
kafkaTemplate.send("library-events", 123, libraryEvent);
```

## Serialization in KafkaTemplate

### What Happens During Serialization

1. **Key Serialization**: `IntegerSerializer` converts Integer → bytes
2. **Value Serialization**: `JsonSerializer` converts LibraryEvent → JSON → bytes

### Serialization Example
```
Input:
LibraryEvent {
    libraryEventId: 1,
    libraryEventType: ADD,
    book: {
        bookId: 10,
        bookName: "Clean Code",
        bookAuthor: "Robert C. Martin"
    }
}

After Serialization (JSON):
{
    "libraryEventId": 1,
    "libraryEventType": "ADD",
    "book": {
        "bookId": 10,
        "bookName": "Clean Code",
        "bookAuthor": "Robert C. Martin"
    }
}

Final (Bytes):
[123, 34, 108, 105, 98, 114, 97, 114, 121, ...]
```

## Error Handling and Retries

### Producer-Level Retries
Configured in Kafka producer properties:
```yaml
spring:
  kafka:
    producer:
      retries: 3
      retry-backoff-ms: 100
```

### Application-Level Exception Handling
```java
try {
    kafkaTemplate.send(topic, key, event).get(3, TimeUnit.SECONDS);
} catch (InterruptedException | ExecutionException | TimeoutException e) {
    log.error("Failed to send message after retries", e);
    // Handle error - return 500 response, log, alert, etc.
}
```

### Callback Error Handling
```java
future.addCallback(
    result -> log.info("Success: {}", result.getRecordMetadata()),
    ex -> {
        log.error("Failed: {}", ex.getMessage());
        // Could retry, update status, alert, etc.
    }
);
```

## Partitioning Strategy

### How KafkaTemplate Determines Partition

1. **If Key is provided**: 
   - Kafka uses the key to determine partition
   - Same key → same partition (preserves order)
   - Formula: `hash(key) % num_partitions`

2. **If Key is null**:
   - Round-robin across partitions
   - No order guarantee

### In Library Events Producer
- **Key**: `libraryEventId`
- **Benefit**: All events for the same library item go to the same partition
- **Guarantees**: Order preserved for events with the same library ID

```
LibraryEventId 1 → Partition 0
LibraryEventId 2 → Partition 0  (if only 1 partition)
LibraryEventId 3 → Partition 0
```

## Performance Considerations

### Batching
KafkaTemplate batches messages to improve throughput:
```yaml
spring:
  kafka:
    producer:
      batch-size: 16384      # Batch size in bytes
      linger-ms: 10          # Wait up to 10ms to batch messages
```

- **Larger batches**: Higher throughput, higher latency
- **Smaller batches**: Lower latency, lower throughput

### Buffering
```yaml
spring:
  kafka:
    producer:
      buffer-memory: 33554432  # Total buffer memory
      compression-type: snappy
```

## Best Practices

### 1. Use Dependency Injection
```java
@Component
public class MyProducer {
    @Autowired
    private KafkaTemplate<Integer, MyEvent> kafkaTemplate;
}
```
✅ Spring manages the bean lifecycle and connection pooling

### 2. Handle Exceptions Appropriately
```java
// ✅ Good: Handle both success and failure
future.addCallback(
    result -> handleSuccess(result),
    ex -> handleFailure(ex)
);

// ❌ Bad: Silently ignore failures
kafkaTemplate.send(topic, message);
```

### 3. Use Type-Safe Generics
```java
// ✅ Good: Type-safe
KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;

// ❌ Bad: Not type-safe
KafkaTemplate kafkaTemplate;
```

### 4. Log Important Events
```java
future.addCallback(
    result -> log.info("Message published: topic={}, partition={}, offset={}",
        result.getRecordMetadata().topic(),
        result.getRecordMetadata().partition(),
        result.getRecordMetadata().offset()),
    ex -> log.error("Failed to publish message", ex)
);
```

### 5. Configure Appropriate Timeouts
```java
try {
    result = kafkaTemplate.send(topic, key, value)
        .get(10, TimeUnit.SECONDS);  // Don't wait forever
} catch (TimeoutException e) {
    log.error("Message send timeout", e);
}
```

## KafkaTemplate vs Low-Level Kafka Producer

### KafkaTemplate (Recommended for Spring Apps)
✅ Spring integration
✅ Automatic configuration
✅ Exception handling
✅ Thread-safe
✅ Cleaner API
✅ Callback support

### Low-Level KafkaProducer
✅ More control
✅ Less overhead
❌ Manual configuration
❌ Manual resource management
❌ More code to write

## Testing KafkaTemplate

### Using Embedded Kafka
```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "library-events")
class LibraryEventsProducerTest {
    
    @Autowired
    private KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;
    
    @Test
    void testSendMessage() throws Exception {
        LibraryEvent event = new LibraryEvent(1, LibraryEventType.ADD, book);
        
        SendResult<Integer, LibraryEvent> result = 
            kafkaTemplate.send("library-events", 1, event).get();
        
        assertEquals("library-events", result.getRecordMetadata().topic());
    }
}
```

### Using MockKafkaTemplate
```java
@SpringBootTest
class LibraryEventsControllerTest {
    
    @MockBean
    private KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;
    
    @Test
    void testControllerWithMockedKafka() {
        // Mock the send behavior
        when(kafkaTemplate.send(anyString(), anyInt(), any(LibraryEvent.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        
        // Test controller logic
    }
}
```

## Common Issues and Solutions

### Issue 1: Serialization Error
```
Error: Cannot serialize object to JSON
```
**Solution**: Ensure your model has getter/setter methods or is annotated with `@Data`

### Issue 2: Message Not Reaching Broker
```
Error: Message silently fails
```
**Solution**: Always add callback or use `.get()` to wait for result

### Issue 3: Out of Memory
```
Error: java.lang.OutOfMemoryError
```
**Solution**: Reduce `buffer-memory` or `batch-size` settings

### Issue 4: Slow Performance
```
Issue: High latency
```
**Solution**: Increase `batch-size` and `linger-ms` for higher throughput

## Summary

| Feature | Description |
|---------|-------------|
| **Purpose** | Simplifies sending messages to Kafka |
| **Type Safety** | Supports generic types for keys and values |
| **Configuration** | Configured via Spring Boot properties |
| **Async Support** | Non-blocking message sending with callbacks |
| **Error Handling** | Built-in retry and exception handling |
| **Partitioning** | Uses message key to determine partition |
| **Serialization** | Automatic serialization of objects to bytes |
| **Thread Safety** | Safe to use across multiple threads |

## Further Reading

- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [KafkaTemplate API Reference](https://docs.spring.io/spring-kafka/docs/current/api/org/springframework/kafka/core/KafkaTemplate.html)

## Related Files in This Project

- `src/main/java/com/learnkafka/producer/LibraryEventProducer.java` - Producer implementation
- `src/main/resources/application.yml` - KafkaTemplate configuration
- `src/test/java/com/learnkafka/controller/LibraryEventsControllerIntegrationTest.java` - Integration tests with embedded Kafka

