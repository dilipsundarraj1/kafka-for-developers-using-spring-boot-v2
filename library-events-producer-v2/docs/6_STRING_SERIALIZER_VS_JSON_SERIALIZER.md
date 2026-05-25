# StringSerializer vs JacksonJsonSerializer in Kafka

## Overview

When producing messages to Kafka, the **value serializer** determines how your Java objects are converted into bytes before being written to a Kafka topic. The two most common choices are:

| Serializer | Class | Package |
|---|---|---|
| **StringSerializer** | `org.apache.kafka.common.serialization.StringSerializer` | `kafka-clients` (built-in) |
| **JacksonJsonSerializer** | `org.springframework.kafka.support.serializer.JacksonJsonSerializer` | `spring-kafka` |

---

## StringSerializer

### What It Does

Converts a Java `String` into bytes using UTF-8 encoding. It does **not** understand object structure — it simply serializes raw string content.

### Configuration

```yaml
spring:
  kafka:
    producer:
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

### How You Use It

Since `StringSerializer` only handles strings, **you must manually convert your objects to JSON** before sending:

```java
@Component
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent)
            throws JsonProcessingException {
        Integer key = libraryEvent.libraryEventId();
        // Manual serialization: object → JSON string
        String value = objectMapper.writeValueAsString(libraryEvent);
        return kafkaTemplate.send(topicName, key, value);
    }
}
```

### Key Points

- `KafkaTemplate` is typed as `KafkaTemplate<Integer, String>`.
- You are responsible for calling `objectMapper.writeValueAsString(...)`.
- You control exactly which `ObjectMapper` instance (and configuration) is used.
- No extra Jackson dependency issues — you use whichever `ObjectMapper` is already in your app (Jackson 3.x `tools.jackson` in Spring Boot 4).
- If serialization fails, you catch `JsonProcessingException` **before** the message is sent to Kafka.

---

## JacksonJsonSerializer

### What It Does

Automatically converts any Java object into JSON bytes using Jackson's `ObjectMapper` internally. It is provided by **Spring Kafka** (not the Kafka client itself).

### Configuration

```yaml
spring:
  kafka:
    producer:
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
```

### How You Use It

You pass your domain object directly to `KafkaTemplate` — no manual conversion needed:

```java
@Component
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, LibraryEvent> kafkaTemplate;

    public CompletableFuture<SendResult<Integer, LibraryEvent>> sendLibraryEvent(LibraryEvent libraryEvent) {
        Integer key = libraryEvent.libraryEventId();
        // Automatic serialization: JacksonJsonSerializer handles object -> bytes
        return kafkaTemplate.send(topicName, key, libraryEvent);
    }
}
```

### Key Points

- `KafkaTemplate` is typed as `KafkaTemplate<Integer, LibraryEvent>` (strongly typed).
- Serialization is handled transparently by the Kafka producer.
- `JacksonJsonSerializer` performs the object-to-JSON conversion for Kafka values.

### ⚠️ Spring Boot 4.x Compatibility Warning

In **Spring Boot 4.x**, prefer Spring Kafka's `JacksonJsonSerializer` for JSON value serialization in producer configs.

This means you must explicitly add the classic Jackson 2.x `jackson-databind` to your build:

```groovy
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-kafka'
    // JSON serialization support required by producer examples.
    // Version must align with jackson-annotations:2.20 constrained by the Jackson 3.x BOM.
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.20.2'
}
```

Without this, you will see errors like:
```
java.lang.ClassNotFoundException: com.fasterxml.jackson.core.type.TypeReference
```
or:
```
java.lang.NoClassDefFoundError: com/fasterxml/jackson/annotation/JsonSerializeAs
```

> **Version alignment is critical.** The Jackson 3.x BOM (`tools.jackson:jackson-bom:3.0.4`) constrains `jackson-annotations` to `2.20`. Your `jackson-databind` version must be compatible with that — use `2.20.x`, **not** `2.21.x`, to avoid `NoClassDefFoundError` from mismatched annotations.

---

## Side-by-Side Comparison

| Aspect | StringSerializer | JacksonJsonSerializer |
|---|---|---|
| **Serialization** | Manual (`objectMapper.writeValueAsString()`) | Automatic (handled internally) |
| **KafkaTemplate type** | `KafkaTemplate<K, String>` | `KafkaTemplate<K, YourDomainObject>` |
| **ObjectMapper control** | Full control — you inject your own | Uses its own internal instance |
| **Type safety** | Weak — everything is a `String` | Strong — template is typed to your domain |
| **Extra dependencies** | None (uses app's existing Jackson) | Requires classic Jackson 2.x on classpath (Spring Boot 4) |
| **Error handling** | `JsonProcessingException` before send | Serialization errors surface as `KafkaException` during send |
| **Wire format** | JSON string → UTF-8 bytes | Object → JSON bytes (via Jackson) |
| **Kafka message content** | Identical | Identical |
| **Consumer compatibility** | Any consumer that reads UTF-8 strings | Works with `JsonDeserializer` or `StringDeserializer` |

> **Important:** The actual bytes written to Kafka are identical in both cases — the message on the topic is JSON either way. The difference is only about **where and when** the serialization happens.

---

## Which Should You Choose?

### Choose `StringSerializer` When:

1. **Spring Boot 4.x with Jackson 3.x** — avoids the dual-Jackson dependency problem entirely.
2. **You need full control** over the `ObjectMapper` configuration (custom modules, naming strategies, etc.).
3. **You want explicit error handling** — catch `JsonProcessingException` at the call site.
4. **You produce messages of varying types** — a single `KafkaTemplate<String, String>` can send any JSON payload.
5. **You want simpler dependency management** — no need to manage Jackson 2.x/3.x version alignment.

### Choose `JacksonJsonSerializer` When:

1. **You want cleaner producer code** — no manual serialization boilerplate.
2. **You prefer strong typing** — `KafkaTemplate<Integer, LibraryEvent>` makes intent clear.
3. **You want Spring Kafka to manage serialization** consistently across producers.
4. **You're on Spring Boot 3.x or earlier** — Jackson 2.x is the default, so no compatibility issues.

---

## This Project's Current Approach

This project uses **`JacksonJsonSerializer`** as configured in `application.yml`:

```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.IntegerSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
```

The producer sends `LibraryEvent` objects directly without manual conversion:

```java
// From LibraryEventProducer.java — no objectMapper.writeValueAsString() needed
kafkaTemplate.send(topicName, key, libraryEvent);
```

To support this on Spring Boot 4.x, the classic Jackson 2.x dependency is explicitly added in `build.gradle`:

```groovy
implementation 'com.fasterxml.jackson.core:jackson-databind:2.20.2'
```

### Alternative: Switching to StringSerializer

If you wanted to eliminate the Jackson 2.x dependency, you could switch to `StringSerializer`:

1. **Change `application.yml`:**
    ```yaml
    spring:
      kafka:
        producer:
          value-serializer: org.apache.kafka.common.serialization.StringSerializer
    ```

2. **Update `LibraryEventProducer`:**
    ```java
    @Component
    public class LibraryEventProducer {
        private final KafkaTemplate<Integer, String> kafkaTemplate;
        private final ObjectMapper objectMapper; // tools.jackson.databind.ObjectMapper (Jackson 3.x)

        public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent)
                throws Exception {
            Integer key = libraryEvent.libraryEventId();
            String value = objectMapper.writeValueAsString(libraryEvent);
            return kafkaTemplate.send(topicName, key, value);
        }
    }
    ```

3. **Remove from `build.gradle`:**
    ```groovy
    // Remove this line:
    // implementation 'com.fasterxml.jackson.core:jackson-databind:2.20.2'
    ```

