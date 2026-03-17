# StringDeserializer vs JsonDeserializer in Spring Kafka

## Overview

When consuming messages from Kafka, you must choose a **value deserializer** that converts raw bytes back into a usable Java object. The two most common choices are:

| | `StringDeserializer` | `JsonDeserializer` |
|---|---|---|
| **Class** | `org.apache.kafka.common.serialization.StringDeserializer` | `org.springframework.kafka.support.serializer.JsonDeserializer` |
| **Output type** | `String` | Typed Java object (e.g., `LibraryEventDto`) |
| **Deserialization** | Bytes → `String` (no JSON parsing) | Bytes → JSON → Java object |
| **Type safety** | None — you get a raw `String` | Full — you get a strongly-typed object |
| **Manual parsing needed?** | Yes — you must call `ObjectMapper.readValue()` yourself | No — Spring Kafka handles it |
| **Kafka type headers** | Ignored | Used to resolve the target class |

---

## StringDeserializer

```yaml
spring.kafka.consumer.value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

The `StringDeserializer` simply converts the raw bytes from Kafka into a Java `String`. It does **no JSON parsing**. Your listener receives a `ConsumerRecord<K, String>`, and you are responsible for converting that `String` into a domain object.

### Consumer Example

```java
@KafkaListener(topics = "library-events")
public void onMessage(ConsumerRecord<Integer, String> record) {
    // record.value() is a raw JSON string like:
    // {"libraryEventId":1,"eventType":"ADD","book":{"bookId":101,...}}
    LibraryEventDto dto = objectMapper.readValue(record.value(), LibraryEventDto.class);
}
```

### When to use
- You want full control over deserialization.
- You need to inspect or log the raw JSON before parsing.
- The producer and consumer have **different class structures** and you want to handle mapping manually.
- You're consuming from a topic where multiple message shapes coexist.

---

## JsonDeserializer

```yaml
spring.kafka.consumer.value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
```

The `JsonDeserializer` (provided by Spring Kafka) converts raw bytes directly into a **typed Java object** using Jackson. Your listener receives a `ConsumerRecord<K, LibraryEventDto>` — already deserialized.

### Consumer Example

```java
@KafkaListener(topics = "library-events")
public void onMessage(ConsumerRecord<Integer, LibraryEventDto> record) {
    // record.value() is already a LibraryEventDto — no manual parsing
    LibraryEventDto dto = record.value();
}
```

### Required Configuration

```yaml
spring:
  kafka:
    consumer:
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.learnkafka.dto,com.learnkafka.domain
        spring.json.value.default.type: com.learnkafka.dto.LibraryEventDto
        spring.json.type.mapping: com.learnkafka.domain.LibraryEvent:com.learnkafka.dto.LibraryEventDto
```

| Property | Purpose |
|---|---|
| `spring.json.trusted.packages` | Security: only allow deserialization of classes from these packages. Prevents arbitrary class instantiation. |
| `spring.json.value.default.type` | Fallback: if no type header is present in the Kafka message, deserialize into this class. |
| `spring.json.type.mapping` | Mapping: when the producer's type header says `com.learnkafka.domain.LibraryEvent`, map it to the consumer's `com.learnkafka.dto.LibraryEventDto`. |

### When to use
- You want **zero manual parsing** — Kafka delivers typed objects.
- Producer and consumer agree on a JSON schema (field names match).
- You want Spring to handle deserialization errors via the error handler pipeline.

---

## Can You Use StringDeserializer If the Producer Used JsonSerializer?

### Yes — absolutely.

The `JsonSerializer` on the producer side converts a Java object to **JSON bytes**. Those JSON bytes are also valid **UTF-8 strings**. The `StringDeserializer` simply reads bytes as a `String`, so it will produce a valid JSON string.

```
Producer                          Kafka                         Consumer
────────                          ─────                         ────────
LibraryEvent obj                                                
   │                                                            
   ▼ JsonSerializer                                             
Java Object → JSON bytes  ──────▶  Topic  ──────▶  JSON bytes  
                                                       │        
                                           StringDeserializer ▼ 
                                                  JSON String   
                                                       │        
                                            ObjectMapper.readValue()
                                                       ▼        
                                                LibraryEventDto 
```

### How it works

1. **Producer** serializes `LibraryEvent` → JSON bytes using `JsonSerializer`.
2. **Kafka** stores the bytes as-is. Kafka has no opinion about the format.
3. **Consumer** with `StringDeserializer` reads the bytes → `String` (valid JSON).
4. **You** call `objectMapper.readValue(jsonString, LibraryEventDto.class)` to get a typed object.

### Example

```java
@Service
public class LibraryEventService {

    private final ObjectMapper objectMapper;

    public void processEvent(ConsumerRecord<Integer, String> record) {
        // record.value() = "{\"libraryEventId\":1,\"eventType\":\"ADD\",\"book\":{...}}"
        LibraryEventDto dto = objectMapper.readValue(record.value(), LibraryEventDto.class);
    }
}
```

### Key difference from JsonDeserializer

When using `StringDeserializer`, the **Kafka type headers are ignored**. You decide the target class yourself when calling `objectMapper.readValue()`. This means:

- ✅ No `spring.json.trusted.packages` needed.
- ✅ No `spring.json.type.mapping` needed.
- ✅ No `trusted packages` errors.
- ❌ No automatic type resolution — you must know the target type.
- ❌ Deserialization errors happen in your code, not in the Kafka error handler pipeline.

---

## Side-by-Side Comparison

| Aspect | StringDeserializer + ObjectMapper | JsonDeserializer |
|---|---|---|
| **Parsing** | Manual (`objectMapper.readValue()`) | Automatic (Spring Kafka) |
| **Type headers** | Ignored | Used for class resolution |
| **Trusted packages config** | Not needed | Required |
| **Type mapping config** | Not needed | Needed when producer/consumer classes differ |
| **Error handling** | Exceptions in your service code | Exceptions in Kafka deserializer → error handler |
| **Flexibility** | High — you control everything | Lower — tied to Spring Kafka's deserialization |
| **Boilerplate** | More (manual parsing in every service) | Less (zero-parsing listeners) |

---

## Recommendation

For this project we use **`JsonDeserializer`** because:

1. The producer sends a consistent `LibraryEvent` JSON structure.
2. We want deserialization errors to be caught by the Kafka error handler (retry + DLT pipeline).
3. It eliminates boilerplate — the listener receives a typed `LibraryEventDto` directly.
4. Type mapping (`com.learnkafka.domain.LibraryEvent` → `com.learnkafka.dto.LibraryEventDto`) cleanly decouples the producer's domain model from the consumer's DTO.

