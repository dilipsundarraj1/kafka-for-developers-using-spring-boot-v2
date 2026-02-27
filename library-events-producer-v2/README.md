# Library Events Producer API

A Spring Boot application that provides REST endpoints to publish library events to Apache Kafka. This service enables clients to emit library event changes (ADD, UPDATE) through a well-defined HTTP API.

## 📋 Quick Links

- **[Product Requirements Document](./docs/PRD.md)** - Complete product specifications, features, and requirements
- **[Implementation Plan](./docs/IMPLEMENTATION_PLAN_README.md)** - Engineering roadmap and implementation details
- **[Architecture Diagram](./docs/ARCHITECTURE_DIAGRAM.md)** - System architecture, testing setup, and component interactions

## 🚀 Quick Start

### Prerequisites
- Java 25+
- Docker & Docker Compose (for local Kafka broker)
- Gradle

### Setup & Run

1. **Start Kafka Broker** (using Docker Compose):
   ```bash
   docker-compose up -d
   ```
   This starts a KRaft-mode Kafka broker on `localhost:9092`

2. **Build the Application**:
   ```bash
   ./gradlew build
   ```

3. **Run the Application**:
   ```bash
   ./gradlew bootRun
   ```
   The API will be available at `http://localhost:8080`

4. **Run Tests**:
   ```bash
   ./gradlew test
   ```
   Tests use embedded Kafka (no Docker required)

5. **Stop Kafka**:
   ```bash
   docker-compose down
   ```

## 📚 Documentation

### Product Requirements Document ([PRD.md](./docs/PRD.md))
Contains:
- Product overview and goals
- Personas and use cases
- Functional requirements
- API endpoint specifications
- Request/response formats
- Validation rules and error handling

**Key Points:**
- POST `/v1/library-events` - Create new library event (ADD type only)
- PUT `/v1/library-events` - Update existing library event (UPDATE type, requires libraryEventId)
- Events are published to Kafka topic: `library-events`

### Implementation Plan ([IMPLEMENTATION_PLAN_README.md](./docs/IMPLEMENTATION_PLAN_README.md))
Covers:
- Domain model design
- API layer implementation
- Kafka producer configuration
- Retry strategy details
- Serialization approach
- Testing strategy
- Observability and logging

### Architecture Diagram ([ARCHITECTURE_DIAGRAM.md](./docs/ARCHITECTURE_DIAGRAM.md))
Includes:
- Production/Development architecture flow
- Testing architecture with embedded Kafka
- System component breakdown
- Technology stack details

## 🏗️ Architecture Overview

### Production/Development Flow
```
REST Client 
    ↓
LibraryEventsController (POST/PUT)
    ↓
Validation (Bean Validation)
    ↓
LibraryEventProducer (KafkaTemplate)
    ↓
Retry Strategy (at-least-once)
    ↓
Kafka Broker
```

### Testing Approach
- **Integration Tests**: Use `@SpringBootTest` with `@EmbeddedKafka` for real component interactions
- **No Mocking**: Tests verify actual behavior without mocks
- **Isolated Environment**: Embedded Kafka provides isolation without external dependencies

## 🔧 Configuration

### Kafka Configuration (`application.yml`)
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.IntegerSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

library:
  events:
    topic: library-events
```

### Docker Compose Setup (`compose.yaml`)
- Runs Kafka 7.4.0 in KRaft mode (no Zookeeper)
- Exposes port 9092 for client connections
- Automatically creates topics

## 📦 Dependencies

### Core
- Spring Boot 4.0.2
- Spring Kafka
- Spring Validation
- Spring Web MVC

### Testing
- Spring Boot Test
- Spring Kafka Test
- JUnit 5
- MockMvc

## 🧪 Testing

### Running Tests
```bash
./gradlew test
```

### Integration Tests
Located in `src/test/java/com/learnkafka/controller/LibraryEventsControllerIntegrationTest.java`

Tests include:
- Valid POST/PUT operations
- Validation error scenarios
- Multiple event publishing
- Invalid JSON handling
- Content-Type validation

**Key Test Configuration:**
```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = "library-events")
```

## 📝 API Examples

### Create Library Event (POST)
```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

**Response (201 Created):**
```json
{
  "libraryEventId": 1,
  "libraryEventType": "ADD",
  "book": {
    "bookId": 1,
    "bookName": "Clean Code",
    "bookAuthor": "Robert C. Martin"
  }
}
```

### Update Library Event (PUT)
```bash
curl -X PUT http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1,
    "libraryEventType": "UPDATE",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code (2nd Edition)",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

## 📂 Project Structure

```
library-events-producer-v2/
├── src/
│   ├── main/
│   │   ├── java/com/learnkafka/
│   │   │   ├── controller/      # API endpoints
│   │   │   ├── domain/          # Domain models
│   │   │   ├── producer/        # Kafka producer
│   │   │   └── ...
│   │   └── resources/
│   │       └── application.yml   # Configuration
│   └── test/
│       ├── java/com/learnkafka/
│       │   └── controller/       # Integration tests
│       └── resources/
├── docs/
│   ├── PRD.md                    # Product requirements
│   ├── IMPLEMENTATION_PLAN_README.md
│   └── ARCHITECTURE_DIAGRAM.md
├── compose.yaml                  # Docker Compose Kafka setup
├── build.gradle                  # Gradle configuration
└── README.md                      # This file
```

## 🔍 Key Concepts

### Event Types
- **ADD**: Create a new library event (POST endpoint)
- **UPDATE**: Update an existing library event (PUT endpoint)

### Validation Rules
- `libraryEventType` is required and must be valid
- For POST: `libraryEventType` must be ADD
- For PUT: `libraryEventId` is required, `libraryEventType` must be UPDATE
- Book fields (`bookId`, `bookName`, `bookAuthor`) are required and cannot be blank

### Kafka Publishing
- Messages are published to `library-events` topic
- Uses `libraryEventId` as message key (when present)
- JSON serialization for payloads
- At-least-once delivery semantics with retry strategy

## 🛠️ Development

### Building
```bash
./gradlew clean build
```

### Development Mode
The `spring-boot-docker-compose` dependency automatically starts Docker Compose services when running the application:
```bash
./gradlew bootRun
```

### Debugging
Enable debug logging in `application.yml`:
```yaml
logging:
  level:
    com.learnkafka: DEBUG
    org.springframework.kafka: DEBUG
```

## 📊 Monitoring & Observability

The application logs:
- Request receipt and validation
- Kafka publish success/failure
- Retry attempts and final failures
- Error details with stack traces

## 🚨 Error Handling

| Status Code | Scenario |
|-------------|----------|
| 201 Created | Event published successfully (POST) |
| 200 OK | Event published successfully (PUT) |
| 400 Bad Request | Validation error (invalid payload, missing fields) |
| 415 Unsupported Media Type | Missing/invalid Content-Type header |
| 500 Internal Server Error | Kafka publish failure after retries |

## 📞 Support & Contributions

For questions or issues:
1. Review the [PRD](./docs/PRD.md) for requirements
2. Check the [Implementation Plan](./docs/IMPLEMENTATION_PLAN_README.md) for technical details
3. Refer to [Architecture Diagram](./docs/ARCHITECTURE_DIAGRAM.md) for system design

## 📄 License

This is an educational project for learning Kafka and Spring Boot integration.

---

**Last Updated**: February 2026  
**Version**: 0.0.1-SNAPSHOT

