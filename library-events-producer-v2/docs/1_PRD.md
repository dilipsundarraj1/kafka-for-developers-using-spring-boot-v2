# Product Requirements Document: Library Events Producer API

## 1. Overview
**Product**: Library Events Producer API  
**Purpose**: Provide REST endpoints to publish library events to a Kafka topic.  
**Audience**: Internal services, library management systems, or batch jobs that emit library event changes.

## 2. Goals
- Enable clients to publish library events via HTTP POST and PUT.
- Validate event payloads and ensure structured messages are published to Kafka.
- Support event types ADD and UPDATE with embedded book details.
- Provide deterministic API responses and clear error handling.

## 3. Non-Goals / Out of Scope
- Consuming or processing Kafka events.
- Persistent storage of events.
- Authentication/authorization (unless specified later).
- Advanced workflow orchestration.

## 4. Personas & Use Cases
**Persona 1**: Library Management System  
- Sends event when a book is created or updated.

**Persona 2**: Admin/Batch Tool  
- Sends bulk updates for existing library event records.

## 5. Functional Requirements

### 5.1 API Endpoints
**POST `/v1/library-events`**
- Creates and publishes a new library event with type ADD.
- `libraryEventType` must be ADD.

**PUT `/v1/library-events`**
- Updates and publishes a library event with type UPDATE.
- `libraryEventId` is required.

### 5.2 Request/Response (JSON)
**LibraryEvent**
```json
{
  "libraryEventId": 123,
  "libraryEventType": "ADD",
  "book": {
    "bookId": 456,
    "bookName": "Clean Code",
    "bookAuthor": "Robert C. Martin"
  }
}
```

**Success Responses**
- POST: 201 Created with echo of validated event.
- PUT: 200 OK with echo of validated event.

**Error Responses**
- 400 Bad Request for validation errors.
- 500 Internal Server Error for Kafka publish failures after retry.

### 5.3 Validation Rules
- `libraryEventType` must be ADD or UPDATE.
- `book.bookId`, `book.bookName`, `book.bookAuthor` are required.
- `libraryEventId` is required for UPDATE.
- `libraryEventType` for POST must be ADD.

### 5.4 Kafka Publishing
- Publish all valid events to Kafka.
- Topic name: `library-events`.
- Message key: `libraryEventId` when present.
- Payload format: JSON.
- Delivery semantics: at-least-once.
- Retry strategy: retry Kafka publish failures before returning server error.

## 6. Non-Functional Requirements
- **Performance**: Typical REST workloads with sub-second response time.
- **Reliability**: Retry strategy for Kafka publish failures; at-least-once delivery.
- **Observability**: Log publish success/failure; optional metrics for counts and latency.
- **Compatibility**: Spring Boot 4, Java 25.

## 7. Risks & Dependencies
- Kafka availability and topic configuration.
- Schema evolution and event versioning.

## 8. Open Questions
- None currently. Pending future stakeholder input.

