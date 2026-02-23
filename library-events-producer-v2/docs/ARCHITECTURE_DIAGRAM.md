# Architecture Diagram: Library Events Producer API

```mermaid
flowchart LR
  subgraph Client
    A[REST Client]
  end

  subgraph Service[Library Events Producer]
    B[LibraryEventsController
POST /v1/library-events
PUT /v1/library-events]
    C[Validation
Bean Validation]
    D[LibraryEventProducer
KafkaTemplate]
    E[Retry Strategy
(at-least-once)]
  end

  subgraph Kafka
    F[(Topic: library-events)]
  end

  A --> B --> C --> D --> E --> F
```

## Notes
- POST requires `libraryEventType = ADD`.
- PUT requires `libraryEventId` and `libraryEventType = UPDATE`.
- Payload format is JSON.
- Publish failures trigger retry; after retries, API returns server error.

