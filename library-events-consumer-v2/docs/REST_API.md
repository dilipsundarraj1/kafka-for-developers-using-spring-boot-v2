# REST API: Exposing Library Events and Books

This reference doc covers the external REST API layer of the Library Events Consumer service — why it exists, how it is structured, and how each endpoint works.

## Table of Contents

- [Why a Consumer Service Exposes REST APIs](#why-a-consumer-service-exposes-rest-apis)
- [Architecture: Kafka Write Path + REST Read Path](#architecture-kafka-write-path--rest-read-path)
- [Domain Model](#domain-model)
- [DTO Design: Input vs Response](#dto-design-input-vs-response)
  - [Input DTOs (Kafka)](#input-dtos-kafka)
  - [Response DTOs (REST)](#response-dtos-rest)
  - [LibraryEventMapper](#libraryeventmapper)
- [Part 1: Library Events API](#part-1-library-events-api)
  - [Why Read-Only?](#why-read-only)
  - [GET /v1/library-events](#get-v1library-events)
  - [GET /v1/library-events/{libraryEventId}](#get-v1library-eventslibraryeventid)
  - [LibraryEventService — Query Methods](#libraryeventservice--query-methods)
- [Part 2: Books API](#part-2-books-api)
  - [GET /v1/books](#get-v1books)
  - [GET /v1/books/{bookId}](#get-v1booksbookid)
  - [POST /v1/books](#post-v1books)
  - [PUT /v1/books/{bookId}](#put-v1booksbookid)
  - [DELETE /v1/books/{bookId}](#delete-v1booksbookid)
  - [BookService](#bookservice)
- [Part 3: Key Design Decisions](#part-3-key-design-decisions)
  - [Book ID is Client-Assigned](#book-id-is-client-assigned)
  - [Delete Breaks the Bidirectional Reference First](#delete-breaks-the-bidirectional-reference-first)
  - [LibraryEvent is Always Read-Only via REST](#libraryevent-is-always-read-only-via-rest)

---

## Why a Consumer Service Exposes REST APIs

A Kafka consumer's primary job is to process messages. But once those messages are persisted to the database, you need a way to read them. REST APIs serve two purposes here:

1. **Operational visibility** — inspect what has been consumed and persisted without querying the database directly.
2. **Service integration** — other systems can query library events and books over HTTP rather than subscribing to the Kafka topic.

This is a lightweight variant of the **CQRS** (Command Query Responsibility Segregation) pattern:
- **Write path** — Kafka → `@KafkaListener` → `LibraryEventService.processEvent()` → database
- **Read path** — HTTP client → REST controller → service → database → response DTO

The two paths share the same database but are completely independent code paths. The REST layer never writes to `library_event` or `book` through Kafka — and the consumer never handles HTTP.

---

## Architecture: Kafka Write Path + REST Read Path

```
                    ┌─────────────────────────────────────┐
                    │       Kafka Write Path              │
                    │                                     │
Kafka Broker        │  LibraryEventsConsumer              │
library-events ────►│  @KafkaListener.onMessage()         │
                    │        │                            │
                    │        ▼                            │
                    │  LibraryEventService.processEvent() │
                    │        │                            │
                    │        ▼                            │
                    │  PostgreSQL (library_event + book)  │
                    │                       │             │
                    └───────────────────────┼─────────────┘
                                            │
                    ┌───────────────────────┼─────────────┐
                    │       REST Read/Write Path           │
                    │                       │             │
HTTP Client ───────►│  LibraryEventController             │
GET /v1/library-events    BookController    │             │
GET /v1/books       │        │              │             │
POST /v1/books      │        ▼              ▼             │
PUT  /v1/books      │  LibraryEventService  BookService   │
DELETE /v1/books    │  .findAll()           .findAll()    │
                    │  .findById()          .create()     │
                    │                       .update()     │
                    │                       .delete()     │
                    └─────────────────────────────────────┘
```

---

## Domain Model

Two JPA entities back the REST API:

```
LibraryEvent                         Book
────────────────────────────         ────────────────────────────
libraryEventId  INTEGER (PK, auto)   bookId          INTEGER (PK, client-assigned)
eventType       VARCHAR (ADD|UPDATE) bookName        VARCHAR
book            OneToOne (cascade)   bookAuthor      VARCHAR
createdAt       TIMESTAMP            libraryEvent    OneToOne (FK: library_event_id)
updatedAt       TIMESTAMP            createdAt       TIMESTAMP
                                     updatedAt       TIMESTAMP
```

**Relationship:** `LibraryEvent` owns one `Book` via cascade. `Book` holds the foreign key `library_event_id`. This is a bidirectional `@OneToOne`:

```java
// LibraryEvent — owns the relationship
@OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
private Book book;

// Book — holds the FK column
@OneToOne
@JoinColumn(name = "library_event_id")
private LibraryEvent libraryEvent;
```

**Key implication:** deleting a `Book` requires clearing the back-reference on `LibraryEvent` first — otherwise the cascade will try to re-persist the book (see [Delete Breaks the Bidirectional Reference First](#delete-breaks-the-bidirectional-reference-first)).

---

## DTO Design: Input vs Response

The project uses separate DTOs for the two different data flows.

### Input DTOs (Kafka)

Used to deserialize Kafka messages. These are Java records with validation annotations:

```java
// Consumed from Kafka — validation enforced during processing
public record LibraryEventDto(
    Integer libraryEventId,
    @NotNull LibraryEventType libraryEventType,
    @NotNull @Valid BookDto book
) {}

public record BookDto(
    @NotNull Integer bookId,
    @NotBlank String bookName,
    @NotBlank String bookAuthor
) {}
```

`BookDto` is also reused as the request body for `POST /v1/books` and `PUT /v1/books/{bookId}`.

### Response DTOs (REST)

Used only for HTTP responses — they are richer than the input shape and include audit timestamps and relationship IDs:

```java
public record LibraryEventResponseDto(
    Integer libraryEventId,
    LibraryEventType eventType,
    BookResponseDto book,     // ← nested, not a raw FK
    LocalDateTime createdAt,
    LocalDateTime updatedAt
) {}

public record BookResponseDto(
    Integer bookId,
    String bookName,
    String bookAuthor,
    Integer libraryEventId,   // ← FK surfaced for the client
    LocalDateTime createdAt,
    LocalDateTime updatedAt
) {}
```

**Why separate DTOs?**
- The input shape (`LibraryEventDto`) matches what producers publish to Kafka — changing it for REST clients would break deserialization.
- The response shape (`LibraryEventResponseDto`) is designed for HTTP consumers — includes timestamps and nested objects that are irrelevant inside Kafka messages.
- Separating the two prevents accidental coupling between the Kafka contract and the REST API contract.

### LibraryEventMapper

A static utility class that converts between entities and DTOs. No Spring beans, no injection — just plain static methods:

```java
public class LibraryEventMapper {

    // Entity → response DTO (for REST responses)
    public static LibraryEventResponseDto toLibraryEventResponseDto(LibraryEvent libraryEvent) {
        BookResponseDto bookResponseDto = libraryEvent.getBook() != null
                ? toBookResponseDto(libraryEvent.getBook())
                : null;
        return new LibraryEventResponseDto(
                libraryEvent.getLibraryEventId(),
                libraryEvent.getEventType(),
                bookResponseDto,
                libraryEvent.getCreatedAt(),
                libraryEvent.getUpdatedAt()
        );
    }

    public static BookResponseDto toBookResponseDto(Book book) {
        Integer libraryEventId = book.getLibraryEvent() != null
                ? book.getLibraryEvent().getLibraryEventId()
                : null;
        return new BookResponseDto(
                book.getBookId(),
                book.getBookName(),
                book.getBookAuthor(),
                libraryEventId,
                book.getCreatedAt(),
                book.getUpdatedAt()
        );
    }

    // Input DTO → entity (used by both Kafka consumer and BookService)
    public static Book toBookEntity(BookDto dto) {
        return new Book(dto.bookId(), dto.bookName(), dto.bookAuthor());
    }

    public static LibraryEvent toEntity(LibraryEventDto dto) {
        Book book = toBookEntity(dto.book());
        return new LibraryEvent(dto.libraryEventId(), dto.libraryEventType(), book);
    }
}
```

**Why static?** Mapping is pure data transformation — no side effects, no dependencies. A static utility is simpler and easier to test than a Spring-managed bean for this job.

---

## Part 1: Library Events API

Base path: `/v1/library-events`

### Why Read-Only?

`LibraryEvent` records are **created and updated exclusively through Kafka**. The consumer is the only writer — no REST endpoint creates or modifies a library event directly. Exposing write endpoints here would bypass the Kafka contract and create two inconsistent write paths.

The REST layer exposes library events as a **query interface** only: inspect what has been persisted from the topic.

### GET /v1/library-events

Returns all library events currently persisted in the database.

**Request**
```
GET /v1/library-events
```

**Response — 200 OK**
```json
[
  {
    "libraryEventId": 1,
    "eventType": "ADD",
    "book": {
      "bookId": 101,
      "bookName": "Kafka: The Definitive Guide",
      "bookAuthor": "Neha Narkhede",
      "libraryEventId": 1,
      "createdAt": "2024-01-15T10:30:00",
      "updatedAt": "2024-01-15T10:30:00"
    },
    "createdAt": "2024-01-15T10:30:00",
    "updatedAt": "2024-01-15T10:30:00"
  }
]
```

Returns an empty list `[]` if no events have been consumed yet — never 404.

**Controller**
```java
@GetMapping
public ResponseEntity<List<LibraryEventResponseDto>> getAllLibraryEvents() {
    log.info("GET /v1/library-events");
    List<LibraryEventResponseDto> libraryEvents = libraryEventService.findAll();
    return ResponseEntity.ok(libraryEvents);
}
```

---

### GET /v1/library-events/{libraryEventId}

Returns a single library event by its database ID.

**Request**
```
GET /v1/library-events/1
```

**Response — 200 OK**
```json
{
  "libraryEventId": 1,
  "eventType": "ADD",
  "book": {
    "bookId": 101,
    "bookName": "Kafka: The Definitive Guide",
    "bookAuthor": "Neha Narkhede",
    "libraryEventId": 1,
    "createdAt": "2024-01-15T10:30:00",
    "updatedAt": "2024-01-15T10:30:00"
  },
  "createdAt": "2024-01-15T10:30:00",
  "updatedAt": "2024-01-15T10:30:00"
}
```

**Response — 404 Not Found** (if `libraryEventId` does not exist)

**Controller**
```java
@GetMapping("/{libraryEventId}")
public ResponseEntity<LibraryEventResponseDto> getLibraryEventById(
        @PathVariable Integer libraryEventId) {
    log.info("GET /v1/library-events/{}", libraryEventId);
    return libraryEventService.findById(libraryEventId)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
}
```

---

### LibraryEventService — Query Methods

The service layer wraps repository calls and maps entities to response DTOs:

```java
public List<LibraryEventResponseDto> findAll() {
    return libraryEventRepository.findAll()
            .stream()
            .map(LibraryEventMapper::toLibraryEventResponseDto)
            .toList();
}

public Optional<LibraryEventResponseDto> findById(Integer libraryEventId) {
    return libraryEventRepository.findById(libraryEventId)
            .map(LibraryEventMapper::toLibraryEventResponseDto);
}
```

The controller maps `Optional.empty()` to 404 — the service never throws for a missing ID, it returns `Optional.empty()` and lets the controller decide the HTTP status.

---

## Part 2: Books API

Base path: `/v1/books`

Unlike library events, books support full CRUD via REST. A book can be queried, created standalone, updated, or deleted independently of any Kafka message.

### GET /v1/books

Returns all books.

**Request**
```
GET /v1/books
```

**Response — 200 OK**
```json
[
  {
    "bookId": 101,
    "bookName": "Kafka: The Definitive Guide",
    "bookAuthor": "Neha Narkhede",
    "libraryEventId": 1,
    "createdAt": "2024-01-15T10:30:00",
    "updatedAt": "2024-01-15T10:30:00"
  }
]
```

`libraryEventId` is `null` for books created directly via `POST /v1/books` (no library event association).

---

### GET /v1/books/{bookId}

Returns a single book by its ID.

**Request**
```
GET /v1/books/101
```

**Response — 200 OK** / **404 Not Found**

---

### POST /v1/books

Creates a new book. The client supplies the `bookId` — there is no auto-generation on the `Book` entity.

**Request**
```
POST /v1/books
Content-Type: application/json

{
  "bookId": 200,
  "bookName": "Clean Code",
  "bookAuthor": "Robert C. Martin"
}
```

**Response — 201 Created**
```json
{
  "bookId": 200,
  "bookName": "Clean Code",
  "bookAuthor": "Robert C. Martin",
  "libraryEventId": null,
  "createdAt": "2024-01-15T11:00:00",
  "updatedAt": "2024-01-15T11:00:00"
}
```

`libraryEventId` is `null` because this book was created standalone — not through a Kafka `ADD` event.

**Validation:** `@Valid` on the request body enforces `@NotNull bookId`, `@NotBlank bookName`, `@NotBlank bookAuthor`. A missing or blank field returns 400 Bad Request.

**Controller**
```java
@PostMapping
public ResponseEntity<BookResponseDto> createBook(@RequestBody @Valid BookDto bookDto) {
    log.info("POST /v1/books - {}", bookDto);
    BookResponseDto created = bookService.create(bookDto);
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
}
```

---

### PUT /v1/books/{bookId}

Updates an existing book's name and author. The `bookId` in the path is the identifier — the body supplies the new values.

**Request**
```
PUT /v1/books/101
Content-Type: application/json

{
  "bookId": 101,
  "bookName": "Kafka: The Definitive Guide (2nd Ed.)",
  "bookAuthor": "Neha Narkhede"
}
```

**Response — 200 OK** (updated book) / **404 Not Found**

**What gets updated:** only `bookName` and `bookAuthor`. The `bookId` and `libraryEvent` association are not touched — you cannot re-assign a book to a different library event via this endpoint.

**Controller**
```java
@PutMapping("/{bookId}")
public ResponseEntity<BookResponseDto> updateBook(@PathVariable Integer bookId,
                                                  @RequestBody @Valid BookDto bookDto) {
    log.info("PUT /v1/books/{} - {}", bookId, bookDto);
    return bookService.update(bookId, bookDto)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.notFound().build());
}
```

---

### DELETE /v1/books/{bookId}

Deletes a book by its ID.

**Request**
```
DELETE /v1/books/101
```

**Response — 204 No Content** (deleted) / **404 Not Found**

**Controller**
```java
@DeleteMapping("/{bookId}")
public ResponseEntity<Void> deleteBook(@PathVariable Integer bookId) {
    log.info("DELETE /v1/books/{}", bookId);
    if (bookService.delete(bookId)) {
        return ResponseEntity.noContent().build();
    }
    return ResponseEntity.notFound().build();
}
```

---

### BookService

```java
@Transactional
public BookResponseDto create(BookDto bookDto) {
    Book book = LibraryEventMapper.toBookEntity(bookDto);
    Book savedBook = bookRepository.save(book);
    return LibraryEventMapper.toBookResponseDto(savedBook);
}

@Transactional
public Optional<BookResponseDto> update(Integer bookId, BookDto bookDto) {
    return bookRepository.findById(bookId)
            .map(existingBook -> {
                existingBook.setBookName(bookDto.bookName());
                existingBook.setBookAuthor(bookDto.bookAuthor());
                Book updatedBook = bookRepository.save(existingBook);
                return LibraryEventMapper.toBookResponseDto(updatedBook);
            });
}

@Transactional
public boolean delete(Integer bookId) {
    return bookRepository.findById(bookId)
            .map(book -> {
                if (book.getLibraryEvent() != null) {
                    book.getLibraryEvent().setBook(null);  // ← break back-reference first
                }
                bookRepository.delete(book);
                return true;
            })
            .orElse(false);
}
```

---

## Part 3: Key Design Decisions

### Book ID is Client-Assigned

`Book.bookId` has no `@GeneratedValue` — the client (or Kafka message) supplies the ID. This matches the Kafka message contract: the producer sets `bookId` in the message, and the consumer persists it as-is.

When creating a book via `POST /v1/books`, the client must provide a `bookId`. If the same `bookId` already exists, the database will throw a `DataIntegrityViolationException` (classified as non-retryable in the Kafka error handler).

---

### Delete Breaks the Bidirectional Reference First

`LibraryEvent` has `cascade = ALL` on its `book` field. If you delete a `Book` without first nulling out `LibraryEvent.book`, Hibernate will attempt to re-persist the book through the cascade after the delete — resulting in an `EntityExistsException`.

`BookService.delete()` clears the back-reference before deleting:

```java
if (book.getLibraryEvent() != null) {
    book.getLibraryEvent().setBook(null);   // ← prevent cascade re-persist
}
bookRepository.delete(book);
```

This is a common pitfall with bidirectional `@OneToOne` with `cascade = ALL` — the owning side must be cleaned up before removing the owned entity.

---

### LibraryEvent is Always Read-Only via REST

The consumer is the only writer to `library_event`. This is enforced by design — there is no `POST`, `PUT`, or `DELETE` on `LibraryEventController`. Adding them would mean library events could be created or modified without going through Kafka, breaking the event log.

If you need to correct a library event, the right approach is to publish a corrective `UPDATE` event to the Kafka topic and let the consumer process it.

---

## API Summary

| Method | Path | Description | Status Codes |
|--------|------|-------------|--------------|
| `GET` | `/v1/library-events` | All library events | 200 |
| `GET` | `/v1/library-events/{id}` | Single library event | 200, 404 |
| `GET` | `/v1/books` | All books | 200 |
| `GET` | `/v1/books/{bookId}` | Single book | 200, 404 |
| `POST` | `/v1/books` | Create a book | 201, 400 |
| `PUT` | `/v1/books/{bookId}` | Update a book | 200, 400, 404 |
| `DELETE` | `/v1/books/{bookId}` | Delete a book | 204, 404 |
