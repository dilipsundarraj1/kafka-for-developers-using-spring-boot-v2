# cURL Commands for Library Events Producer API

## Overview
The endpoint is located at: `POST /v1/library-events`
Default Base URL: `http://localhost:8080`

---

## 1. Basic Valid POST Request

Creates a new library event with an ADD type (required for POST):

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

---

## 2. POST with Different Book Data

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 2,
      "bookName": "Design Patterns",
      "bookAuthor": "Gang of Four"
    }
  }'
```

---

## 3. Invalid: Missing Book Name (should fail with 400)

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

---

## 4. Invalid: Missing Book Author (should fail with 400)

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": ""
    }
  }'
```

---

## 5. Invalid: Wrong Library Event Type (should fail with 400)

POST endpoint requires libraryEventType to be "ADD". Using "UPDATE" will fail:

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1,
    "libraryEventType": "UPDATE",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

---

## 6. Invalid: Missing Book ID (should fail with 400)

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

---

## 7. Invalid: Malformed JSON (should fail with 400)

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    invalid json here
  }'
```

---

## 8. Multiple Sequential Requests

Run multiple valid POST requests:

```bash
# Request 1
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": null, "libraryEventType": "ADD", "book": {"bookId": 1, "bookName": "Clean Code", "bookAuthor": "Robert C. Martin"}}'

# Request 2
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": null, "libraryEventType": "ADD", "book": {"bookId": 2, "bookName": "Effective Java", "bookAuthor": "Joshua Bloch"}}'

# Request 3
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": null, "libraryEventType": "ADD", "book": {"bookId": 3, "bookName": "Spring in Action", "bookAuthor": "Craig Walls"}}'
```

---

## 9. With Verbose Output (for debugging)

```bash
curl -v -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

---

## 10. With Pretty-Printed Response (requires jq)

```bash
curl -X POST http://localhost:8080/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 1,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }' | jq
```

---

## Expected Responses

### Success (201 Created)
```json
{
  "libraryEventId": null,
  "libraryEventType": "ADD",
  "book": {
    "bookId": 1,
    "bookName": "Clean Code",
    "bookAuthor": "Robert C. Martin"
  }
}
```

### Validation Error (400 Bad Request)
```json
{
  "timestamp": "2026-03-02T12:00:00.000+00:00",
  "status": 400,
  "error": "Bad Request",
  "message": "Validation failed",
  "errors": [
    {
      "field": "bookName",
      "message": "must not be blank"
    }
  ]
}
```

---

## Notes

- **libraryEventId**: Should be `null` for POST requests (only used in PUT/UPDATE operations)
- **libraryEventType**: Must be "ADD" for POST requests
- **Book fields**: All book fields are required and non-blank
- **Content-Type**: Must be `application/json`
- **Expected Status**: 201 Created (with response body containing the created event)

