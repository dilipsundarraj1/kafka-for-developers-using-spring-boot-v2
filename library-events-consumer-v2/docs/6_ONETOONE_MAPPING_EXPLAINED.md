# @OneToOne JPA Mapping — Explained with Our Domain Model

## Overview

`@OneToOne` defines a single-valued association between two entities where each row in table A
maps to exactly one row in table B, and vice versa.

In our project:

```
┌──────────────────────┐         ┌─────────────────────────┐
│    library_event     │         │          book            │
├──────────────────────┤         ├─────────────────────────┤
│ library_event_id (PK)│◀────────┼ library_event_id (FK)    │
│ event_type           │         │ book_id (PK)             │
└──────────────────────┘         │ book_name                │
                                 │ book_author              │
                                 └─────────────────────────┘
```

One `LibraryEvent` has exactly one `Book`. One `Book` belongs to exactly one `LibraryEvent`.
The FK (`library_event_id`) lives in the **`book`** table.

---

## The Two Sides

### Owning Side — `Book`

```java
@Entity
public class Book {

    @OneToOne
    @JoinColumn(name = "library_event_id")
    private LibraryEvent libraryEvent;
}
```

- **`@OneToOne`** — declares the relationship.
- **`@JoinColumn(name = "library_event_id")`** — this entity's table (`book`) holds the foreign key column `library_event_id` pointing to `library_event.library_event_id`.
- This is the **owning side** because it has `@JoinColumn` — it controls the FK in the database.

### Inverse (Non-Owning) Side — `LibraryEvent`

```java
@Entity
public class LibraryEvent {

    @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
    private Book book;
}
```

- **`mappedBy = "libraryEvent"`** — tells JPA: "I don't own this relationship. The FK is managed by the `libraryEvent` field in the `Book` entity."
- The `"libraryEvent"` string refers to the **Java field name** `private LibraryEvent libraryEvent` in `Book`, NOT a column name.
- This side has **no `@JoinColumn`** — it never writes the FK. It's read-only for the relationship.
- **`cascade = {CascadeType.ALL}`** — operations on `LibraryEvent` cascade to `Book` (persist, merge, remove, etc.).

---

## `@JoinColumn`

```java
@JoinColumn(name = "library_event_id")
```

| Attribute | Meaning |
|-----------|---------|
| `name` | The FK column name in the **owning** entity's table (`book.library_event_id`) |
| (references) | By default, it references the PK of the target entity (`library_event.library_event_id`) |

Without `@JoinColumn`, JPA would auto-generate a column name like `library_event_library_event_id` — specifying it gives us a clean `library_event_id`.

---

## `mappedBy`

```java
@OneToOne(mappedBy = "libraryEvent")
```

| What it does | Details |
|--------------|---------|
| Declares the inverse side | This entity does NOT own the FK |
| Value | The field name on the owning side (`Book.libraryEvent`) that holds the `@JoinColumn` |
| Database effect | **No FK column** is created in this entity's table |

**Common mistake:** Setting `@JoinColumn` on BOTH sides → creates two FK columns, one in each table, causing confusion and data inconsistency.

---

## Cascade Types

```java
@OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
```

Cascade means: "when I perform this operation on `LibraryEvent`, do the same on `Book`."

| CascadeType | Meaning | Our usage |
|-------------|---------|-----------|
| `PERSIST` | When `LibraryEvent` is persisted, also persist `Book` | ✅ Included via `ALL` |
| `MERGE` | When `LibraryEvent` is merged, also merge `Book` | ✅ Included — needed for UPDATE flow |
| `REMOVE` | When `LibraryEvent` is deleted, also delete `Book` | ✅ Included — cleanup |
| `REFRESH` | When `LibraryEvent` is refreshed, also refresh `Book` | ✅ Included via `ALL` |
| `DETACH` | When `LibraryEvent` is detached, also detach `Book` | ✅ Included via `ALL` |
| `ALL` | All of the above | ✅ Used |

> **Note:** `CascadeType.ALL` is now safe because we control the save order — `LibraryEvent`
> is saved first (gets its DB-generated ID), then `Book` is saved with the FK. The cascade
> operates from `LibraryEvent` (inverse side) to `Book` (owning side).

---

## Owning Side vs Inverse Side — Why Does It Matter?

```
Owning side (Book)             Inverse side (LibraryEvent)
──────────────────             ────────────────────────────
Has @JoinColumn                Has mappedBy
Controls the FK column         Does NOT control the FK
Changes here update the DB     Changes here are IGNORED by JPA for FK writes
```

**Critical implication:** If you only set `libraryEvent.setBook(book)` (inverse side) but don't
set `book.setLibraryEvent(event)` (owning side), **the FK will NOT be written to the database**.
JPA only looks at the owning side when writing the FK.

### Example — what works vs what doesn't

```java
// ✅ This writes the FK — setting the owning side
book.setLibraryEvent(savedEvent);
bookRepository.save(book);
// → book.library_event_id = 1 ✅

// ❌ This alone does NOTHING to the FK — setting only the inverse side
libraryEvent.setBook(book);
libraryEventRepository.save(libraryEvent);
// → book.library_event_id = null ❌
```

---

## Our Save Order (and why)

```java
// 1. Detach book temporarily to avoid cascade issues on persist
libraryEvent.setBook(null);

// 2. Save LibraryEvent first — it has @GeneratedValue(IDENTITY), DB generates the ID
LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

// 3. Create Book and set the FK (owning side) pointing to the persisted LibraryEvent
Book book = LibraryEventMapper.toBookEntity(libraryEventDto.book());
book.setLibraryEvent(savedEvent);   // ← owning side: this writes the FK
Book savedBook = bookRepository.save(book);

// 4. Set inverse back-reference (for in-memory consistency)
savedEvent.setBook(savedBook);
```

**Why this order?**
- `LibraryEvent` has `@GeneratedValue(IDENTITY)` → its ID is null until saved → must be saved first so the FK value exists.
- `Book` has a producer-provided ID (`bookId = 1`) → saved second with `library_event_id` pointing to the now-persisted `LibraryEvent`.
- Step 4 is optional for the database (JPA ignores the inverse side for FK writes), but keeps the in-memory object graph consistent.

---

## Generated SQL

When `libraryEventRepository.save(libraryEvent)` executes:

```sql
INSERT INTO library_event (event_type)
VALUES ('ADD')
-- Returns generated library_event_id = 1
```

The `library_event` table has **no FK column** — it's the inverse side.

When `bookRepository.save(book)` executes:

```sql
INSERT INTO book (book_id, book_name, book_author, library_event_id)
VALUES (1, 'Clean Code', 'Robert C. Martin', 1)
```

The `library_event_id` FK is written because `Book` is the **owning side** with `@JoinColumn(name = "library_event_id")`.

---

## Summary

| Concept | `Book` (Owner) | `LibraryEvent` (Inverse) |
|---------|----------------|--------------------------|
| Annotation | `@OneToOne` | `@OneToOne(mappedBy = "libraryEvent")` |
| FK column | `@JoinColumn(name = "library_event_id")` | None |
| Controls FK? | ✅ Yes | ❌ No |
| Cascade | None (cascaded from `LibraryEvent`) | `CascadeType.ALL` |
| `@GeneratedValue` | None (producer provides `bookId`) | `IDENTITY` (DB generates ID) |
| Save order | Saved second | Saved first |


