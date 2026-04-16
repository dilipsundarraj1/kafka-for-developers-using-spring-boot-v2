# Flyway Schema Management

## What Is Flyway?

Flyway is a **version-controlled database migration tool**. You write explicit SQL
scripts — one per change — and Flyway applies them **in order**, exactly once, tracking
progress in a `flyway_schema_history` table.

```
V1__init_schema.sql          ← creates tables
V2__add_audit_columns.sql    ← alters tables to add columns
V3__add_index_on_book.sql    ← future migration
```

On every application startup Flyway checks which migrations have already been applied
and runs only the new ones.

---

## Why Flyway

| Benefit | What it means |
|---------|---------------|
| **Full SQL control** | Plain SQL — `CREATE`, `ALTER`, `DROP`, `RENAME`, data backfills. Nothing is inferred. |
| **Versioned and tracked** | Numbered files in Git + `flyway_schema_history` table. Always know what was applied and when. |
| **Consistent across environments** | Same migrations always produce the same schema in dev, staging, and production. |
| **Data + schema in one migration** | Backfills and transforms live in the same file as the `ALTER TABLE`, keeping changes atomic. |
| **Fails fast on tampering** | Checksums every applied file — changed file means the app refuses to start. |
| **Team-safe** | Version conflicts surface as Git merge conflicts, not runtime surprises. |

---

## How This Project Uses Flyway

### Dependencies (`build.gradle`)

```groovy
implementation 'org.flywaydb:flyway-core'
implementation 'org.flywaydb:flyway-database-postgresql'
implementation 'org.springframework.boot:spring-boot-starter-flyway'
```

- `flyway-core` — the migration engine.
- `flyway-database-postgresql` — PostgreSQL-specific dialect support (required since Flyway 10+).
- `spring-boot-starter-flyway` — auto-configures Flyway to run before JPA/Hibernate starts.

### Application Configuration

```yaml
spring:
  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: true
  jpa:
    hibernate:
      ddl-auto: none   # Flyway owns the schema — Hibernate must not touch it
```

| Property              | Purpose |
|-----------------------|---------|
| `enabled: true`       | Flyway runs on startup (default when the starter is present). |
| `locations`           | Where migration SQL files live. |
| `baseline-on-migrate` | If the DB already has tables but no `flyway_schema_history`, Flyway creates a baseline instead of failing. Useful when adopting Flyway on an existing database. |
| `ddl-auto: none`      | Prevents Hibernate from generating DDL. Flyway is the single source of truth for schema changes. |

### Test Configuration

```yaml
spring:
  flyway:
    enabled: true
    locations: classpath:db/migration
    clean-disabled: false     # allows Flyway.clean() in tests — never set in production
  jpa:
    hibernate:
      ddl-auto: none
```

### Migration Files

Migrations live in `src/main/resources/db/migration/` and follow the naming convention:

```
V<version>__<description>.sql
```

- `V` — prefix indicating a versioned migration.
- `<version>` — numeric (e.g. `1`, `2`, `3`). Must be unique and increasing.
- `__` — double underscore separator.
- `<description>` — human-readable name (underscores become spaces in the history table).

#### V1 — Initial Schema

```sql
-- V1__init_schema.sql
CREATE TABLE library_event (
    library_event_id SERIAL PRIMARY KEY,
    event_type       VARCHAR(255) NOT NULL
);

CREATE TABLE book (
    book_id          INTEGER      PRIMARY KEY,
    book_name        VARCHAR(255) NOT NULL,
    book_author      VARCHAR(255) NOT NULL,
    library_event_id INTEGER,
    CONSTRAINT fk_book_library_event
        FOREIGN KEY (library_event_id)
        REFERENCES library_event (library_event_id)
);
```

Creates the two core tables with their PK and FK relationship.

#### V2 — Add Audit Columns

```sql
-- V2__add_audit_columns.sql
ALTER TABLE library_event
    ADD COLUMN created_at  TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN updated_at  TIMESTAMP NOT NULL DEFAULT now();

ALTER TABLE book
    ADD COLUMN created_at  TIMESTAMP NOT NULL DEFAULT now(),
    ADD COLUMN updated_at  TIMESTAMP NOT NULL DEFAULT now();
```

Adds `created_at` and `updated_at` to both tables. The `DEFAULT now()` ensures existing
rows get a sensible value at the time of migration.

---

## How Flyway Executes on Startup

```
Application starts
  → Spring Boot auto-configures Flyway (before JPA EntityManagerFactory)
    → Flyway reads flyway_schema_history table
      → Determines which migrations are pending
        → Runs V1__init_schema.sql        (if not yet applied)
        → Runs V2__add_audit_columns.sql  (if not yet applied)
    → Flyway finishes
  → JPA/Hibernate starts with ddl-auto: none
  → Application is ready
```

Flyway runs *before* Hibernate. By the time JPA boots, the schema is already up to date.

---

## Adding a New Migration

1. **Create a new file** in `src/main/resources/db/migration/`:
   ```
   V3__add_isbn_to_book.sql
   ```
2. **Write the SQL**:
   ```sql
   ALTER TABLE book ADD COLUMN isbn VARCHAR(13);
   ```
3. **Update the JPA entity** to match (add the `isbn` field to `Book.java`).
4. **Commit both** the migration file and the entity change together — they are a pair.

> **Rule:** Never edit or delete an already-applied migration. Flyway checksums each
> file; if the checksum changes, the application refuses to start. Always create a
> new migration for corrections.

---

## Common Pitfalls

| Pitfall | Why It Happens | How to Avoid |
|---------|---------------|--------------|
| Editing an applied migration | Flyway detects checksum mismatch and fails on startup | Always add a new `V<n+1>__fix.sql` instead |
| Forgetting the double underscore | `V3_description.sql` is not recognized by Flyway | Always use `V3__description.sql` (two underscores) |
| Non-sequential version numbers with gaps | Not a problem — Flyway only cares about ordering, not contiguity | Gaps like V1, V2, V5 are fine |
| Running `Flyway.clean()` in production | Drops **all** objects in the schema | Keep `clean-disabled: true` (default) in production |

---

## Summary

| Aspect | Recommendation |
|--------|---------------|
| Schema ownership | Flyway — via versioned SQL migrations |
| JPA `ddl-auto` | `none` — always |
| Migration location | `src/main/resources/db/migration/` |
| Naming convention | `V<number>__<description>.sql` |
| Applied migration files | **Never edit or delete** — create a new migration instead |
| Test environments | Use Testcontainers + Flyway; optionally enable `clean-disabled: false` |
