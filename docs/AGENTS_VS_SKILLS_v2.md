# AGENTS.md vs SKILL.md — What's the Difference?

## Quick Summary

| | `AGENTS.md` | `.github/skills/*/SKILL.md` |
|---|---|---|
| **Role** | Project-wide instruction manual | Task-specific playbook |
| **Analogy** | The "map" of the codebase | A "recipe" for one type of task |
| **Scope** | Broad — covers everything | Narrow and deep — one domain only |
| **Loaded when?** | Always — every agent reads it first | On demand — only when the task matches the skill |
| **Location** | Project root (`AGENTS.md`) | `.github/skills/<skill-name>/SKILL.md` |

---

## `AGENTS.md` — The Global Context File

**Purpose:** Give any AI agent (or human) a fast orientation to the *entire* project so it can work on *any* task without violating conventions.

**What it contains:**

- **Project overview** — "This is a Spring Boot 4.0 Kafka consumer that persists to PostgreSQL."
- **Architecture & data flow** — The call chain from `@KafkaListener` → Service → Mapper → Repository.
- **Key design decisions** — DTO/Entity separation, bidirectional OneToOne mapping, manual offset commit, Kafka type remapping.
- **Build & run commands** — `./gradlew build`, `docker compose up`, etc.
- **Coding conventions** — No Lombok, constructor injection, SLF4J logging, Flyway-only schema changes.
- **Testing patterns** — Brief bullet points (e.g., "use MockMvc", "delete child FK first") — just enough to avoid mistakes.

**Characteristics:**

- ~55 lines. Concise and scannable.
- Answers: *"What is this project and what rules do I follow?"*
- Does **not** contain copy-paste templates or step-by-step instructions.

---

## `SKILL.md` — A Task-Specific Deep Dive

**Purpose:** Give an AI agent everything it needs to **execute one specific type of task** correctly — with exact annotations, imports, templates, and edge-case warnings.

**What it contains (using the testing skill as example):**

- **Full technology stack table** — Exact versions and package paths (e.g., `tools.jackson.databind.ObjectMapper`, not `com.fasterxml`).
- **Test dependencies** — The exact `build.gradle` lines needed.
- **Test configuration** — What goes in `src/test/resources/application.yml` and why.
- **Three detailed test category breakdowns**, each with:
  - Exact class-level annotations to use.
  - Key patterns explained (e.g., how to build a `ConsumerRecord` manually).
  - Helper method code blocks.
  - Copy-paste test method templates.
- **Conventions** — Data cleanup order, naming conventions, assertion library choice, Jackson version warnings.
- **Run commands** — How to execute specific tests.

**Characteristics:**

- ~250 lines. Exhaustive and prescriptive.
- Answers: *"How exactly do I write/modify a test in this project?"*
- Contains **ready-to-use code templates** and **gotcha warnings**.

---

## When Each File Is Used

```
User asks: "Add a retry mechanism to the Kafka consumer"
  → Agent reads AGENTS.md
  → Understands architecture, conventions, ack mode
  → Makes the change following project rules

User asks: "Write integration tests for the new retry logic"
  → Agent reads AGENTS.md (for overall context)
  → Agent reads .github/skills/testing-skills/SKILL.md (for exact test patterns)
  → Writes tests using the correct annotations, helpers, and templates

User asks: "Add a new column to the Book table"
  → Agent reads AGENTS.md (for overall context)
  → Agent reads .github/skills/db-skills/SKILL.md (for Flyway migration patterns)
  → Creates a versioned migration file
```

---

## Side-by-Side: How Testing Is Covered

| Aspect | `AGENTS.md` | `testing-skills/SKILL.md` |
|---|---|---|
| Mentions MockMvc? | ✅ One bullet point | ✅ Full template with `jsonPath` assertions |
| Lists exact annotations? | ❌ | ✅ `@SpringBootTest`, `@EmbeddedKafka`, `@ImportTestcontainers`, etc. |
| Shows import paths? | Mentions `org.springframework.boot.webmvc.test.autoconfigure` | ✅ All imports for all test types |
| Explains Testcontainers setup? | Says "Testcontainers auto-start PostgreSQL" | ✅ Full `@ServiceConnection` pattern with warnings about TC 1.x vs 2.x |
| Provides test method templates? | ❌ | ✅ Three categories, each with copy-paste code |
| Explains `ConsumerRecord` helper? | References the method name | ✅ Full implementation with parameter docs |
| Covers data cleanup order? | ✅ One line: "delete bookRepository first" | ✅ Explained with rationale (FK constraints, `@BeforeEach` not `@AfterEach`) |
| Covers Jackson version? | ❌ | ✅ Explicit: `tools.jackson.databind.ObjectMapper` not `com.fasterxml` |
| Naming conventions? | ❌ | ✅ `{method}_{scenario}_{expected}()` pattern |

---

## Rule of Thumb

- **`AGENTS.md`** = "What do I need to know to work on this project?" (read always)
- **`SKILL.md`** = "What do I need to know to do *this specific thing* well?" (read when relevant)

