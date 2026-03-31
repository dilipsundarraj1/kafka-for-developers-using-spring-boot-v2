# AGENTS.md vs SKILL.md — A Developer's Guide

---

## Table of Contents

- [Introduction](#introduction)
- [Open Format](#open-format)
- [What is AGENTS.md?](#what-is-agentsmd)
  - [What goes inside AGENTS.md?](#what-goes-inside-agentsmd)
  - [How does AGENTS.md work?](#how-does-agentsmd-work)
  - [Without AGENTS.md vs With AGENTS.md](#without-agentsmd-vs-with-agentsmd)
  - [Real-World Example](#real-world-example-agentsmd)
  - [Key Idea](#key-idea)
- [How to Generate AGENTS.md in GitHub Copilot](#how-to-generate-agentsmd-in-github-copilot)
  - [Step-by-Step](#step-by-step-agentsmd)
  - [What a Good AGENTS.md Looks Like](#what-a-good-agentsmd-looks-like)
  - [Tips](#tips-agentsmd)
- [What is SKILL.md?](#what-is-skillmd)
  - [What goes inside SKILL.md?](#what-goes-inside-skillmd)
  - [How does SKILL.md work?](#how-does-skillmd-work)
  - [Without SKILL.md vs With SKILL.md](#without-skillmd-vs-with-skillmd)
  - [Real-World Example](#real-world-example-skillmd)
  - [Where to place SKILL.md files?](#where-to-place-skillmd-files)
  - [Key Idea](#key-idea-1)
- [How to Generate SKILL.md in GitHub Copilot](#how-to-generate-skillmd-in-github-copilot)
  - [Step-by-Step](#step-by-step-skillmd)
  - [What a Good SKILL.md Looks Like](#what-a-good-skillmd-looks-like)
  - [Tips](#tips-skillmd)
- [AGENTS.md vs SKILL.md — Quick Comparison](#agentsmd-vs-skillmd--quick-comparison)
- [Full Flow](#full-flow)
- [When Things Go Wrong Without These Files](#when-things-go-wrong-without-these-files)
- [Rule of Thumb](#rule-of-thumb)

---

## Introduction

AI coding agents are only as good as the context you give them. Without proper context, they guess. They use generic patterns. They produce code that technically works but doesn't fit your project — wrong architecture, wrong annotations, wrong conventions, wrong test style.

`AGENTS.md` and `SKILL.md` are the two files that solve this problem. Together, they give the AI a complete picture of your project and eliminate guesswork entirely.

| File | Purpose | Scope | Loaded |
|------|---------|-------|--------|
| `AGENTS.md` | Project-wide context — who you are, what the project is, how to behave | Entire project | Always, automatically |
| `SKILL.md` | Task-specific playbook — exact steps, annotations, templates for one task | One task type | On demand |

Think of it this way:
- **AGENTS.md** is the onboarding document you'd give a new developer on their first day
- **SKILL.md** is the runbook you'd hand them when they need to do a specific task for the first time

### Open Format

Both `AGENTS.md` and `SKILL.md` are **plain Markdown files** — there is nothing proprietary about them. This is intentional and important.

| Property | What it means |
|----------|--------------|
| **Plain Markdown** | No special syntax, no tooling required — any text editor can read, write, and review them |
| **Vendor-neutral** | Not owned by any single AI vendor — GitHub Copilot, Claude Code, Cursor, and others all recognize the same conventions |
| **Version-controllable** | Checked into Git alongside your code — they evolve with the project and are visible in PRs, diffs, and history |
| **Human-readable** | A developer can read `AGENTS.md` and immediately understand how the project works — it is documentation for humans too, not just AI |
| **Portable** | Copy them to a new project as a starting point, share them across teams, or publish them as templates |

> Because the format is open, you are never locked in. The same files that guide GitHub Copilot today will guide any AI coding agent you adopt tomorrow.

---

## What is AGENTS.md?

`AGENTS.md` is a project-level context file that AI coding agents (GitHub Copilot, Claude Code, Cursor, and others) automatically load before they do any work. It establishes the AI's understanding of your project — the technology stack, architecture, coding conventions, build commands, and the rules it must always follow.

Think of it as an onboarding document written for the AI. Just as a new developer joining your team needs to understand the project before contributing, the AI needs the same orientation. The difference is: the AI reads `AGENTS.md` fresh at the start of every session.

Without `AGENTS.md`, every time you ask the AI to do something, it starts with no project knowledge. It makes decisions based on what it has seen in its training data — which may not match your specific project at all.

### What goes inside AGENTS.md?

A well-structured `AGENTS.md` covers six areas:

**1. Project Overview**
What does this project do? What problem does it solve? What is the tech stack?

```
This is a Spring Boot 4.0 Kafka consumer application.
It consumes library events from a Kafka topic and persists
them to a PostgreSQL database using Spring Data JPA.
```

**2. Architecture**
How is the code organized? What are the key layers and modules? What architectural pattern is followed?

```
Layered Architecture:
  - controller/   REST endpoints (Spring MVC)
  - service/      Business logic (event processing)
  - domain/       JPA entities (LibraryEvent, Book)
  - repository/   Spring Data JPA repositories
  - consumer/     Kafka listener (@KafkaListener)
```

**3. Coding Conventions**
Naming styles, patterns, things the AI should always do, things it should never do.

```
- Always use constructor injection (never field injection)
- Use records for DTOs
- Entities use IDENTITY generation strategy
- Never use Lombok
- Package names: com.learnkafka.*
```

**4. Build & Test Commands**
How to compile, run, and test the project.

```
./gradlew build           # Build
./gradlew test            # Run all tests (requires Docker)
./gradlew bootRun         # Run locally
```

**5. Key Dependencies**
What libraries are in use and at what version.

```
- Spring Boot 4.0.3
- Kafka (spring-kafka)
- Flyway (database migrations)
- Testcontainers 2.x (PostgreSQL in tests)
- Jackson 3.x (tools.jackson, NOT com.fasterxml.jackson)
```

**6. Rules the AI Must Always Follow**
Explicit constraints that prevent the AI from making wrong assumptions.

```
- Never use @Autowired field injection
- Never use com.fasterxml.jackson — use tools.jackson (Jackson 3.x)
- Never use @Testcontainers or @Container (Testcontainers 1.x)
- Always use @ImportTestcontainers (Spring Boot 4.0 / TC 2.x)
- Schema changes must go through Flyway migrations only
```

---

### How does AGENTS.md work?

When you make a request, the AI agent reads `AGENTS.md` first — before looking at any code. This gives it a complete orientation so every decision it makes is grounded in your project's reality.

```
Developer asks: "Add a new REST endpoint for orders"
          |
          v
AI reads AGENTS.md
          |
          v
AI now knows:
  - This is a Spring Boot 4.0 project
  - Uses Layered Architecture (controller → service → domain)
  - Always uses constructor injection
  - Uses Jackson 3.x (tools.jackson) for JSON
  - Uses Spring MVC (@RestController, @RequestMapping)
          |
          v
AI generates code that exactly fits your project structure,
annotations, and conventions — without being told explicitly
```

The AI does not need to guess what injection style you use, what Jackson version is in play, or how your packages are structured. It already knows because `AGENTS.md` told it.

---

### Without AGENTS.md vs With AGENTS.md

**Without AGENTS.md:**

```
Developer: "Write an integration test for the service layer"

AI generates:
  @ExtendWith(MockitoExtension.class)           ← wrong — project uses no mocks
  @Mock LibraryEventRepository repository;      ← wrong — project uses real DB
  @InjectMocks LibraryEventService service;     ← wrong — project uses constructor injection

  import com.fasterxml.jackson.databind...      ← wrong — project uses Jackson 3.x
```

The AI used valid Java — but it's completely wrong for this project. It took assumptions from its training data instead of your actual conventions.

**With AGENTS.md:**

```
Developer: "Write an integration test for the service layer"

AI generates:
  @SpringBootTest                               ← correct
  @ImportTestcontainers                         ← correct (Spring Boot 4.0 / TC 2.x)

  @ServiceConnection
  static PostgreSQLContainer<?> postgres = ...  ← correct

  import tools.jackson.databind.ObjectMapper;   ← correct (Jackson 3.x)
```

Same request. Completely different output. The only difference is `AGENTS.md`.

---

### Real-World Example (AGENTS.md)

Here is what a portion of a real `AGENTS.md` looks like for this Kafka consumer project:

```markdown
# AGENTS.md

## Project
Spring Boot 4.0 Kafka consumer. Consumes library events from topic
`library-events` and persists them to PostgreSQL via Spring Data JPA.

## Tech Stack
- Spring Boot 4.0.3
- Kafka (spring-kafka)
- PostgreSQL + Spring Data JPA
- Flyway (schema migrations)
- Testcontainers 2.x
- Jackson 3.x: tools.jackson (NOT com.fasterxml.jackson)

## Architecture
Layered: consumer → service → domain → repository

## Rules
- Constructor injection only. Never @Autowired on fields.
- Never use Lombok.
- Jackson: always import tools.jackson.*, never com.fasterxml.*
- Tests: @ImportTestcontainers + @ServiceConnection. Never @Testcontainers/@Container.
- Schema: Flyway migrations only. ddl-auto is none.
```

This is a compact but complete orientation document. Every line prevents a class of AI mistakes.

---

### Key Idea

> `AGENTS.md` does **not** give the AI step-by-step instructions for a task.
> It gives the AI **awareness** of the project so it never produces out-of-place code.

---

## How to Generate AGENTS.md in GitHub Copilot

Newer versions of GitHub Copilot Chat have a built-in option that generates `AGENTS.md` automatically by analyzing your entire codebase.

### Step-by-Step (AGENTS.md)

1. Open **GitHub Copilot Chat** in VS Code
2. Click the **sparkle / suggestions icon** at the top of the chat panel
3. Select the option: **"Generate Agent Instructions to onboard AI onto your codebase"**
4. Copilot scans your entire workspace — source files, `build.gradle`/`pom.xml`, configuration files, test structure — and generates a complete `AGENTS.md`
5. Review the output carefully — add any project-specific rules Copilot may have missed
6. Save the file as `AGENTS.md` in the **root of your project**

---

### What a Good AGENTS.md Looks Like

A good `AGENTS.md` is:

- **Concise** — not a novel. The AI reads it every session; keep it focused
- **Explicit about anti-patterns** — what to avoid is as important as what to do
- **Version-specific** — mention exact versions when they affect how the AI writes code (e.g., Jackson 3.x vs 2.x)
- **Architecture-aware** — describes how layers relate to each other
- **Rule-oriented** — clear, unambiguous statements the AI can follow directly

A poor `AGENTS.md` is vague: *"Follow best practices"* tells the AI nothing. A good one is specific: *"Never use field injection. Always use constructor injection."*

---

### Tips (AGENTS.md)

- Place `AGENTS.md` at the project root — all AI tools (Copilot, Claude Code, Cursor) automatically detect it there
- Treat it as a living document — update it when you adopt a new library, change your architecture, or establish a new convention
- Be explicit about things you want the AI to **never do** — these negative rules prevent the most common mistakes
- Review the generated output carefully — Copilot does a good job but may miss project-specific rules that only you know

---

## What is SKILL.md?

`SKILL.md` is a task-specific instruction file. While `AGENTS.md` gives the AI broad awareness of the project, `SKILL.md` gives it **precise, step-by-step execution guidance** for one specific task — like writing integration tests, creating a Flyway migration, adding a new REST endpoint, or setting up a Kafka consumer.

The problem `SKILL.md` solves is different from `AGENTS.md`. Even with full project context, when you ask the AI to "write an integration test," it still has to decide:
- Which annotations to combine?
- How to set up Testcontainers?
- What cleanup strategy to use?
- What assertion style to use?
- How to name the test methods?

Without explicit guidance, the AI makes plausible-looking choices that may not match your exact established patterns. `SKILL.md` eliminates this by giving the AI a concrete playbook for that specific task.

Think of it as the difference between knowing what city you're in (AGENTS.md) and having a turn-by-turn navigation guide to your destination (SKILL.md).

---

### What goes inside SKILL.md?

A `SKILL.md` for a specific task covers:

**1. Skill Metadata (frontmatter)**
```yaml
---
name: testing
description: Integration testing patterns for this Spring Boot Kafka consumer project
---
```

**2. Technology Stack for this Task**
Exact versions, package names, and what NOT to use:
```
| Component       | Version | Notes                                      |
|-----------------|---------|---------------------------------------------|
| JUnit Jupiter   | 6.x     | Via spring-boot-starter-test                |
| Testcontainers  | 2.x     | Use @ImportTestcontainers + @ServiceConnection |
| EmbeddedKafka   | managed | @EmbeddedKafka from spring-kafka-test       |
| Jackson         | 3.x     | tools.jackson.databind (NOT com.fasterxml)  |
```

**3. Required Dependencies**
Exact Gradle/Maven entries so the AI doesn't guess:
```groovy
testImplementation 'org.springframework.boot:spring-boot-starter-webmvc-test'
testImplementation 'org.springframework.boot:spring-boot-testcontainers'
testImplementation 'org.testcontainers:testcontainers-postgresql'
```

**4. Test Configuration**
What `application.yml` settings are needed in tests:
```yaml
server.port: 0
spring.flyway.clean-disabled: false
spring.jpa.hibernate.ddl-auto: none
```

**5. Exact Annotation Combinations**
The precise annotations and their order for each test category:
```java
// Kafka consumer integration test:
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"library-events"},
    bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers")
@TestPropertySource(properties = {"spring.kafka.consumer.auto-offset-reset=earliest"})
@ImportTestcontainers

// Service integration test:
@SpringBootTest
@ImportTestcontainers

// REST controller integration test:
@SpringBootTest
@AutoConfigureMockMvc
@ImportTestcontainers
```

**6. Code Templates**
Copy-paste-ready test templates derived from your actual code:
```java
@Test
void processEvent_ADD_shouldPersistLibraryEventAndBook() {
    // given
    BookDto bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
    LibraryEventDto dto = new LibraryEventDto(null, LibraryEventType.ADD, bookDto);
    ConsumerRecord<Integer, LibraryEventDto> record = buildConsumerRecord(null, dto);

    // when
    libraryEventService.processEvent(record);

    // then
    assertEquals(1, libraryEventRepository.count());
    assertEquals(1, bookRepository.count());
}
```

**7. Conventions and Rules**
Explicit dos and don'ts:
```
- Naming: {methodUnderTest}_{scenario}_{expectedBehavior}()
- Cleanup: @BeforeEach (not @AfterEach) — delete child before parent due to FK
- Assertions: JUnit Jupiter only — no AssertJ, no Hamcrest
- No mocks — all tests use real infrastructure
```

**8. How to Run**
```bash
./gradlew test                                      # All tests
./gradlew test --tests "*.BookControllerIntegrationTest"  # One class
```

---

### How does SKILL.md work?

SKILL.md is loaded **on demand** when you tell the AI to follow a specific skill. The AI combines the project context from `AGENTS.md` with the task-specific playbook from `SKILL.md` to produce output that precisely matches your established patterns.

```
Developer asks: "Write an integration test for the processEvent ADD scenario"
          |
          v
AI loads AGENTS.md (project context)
          |
          v
  - Spring Boot 4.0 project
  - Layered architecture
  - Constructor injection
  - Jackson 3.x
          |
          v
AI loads testing/SKILL.md (task playbook)
          |
          v
  - Use @SpringBootTest + @ImportTestcontainers
  - Use @ServiceConnection on static PostgreSQLContainer
  - No mocks — real Testcontainers PostgreSQL
  - JUnit Jupiter assertions only
  - Method naming: method_scenario_expectedBehavior()
  - @BeforeEach cleanup: child before parent (FK order)
          |
          v
AI generates a test that exactly matches your project's
established patterns — no guessing, no style drift
```

---

### Without SKILL.md vs With SKILL.md

**Without SKILL.md:**

Even with `AGENTS.md` loaded, the AI still has to choose between multiple valid patterns:

```java
// AI might generate this (valid but wrong for this project):
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = AppConfig.class)
@Testcontainers                                    ← wrong (TC 1.x style)
class LibraryEventServiceTest {

    @Container
    static PostgreSQLContainer<?> postgres = ...   ← wrong

    @Autowired
    LibraryEventService service;

    @AfterEach                                     ← wrong (project uses @BeforeEach)
    void cleanup() { repository.deleteAll(); }

    @Test
    void testProcessEvent() {                      ← wrong naming convention
        assertThat(result).isEqualTo(...);         ← wrong (project uses JUnit Jupiter, not AssertJ)
    }
}
```

This compiles. It may even pass. But it uses different patterns than every other test in the codebase.

**With SKILL.md:**

```java
// AI generates this — exactly matching your established pattern:
@SpringBootTest
@ImportTestcontainers                              ← correct (TC 2.x / Spring Boot 4.0)
class LibraryEventServiceIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = ...   ← correct

    @BeforeEach                                    ← correct
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
    }

    @Test
    void processEvent_ADD_shouldPersistLibraryEventAndBook() {  ← correct naming
        // ...
        assertEquals(1, libraryEventRepository.count());        ← correct (JUnit Jupiter)
    }
}
```

Consistent with every other test in the project. No style drift. No wrong annotations.

---

### Real-World Example (SKILL.md)

Here is what the frontmatter and opening of a real `SKILL.md` looks like for this project:

```markdown
---
name: testing
description: Integration testing patterns for this Spring Boot 4.0 Kafka consumer
             project using JUnit Jupiter, Testcontainers 2.x, EmbeddedKafka, MockMvc
---

## About this skill

No mocks — every test runs against real infrastructure (EmbeddedKafka,
Testcontainers PostgreSQL).

## Technology Stack

| Component      | Version | Notes                                         |
|----------------|---------|-----------------------------------------------|
| JUnit Jupiter  | 6.x     | Via spring-boot-starter-test (Spring Boot 4.0)|
| Testcontainers | 2.x     | @ImportTestcontainers + @ServiceConnection    |
| EmbeddedKafka  | managed | @EmbeddedKafka from spring-kafka-test         |
| MockMvc        | managed | AutoConfigureMockMvc (Spring Boot 4.0 package)|
| Jackson        | 3.x     | tools.jackson.databind.ObjectMapper           |
```

Every line eliminates a category of AI mistakes.

---

### Where to place SKILL.md files?

SKILL.md files live inside a `.github/skills/` folder at the project root, organized by task type. This location is a convention that AI tools recognize.

```
.github/
  skills/
    testing-skills/
      SKILL.md       ← How to write integration tests in this project
    db-skills/
      SKILL.md       ← How to create Flyway migrations in this project
    feature-skills/
      SKILL.md       ← How to add a new feature end-to-end
    kafka-skills/
      SKILL.md       ← How to add a new Kafka consumer or producer
```

Each skill is self-contained. When you need to write a test, you load `testing-skills/SKILL.md`. When you need a migration, you load `db-skills/SKILL.md`. Only the relevant skill is loaded, keeping the AI's context focused.

---

### Key Idea

> `SKILL.md` does not replace `AGENTS.md` — it extends it.
> `AGENTS.md` gives the AI awareness of the project.
> `SKILL.md` gives it a precise playbook for a specific task.
> Together, they eliminate both strategic and tactical guesswork.

---

## How to Generate SKILL.md in GitHub Copilot

GitHub Copilot can analyze your existing code and capture your patterns into a `SKILL.md` automatically. The key is to point it at **actual examples** from your codebase — not describe what you want in the abstract.

### Step-by-Step (SKILL.md)

1. Open **GitHub Copilot Chat** in VS Code
2. Use the `@workspace` agent so Copilot can access your codebase
3. Identify an existing file or folder that contains the pattern you want to capture
4. Use a prompt like this — tailored to the task you want to encode:

**For testing skills:**
```
@workspace Look at all the integration tests in
src/test/java/com/learnkafka/ and generate a SKILL.md file
that captures our exact testing patterns.

Include:
- Frontmatter with name and description
- Technology stack table with exact versions and package names
- Required Gradle test dependencies (exact artifact IDs)
- Test configuration from src/test/resources/application.yml
- Exact annotation combinations for each test category
  (consumer tests, service tests, controller tests)
- Code templates based on existing tests
- Naming conventions for test classes and methods
- Data cleanup strategy (@BeforeEach order, FK constraints)
- Assertion style (JUnit Jupiter only — no AssertJ, no Hamcrest)
- Anti-patterns to avoid (what NOT to use)
- How to run the tests
```

**For database migration skills:**
```
@workspace Look at src/main/resources/db/migration/ and generate
a SKILL.md for creating new Flyway migrations in this project.

Include:
- File naming convention
- Version numbering strategy
- How DDL-auto is configured and why
- Example migration templates
- What NOT to do (common Flyway mistakes)
```

5. Review the generated output for accuracy — Copilot reads your code but you know your project best
6. Save it as `SKILL.md` inside `.github/skills/<task-name>/`

---

### What a Good SKILL.md Looks Like

A good `SKILL.md` is:

- **Task-scoped** — one skill per file, one task type per skill
- **Template-driven** — includes copy-paste-ready code snippets from your actual codebase
- **Anti-pattern explicit** — states what NOT to do, with reasons
- **Version-precise** — exact package names and import paths, not just library names
- **Self-contained** — someone (or an AI) reading it alone should be able to complete the task correctly

A poor `SKILL.md` says: *"Write tests using Testcontainers."*
A good `SKILL.md` says: *"Use `@ImportTestcontainers` (NOT `@Testcontainers`). Declare a `static PostgreSQLContainer<?>` annotated with `@ServiceConnection` (NOT `@DynamicPropertySource`). Use `@BeforeEach` for cleanup, deleting child entities before parent due to FK constraints."*

---

### Tips (SKILL.md)

- Create one `SKILL.md` per task type — don't mix testing and migration patterns in the same file
- Point Copilot at **existing code**, not at a description — it captures real patterns better from examples
- Be explicit about version-specific imports — this is where the most common AI mistakes happen
- Encode anti-patterns explicitly — if your team has been burned by a particular mistake, document it
- Update `SKILL.md` whenever your patterns evolve — stale skills produce stale code

---

## AGENTS.md vs SKILL.md — Quick Comparison

| | AGENTS.md | SKILL.md |
|---|---|---|
| What is it? | Project-wide context and rules | Task-specific execution playbook |
| Analogy | Developer onboarding document | Step-by-step task runbook |
| Scope | Entire project | One specific task type |
| When loaded? | Always, automatically | On demand, for a specific task |
| Who writes it? | Once (AI-generated + reviewed) | Once per task type (AI-generated + reviewed) |
| What it prevents | Wrong architecture, wrong conventions, wrong libraries | Wrong annotations, wrong patterns, style drift |
| Without it | AI guesses project structure and conventions | AI guesses task-specific patterns |
| With it | AI generates code that fits the project | AI generates code that matches your exact established patterns |
| Update frequency | When project conventions change | When task patterns change |
| Location | Project root (`AGENTS.md`) | `.github/skills/<task>/SKILL.md` |

---

## Full Flow

```
Developer makes a request
          |
          v
  AGENTS.md is loaded (always — every session)
          |
          v
  AI understands the full project context:
    - Tech stack and versions
    - Architecture and layers
    - Coding conventions
    - What to never do
          |
          +---------------------------------------+
          |                                       |
          v                                       v
   General Task                           Specific Task
  (e.g., explain code,               (e.g., write integration test,
   fix a bug, refactor)               add Flyway migration,
          |                            add new REST endpoint)
          |                                       |
          v                                       v
  AI uses project context              SKILL.md is loaded
  to execute directly                  (task-specific playbook)
                                               |
                                               v
                                 AI now has both:
                                   - Project context (AGENTS.md)
                                   - Task playbook (SKILL.md)
                                               |
                                               v
                                 AI follows step-by-step guide:
                                   - Correct annotations
                                   - Correct imports
                                   - Correct naming
                                   - Correct cleanup strategy
                                               |
                                               v
                                 Code matches your exact
                                 established patterns precisely
```

---

## When Things Go Wrong Without These Files

Here is a concrete summary of what AI agents get wrong without `AGENTS.md` and `SKILL.md`, and what they get right with them:

| Scenario | Without | With |
|----------|---------|------|
| Writing a test | Uses Mockito, `@ExtendWith(MockitoExtension.class)` | Uses `@SpringBootTest` + `@ImportTestcontainers` |
| Testcontainers setup | `@Testcontainers` + `@Container` (TC 1.x) | `@ImportTestcontainers` + `@ServiceConnection` (TC 2.x) |
| Jackson import | `com.fasterxml.jackson.databind.ObjectMapper` | `tools.jackson.databind.ObjectMapper` |
| Test cleanup | `@AfterEach` — misses failures | `@BeforeEach` — always clean |
| Test naming | `testProcessEvent()` | `processEvent_ADD_shouldPersist...()` |
| Injection style | `@Autowired` field injection | Constructor injection |
| Schema changes | Suggests `ddl-auto: update` | Creates a new Flyway migration |

Every row in this table is a real category of mistake that AI agents make without proper context files. Each one is prevented by `AGENTS.md` or `SKILL.md`.

---

## Rule of Thumb

- **AGENTS.md** → Gives the AI awareness of your project (the "what" and "who")
- **SKILL.md** → Gives the AI a precise playbook for a task (the "how")

Neither file replaces the other. `AGENTS.md` without `SKILL.md` means the AI knows your project but still guesses on specific tasks. `SKILL.md` without `AGENTS.md` means the AI has a task playbook but no project context to anchor it.

Together → **Reliable, consistent, pattern-matched AI-assisted development — every time.**

---

## References

- [AgentSkills.io](https://agentskills.io/home) — Official resource for AGENTS.md and SKILL.md patterns
