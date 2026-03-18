# Spec-Driven Development

## Table of Contents

1. [The Problem with Jumping Straight to Code](#the-problem-with-jumping-straight-to-code)
2. [What is Spec-Driven Development?](#what-is-spec-driven-development)
3. [What is a PRD?](#what-is-a-prd)
4. [PRD Structure — The Template](#prd-structure--the-template)
5. [Why Non-Goals Matter](#why-non-goals-matter)
6. [The API Contract — Most Critical Section](#the-api-contract--most-critical-section)
7. [Writing a PRD](#writing-a-prd)
8. [PRD Output Walkthrough](#prd-output-walkthrough)
   - [Overview and Goals](#overview-and-goals)
   - [Functional Requirements](#functional-requirements)
   - [Error Handling](#error-handling)
9. [From PRD to Implementation](#from-prd-to-implementation)
10. [The PRD as a Living Document](#the-prd-as-a-living-document)
11. [Benefits Summary](#benefits-summary)
12. [Common PRD Mistakes](#common-prd-mistakes)
13. [Summary](#summary)

---

## The Problem with Jumping Straight to Code

Most developers receive a requirement — *"build a Kafka producer for library events"* — and immediately open their IDE. They write code based on assumptions. Then they hit edge cases they didn't think about. Validation rules don't match what the business actually needs. Three-quarters through the implementation, they discover a fundamental requirement was misunderstood.

This is compounded when working with AI coding assistants. **Vague prompt → vague code.** The AI fills in assumptions on your behalf, and those assumptions may be wrong.

```
The Cycle Without a Spec:

  Vague requirement
       ↓
  Write code (based on assumptions)
       ↓
  Hit edge cases → rewrite
       ↓
  Fix bugs from unclear requirements
       ↓
  Repeat
```

The root cause is always the same: **the requirements were never fully defined before coding began.**

---

## What is Spec-Driven Development?

Spec-Driven Development (SDD) is the practice of writing a complete specification **before** writing any code.

The specification is a structured document that captures:

- **What** the system does (functional requirements)
- **What** the system does NOT do (non-goals)
- Inputs, outputs, and validation rules
- Error handling strategies
- Acceptance criteria that define "done"

```
The Spec-Driven Workflow:

  Write PRD
     ↓
  Generate Implementation Plan
     ↓
  Write Code (layer by layer)
     ↓
  Write Tests (derived from acceptance criteria)
     ↓
  Done — verifiably
```

Code is **derived from** the spec. Not the other way around. The spec is the single source of truth throughout the entire development effort.

---

## What is a PRD?

A **Product Requirements Document (PRD)** is the most widely used form of specification in software development. It operates at the requirements level — it defines the problem space, not the solution space.

A PRD answers five questions:

| Question | Purpose |
|----------|---------|
| **WHAT** are we building? | Defines scope |
| **WHO** is it for? | Defines personas and use cases |
| **WHY** are we building it? | Defines goals and motivation |
| **WHAT** are the rules and constraints? | Defines validation, error handling, data contracts |
| **HOW** do we know when we're done? | Defines acceptance criteria |

> **Important distinction:** A PRD is not a design document (it doesn't say *how* to implement something) and it's not a technical spec (it doesn't prescribe architecture). It's a contract between the problem and the solution.

---

## PRD Structure — The Template

```
PRD Sections

 1. Overview / Product Summary
 2. Goals
 3. Non-Goals  ← explicit out-of-scope
 4. Personas & Use Cases
 5. Functional Requirements
      ├── API endpoints (HTTP method, path, response codes)
      ├── Request/Response contracts (JSON schemas)
      ├── Validation rules
      └── Business logic rules
 6. Data Requirements (entities, DB schema)
 7. Non-Functional Requirements
      └── Performance, reliability, observability
 8. Error Handling Strategies
 9. Acceptance Criteria
10. Open Questions / Dependencies
```

Every section serves a purpose. Skipping sections is where bugs are born.

---

## Why Non-Goals Matter

Non-Goals are the most overlooked section of a PRD — and one of the most valuable.

Without explicit non-goals, **everything is potentially in scope.** Someone will always ask "can we also add X?" and without a documented boundary, that's a reasonable question that can drag the project sideways.

**Example — Library Events Producer PRD:**

```
Non-Goals (explicit out-of-scope):

  ❌  Consuming or processing Kafka events
  ❌  Persistent storage of events
  ❌  Authentication / authorization
  ❌  Advanced workflow orchestration
```

When this PRD is used as input, the non-goals act as guardrails. The AI coding assistant won't add database repositories or authentication filters because the spec explicitly excludes them. The more precise the spec, the more precise the generated code.

---

## The API Contract — Most Critical Section

For a REST API service, the Functional Requirements section is where bugs are prevented or created. Every validation rule you write here is a bug you won't have to chase down in production.

**What to specify exactly:**

- HTTP method and path
- Required vs optional fields
- Data types and constraints
- Success response (status code + body shape)
- Error responses (every status code + meaning)

**Example from the Library Events Producer PRD:**

```
POST /v1/library-events

  Success:
    201 Created — event body echoed back

  Errors:
    400 — missing required fields
    400 — libraryEventType is not ADD
    500 — Kafka publish failed after retry exhaustion
```

These four response scenarios directly drove the test cases for this endpoint. The unit tests, integration tests — they all map to these lines in the PRD. When code is generated from a spec this precise, it produces the correct validation annotations, correct response codes, and correct exception handlers.

---

## Writing a PRD

The key is to work at the requirements level first — not the implementation level.

**Example description to start from:**

```
"I need to build a Spring Boot REST API that acts as a
 Kafka producer for library events. The API should accept
 book additions and updates, validate the payload, and
 publish to Kafka. Please create a PRD document for this
 feature following our standard PRD template."
```

Notice what this description does **not** say: it doesn't mention `KafkaTemplate`, `@RestController`, `application.yml`, or any implementation detail. The PRD process surfaces the requirements. The implementation decisions come later.

**The workflow:**

```
1. Start from the project directory
2. Describe the feature in plain English
3. Draft the PRD using the standard template
4. Review and refine collaboratively
5. Save the final PRD to docs/
```

---

## PRD Output Walkthrough

### Overview and Goals

```markdown
## 1. Overview
Library Events Producer API — REST endpoints to publish library
events (book additions and updates) to Kafka topic `library-events`.

## 2. Goals
- Enable clients to publish via HTTP POST and PUT
- Validate event payloads before publishing
- Support ADD and UPDATE event types
- Provide deterministic API responses and clear error handling

## 3. Non-Goals
- No event consumption or processing
- No persistent storage of events
- No authentication / authorization

## 4. Personas
- Library Management System → sends book creation/update events
- Admin / Batch Tool → sends bulk updates for existing records
```

Two distinct users (personas) were identified by the spec process — before a single line of code was written. These personas drive the test scenarios later.

---

### Functional Requirements

```markdown
## 5. Functional Requirements

### API Endpoints
  POST /v1/library-events  → libraryEventType must be ADD
  PUT  /v1/library-events  → libraryEventId is required

### Request Body
  {
    "libraryEventId": 123,
    "libraryEventType": "ADD",
    "book": {
      "bookId": 456,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }

### Validation Rules
  • libraryEventType required (ADD or UPDATE)
  • book object required
  • bookId, bookName, bookAuthor all required
  • libraryEventId required for PUT requests
  • libraryEventType must be ADD for POST requests
```

These validation rules translate directly to Bean Validation annotations in Java (`@NotNull`, `@NotBlank`, custom validators). Each rule is a test case. Nothing is left to interpretation.

---

### Error Handling

```markdown
## 8. Error Handling Strategies

  1. Validation Failure
     → 400 Bad Request
     → Response body contains field-level error descriptions

  2. Business Rule Violation
     → 400 Bad Request
     → Example: PUT received with null libraryEventId

  3. Kafka Publish Failure
     → Retry up to N attempts (configurable)
     → 500 Internal Server Error after retry exhaustion

  Observability:
     → Log success: topic, partition, offset
     → Log failure: exception type and message
```

Error handling captured in the spec means error handling in the code is not an afterthought — it's a first-class requirement.

---

## From PRD to Implementation

Once the PRD is reviewed and agreed upon, the next step is generating an **Implementation Plan**.

**Prompt:**
```
"Given this PRD, create a step-by-step implementation plan
 for a Spring Boot application."
```

The implementation plan identifies:

- Every file to create (controllers, services, DTOs, config)
- Every dependency to add (`spring-kafka`, `spring-boot-starter-validation`)
- The build order (domain → service → controller → tests)
- Test cases derived directly from acceptance criteria

```
Implementation Order (derived from PRD):

  1. Domain layer    — LibraryEvent, Book, EventType
  2. DTO layer       — request/response shapes from Section 5.2
  3. Service layer   — Kafka publish logic from Section 5.4
  4. Controller layer — endpoints from Section 5.1
  5. Error handling  — ControllerAdvice from Section 8
  6. Tests           — unit + integration from Section 9
```

At every step, the PRD is the reference. If there is ambiguity about what a method should return, the PRD answers it.

---

## The PRD as a Living Document

PRDs are not set in stone. They evolve as understanding grows.

**The rule:** when requirements change, update the PRD **first**, then the code, then the tests.

This order matters. If you update the code first, the PRD becomes stale documentation. If you update the PRD first, you are forced to think through the implications of the change before writing a single line of code.

```
Requirement Change Workflow:

  New or changed requirement
        ↓
  Update PRD (section X.Y)
        ↓
  Update the implementation to match
        ↓
  Update tests
```

Common reasons a PRD evolves during development:

- Open Questions get answered → PRD updated
- An edge case is discovered → a validation rule is added
- Stakeholder feedback → a non-goal becomes a goal

---

## Benefits Summary

### For the Developer

| Benefit | Why |
|---------|-----|
| Clear scope | You know exactly what you're building |
| Test cases pre-defined | Acceptance criteria → test cases |
| Fewer surprises | Edge cases caught in spec phase (cheap) |
| Faster code reviews | Reviewer checks code against PRD |

### For AI-Assisted Development

| Benefit | Why |
|---------|-----|
| Precise output | Precise spec → precise code |
| Correct annotations | Validation rules → `@NotBlank`, `@NotNull`, etc. |
| Correct response codes | Error strategies → HTTP status codes |
| Useful tests | Acceptance criteria → generated test cases |

### For the Team

| Benefit | Why |
|---------|-----|
| Shared understanding | Alignment before a line is written |
| Stakeholder alignment | Scope and non-goals visible to all |
| Traceability | Every feature tied to a requirement |

---

## Common PRD Mistakes

**Vague validation rules**
```
❌  "bookName should be valid"
✅  "bookName must be non-blank and not exceed 200 characters"
```

**Missing Non-Goals**
Leaving scope ambiguous invites scope creep. If it is not explicitly excluded, it is implicitly included.

**No acceptance criteria**
Without acceptance criteria, "done" is a feeling, not a verifiable state.

**Describing implementation, not requirements**
```
❌  "Use a KafkaTemplate to publish the event"   ← HOW
✅  "Publish the event to topic `library-events`" ← WHAT
```

The PRD defines WHAT the system does. The implementation plan defines HOW.

**Skipping error scenarios**
Happy path specs produce happy path code. Real users hit error paths constantly.

**Not updating the PRD when requirements change**
A stale PRD is worse than no PRD — it actively misleads the people reading it.

---

## Summary

Spec-Driven Development is the practice of writing a PRD **before** writing code. The PRD captures scope, validation rules, data contracts, error handling strategies, and acceptance criteria. It becomes the single source of truth for the entire development effort.

```
The Three-Step Workflow:

  Step 1 — Write the PRD
    • Scope, non-goals, validation rules,
      error handling, acceptance criteria

  Step 2 — Generate an Implementation Plan
    • The PRD drives the build plan

  Step 3 — Implement from the Plan
    • Domain → Service → Controller → Tests
    • Every decision traced back to the PRD
```

The PRD we built for the Library Events Producer is at:

```
library-events-producer-v2/docs/1_PRD.md
```

The implementation plan derived from it is at:

```
library-events-producer-v2/docs/3_IMPLEMENTATION_PLAN_README.md
```

Everything in the codebase was derived from the PRD. Nothing was added that wasn't in the spec. Nothing was missed because it was captured in the spec before coding began.

> *"Write the spec first. Let the code follow."*
