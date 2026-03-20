# AGENTS.md vs SKILL.md — A Developer's Guide

---

## Table of Contents

- [What is AGENTS.md?](#what-is-agentsmd)
  - [What goes inside AGENTS.md?](#what-goes-inside-agentsmd)
  - [How does AGENTS.md work?](#how-does-agentsmd-work)
  - [Key Idea](#key-idea)
- [How to Generate AGENTS.md in GitHub Copilot](#how-to-generate-agentsmd-in-github-copilot)
- [What is SKILL.md?](#what-is-skillmd)
  - [What goes inside SKILL.md?](#what-goes-inside-skillmd)
  - [How does SKILL.md work?](#how-does-skillmd-work)
  - [Where to place SKILL.md files?](#where-to-place-skillmd-files)
  - [Key Idea](#key-idea-1)
- [How to Generate SKILL.md in GitHub Copilot](#how-to-generate-skillmd-in-github-copilot)
- [AGENTS.md vs SKILL.md — Quick Comparison](#agentsmd-vs-skillmd--quick-comparison)
- [Full Flow](#full-flow)
- [Rule of Thumb](#rule-of-thumb)

---

## What is AGENTS.md?

`AGENTS.md` is a project-level context file that AI coding agents (like GitHub Copilot, Claude Code, Cursor, etc.) automatically load before doing any work. It tells the AI **who you are**, **what your project is**, and **how it should behave** while assisting you.

Think of it as an onboarding document written for the AI — just like you'd onboard a new developer joining your team.

### What goes inside AGENTS.md?

- **Project overview** — What does this project do? What is the tech stack?
- **Architecture** — How is the code organized? What are the key modules?
- **Coding conventions** — Naming styles, patterns, what to avoid
- **Build & test commands** — How to compile, run, and test the project
- **Key rules** — Any constraints the AI must always follow (e.g., "never use raw SQL", "always use constructor injection")

### How does AGENTS.md work?

When you ask the AI agent to perform any task, it first reads `AGENTS.md` to load the full context of your project. This prevents it from making assumptions or producing code that doesn't fit your project's style and structure.

```
Developer asks: "Add a new REST endpoint for orders"
          |
          v
AI reads AGENTS.md
          |
          v
AI now knows:
  - This is a Spring Boot project
  - Uses Hexagonal Architecture
  - Follows REST naming conventions
  - Uses constructor injection
          |
          v
AI generates code that fits your project
```

### Key Idea

> AGENTS.md does **not** give step-by-step instructions for a task.
> It gives the AI **awareness** of the project so it never produces out-of-place code.

---

## How to Generate AGENTS.md in GitHub Copilot

Newer versions of GitHub Copilot Chat have a built-in option that does this for you automatically.

### Step-by-Step

1. Open **GitHub Copilot Chat** in VS Code
2. Click the **sparkle / suggestions icon** at the top of the chat panel
3. Select the option: **"Generate Agent Instructions to onboard AI onto your codebase"**
4. Copilot analyzes your entire workspace and generates a complete `AGENTS.md`
5. Review the output and save it as `AGENTS.md` in the **root of your project**

### Tips

- Place `AGENTS.md` at the project root so all AI tools automatically pick it up
- Keep it updated as your project evolves
- Be explicit about things you want the AI to **never do** (e.g., "do not use field injection", "do not generate Lombok @Data on entities")

---

## What is SKILL.md?

`SKILL.md` is a task-specific instruction file. While `AGENTS.md` gives the AI broad project context, `SKILL.md` gives the AI **precise, step-by-step guidance** for a specific task — like writing integration tests, setting up database migrations, or adding a new feature following your exact pattern.

Think of it as a recipe: follow these exact steps to complete this one task correctly.

### What goes inside SKILL.md?

- **Exact annotations** to use (e.g., `@SpringBootTest`, `@ImportTestcontainers`)
- **Technology stack details** — versions, package names, what to avoid
- **Code templates** — real, copy-paste-ready examples in your project's style
- **Step-by-step instructions** for the task
- **Edge cases and conventions** — what to do and what NOT to do
- **How to run** — commands to execute the task

### How does SKILL.md work?

SKILL.md is loaded **on demand** when you ask the AI to perform a specific task. Instead of the AI guessing how you write tests or migrations, it follows your exact playbook.

```
Developer asks: "Write an integration test for the processEvent ADD scenario"
          |
          v
AI loads AGENTS.md (project context)
          |
          v
AI loads testing/SKILL.md (task-specific guide)
          |
          v
AI now knows:
  - Use @SpringBootTest + @ImportTestcontainers
  - Use @ServiceConnection on static PostgreSQLContainer
  - No mocks — real Testcontainers PostgreSQL
  - Use JUnit Jupiter assertions only
  - Test method naming: method_scenario_expectedBehavior()
          |
          v
AI generates a test that exactly matches your project's pattern
```

### Where to place SKILL.md files?

SKILL.md files live inside a `.github/skills/` folder, organized by task type:

```
.github/
  skills/
    testing-skills/
      SKILL.md       ← How to write integration tests
    db-skills/
      SKILL.md       ← How to write Flyway migrations
    feature-skills/
      SKILL.md       ← How to add a new feature end-to-end
```

This keeps skills organized and allows you to load only what's needed for a given task.

### Key Idea

> SKILL.md **removes guesswork** from repetitive or complex tasks by giving the AI a precise, project-specific playbook to follow every time.

---

## How to Generate SKILL.md in GitHub Copilot

GitHub Copilot can analyze your existing code and generate a SKILL.md for any task pattern it finds.

### Step-by-Step

1. Open **GitHub Copilot Chat** in VS Code
2. Use the `@workspace` agent to give Copilot access to your codebase
3. Point Copilot at an existing file that represents the pattern you want to capture
4. Use a prompt like:

```
@workspace Look at the integration tests in src/test/java/com/learnkafka/
and generate a SKILL.md file for writing new integration tests in this project.

Include:
- What annotations to use and why
- Technology stack with exact versions and package names
- Step-by-step instructions for writing a new test
- Code templates based on the existing tests
- Key conventions (naming, data cleanup, assertions)
- What NOT to do (common mistakes to avoid)
- How to run the tests
```

5. Review and save the output as `SKILL.md` inside `.github/skills/<task-name>/`

### Tips

- Create one SKILL.md per task type — testing, migrations, REST endpoints, etc.
- Reference existing code files so Copilot captures your **actual** patterns, not generic ones
- Be explicit about anti-patterns (e.g., "no Mockito", "no `@DynamicPropertySource`")
- Update SKILL.md whenever your patterns evolve — it's a living document

---

## AGENTS.md vs SKILL.md — Quick Comparison

| | AGENTS.md | SKILL.md |
|---|---|---|
| What is it? | Project-wide context guide | Task-specific instruction guide |
| Analogy | City Map | Cooking Recipe |
| Scope | Entire project | One specific task |
| When used? | Always loaded automatically | Loaded on demand for a specific task |
| Who writes it? | Developer (or AI-generated once) | Developer for each repeatable task |
| Purpose | Prevent wrong assumptions | Eliminate guesswork on execution |

---

## Full Flow

```
Developer makes a request
          |
          v
  AGENTS.md is loaded (always)
          |
          v
  AI understands project context:
  architecture, conventions, rules
          |
          +---------------------------+
          |                           |
          v                           v
   General Task                 Specific Task
  (e.g., explain code)      (e.g., write integration test)
          |                           |
          v                           v
  AI executes directly         SKILL.md is loaded
                                      |
                                      v
                          AI follows step-by-step guide
                                      |
                                      v
                          Code matches your exact pattern
```

---

## Rule of Thumb

- **AGENTS.md** → Understands your project
- **SKILL.md** → Executes a task precisely

Together → **Reliable, consistent, AI-assisted development**
