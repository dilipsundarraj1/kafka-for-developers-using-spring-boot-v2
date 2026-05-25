# JUnit 5 vs JUnit 6 — Comparison

## Table of Contents

1. [At a Glance](#at-a-glance)
2. [Key Differences](#key-differences)
   - [Java Version Requirement](#1-java-version-requirement)
   - [Unified Module Versioning](#2-unified-module-versioning)
   - [Null-Safety Annotations](#3-null-safety-annotations)
   - [Native Kotlin Coroutine Support](#4-native-kotlin-coroutine-support)
   - [Fail-Fast Execution](#5-fail-fast-execution)
   - [Improved Parameterized Tests](#6-improved-parameterized-tests)
   - [CSV Parsing — FastCSV](#7-csv-parsing--fastcsv)
3. [Breaking Changes](#breaking-changes)
4. [Should You Upgrade?](#should-you-upgrade)

---

## At a Glance

| | JUnit 5 | JUnit 6 |
|---|---|---|
| **Latest version** | 5.14.3 | 6.0.3 |
| **Min Java version** | Java 8 | **Java 17** |
| **GA release** | 2017 | Sep 30, 2025 |
| **Module versioning** | Platform/Jupiter/Vintage versioned separately | All modules share one version number |

---

## Key Differences

### 1. Java Version Requirement

JUnit 6 drops Java 8/11 support entirely. **Java 17 is the minimum**, enabling modern language features like records, sealed classes, and pattern matching in tests.

### 2. Unified Module Versioning

In JUnit 5, the Platform, Jupiter, and Vintage modules had independent version numbers, which caused confusion in dependency management. JUnit 6 aligns all modules under a single version.

```xml
<!-- JUnit 5 — confusing split versions -->
<dependency>
    <groupId>org.junit.platform</groupId>
    <artifactId>junit-platform-launcher</artifactId>
    <version>1.11.3</version>
</dependency>
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>5.11.3</version>
</dependency>

<!-- JUnit 6 — unified version -->
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>6.0.3</version>
</dependency>
```

### 3. Null-Safety Annotations

JUnit 6 adopts **JSpecify** annotations (`@Nullable`, `@NonNull`, `@NullMarked`) across all APIs — better static analysis and Kotlin interop out of the box.

### 4. Native Kotlin Coroutine Support

JUnit 5 required `runBlocking` wrappers for coroutine-based tests. JUnit 6 supports `suspend` test methods natively.

```kotlin
// JUnit 5 — workaround needed
@Test
fun testSomething() = runBlocking {
    val result = myService.fetchAsync()
    assertEquals("expected", result)
}

// JUnit 6 — native suspend support
@Test
suspend fun testSomething() {
    val result = myService.fetchAsync()
    assertEquals("expected", result)
}
```

### 5. Fail-Fast Execution

JUnit 6 introduces a `CancellationToken` API — test runs can terminate early on first failure, saving time in large test suites.

### 6. Improved Parameterized Tests

`@ParameterizedClass` gains new lifecycle callbacks:

```java
// JUnit 6
@ParameterizedClass
@MethodSource("provideBooks")
class LibraryEventParameterizedTest {

    @BeforeParameterizedClassInvocation
    static void setup(String bookTitle) {
        // runs before each parameterized invocation
    }

    @Test
    void testBookEvent(String bookTitle) {
        assertNotNull(bookTitle);
    }
}
```

### 7. CSV Parsing — FastCSV

`@CsvSource` and `@CsvFileSource` now use **FastCSV** instead of the unmaintained `univocity-parsers` library.

---

## Breaking Changes

| Removed in JUnit 6 | Alternative |
|---|---|
| `junit-platform-runner` | Use `junit-platform-launcher` directly |
| `junit-platform-jfr` | Functionality merged into `junit-platform-launcher` |
| `junit-platform-suite-commons` | Merged into `junit-platform-suite` |
| `junit-jupiter-migrationsupport` | Deprecated — migrate to JUnit 5 annotations |

---

## Should You Upgrade?

| Scenario | Recommendation |
|---|---|
| New project, Java 17+ | Start with JUnit 6 |
| Existing Spring Boot project | Wait for Spring Boot official JUnit 6 support |
| Kotlin-heavy project | JUnit 6 offers clear wins with coroutine support |
| Java 8/11 project | Stay on JUnit 5 — JUnit 6 won't run |

---

## Sources

- [What's changed between JUnit 5 and JUnit 6 – Test Automation Zone](https://testautomationzone.com/whats-changed-between-junit-5-and-junit-6/)
- [JUnit 5 is dead, long live JUnit 6! – Medium](https://medium.com/javarevisited/junit-5-is-dead-long-live-junit-6-e142806c11a6)
- [What's new in JUnit 6: Key Changes and Improvements – Medium](https://medium.com/javarevisited/whats-new-in-junit-6-key-changes-and-improvements-551a84d7ed1f)
- [JUnit 6 Release Notes](https://docs.junit.org/6.0.0/release-notes.html)
- [JUnit through the years – Medium](https://medium.com/@kaustubh.saha/junit-through-the-years-from-legacy-junit-3-4-to-modern-junit-5-and-junit-6-ecosystem-0f9cb55010dd)
