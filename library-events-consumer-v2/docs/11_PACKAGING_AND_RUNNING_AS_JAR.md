# Packaging and Running the App as a JAR File

<!-- TOC -->
* [Packaging and Running the App as a JAR File](#packaging-and-running-the-app-as-a-jar-file)
  * [Step 1: Build the JAR](#step-1-build-the-jar)
    * [Skip Tests (optional but faster)](#skip-tests-optional-but-faster)
    * [Where Is the JAR?](#where-is-the-jar)
  * [Step 2: Run the JAR](#step-2-run-the-jar)
    * [Default Profile](#default-profile)
    * [Override the Active Profile](#override-the-active-profile)
    * [Override Any Property at Runtime](#override-any-property-at-runtime)
  * [Configuring the App with Environment Variables](#configuring-the-app-with-environment-variables)
  * [Step 3: Verify the App Is Running](#step-3-verify-the-app-is-running)
  * [Benefits of Packaging as a JAR](#benefits-of-packaging-as-a-jar)
    * [1. Environment Independence](#1-environment-independence)
    * [2. No IDE Required on Target Machines](#2-no-ide-required-on-target-machines)
    * [3. Consistent, Reproducible Builds](#3-consistent-reproducible-builds)
    * [4. Easy Profile and Property Overrides](#4-easy-profile-and-property-overrides)
    * [5. Ready for CI/CD Pipelines](#5-ready-for-cicd-pipelines)
    * [6. Foundation for Docker Images](#6-foundation-for-docker-images)
    * [7. Actuator and Health Checks Work Out of the Box](#7-actuator-and-health-checks-work-out-of-the-box)
  * [IntelliJ vs. JAR — Side-by-Side Comparison](#intellij-vs-jar--side-by-side-comparison)
  * [Cleaning Up Build Artifacts](#cleaning-up-build-artifacts)
  * [Gradle Tasks Quick Reference](#gradle-tasks-quick-reference)
<!-- TOC -->

---

## Step 1: Build the JAR

Run the following command from the project root (same directory as `build.gradle`):

```bash
./gradlew build
```

### Skip Tests (optional but faster)

```bash
./gradlew build -x test
```

### Where Is the JAR?

After a successful build the JAR lands at:

```
build/libs/library-events-consumer-v2-0.0.1-SNAPSHOT.jar
```

You can confirm it exists:

```bash
ls -lh build/libs/
```

Expected output:

```
-rw-r--r--  1 user  staff  XX MB  library-events-consumer-v2-0.0.1-SNAPSHOT.jar
```

---

## Step 2: Run the JAR

Make sure **Kafka** and **PostgreSQL** are running (e.g., via Docker Compose) before starting the app.

### Default Profile

```bash
java -jar build/libs/library-events-consumer-v2-0.0.1-SNAPSHOT.jar
```

This picks up `application.yml` (the default profile).

### Override the Active Profile

**Local:**

```bash
java -jar build/libs/library-events-consumer-v2-0.0.1-SNAPSHOT.jar \
  --spring.profiles.active=local
```

**Stage:**

```bash
java -jar build/libs/library-events-consumer-v2-0.0.1-SNAPSHOT.jar \
  --spring.profiles.active=stage
```

**Prod:**

```bash
java -jar build/libs/library-events-consumer-v2-0.0.1-SNAPSHOT.jar \
  --spring.profiles.active=prod
```

Each activates the corresponding `application-{profile}.yml` on top of the base `application.yml`.

### Override Any Property at Runtime

Spring Boot property resolution means command-line arguments always win over `application.yml`:

```bash
java -jar build/libs/library-events-consumer-v2-0.0.1-SNAPSHOT.jar \
  --spring.profiles.active=local \
  --server.port=8081 \
  --spring.kafka.consumer.bootstrap-servers=localhost:9092 \
  --spring.kafka.consumer.group-id=library-events-listener-group32433 \
  --spring.datasource.url=jdbc:postgresql://localhost:5432/mydatabase
```

---

## Configuring the App with Environment Variables

Instead of hardcoding values in `application.yml` or passing command-line arguments, you can set environment variables before launching the JAR. Spring Boot automatically maps uppercase, underscore-separated env var names to their corresponding properties.

| Environment Variable              | Maps To                                   | Default                                       |
|-----------------------------------|-------------------------------------------|-----------------------------------------------|
| `SPRING_PROFILES_ACTIVE`          | `spring.profiles.active`                  | _(none)_                                      |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS`  | `spring.kafka.consumer.bootstrap-servers` | `localhost:9092`                              |
| `SPRING_KAFKA_CONSUMER_GROUP_ID`  | `spring.kafka.consumer.group-id`          | `library-events-listener-group32433`          |
| `SPRING_DATASOURCE_URL`           | `spring.datasource.url`                   | `jdbc:postgresql://localhost:5432/mydatabase` |
| `SPRING_DATASOURCE_USERNAME`      | `spring.datasource.username`              | `myuser`                                      |
| `SPRING_DATASOURCE_PASSWORD`      | `spring.datasource.password`              | `secret`                                      |

```bash
SPRING_PROFILES_ACTIVE=local \
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
SPRING_KAFKA_CONSUMER_GROUP_ID=library-events-listener-group32433 \
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/mydatabase \
SPRING_DATASOURCE_USERNAME=myuser \
SPRING_DATASOURCE_PASSWORD=secret \
java -jar build/libs/library-events-consumer-v2-0.0.1-SNAPSHOT.jar
```

> **Why env vars instead of command-line args?**
> Environment variables are the standard way to configure apps in Docker and Kubernetes. A container runtime (or a `docker run -e` flag) injects them at startup — no need to modify the `java -jar` command itself.

---

## Step 3: Verify the App Is Running

Once you see the Spring Boot banner and the log line `Started LibraryEventsConsumerV2Application`, the app is up and listening on Kafka.

Check the health endpoints:

```bash
# General health
curl http://localhost:8081/actuator/health

# Liveness probe
curl http://localhost:8081/livez

# Readiness probe (includes Kafka readiness)
curl http://localhost:8081/readyz
```

Expected:

```json
{"status":"UP"}
```

Access the Swagger UI to explore the consumer's REST API:

```
http://localhost:8081/swagger-ui/index.html
```

---

## Benefits of Packaging as a JAR

### 1. Environment Independence

The JAR bundles every dependency. As long as the target machine has the right Java version (Java 25 in this project), the app runs identically whether you are on macOS, Linux, or Windows — no "works on my machine" surprises.

### 2. No IDE Required on Target Machines

Servers, CI agents, and containers do not have IntelliJ installed. A JAR is the standard, self-contained deployment unit for JVM applications.

### 3. Consistent, Reproducible Builds

`./gradlew build` always produces the same artifact from the same source tree. The Gradle build cache and lock files (if used) make builds deterministic.

### 4. Easy Profile and Property Overrides

Spring Boot's externalized configuration hierarchy lets ops teams change Kafka brokers, ports, log levels, and credentials without touching or rebuilding the source code:

```
Command-line args > Environment variables > application-{profile}.yml > application.yml
```

This separation of code from configuration is a core [12-Factor App](https://12factor.net/config) principle.

### 5. Ready for CI/CD Pipelines

A CI pipeline (GitHub Actions, Jenkins, etc.) can:

1. Run `./gradlew build` — compiles, tests, and packages.
2. Archive `build/libs/*.jar` as a pipeline artifact.
3. Ship the artifact to staging/production without any further build steps.

### 6. Foundation for Docker Images

The JAR is what goes inside a Docker image. The `Dockerfile` already present in this project (`Dockerfile`) copies this JAR and runs it with `java -jar`. Building and running the JAR locally first is the easiest way to confirm the app works before layering in Docker.

### 7. Actuator and Health Checks Work Out of the Box

`spring-boot-starter-actuator` is already a dependency. When the app runs as a JAR, `/actuator/health`, `/livez`, and `/readyz` are available immediately — no IDE plugin needed. These are the same endpoints Kubernetes liveness/readiness probes call.

---

## IntelliJ vs. JAR — Side-by-Side Comparison

| Scenario | IntelliJ Run | `java -jar` |
|---|---|---|
| Development / rapid iteration | Best choice | Overhead of rebuild |
| Sharing with teammates | They need your IDE setup | Hand them the JAR |
| Running on a Linux server | Not possible | `java -jar app.jar` |
| Deploying to Docker | Not applicable | `COPY app.jar` + `java -jar` |
| Running in Kubernetes | Not applicable | Container entry point |
| Profile / env-var overrides | IDE run config | CLI args or env vars |
| Attaching a debugger | Built-in | `java -agentlib:jdwp=...` |

---

## Cleaning Up Build Artifacts

To remove the `build/` directory and start fresh:

```bash
./gradlew clean
```

To clean and rebuild in one step:

```bash
./gradlew clean build
```

---

## Gradle Tasks Quick Reference

| Task | What it does |
|---|---|
| `./gradlew build` | Compiles, runs all tests, then packages the JAR |
| `./gradlew build -x test` | Compiles and packages, skipping tests |
| `./gradlew clean` | Deletes the `build/` directory |
| `./gradlew clean build` | Clean rebuild of the JAR |
| `./gradlew bootRun` | Runs the app via Gradle (similar to IntelliJ run, no JAR produced) |
| `./gradlew tasks` | Lists all available Gradle tasks |
