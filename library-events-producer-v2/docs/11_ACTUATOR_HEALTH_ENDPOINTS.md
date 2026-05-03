# Actuator Health Endpoints — Liveness and Readiness

<!-- TOC -->
* [Actuator Health Endpoints — Liveness and Readiness](#actuator-health-endpoints--liveness-and-readiness)
  * [Overview](#overview)
  * [Step 1: Add the Dependency](#step-1-add-the-dependency)
  * [Step 2: Enable Health Endpoints in application.yml](#step-2-enable-health-endpoints-in-applicationyml)
  * [Liveness and Readiness Checks](#liveness-and-readiness-checks)
    * [Liveness](#liveness)
    * [Readiness](#readiness)
  * [KafkaReadinessHealthIndicator](#kafkareadinesshealthindicator)
    * [Why It Matters](#why-it-matters)
    * [The Implementation](#the-implementation)
    * [Wiring It into the Readiness Group](#wiring-it-into-the-readiness-group)
  * [Available Endpoints](#available-endpoints)
  * [Checking the Endpoints](#checking-the-endpoints)
  * [How Kubernetes Uses These Endpoints](#how-kubernetes-uses-these-endpoints)
<!-- TOC -->

---

## Overview

When an application runs in production, the platform managing it — whether that is Kubernetes, Docker Swarm, or a cloud service — needs a reliable way to ask two questions at any point in time:

- Is this instance healthy enough to keep running?
- Is this instance ready to handle incoming requests?

Without answers to these questions, the platform has no choice but to blindly route traffic to every running instance, even ones that are broken or waiting on a dependency. The result is failed requests, degraded user experience, and harder-to-diagnose outages.

Spring Boot Actuator solves this by exposing dedicated HTTP health endpoints that the platform can probe on a schedule. For this app specifically, the stakes are higher than a typical REST service — **every request results in a Kafka message being produced**. If the Kafka broker is unreachable, the app cannot do its job at all. An unhealthy Kafka connection should immediately stop traffic from being routed to the instance, not silently fail requests.

By enabling Actuator and the `KafkaReadinessHealthIndicator` in this app, we get:

- **Automatic traffic management** — the platform stops routing to an instance the moment Kafka becomes unreachable, and resumes automatically once it recovers.
- **No unnecessary restarts** — a Kafka outage does not restart the pod; it just temporarily removes it from rotation.
- **Faster incident response** — health endpoint details expose exactly which dependency is down and why, without needing to dig through logs.
- **Production-grade observability** — the same endpoints used by Kubernetes probes can be called manually during debugging or by monitoring tools.

---

## Step 1: Add the Dependency

Add the Actuator starter to `build.gradle`:

```groovy
implementation 'org.springframework.boot:spring-boot-starter-actuator'
```

This pulls in the `/actuator/health` endpoint and all the built-in health indicators. No extra code is needed to get a basic health check — configuration alone drives the rest.

---

## Step 2: Enable Health Endpoints in application.yml

Add the following to `application.yml`:

```yaml
management:
  endpoint:
    health:
      probes:
        enabled: true               # enables /actuator/health/liveness and /actuator/health/readiness
        add-additional-paths: true  # also exposes /livez and /readyz at the root
      show-details: always
      group:
        liveness:
          include: livenessState
        readiness:
          include: readinessState,kafkaReadiness  # readiness fails if Kafka is unreachable
  health:
    livenessstate:
      enabled: true
    readinessstate:
      enabled: true
  endpoints:
    web:
      exposure:
        include: health,info        # only health and info are exposed over HTTP
```

Key settings:
- `probes.enabled: true` — activates the dedicated `/liveness` and `/readiness` sub-paths.
- `add-additional-paths: true` — registers `/livez` and `/readyz` at the root as shorthand aliases, which some platforms prefer.
- `show-details: always` — returns the full breakdown of each health component in the response body.
- Only `health` and `info` are exposed — all other actuator endpoints remain off over HTTP.

---

## Liveness and Readiness Checks

### Liveness

> "Is the app alive and not stuck in a broken state?"

The liveness probe checks whether the application process itself is healthy. It only includes `livenessState`, which Spring Boot manages internally. It fails if the app enters a state it cannot recover from on its own (e.g., a deadlock or a fatal startup error).

**When it fails:** the platform restarts the container.

### Readiness

> "Is the app ready to accept traffic right now?"

The readiness probe checks whether the app and all its required dependencies are available. It includes both `readinessState` and `kafkaReadiness`. If Kafka is down, the readiness probe fails even though the app process itself is fine.

**When it fails:** the platform stops routing traffic to this instance — no restart, no dropped requests.

The distinction matters: a Kafka outage should not cause a container restart. It should cause traffic to be redirected until Kafka recovers, and the instance should come back into rotation automatically once it does.

---

## KafkaReadinessHealthIndicator

### Why It Matters

Spring Boot's built-in health indicators do not check Kafka broker availability out of the box in a way that is useful for a readiness probe. Without a custom indicator:

- The app could report `UP` while Kafka is completely unreachable.
- Requests would be routed to the instance, fail to produce events, and surface as errors to callers.

The `KafkaReadinessHealthIndicator` closes this gap. It actively connects to the Kafka cluster and reports `DOWN` if no brokers are reachable, giving the platform a reliable signal to stop traffic until the broker recovers.

### The Implementation

The indicator is registered as a Spring component named `kafkaReadiness`:

```java
@Component("kafkaReadiness")
public class KafkaReadinessHealthIndicator implements HealthIndicator {

    private final String bootstrapServers;

    public KafkaReadinessHealthIndicator(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public Health health() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(2).toMillis());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(2).toMillis());
        config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                Duration.ofSeconds(1).toMillis());

        try (AdminClient adminClient = AdminClient.create(config)) {
            int brokerCount = adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS).size();
            if (brokerCount == 0) {
                return Health.down()
                        .withDetail("bootstrapServers", bootstrapServers)
                        .withDetail("reason", "No brokers available")
                        .build();
            }
            return Health.up()
                    .withDetail("bootstrapServers", bootstrapServers)
                    .withDetail("brokerCount", brokerCount)
                    .build();
        } catch (Exception ex) {
            return Health.down(ex)
                    .withDetail("bootstrapServers", bootstrapServers)
                    .build();
        }
    }
}
```

What it does:
- Creates a short-lived `AdminClient` with tight timeouts (2 s max) so health checks never block for long.
- Calls `describeCluster()` to count available brokers.
- Returns `UP` with the broker count when at least one broker is reachable.
- Returns `DOWN` with the reason when the broker count is zero or the connection fails.

### Wiring It into the Readiness Group

The component name `kafkaReadiness` (from `@Component("kafkaReadiness")`) maps directly to the `include` list in `application.yml`:

```yaml
group:
  readiness:
    include: readinessState,kafkaReadiness
```

Spring Boot looks up the health contributor by that name and rolls it into the readiness response automatically. No additional wiring is needed.

---

## Available Endpoints

| Endpoint | Purpose |
|---|---|
| `/actuator/health` | Overall health (all indicators combined) |
| `/actuator/health/liveness` | Liveness state only |
| `/actuator/health/readiness` | Readiness state — includes Kafka check |
| `/livez` | Shorthand alias for liveness |
| `/readyz` | Shorthand alias for readiness |

---

## Checking the Endpoints

**Overall health:**

```bash
curl http://localhost:8080/actuator/health
```

```json
{
  "status": "UP",
  "components": {
    "livenessState":  { "status": "UP" },
    "readinessState": { "status": "UP" },
    "kafkaReadiness": {
      "status": "UP",
      "details": {
        "bootstrapServers": "localhost:9092",
        "brokerCount": 1
      }
    }
  }
}
```

**Liveness:**

```bash
curl http://localhost:8080/livez
```

```json
{ "status": "UP" }
```

**Readiness (Kafka reachable):**

```bash
curl http://localhost:8080/readyz
```

```json
{ "status": "UP" }
```

**Readiness (Kafka unreachable):**

```json
{
  "status": "OUT_OF_SERVICE",
  "components": {
    "kafkaReadiness": {
      "status": "DOWN",
      "details": {
        "bootstrapServers": "localhost:9092"
      }
    }
  }
}
```

HTTP status will be `503 Service Unavailable`.

---

## How Kubernetes Uses These Endpoints

```yaml
livenessProbe:
  httpGet:
    path: /livez
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 15
  periodSeconds: 10
```

- `/livez` fails → Kubernetes **restarts** the pod.
- `/readyz` fails → Kubernetes **removes the pod from the service endpoint list** until it recovers. No restart, no lost messages.
