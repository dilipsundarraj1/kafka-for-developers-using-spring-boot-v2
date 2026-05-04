# Actuator Health Endpoints — Liveness and Readiness

<!-- TOC -->
* [Actuator Health Endpoints — Liveness and Readiness](#actuator-health-endpoints--liveness-and-readiness)
  * [Overview](#overview)
  * [The Production Problem](#the-production-problem)
  * [Why Blind Traffic Routing Is Dangerous](#why-blind-traffic-routing-is-dangerous)
  * [How Spring Boot Actuator Helps](#how-spring-boot-actuator-helps)
  * [Why This Matters More for a Kafka Consumer](#why-this-matters-more-for-a-kafka-consumer)
  * [Benefits of Health Checks](#benefits-of-health-checks)
    * [Automatic Traffic Management](#automatic-traffic-management)
    * [No Unnecessary Restarts](#no-unnecessary-restarts)
    * [Faster Incident Response](#faster-incident-response)
    * [Production-Grade Observability](#production-grade-observability)
  * [Kubernetes Context](#kubernetes-context)
  * [What We Will Cover](#what-we-will-cover)
  * [Step 1: Add the Dependency](#step-1-add-the-dependency)
  * [Step 2: Enable Health Endpoints in application.yml](#step-2-enable-health-endpoints-in-applicationyml)
  * [Liveness Check](#liveness-check)
  * [Readiness Check](#readiness-check)
    * [KafkaReadinessHealthIndicator](#kafkareadinesshealthindicator)
    * [The Implementation](#the-implementation)
    * [Wiring It into the Readiness Group](#wiring-it-into-the-readiness-group)
  * [Available Endpoints](#available-endpoints)
  * [Checking the Endpoints](#checking-the-endpoints)
<!-- TOC -->

---

## Overview

So far, we have been focusing on building the application, consuming messages from Kafka, testing the consumer flow, and making sure our API works as expected.

But when this application runs in production (AWS or GKE or Kubernetes), there is one more very important question we need to answer.

- How does the platform know whether this application is actually healthy?
- How does the platform know whether this application is ready to consume traffic?

This is where **Spring Boot Actuator health checks** become extremely useful.

---

## The Production Problem

When an application runs in production, it is usually managed by a platform like **Kubernetes**, **AWS ECS**, or **Azure Container Apps**. These platforms constantly monitor the running instances of our application and need reliable answers to two questions:

- **Is this application instance healthy enough to keep running?**
- **Is this application instance ready to handle incoming requests?**

These two questions may sound similar, but they are not the same. An application may be running but not ready to handle traffic.

For example — the Spring Boot process may be up, the embedded Tomcat server may be running, and `/actuator/health` may respond. But if Kafka is down, this consumer application cannot successfully consume library events. From a production standpoint, this instance should not receive traffic until Kafka becomes available again.

---

## Why Blind Traffic Routing Is Dangerous

Without health checks, the platform only knows that the container or process is running. It has no clear signal about the real state of the application, so it continues routing requests to every running instance — even broken ones.

```
                        ┌──────────────────────┐
       HTTP Request     │                      │
  Client ─────────────▶ │    Load Balancer     │
                        │                      │
                        └──────────┬───────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │  No health signal — routes   │
                    │  traffic to ALL instances    │
                    └──────────────┬──────────────┘
                                   │
               ┌───────────────────┴────────────────────┐
               ▼                                         ▼
  ┌────────────────────────┐           ┌────────────────────────┐
  │      Instance 1        │           │      Instance 2        │
  │   Process : UP  ✓      │           │   Process : UP  ✓      │
  │   Kafka   : UP  ✓      │           │   Kafka   : DOWN  ✗    │
  └────────────┬───────────┘           └────────────┬───────────┘
               │                                    │
               ▼                                    ▼
  ┌────────────────────────┐           ┌────────────────────────┐
  │    Kafka Broker        │           │    Kafka Broker        │
  │    Reachable  ✓        │           │    Unreachable  ✗      │
  │    Message published   │           │    Publish FAILED      │
  └────────────────────────┘           └────────────┬───────────┘
                                                    │
                                                    ▼
                                       ┌────────────────────────┐
                                       │   500 Error returned   │
                                       │   to Client  ✗         │
                                       └────────────────────────┘
```

This creates serious problems:

- Users send requests, but the application fails while trying to consume from Kafka.
- API requests fail because the consumer cannot access the Kafka topic.
- Outages become harder to diagnose because everything looks like the application is running, while an important dependency is not available.

This is exactly the kind of production issue we want to avoid.

---

## How Spring Boot Actuator Helps

Spring Boot Actuator solves this by exposing dedicated HTTP endpoints that report the real health of the application. These endpoints can be polled by any deployment platform on a regular schedule.

Instead of guessing, the platform can call the health endpoint and make a decision based on the response:

- Is the application alive?
- Is the application ready to receive traffic?

With the right configuration, important dependencies like Kafka can be included as part of the readiness check — meaning the application can clearly communicate its actual state to the platform.

---

## Why This Matters More for a Kafka Consumer

For this specific application, health checks are even more critical than for a typical REST service.

**Why?** Because the consumer continuously listens to the Kafka topic for incoming events. When a library event is published to Kafka, the consumer processes that message and persists it to the database.

Kafka is not an optional dependency here. **Kafka is part of the main business flow.**

If Kafka is unreachable, this application cannot do its job correctly. We should not allow traffic to continue flowing to this instance when Kafka is unavailable. Instead, the platform should temporarily stop routing requests to this instance until Kafka becomes available again.

---

## Benefits of Health Checks

### Automatic Traffic Management

When Kafka becomes unreachable, the readiness endpoint reports that the application is not ready. The platform then removes that instance from the traffic rotation — users are no longer routed to an instance that cannot consume messages. When Kafka recovers, the readiness endpoint becomes healthy again and the platform automatically resumes sending traffic to that instance.

### No Unnecessary Restarts

A Kafka outage should not restart the application. If the application process itself is broken, a restart makes sense. But if Kafka is temporarily unavailable, restarting the container will not fix Kafka — the application may be perfectly fine.

Instead of restarting unnecessarily, the platform removes the instance from rotation temporarily and brings it back once Kafka recovers. This is why readiness checks are so useful: they separate **application failure** from **dependency unavailability**.

### Faster Incident Response

When something goes wrong in production, health endpoints give an immediate, clear signal about which dependency is causing the problem — without digging through log lines.

Instead of "The application is not working," the team can say: "The application is running, but Kafka is currently unavailable." That is a much clearer and more actionable signal for developers and operations teams.

### Production-Grade Observability

These health endpoints are not just for automated probes. They can be called manually during debugging and integrated with monitoring tools, dashboards, and alerting systems — making them part of the overall observability strategy for the application.

---

## Kubernetes Context

In this course, we will be deploying and running this application in **Kubernetes**. Kubernetes relies heavily on health checks to manage the lifecycle of every running pod:

- **Liveness probes** — Kubernetes uses these to decide whether the pod should keep running or be restarted.
- **Readiness probes** — Kubernetes uses these to decide whether the pod should receive traffic.

The work we do in this section is not just for local testing. It is directly connected to how our application will behave in a real production environment on Kubernetes.

---

## What We Will Cover

In the following sections we will:

1. Enable Spring Boot Actuator in the Library Events Consumer application.
2. Expose and configure the liveness and readiness health endpoints.
3. Build and wire in the `KafkaReadinessHealthIndicator` so that Kafka availability is part of the readiness state.

Once this is done, the application will be able to clearly signal to the platform whether it is ready to accept traffic — giving us a much more production-ready Kafka consumer.

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
  health:
    livenessstate:
      enabled: true
  endpoints:
    web:
      exposure:
        include: health,info      # only health and info are exposed over HTTP
```

Key settings:
- `probes.enabled: true` — activates the dedicated `/liveness` and `/readiness` sub-paths.
- `add-additional-paths: true` — registers `/livez` and `/readyz` at the root as shorthand aliases, which some platforms prefer.
- `show-details: always` — returns the full breakdown of each health component in the response body.
- `group.readiness.include` — includes both `readinessState` and `kafkaReadiness` so Kafka availability is part of the readiness check.
- `group.liveness.include` — includes only `livenessState` for the liveness probe.
- Only `health` and `info` are exposed — all other actuator endpoints remain off over HTTP.

---

## Liveness Check

> "Is the app alive and not stuck in a broken state?"

The liveness probe checks whether the application process itself is healthy. It only includes `livenessState`, which Spring Boot manages internally. It fails if the app enters a state it cannot recover from on its own (e.g., a deadlock or a fatal startup error).

**When it fails:** the platform restarts the container.

---

## Readiness Check

> "Is the app ready to accept traffic right now?"

The readiness probe checks whether the app and all its required dependencies are available. Unlike liveness, readiness is about external conditions — if a dependency the app relies on is down, the app should stop accepting traffic until that dependency recovers.

**When it fails:** the platform stops routing traffic to this instance — no restart, no dropped requests.

This distinction matters: a Kafka outage should not cause a container restart. It should temporarily remove the instance from rotation, and bring it back automatically once Kafka recovers.

### KafkaReadinessHealthIndicator

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
            @Value("${spring.kafka.consumer.bootstrap-servers}") String bootstrapServers) {
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

Now that the indicator exists, update `application.yml` to add the readiness group and include `kafkaReadiness` in it. The component name `kafkaReadiness` (from `@Component("kafkaReadiness")`) maps directly to the `include` list:

```yaml
management:
  endpoint:
    health:
      group:
        readiness:
          include: readinessState,kafkaReadiness
  health:
    readinessstate:
      enabled: true
```

Spring Boot looks up the health contributor by that name and rolls it into the readiness response automatically. No additional wiring is needed. From this point on, if the Kafka broker becomes unreachable, `/readyz` will return `503` and the platform will stop routing traffic to this instance until the broker recovers.

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
curl http://localhost:8081/actuator/health
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
curl http://localhost:8081/livez
```

```json
{ "status": "UP" }
```

**Readiness (Kafka reachable):**

```bash
curl http://localhost:8081/readyz
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

