# Packaging and Running the Consumer App

---

## Table of Contents

1. [Your First Dockerfile](#1-your-first-dockerfile)
2. [Build & Run Locally](#2-build--run-locally)
3. [Running in Different Environments](#3-running-in-different-environments)
4. [Push to Docker Registry](#4-push-to-docker-registry)
5. [Full Lifecycle at a Glance](#5-full-lifecycle-at-a-glance)
6. [What's Next?](#6-whats-next)

---

## 1. Your First Dockerfile

### What is a Dockerfile?

A Dockerfile is a plain text file named `Dockerfile` (no extension) that contains instructions for building a Docker image. Docker reads it top to bottom and executes each instruction in order.

### Dockerfile instructions explained

```dockerfile
# ─────────────────────────────────────────────────────────
# FROM — choose your base image
# Every Dockerfile starts with FROM.
# Think of it as: "Start with this pre-built environment."
# ─────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre

# ─────────────────────────────────────────────────────────
# WORKDIR — set the working directory inside the container
# All subsequent commands run from this directory.
# If it doesn't exist, Docker creates it.
# ─────────────────────────────────────────────────────────
WORKDIR /app

# ─────────────────────────────────────────────────────────
# ARG — defines a build-time variable
# JAR_FILE can be overridden when running docker build.
# This makes the Dockerfile flexible for different build outputs.
# ─────────────────────────────────────────────────────────
ARG JAR_FILE=build/libs/*-SNAPSHOT.jar

# ─────────────────────────────────────────────────────────
# COPY — copy files from your machine into the container
# Syntax: COPY <source on host> <destination in container>
# ─────────────────────────────────────────────────────────
COPY ${JAR_FILE} /app/app.jar

# ─────────────────────────────────────────────────────────
# EXPOSE — document which port the app listens on
# This is informational — it does NOT publish the port.
# You still need -p when running the container.
# ─────────────────────────────────────────────────────────
EXPOSE 8081

# ─────────────────────────────────────────────────────────
# ENTRYPOINT — the command to run when the container starts
# Unlike CMD, ENTRYPOINT cannot be overridden at runtime.
# Use JSON array format (exec form) — avoids shell overhead.
# ─────────────────────────────────────────────────────────
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

### Why build the JAR outside Docker?

This project follows a **Gradle-first** approach: build the JAR on your machine, then copy it into the image. This is intentional.

| Benefit | Explanation |
|---|---|
| **Faster image builds** | Gradle work stays outside Docker — image creation is just a copy step |
| **Smaller runtime image** | Container only needs JRE + your JAR (no Gradle cache or source code) |
| **Easier debugging** | Verify the JAR locally before containerizing — narrows down failures quickly |
| **CI/CD friendly** | Pipelines can reuse the same JAR artifact for testing, scanning, and image creation |
| **Clear separation** | Build concerns stay with Gradle; runtime concerns stay with Docker |

### Understanding Image Layers

Every instruction in a Dockerfile creates a **layer**. Layers are cached and reused.

```
FROM eclipse-temurin:25-jre                  ← Layer 1 (base OS + JRE)
WORKDIR /app                                 ← Layer 2 (working directory)
ARG JAR_FILE=build/libs/*-SNAPSHOT.jar       ← Layer 3 (build-time variable)
COPY ${JAR_FILE} /app/app.jar                ← Layer 4 (your compiled JAR)
EXPOSE 8081                                  ← Layer 5 (metadata)
ENTRYPOINT ["java", "-jar", "/app/app.jar"]  ← Layer 6 (startup command)

If you change only your code and rebuild:
  Layer 1 → CACHED (not re-downloaded)
  Layer 2 → CACHED
  Layer 3 → CACHED (ARG value unchanged)
  Layer 4 → REBUILT (your jar changed)
  Layer 5 → REBUILT
  Layer 6 → REBUILT
```

**Lesson:** Put things that change frequently (your code) at the bottom. Put stable things (base image, dependencies) at the top. This maximizes cache usage and speeds up builds.

---

## 2. Build & Run Locally

### Step 1: Build the App JAR

Before building the Docker image, compile the Spring Boot application with Gradle:

```bash
./gradlew clean build
```

This creates the JAR under `build/libs/`. The Dockerfile will copy it from there.

### Step 2: Build the Docker Image

```bash
docker build -t library-events-consumer:v1 .
```

Breaking down the flags:

```
docker build          → Build an image from a Dockerfile
  -t                  → Tag the image with a name
  library-events-consumer:v1
    │                 → Image name (can be anything)
    └── :v1           → Tag / version (default is :latest)
  .                   → Build context: current directory
                        (Docker sends all files here to the engine)
```

Expected output:

```
[+] Building 5.2s (7/7) FINISHED
 => [internal] load build definition from Dockerfile
 => [1/3] FROM eclipse-temurin:25-jre
 => [2/3] WORKDIR /app
 => [3/3] COPY build/libs/*-SNAPSHOT.jar /app/app.jar
 => exporting to image
 => naming to docker.io/library/library-events-consumer:v1
```

### Step 3: Verify the Image

```bash
docker images
```

```
REPOSITORY                  TAG    IMAGE ID       CREATED         SIZE
library-events-consumer     v1     a1b2c3d4e5f6   2 minutes ago   285MB
```

### Step 4: Run the Container

```bash
docker run --name library-events-consumer \
  -p 8081:8081 \
  -e SPRING_PROFILES_ACTIVE=dev \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/mydatabase \
  -e SPRING_DATASOURCE_USERNAME=myuser \
  -e SPRING_DATASOURCE_PASSWORD=secret \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092 \
  -e SPRING_KAFKA_CONSUMER_GROUP_ID=library-events-listener-group-dev \
  library-events-consumer:v1
```

```
-name                              → Give it a readable name
-p 8081:8081                        → Map host port 8081 to container port 8081
-e SPRING_PROFILES_ACTIVE=dev       → Set the active Spring profile
-e SPRING_DATASOURCE_URL            → DB URL for app running in Docker
-e SPRING_DATASOURCE_USERNAME/PASSWORD → DB credentials passed at runtime
-e SPRING_KAFKA_BOOTSTRAP_SERVERS   → Tell the app where Kafka is
-e SPRING_KAFKA_CONSUMER_GROUP_ID   → Sets the Kafka consumer group id
host.docker.internal:29092          → Kafka address reachable from inside Docker
```

If port `8081` is already in use, map a different host port:

```bash
docker run --name library-events-consumer \
  -p 18081:8081 \
  -e SPRING_PROFILES_ACTIVE=dev \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/mydatabase \
  -e SPRING_DATASOURCE_USERNAME=myuser \
  -e SPRING_DATASOURCE_PASSWORD=secret \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092 \
  -e SPRING_KAFKA_CONSUMER_GROUP_ID=library-events-listener-group \
  library-events-consumer:v1
```

### Why `host.docker.internal` and not `localhost`?

This is one of the most common points of confusion when running Spring Boot in Docker.

When your app runs **inside a Docker container**, `localhost` means the container itself — not your Mac and not any other container.

```
If you pass: -e SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092

The app container tries to find Kafka inside itself → fails.
```

Same rule applies to PostgreSQL:

```
If you pass: -e SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/mydatabase

The app container tries to find Postgres inside itself → fails.
```

`host.docker.internal` is a special Docker hostname that resolves to your host machine from inside a container.

```
localhost:9092              → correct when app runs directly on your machine
host.docker.internal:29092  → correct when app runs in Docker, Kafka is on the host
kafka1:19092                → correct when app and Kafka share the same Docker network
```

Database host rules:

```
jdbc:postgresql://localhost:5432/mydatabase            → app runs on host JVM
jdbc:postgresql://host.docker.internal:5432/mydatabase → app runs in Docker, DB on host/compose published port
jdbc:postgresql://postgres:5432/mydatabase             → app + DB run in same Docker network
```

### Step 5: Confirm it's Running

```bash
docker ps
```

```
CONTAINER ID   IMAGE                          STATUS         PORTS                    NAMES
c3d4e5f6a1b2   library-events-consumer:v1    Up 3 seconds   0.0.0.0:8081->8081/tcp   library-events-consumer
```

### Step 6: Verify the Application

```bash
curl -i http://localhost:8081/v1/library-events
```

If you mapped to port `18081`:

```bash
curl -i http://localhost:18081/v1/library-events
```

### Step 7: View Logs

```bash
# View logs (static snapshot)
docker logs library-events-consumer

# Follow logs in real-time
docker logs library-events-consumer -f

# View last 100 lines
docker logs library-events-consumer --tail 100
```

### Step 8: Stop and Remove

```bash
docker stop library-events-consumer
docker rm library-events-consumer
```

---

## 3. Running in Different Environments

One of Docker's biggest strengths: **build the image once, run it anywhere** by changing only environment variables at runtime. No rebuilding, no different images.

### Optional: Use a Different Kafka Broker

If your Kafka broker is not from the local `compose.yaml`, change only this environment variable:

```bash
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=<your-broker-host>:<your-broker-port>
```

Examples:

```bash
# App runs on your machine, Kafka is on localhost
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# App runs in Docker, Kafka is exposed from compose.yaml on the host
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092

# App and Kafka both run in the same Docker network
-e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka1:19092
```

### Run in `stage`

```bash
docker run --name library-events-consumer-stage \
  -p 8081:8081 \
  -e SPRING_PROFILES_ACTIVE=stage \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/mydatabase \
  -e SPRING_DATASOURCE_USERNAME=myuser \
  -e SPRING_DATASOURCE_PASSWORD=secret \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=stage-broker1:9092,stage-broker2:9092 \
  -e SPRING_KAFKA_CONSUMER_GROUP_ID=library-events-listener-group-stage \
  library-events-consumer:v1
```

### Run in `prod`

```bash
docker run --name library-events-consumer-prod \
  -p 8081:8081 \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/mydatabase \
  -e SPRING_DATASOURCE_USERNAME=myuser \
  -e SPRING_DATASOURCE_PASSWORD=secret \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=prod-broker1:9092,prod-broker2:9092,prod-broker3:9092 \
  -e SPRING_KAFKA_CONSUMER_GROUP_ID=library-events-listener-group-prod \
  library-events-consumer:v1
```

### Key Idea

```
Build once:
  docker build -t library-events-consumer:v1 .

Run anywhere by changing only:
  SPRING_PROFILES_ACTIVE        → controls which application-{profile}.yml loads
  SPRING_KAFKA_BOOTSTRAP_SERVERS → controls which Kafka cluster to connect to
  SPRING_KAFKA_CONSUMER_GROUP_ID → controls which consumer group this instance joins

Same image. Different behavior. Zero rebuilds.
```

---

## 4. Push to Docker Registry

### What is a Docker Registry?

A Docker Registry is a **remote storage system for Docker images**. It's like GitHub, but for container images.

```
┌──────────────────────────────────────────────────────┐
│                  Docker Registries                   │
│                                                      │
│  Docker Hub (hub.docker.com)   ← Default, public    │
│  AWS ECR                       ← Amazon's registry  │
│  Google Artifact Registry      ← Google's registry  │
│  GitHub Container Registry     ← GitHub's registry  │
│  Self-hosted (Harbor, Nexus)   ← Private enterprise │
└──────────────────────────────────────────────────────┘
```

### Image naming convention

```
[registry]/[username]/[image-name]:[tag]

Examples:
  nginx                                   → Docker Hub official image
  dilip/library-events-consumer:1.0       → Your Docker Hub image
  123456789.dkr.ecr.us-east-1.amazonaws.com/my-app:latest → AWS ECR
```

### Pushing to Docker Hub

```bash
# Step 1: Login to Docker Hub
docker login
# Enter your Docker Hub username and password

# Step 2: Tag the image with your Docker Hub username
# The tag must match the registry path format
docker tag library-events-consumer:v1 yourusername/library-events-consumer:v1

# Step 3: Push the image
docker push yourusername/library-events-consumer:1.0
```

#### Push to my repo
```
docker tag library-events-consumer:v1 dilipthelip/library-events-consumer:v1
docker push dilipthelip/library-events-consumer:v1
```

Expected push output:

```
The push refers to repository [docker.io/yourusername/library-events-consumer]
a1b2c3d4: Pushed
e5f6a7b8: Pushed
1.0: digest: sha256:abc123... size: 742
```

### Pulling and running from anywhere

Once pushed, anyone with access can run your app without any source code or build tools:

```bash
# Pull the image
docker pull dilipthelip/library-events-consumer:v1

# Run the container
docker run --name library-events-consumer \
  -p 8081:8081 \
  -e SPRING_PROFILES_ACTIVE=dev \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/mydatabase \
  -e SPRING_DATASOURCE_USERNAME=myuser \
  -e SPRING_DATASOURCE_PASSWORD=secret \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092 \
  -e SPRING_KAFKA_CONSUMER_GROUP_ID=library-events-listener-group \
  dilipthelip/library-events-consumer:v1
```

### Interacting with the consumer REST endpoints

Once the container is running, call the REST endpoints exposed by this consumer service:

```
http://localhost:8081/v1/library-events
```

You can also use `curl` to verify the app is up:

```bash
# Check library events endpoint
curl -i http://localhost:8081/v1/library-events

# Check books endpoint
curl -i http://localhost:8081/v1/books
```

These endpoints let you validate that the service is running and connected to the database.

If you mapped to a different host port (e.g. `18081`):

```
http://localhost:18081/v1/library-events
```

This is how teams share applications and how CI/CD pipelines deploy to production.

---

## 5. Full Lifecycle at a Glance

```
┌─────────────────────────────────────────────────────────────────┐
│                   Docker Full Lifecycle                         │
│                                                                 │
│   1. Write Code                                                 │
│         │                                                       │
│         ▼                                                       │
│   2. Write Dockerfile   (recipe for packaging the app)         │
│         │                                                       │
│         ▼                                                       │
│   3. ./gradlew clean build   (compile and produce the JAR)     │
│         │                                                       │
│         ▼                                                       │
│   4. docker build       (create the image locally)             │
│         │                                                       │
│         ▼                                                       │
│   5. docker run         (run as a container locally)           │
│         │                                                       │
│         ▼                                                       │
│   6. docker push        (upload image to registry)             │
│         │                                                       │
│         ▼                                                       │
│   7. docker pull        (any machine downloads the image)      │
│         │                                                       │
│         ▼                                                       │
│   8. docker run         (runs identically anywhere)  ✓         │
└─────────────────────────────────────────────────────────────────┘
```

### Commands summary

| Action | Command |
|---|---|
| Build JAR | `./gradlew clean build` |
| Build image | `docker build -t library-events-consumer:v1 .` |
| List images | `docker images` |
| Run container | `docker run -p 8081:8081 -e SPRING_PROFILES_ACTIVE=dev library-events-consumer:v1` |
| List containers | `docker ps` |
| View logs | `docker logs -f <name>` |
| Stop container | `docker stop <name>` |
| Remove container | `docker rm <name>` |
| Push to registry | `docker push username/library-events-consumer:v1` |
| Pull from registry | `docker pull username/library-events-consumer:v1` |

---

## 6. What's Next?

Now that you understand single containers, the next step is running **multiple containers together** for local dependencies.

### Docker Compose

Docker Compose lets you define and run an entire multi-container stack in a single file. Instead of starting each container manually with `docker run`, you declare all services in one YAML file and bring them all up with one command.

This consumer project includes a lightweight local dependency stack in:

**[`compose.yaml`](../../compose.yaml)**

```yaml
services:
  postgres:
    image: 'postgres:latest'
    environment:
      - 'POSTGRES_DB=mydatabase'
      - 'POSTGRES_PASSWORD=secret'
      - 'POSTGRES_USER=myuser'
    ports:
      - '5432:5432'
```

### How this file works

This compose file is intentionally minimal: it starts PostgreSQL for local development. In this consumer project, Kafka can be run externally (local broker, course cluster, or another compose stack).

#### The local database service

```yaml
postgres:
  image: 'postgres:latest'
  environment:
    - 'POSTGRES_DB=mydatabase'
    - 'POSTGRES_PASSWORD=secret'
    - 'POSTGRES_USER=myuser'
  ports:
    - '5432:5432'
```

- **`image`**: uses the official PostgreSQL image.
- **Environment variables**: initialize DB name/user/password expected by this project's `application.yml`.
- **Port mapping**: publishes PostgreSQL on `localhost:5432` for both app and IDE DB tools.

#### How the Docker network connects everything

Docker Compose automatically creates a shared network for all services in the file. For this setup, the main interaction is between your app and PostgreSQL.

```
Inside the Docker network:
  your-app-container  →  postgres:5432  ✓

From your Mac:
  psql -h localhost -p 5432 -U myuser -d mydatabase  ✓
  app (local JVM) → jdbc:postgresql://localhost:5432/mydatabase  ✓
```

#### Starting and stopping the stack

```bash
# Start all services in the background
docker compose up -d

# Check all containers are running
docker compose ps

# Follow logs for all services
docker compose logs -f

# Follow logs for postgres only
docker compose logs -f postgres

# Stop and remove all containers
docker compose down
```

Once running, point your app to PostgreSQL based on where the app runs:

```
App on host JVM:      jdbc:postgresql://localhost:5432/mydatabase
App in Docker:        jdbc:postgresql://host.docker.internal:5432/mydatabase
```

### Beyond Docker Compose

| Tool | Purpose |
|---|---|
| **Docker Compose** | Multi-container apps on a single machine |
| **Docker Networks** | Control how containers communicate |
| **Docker Volumes** | Persist data across container restarts |

In this consumer module, we use **Docker Compose** to run PostgreSQL locally and connect Kafka separately as needed.
