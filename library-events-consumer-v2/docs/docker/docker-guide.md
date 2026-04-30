# Intro to Docker

---

## Table of Contents

1. [The Problem: "It works on my machine"](#1-the-problem-it-works-on-my-machine)
2. [The First Solution: Virtual Machines (VMs)](#2-the-first-solution-virtual-machines-vms)
3. [Containers: A Smarter Approach](#3-containers-a-smarter-approach)
5. [Introducing Docker](#5-introducing-docker)
6. [Why Docker Needs a Linux VM on Mac](#6-why-docker-needs-a-linux-vm-on-mac)
7. [Verify Docker is Installed](#7-verify-docker-is-installed)
8. [Key Docker Commands](#8-key-docker-commands)
9. [Your First Dockerfile](#9-your-first-dockerfile)
10. [Build & Run Locally](#10-build--run-locally)
11. [Running in Different Environments](#11-running-in-different-environments)
12. [Push to Docker Registry](#12-push-to-docker-registry)
13. [Full Lifecycle at a Glance](#13-full-lifecycle-at-a-glance)
14. [What's Next?](#14-whats-next)

---

## 1. The Problem: "It works on my machine"

Imagine you've written a Spring Boot application. It runs perfectly on your laptop. You hand it off to a teammate or deploy it to a production server — and it crashes immediately.

Why? Because software doesn't run in isolation. It depends on:

- The **Java version** installed on the machine
- The **OS** and its libraries
- **Environment variables** and config files
- **Database versions**, ports, and network settings

Here's the classic scenario:

```
Developer's Laptop         Production Server
─────────────────          ─────────────────
Java 25                    Java 21
Maven 3.9                  Maven 3.6
Ubuntu 22.04               CentOS 7
MySQL 8                    MySQL 5.7
PORT=8080                  PORT=9090

Result: App works locally. Crashes in production.
```

This environment mismatch problem plagued the industry for decades.
The first attempt to solve it was **Virtual Machines**.

---

## 2. The First Solution: Virtual Machines (VMs)

Before containers existed, Virtual Machines were the industry's go-to solution for environment consistency. If you needed to run an app in an isolated, reproducible environment — a VM was the standard answer. Cloud providers like AWS and VMware built entire businesses around this model. It worked, but it came with significant trade-offs that became harder to ignore as software teams moved faster and deployments became more frequent.

### What is a VM?

A Virtual Machine emulates an **entire physical computer** in software — including the CPU, memory, disk, and network card. It runs a full operating system on top of your existing OS using a piece of software called a **Hypervisor**.

```
┌──────────────────────────────────────────────────┐
│                  Physical Machine                │
│                  (Your Laptop)                   │
│                                                  │
│   ┌──────────────────────────────────────────┐   │
│   │           Host Operating System          │   │
│   │           (e.g., macOS / Windows)        │   │
│   │                                          │   │
│   │   ┌──────────────────────────────────┐   │   │
│   │   │     Hypervisor Layer             │   │   │
│   │   │  (VMware / VirtualBox / HyperV)  │   │   │
│   │   │                                  │   │   │
│   │   │  ┌─────────────┐ ┌────────────┐  │   │   │
│   │   │  │    VM 1     │ │    VM 2    │  │   │   │
│   │   │  │─────────────│ │────────────│  │   │   │
│   │   │  │ Guest OS    │ │ Guest OS   │  │   │   │
│   │   │  │ (Ubuntu)    │ │ (CentOS)   │  │   │   │
│   │   │  │─────────────│ │────────────│  │   │   │
│   │   │  │ Java App    │ │ Node App   │  │   │   │
│   │   │  └─────────────┘ └────────────┘  │   │   │
│   │   └──────────────────────────────────┘   │   │
│   └──────────────────────────────────────────┘   │
└──────────────────────────────────────────────────┘
```

### How the Hypervisor works

The Hypervisor sits between the hardware and the VMs. It tricks each VM into thinking it has its own dedicated CPU, memory, and disk — when in reality they're all sharing the same physical hardware.

### VMs solved the environment problem — but created new ones

| Problem | Why It Happens |
|---|---|
| Each VM is **GBs in size** | A full OS (Ubuntu, CentOS) is 1–4 GB minimum |
| **Slow to start** (2–5 minutes) | Booting a full OS takes time |
| **Resource hungry** | Even an idle VM consumes RAM and CPU |
| **Hard to replicate exactly** | Slight differences in OS config cause issues |
| **Slow to ship** | VM images are huge — slow to upload/download |

### The analogy

> Think of a VM like **renting an entire apartment just to keep one piece of furniture**.
> You get full isolation, but you're paying for the whole apartment even if you only need the couch.

VMs are still used today for full OS isolation (cloud servers, for example), but for running applications, they are overkill. We needed something lighter.

---

## 3. Containers: A Smarter Approach

### The key insight: Share the OS kernel

Containers don't emulate hardware. They don't run a full guest OS. Instead, they **share the host machine's OS kernel** and only package the application and its dependencies.

```
┌──────────────────────────────────────────────────┐
│                  Physical Machine                │
│                                                  │
│   ┌──────────────────────────────────────────┐   │
│   │     Host Operating System                │   │
│   │     Linux Kernel (shared by all)         │   │
│   │                                          │   │
│   │   ┌──────────────────────────────────┐   │   │
│   │   │       Docker Engine              │   │   │
│   │   │  (Container Runtime)             │   │   │
│   │   │                                  │   │   │
│   │   │  ┌───────────┐  ┌───────────┐   │   │   │
│   │   │  │Container A│  │Container B│   │   │   │
│   │   │  │───────────│  │───────────│   │   │   │
│   │   │  │ Java App  │  │ Node App  │   │   │   │
│   │   │  │ Java 17   │  │ Node 18   │   │   │   │
│   │   │  │ libs      │  │ libs      │   │   │   │
│   │   │  └───────────┘  └───────────┘   │   │   │
│   │   │   No Guest OS needed!            │   │   │
│   │   └──────────────────────────────────┘   │   │
│   └──────────────────────────────────────────┘   │
└──────────────────────────────────────────────────┘
```

Each container gets its own:
- **Filesystem** — isolated, can't see other containers' files
- **Network** — its own IP address and ports
- **Process space** — can't see or kill other containers' processes
- **Environment variables** — completely isolated config

But they all **share the same Linux kernel** underneath.

### VM vs Container: Side by Side

| Feature | Virtual Machine | Container |
|---|---|---|
| Size | 1–4 GB per VM | 50–300 MB per container |
| Boot time | 2–5 minutes | Under 1 second |
| OS overhead | Full guest OS per VM | Shared host kernel |
| Isolation | Hardware-level | Process-level |
| Portability | Difficult (large, OS-specific) | Very easy (small, portable) |
| Resource usage | High (even when idle) | Minimal |
| Startup speed | Slow | Near-instant |
| Use case | Full OS isolation | App packaging & deployment |

### The analogy

> Think of containers like **shipping containers on a cargo ship**.
> Every container is self-contained with its own contents.
> The ship (host OS) carries all of them without caring what's inside each one.
> You can move a container from one ship to another and it works exactly the same way.

---

## 4. Why Containers? Core Benefits

### A. Consistency Across Environments

The container packages **everything the app needs** — code, runtime, libraries, config. It runs the same way on every machine.

```
Your Laptop  →  CI/CD Pipeline  →  Staging  →  Production
    ✓                ✓                ✓              ✓
              Same container, same behavior everywhere
```

No more "it works on my machine." If it works in your container locally, it will work in production.

---

### B. Isolation — Multiple Apps, No Conflicts

Different apps can have completely different dependencies and they won't interfere with each other.

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Container A    │    │  Container B    │    │  Container C    │
│─────────────────│    │─────────────────│    │─────────────────│
│  Java 8         │    │  Java 17        │    │  Python 3.11    │
│  MySQL 5.7      │    │  Postgres 15    │    │  MongoDB        │
│  App: Legacy    │    │  App: New API   │    │  App: ML Model  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         All running side by side on the same machine ✓
```

---

### C. Speed — Start in Milliseconds

```
VM startup:        2–5 minutes   (booting full OS)
Container startup: < 1 second    (process launch)
```

This matters enormously for:
- Local development (restart app instantly)
- CI/CD pipelines (faster builds and tests)
- Auto-scaling in production (spin up new instances instantly)

---

### D. Efficiency — More Apps, Same Hardware

Because containers share the OS kernel and don't waste resources on duplicate OS copies:

```
A server with 8GB RAM:
  Using VMs:        Run ~3–4 VMs   (each VM needs ~2GB just for OS)
  Using Containers: Run ~20–30 containers (no OS overhead per container)
```

This translates directly to **lower infrastructure costs**.

---

### E. Perfect for Microservices

In a microservices architecture, each service is independent. Containers are the natural packaging unit.

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Container   │  │  Container   │  │  Container   │  │  Container   │
│  ──────────  │  │  ──────────  │  │  ──────────  │  │  ──────────  │
│  Library     │  │  Kafka       │  │  Zookeeper   │  │  Library     │
│  Events      │  │  Broker      │  │              │  │  Events      │
│  Producer    │  │              │  │              │  │  Consumer    │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
         Each service is independently deployable and scalable
```

This is exactly how we'll run Kafka + our Spring Boot app in this course.

---

## 5. Introducing Docker

### What is Docker?

Docker is the most popular **platform for building, shipping, and running containers**. It provides:

- A standard format for packaging apps (the **Docker image**)
- A runtime to run those packages (the **Docker Engine**)
- Tools to manage and share images (the **Docker CLI** and **Docker Hub**)

> Containers existed before Docker (via Linux namespaces and cgroups), but Docker made them accessible to every developer with a simple toolchain.

### The Three Core Concepts

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Ecosystem                            │
│                                                                     │
│                                                                     │
│   Dockerfile   ──build──▶   Image    ──run──▶    Container         │
│   (recipe)                (blueprint)            (running app)     │
│                               │                                     │
│                            push/pull                                │
│                               │                                     │
│                               ▼                                     │
│                        Docker Registry                              │
│                    (Docker Hub / AWS ECR)                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Concept Breakdown

| Term | Real-World Analogy | Technical Description |
|---|---|---|
| **Dockerfile** | A recipe / cooking instructions | A text file with step-by-step instructions to build an image |
| **Image** | A blueprint / a Java Class | A read-only, immutable snapshot of your app + its environment |
| **Container** | A running object / a running process | A live, running instance of an image |
| **Registry** | An App Store / GitHub for images | A remote repository to store and share images |

### Key relationships

- One **Dockerfile** → builds one **Image**
- One **Image** → can run many **Containers** simultaneously
- An **Image** can be pushed to a **Registry** and pulled anywhere

### Analogy: Object-Oriented Programming

If you come from Java:

```
Dockerfile  =  Source code
Image       =  Compiled .class file / JAR
Container   =  Running JVM instance of that JAR
Registry    =  Maven Central / Nexus (artifact repository)
```

---

## 6. Why Docker Needs a Linux VM on Mac

### The root reason: Containers are a Linux technology

Containers use two Linux kernel features:
- **Namespaces** — isolate what a process can see (filesystem, network, processes)
- **cgroups** — limit how much CPU and RAM a container can use

These features exist **only in the Linux kernel**.

```
Linux machine:
  Your App → Container → Linux Kernel (native)  ✓ Direct, no overhead

macOS machine:
  macOS kernel (Darwin) ≠ Linux kernel
  → Docker Desktop installs a lightweight Linux VM silently
  → Your containers run inside that Linux VM
  → You interact with Docker CLI as if it's native
  → The VM is invisible to you
```

### What Docker Desktop installs on Mac

```
Docker Desktop
├── Docker Engine          ← The container runtime (runs inside Linux VM)
├── Docker CLI             ← The `docker` command in your terminal
├── Docker Compose         ← Tool to run multi-container applications
├── Docker Dashboard       ← GUI to view containers, images, logs
├── Docker Scout           ← Image vulnerability scanning
└── Linux VM (via Apple Hypervisor)
      └── This is where your containers actually run
```

### Does this affect you as a developer?

In practice, **no**. You use the same `docker` commands. The VM is completely transparent. You only notice it if you look at Docker Desktop's resource settings (RAM/CPU allocated to the VM).

---

## 7. Verify Docker is Installed

### Check the installation

```bash
# Check Docker CLI version
docker --version
# Expected: Docker version 27.x.x, build xxxxxxx

# Check Docker Engine is running
docker info
# Shows system-wide Docker info — confirms engine is reachable
```

### Run your first container

```bash
docker run hello-world
```

### What happens step by step

```
Step 1: Docker CLI receives the command "run hello-world"
         ↓
Step 2: Docker checks local image cache
        → "hello-world" image not found locally
         ↓
Step 3: Docker pulls the image from Docker Hub
        → "Unable to find image 'hello-world:latest' locally"
        → "latest: Pulling from library/hello-world"
         ↓
Step 4: Docker creates a container from the image
         ↓
Step 5: Container runs, prints the "Hello from Docker!" message
         ↓
Step 6: Container exits (this app's job is done)
```

### Expected output

```
Unable to find image 'hello-world:latest' locally
latest: Pulling from library/hello-world
...
Hello from Docker!

This message shows that your installation appears to be working correctly.

To generate this message, Docker took the following steps:
 1. The Docker client contacted the Docker daemon.
 2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
 3. The Docker daemon created a new container from that image...
 4. The Docker daemon streamed that output to the Docker client...
```

You just pulled an image from the internet, ran it as a container, and saw its output — in under 5 seconds. That's the power of Docker.

---

## 8. Key Docker Commands

### Managing Images

```bash
# Pull an image from Docker Hub (downloads, doesn't run)
docker pull nginx

# List all images stored locally
docker images
# Shows: REPOSITORY, TAG, IMAGE ID, CREATED, SIZE

# Remove an image
docker rmi nginx

# Remove all unused images (cleanup)
docker image prune
```

### Starting Containers

```bash
# Run a container (foreground — blocks your terminal)
docker run nginx

# Run in background — detached mode (frees your terminal)
# Note: no port mapping here — the container is running but
# there is no way to interact with it from outside
docker run -d nginx

# Run with a custom name (easier to reference than container ID)
docker run -d --name my-nginx nginx

# Run with port mapping: -p <host-port>:<container-port>
docker run -d -p 8080:80 nginx
#   Your machine's port 8080 → container's port 80
#   Visit http://localhost:8080 to see nginx

# Run and automatically remove container when it stops
docker run --rm hello-world
```

### Port mapping explained visually

```
Your Machine (Host)          Container
─────────────────            ─────────────
Port 8080        ──────▶     Port 80 (nginx)
Port 9092        ──────▶     Port 9092 (kafka)
Port 5432        ──────▶     Port 5432 (postgres)

Command: docker run -p 8080:80 nginx
                        │    │
                        │    └── Container port (what the app listens on)
                        └─────── Host port (what you access from browser)
```

### Stopping and Removing Containers

```bash
# List running containers
docker ps

# List ALL containers (running + stopped)
docker ps -a

# Stop a running container (graceful shutdown)
docker stop <container-id or name>

# Start a stopped container
docker start <container-id or name>

# Restart a container
docker restart <container-id or name>

# Forcefully kill a container (immediate)
docker kill <container-id or name>

# Remove a stopped container
docker rm <container-id or name>

# Remove a running container (force)
docker rm -f <container-id or name>
```

### Debugging and Inspecting Containers

```bash
# View logs from a container
docker logs <container-id or name>

# Follow logs in real-time (like tail -f)
docker logs -f <container-id or name>

# Show last 50 lines of logs
docker logs --tail 50 <container-id or name>

# Open an interactive shell inside a running container
docker exec -it <container-id or name> bash

# For Alpine-based images (no bash, use sh)
docker exec -it <container-id or name> sh

# Inspect full container metadata (JSON)
docker inspect <container-id or name>

# Show container resource usage (CPU, memory)
docker stats
```

---

## 9. Your First Dockerfile

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

## 10. Build & Run Locally

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

## 11. Running in Different Environments

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

## 12. Push to Docker Registry

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

## 13. Full Lifecycle at a Glance

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

## 14. What's Next?

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
