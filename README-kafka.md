# Kafka Local Development Setup

This guide covers running Kafka locally using Docker with KRaft mode (no Zookeeper required).

## Table of Contents

- [Prerequisites](#prerequisites)
- [Starting Kafka](#starting-kafka)
  - [Single Broker](#single-broker)
  - [Verify Kafka is Running](#verify-kafka-is-running)
  - [Connection Summary](#connection-summary)
- [Stopping Kafka](#stopping-kafka)
- [Working with Topics](#working-with-topics)
  - [List Topics](#list-topics)
  - [Create a Topic](#create-a-topic)
  - [Describe a Topic](#describe-a-topic)
  - [Delete a Topic](#delete-a-topic)
- [Producing and Consuming Messages](#producing-and-consuming-messages)
  - [Producer](#producer)
  - [Consumer](#consumer)
  - [Consume from Beginning](#consume-from-beginning)
  - [Producer with Keys](#producer-with-keys)
  - [Consumer with Keys](#consumer-with-keys)
  - [Consume with Consumer Group](#consume-with-consumer-group)
- [Consumer Groups](#consumer-groups)
  - [List Consumer Groups](#list-consumer-groups)
  - [Describe Consumer Group](#describe-consumer-group)
  - [Reset Consumer Group Offset](#reset-consumer-group-offset)
- [Multi-Broker Demonstrations](#multi-broker-demonstrations)
  - [Demo 1: Replication in Action](#demo-1-replication-in-action)
  - [Demo 2: Leader Election (Failover)](#demo-2-leader-election-failover)
  - [Demo 3: ISR and min.insync.replicas](#demo-3-isr-and-mininsuncreplicas)
  - [Demo 4: Partition Distribution](#demo-4-partition-distribution)
- [Commit Log & Retention](#commit-log--retention)
  - [Understanding the Commit Log](#understanding-the-commit-log)
  - [Log Storage Location](#log-storage-location)
  - [Viewing the Commit Log](#viewing-the-commit-log)
  - [Retention Period](#retention-period)
  - [Retention Configuration Summary](#retention-configuration-summary)
  - [Log Compaction](#log-compaction)
- [Connection Details](#connection-details)
- [Troubleshooting](#troubleshooting)
  - [Validate KRaft Configuration](#validate-kraft-configuration)
  - [Check Kafka Logs](#check-kafka-logs)
  - [Check Broker Status](#check-broker-status)
  - [Check Cluster Metadata](#check-cluster-metadata)
  - [Read __cluster_metadata Topic](#read-__cluster_metadata-topic)
  - [Container Shell Access](#container-shell-access)

## Prerequisites

- Docker and Docker Compose installed
- Ports 9092 and 29092 available (single broker)
- Ports 9092-9094 and 29092-29094 available (multi-broker)

## Starting Kafka

### Single Broker

```bash
docker-compose up -d
```

### Verify Kafka is Running

```bash
docker-compose ps
docker logs -f kafka1
```

### Connection Summary

```
┌──────────────────────┐
│   Your Host Machine  │
│   (Spring Boot App)  │
│                      │
│   localhost:9092 ────┼──────┐
└──────────────────────┘      │
                              │ EXTERNAL
                              ▼
                    ┌─────────────────┐
                    │     kafka1      │
                    │   (container)   │
                    └─────────────────┘
                              ▲
                              │ DOCKER
┌──────────────────────┐      │
│  Other Container     │      │
│  (e.g., microservice)│      │
│                      │      │
│ host.docker.internal:├──────┘
│        29092         │
└──────────────────────┘
```

| Connection From | Bootstrap Server |
|-----------------|------------------|
| Host machine | `localhost:9092` |
| Other Docker containers | `host.docker.internal:29092` |

## Stopping Kafka

### Single Broker

```bash
docker-compose down
```

### Multi-Broker

```bash
docker-compose -f docker-compose-multi-broker.yml down
```

To remove volumes and start fresh:

```bash
docker-compose down -v
```

## Working with Topics

### List Topics

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list
```

### Create a Topic

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic my-topic --partitions 3 --replication-factor 1
```

For multi-broker setup, you can use a higher replication factor:

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic my-topic --partitions 3 --replication-factor 3
```

### Describe a Topic

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic my-topic
```

### Delete a Topic

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic my-topic
```

## Producing and Consuming Messages

### Producer

```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic my-topic
```

Type your messages and press Enter after each. Press `Ctrl+C` to exit.

### Consumer

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic my-topic
```

### Consume from Beginning

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic my-topic --from-beginning
```

---

### Producer with Keys

```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic my-topic \
  --property parse.key=true \
  --property key.separator=-
```

Format: `key-value` (e.g., `user1-Hello World`)

### Consumer with Keys

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic my-topic --from-beginning \
  --property print.key=true \
  --property key.separator=-
```

---

### Consume with Consumer Group

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic my-topic --group my-consumer-group
```

## Consumer Groups

### List Consumer Groups

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### Describe Consumer Group

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-consumer-group
```

### Reset Consumer Group Offset

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group my-consumer-group --topic my-topic --reset-offsets --to-earliest --execute
```

## Multi-Broker Demonstrations

The following demonstrations require the 3-broker cluster. Start it with:

```bash
docker-compose -f docker-compose-multi-broker.yml up -d
```

> **Important: Bootstrap Server for Multi-Broker**
>
> When running commands inside containers with `docker exec`, use the **internal listener** (`kafka1:19092`) instead of `localhost:9092`. This is because the cluster metadata returns internal addresses for all brokers, which are only resolvable within the Docker network.
>
> | Context | Bootstrap Server |
> |---------|------------------|
> | Inside container (docker exec) | `kafka1:19092` |
> | From host machine | `localhost:9092` |

---

### Demo 1: Replication in Action

> **What is Replication?**
> - Replication means storing copies of the same data on multiple brokers
> - The `replication.factor` setting determines how many copies exist
> - One broker is the **Leader** (handles reads/writes), others are **Followers** (replicate data)
> - If a broker fails, data is still available on other brokers
> - Industry standard: `replication.factor=3` for production workloads

**Concept:** Data is automatically replicated across all 3 brokers.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    REPLICATION                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Producer ───▶ Broker 1 (Leader)                                           │
│                     │                                                       │
│                     ├───▶ Broker 2 (Follower) - replicates                  │
│                     └───▶ Broker 3 (Follower) - replicates                  │
│                                                                             │
│   Result: All 3 brokers have the same data                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Create a topic with replication factor 3:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --create --topic demo-topic --partitions 3 --replication-factor 3
```

**Describe the topic to see replication:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic demo-topic
```

**Expected Output:**

```
Topic: demo-topic    PartitionCount: 3    ReplicationFactor: 3
    Partition: 0    Leader: 1    Replicas: 1,2,3    Isr: 1,2,3
    Partition: 1    Leader: 2    Replicas: 2,3,1    Isr: 2,3,1
    Partition: 2    Leader: 3    Replicas: 3,1,2    Isr: 3,1,2
```

**What to observe:**
- Each partition has a different leader (distributed leadership)
- `Replicas` lists all 3 brokers for each partition
- `Isr` (In-Sync Replicas) shows all 3 are synchronized

#### Live Producer-Consumer Interaction

This demonstrates real-time message flow between producer and consumer across the replicated cluster.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PRODUCER-CONSUMER FLOW                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Terminal 1 (Producer)              Terminal 2 (Consumer)                  │
│   ┌─────────────────────┐            ┌─────────────────────┐                │
│   │ > hello             │            │                     │                │
│   │ > world             │ ────────▶  │ hello               │                │
│   │ > kafka rocks!      │            │ world               │                │
│   │                     │            │ kafka rocks!        │                │
│   └─────────────────────┘            └─────────────────────┘                │
│         │                                   ▲                               │
│         │         ┌─────────────────────────┤                               │
│         ▼         ▼                         │                               │
│   ┌─────────┬─────────┬─────────┐          │                               │
│   │ kafka1  │ kafka2  │ kafka3  │──────────┘                               │
│   │ (write) │(replica)│(replica)│  Consumer can read from ANY broker       │
│   └─────────┴─────────┴─────────┘                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Terminal 1 - Start a Consumer (run this first):**

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
  --topic demo-topic --from-beginning
```

**Terminal 2 - Start a Producer:**

```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:19092 \
  --topic demo-topic
```

Type messages in the producer terminal and watch them appear instantly in the consumer terminal.

**Bonus: Consumer from a Different Broker**

To prove replication works, start another consumer connected to a **different broker**:

```bash
# Consumer connected to kafka2
docker exec -it kafka2 kafka-console-consumer --bootstrap-server kafka2:19094 \
  --topic demo-topic --from-beginning

# Consumer connected to kafka3
docker exec -it kafka3 kafka-console-consumer --bootstrap-server kafka3:19096 \
  --topic demo-topic --from-beginning
```

**What to observe:**
- All consumers receive the **same messages** regardless of which broker they connect to
- This proves data is replicated across all brokers
- Consumers can connect to any broker - Kafka handles routing automatically

---

#### Viewing Replicated Data on Disk

Now let's verify the same data exists on all 3 brokers at the file system level.

**Step 1: Produce messages to the topic (if not already done):**

```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:19092 \
  --topic demo-topic
```

Type a few messages (e.g., `message1`, `message2`, `message3`) and press `Ctrl+C` to exit.

**Step 2: Verify partition directories exist on all brokers:**

```bash
# Check kafka1
docker exec kafka1 ls -la /var/lib/kafka/data/ | grep demo-topic

# Check kafka2
docker exec kafka2 ls -la /var/lib/kafka/data/ | grep demo-topic

# Check kafka3
docker exec kafka3 ls -la /var/lib/kafka/data/ | grep demo-topic
```

All 3 brokers should show `demo-topic-0`, `demo-topic-1`, and `demo-topic-2` directories.

**Step 3: Find which partition has data:**

Without message keys, Kafka uses sticky partitioning - all messages in a batch go to the same partition. First, check which partition has data:

```bash
# Check log file sizes to find which partition has data
docker exec kafka1 ls -la /var/lib/kafka/data/demo-topic-*/00000000000000000000.log
```

Look for the partition with a non-zero file size (e.g., 228 bytes instead of 0).

**Step 4: View the actual data on each broker using kafka-dump-log:**

Replace `demo-topic-1` with whichever partition has data:

```bash
# View data on Broker 1
docker exec kafka1 kafka-dump-log \
  --files /var/lib/kafka/data/demo-topic-1/00000000000000000000.log \
  --print-data-log

# View data on Broker 2 - SAME DATA!
docker exec kafka2 kafka-dump-log \
  --files /var/lib/kafka/data/demo-topic-1/00000000000000000000.log \
  --print-data-log

# View data on Broker 3 - SAME DATA!
docker exec kafka3 kafka-dump-log \
  --files /var/lib/kafka/data/demo-topic-1/00000000000000000000.log \
  --print-data-log
```

**What you'll see:**

```
Dumping /var/lib/kafka/data/demo-topic-1/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 ... payload: message1
baseOffset: 1 ... payload: message2
baseOffset: 2 ... payload: message3
```

**Key observation:** The exact same messages with the same offsets appear on all 3 brokers. This is replication in action - Kafka automatically copies every message to all replicas.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA REPLICATION PROOF                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Broker 1 (kafka1)          Broker 2 (kafka2)          Broker 3 (kafka3)   │
│   /var/lib/kafka/data/       /var/lib/kafka/data/       /var/lib/kafka/data/│
│   └── demo-topic-X/          └── demo-topic-X/          └── demo-topic-X/   │
│       └── ...00000.log           └── ...00000.log           └── ...00000.log│
│           offset 0: msg1             offset 0: msg1             offset 0: msg1│
│           offset 1: msg2             offset 1: msg2             offset 1: msg2│
│           offset 2: msg3             offset 2: msg3             offset 2: msg3│
│                                                                             │
│   IDENTICAL DATA ON ALL 3 BROKERS! (X = partition with data)                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Why this matters:**
- If any broker fails, the data is safe on the other 2 brokers
- Consumers can continue reading from surviving brokers
- No manual intervention needed - replication is automatic and continuous

---

### Demo 2: Leader Election (Failover)

> **What is Leader Election?**
> - Each partition has ONE leader that handles all reads and writes
> - Followers only replicate data from the leader
> - When a leader broker fails, Kafka automatically elects a new leader from the followers
> - Election happens in milliseconds (< 1 second with KRaft)
> - Producers and consumers automatically reconnect to the new leader
> - This is how Kafka achieves **high availability** - no manual intervention needed

**Concept:** When a leader fails, a follower automatically becomes the new leader.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LEADER ELECTION                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   BEFORE:                          AFTER (kafka1 stopped):                  │
│   ┌─────────┐                      ┌─────────┐                              │
│   │ kafka1  │                      │ kafka1  │                              │
│   │ P0: L   │  ── stop ──▶        │  (down) │                              │
│   └─────────┘                      └─────────┘                              │
│   ┌─────────┐                      ┌─────────┐                              │
│   │ kafka2  │                      │ kafka2  │                              │
│   │ P0: F   │  ── promoted ──▶    │ P0: L   │ ◀── NEW LEADER               │
│   └─────────┘                      └─────────┘                              │
│                                                                             │
│   ISR changes from [1,2,3] to [2,3]                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Step 1: Check current leader for partition 0:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic demo-topic | grep "Partition: 0"
```

**Step 2: Stop broker 1:**

```bash
docker stop kafka1
```

**Step 3: Check the new leader (connect to kafka2):**

```bash
docker exec kafka2 kafka-topics --bootstrap-server kafka2:19094 \
  --describe --topic demo-topic
```

**What to observe:**
- Leader changed from broker 1 to another broker
- ISR shrinks from `[1,2,3]` to `[2,3]`
- **No data loss** - all messages still available

**Step 4: Restart broker 1:**

```bash
docker start kafka1
```

**Step 5: Verify ISR is restored:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic demo-topic
```

ISR should expand back to include all 3 brokers.

---

### Demo 3: ISR and min.insync.replicas

> **What is ISR (In-Sync Replicas)?**
> - ISR is the set of replicas that are fully caught up with the leader
> - A replica falls out of ISR if it lags too far behind (network issues, slow disk)
> - Only ISR members can be elected as new leader (they have all the data)
>
> **What is min.insync.replicas?**
> - Minimum number of replicas that must acknowledge a write (when `acks=all`)
> - If ISR count falls below this value, producers receive `NotEnoughReplicasException`
> - This protects against data loss - better to reject writes than lose them
> - Common setting: `replication.factor=3` with `min.insync.replicas=2`

**Concept:** Writes require acknowledgment from a minimum number of replicas.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ISR AND MIN.INSYNC.REPLICAS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Configuration: replication.factor=3, min.insync.replicas=2, acks=all      │
│                                                                             │
│   SCENARIO 1: All brokers healthy (ISR=3)                                   │
│   ISR size = 3 >= min.insync.replicas = 2  ✓ WRITES ALLOWED                 │
│                                                                             │
│   SCENARIO 2: One broker down (ISR=2)                                       │
│   ISR size = 2 >= min.insync.replicas = 2  ✓ WRITES STILL ALLOWED           │
│                                                                             │
│   SCENARIO 3: Two brokers down (ISR=1)                                      │
│   ISR size = 1 < min.insync.replicas = 2   ✗ WRITES REJECTED!               │
│   Producer receives: NotEnoughReplicasException                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Step 1: Create a topic with min.insync.replicas=2:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --create --topic isr-demo --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2
```

**Step 2: Start a producer with acks=all:**

```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:19092 \
  --topic isr-demo --producer-property acks=all
```

**Step 3: In another terminal, check ISR:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic isr-demo
```

**Step 4: Stop broker 2 and check ISR:**

```bash
docker stop kafka2
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic isr-demo
```

ISR should show only 2 brokers. Producer **still works** (ISR=2 >= min.insync.replicas=2).

**Step 5: Stop broker 3:**

```bash
docker stop kafka3
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic isr-demo
```

ISR now shows only 1 broker. Try to produce - it should **FAIL** with `NotEnoughReplicasException`.

**Step 6: Restart brokers:**

```bash
docker start kafka2 kafka3
```

**Key takeaway:** `min.insync.replicas` protects your data by refusing writes when durability can't be guaranteed.

---

### Demo 4: Partition Distribution

> **What is Partition Distribution?**
> - Kafka spreads partitions across all available brokers
> - Each partition's leader is assigned to a different broker (round-robin)
> - This ensures **load balancing** - no single broker handles all the traffic
> - More partitions = more parallelism for producers and consumers
> - When a broker fails, only its partitions need to failover (not all partitions)
>
> **Rule of thumb:**
> - Number of partitions determines max consumer parallelism
> - A consumer group can have at most as many consumers as partitions

**Concept:** Partitions are spread across brokers for parallelism and fault tolerance.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PARTITION DISTRIBUTION                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   TOPIC: orders (6 partitions, RF=3)                                        │
│                                                                             │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│   │    BROKER 1     │  │    BROKER 2     │  │    BROKER 3     │            │
│   ├─────────────────┤  ├─────────────────┤  ├─────────────────┤            │
│   │ P0: LEADER      │  │ P0: follower    │  │ P0: follower    │            │
│   │ P1: follower    │  │ P1: LEADER      │  │ P1: follower    │            │
│   │ P2: follower    │  │ P2: follower    │  │ P2: LEADER      │            │
│   │ P3: LEADER      │  │ P3: follower    │  │ P3: follower    │            │
│   │ P4: follower    │  │ P4: LEADER      │  │ P4: follower    │            │
│   │ P5: follower    │  │ P5: follower    │  │ P5: LEADER      │            │
│   └─────────────────┘  └─────────────────┘  └─────────────────┘            │
│                                                                             │
│   Each broker leads 2 partitions - BALANCED!                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Step 1: Create a topic with 6 partitions:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --create --topic orders --partitions 6 --replication-factor 3
```

**Step 2: Describe to see distribution:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic orders
```

**Step 3: Count leaders per broker:**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 \
  --describe --topic orders | grep "Leader:" | awk '{print $4}' | sort | uniq -c
```

**What to observe:**
- Leadership is distributed evenly across brokers
- Each broker handles roughly equal load
- If a broker fails, only its partitions need to failover

## Commit Log & Retention

### Understanding the Commit Log

Kafka stores messages in a **commit log** - an append-only, immutable sequence of records. Each partition is a separate log stored on disk.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Partition Log (my-topic-0)                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐           │
│  │  0  │  1  │  2  │  3  │  4  │  5  │  6  │  7  │  8  │ ───▶      │
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘           │
│    ▲                                               ▲                │
│    │                                               │                │
│  Oldest                                         Newest              │
│  (may be deleted                            (append here)           │
│   by retention)                                                     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Key characteristics:**
- **Append-only**: New messages are always added at the end
- **Immutable**: Once written, messages cannot be modified
- **Offset-based**: Each message has a unique sequential offset within the partition
- **Segmented**: Logs are split into segment files for efficient cleanup

### Log Storage Location

View the partition directories on disk:

```bash
docker exec kafka1 ls -la /var/lib/kafka/data/
```

View contents of a specific topic partition:

```bash
docker exec kafka1 ls -la /var/lib/kafka/data/my-topic-0/
```

Typical files in a partition directory:
- `00000000000000000000.log` - The actual message data (segment file)
- `00000000000000000000.index` - Offset index for fast lookups
- `00000000000000000000.timeindex` - Timestamp index
- `leader-epoch-checkpoint` - Leader epoch information
- `partition.metadata` - Partition metadata

### Viewing the Commit Log

Dump the contents of a log segment:

```bash
docker exec kafka1 kafka-dump-log \
  --files /var/lib/kafka/data/my-topic-0/00000000000000000000.log \
  --print-data-log
```

View log with deep iteration (shows all record details):

```bash
docker exec kafka1 kafka-dump-log \
  --files /var/lib/kafka/data/my-topic-0/00000000000000000000.log \
  --print-data-log \
  --deep-iteration
```

### Retention Period

Kafka automatically deletes old data based on retention policies. This is configured at the broker or topic level.

**Time-based retention** (default: 7 days):

```bash
# Check broker default retention in hours (168 hours = 7 days)
docker exec kafka1 cat /etc/kafka/kafka.properties | grep log.retention

# Check current retention for a topic
docker exec kafka1 kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name my-topic --describe --all | grep retention

# Set retention to 1 hour (3600000 ms)
docker exec kafka1 kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name my-topic \
  --alter --add-config retention.ms=3600000

# Set retention to 7 days (604800000 ms) - default
docker exec kafka1 kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name my-topic \
  --alter --add-config retention.ms=604800000
```

**Size-based retention**:

```bash
# Set max size to 1GB per partition
docker exec kafka1 kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name my-topic \
  --alter --add-config retention.bytes=1073741824

# Unlimited size (-1)
docker exec kafka1 kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name my-topic \
  --alter --add-config retention.bytes=-1
```

### Retention Configuration Summary

| Config | Default | Description |
|--------|---------|-------------|
| `log.retention.hours` | 168 (7 days) | Broker-level retention in hours |
| `log.retention.minutes` | - | Broker-level retention in minutes (overrides hours) |
| `log.retention.ms` | - | Broker-level retention in ms (overrides minutes) |
| `retention.ms` | 604800000 (7 days) | Topic-level retention in ms |
| `retention.bytes` | -1 (unlimited) | Max size per partition before deletion |
| `log.segment.bytes` | 1073741824 (1GB) | Size of each log segment file |
| `log.retention.check.interval.ms` | 300000 (5 min) | How often to check for expired segments |
| `cleanup.policy` | delete | `delete` (remove old) or `compact` (keep latest per key) |

**Retention precedence:** `retention.ms` (topic) > `log.retention.ms` > `log.retention.minutes` > `log.retention.hours` (broker)

### Log Compaction

For topics with `cleanup.policy=compact`, Kafka keeps only the latest value for each key:

```bash
# Enable log compaction
docker exec kafka1 kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name my-topic \
  --alter --add-config cleanup.policy=compact

# Enable both delete and compact
docker exec kafka1 kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name my-topic \
  --alter --add-config cleanup.policy=compact,delete
```

## Connection Details

### Single Broker

| Connection Type | Bootstrap Server |
|-----------------|------------------|
| From host machine | `localhost:9092` |
| From Docker containers | `host.docker.internal:29092` |
| Inside container (docker exec) | `localhost:9092` |

### Multi-Broker (3 nodes)

| Connection Type | Bootstrap Server |
|-----------------|------------------|
| From host machine | `localhost:9092` (or `localhost:9094`, `localhost:9096`) |
| From Docker containers | `host.docker.internal:29092` |
| Inside container (docker exec) | `kafka1:19092` (or `kafka2:19094`, `kafka3:19096`) |

> **Note:** For multi-broker, when running `docker exec` commands, you **must** use the internal listener (e.g., `kafka1:19092`) because the cluster metadata returns internal addresses that are only resolvable within the Docker network.

### Spring Boot Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
```

## Troubleshooting

### Validate KRaft Configuration

Verify the `process.roles` property and other KRaft settings inside the container:

```bash
docker exec kafka1 cat /etc/kafka/kafka.properties
```

This should show `process.roles=broker,controller` confirming the node is running in KRaft combined mode.

### Check Kafka Logs

```bash
docker logs kafka1 -f
```

### Check Broker Status

```bash
docker exec kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Check Cluster Metadata

```bash
docker exec kafka1 kafka-metadata --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log --command "cat"
```

### Read __cluster_metadata Topic

The `__cluster_metadata` topic stores KRaft cluster state. Use `kafka-dump-log` to read its contents:

```bash
docker exec kafka1 kafka-dump-log --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log
```

### Container Shell Access

```bash
docker exec -it kafka1 bash
```
