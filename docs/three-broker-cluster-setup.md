# Three-Broker Kafka Cluster for Learning

This guide explains why a 3-broker cluster is ideal for learning Kafka concepts and provides hands-on demonstrations.

---

## Why 3 Brokers?

### The Magic Number

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WHY 3 BROKERS IS THE SWEET SPOT                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   1 BROKER:     No replication possible                                     │
│   ─────────     ┌───────┐                                                   │
│                 │ Kafka │  Single point of failure                          │
│                 └───────┘  Can't demonstrate distributed concepts           │
│                                                                             │
│                                                                             │
│   2 BROKERS:    No quorum possible (need >50%)                              │
│   ──────────    ┌───────┐    ┌───────┐                                      │
│                 │Kafka 1│    │Kafka 2│  If 1 dies, remaining can't          │
│                 └───────┘    └───────┘  form majority (1/2 = 50%, not >50%) │
│                                                                             │
│                                                                             │
│   3 BROKERS:    ✓ PERFECT for learning!                                     │
│   ──────────    ┌───────┐    ┌───────┐    ┌───────┐                         │
│                 │Kafka 1│    │Kafka 2│    │Kafka 3│                         │
│                 └───────┘    └───────┘    └───────┘                         │
│                                                                             │
│                 • Quorum = 2 (majority of 3)                                │
│                 • Can lose 1 broker and continue                            │
│                 • Replication factor 3 = industry standard                  │
│                 • Small enough for a laptop                                 │
│                 • Large enough for ALL concepts                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### What You Can Demonstrate

| Concept | Why 3 Brokers is Needed |
|---------|------------------------|
| **Replication** | RF=3 means data exists on all 3 brokers |
| **Leader Election** | Kill the leader, watch a follower take over |
| **ISR** | See replicas join/leave the in-sync set |
| **min.insync.replicas=2** | Majority requirement makes sense with 3 |
| **Fault Tolerance** | Survive 1 failure, fail on 2 failures |
| **Quorum** | Controller needs 2/3 to function |
| **Partition Spread** | See partitions distributed across brokers |
| **acks=all** | See writes wait for all replicas |

### Voice Script - Why 3 Brokers

> "Why are we setting up a 3-broker cluster? Because 3 is the magic number for distributed systems.
>
> With 1 broker, you can't demonstrate replication or failover. There's nothing distributed about it.
>
> With 2 brokers, you have a problem - there's no quorum possible. Quorum means majority - more than 50%. With 2 nodes, if 1 dies, the remaining node has exactly 50%, not more than 50%. The system can't make decisions. This is why 2-node clusters are actually worse than 1-node for availability.
>
> With 3 brokers, everything works. Quorum is 2 out of 3. You can lose 1 broker and the remaining 2 still form a majority. You can set replication factor to 3, which is the industry standard. You can demonstrate leader election, ISR changes, fault tolerance - all the concepts you need to understand Kafka.
>
> And 3 brokers is small enough to run on your laptop. We don't need 5 or 7 brokers for learning - that just wastes resources. 3 gives us everything we need."

---

## Cluster Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    3-BROKER KAFKA CLUSTER ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                         CONTROLLER QUORUM (KRaft)                           │
│                    ┌─────────────────────────────────┐                      │
│                    │  All 3 brokers are controllers  │                      │
│                    │  Quorum = 2 (majority of 3)     │                      │
│                    └─────────────────────────────────┘                      │
│                                                                             │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│   │    BROKER 1     │  │    BROKER 2     │  │    BROKER 3     │            │
│   │   node.id = 1   │  │   node.id = 2   │  │   node.id = 3   │            │
│   │   Port: 9092    │  │   Port: 9094    │  │   Port: 9096    │            │
│   │                 │  │                 │  │                 │            │
│   │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │            │
│   │ │ orders-P0   │ │  │ │ orders-P0   │ │  │ │ orders-P0   │ │            │
│   │ │  LEADER     │◀┼──┼▶│  FOLLOWER   │◀┼──┼▶│  FOLLOWER   │ │            │
│   │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │            │
│   │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │            │
│   │ │ orders-P1   │ │  │ │ orders-P1   │ │  │ │ orders-P1   │ │            │
│   │ │  FOLLOWER   │◀┼──┼▶│  LEADER     │◀┼──┼▶│  FOLLOWER   │ │            │
│   │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │            │
│   │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │            │
│   │ │ orders-P2   │ │  │ │ orders-P2   │ │  │ │ orders-P2   │ │            │
│   │ │  FOLLOWER   │◀┼──┼▶│  FOLLOWER   │◀┼──┼▶│  LEADER     │ │            │
│   │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │            │
│   │                 │  │                 │  │                 │            │
│   │  Controller:    │  │  Controller:    │  │  Controller:    │            │
│   │  ACTIVE         │  │  STANDBY        │  │  STANDBY        │            │
│   └─────────────────┘  └─────────────────┘  └─────────────────┘            │
│           │                    │                    │                       │
│           └────────────────────┼────────────────────┘                       │
│                                │                                            │
│                    ┌───────────▼───────────┐                                │
│                    │   __cluster_metadata  │                                │
│                    │   (replicated via     │                                │
│                    │    Raft protocol)     │                                │
│                    └───────────────────────┘                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Port Mapping

| Broker | Internal Port | External Port | Docker Port |
|--------|---------------|---------------|-------------|
| kafka1 | 19092 | 9092 | 29092 |
| kafka2 | 19094 | 9094 | 29094 |
| kafka3 | 19096 | 9096 | 29096 |

---

## Docker Compose Setup

The `docker-compose-multi-broker.yml` file sets up a 3-broker cluster:

```yaml
services:
  kafka1:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka1
    container_name: kafka1
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka1:19092,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9092,DOCKER://host.docker.internal:29092
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://kafka1:9093,INTERNAL://kafka1:19092,EXTERNAL://0.0.0.0:9092,DOCKER://0.0.0.0:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

  kafka2:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka2
    container_name: kafka2
    ports:
      - "9094:9094"
      - "29094:29094"
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka2:19094,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9094,DOCKER://host.docker.internal:29094
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://kafka2:9093,INTERNAL://kafka2:19094,EXTERNAL://0.0.0.0:9094,DOCKER://0.0.0.0:29094
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

  kafka3:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka3
    container_name: kafka3
    ports:
      - "9096:9096"
      - "29096:29096"
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka3:19096,EXTERNAL://${DOCKER_HOST_IP:-127.0.0.1}:9096,DOCKER://host.docker.internal:29096
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://kafka3:9093,INTERNAL://kafka3:19096,EXTERNAL://0.0.0.0:9096,DOCKER://0.0.0.0:29096
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
```

### Key Configuration Explained

| Config | Value | Why |
|--------|-------|-----|
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka1:9093,2@kafka2:9093,3@kafka3:9093` | All 3 brokers participate in controller quorum |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | `3` | New topics get 3 replicas by default |
| `KAFKA_MIN_INSYNC_REPLICAS` | `2` | Writes need 2 replicas to acknowledge |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `3` | Consumer offsets replicated across all brokers |
| `CLUSTER_ID` | Same for all | All brokers must share the same cluster ID |

### Voice Script - Cluster Setup

> "Let's look at our 3-broker cluster configuration.
>
> Each broker has a unique node ID - 1, 2, and 3. They all share the same cluster ID, which identifies them as part of the same cluster.
>
> The key configuration is KAFKA_CONTROLLER_QUORUM_VOTERS. This lists all three brokers as participants in the controller quorum. In KRaft mode, the controller quorum uses the Raft consensus protocol. With 3 voters, we need 2 to agree on any decision - that's our majority.
>
> We set KAFKA_DEFAULT_REPLICATION_FACTOR to 3, so every topic we create will have its data on all 3 brokers. And KAFKA_MIN_INSYNC_REPLICAS is 2, meaning at least 2 brokers must acknowledge a write before we consider it committed.
>
> This setup gives us the foundation to demonstrate every important Kafka concept."

---

## Starting the Cluster

### Launch the Cluster

```bash
docker-compose -f docker-compose-multi-broker.yml up -d
```

### Verify All Brokers Are Running

```bash
docker-compose -f docker-compose-multi-broker.yml ps
```

Expected output:
```
NAME      COMMAND                  SERVICE   STATUS    PORTS
kafka1    "/etc/confluent/dock…"   kafka1    running   0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp
kafka2    "/etc/confluent/dock…"   kafka2    running   0.0.0.0:9094->9094/tcp, 0.0.0.0:29094->29094/tcp
kafka3    "/etc/confluent/dock…"   kafka3    running   0.0.0.0:9096->9096/tcp, 0.0.0.0:29096->29096/tcp
```

### Check Cluster Metadata

```bash
docker exec kafka1 kafka-metadata --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log --command "cat /brokers"
```

---

## Hands-On Demonstrations

### Demo 1: Replication in Action

**Concept:** Data is automatically replicated across all 3 brokers.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    REPLICATION DEMONSTRATION                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Step 1: Create topic with RF=3                                            │
│                                                                             │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                              │
│   │ Broker 1│     │ Broker 2│     │ Broker 3│                              │
│   │ (empty) │     │ (empty) │     │ (empty) │                              │
│   └─────────┘     └─────────┘     └─────────┘                              │
│                                                                             │
│   Step 2: Producer writes "Hello"                                           │
│                                                                             │
│   Producer ───▶ Broker 1 (Leader)                                           │
│                     │                                                       │
│                     ├───▶ Broker 2 (Follower) - replicates                  │
│                     └───▶ Broker 3 (Follower) - replicates                  │
│                                                                             │
│   Step 3: All brokers have the data                                         │
│                                                                             │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                              │
│   │ Broker 1│     │ Broker 2│     │ Broker 3│                              │
│   │ "Hello" │     │ "Hello" │     │ "Hello" │                              │
│   │ (Leader)│     │(Follower│     │(Follower│                              │
│   └─────────┘     └─────────┘     └─────────┘                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Commands:**

```bash
# Create a topic with 3 partitions and replication factor 3
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic demo-topic --partitions 3 --replication-factor 3

# Describe the topic - see how partitions are distributed
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic demo-topic
```

**Expected Output:**
```
Topic: demo-topic    TopicId: xxxxx    PartitionCount: 3    ReplicationFactor: 3
    Topic: demo-topic    Partition: 0    Leader: 1    Replicas: 1,2,3    Isr: 1,2,3
    Topic: demo-topic    Partition: 1    Leader: 2    Replicas: 2,3,1    Isr: 2,3,1
    Topic: demo-topic    Partition: 2    Leader: 3    Replicas: 3,1,2    Isr: 3,1,2
```

**What to observe:**
- Each partition has a different leader (distributed leadership)
- Replicas lists all 3 brokers for each partition
- ISR (In-Sync Replicas) shows all 3 are in sync

### Voice Script - Replication Demo

> "Let's see replication in action. I'll create a topic with 3 partitions and replication factor 3.
>
> When I describe the topic, notice a few things. First, each partition has a different leader - partition 0 is led by broker 1, partition 1 by broker 2, partition 2 by broker 3. Kafka automatically distributes leadership to balance the load.
>
> Second, look at the Replicas column - every partition shows 1,2,3 or some permutation. This means every partition exists on all 3 brokers.
>
> Third, the ISR column - In-Sync Replicas - also shows all 3 brokers. This means all replicas are caught up with the leader. They're synchronized and ready to take over if needed.
>
> This is replication in action. Our data isn't sitting on just one broker - it's on all three."

---

### Demo 2: Leader Election (Failover)

**Concept:** When a leader fails, a follower automatically becomes the new leader.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LEADER ELECTION DEMONSTRATION                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   BEFORE: Broker 1 is leader for partition 0                                │
│                                                                             │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                              │
│   │ Broker 1│     │ Broker 2│     │ Broker 3│                              │
│   │  P0: L  │     │  P0: F  │     │  P0: F  │                              │
│   │   ✓     │     │   ✓     │     │   ✓     │                              │
│   └─────────┘     └─────────┘     └─────────┘                              │
│                                                                             │
│                                                                             │
│   ACTION: Stop Broker 1                                                     │
│                                                                             │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                              │
│   │ Broker 1│     │ Broker 2│     │ Broker 3│                              │
│   │   💀    │     │  P0: ?  │     │  P0: ?  │                              │
│   │ (down)  │     │   ✓     │     │   ✓     │                              │
│   └─────────┘     └─────────┘     └─────────┘                              │
│                                                                             │
│                                                                             │
│   AFTER: Broker 2 elected as new leader                                     │
│                                                                             │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                              │
│   │ Broker 1│     │ Broker 2│     │ Broker 3│                              │
│   │   💀    │     │  P0: L  │     │  P0: F  │                              │
│   │ (down)  │     │ (NEW!)  │     │   ✓     │                              │
│   └─────────┘     └─────────┘     └─────────┘                              │
│                                                                             │
│   ISR changes from [1,2,3] to [2,3]                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Commands:**

```bash
# Check current leader for partition 0
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic demo-topic | grep "Partition: 0"

# Stop broker 1
docker stop kafka1

# Wait a few seconds, then check leader again (connect to kafka2 now)
docker exec kafka2 kafka-topics --bootstrap-server localhost:9094 \
  --describe --topic demo-topic | grep "Partition: 0"

# Observe the ISR has shrunk
docker exec kafka2 kafka-topics --bootstrap-server localhost:9094 \
  --describe --topic demo-topic

# Restart broker 1
docker start kafka1

# Wait a few seconds, ISR should include broker 1 again
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic demo-topic
```

**What to observe:**
- Leader changes from broker 1 to another broker
- ISR shrinks from [1,2,3] to [2,3]
- After restart, broker 1 rejoins ISR
- **No data loss!** - All messages still available

### Voice Script - Leader Election Demo

> "Now let's demonstrate leader election. I'll stop broker 1 and watch what happens.
>
> Before stopping, partition 0 has broker 1 as its leader, with all three brokers in the ISR.
>
> I stop broker 1 with docker stop kafka1. After a few seconds, I check the topic description again - but now I connect to kafka2 since kafka1 is down.
>
> Look at the changes. Partition 0 now has a new leader - broker 2 or 3. This happened automatically, no manual intervention. The ISR now shows only 2 brokers - the one that died is no longer in sync.
>
> Now I restart broker 1. After it comes back up and catches up, watch the ISR. It expands back to include all 3 brokers. Broker 1 rejoined the in-sync set.
>
> The key point: during all of this, no data was lost. Producers and consumers could continue working with the remaining brokers. This is fault tolerance in action."

---

### Demo 3: ISR and min.insync.replicas

**Concept:** Writes require acknowledgment from a minimum number of replicas.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ISR AND MIN.INSYNC.REPLICAS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   CONFIGURATION:                                                            │
│   • replication.factor = 3                                                  │
│   • min.insync.replicas = 2                                                 │
│   • acks = all                                                              │
│                                                                             │
│                                                                             │
│   SCENARIO 1: All brokers healthy (ISR = 3)                                 │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                              │
│   │ Broker 1│     │ Broker 2│     │ Broker 3│                              │
│   │  (ISR)  │     │  (ISR)  │     │  (ISR)  │                              │
│   └─────────┘     └─────────┘     └─────────┘                              │
│        │               │               │                                    │
│        └───────────────┴───────────────┘                                    │
│                        │                                                    │
│                   ISR size = 3                                              │
│                   min.insync.replicas = 2                                   │
│                   3 >= 2 ✓ WRITES ALLOWED                                   │
│                                                                             │
│                                                                             │
│   SCENARIO 2: One broker down (ISR = 2)                                     │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                              │
│   │ Broker 1│     │ Broker 2│     │ Broker 3│                              │
│   │   💀    │     │  (ISR)  │     │  (ISR)  │                              │
│   └─────────┘     └─────────┘     └─────────┘                              │
│                        │               │                                    │
│                        └───────────────┘                                    │
│                               │                                             │
│                          ISR size = 2                                       │
│                          min.insync.replicas = 2                            │
│                          2 >= 2 ✓ WRITES STILL ALLOWED                      │
│                                                                             │
│                                                                             │
│   SCENARIO 3: Two brokers down (ISR = 1)                                    │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                              │
│   │ Broker 1│     │ Broker 2│     │ Broker 3│                              │
│   │   💀    │     │   💀    │     │  (ISR)  │                              │
│   └─────────┘     └─────────┘     └─────────┘                              │
│                                        │                                    │
│                                   ISR size = 1                              │
│                                   min.insync.replicas = 2                   │
│                                   1 < 2 ✗ WRITES REJECTED!                  │
│                                                                             │
│   Producer receives: NotEnoughReplicasException                             │
│   This prevents data loss - better to reject than lose data                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Commands:**

```bash
# Create a topic with min.insync.replicas = 2
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic isr-demo --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2

# Start a producer with acks=all
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic isr-demo --producer-property acks=all

# In another terminal, check ISR
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic isr-demo

# Stop broker 2
docker stop kafka2

# Check ISR again - should show only 2 brokers
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic isr-demo

# Try to produce - should still work (ISR=2 >= min.insync.replicas=2)

# Stop broker 3
docker stop kafka3

# Check ISR - only 1 broker left
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic isr-demo

# Try to produce - should FAIL with NotEnoughReplicasException
# because ISR=1 < min.insync.replicas=2

# Restart brokers
docker start kafka2 kafka3
```

### Voice Script - ISR Demo

> "Let's demonstrate how min.insync.replicas protects your data.
>
> We have a topic with replication factor 3 and min.insync.replicas set to 2. The producer uses acks equals all, meaning it waits for all ISR replicas to acknowledge.
>
> With all 3 brokers healthy, ISR is 3. The producer writes successfully because 3 is greater than or equal to 2.
>
> Now I stop broker 2. The ISR shrinks to 2. The producer can still write because 2 equals our minimum requirement.
>
> Here's the critical part. I stop broker 3 as well. Now only broker 1 remains, ISR is 1. Watch what happens when I try to produce.
>
> The producer fails with NotEnoughReplicasException. Kafka refuses to accept the write because it can't guarantee durability with only 1 replica.
>
> This is exactly what we want! It's better to reject a write than to accept it and potentially lose it. The producer can retry when more brokers come back online.
>
> When I restart the brokers, the ISR expands back to 3, and writes succeed again. This is how Kafka maintains data consistency even during failures."

---

### Demo 4: Partition Distribution

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
│   LEADERS PER BROKER:                                                       │
│   • Broker 1: 2 leaders (P0, P3)                                           │
│   • Broker 2: 2 leaders (P1, P4)                                           │
│   • Broker 3: 2 leaders (P2, P5)                                           │
│                                                                             │
│   BALANCED! Each broker handles equal leadership responsibility             │
│                                                                             │
│                                                                             │
│   CONSUMER GROUP (6 consumers):                                             │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │  C1 ◀── P0    C2 ◀── P1    C3 ◀── P2                           │       │
│   │  C4 ◀── P3    C5 ◀── P4    C6 ◀── P5                           │       │
│   │                                                                 │       │
│   │  Maximum parallelism: 6 consumers for 6 partitions             │       │
│   └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Commands:**

```bash
# Create a topic with 6 partitions
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 6 --replication-factor 3

# Describe to see distribution
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic orders

# Check leader distribution
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic orders | grep "Leader:" | sort | uniq -c
```

### Voice Script - Partition Distribution

> "Let's look at how Kafka distributes partitions across brokers.
>
> I create a topic called orders with 6 partitions and replication factor 3. When I describe it, notice how leadership is distributed.
>
> Partitions 0 and 3 have broker 1 as leader. Partitions 1 and 4 have broker 2. Partitions 2 and 5 have broker 3. Each broker leads exactly 2 partitions - it's balanced.
>
> Why does this matter? Leadership means handling all reads and writes for that partition. By distributing leadership evenly, Kafka balances the load across brokers. No single broker becomes a bottleneck.
>
> This also means if broker 1 fails, we only lose leadership for 2 partitions, not all 6. Those partitions failover to other brokers while the remaining 4 continue unaffected.
>
> And for consumers, 6 partitions means we can have up to 6 parallel consumers in a group. Each consumer handles one partition, giving us maximum throughput."

---

### Demo 5: Producer Acknowledgments (acks)

**Concept:** The acks setting determines consistency vs performance trade-off.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PRODUCER ACKNOWLEDGMENTS                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   acks=0: Fire and Forget                                                   │
│   ─────────────────────────                                                 │
│                                                                             │
│   Producer ──▶ Broker 1 (Leader)                                            │
│       │          │                                                          │
│       │          │──▶ Broker 2 (maybe)                                      │
│       │          │──▶ Broker 3 (maybe)                                      │
│       │          │                                                          │
│       ▼          ▼                                                          │
│   Continue    No guarantee                                                  │
│   immediately  of write                                                     │
│                                                                             │
│   FASTEST | LEAST SAFE | Use for: metrics, logs                             │
│                                                                             │
│                                                                             │
│   acks=1: Leader Only                                                       │
│   ─────────────────────                                                     │
│                                                                             │
│   Producer ──▶ Broker 1 (Leader) ──▶ Write to disk                          │
│       │          │                      │                                   │
│       │◀─ ACK ───┘                      │                                   │
│       │                                 │──▶ Broker 2 (later)               │
│       ▼                                 └──▶ Broker 3 (later)               │
│   Continue                                                                  │
│                                                                             │
│   BALANCED | MEDIUM SAFE | Use for: general purpose                         │
│                                                                             │
│                                                                             │
│   acks=all: All In-Sync Replicas                                            │
│   ──────────────────────────────                                            │
│                                                                             │
│   Producer ──▶ Broker 1 (Leader)                                            │
│       │          │                                                          │
│       │          │──▶ Broker 2 ──▶ Write to disk ──┐                        │
│       │          │                                 │                        │
│       │          │──▶ Broker 3 ──▶ Write to disk ──┤                        │
│       │          │                                 │                        │
│       │          │◀───────────────────────────────┘                        │
│       │◀─ ACK ───┘ (after all ISR confirm)                                  │
│       │                                                                     │
│       ▼                                                                     │
│   Continue (data is SAFE on all replicas)                                   │
│                                                                             │
│   SLOWEST | MOST SAFE | Use for: financial, orders, critical data           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Commands:**

```bash
# Producer with acks=0 (fastest, unsafe)
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic demo-topic --producer-property acks=0

# Producer with acks=1 (balanced)
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic demo-topic --producer-property acks=1

# Producer with acks=all (safest)
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic demo-topic --producer-property acks=all
```

**Demonstration:**

```bash
# Test with acks=all while stopping brokers to see the difference

# Terminal 1: Start producer with acks=all
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic isr-demo --producer-property acks=all

# Terminal 2: Stop brokers one by one and observe producer behavior
docker stop kafka2
# Producer still works (ISR=2 >= min.insync.replicas=2)

docker stop kafka3
# Producer BLOCKS or fails (ISR=1 < min.insync.replicas=2)
```

### Voice Script - Producer Acks

> "The acks setting is one of the most important producer configurations. It determines the trade-off between speed and safety.
>
> With acks equals 0, fire and forget. The producer sends the message and immediately continues without waiting. It's the fastest option but provides no guarantee - the message might be lost if the broker crashes before writing to disk. Use this for metrics or logs where losing some data is acceptable.
>
> With acks equals 1, the producer waits for the leader to acknowledge. This is the default and provides a good balance. The message is written to the leader's disk, but if the leader crashes before replication, you might lose it.
>
> With acks equals all, the producer waits for all in-sync replicas to acknowledge. This is the safest option. Your message exists on multiple brokers before you get confirmation. Use this for financial transactions, orders, or any critical data.
>
> Let me demonstrate. I start a producer with acks equals all while monitoring the cluster. When I stop a broker, the producer still works because we have 2 replicas, meeting our minimum. But when I stop a second broker, the producer blocks - it can't get enough acknowledgments. This is exactly what we want for data safety."

---

### Demo 6: Consumer Group Rebalancing

**Concept:** When consumers join or leave, partitions are automatically redistributed.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONSUMER GROUP REBALANCING                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   INITIAL STATE: 2 consumers, 6 partitions                                  │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │  Consumer 1: P0, P1, P2                                         │       │
│   │  Consumer 2: P3, P4, P5                                         │       │
│   └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
│                                                                             │
│   ADD CONSUMER 3: Rebalance!                                                │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │  Consumer 1: P0, P1                                             │       │
│   │  Consumer 2: P2, P3                                             │       │
│   │  Consumer 3: P4, P5                                             │       │
│   └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
│                                                                             │
│   CONSUMER 2 DIES: Rebalance!                                               │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │  Consumer 1: P0, P1, P2                                         │       │
│   │  Consumer 2: (dead)                                             │       │
│   │  Consumer 3: P3, P4, P5                                         │       │
│   └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
│   Automatic redistribution ensures all partitions are consumed              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Commands:**

```bash
# Terminal 1: Start consumer 1
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --group my-group --from-beginning

# Terminal 2: Start consumer 2
docker exec -it kafka2 kafka-console-consumer --bootstrap-server localhost:9094 \
  --topic orders --group my-group --from-beginning

# Terminal 3: Watch the consumer group
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-group

# Add more consumers or kill one and observe rebalancing
```

### Voice Script - Consumer Rebalancing

> "Let's see how consumer groups automatically balance work across consumers.
>
> I have a topic with 6 partitions. I start one consumer in the group. It gets all 6 partitions - it's doing all the work alone.
>
> Now I start a second consumer in the same group. Watch what happens - a rebalance occurs. The partitions are redistributed. Now each consumer handles 3 partitions. The workload is shared.
>
> If I start a third consumer, another rebalance. Each consumer now handles 2 partitions.
>
> What if a consumer dies? I'll kill consumer 2. After the rebalance, its partitions are distributed to the remaining consumers. No messages are lost, processing continues.
>
> This is the beauty of consumer groups. You can scale your processing by adding consumers, and Kafka handles the coordination automatically. If a consumer fails, others pick up the slack."

---

## Summary: What You Can Teach with 3 Brokers

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TEACHING SUMMARY                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   CONCEPT                  │ DEMONSTRATION                                  │
│   ─────────────────────────┼──────────────────────────────────────────────  │
│                            │                                                │
│   Replication              │ Create topic with RF=3, see data on all       │
│                            │ brokers                                        │
│                            │                                                │
│   Leader Election          │ Stop a broker, watch leadership transfer      │
│                            │                                                │
│   ISR                      │ Stop brokers, watch ISR shrink/expand         │
│                            │                                                │
│   min.insync.replicas      │ Drop below minimum, see writes rejected       │
│                            │                                                │
│   Partition Distribution   │ Create multi-partition topic, see balance     │
│                            │                                                │
│   acks Configuration       │ Compare acks=0, 1, all behavior               │
│                            │                                                │
│   Consumer Groups          │ Add/remove consumers, watch rebalancing       │
│                            │                                                │
│   Fault Tolerance          │ Kill 1 broker - system continues              │
│                            │ Kill 2 brokers - system stops (safely)        │
│                            │                                                │
│   Controller Quorum        │ Active controller fails, standby takes over   │
│                            │                                                │
│   Data Consistency         │ Produce with acks=all, verify no loss         │
│                            │                                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Reference Commands

```bash
# Start cluster
docker-compose -f docker-compose-multi-broker.yml up -d

# Stop cluster
docker-compose -f docker-compose-multi-broker.yml down

# Check all brokers
docker-compose -f docker-compose-multi-broker.yml ps

# Create topic with replication
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic my-topic --partitions 3 --replication-factor 3

# Describe topic (see leaders, ISR)
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic my-topic

# Stop a broker
docker stop kafka1

# Start a broker
docker start kafka1

# Check consumer group
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-group

# Produce with acks=all
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic my-topic --producer-property acks=all

# Consume
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic my-topic --group my-group --from-beginning
```

---

## Voice Script - Wrap Up

> "And that's why we use a 3-broker cluster for learning Kafka. It's the minimum setup that allows us to demonstrate all the important distributed concepts: replication, leader election, ISR, fault tolerance, quorum, and consistency.
>
> You can run all these demonstrations on your laptop. The three brokers use minimal resources but give us everything we need to understand how Kafka works in production.
>
> Remember: 1 broker can't show distribution, 2 brokers can't achieve quorum, but 3 brokers is the sweet spot. It's the foundation of every production Kafka deployment, and now you understand why.
>
> Practice these demos. Stop brokers, watch what happens, restart them. That hands-on experience will give you deep understanding of Kafka's distributed nature."
