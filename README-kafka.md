# Kafka Local Development Setup

This guide covers running Kafka locally using Docker with KRaft mode (no Zookeeper required).

## Prerequisites

- Docker and Docker Compose installed
- Ports 9092 and 29092 available (single broker)
- Ports 9092-9094 and 29092-29094 available (multi-broker)

## Starting Kafka

### Single Broker

```bash
docker-compose up -d
```

### Multi-Broker (3 nodes)

```bash
docker-compose -f docker-compose-multi-broker.yml up -d
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

| Connection Type | Bootstrap Server |
|-----------------|------------------|
| From host machine | `localhost:9092` |
| From Docker containers | `host.docker.internal:29092` |
| Internal (between containers) | `kafka1:19092` |

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
