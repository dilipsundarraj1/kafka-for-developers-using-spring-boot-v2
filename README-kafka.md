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

## Producing Messages

### Interactive Producer (type messages manually)

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic my-topic
```

Type your messages and press Enter after each. Press `Ctrl+C` to exit.

### Producer with Keys

```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic my-topic \
  --property parse.key=true \
  --property key.separator=:
```

Format: `key:value` (e.g., `user1:Hello World`)

## Consuming Messages

### Consume New Messages

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic my-topic
```

### Consume from Beginning

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic my-topic --from-beginning
```

### Consume with Keys

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic my-topic --from-beginning \
  --property print.key=true \
  --property key.separator=:
```

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

### Container Shell Access

```bash
docker exec -it kafka1 bash
```
