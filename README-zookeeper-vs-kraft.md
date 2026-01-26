# Zookeeper vs KRaft: Kafka Metadata Management

## Overview

Kafka traditionally relied on Apache Zookeeper for cluster metadata management. Starting with Kafka 2.8 (KIP-500), a new consensus protocol called KRaft (Kafka Raft) was introduced, allowing Kafka to manage its own metadata without external dependencies.

## Architecture Comparison

### Zookeeper Mode (Legacy)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Zookeeper  │     │  Zookeeper  │     │  Zookeeper  │
│   Node 1    │────▶│   Node 2    │────▶│   Node 3    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
       ┌───────────────────┼───────────────────┐
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka     │     │   Kafka     │     │   Kafka     │
│  Broker 1   │     │  Broker 2   │     │  Broker 3   │
└─────────────┘     └─────────────┘     └─────────────┘
```

### KRaft Mode (Modern)

```
┌─────────────────────────────────────────────────────┐
│                   Kafka Cluster                      │
│                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │   Kafka     │  │   Kafka     │  │   Kafka     │  │
│  │  Broker 1   │  │  Broker 2   │  │  Broker 3   │  │
│  │ (Controller)│  │ (Controller)│  │ (Controller)│  │
│  └─────────────┘  └─────────────┘  └─────────────┘  │
│         │                │                │         │
│         └────────────────┼────────────────┘         │
│                          │                          │
│                    Raft Consensus                   │
└─────────────────────────────────────────────────────┘
```

## Feature Comparison

| Feature | Zookeeper Mode | KRaft Mode |
|---------|---------------|------------|
| External dependency | Requires Zookeeper cluster | Self-contained |
| Metadata storage | Zookeeper znodes | Internal Raft log |
| Controller election | Via Zookeeper | Via Raft consensus |
| Partition limit | ~200,000 | Millions |
| Recovery time | Minutes | Seconds |
| Operational complexity | High (two systems) | Low (single system) |
| Resource usage | Higher (separate JVMs) | Lower |
| Configuration | Complex | Simplified |
| Monitoring | Two systems | Single system |

## Advantages of KRaft

### 1. Simplified Architecture

- **Single System**: No need to deploy, configure, and maintain a separate Zookeeper cluster
- **Unified Monitoring**: Monitor only Kafka instead of two distributed systems
- **Easier Deployment**: Fewer moving parts mean simpler deployment and upgrades

### 2. Improved Scalability

- **Higher Partition Limits**: KRaft can handle millions of partitions compared to ~200,000 with Zookeeper
- **Faster Metadata Propagation**: Metadata changes propagate more efficiently through the Raft protocol
- **Better Resource Utilization**: No separate JVM processes for Zookeeper

### 3. Faster Recovery

- **Controller Failover**: New controller election takes seconds instead of minutes
- **Broker Startup**: Brokers start faster as metadata is loaded from local log
- **Partition Recovery**: Faster recovery after broker failures

### 4. Reduced Operational Overhead

| Operation | Zookeeper Mode | KRaft Mode |
|-----------|---------------|------------|
| Cluster setup | Configure both Kafka and Zookeeper | Configure Kafka only |
| Upgrades | Coordinate upgrades of two systems | Single system upgrade |
| Backup | Back up both systems | Single backup strategy |
| Security | Secure both systems | Single security configuration |

### 5. Better Consistency

- **Single Source of Truth**: Metadata stored in Kafka's own commit log
- **Stronger Guarantees**: Raft protocol provides clear consistency semantics
- **No Split-Brain**: Improved handling of network partitions

### 6. Lower Latency

- **Direct Communication**: Controllers communicate directly using Raft
- **No Zookeeper Round-trips**: Eliminates network hops to external system
- **Efficient Metadata Updates**: Changes committed with single Raft round

## KRaft Terminology

| Term | Description |
|------|-------------|
| Controller | Node responsible for cluster metadata management |
| Quorum | Set of controllers that participate in Raft consensus |
| Active Controller | The elected leader among controllers |
| Broker | Node that handles client requests and stores data |
| Combined Mode | Single node acts as both controller and broker |

## Process Roles

In KRaft mode, each node can have one or more roles:

| Role | Responsibility |
|------|----------------|
| `controller` | Participates in metadata quorum, manages cluster state |
| `broker` | Handles produce/consume requests, stores partitions |
| `broker,controller` | Combined mode - performs both roles |

### Deployment Patterns

**Development (Combined Mode)**
```
Node 1: broker,controller
```

**Small Production (Combined Mode)**
```
Node 1: broker,controller
Node 2: broker,controller
Node 3: broker,controller
```

**Large Production (Dedicated Controllers)**
```
Node 1-3: controller (dedicated quorum)
Node 4-N: broker (data nodes)
```

## Migration Path

### Zookeeper to KRaft Migration

1. **Kafka 3.3+**: KRaft became production-ready
2. **Kafka 3.5+**: Migration tooling available
3. **Kafka 4.0**: Zookeeper support removed

### Migration Steps (High-Level)

1. Upgrade to Kafka 3.5+ (if not already)
2. Deploy KRaft controllers alongside existing cluster
3. Run migration tool to transfer metadata
4. Switch brokers to KRaft mode
5. Decommission Zookeeper

## Configuration Comparison

### Zookeeper Mode Configuration

```properties
# Broker configuration
broker.id=1
zookeeper.connect=zoo1:2181,zoo2:2181,zoo3:2181
```

### KRaft Mode Configuration

```properties
# Node configuration
node.id=1
process.roles=broker,controller
controller.quorum.voters=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
controller.listener.names=CONTROLLER
```

## When to Use Each Mode

### Use KRaft When

- Starting new Kafka deployments
- Running Kafka 3.3 or later
- Requiring high partition counts
- Wanting simplified operations
- Need faster failover times

### Consider Zookeeper When

- Running Kafka versions before 3.3
- Existing production clusters not ready for migration
- Dependent tooling requires Zookeeper

## Summary

KRaft represents the future of Kafka, offering significant improvements in scalability, simplicity, and performance. New deployments should use KRaft mode, and existing Zookeeper-based clusters should plan migration as part of their Kafka upgrade path.

| Aspect | Winner | Reason |
|--------|--------|--------|
| Simplicity | KRaft | Single system to manage |
| Scalability | KRaft | Millions of partitions |
| Performance | KRaft | Faster failover and recovery |
| Maturity | Zookeeper | Years of production use |
| Future | KRaft | Zookeeper support removed in Kafka 4.0 |
