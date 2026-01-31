# KRaft Mode - Introduction Slides

## Slide 1: What is KRaft? - Kafka Without ZooKeeper

**Slide Content:**
```
What is KRaft Mode?

KRaft = Kafka Raft (Consensus Protocol)

┌─────────────────────────────────────────────────────────────┐
│                    THE OLD WAY (Pre-Kafka 3.x)              │
│                                                             │
│   ┌─────────────┐         ┌─────────────────────────────┐  │
│   │  ZooKeeper  │◄───────►│      Kafka Cluster          │  │
│   │  Cluster    │         │  ┌───────┐ ┌───────┐        │  │
│   │ (3-5 nodes) │         │  │Broker1│ │Broker2│ ...    │  │
│   └─────────────┘         │  └───────┘ └───────┘        │  │
│         ▲                 └─────────────────────────────┘  │
│         │                                                   │
│   Stores: broker metadata, topic configs,                  │
│   partition leaders, ACLs, quotas                          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    THE NEW WAY (KRaft Mode)                 │
│                                                             │
│            ┌─────────────────────────────────────┐         │
│            │         Kafka Cluster               │         │
│            │  ┌───────┐ ┌───────┐ ┌───────┐     │         │
│            │  │Broker1│ │Broker2│ │Broker3│     │         │
│            │  │  +    │ │  +    │ │  +    │     │         │
│            │  │Controller│Controller│Controller│ │         │
│            │  └───────┘ └───────┘ └───────┘     │         │
│            └─────────────────────────────────────┘         │
│                                                             │
│   ✓ No external dependency - metadata managed internally   │
│   ✓ Uses Raft consensus protocol for leader election       │
└─────────────────────────────────────────────────────────────┘
```

**Voice Script:**
> "Before we set up our Kafka broker, let's understand KRaft mode and why it's important.
>
> KRaft stands for Kafka Raft, where Raft is a consensus protocol. To understand KRaft, we first need to understand how Kafka worked before.
>
> In the old architecture, Kafka depended on an external system called ZooKeeper. ZooKeeper was responsible for storing critical metadata: which brokers are alive, topic configurations, who is the leader for each partition, access control lists, and quotas. You had to set up and manage a separate ZooKeeper cluster, typically 3 to 5 nodes, alongside your Kafka cluster. This meant two distributed systems to manage, monitor, and secure.
>
> With KRaft mode, introduced in Kafka 2.8 and production-ready since Kafka 3.3, ZooKeeper is completely removed. Kafka now manages its own metadata internally using the Raft consensus protocol. Some Kafka brokers take on an additional role as 'controllers' and they form a quorum to manage cluster metadata. In many setups, especially smaller ones, the same nodes act as both brokers and controllers.
>
> This is a fundamental architectural shift that simplifies Kafka deployments significantly."

---

## Slide 2: Benefits of KRaft Mode

**Slide Content:**
```
Why KRaft? The Benefits

┌─────────────────────────────────────────────────────────────┐
│  1. SIMPLIFIED ARCHITECTURE                                 │
│     • One system instead of two                             │
│     • Single deployment, single security model              │
│     • Fewer moving parts = fewer failure points             │
│                                                             │
│  2. IMPROVED SCALABILITY                                    │
│     ┌────────────────────────────────────────────┐         │
│     │  ZooKeeper Mode    │    KRaft Mode         │         │
│     │  ~200K partitions  │    Millions of        │         │
│     │  (practical limit) │    partitions         │         │
│     └────────────────────────────────────────────┘         │
│     • Metadata stored in an efficient internal log          │
│     • Faster partition leader elections                     │
│                                                             │
│  3. FASTER RECOVERY                                         │
│     • Controller failover: seconds → milliseconds           │
│     • Broker shutdown/startup is significantly faster       │
│     • No ZooKeeper session timeouts to wait for             │
│                                                             │
│  4. EASIER OPERATIONS                                       │
│     • Single set of configurations                          │
│     • Unified monitoring and alerting                       │
│     • Simpler upgrades (one system, not two)                │
│     • Lower resource footprint (no separate ZK cluster)     │
└─────────────────────────────────────────────────────────────┘

KRaft is the FUTURE of Kafka
• ZooKeeper support deprecated in Kafka 3.5
• ZooKeeper will be removed in Kafka 4.0
• All new deployments should use KRaft
```

**Voice Script:**
> "So why should you care about KRaft? Let me walk you through the key benefits.
>
> First, simplified architecture. Instead of managing two distributed systems, you now manage one. This means a single deployment process, a single security model to configure, and fewer moving parts. Fewer components means fewer things that can fail and fewer things to troubleshoot.
>
> Second, dramatically improved scalability. With ZooKeeper, the practical limit was around 200,000 partitions per cluster. This was because ZooKeeper stored all partition metadata in memory and had to handle watch notifications for changes. With KRaft, Kafka can scale to millions of partitions. The metadata is stored in an efficient internal log, and partition leader elections happen much faster.
>
> Third, faster recovery. When a controller fails in ZooKeeper mode, failover could take several seconds due to session timeouts. With KRaft, controller failover happens in milliseconds because of the Raft protocol's efficient leader election. Broker restarts are also significantly faster since there's no waiting for ZooKeeper session establishment.
>
> Fourth, easier day-to-day operations. You have one set of configurations instead of two, unified monitoring, simpler upgrade procedures, and a lower overall resource footprint since you don't need dedicated ZooKeeper nodes.
>
> It's important to know that KRaft is not just an option, it's the future of Kafka. ZooKeeper support was deprecated in Kafka 3.5, and it will be completely removed in Kafka 4.0. All new Kafka deployments should use KRaft mode, and that's exactly what we'll be using throughout this course.
>
> In the next lecture, we'll set up a Kafka broker using KRaft mode and see how simple it is to get started."
