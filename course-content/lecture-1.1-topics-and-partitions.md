# Lecture 1.1: Kafka Topics and Partitions - Theory

## Slide 1: Title Slide

**Slide Content:**
```
Kafka Topics and Partitions
Understanding the Core Building Blocks of Apache Kafka

Module 1 - Lecture 1
```

**Voice Script:**
> "Welcome to this lecture on Kafka Topics and Partitions. These are the fundamental building blocks of Apache Kafka, and understanding them deeply is essential before we start working with Kafka. In this lecture, we'll explore what topics and partitions are, how they work together, and why they're designed this way. By the end of this lecture, you'll have a solid mental model of how Kafka organizes and stores data."

---

## Slide 2: What is Apache Kafka? (Quick Recap)

**Slide Content:**
```
What is Apache Kafka?

• A distributed event streaming platform

• Used for:
  - Real-time data pipelines
  - Event-driven architectures
  - Log aggregation
  - Stream processing

• Think of it as a "distributed commit log"
```

**Voice Script:**
> "Before we dive into topics and partitions, let's quickly recap what Apache Kafka is. Kafka is a distributed event streaming platform. It's used for building real-time data pipelines, event-driven architectures, log aggregation, and stream processing. The key thing to remember is that Kafka is essentially a distributed commit log. It stores messages in an ordered, immutable sequence. This 'commit log' concept will become clearer as we explore topics and partitions."

---

## Slide 3: What is a Kafka Topic?

**Slide Content:**
```
What is a Kafka Topic?

• A Topic is a logical channel or category for messages

• Similar to:
  - A table in a database
  - A folder in a file system
  - A channel in a messaging system

• Examples:
  - "user-signups"
  - "order-events"
  - "payment-transactions"
  - "inventory-updates"

┌─────────────────────────────────────────┐
│           Topic: order-events           │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐       │
│  │msg 1│ │msg 2│ │msg 3│ │msg 4│  ...  │
│  └─────┘ └─────┘ └─────┘ └─────┘       │
└─────────────────────────────────────────┘
```

**Voice Script:**
> "So what exactly is a Kafka Topic? A topic is a logical channel or category for organizing messages. Think of it as a named stream of data. If you're coming from a database background, you can think of a topic as similar to a table. If you're familiar with file systems, think of it like a folder. And if you've worked with messaging systems before, it's like a channel.
>
> For example, in an e-commerce application, you might have topics like 'user-signups' for new user registrations, 'order-events' for all order-related activities, 'payment-transactions' for payment processing, and 'inventory-updates' for stock changes.
>
> The key idea is that topics help you organize your data by category or purpose. Producers write messages to topics, and consumers read messages from topics."

---

## Slide 4: Topic Characteristics

**Slide Content:**
```
Topic Characteristics

1. Named Entity
   • Each topic has a unique name within a Kafka cluster
   • Names are case-sensitive

2. Append-Only
   • Messages are always added at the end
   • You cannot insert or modify existing messages

3. Immutable Messages
   • Once written, messages cannot be changed
   • They can only be deleted based on retention policy

4. Multi-Subscriber
   • Multiple consumers can read from the same topic
   • Each consumer maintains its own position (offset)
```

**Voice Script:**
> "Let's understand the key characteristics of a Kafka topic.
>
> First, a topic is a named entity. Each topic has a unique name within a Kafka cluster, and these names are case-sensitive, so 'Orders' and 'orders' would be two different topics.
>
> Second, topics are append-only. This is very important. Messages are always added at the end of the topic. You cannot insert a message in the middle or modify an existing message. This append-only nature is what makes Kafka so fast and reliable.
>
> Third, messages in a topic are immutable. Once a message is written to a topic, it cannot be changed. The only way messages are removed is through Kafka's retention policy, which we'll cover in a later lecture.
>
> Fourth, topics support multiple subscribers. This means many different consumers can read from the same topic independently. Each consumer keeps track of its own position, called an offset. This is a powerful feature that enables many use cases."

---

## Slide 5: The Problem with a Single Log

**Slide Content:**
```
The Problem: What if we have only ONE log?

Single Log Architecture:
┌──────────────────────────────────────────────────┐
│                Topic: order-events               │
│ [msg1][msg2][msg3][msg4][msg5][msg6][msg7]...   │
└──────────────────────────────────────────────────┘
                      ↑
              Single Consumer
              (Processing one by one)

Problems:
• Limited throughput - only one consumer can process
• No parallelism - sequential processing only
• Single point of bottleneck
• Cannot scale horizontally

How do we scale this?
```

**Voice Script:**
> "Now, let's understand why we need partitions. Imagine we have a topic called 'order-events' and it's just one single log of messages. With this design, we have a problem.
>
> If we have a single consumer reading from this topic, it has to process messages one by one, sequentially. As the volume of messages increases, this single consumer becomes a bottleneck. We can't add more consumers to process messages in parallel because there's only one sequence of messages.
>
> This means limited throughput, no parallelism, and no way to scale horizontally. In a real-world scenario where you might have thousands or millions of messages per second, this would be completely unacceptable.
>
> So how do we solve this? The answer is partitions."

---

## Slide 6: Introducing Partitions

**Slide Content:**
```
Solution: Partitions

A Partition is a subset of a topic's data

┌─────────────────────────────────────────────────────┐
│                 Topic: order-events                 │
│                                                     │
│  Partition 0: [msg1][msg4][msg7][msg10]...         │
│                                                     │
│  Partition 1: [msg2][msg5][msg8][msg11]...         │
│                                                     │
│  Partition 2: [msg3][msg6][msg9][msg12]...         │
│                                                     │
└─────────────────────────────────────────────────────┘

• A topic is divided into one or more partitions
• Each partition is an independent, ordered log
• Partitions enable parallel processing
• You specify the number of partitions when creating a topic
```

**Voice Script:**
> "This is where partitions come in. A partition is a way to divide a topic's data into smaller, independent subsets. Instead of having one big log, we split the topic into multiple partitions.
>
> Look at this diagram. Our 'order-events' topic is now divided into three partitions: Partition 0, Partition 1, and Partition 2. Messages are distributed across these partitions. Message 1 goes to Partition 0, Message 2 goes to Partition 1, Message 3 goes to Partition 2, and then it cycles or distributes based on certain rules.
>
> Each partition is its own independent, ordered log. This is crucial to understand. Each partition maintains its own sequence of messages, independent of other partitions.
>
> When you create a topic in Kafka, you specify how many partitions you want. For example, you might create a topic with 3 partitions, or 10 partitions, or even 100 partitions depending on your throughput requirements."

---

## Slide 7: How Partitions Enable Parallelism

**Slide Content:**
```
Partitions Enable Parallel Processing

┌─────────────────────────────────────────────────────┐
│                 Topic: order-events                 │
│                                                     │
│  Partition 0: [msg1][msg4][msg7]    ← Consumer 1   │
│                                                     │
│  Partition 1: [msg2][msg5][msg8]    ← Consumer 2   │
│                                                     │
│  Partition 2: [msg3][msg6][msg9]    ← Consumer 3   │
│                                                     │
└─────────────────────────────────────────────────────┘

Benefits:
• 3 consumers can now process in parallel
• Throughput increases 3x
• Each consumer handles a subset of the data
• Easy to scale: add more partitions = add more consumers
```

**Voice Script:**
> "Now here's where the magic happens. With partitions, we can have multiple consumers reading in parallel.
>
> Look at this diagram. We have three partitions, and now we can have three consumers, each reading from one partition. Consumer 1 reads from Partition 0, Consumer 2 reads from Partition 1, and Consumer 3 reads from Partition 2.
>
> This means our throughput has effectively tripled. Instead of one consumer processing all messages sequentially, we now have three consumers working simultaneously, each handling a third of the data.
>
> And the beauty of this design is scalability. If we need more throughput, we can increase the number of partitions and add more consumers. This is how Kafka achieves horizontal scalability. This is a fundamental concept that you'll see throughout your work with Kafka."

---

## Slide 8: Partition Numbering and Offsets

**Slide Content:**
```
Partition Numbering and Offsets

Partitions are numbered starting from 0

                    Offsets (positions within partition)
                    ↓   ↓   ↓   ↓   ↓   ↓
┌────────────────────────────────────────────────────┐
│ Partition 0:  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ ...       │
│               [msg][msg][msg][msg][msg][msg]       │
├────────────────────────────────────────────────────┤
│ Partition 1:  │ 0 │ 1 │ 2 │ 3 │ 4 │ ...           │
│               [msg][msg][msg][msg][msg]            │
├────────────────────────────────────────────────────┤
│ Partition 2:  │ 0 │ 1 │ 2 │ 3 │ ...               │
│               [msg][msg][msg][msg]                 │
└────────────────────────────────────────────────────┘

• Offset: A unique identifier for a message within a partition
• Offsets start at 0 and increment by 1
• Offsets are NOT reused (even after deletion)
• Each partition maintains its own offset sequence
```

**Voice Script:**
> "Let's talk about partition numbering and offsets, which are essential concepts in Kafka.
>
> First, partitions are numbered starting from zero. So if you have three partitions, they'll be Partition 0, Partition 1, and Partition 2.
>
> Within each partition, every message has a unique identifier called an offset. Think of an offset as a position number or index. The first message in a partition has offset 0, the second message has offset 1, and so on.
>
> Here's something important: offsets are never reused. Even if older messages are deleted due to retention policies, the offset counter keeps incrementing. So if messages with offsets 0 through 100 are deleted, the next new message will still have offset 101, not 0.
>
> Also notice that each partition has its own independent offset sequence. Partition 0 has its own offsets starting at 0, Partition 1 has its own offsets starting at 0, and so on. An offset only has meaning within a specific partition."

---

## Slide 9: Message Ordering Guarantees

**Slide Content:**
```
Message Ordering Guarantees

IMPORTANT: Ordering is guaranteed ONLY within a partition

✅ Within a partition:
┌─────────────────────────────────────────┐
│ Partition 0: [A] → [B] → [C] → [D]     │
└─────────────────────────────────────────┘
Messages are always read in order: A, B, C, D

❌ Across partitions:
┌─────────────────────────────────────────┐
│ Partition 0: [A] → [C]                  │
│ Partition 1: [B] → [D]                  │
└─────────────────────────────────────────┘
No guarantee of order: Could be A,B,C,D or B,A,D,C or A,B,D,C...

This is a fundamental trade-off:
Partitions give us parallelism, but we lose global ordering
```

**Voice Script:**
> "This is one of the most important concepts to understand about Kafka: message ordering guarantees.
>
> Kafka guarantees message ordering ONLY within a single partition. If you send messages A, B, C, D to the same partition in that order, they will always be read in that exact order: A, B, C, D. This is guaranteed.
>
> However, there is NO ordering guarantee across different partitions. If message A and C go to Partition 0, and messages B and D go to Partition 1, you cannot predict the order in which a consumer will process them. It could be A, B, C, D or B, A, D, C or any other combination.
>
> This is a fundamental trade-off in distributed systems. Partitions give us parallelism and scalability, but we sacrifice global ordering. This is not a bug; it's a design decision. And as we'll see next, Kafka gives us tools to control which messages go to which partition when ordering matters."

---

## Slide 10: Message Keys - Controlling Partition Assignment

**Slide Content:**
```
Message Keys: Controlling Which Partition a Message Goes To

Every message can have an optional KEY

┌──────────────────────────────────────────────────────┐
│  Message Structure:                                  │
│  ┌─────────┬─────────────────────────────┐          │
│  │   Key   │           Value             │          │
│  │ (opt)   │    (actual message data)    │          │
│  └─────────┴─────────────────────────────┘          │
└──────────────────────────────────────────────────────┘

How partition is determined:

• If Key is NULL → Round-robin distribution (Kafka 2.4+: sticky)
• If Key is present → hash(key) % number_of_partitions

Example with Key = "order-123":
  hash("order-123") % 3 = 1  →  Goes to Partition 1

Same key ALWAYS goes to same partition (if partition count unchanged)
```

**Voice Script:**
> "Now let's talk about message keys, which give you control over partition assignment.
>
> Every message in Kafka has two main components: an optional key and a value. The value is your actual message data. The key is optional but very powerful.
>
> Here's how Kafka decides which partition a message goes to:
>
> If you don't provide a key, meaning the key is null, Kafka will distribute messages across partitions. In older versions, this was simple round-robin. Since Kafka 2.4, it uses a 'sticky partitioner' which batches messages to the same partition for better performance, but the messages still get distributed across all partitions over time.
>
> If you DO provide a key, Kafka uses a hash function. It calculates the hash of your key, then takes modulo of the number of partitions. For example, if hash of 'order-123' gives us some number, and we have 3 partitions, we calculate that number modulo 3, which might give us 1. So this message goes to Partition 1.
>
> The critical thing to understand is: the same key will ALWAYS go to the same partition, as long as the partition count doesn't change. This is how you maintain ordering for related messages."

---

## Slide 11: Use Case - Why Keys Matter

**Slide Content:**
```
Use Case: Order Processing System

Requirement: All events for the same order must be processed in order

Order Events:
  Order-123: CREATED → PAID → SHIPPED → DELIVERED

Without Key (messages scattered):
┌─────────────────────────────────────────────────────┐
│ P0: [SHIPPED-123]                                   │
│ P1: [CREATED-123] [DELIVERED-123]                   │
│ P2: [PAID-123]                                      │
└─────────────────────────────────────────────────────┘
❌ Order could appear: SHIPPED → CREATED → PAID → DELIVERED

With Key = "Order-123" (all go to same partition):
┌─────────────────────────────────────────────────────┐
│ P0: [...]                                           │
│ P1: [CREATED-123][PAID-123][SHIPPED-123][DELIVERED] │
│ P2: [...]                                           │
└─────────────────────────────────────────────────────┘
✅ Order guaranteed: CREATED → PAID → SHIPPED → DELIVERED
```

**Voice Script:**
> "Let me show you a real-world example of why keys matter.
>
> Imagine you're building an order processing system. For each order, you have multiple events: the order is created, then paid, then shipped, then delivered. It's crucial that these events are processed in the correct order. You can't process 'shipped' before 'created' or you'll have data consistency issues.
>
> Without using a message key, Kafka distributes these events across different partitions. The CREATED event might go to Partition 1, PAID to Partition 2, SHIPPED to Partition 0, and so on. When consumers process these in parallel, there's no guarantee of order. You might process SHIPPED before CREATED, which would break your application logic.
>
> But if you use the Order ID as the message key, something magical happens. All events for Order-123 will always go to the same partition. In this example, they all go to Partition 1. Now, within that partition, the order is guaranteed. CREATED comes before PAID, which comes before SHIPPED, which comes before DELIVERED.
>
> This is the pattern you'll use constantly in real-world Kafka applications. Use a logical identifier as your key to ensure related messages stay together and maintain their order."

---

## Slide 12: Choosing the Number of Partitions

**Slide Content:**
```
How Many Partitions Should You Create?

Factors to consider:

1. Desired Throughput
   • More partitions = more parallel consumers = higher throughput
   • If you need 100 MB/s and each consumer handles 10 MB/s
     → You need at least 10 partitions

2. Number of Consumers
   • Max consumers in a group = number of partitions
   • 6 partitions → maximum 6 consumers can work in parallel

3. Future Growth
   • You can increase partitions later
   • But decreasing is not recommended (breaks key ordering)

General Guidelines:
• Start with: partitions = max(t/p, c)
  - t = target throughput, p = throughput per partition
  - c = number of consumers you plan to run
• Common starting point: 3-6 partitions for moderate workloads
• High-volume topics: 12, 30, 50+ partitions
```

**Voice Script:**
> "A common question is: how many partitions should I create for my topic? There's no one-size-fits-all answer, but here are the factors to consider.
>
> First, think about your desired throughput. More partitions mean you can have more consumers processing in parallel, which means higher throughput. If you need to process 100 megabytes per second and each consumer can handle 10 megabytes per second, you need at least 10 partitions.
>
> Second, consider the number of consumers you plan to run. Here's an important rule: the maximum number of consumers that can actively consume from a topic in the same consumer group equals the number of partitions. If you have 6 partitions, you can have at most 6 consumers working in parallel. Any additional consumers will sit idle.
>
> Third, think about future growth. You CAN increase the number of partitions later, but be careful: increasing partitions will break key-based ordering for existing keys because the hash calculation will change. You cannot easily decrease partitions.
>
> As a general guideline, for moderate workloads, start with 3 to 6 partitions. For high-volume topics, you might use 12, 30, or even 50+ partitions. A good formula is to take the maximum of your throughput requirement divided by per-partition throughput, or the number of consumers you plan to run."

---

## Slide 13: Partitions and Kafka Brokers

**Slide Content:**
```
How Partitions are Distributed Across Brokers

In a multi-broker Kafka cluster:

┌─────────────────────────────────────────────────────────┐
│                    Topic: orders                        │
│                  (6 partitions)                         │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Broker 1         Broker 2         Broker 3            │
│  ┌───────┐        ┌───────┐        ┌───────┐           │
│  │ P0    │        │ P1    │        │ P2    │           │
│  │ P3    │        │ P4    │        │ P5    │           │
│  └───────┘        └───────┘        └───────┘           │
│                                                         │
└─────────────────────────────────────────────────────────┘

• Partitions are distributed across brokers for load balancing
• Each partition lives on one broker (before replication)
• This enables horizontal scaling of storage and throughput
• We'll cover replication in a later lecture
```

**Voice Script:**
> "Let's briefly touch on how partitions relate to Kafka brokers. We'll go into much more detail on this when we cover Kafka clusters, but it's good to have a basic understanding now.
>
> In a multi-broker Kafka cluster, partitions are distributed across the brokers. Looking at this diagram, we have a topic called 'orders' with 6 partitions, and we have 3 brokers in our cluster.
>
> Kafka automatically distributes these partitions across the brokers. Broker 1 gets Partitions 0 and 3, Broker 2 gets Partitions 1 and 4, and Broker 3 gets Partitions 2 and 5.
>
> This distribution serves two purposes. First, it balances the storage load across all brokers. Second, it balances the throughput since producers and consumers for different partitions will connect to different brokers.
>
> Note that for now, we're talking about a simple case where each partition exists on only one broker. In production, you'll use replication where each partition has copies on multiple brokers for fault tolerance. We'll cover that in a later lecture on replication and ISR."

---

## Slide 14: Summary - Topics and Partitions

**Slide Content:**
```
Summary: Topics and Partitions

Topics:
• Logical category/channel for messages
• Named, append-only, immutable
• Can have multiple producers and consumers

Partitions:
• Physical subdivision of a topic
• Enable parallelism and horizontal scaling
• Each partition is an ordered, immutable log
• Messages identified by offset within partition

Key Concepts:
• Ordering guaranteed ONLY within a partition
• Message keys control partition assignment
• Same key → Same partition → Ordered processing
• Number of partitions = max parallel consumers

Coming up next: Setting up Kafka locally to see this in action!
```

**Voice Script:**
> "Let's summarize what we've learned in this lecture.
>
> Topics are the logical way to organize messages in Kafka. They're named channels that are append-only and contain immutable messages. Multiple producers can write to a topic, and multiple consumers can read from it independently.
>
> Partitions are the physical subdivision of topics. They're what enable Kafka's parallelism and horizontal scaling. Each partition is its own ordered, immutable log, and messages within a partition are identified by their offset.
>
> The key concepts to remember are: First, ordering is guaranteed only within a partition, not across partitions. Second, message keys control which partition a message goes to. The same key always goes to the same partition, which ensures ordered processing for related messages. Third, the number of partitions determines the maximum number of parallel consumers you can have.
>
> In our next lecture, we'll set up a Kafka broker locally and see these concepts in action through hands-on terminal exercises. You'll create topics with multiple partitions, produce messages with and without keys, and observe how partitions work in practice."

---

## Slide 15: Quiz / Key Takeaways

**Slide Content:**
```
Quick Check: Test Your Understanding

1. What is the maximum number of consumers that can actively
   consume from a topic with 5 partitions in the same consumer group?

2. If you send messages with key="user-42" to a topic with 4 partitions,
   will these messages always go to the same partition?

3. True or False: Messages in Kafka can be modified after being written.

4. If you need to ensure all events for a specific Order ID are
   processed in order, what should you use as the message key?

Answers:
1. 5 consumers (one per partition)
2. Yes, same key → same partition (if partition count unchanged)
3. False - messages are immutable
4. The Order ID should be the message key
```

**Voice Script:**
> "Before we wrap up, let's do a quick check to make sure you've understood the key concepts.
>
> Question 1: What is the maximum number of consumers that can actively consume from a topic with 5 partitions in the same consumer group? The answer is 5. One consumer per partition is the maximum for parallel processing.
>
> Question 2: If you send messages with key equals 'user-42' to a topic with 4 partitions, will these messages always go to the same partition? Yes, they will. The same key always produces the same hash, which maps to the same partition, as long as you don't change the partition count.
>
> Question 3: True or False: Messages in Kafka can be modified after being written. This is False. Messages in Kafka are immutable. Once written, they cannot be changed.
>
> Question 4: If you need to ensure all events for a specific Order ID are processed in order, what should you use as the message key? You should use the Order ID as the message key. This ensures all events for that order go to the same partition and are processed in order.
>
> Great job! In the next lecture, we'll get hands-on and set up a Kafka broker locally."

---

## Slide 16: End Slide

**Slide Content:**
```
Thank You!

Topics and Partitions - Complete ✓

Next Lecture:
Setting Up a Kafka Broker Locally using KRaft

See you in the next lecture!
```

**Voice Script:**
> "That's it for our lecture on Kafka Topics and Partitions. We've covered the fundamental concepts that everything else in Kafka builds upon. Make sure you understand these concepts well because we'll be building on them throughout this course.
>
> In the next lecture, we'll get our hands dirty by setting up a Kafka broker locally using KRaft mode. We'll then create topics, produce and consume messages, and see everything we learned today in action.
>
> Thank you for watching, and I'll see you in the next lecture!"

---

# Additional Materials

## Diagrams to Create

1. **Topic as a stream** - Simple horizontal flow diagram
2. **Single partition bottleneck** - One queue with one consumer
3. **Multiple partitions with consumers** - Parallel processing visual
4. **Offset numbering** - Clear numbered boxes in partitions
5. **Key hashing flowchart** - Key → Hash → Modulo → Partition
6. **Order processing example** - Before/after using keys

## Suggested Slide Design Notes

- Use a dark theme (easier on eyes for coding content)
- Use monospace fonts for any technical terms
- Keep text minimal; use diagrams heavily
- Use color coding: green for correct/good, red for incorrect/bad
- Animate diagrams where possible (messages flowing into partitions)
