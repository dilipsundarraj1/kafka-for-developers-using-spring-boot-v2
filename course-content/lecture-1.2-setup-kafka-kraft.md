# Lecture 1.2: Setting Up a Kafka Broker Locally using KRaft

## Slide 1: Title Slide

**Slide Content:**
```
Setting Up a Kafka Broker Locally
Using KRaft Mode (No ZooKeeper!)

Module 1 - Lecture 2
Hands-On Lab
```

**Voice Script:**
> "Welcome to this hands-on lecture where we'll set up a Kafka broker locally using KRaft mode. By the end of this lecture, you'll have a fully functional Kafka broker running on your machine, and you'll understand the setup process step by step. This is an exciting lecture because we'll move from theory to practice. Let's get started!"

---

## Slide 2: Prerequisites

**Slide Content:**
```
Prerequisites

Before we begin, ensure you have:

1. Java Development Kit (JDK) 17 or higher
   $ java -version
   openjdk version "17.0.x" or higher

2. Sufficient disk space
   • At least 1 GB free for Kafka installation and logs

3. Terminal/Command Line access
   • macOS: Terminal or iTerm2
   • Windows: PowerShell or WSL (recommended)
   • Linux: Any terminal emulator

4. Download Apache Kafka
   • https://kafka.apache.org/downloads
   • Download the binary (not source)
   • Example: kafka_2.13-3.7.0.tgz
     └── 2.13 = Scala version
     └── 3.7.0 = Kafka version
```

**Voice Script:**
> "Before we begin, let's make sure you have everything you need.
>
> First, you need Java Development Kit version 17 or higher. Kafka is written in Java and Scala, so it requires a JVM to run. Open your terminal and type 'java -version' to check. You should see version 17 or higher.
>
> Second, make sure you have at least 1 gigabyte of free disk space. Kafka will need space for its installation files and for storing message logs.
>
> Third, you need terminal access. On macOS, you can use the built-in Terminal or iTerm2. On Windows, I recommend using WSL, Windows Subsystem for Linux, as it provides a Linux-like environment. On Linux, any terminal will work.
>
> Fourth, download Apache Kafka from kafka.apache.org/downloads. Make sure to download the binary distribution, not the source code. The file name will look something like kafka underscore 2.13 dash 3.7.0. The 2.13 refers to the Scala version Kafka was built with, and 3.7.0 is the Kafka version. Download the latest stable version available."

---

## Slide 3: Download and Extract Kafka

**Slide Content:**
```
Step 1: Download and Extract Kafka

# Navigate to your preferred directory
$ cd ~

# Download Kafka (or download from browser)
$ curl -O https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz

# Extract the archive
$ tar -xzf kafka_2.13-3.7.0.tgz

# Navigate into the Kafka directory
$ cd kafka_2.13-3.7.0

# View the directory structure
$ ls -la

Directory Structure:
┌─────────────────────────────────────────────────┐
│  kafka_2.13-3.7.0/                              │
│  ├── bin/           # Kafka CLI scripts         │
│  ├── config/        # Configuration files       │
│  ├── libs/          # Java JAR files            │
│  ├── licenses/      # License information       │
│  ├── site-docs/     # Documentation             │
│  └── LICENSE        # Apache License            │
└─────────────────────────────────────────────────┘
```

**Voice Script:**
> "Let's start by downloading and extracting Kafka.
>
> Open your terminal and navigate to your preferred directory. I'll use the home directory for this demo.
>
> You can download Kafka using curl or simply download it from your browser. The command shown here downloads Kafka 3.7.0. If a newer version is available, feel free to use that instead.
>
> Once downloaded, extract the archive using 'tar -xzf' followed by the filename. This will create a directory with the Kafka installation.
>
> Navigate into that directory using 'cd kafka_2.13-3.7.0'.
>
> Let's look at what's inside. The 'bin' directory contains all the command-line scripts we'll use to interact with Kafka. The 'config' directory contains configuration files for brokers, producers, consumers, and more. The 'libs' directory contains all the Java JAR files that make up Kafka. We'll spend most of our time using scripts from the 'bin' directory and occasionally modifying files in the 'config' directory."

---

## Slide 4: Understanding KRaft Configuration

**Slide Content:**
```
Step 2: Understanding the KRaft Configuration File

Location: config/kraft/server.properties

Key Configuration Properties:
┌─────────────────────────────────────────────────────────────┐
│ # Node Identity                                             │
│ node.id=1                      # Unique ID for this node    │
│ process.roles=broker,controller # This node is both        │
│                                                             │
│ # Controller Quorum                                         │
│ controller.quorum.voters=1@localhost:9093                   │
│ # Format: nodeId@host:port                                  │
│                                                             │
│ # Listeners (Network)                                       │
│ listeners=PLAINTEXT://:9092,CONTROLLER://:9093              │
│ # 9092 = client connections (producers/consumers)           │
│ # 9093 = controller communication                           │
│                                                             │
│ # Storage                                                   │
│ log.dirs=/tmp/kraft-combined-logs                           │
│ # Where Kafka stores partition data                         │
│                                                             │
│ # Defaults for new topics                                   │
│ num.partitions=1               # Default partition count    │
│ default.replication.factor=1   # Default replication        │
└─────────────────────────────────────────────────────────────┘
```

**Voice Script:**
> "Before we start Kafka, let's understand the configuration file for KRaft mode. The file is located at config/kraft/server.properties.
>
> Let me walk you through the key properties.
>
> 'node.id' is a unique identifier for this Kafka node. In a cluster, each node must have a different ID. For our single-node setup, we'll use 1.
>
> 'process.roles' defines what roles this node plays. It can be 'broker', 'controller', or both. In our setup, we're running both roles on the same node, which is perfect for development and learning. In production, you might separate these roles.
>
> 'controller.quorum.voters' lists all the nodes that participate in the controller quorum. The format is nodeId at host colon port. For a single node, it's just our node.
>
> 'listeners' defines the network endpoints. Port 9092 is for client connections, this is where producers and consumers connect. Port 9093 is for controller-to-controller communication.
>
> 'log.dirs' specifies where Kafka stores all the partition data. This is where your messages actually live on disk.
>
> Finally, 'num.partitions' and 'default.replication.factor' set the defaults for newly created topics. We'll override these when creating topics explicitly."

---

## Slide 5: Generate Cluster ID

**Slide Content:**
```
Step 3: Generate a Cluster ID

Every Kafka cluster needs a unique identifier (UUID)

# Generate a new Cluster UUID
$ ./bin/kafka-storage.sh random-uuid

Output:
MkU3OEVBNTcwNTJENDM2Qk    ← Your unique cluster ID
                            (yours will be different)

Why do we need this?
┌─────────────────────────────────────────────────────────────┐
│ • Identifies this specific Kafka cluster                    │
│ • Prevents accidentally mixing data from different clusters │
│ • Required for KRaft metadata initialization                │
│ • Generated once, used forever for this cluster             │
└─────────────────────────────────────────────────────────────┘

💡 Save this ID! You'll need it in the next step.
```

**Voice Script:**
> "Now let's generate a cluster ID. Every Kafka cluster needs a unique identifier, which is a UUID.
>
> Run the command: './bin/kafka-storage.sh random-uuid'
>
> This will output a unique identifier like the one shown here. Your ID will be different from mine, and that's expected. Each cluster should have its own unique ID.
>
> Why do we need this? The cluster ID serves several purposes. It uniquely identifies this specific Kafka cluster. It prevents accidentally mixing data from different clusters, which could cause serious data corruption. It's required for initializing the KRaft metadata. And once generated, you use the same ID for the lifetime of this cluster.
>
> Make sure to copy this ID because you'll need it in the next step. If you're following along, pause the video now and run this command to get your own cluster ID."

---

## Slide 6: Format Storage Directory

**Slide Content:**
```
Step 4: Format the Storage Directory

Initialize the storage with cluster ID and configuration

# Format the log directories
$ ./bin/kafka-storage.sh format \
    -t <YOUR-CLUSTER-UUID> \
    -c ./config/kraft/server.properties

Example:
$ ./bin/kafka-storage.sh format \
    -t MkU3OEVBNTcwNTJENDM2Qk \
    -c ./config/kraft/server.properties

Expected Output:
┌─────────────────────────────────────────────────────────────┐
│ Formatting /tmp/kraft-combined-logs with metadata.version   │
│ 3.7-IV4                                                     │
└─────────────────────────────────────────────────────────────┘

What this does:
• Creates the log directory structure
• Initializes metadata with cluster ID
• Sets up the internal __cluster_metadata topic
• Prepares storage for the Raft consensus log

⚠️  This only needs to be done ONCE per cluster setup
```

**Voice Script:**
> "Now we need to format the storage directory. This initializes Kafka's storage with our cluster ID and configuration.
>
> Run the kafka-storage.sh format command with two arguments: '-t' followed by your cluster UUID from the previous step, and '-c' followed by the path to the server.properties file.
>
> When you run this, you'll see output indicating that Kafka is formatting the log directory. The metadata version shown corresponds to your Kafka version.
>
> What does this command actually do? It creates the directory structure for storing data. It initializes the metadata with your cluster ID. It sets up an internal topic called '__cluster_metadata' which stores all the cluster state that ZooKeeper used to store. And it prepares the storage for the Raft consensus log.
>
> Important: This format command only needs to be run once when you first set up the cluster. If you run it again, it will fail because the directory is already formatted. If you want to start fresh, you'd need to delete the log directory first."

---

## Slide 7: Start the Kafka Broker

**Slide Content:**
```
Step 5: Start the Kafka Broker

# Start Kafka in the foreground
$ ./bin/kafka-server-start.sh ./config/kraft/server.properties

Startup Logs to Look For:
┌─────────────────────────────────────────────────────────────┐
│ [INFO] Registered broker 1 at path /brokers/ids/1          │
│ [INFO] Kafka Server started (kafka.server.KafkaServer)     │
│ [INFO] [Controller 1] The active controller is now 1       │
└─────────────────────────────────────────────────────────────┘

✅ Success indicators:
• "Kafka Server started"
• "The active controller is now X"
• No ERROR messages
• Process keeps running (doesn't exit)

# To run in background (optional):
$ ./bin/kafka-server-start.sh -daemon ./config/kraft/server.properties

# To stop Kafka:
$ ./bin/kafka-server-stop.sh
```

**Voice Script:**
> "Now for the exciting part, let's start the Kafka broker!
>
> Run: './bin/kafka-server-start.sh' followed by the path to the server.properties file.
>
> Kafka will start and you'll see a lot of log output. Don't be overwhelmed by it. Look for these key messages: 'Kafka Server started' indicates the broker is up and running. 'The active controller is now 1' confirms that the controller has been elected and is active.
>
> If you see these messages and the process keeps running without exiting, congratulations! Your Kafka broker is now running!
>
> A few notes: Running Kafka in the foreground like this is great for learning because you can see all the logs in real time. If you want to run Kafka in the background, add the '-daemon' flag. To stop Kafka, you can either press Control-C if it's running in the foreground, or run the 'kafka-server-stop.sh' script.
>
> For this course, I recommend running Kafka in the foreground so you can observe what's happening."

---

## Slide 8: Verify Kafka is Running

**Slide Content:**
```
Step 6: Verify Kafka is Running

Open a NEW terminal window and run:

# Check if Kafka is listening on port 9092
$ lsof -i :9092
# or
$ netstat -an | grep 9092

Expected output:
COMMAND   PID  USER   FD   TYPE  DEVICE  NODE NAME
java      1234 user   123u IPv6  0x...   TCP *:9092 (LISTEN)

# Try to list topics (should return empty or internal topics)
$ ./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Check broker metadata
$ ./bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

Quick Health Check Summary:
┌─────────────────────────────────────────────────────────────┐
│ ✅ Port 9092 is listening                                   │
│ ✅ kafka-topics.sh connects successfully                    │
│ ✅ No connection errors                                     │
│ ✅ Broker responds to API version request                   │
└─────────────────────────────────────────────────────────────┘
```

**Voice Script:**
> "With Kafka running, let's verify everything is working correctly. Open a new terminal window while keeping Kafka running in the first one.
>
> First, let's check if Kafka is listening on port 9092. You can use 'lsof -i :9092' on macOS or Linux. You should see a Java process listening on that port.
>
> Next, let's try to connect to Kafka using the kafka-topics script. Run: './bin/kafka-topics.sh --list --bootstrap-server localhost:9092'. If Kafka is running correctly, this command will connect and return either an empty list or show some internal topics. The important thing is that it doesn't show a connection error.
>
> You can also run 'kafka-broker-api-versions.sh' to see detailed information about the broker, including all the API versions it supports. This is a good way to confirm the broker is healthy and responding.
>
> If all these checks pass, your Kafka broker is up and running successfully!"

---

## Slide 9: Terminal Demo - Complete Setup Flow

**Slide Content:**
```
LIVE DEMO: Complete Setup Flow

Terminal Commands in Sequence:
┌─────────────────────────────────────────────────────────────┐
│ # 1. Navigate to Kafka directory                            │
│ $ cd ~/kafka_2.13-3.7.0                                     │
│                                                             │
│ # 2. Generate cluster ID                                    │
│ $ KAFKA_CLUSTER_ID=$(./bin/kafka-storage.sh random-uuid)    │
│ $ echo $KAFKA_CLUSTER_ID                                    │
│                                                             │
│ # 3. Format storage                                         │
│ $ ./bin/kafka-storage.sh format \                           │
│     -t $KAFKA_CLUSTER_ID \                                  │
│     -c ./config/kraft/server.properties                     │
│                                                             │
│ # 4. Start Kafka                                            │
│ $ ./bin/kafka-server-start.sh ./config/kraft/server.properties │
│                                                             │
│ # (In new terminal)                                         │
│ # 5. Verify                                                 │
│ $ ./bin/kafka-topics.sh --list --bootstrap-server localhost:9092 │
└─────────────────────────────────────────────────────────────┘

💡 Pro tip: Store KAFKA_CLUSTER_ID in a shell variable
   to use it easily in the format command
```

**Voice Script:**
> "Let me show you the complete setup flow in one terminal session.
>
> First, navigate to your Kafka directory.
>
> Here's a pro tip: instead of copying the cluster ID manually, you can store it in a shell variable. Run 'KAFKA_CLUSTER_ID=$(./bin/kafka-storage.sh random-uuid)'. This captures the output in a variable. You can verify it by running 'echo $KAFKA_CLUSTER_ID'.
>
> Now use that variable in the format command: './bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c ./config/kraft/server.properties'. This formats the storage directory.
>
> Finally, start Kafka with: './bin/kafka-server-start.sh ./config/kraft/server.properties'.
>
> Open a new terminal and verify with: './bin/kafka-topics.sh --list --bootstrap-server localhost:9092'.
>
> And that's it! You now have a running Kafka broker. In the next lecture, we'll create topics and start producing and consuming messages."

---

## Slide 10: Understanding the Log Directory

**Slide Content:**
```
Exploring the Log Directory

$ ls -la /tmp/kraft-combined-logs/

Directory Contents:
┌─────────────────────────────────────────────────────────────┐
│ /tmp/kraft-combined-logs/                                   │
│ ├── __cluster_metadata-0/    # KRaft metadata partition     │
│ │   ├── 00000000000000000000.log      # Segment file       │
│ │   ├── 00000000000000000000.index    # Offset index       │
│ │   └── 00000000000000000000.timeindex # Time index        │
│ ├── meta.properties          # Broker metadata              │
│ ├── bootstrap.checkpoint     # Bootstrap state              │
│ └── .lock                    # Process lock file            │
└─────────────────────────────────────────────────────────────┘

__cluster_metadata topic:
• Internal topic managed by KRaft
• Stores all cluster metadata (topics, partitions, configs)
• Replicated across all controllers
• This is what replaced ZooKeeper's data storage!

⚠️  Never manually modify files in this directory!
```

**Voice Script:**
> "Let's take a peek at what Kafka created in the log directory. Navigate to '/tmp/kraft-combined-logs' and list the contents.
>
> You'll see several files and directories. The most interesting one is '__cluster_metadata-0'. This is a special internal topic that KRaft uses to store all cluster metadata. Remember how we said KRaft replaces ZooKeeper? This is where that metadata now lives!
>
> Inside the metadata directory, you'll see segment files with '.log' extension, these contain the actual data. The '.index' and '.timeindex' files are indexes that help Kafka quickly locate messages.
>
> The 'meta.properties' file contains information about this broker, including the cluster ID and node ID. The '.lock' file prevents multiple Kafka processes from using the same log directory.
>
> As we create topics and produce messages, you'll see new directories appear here for each topic partition.
>
> One important warning: never manually edit or delete files in this directory while Kafka is running. It can corrupt your data. If you need to reset, stop Kafka first, then delete the directory, then re-format."

---

## Slide 11: Common Setup Issues and Solutions

**Slide Content:**
```
Troubleshooting Common Issues

┌─────────────────────────────────────────────────────────────┐
│ Issue: "Address already in use" on port 9092                │
│ Solution:                                                   │
│   $ lsof -i :9092  # Find what's using the port            │
│   $ kill <PID>     # Stop that process                      │
│   # Or change port in server.properties                     │
├─────────────────────────────────────────────────────────────┤
│ Issue: "Log directory already formatted"                    │
│ Solution:                                                   │
│   $ rm -rf /tmp/kraft-combined-logs                         │
│   # Then re-run format command                              │
├─────────────────────────────────────────────────────────────┤
│ Issue: "JAVA_HOME is not set"                               │
│ Solution:                                                   │
│   $ export JAVA_HOME=$(/usr/libexec/java_home)  # macOS    │
│   $ export JAVA_HOME=/usr/lib/jvm/java-17       # Linux    │
├─────────────────────────────────────────────────────────────┤
│ Issue: Kafka crashes with OutOfMemoryError                  │
│ Solution:                                                   │
│   # Edit bin/kafka-server-start.sh                          │
│   # Reduce KAFKA_HEAP_OPTS to "-Xmx512M -Xms512M"          │
├─────────────────────────────────────────────────────────────┤
│ Issue: "Connection refused" when running kafka-topics.sh    │
│ Solution:                                                   │
│   # Make sure Kafka fully started                           │
│   # Wait for "Kafka Server started" in logs                 │
└─────────────────────────────────────────────────────────────┘
```

**Voice Script:**
> "Before we wrap up, let me share some common issues you might encounter and how to solve them.
>
> If you see 'Address already in use' on port 9092, it means something else is using that port. Use 'lsof -i :9092' to find the process ID, then kill it, or change the port in server.properties.
>
> If you get 'Log directory already formatted', it means you've already initialized the storage. Either delete the log directory and re-format, or simply start Kafka if it was already set up.
>
> If you see 'JAVA_HOME is not set', you need to configure your Java environment. On macOS, you can use the command shown here. On Linux, point it to your Java installation directory.
>
> If Kafka crashes with OutOfMemoryError, your machine might not have enough RAM. Edit the kafka-server-start script and reduce the heap size. 512 megabytes is usually enough for development.
>
> If you get 'Connection refused' when running kafka-topics.sh, Kafka might not be fully started yet. Go back to the Kafka terminal and make sure you see 'Kafka Server started' in the logs."

---

## Slide 12: Setting Up PATH (Optional but Recommended)

**Slide Content:**
```
Making Kafka Commands Available Globally (Optional)

Instead of typing full paths every time...
$ ./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

Add Kafka to your PATH:

# Add to ~/.bashrc or ~/.zshrc
export KAFKA_HOME=~/kafka_2.13-3.7.0
export PATH=$PATH:$KAFKA_HOME/bin

# Reload your shell
$ source ~/.bashrc   # or source ~/.zshrc

Now you can run from anywhere:
$ kafka-topics.sh --list --bootstrap-server localhost:9092
$ kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
$ kafka-console-consumer.sh --topic test --bootstrap-server localhost:9092

Much cleaner! ✨
```

**Voice Script:**
> "Here's an optional but highly recommended step: add Kafka to your system PATH. This way, you can run Kafka commands from any directory without typing the full path.
>
> Open your shell configuration file. That's '.bashrc' if you use bash, or '.zshrc' if you use zsh.
>
> Add these two lines: First, set KAFKA_HOME to your Kafka installation directory. Second, add the bin directory to your PATH.
>
> Save the file and reload your shell with 'source ~/.bashrc' or 'source ~/.zshrc'.
>
> Now you can run Kafka commands from anywhere. Instead of typing './bin/kafka-topics.sh', you can just type 'kafka-topics.sh'. This makes working with Kafka much more convenient, especially as we start using more commands in upcoming lectures."

---

## Slide 13: Summary

**Slide Content:**
```
Summary: Setting Up Kafka with KRaft

What We Did:
┌─────────────────────────────────────────────────────────────┐
│ 1. Downloaded and extracted Apache Kafka                    │
│                                                             │
│ 2. Understood the KRaft configuration                       │
│    • node.id, process.roles, listeners, log.dirs           │
│                                                             │
│ 3. Generated a unique Cluster ID                            │
│    $ kafka-storage.sh random-uuid                           │
│                                                             │
│ 4. Formatted the storage directory                          │
│    $ kafka-storage.sh format -t <id> -c server.properties   │
│                                                             │
│ 5. Started the Kafka broker                                 │
│    $ kafka-server-start.sh server.properties                │
│                                                             │
│ 6. Verified it's running                                    │
│    $ kafka-topics.sh --list --bootstrap-server localhost:9092│
└─────────────────────────────────────────────────────────────┘

Key Takeaway:
KRaft mode = Simple, self-contained Kafka (no ZooKeeper!)

Next: Creating Topics and Producing/Consuming Messages
```

**Voice Script:**
> "Let's summarize what we accomplished in this lecture.
>
> We downloaded and extracted Apache Kafka from the official website.
>
> We explored the KRaft configuration file and understood the key properties: node.id for identifying our broker, process.roles for specifying broker and controller roles, listeners for network configuration, and log.dirs for data storage.
>
> We generated a unique cluster ID using kafka-storage.sh random-uuid. This ID uniquely identifies our Kafka cluster.
>
> We formatted the storage directory, which initializes the metadata and prepares Kafka for storing data.
>
> We started the Kafka broker and saw it come online with the controller becoming active.
>
> And finally, we verified everything is working by connecting with kafka-topics.sh.
>
> The key takeaway is that KRaft mode makes Kafka setup simple and self-contained. No ZooKeeper means one less system to manage.
>
> In the next lecture, we'll create our first topic and start producing and consuming messages. That's where things get really interesting!"

---

## Slide 14: Quick Reference Card

**Slide Content:**
```
Quick Reference: Kafka KRaft Setup Commands

┌─────────────────────────────────────────────────────────────┐
│ SETUP (One-time)                                            │
│ ──────────────────────────────────────────────────────────  │
│ Generate ID:   kafka-storage.sh random-uuid                 │
│ Format:        kafka-storage.sh format -t <ID> -c <config>  │
│                                                             │
│ START/STOP                                                  │
│ ──────────────────────────────────────────────────────────  │
│ Start:         kafka-server-start.sh <config>               │
│ Start daemon:  kafka-server-start.sh -daemon <config>       │
│ Stop:          kafka-server-stop.sh                         │
│                                                             │
│ VERIFY                                                      │
│ ──────────────────────────────────────────────────────────  │
│ List topics:   kafka-topics.sh --list \                     │
│                  --bootstrap-server localhost:9092          │
│ Check port:    lsof -i :9092                                │
│                                                             │
│ CONFIG FILE                                                 │
│ ──────────────────────────────────────────────────────────  │
│ Location:      config/kraft/server.properties               │
│ Log dir:       /tmp/kraft-combined-logs (default)           │
└─────────────────────────────────────────────────────────────┘
```

**Voice Script:**
> "Here's a quick reference card with all the commands we used today. I recommend bookmarking this slide or taking a screenshot. These are the commands you'll use every time you set up or restart Kafka.
>
> For initial setup, you need kafka-storage.sh to generate the cluster ID and format the storage.
>
> For starting and stopping, use kafka-server-start.sh and kafka-server-stop.sh. Add the -daemon flag to run Kafka in the background.
>
> To verify Kafka is running, use kafka-topics.sh to try connecting, or check if port 9092 is listening.
>
> And remember, the KRaft configuration file is at config/kraft/server.properties.
>
> That's it for this lecture! Your homework is to set up Kafka on your own machine following these steps. In the next lecture, we'll start using Kafka by creating topics and sending messages. See you there!"

---

## Slide 15: End Slide

**Slide Content:**
```
Great Job!

Kafka Broker Setup - Complete ✓

Your Kafka broker is now running in KRaft mode!

Next Lecture:
Create Topic, Produce and Consume Messages using the CLI

Homework:
• Set up Kafka on your local machine
• Verify it's running with kafka-topics.sh --list
• Explore the log directory structure

See you in the next lecture!
```

**Voice Script:**
> "Congratulations! You've successfully set up a Kafka broker using KRaft mode. This is a significant milestone. You now have a real Kafka environment to experiment with.
>
> For homework, if you haven't already, set up Kafka on your own machine following the steps we covered. Verify it's running by listing topics. And take some time to explore the log directory structure to understand how Kafka organizes its data.
>
> In the next lecture, we'll create our first topic and start producing and consuming messages. This is where you'll really start to see Kafka in action and understand how the concepts from our first lecture apply in practice.
>
> Thank you for watching, and I'll see you in the next lecture!"

---

# Instructor Notes

## Demo Checklist
- [ ] Have Kafka downloaded and ready
- [ ] Clear any previous log directories before demo
- [ ] Test all commands before recording
- [ ] Have two terminal windows ready
- [ ] Consider using larger font size for terminal

## Timing Suggestions
- Prerequisites: 2 minutes
- Download/Extract: 3 minutes
- Configuration explanation: 5 minutes
- Generate ID + Format: 3 minutes
- Start Kafka + Verify: 5 minutes
- Troubleshooting: 3 minutes
- Summary: 2 minutes

## Common Student Questions
1. "Can I use Docker instead?" - Yes, but manual setup teaches you more
2. "Why KRaft not ZooKeeper?" - Explain KRaft benefits slide
3. "What if I have an older Kafka?" - KRaft requires Kafka 3.3+
4. "Can I change the log directory?" - Yes, modify log.dirs in config
