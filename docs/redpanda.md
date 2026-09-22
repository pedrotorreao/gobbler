# Redpanda: Concepts, Architecture & Developer Guide

This document summarizes the architectural concepts, operational mechanics, and local developer workflows for Redpanda as utilized in the Gobbler Market Surveillance platform.

---

## 1. What is Redpanda?

**Redpanda** is a modern, high-throughput distributed event streaming platform designed as a **drop-in, 100% API-compatible replacement for Apache Kafka**.

* **Kafka API Compatibility**: Any client speaking the Kafka wire protocol (such as `segmentio/kafka-go` or Python consumer libraries) connects to Redpanda without modifications.
* **Modern C++20 Runtime**: Built from scratch using the **Seastar** asynchronous framework, eliminating the Java Virtual Machine (JVM), Garbage Collection (GC) pauses, and external consensus services (like ZooKeeper).
* **Role in Gobbler**: Serves as the high-speed, shock-absorbing event broker that decouples the Go Ingestion Gateway from downstream Python ML inference and QuestDB time-series storage.

---

## 2. Core Concepts: Log vs. Queue

### The Commit Log Abstraction
Unlike relational databases (which update rows in place), Redpanda is an **append-only, immutable commit log**. When ticks arrive, they are sequentially appended to disk and given a monotonically increasing integer identifier called an **Offset** (`0, 1, 2, 3...`).

### Traditional Queue vs. Redpanda Log
| Dimension | Traditional Queue (RabbitMQ, SQS, Go channels) | Redpanda / Kafka Log |
| :--- | :--- | :--- |
| **Read Behavior** | **Destructive Read**: Consuming an item pops/deletes it. | **Non-destructive Read**: Records persist on disk after consumption. |
| **Multi-Consumer Access** | Competing consumers: only one gets a given message. | **Publish/Subscribe**: Multiple independent services read the same stream simultaneously. |
| **Replayability** | Cannot replay consumed data. | Consumers can rewind their offset pointer to replay historical data. |

In Gobbler, this non-destructive model allows both the **Python ML Anomaly Worker** (Phase 3) and the **QuestDB Persistence Service** (Phase 4) to consume `market-ticks` independently at their own processing speeds without stealing data from one another.

---

## 3. Topics, Partitions, and the Partition Key Guarantee

```
[Producer: Go Gateway]
         │ (Key: "PETR4", Value: JSON Tick)
         ▼
 ┌───────────────────────────────────────────────────────────┐
 │                   Topic: "market-ticks"                   │
 │                                                           │
 │  Partition 0:  [Offset 0] [Offset 1] [Offset 2] ... ◄─────┤ (Strict FIFO for PETR4)
 │  Partition 1:  [Offset 0] [Offset 1] ...                  │
 │  Partition 2:  [Offset 0] [Offset 1] [Offset 2] ...       │
 │  Partition 3:  [Offset 0] ...                             │
 └───────────────────────────────────────────────────────────┘
         ▲
         │ (Parallel Pull)
[Consumer Group: Python ML Workers]
```

### 1. Topics & Partitions
* A **Topic** (e.g., `market-ticks`) is a logical feed of records.
* Topics are split into **Partitions** to enable horizontal scaling and parallel I/O.

### 2. The Partition Ordering Guarantee
* Kafka/Redpanda guarantees **strict FIFO ordering within each individual partition**.
* Across different partitions, global ordering is not guaranteed.

### 3. Key-Based Routing (`hash(key) % N`)
* When publishing a tick, the producer assigns a **Key** (the ticker symbol, e.g., `"PETR4"`).
* The broker hashes the key to deterministically select the destination partition:
  $$\text{Partition} = \text{hash}(\text{ticker}) \pmod{\text{Partition Count}}$$
* **Guarantee**: Every tick for `"PETR4"` always lands on the exact same partition in the exact order it occurred.

### 4. Consumer Group Parallelism
* In a Consumer Group, **each partition is assigned to at most one consumer instance at a time**.
* With **4 partitions**, up to 4 worker processes can read concurrently without locks or thread contention.

---

## 4. Under the Hood: Performance & Hardware Affinity

Traditional Kafka can experience latency jitter (p99 spikes) due to JVM garbage collection pauses and OS kernel page cache flushing under high load. Redpanda solves this through:

1. **Thread-per-Core (Shared-Nothing Architecture)**:
   * Redpanda pins one operating system thread to each CPU core.
   * Each thread independently manages its own memory, network polling, and disk writes, avoiding cross-core mutex lock contention.
2. **Direct I/O (`O_DIRECT`)**:
   * Bypasses the Linux kernel's Page Cache to write directly to storage devices, ensuring deterministic, low-jitter write latencies.
3. **Embedded Raft**:
   * Distributed consensus is integrated into every partition, providing leader election without auxiliary coordinator processes.

---

## 5. Local Docker Setup & Ephemeral Storage Strategy

### Ephemeral Storage Rationale (Development Phase)
During active development, Redpanda runs **without persistent volumes**:
* **Zero Ghost State**: Prevents old consumer group offsets from skipping newly generated test data.
* **Rapid Configuration Changes**: Allows changing topic partition counts freely on restart without schema or log migration.
* **Disk Conservation**: Prevents high-throughput simulations (50k msgs/sec) from filling host storage with stale log segments.

### Docker Compose Service (`docker-compose.yml`)
```yaml
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:latest
    container_name: redpanda
    command:
      - redpanda
      - start
      - --mode dev-container      # Relaxes hardware locking for local dev
      - --smp 1                   # Allocates 1 CPU core
      - --memory 1G               # Memory limit
      - --kafka-addr 0.0.0.0:9092 # Listens on all container interfaces
      - --advertise-kafka-addr localhost:9092 # Advertises host access
    ports:
      - "9092:9092"               # Kafka wire protocol port
      - "9644:9644"               # Admin API port (for rpk)
```

---

## 6. CLI Cheat Sheet: `rpk` (Redpanda Keeper)

Redpanda bundles the `rpk` management tool inside the container:

* **Inspect Cluster Health**:
  ```bash
  docker exec redpanda rpk cluster info
  ```

* **Create a Topic with Partitions**:
  ```bash
  docker exec redpanda rpk topic create market-ticks --partitions 4
  ```

* **Inspect Topic Details & Partition Offsets**:
  ```bash
  docker exec redpanda rpk topic describe market-ticks
  ```

* **Live Stream Monitoring (Tail Topic in Real Time)**:
  ```bash
  docker exec redpanda rpk topic consume market-ticks
  ```

* **List All Topics**:
  ```bash
  docker exec redpanda rpk topic list
  ```

---

## 7. Next Step: Gateway Integration (`segmentio/kafka-go`)

* **Library**: `segmentio/kafka-go` (100% pure Go, zero Cgo overhead, multi-arch friendly for ARM64/Raspberry Pi).
* **Producer Structure**: A single, thread-safe `*kafka.Writer` shared across all worker goroutines in `main.go`.
* **Routing**: Configured with `Balancer: &kafka.Hash{}` to route ticks using `MarketTick.Ticker` as the message key.
