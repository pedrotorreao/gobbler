# Valkey: Distributed In-Memory Cache & Idempotency Guide

This document summarizes the role, mechanics, and operational commands for **Valkey** as utilized in the Gobbler platform.

---

## 1. What is Valkey?

**Valkey** is an open-source (BSD-licensed), high-performance in-memory key-value data store developed under the **Linux Foundation** as the community-driven, drop-in replacement for Redis.

* **100% RESP Protocol Compatible**: Speaks the standard Redis wire protocol on port `6379`. Any standard Redis client (such as Python `redis-py` or Go clients) connects seamlessly without code changes.
* **Pure In-Memory Performance**: Microsecond read/write operations critical for low-latency streaming financial pipelines.
* **Role in Gobbler**: Serves as the distributed cache layer enabling the **Idempotent Consumer Pattern** for the Python ML inference tier in Phase 3.

---

## 2. The Idempotent Consumer Pattern

### The Problem: At-Least-Once Delivery & Statistical Skew
* Streaming event brokers (Kafka/Redpanda) provide **at-least-once delivery** semantics. Under network retries, broker rebalances, or producer backpressure pauses, duplicate messages can be emitted.
* If our Python ML worker computes sliding-window statistics (rolling Z-scores over price drifts) on duplicate ticks, the mathematical variance is skewed, potentially triggering false-positive market anomaly alerts.

### The Solution: Atomic `SETNX` with Expiration (TTL)
Before computing feature engineering or anomaly inference on any tick, the Python worker executes an atomic **`SETNX`** (SET if Not eXists) command in Valkey:

```text
[Redpanda Topic: market-ticks]
             │
             ▼
    [Python ML Worker]
             │
             ├──► Key: "tick:<message_uuid>"
             ▼
     [Valkey Cache]
      ├── If Key Absent  ──► Returns 1 (NEW) ──► Compute Z-Score & ML Inference
      └── If Key Present ──► Returns 0 (DUPLICATE) ──► Drop tick immediately
```

* **Atomic Check-and-Set**: `SETNX` guarantees that concurrent worker routines cannot both process the same message ID.
* **Automatic Expiry (TTL)**: Keys expire automatically (e.g., `EX 60` seconds), bounding Valkey's RAM footprint so memory does not grow infinitely.

---

## 3. Docker Compose Service Definition

In [`docker-compose.yml`](../docker-compose.yml):

```yaml
  valkey:
    image: valkey/valkey:latest
    container_name: valkey
    ports:
      - "6379:6379" # Default Valkey/Redis port
    command: [ "valkey-server", "--save", "" ] # Disables RDB disk snapshots for pure in-memory ephemeral speed
```

---

## 4. CLI Cheat Sheet: `valkey-cli`

Execute commands directly inside the container:

* **Ping / Health Check**:
  ```bash
  docker exec -it valkey valkey-cli ping
  # Response: PONG
  ```

* **Test Atomic `SETNX` with TTL (60 seconds)**:
  ```bash
  docker exec -it valkey valkey-cli set tick:test-uuid "seen" ex 60 nx
  # Response: OK (First attempt succeeds)

  docker exec -it valkey valkey-cli set tick:test-uuid "seen" ex 60 nx
  # Response: (nil) (Duplicate attempt is rejected)
  ```

* **Inspect Key Value & Remaining TTL**:
  ```bash
  docker exec -it valkey valkey-cli get tick:test-uuid
  docker exec -it valkey valkey-cli ttl tick:test-uuid
  ```

* **Monitor Real-Time Operations**:
  ```bash
  docker exec -it valkey valkey-cli monitor
  ```

* **Inspect Memory & Performance Stats**:
  ```bash
  docker exec -it valkey valkey-cli info memory
  ```
