# Gobbler: Real-Time Financial Market Surveillance & Anomaly Detection Platform

Gobbler is a high-throughput, distributed market surveillance and streaming analytics platform. It ingests simulated high-frequency financial exchange order flow via a low-latency network gateway, decouples streams using an event broker, processes trade data for statistical price anomalies, and stores time-series data for forensic analysis.

```
[Go Mock Exchange]
       │ (TCP Stream: ~50k msgs/sec)
       ▼
[Go Ingestion Gateway] ── (Zero-Allocation Buffers / sync.Pool / Bounded Worker Pool)
       │
       ▼
[Redpanda (Kafka API)] ── (4 Partitions, Keyed by Asset Ticker)
       │
       ├──► [Python ML Engine] ──► [Valkey Distributed Cache] (Idempotent Inference)
       │           │ (Z-Scores & Isolation Forest Anomaly Alerts)
       │           ▼
       └─────► [QuestDB] ◄── (High-Speed Time-Series Persistence)
                   ▲
[Kind (K8s) / OpenTofu / Prometheus Observability Layer]
```

---

## Current Architecture & Status

### Phase 1: Ingestion Gateway & Go Concurrency (✅ Completed)
* **High-Throughput TCP Server**: Non-blocking connection accept loop listening on port `8008`.
* **Bounded Concurrency**: Dedicated bounded queue channel (`50,000` capacity) feeding a CPU-scaled worker pool (`runtime.NumCPU()` workers).
* **Buffer Safety**: Solves `bufio.Scanner` buffer recycling gotchas via clean byte copying.
* **Production Lifecycle**: Intercepts OS signals (`SIGINT`, `SIGTERM`), tears down active sockets, and drains in-flight items without data loss.
* **Observability**: Embedded real-time throughput reporter and live `pprof` diagnostic server on port `6060`.

### Phase 2: Event Streaming & Decoupled Architecture (✅ Completed)
* **Broker Foundation**: Single-node Redpanda cluster running in Docker (`docker-compose.yml`) using ephemeral development storage.
* **Pure Go Producer**: Integrated `segmentio/kafka-go` (100% pure Go, zero Cgo overhead, multi-arch friendly for ARM64/Raspberry Pi).
* **Keyed Partitioning**: Explicit hashing balancer (`&kafka.Hash{}`) using 10 asset tickers as partition keys, guaranteeing strict per-asset FIFO ordering across the 4 topic partitions.
* **Zero-Allocation Forwarding**: Direct reuse of validated raw byte payloads, bypassing re-marshaling and eliminating garbage collector pressure.
* **Failure Resilience & Backpressure**: Self-healing exponential backoff (`dispatchMsg`) that blocks worker routines on broker outages, safely propagating backpressure upstream to the TCP socket without dropping ticks or exceeding memory bounds.
* **Chaos Engineering Verified**: 192,754 / 192,754 ticks delivered during live mid-stream broker freeze tests with 100.000% data integrity.

### Phase 3: Stream Inference, Anomaly Detection & Distributed Caching (✅ Completed)
* **Distributed Idempotency**: Valkey (Redis-compatible) cache on port `6379`. Each tick is deduplicated via an atomic `SET tick:<ticker>:<timestamp_ns> seen EX 60 NX` before reaching the inference layer, providing effectively-once processing across horizontally scaled consumer pods.
* **Consumer Group Parallelism**: Python worker (`consumer/consumer.py`) subscribes via `confluent-kafka` with `enable.auto.commit=False`, committing offsets explicitly only after deduplication and inference. Graceful `LeaveGroup` on `SIGINT`/`SIGTERM` triggers immediate partition rebalancing.
* **Two-Tier Anomaly Detector** (`consumer/detector.py`):
  * **Tier 1 — Statistical (sub-0.1ms)**: Per-ticker `deque`-backed rolling window of `(price, volume)` tuples. Z-score Dual-Condition Guard: `|Z| > 3.0` AND `pct_change >= 2%`. Catches sharp univariate price shocks (flash crashes, pumps).
  * **Tier 2 — ML (sub-2ms)**: `IsolationForest(n_estimators=50)` fitted on normalized `[z_price, z_volume]` features from the rolling window, re-fitted every 100 ticks. Uses `score_samples` threshold (`< -0.65`) to avoid the Contamination Trap. Catches multivariate co-movement anomalies (e.g., volume-only spikes with no price shock).
  * **Detection Logic**: `is_anomaly = tier1_anomaly or (ml_score < -0.65)` — independent OR, either tier can fire without the other.

### Phase 4: Time-Series Persistence & Infrastructure-as-Code (🚀 In Progress)
* **High-Throughput Time-Series Database**: QuestDB integrated via Docker Compose (`docker-compose.yml`) exposing ILP (`:9000` HTTP, `:9009` TCP) and PGWire (`:8812`) with daily partitioned columnar storage for `market_ticks` and `market_anomalies`.
* **Decoupled Go Persister Service**: Streaming persistence worker (`persister/persister.go`) consuming ticks and anomaly alerts with dual-trigger batching (5,000 rows or 500ms) and post-flush consumer offset commits.
* **Production Containerization**: Multi-stage Dockerfiles for all microservices (`Dockerfile.gateway`, `Dockerfile.generator`, `Dockerfile.persister`, `Dockerfile.consumer`). Pure Go services compile to 100% static binaries on `scratch` (~2.5MB–8MB) and Python ML worker on `python:3.12-slim` (~413MB), all running as unprivileged user `appuser` (UID 10001).

---

## Repository Structure

```text
├── AGENTS.md                    # Agent persona rules (Socratic coding tutor)
├── .dockerignore                # Build context filtering (excludes venv, pycache, git, logs)
├── Dockerfile.consumer          # Python ML worker: multi-stage python:3.12-slim (/opt/venv -> non-root)
├── Dockerfile.gateway           # TCP Gateway: multi-stage golang:1.25-alpine -> scratch (8.09MB)
├── Dockerfile.generator         # Mock Generator: multi-stage golang:1.25-alpine -> scratch (2.57MB)
├── Dockerfile.persister         # ILP Persister: multi-stage golang:1.25-alpine -> scratch (7.73MB)
├── consumer/
│   ├── .venv/                   # Isolated Python virtual environment (uv-managed, git-ignored)
│   ├── consumer.py              # Kafka consumer worker: dedup, inference loop, manual commits
│   ├── detector.py              # Two-tier anomaly detector: Z-Score (Tier 1) + IsolationForest (Tier 2)
│   └── requirements.txt         # Python dependencies (confluent-kafka, redis, numpy, scikit-learn)
├── docker-compose.yml           # Redpanda, Valkey & QuestDB container service definitions
├── docs/
│   ├── context.md               # Continuous architecture state for pair-programming agents
│   ├── mlinferencing.md         # Study guide: rolling windows, Z-score, IsolationForest math
│   ├── plan.md                  # Interactive project roadmap, milestones & definitions of done
│   ├── questdb.md               # Deep-dive architecture reference, ILP guide & SQL reference for QuestDB
│   ├── redpanda.md              # Deep-dive architecture reference & CLI guide for Redpanda
│   └── valkey.md                # Valkey concepts, Idempotent Consumer pattern & CLI guide
├── generator/
│   └── generator.go             # Standalone synthetic market load generator (10 B3 assets)
├── go.mod                       # Go module definitions
├── go.sum                       # Checksums for external dependencies
├── main.go                      # Core Ingestion Gateway, TCP server & Kafka producer
├── persister/
│   └── persister.go             # High-throughput ILP persistence worker (Kafka -> QuestDB)
└── README.md                    # Project documentation
```

---

## Quickstart Guide

### 1. Prerequisites
* [Go 1.22+](https://golang.org/)
* [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
* [Python 3.12+](https://www.python.org/) and [uv](https://github.com/astral-sh/uv) (or `pip`)

### 2. Start Redpanda & Valkey
```bash
docker compose up -d
```
Verify cluster health:
```bash
docker exec redpanda rpk cluster info
```

### 3. Create Partitioned Topics
Create the `market-ticks` ingest topic and the `market-anomalies` alert topic, both with 4 partitions.
Matching partition counts lets a downstream consumer group mirror the upstream worker topology:
```bash
docker exec redpanda rpk topic create market-ticks --partitions 4
docker exec redpanda rpk topic create market-anomalies --partitions 4
```
Verify both topics were created:
```bash
docker exec redpanda rpk topic list
```

### 4. Run the Ingestion Gateway
In a dedicated terminal:
```bash
go run main.go
```
* The gateway starts listening for TCP connections on `127.0.0.1:8008`.
* Live pprof diagnostics are available at `http://127.0.0.1:6060/debug/pprof/`.

### 5. Launch the Synthetic Exchange Load Generator
In a second terminal:
```bash
go run ./generator
```
The generator connects and blasts multi-ticker random-walk order flows with injected anomaly spikes.

### 6. Inspect Real-Time Streams in Redpanda
In a third terminal:
```bash
docker exec -it redpanda rpk topic consume market-ticks
```
Inspect topic watermarks:
```bash
docker exec redpanda rpk topic describe -p market-ticks
```

### 7. Set Up the Python Consumer Environment
In a fourth terminal:
```bash
cd consumer
uv venv .venv
uv pip install -r requirements.txt
```

### 8. Run the Streaming Anomaly Detection Worker
```bash
PYTHONUNBUFFERED=1 .venv/bin/python consumer.py
```
The worker subscribes to `market-ticks`, deduplicates via Valkey, and emits anomaly events:
```text
[STARTUP] Consumer subscribed to 'market-ticks' topic.
Received: Ticker=PETR4, Price=38.45, Timestamp=...
[ANOMALY ALERT] PETR4 @ $32.68 (Vol: 7400, Z-Score: -3.82, ML-Score: -0.891)
```

### 9. Monitor the Anomaly Alert Stream
In a fifth terminal, consume the `market-anomalies` topic to see confirmed anomaly events in real time:
```bash
docker exec -it redpanda rpk topic consume market-anomalies
```
Each event is a JSON payload keyed by asset ticker (ensuring per-ticker FIFO ordering):
```json
{
  "ticker": "PETR4",
  "price": 32.68,
  "volume": 7400,
  "timestamp_ns": 1726987234512345678,
  "z_score": -3.82,
  "ml_score": -0.891,
  "anomaly_tier": "BOTH"
}
```
Inspect partition-level watermarks to verify events are keyed and routed correctly:
```bash
docker exec redpanda rpk topic describe -p market-anomalies
```

### 10. Run the Time-Series Persistence Worker
In a sixth terminal, start the Go persistence worker to batch streaming ticks and anomalies into QuestDB via InfluxDB Line Protocol:
```bash
go run ./persister/persister.go
```
Verify ingested rows and execute analytics in QuestDB Web Console (`http://localhost:9000`) or via REST:
```bash
# Query total persisted ticks
curl -G "http://localhost:9000/exec" --data-urlencode "query=SELECT count(*) FROM market_ticks;"

# Query anomaly alerts joined with prevailing market prices
curl -G "http://localhost:9000/exec" --data-urlencode "query=SELECT a.timestamp, a.ticker, a.anomaly_tier, a.z_score, t.price FROM market_anomalies a ASOF JOIN market_ticks t ON (ticker);"
```

---

## Failure & Chaos Testing

To test system resilience against broker failure:

1. While the generator and gateway are actively streaming, pause the broker:
   ```bash
   docker pause redpanda
   ```
2. **Observe**: The gateway logs warning retries (`context deadline exceeded. Retrying in 50ms...`), buffers up to 50,000 items in `jobQueue`, and backpressures the generator socket. Memory usage remains constant.
3. Unpause the broker:
   ```bash
   docker unpause redpanda
   ```
4. **Observe**: The gateway immediately reconnects, flushes all pending batches to Redpanda, and resumes streaming without data loss.

---

## Performance Profiling with `pprof`

The Ingestion Gateway exposes a live diagnostic endpoint on `http://127.0.0.1:6060/debug/pprof/` powered by the standard library's `net/http/pprof`. This enables real-time CPU, memory, and goroutine profiling while streaming high-throughput tick data.

### 1. Web-Based Visual Profiler (Flame Graphs & Call Graphs)
Launch an interactive web dashboard (requires [Graphviz](https://graphviz.org/) for graph rendering):

* **CPU Profiling (30-second capture with Flame Graph)**:
  ```bash
  go tool pprof -http=:8081 http://127.0.0.1:6060/debug/pprof/profile?seconds=30
  ```
* **Heap Memory Profiling (Live Allocations)**:
  ```bash
  go tool pprof -http=:8081 http://127.0.0.1:6060/debug/pprof/heap
  ```
Navigate to `http://localhost:8081` in your browser to view Top lists, Call Graphs, and Flame Graphs.

### 2. Interactive Terminal CLI Profiling
Inspect hotspots directly in your terminal:

* **Analyze CPU Hotspots**:
  ```bash
  go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=15
  ```
  Useful interactive commands inside the pprof shell:
  * `top 10`: Display the top 10 CPU-consuming functions.
  * `list worker`: Disassemble and inspect per-line CPU time inside the `worker` function.
  * `web`: Export and open a SVG call graph in your default browser.

* **Analyze Heap Allocations (Preparing for `sync.Pool` Optimization)**:
  ```bash
  # Inspect total allocations over time (cumulative heap stress):
  go tool pprof -alloc_space http://127.0.0.1:6060/debug/pprof/heap

  # Inspect current live in-memory objects:
  go tool pprof -inuse_space http://127.0.0.1:6060/debug/pprof/heap
  ```

* **Goroutine Leak Detection**:
  ```bash
  go tool pprof http://127.0.0.1:6060/debug/pprof/goroutine
  ```
  Verify that the worker pool and connection handlers terminate cleanly and do not leak routines over long simulation runs.

### 3. Quick Browser Inspection
Visit `http://127.0.0.1:6060/debug/pprof/` in any browser while `main.go` is running to view raw counts of active goroutines, heap allocations, thread creation events, and mutex contention blocks.

