# Gobbler: System Context & Session State

> **Notice for New Agent Sessions**: This document is the single source of operational truth for Gobbler. It tracks active architecture decisions, solved engineering trade-offs, configuration invariants, and current roadmap state. Update this file whenever completing a phase or making architectural adjustments.

---

## 1. Project Mission & High-Level Architecture

Gobbler is a distributed real-time financial market surveillance and anomaly detection platform simulating high-throughput exchange data (~50,000 msgs/sec), ingesting it through a low-latency network gateway, streaming it via an event broker, detecting price anomalies with Python ML workers, and persisting records in a time-series database.

* **Tech Stack**: Go 1.22+, Docker & Docker Compose, Redpanda (Kafka API), Python (NumPy/Scikit-learn - Phase 3), Valkey (Phase 3), QuestDB (Phase 4), Kind/OpenTofu (Phase 4).
* **Roadmap Reference**: [`plan.md`](plan.md)
* **Redpanda Deep Dive**: [`redpanda.md`](redpanda.md)
* **Valkey Deep Dive**: [`valkey.md`](valkey.md)
* **QuestDB Deep Dive**: [`questdb.md`](questdb.md)
* **Terraform / IaC Deep Dive**: [`terraform.md`](terraform.md)

---

## 2. Completed Phases & Implemented Milestones

### Phase 1: Ingestion Gateway & Go Concurrency (✅ COMPLETED)
1. **TCP Listener**: Listens on `127.0.0.1:8008` in a non-blocking `Accept()` loop.
2. **Worker Pool**: `runtime.NumCPU()` worker goroutines consuming from a bounded buffered channel (`jobQueue`, capacity `50,000`).
3. **Buffer Management**: Fixed `bufio.Scanner` slice reuse pitfall by copying byte payloads before dispatching to the channel.
4. **Graceful Shutdown**: 5-phase teardown coordinated by `signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)`:
   * Phase A: Close listener (`[1/5]`).
   * Phase B: Sever active client connections tracked in `sync.Map`.
   * Phase C: Wait for connection handler routines (`connWg.Wait()`, `[2/5]`).
   * Phase D: Close `jobQueue` (`[3/5]`).
   * Phase E: Wait for workers to drain remaining jobs (`workerWg.Wait()`, `[4/5]`).
   * Phase F: Flush and close Kafka writer (`[5/5]`).
5. **Observability**: Background throughput reporter (`reportThroughput`) and embedded `net/http/pprof` server on `127.0.0.1:6060`.
6. **Synthetic Generator**: Standalone executable in [`generator/generator.go`](../generator/generator.go) simulating random-walk order flows (`PETR4`, `VALE3`, `ITUB4`, `BBDC4`) with 0.1% probability anomaly spikes.

### Phase 2: Event Streaming & Decoupled Architecture (✅ COMPLETED)
1. **Redpanda in Docker**: Single-node broker deployed via [`docker-compose.yml`](../docker-compose.yml) exposing port `9092` (Kafka API) and `9644` (Admin API). Runs in `--mode dev-container`.
2. **Kafka Producer**: Integrated `segmentio/kafka-go` into [`main.go`](../main.go) sharing a single thread-safe `*kafka.Writer`.
3. **Partitioning Strategy**: Unified topic `market-ticks` with **4 partitions**. Configured `Balancer: &kafka.Hash{}` with `Key: []byte(tick.Ticker)` guaranteeing strict per-asset FIFO ordering.
4. **Producer Resilience & Backpressure**: Implemented `dispatchMsg` with exponential backoff (`50ms` up to `1s` ceiling). Drains safely on writer closure via `io.ErrClosedPipe`.
5. **Chaos Verification**: Live mid-stream broker pause (`docker pause redpanda`) proved zero data loss: **192,754 / 192,754 ticks delivered (100.000%)** with automatic backpressure halting the generator and self-healing upon unpause.

### Phase 3: Stream Inference, Anomaly Detection & Distributed Caching (✅ COMPLETED)
1. **Valkey Service (Step 1 - Complete)**: Added `valkey/valkey:latest` to [`docker-compose.yml`](../docker-compose.yml) on port `6379:6379` with `command: ["valkey-server", "--save", ""]` for ephemeral, ultra-fast in-memory execution without background snapshot disk I/O.
2. **Stateless Python Consumer (Step 2 - Complete & Verified)**:
   * Built isolated analytical worker environment inside [`consumer/`](../consumer/) managed with `uv`.
   * Integrated `confluent-kafka` client with manual offset commits (`enable.auto.commit = False`) to guarantee at-least-once delivery.
   * Graceful signal handling (`SIGINT`/`SIGTERM`) triggering explicit `consumer.close()` (`LeaveGroup` protocol request) for instantaneous partition rebalance.
   * End-to-end verification passed: **121,416 / 121,416 messages** ingested by Go gateway, published to Redpanda, consumed by Python worker, and committed with **0 total lag**.
3. **Distributed Idempotent Consumer (Step 3 - Complete)**:
   * Fixed `TimestampNs` from `int16` to `int64` across [`main.go`](../main.go) and [`generator/generator.go`](../generator/generator.go), eliminating wrap-around collisions and enabling zero-allocation nanosecond trade tracking.
   * Expanded ticker seeds from 4 to 10 assets (`PETR4`, `VALE3`, `BBDC4`, `JBSS3`, `ITUB4`, `ABEV3`, `BBAS3`, `WEGE3`, `MGLU3`, `RENT3`) to uniformly saturate all 4 Kafka partitions under FNV-1a hashing.
   * Integrated atomic check-and-set deduplication in [`consumer/consumer.py`](../consumer/consumer.py) using `valkey_client.set(f"tick:{ticker}:{timestamp}", "seen", ex=60, nx=True)`. Duplicates are acknowledged to Kafka and dropped before reaching the ML inference layer.
4. **Stream Inference & Anomaly Detection (Step 4 - Complete)**:
   * Built [`consumer/detector.py`](../consumer/detector.py): `RollingZScoreDetector` maintaining a 2D circular buffer `deque[tuple[float, int]]` storing `(price, volume)` per ticker.
   * **Tier 1 (Statistical)**: Dual-Condition Guard — `|z_price| > 3.0` AND `pct_change >= 2.0%` — guards against the micro-volatility trap (zero-std false positives).
   * **Tier 2 (ML)**: `sklearn.ensemble.IsolationForest(n_estimators=50, contamination=0.02, random_state=42)`, fitted on normalized features `[z_price, z_volume]` and re-fitted every `refit_interval=100` ticks.
   * **ML Score Delay**: Both fitting and scoring deferred until `tick_counters[ticker] >= ml_score_delay` (default 500). IsolationForest is stateless — each `.fit()` discards the previous model. Early fits on the narrow cold-start window produce poorly-calibrated boundaries. Delaying both fit and score to tick 500 ensures the first trusted model is trained on a regime-stable distribution. During delay: `ml_score = 0.0`, Tier 1 only.
   * **Feature normalization**: Raw `[price, volume]` replaced by `[(price - μ_P)/σ_P, (volume - μ_V)/σ_V]` before `.fit()` and `.score_samples()`, centering the normal cluster at $(0, 0)$ and equalizing axis variance.
   * **Detection logic**: `is_anomaly = tier1_anomaly or (ml_scored_sample < -0.65)` — independent OR-logic; either tier can trigger. Uses `score_samples` threshold instead of `predict` to avoid the Contamination Trap.
   * **Diagnosed and resolved false-positive alert storm**: Root cause was training `IsolationForest` on raw `[price, volume]` and using `predict == -1` as the anomaly gate, causing the outer 2% boundary of the normal distribution to constantly fire.
5. **Anomaly Event Emission (Step 5 - Complete)**:
   * `confluent_kafka.Producer` initialized alongside the consumer (`acks=1`, `message.send.max.retries=3`).
   * Confirmed anomalies produce a structured JSON event to the `market-anomalies` topic, keyed by `ticker` to preserve per-ticker FIFO ordering across all 4 partitions.
   * `anomaly_tier` field serialized as `AnomalyTierIndicator.name` (`'STATISTICAL'`, `'ML'`, or `'BOTH'`) for human-readable downstream consumption.
   * `producer.poll(0)` called after each `produce()` to drain librdkafka delivery callbacks without blocking the consumer loop.
   * `producer.flush()` in the `finally` block guarantees no buffered anomaly events are dropped on shutdown.
   * End-to-end verified live: `market-anomalies` topic consumed, confirming correct JSON schema, partition routing by ticker key, and FIFO ordering.

### Phase 4: Time-Series Persistence & Infrastructure-as-Code (🚀 IN PROGRESS)
1. **QuestDB Ingestion Infrastructure (Step 1 - Complete)**: Added `questdb/questdb:latest` to [`docker-compose.yml`](../docker-compose.yml) exposing port `9000` (Web Console, REST, HTTP ILP), `9009` (TCP ILP), and `8812` (PGWire). Pre-created partitioned tables `market_ticks` and `market_anomalies` (`PARTITION BY DAY`) with symbol caching (`CAPACITY 16 CACHE`), and verified ILP HTTP writes and `ASOF JOIN` queries.
2. **Go Persistence Worker (Step 2 - Built & Documented)**:
   * Built dedicated streaming worker in [`persister/persister.go`](../persister/persister.go).
   * Separate consumer groups: `gobbler-ticks-persister-group` and `gobbler-anomalies-persister-group` with `StartOffset: kafka.FirstOffset`.
   * High-performance ILP formatting via `strconv.Append*` and slice buffer reuse.
   * Dual-trigger batching engine (`batchSize=5000` or `flushInterval=500ms`).
   * HTTP POST flushes to QuestDB `/write` endpoint with Keep-Alive connection pooling.
7. Multi-partition offset commits (`reader.CommitMessages(ctx, batchKafkaMessages...)`) executed only post-flush acknowledgment (HTTP 204/200).
   * Clean shutdown lifecycle handling draining in-flight buffers under a 3-second bounded context timeout.
3. **Microservices Containerization (Step 3 - Complete & Documented)**:
   * **12-Factor Configuration**: Integrated dynamic environment variable overrides (`KAFKA_BROKER`, `INGESTION_GATEWAY_ADDR`, `INGESTION_GATEWAY_PORT`, `PPROF_ADDR`, `QUESTDB_URL`, `VALKEY_HOST`, `VALKEY_PORT`) across Go and Python codebases.
   * **Go Services ([`Dockerfile.gateway`](../Dockerfile.gateway), [`Dockerfile.generator`](../Dockerfile.generator), [`Dockerfile.persister`](../Dockerfile.persister))**:
     - Multi-stage: `golang:1.25-alpine` builder -> `FROM scratch` runtime.
     - Security: 100% static binaries (`CGO_ENABLED=0`, `-ldflags="-s -w"`), running as unprivileged `appuser` (UID 10001).
     - Minimal footprints: Gateway (~8.09 MB), Generator (~2.57 MB), Persister (~7.73 MB).
   * **Python ML Worker ([`Dockerfile.consumer`](../Dockerfile.consumer))**:
     - Multi-stage: `python:3.12-slim` builder (isolated virtualenv `/opt/venv`) -> `python:3.12-slim` runtime.
     - Debian glibc chosen over Alpine to utilize pre-compiled wheels for NumPy, SciPy, Scikit-learn, and Confluent-Kafka without compiling from source.
     - Real-time logging configured via `ENV PYTHONUNBUFFERED=1` and unprivileged execution (`UID 10001`).
   * **Build Context Optimization ([`.dockerignore`](../.dockerignore))**: Excluded virtualenvs, Python caches, git objects, and logs, shrinking context from 242MB to <50KB.
   * **Documentation**: Full line-by-line comments added across all Dockerfiles detailing Docker syntax, layer caching, and security rationale.
4. **Infrastructure-as-Code with Terraform / OpenTofu (Step 4 - Complete & Verified)**:
   * Codified entire local data plane and microservice stack under [`infra/`](../infra/) using `kreuzwerker/docker >= 3.0.2`.
   * **Foundational Layer ([`infra/main.tf`](../infra/main.tf), [`infra/versions.tf`](../infra/versions.tf))**: Provisions dedicated bridge network `gobbler-network` with automatic container DNS, and named volume `questdb_data` mounted to `/var/lib/questdb`.
   * **Auxiliary Data Plane ([`infra/auxiliary.tf`](../infra/auxiliary.tf))**:
     - `valkey`: Configured with `--save "\"\""` to disable RDB snapshots for sub-millisecond in-memory cache operations, satisfying provider validation.
     - `redpanda`: Configured with `--smp 1`, `--memory 1G`, and **Dual Named Listeners** (`internal://0.0.0.0:29092` advertised as `internal://redpanda:29092` for container-to-container traffic, and `external://0.0.0.0:9092` advertised as `external://localhost:9092` for host development and `rpk` CLI).
     - `questdb`: Configured with ports `9000` (Web/REST/ILP), `9009` (TCP ILP), `8812` (PGWire), and persistent volume mount.
   * **Option C Modular Architecture ([`infra/workloads.tf`](../infra/workloads.tf))**:
     - Feature toggle via `count = var.enable_microservices ? 1 : 0`.
     - Default (`false`): Provisions auxiliary data plane only, enabling rapid host-based development loops.
     - Full-Stack (`true`): Provisions `gobbler-gateway`, `gobbler-persister`, and `gobbler-consumer` containers with 12-factor environment variables and DAG dependencies (`depends_on`).
   * **Output Contracts ([`infra/outputs.tf`](../infra/outputs.tf))**: Exports connection endpoints and dynamic gateway status (`localhost:8008` vs disabled).
   * **Live Verification**: `terraform apply` successfully provisioned all resources; verified container health, Redpanda `rpk cluster info`, Valkey `PONG`, and QuestDB SQL REST API (`/exec?query=show+tables`).

---

## 3. Core Problems Solved & Architectural Rationale

| Decision | Chosen Approach | Alternative Considered | Rationale |
| :--- | :--- | :--- | :--- |
| **Kafka Client (Go)** | `segmentio/kafka-go` | `confluent-kafka-go` | 100% Pure Go (Zero Cgo). Enables minimal `scratch`/`alpine` Docker images, seamless ARM64 cross-compilation for future Raspberry Pi cluster, and full `pprof` visibility. |
| **Kafka Client (Python)**| `confluent-kafka` | `kafka-python` | Native C bindings (`librdkafka`). Runs polling, batch decompression, and protocol framing outside the Python GIL, handling 100k+ msg/s with sub-millisecond polling. |
| **Streaming Broker** | **Redpanda** | Apache Kafka | Native C++20 engine (Seastar), zero JVM GC pauses, Direct I/O (`O_DIRECT`), 2-second local startup, unified `rpk` CLI tooling, and flat p99 latency for high-frequency finance. |
| **Broker Storage** | **Ephemeral (No volumes)** | Persistent volume mounts | Prevents stale consumer offsets, allows changing partition counts freely, and avoids accumulating gigabytes of local disk log segments during rapid load testing. |
| **Topic Topology** | **Single topic + Key hashing** | Topic per ticker | Avoids broker metadata explosion, enables horizontal consumer group scaling across 4 partitions, and simplifies downstream worker subscriptions. |
| **Commit Semantics** | **Manual Commits (`enable.auto.commit = False`)** | Auto-commit | Auto-commit commits periodically regardless of worker status, risking unanalyzed tick loss upon mid-batch crashes. Manual commits guarantee at-least-once processing. |
| **Deduplication Cache** | **Valkey atomic `SETNX` (`EX 60`)** | In-memory Python sets | Worker sets cannot coordinate across horizontally scaled consumer pods and cause OOMs over time. Valkey provides single-threaded atomic check-and-set with bounded TTL memory (~24MB for 60s). |
| **Message Identity** | **`int64` Nanoseconds (`time.Now().UnixNano()`)** | Random UUIDv4 string | UUIDs incur ~150ns + string allocations + 36 bytes of extra JSON wire overhead per tick. High-res nanosecond timestamps are zero-allocation, monotonic, and standard in quantitative finance. |
| **Partition Balancer** | **FNV-1a Hash + 10 Ticker Seeds** | Round-Robin | Round-Robin scatters an asset's ticks across partitions, breaking FIFO ordering and sliding window calculations. Key hashing maintains strict per-asset FIFO while 10 tickers ensure all 4 partitions are active. |
| **ML Anomaly Tier** | **`IsolationForest.score_samples` threshold (`< -0.65`)** | `IsolationForest.predict` | `predict` with `contamination=0.02` guarantees 2% of any training window is labeled as anomaly, producing constant false positives on normal random-walk ticks (Contamination Trap). `score_samples` exposes the continuous path-length score for domain-calibrated thresholding. |
| **ML Feature Space** | **Normalized `[z_price, z_volume]`** | Raw `[price, volume]` | Raw features have incompatible scales (price in cents, volume in hundreds). Volume dominates isolation tree splits. Z-score normalization centers the normal cluster at $(0,0)$ with unit variance on both axes, equalizing split probability and geometrically separating genuine outliers. |
| **Anomaly Logic** | **Independent OR (`tier1 or ml_scored < -0.65`)** | AND cascade | AND logic gates Tier 2 behind Tier 1, making ML blind to volume-only anomalies (no significant price move). OR logic lets each tier detect its complementary failure mode independently. |
| **ML Score Delay** | **Deferred fit+score (`ml_score_delay=500`)** | Fit from tick 100, score immediately | `IsolationForest` is stateless: each `.fit()` discards the previous model. The first 100-tick window has a narrow, unrepresentative distribution producing borderline false positives. Delaying both fit and score to tick 500 (5 × `refit_interval`) ensures the first trusted model is regime-stable. Delaying both (not just scoring) avoids wasted CPU on throwaway fits. |
| **Persistence Protocol** | **InfluxDB Line Protocol (ILP over HTTP/TCP)** | PostgreSQL Wire (PGWire) | ILP bypasses SQL parsing, query planning, and transactional locking, appending directly to column memory buffers with zero-copy symbol interning. At 50,000+ msgs/sec, ILP scales to millions of rows/sec without CPU bottlenecks. |
| **Persistence Architecture** | **Decoupled Go Service (`persister/`)** | Colocated inside Python ML Consumer | Isolates failure domains and prevents Head-of-Line blocking (slow DB writes stall ML inference). Lets CPU-heavy ML and I/O-heavy persistence scale independently. Uses Go for high-throughput zero-allocation batch assembly. |
| **Storage Topology** | **Two Dedicated Tables (`market_ticks`, `market_anomalies`)** | Single Unified Table | Time-series databases are append-only (no row `UPDATE`). Dedicated tables keep tick ingestion at maximum append throughput, keep anomaly audit queries instantaneous without full table scans, and allow clean `ASOF JOIN` queries. |
| **Table Partitioning** | **`PARTITION BY DAY`** | `PARTITION BY HOUR` | For local development and test runs, `BY DAY` bounds open file descriptors and directory sprawl. Under continuous 50k msgs/sec (~180M rows/hr), `BY HOUR` reaches 1-3GB partitions, aligning with QuestDB's 100MB-10GB partition sizing rule. |
| **Persistence Batching** | **Dual-Trigger (`5,000` rows or `500ms`)** | Per-message streaming insert | Optimizes QuestDB ILP chunk sizes while bounding freshness latency to 500ms during quiet periods. Consumer offsets are committed only post-flush to guarantee at-least-once persistence. |

---

## 4. Configuration Invariants & Network Ports

* **TCP Gateway Port**: `127.0.0.1:8008`
* **Gateway pprof Port**: `127.0.0.1:6060` (`/debug/pprof/`)
* **Redpanda Kafka Port**: `localhost:9092`
* **Redpanda Admin Port**: `localhost:9644`
* **Valkey Port**: `localhost:6379`
* **Target Topic**: `market-ticks` (Pre-created with **4 partitions**)
* **Anomaly Topic**: `market-anomalies` (Pre-created with **4 partitions**, keyed by ticker)
* **Consumer Group ID**: `gobbler-ml-consumers`
* **Gateway Buffer Capacity**: `50,000` payloads (`jobQueue`)
* **Detector Parameters**:
  * `window_size=100` ticks (circular buffer per ticker)
  * `min_observations=20` ticks (Tier 1 cold-start guard)
  * `z_threshold=3.0` (Tier 1 outlier threshold)
  * `min_pct_change=0.02` (Tier 1 material-move guard)
  * `refit_interval=100` ticks (Tier 2 periodic re-fit cadence)
  * `ml_score_delay=500` ticks (Tier 2 deferred activation threshold)
  * `ml_score_threshold=-0.65` (Tier 2 isolated-outlier boundary)
* **Gateway Buffer Capacity**: `50,000` payloads (`jobQueue`)
* **Producer Batching Tuning**:
  * `BatchSize: 100` messages
  * `BatchTimeout: 5 * time.Millisecond`
  * `RequiredAcks: kafka.RequireOne`
  * `WriteTimeout: 2 * time.Second`
* **Retry Parameters**: Initial backoff `50ms`, multiplier `2x`, max ceiling `1s`.
* **Valkey Deduplication TTL**: `60` seconds (`EX 60`).
* **QuestDB Web Console / REST / HTTP ILP Port**: `localhost:9000`
* **QuestDB TCP ILP Port**: `localhost:9009`
* **QuestDB PostgreSQL Wire Port**: `localhost:8812`
* **QuestDB Target Tables**: `market_ticks` (`PARTITION BY DAY`), `market_anomalies` (`PARTITION BY DAY`)
* **Persister Consumer Group ID**: `gobbler-persister-group`
* **Persister Batch Tuning**: `MaxBatchSize: 5000` rows, `MaxFlushInterval: 500ms`

---

## 5. Current File Map

```text
├── AGENTS.md                    # Agent persona rules (Socratic coding tutor)
├── .dockerignore                # Build context filtering (excludes venv, pycache, git, logs)
├── Dockerfile.consumer          # Python ML worker: multi-stage python:3.12-slim (/opt/venv -> non-root)
├── Dockerfile.gateway           # TCP Gateway: multi-stage golang:1.25-alpine -> scratch (8.09MB)
├── Dockerfile.generator         # Mock Generator: multi-stage golang:1.25-alpine -> scratch (2.57MB)
├── Dockerfile.persister         # ILP Persister: multi-stage golang:1.25-alpine -> scratch (7.73MB)
├── consumer/
│   ├── .venv/                   # Isolated virtual environment (git-ignored)
│   ├── consumer.py              # Kafka consumer: dedup, inference loop, anomaly producer, manual commits
│   ├── detector.py              # RollingZScoreDetector: two-tier Z-Score + IsolationForest
│   └── requirements.txt         # Python dependencies (confluent-kafka, redis, numpy, scikit-learn)
├── docker-compose.yml           # Redpanda & Valkey container service definitions
├── docs/
│   ├── context.md               # Continuous architecture state (this file)
│   ├── mlinferencing.md         # Study guide: rolling windows, Z-score, IsolationForest math
│   ├── plan.md                  # Project roadmap & progress tracker
│   ├── questdb.md               # QuestDB concepts, ILP format, table partitioning & SQL reference
│   ├── redpanda.md              # Redpanda concepts, CLI guide & architecture reference
│   ├── terraform.md             # Terraform & OpenTofu concepts, Docker provider & CLI guide
│   └── valkey.md                # Valkey concepts, Idempotent Consumer & CLI guide
├── generator/
│   └── generator.go             # Package main - standalone exchange simulator (10 B3 assets)
├── go.mod                       # Module github.com/pedrotorreao/gobbler
├── go.sum                       # Dependency checksums
├── main.go                      # Package main - Ingestion Gateway, pprof & Kafka producer
├── persister/
│   └── persister.go             # Package main - ILP persistence worker (Kafka -> QuestDB)
└── README.md                    # Public project overview & quickstart
```

---

## 6. Immediate Next Steps: Phase 4 (Time-Series Persistence & Infrastructure-as-Code)

* [x] **Step 1**: Spin up **QuestDB** in [`docker-compose.yml`](../docker-compose.yml) (Port `9000` Web/HTTP/ILP, Port `9009` TCP ILP, Port `8812` PGWire). Verified DDL table creation (`market_ticks`, `market_anomalies`), ILP HTTP ingestion (`204 OK`), and `ASOF JOIN` analytics.
* [x] **Step 2**: Implement dedicated Go persistence service ([`persister/persister.go`](../persister/persister.go)) subscribing to `market-ticks` and `market-anomalies`, formatting ILP batches (5,000 rows or 500ms), and streaming to QuestDB with post-flush offset commits.
* [x] **Step 3**: Package every custom microservice using multi-stage, minimal-footprint Dockerfiles ([`Dockerfile.gateway`](../Dockerfile.gateway), [`Dockerfile.generator`](../Dockerfile.generator), [`Dockerfile.persister`](../Dockerfile.persister), [`Dockerfile.consumer`](../Dockerfile.consumer)). Verified all Go binaries on `scratch` (~2.5MB–8MB) and Python on `python:3.12-slim` (~413MB) running as unprivileged user `appuser` (UID 10001).
* [x] **Step 4**: Declarative container, network, and volume orchestration via **Terraform / OpenTofu** in [`infra/`](../infra/). Verified auxiliary data plane live apply, dual-listener Kafka architecture, and Option C microservice toggle.
* **Next Steps**: Step 5 (Kind Kubernetes cluster manifests, Helm/Kustomize, and HPA).
