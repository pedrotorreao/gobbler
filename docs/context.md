# Gobbler: System Context & Session State

> **Notice for New Agent Sessions**: This document is the single source of operational truth for Gobbler. It tracks active architecture decisions, solved engineering trade-offs, configuration invariants, and current roadmap state. Update this file whenever completing a phase or making architectural adjustments.

---

## 1. Project Mission & High-Level Architecture

Gobbler is a distributed real-time financial market surveillance and anomaly detection platform simulating high-throughput exchange data (~50,000 msgs/sec), ingesting it through a low-latency network gateway, streaming it via an event broker, detecting price anomalies with Python ML workers, and persisting records in a time-series database.

* **Tech Stack**: Go 1.22+, Docker & Docker Compose, Redpanda (Kafka API), Python (NumPy/Scikit-learn - Phase 3), Valkey (Phase 3), QuestDB (Phase 4), Kind/OpenTofu (Phase 4).
* **Roadmap Reference**: [`plan.md`](file:///home/pedrotorreao/Documents/Projects/gobbler/docs/plan.md)
* **Redpanda Deep Dive**: [`redpanda.md`](file:///home/pedrotorreao/Documents/Projects/gobbler/docs/redpanda.md)

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
6. **Synthetic Generator**: Standalone executable in [`generator/generator.go`](file:///home/pedrotorreao/Documents/Projects/gobbler/generator/generator.go) simulating random-walk order flows (`PETR4`, `VALE3`, `ITUB4`, `BBDC4`) with 0.1% probability anomaly spikes.

### Phase 2: Event Streaming & Decoupled Architecture (✅ COMPLETED)
1. **Redpanda in Docker**: Single-node broker deployed via [`docker-compose.yml`](file:///home/pedrotorreao/Documents/Projects/gobbler/docker-compose.yml) exposing port `9092` (Kafka API) and `9644` (Admin API). Runs in `--mode dev-container`.
2. **Kafka Producer**: Integrated `segmentio/kafka-go` into [`main.go`](file:///home/pedrotorreao/Documents/Projects/gobbler/main.go) sharing a single thread-safe `*kafka.Writer`.
3. **Partitioning Strategy**: Unified topic `market-ticks` with **4 partitions**. Configured `Balancer: &kafka.Hash{}` with `Key: []byte(tick.Ticker)` guaranteeing strict per-asset FIFO ordering.
4. **Producer Resilience & Backpressure**: Implemented `dispatchMsg` with exponential backoff (`50ms` up to `1s` ceiling). Drains safely on writer closure via `io.ErrClosedPipe`.
5. **Chaos Verification**: Live mid-stream broker pause (`docker pause redpanda`) proved zero data loss: **192,754 / 192,754 ticks delivered (100.000%)** with automatic backpressure halting the generator and self-healing upon unpause.

### Phase 3: Stream Inference, Anomaly Detection & Distributed Caching (🚀 IN PROGRESS)
1. **Valkey Service (Step 1 - Complete)**: Added `valkey/valkey:latest` to [`docker-compose.yml`](file:///home/pedrotorreao/Documents/Projects/gobbler/docker-compose.yml) on port `6379:6379` with `command: ["valkey-server", "--save", ""]` for ephemeral, ultra-fast in-memory execution without background snapshot disk I/O.
2. **Stateless Python Consumer (Step 2 - Complete & Verified)**:
   * Built isolated analytical worker environment inside [`consumer/`](file:///home/pedrotorreao/Documents/Projects/gobbler/consumer) managed with `uv`.
   * Integrated `confluent-kafka` client with manual offset commits (`enable.auto.commit = False`) to guarantee at-least-once delivery.
   * Graceful signal handling (`SIGINT`/`SIGTERM`) triggering explicit `consumer.close()` (`LeaveGroup` protocol request) for instantaneous partition rebalance.
   * End-to-end verification passed: **121,416 / 121,416 messages** ingested by Go gateway, published to Redpanda, consumed by Python worker, and committed with **0 total lag**.
3. **Distributed Idempotent Consumer (Step 3 - Complete)**:
   * Fixed `TimestampNs` from `int16` to `int64` across [`main.go`](file:///home/pedrotorreao/Documents/Projects/gobbler/main.go) and [`generator/generator.go`](file:///home/pedrotorreao/Documents/Projects/gobbler/generator/generator.go), eliminating wrap-around collisions and enabling zero-allocation nanosecond trade tracking.
   * Expanded ticker seeds from 4 to 10 assets (`PETR4`, `VALE3`, `BBDC4`, `JBSS3`, `ITUB4`, `ABEV3`, `BBAS3`, `WEGE3`, `MGLU3`, `RENT3`) to uniformly saturate all 4 Kafka partitions under FNV-1a hashing.
   * Integrated atomic check-and-set deduplication in [`consumer/consumer.py`](file:///home/pedrotorreao/Documents/Projects/gobbler/consumer/consumer.py) using `valkey_client.set(f"tick:{ticker}:{timestamp}", "seen", ex=60, nx=True)`. Duplicates are acknowledged to Kafka and dropped before reaching the ML inference layer.
4. **Stream Inference & Anomaly Detection (Step 4 - Complete)**:
   * Built [`consumer/detector.py`](file:///home/pedrotorreao/Documents/Projects/gobbler/consumer/detector.py): `RollingZScoreDetector` maintaining a 2D circular buffer `deque[tuple[float, int]]` storing `(price, volume)` per ticker.
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

---

## 5. Current File Map

```text
├── AGENTS.md                    # Agent persona rules (Socratic coding tutor)
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
│   ├── redpanda.md              # Redpanda concepts, CLI guide & architecture reference
│   └── valkey.md                # Valkey concepts, Idempotent Consumer & CLI guide
├── generator/
│   └── generator.go             # Package main - standalone exchange simulator (10 B3 assets)
├── go.mod                       # Module github.com/pedrotorreao/gobbler
├── go.sum                       # Dependency checksums
├── main.go                      # Package main - Ingestion Gateway, pprof & Kafka producer
└── README.md                    # Public project overview & quickstart
```

---

## 6. Immediate Next Steps: Phase 3 (Stream Inference & Anomaly Detection)

* [x] **Step 1**: Spin up **Valkey** in [`docker-compose.yml`](file:///home/pedrotorreao/Documents/Projects/gobbler/docker-compose.yml) (Port `6379`).
* [x] **Step 2**: Build stateless Python consumer using consumer groups ([`consumer/consumer.py`](file:///home/pedrotorreao/Documents/Projects/gobbler/consumer/consumer.py)).
* [x] **Step 3**: Implement **Idempotent Consumer pattern** with Valkey atomic `SETNX` (with 60s TTL).
* [x] **Step 4**: Build the stream inference layer in [`consumer/detector.py`](file:///home/pedrotorreao/Documents/Projects/gobbler/consumer/detector.py): Two-tier hybrid detector with rolling Z-score (Tier 1) and `IsolationForest` on normalized `[z_price, z_volume]` features (Tier 2). Independent OR-logic with `score_samples < -0.65` threshold. ML activation deferred to tick 500 via `ml_score_delay`.
* [x] **Step 5**: Emit detected anomalies to a dedicated `market-anomalies` Kafka topic. Keyed by ticker, full JSON payload with `anomaly_tier` name. Producer lifecycle (init, `poll(0)`, `flush()`) fully wired into the consumer event loop. End-to-end verified.
* **Phase 3 Status**: ✅ COMPLETED — Next: Phase 4 (QuestDB persistence).
