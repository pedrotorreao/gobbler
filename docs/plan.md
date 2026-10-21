# Project Mission & Architectural Goal

Build a production-grade, distributed Real-Time Financial Market Surveillance & Anomaly Detection Platform.
The system simulates high-throughput financial exchange data, ingests it via a high-performance network gateway,
decouples it using an event broker, processes it for statistical anomalies with machine learning, and stores it
in a high-speed time-series database. The entire environment is containerized, orchestrated, and provisioned via
Infrastructure-as-Code.

```
[Go Mock Exchange]
       │ (TCP Stream: ~50k msgs/sec)
       ▼
[Go Ingestion Gateway] ── (Zero-Allocation Buffers / sync.Pool)
       │
       ▼
[Apache Kafka / Redpanda] ── (Partitioned by Asset Ticker)
       │
       ├──► [Python ML Engine] ──► [Valkey Distributed Cache] (Idempotency)
       │           │ (Inference / Anomaly Alert)
       │           ▼
       │     (market-anomalies)
       │           │
       ▼           ▼
[Go Persistence Service] ── (ILP Batching: 5k rows / 500ms)
       │
       ▼
   [QuestDB] ◄── (Time-Series Storage & SQL Analytics)
                   ▲
[Kind (K8s) / OpenTofu / Prometheus Observability Layer]
```

---

## Project Roadmap & Task List (10 Hours/Week)

### Phase 1: Ingestion Gateway & Go Concurrency (Weeks 1–3) — ✅ COMPLETED
*Build the high-throughput, low-level networking foundation.*

- [x] **Step 1:** Write a basic single-threaded TCP server in Go using the standard `net` package to observe blocking behavior under multiple clients.
- [x] **Step 2:** Refactor the server using a bounded worker pool pattern (goroutines + buffered channels) to decouple socket reads from payload processing.
- [x] **Step 3:** Implement production-grade lifecycle controls: signal handling (`SIGINT`, `SIGTERM`), `context.Context` cancellation, and graceful socket shutdown.
- [x] **Step 4:** Write a standalone Go load-generator utility (`generator/`) that simulates an exchange, blasting synthetically randomized price ticks (`PETR4`, `VALE3`, `ITUB4`, `BBDC4`) over TCP.
- [x] **Bonus Step:** Add throughput metrics reporter per second and integrate `net/http/pprof` for real-time profiling.

**Definition of Done (DoD):**
- [x] The Go server accepts concurrent client connections without dropping traffic or spawning unconstrained goroutines.
- [x] Terminating the server gracefully finishes draining all buffered in-flight messages without data loss.
- [x] The mock generator can saturate the local network socket without deadlocking the Go runtime.

---

### Phase 2: Event Streaming & Decoupled Architecture (Weeks 4–6) — ✅ COMPLETED
*Introduce fault tolerance, shock absorption, and horizontal routing.*

- [x] **Step 1:** Run a single-node Apache Kafka (or Redpanda) instance locally inside Docker (`docker-compose.yml`).
- [x] **Step 2:** Integrate a native Go Kafka producer client (`segmentio/kafka-go`) into the Ingestion Gateway (`main.go`).
- [x] **Step 3:** Implement an explicit partitioning strategy using asset tickers as partition keys to guarantee strict per-asset message ordering (`&kafka.Hash{}`).
- [x] **Step 4:** Configure producer resilience: client-side buffering, batch sizes, retry timeouts, and producer backpressure when the broker slows down (`dispatchMsg` exponential backoff).

**Definition of Done (DoD):**
- [x] Incoming TCP ticks are serialized into structured formats (JSON or Protocol Buffers) and published directly to Kafka.
- [x] Messages with the same ticker symbol consistently route to the exact same Kafka partition.
- [x] Killing the broker process causes the Go gateway to buffer and back off gracefully without crashing or running out of memory.

---

### Phase 3: Stream Inference, Anomaly Detection & Distributed Caching (Weeks 7–9) — ✅ COMPLETED
*Implement real-time analytical workers, machine learning, and network reliability patterns.*

- [x] **Step 1:** Spin up Valkey in a local Docker container (`docker-compose.yml`).
- [x] **Step 2:** Write a stateless Python consumer using consumer groups to pull real-time ticks from Kafka.
- [x] **Step 3:** Implement the Idempotent Consumer pattern: assign unique composite IDs to messages and check Valkey via atomic `SETNX` operations (with a 60s expiration TTL) before processing.
- [x] **Step 4:** Build the AI inference layer using NumPy and Scikit-learn:
  - **Tier 1 (Statistical)**: Rolling Z-score with a Dual-Condition Guard (`|Z| > 3.0` AND `pct_change >= 2%`) against the micro-volatility trap.
  - **Tier 2 (ML)**: `IsolationForest` fitted on normalized `[z_price, z_volume]` features from the rolling window, re-fitted every 100 ticks. Uses `score_samples` with a calibrated threshold (`< -0.65`) instead of `predict` to avoid the Contamination Trap.
  - **Detection Logic**: Independent OR — either tier independently triggers an alert, covering both univariate price shocks and multivariate co-movement anomalies (e.g., volume-only spikes).
  - **ML Score Delay** (`ml_score_delay=500`): IsolationForest fitting and scoring are both deferred until 500 ticks have been observed per ticker. Since each `.fit()` call is stateless (no memory across fits), early fits during the cold-start window would be throwaway fits producing poorly-calibrated boundaries. Deferring both saves CPU and ensures the first trusted score comes from a regime-stable distribution.
- [x] **Step 5:** Emit detected anomalies to a separate `market-anomalies` Kafka topic for downstream notification.
  - Confirmed `confluent_kafka.Producer` initialized with `acks=1` and `message.send.max.retries=3`.
  - Anomaly JSON payload keyed by `ticker` (preserving per-ticker FIFO ordering on `market-anomalies`).
  - `anomaly_tier` serialized as string name (`'STATISTICAL'`, `'ML'`, `'BOTH'`) via `AnomalyTierIndicator.name`.
  - `producer.poll(0)` called after each `produce()` to drain librdkafka's delivery callback queue.
  - `producer.flush()` called in the `finally` block after `consumer.close()` to guarantee no buffered events are dropped on shutdown.
  - End-to-end verified: `market-anomalies` topic consumed live, confirming correct JSON schema, ticker key routing to correct partition, and FIFO ordering.

**Definition of Done (DoD):**
- [x] Injected duplicate messages are recognized and dropped by the Valkey cache check.
- [x] The Python worker computes sliding-window statistics over streaming data with sub-15ms inference latency.
- [x] Simulated anomalies consistently trigger downstream alerts to the dedicated `market-anomalies` topic (end-to-end verified).

---

### Phase 4: Time-Series Persistence & Infrastructure-as-Code (Weeks 10–12) — 🚀 IN PROGRESS
*Automate environment setup and configure high-throughput analytical storage.*

- [x] **Step 1:** Deploy QuestDB using Docker (`docker-compose.yml`) exposing InfluxDB Line Protocol (HTTP `:9000` & TCP `:9009`), PostgreSQL wire (`:8812`), and Web Console (`:9000`). Verified DDL table creation (`market_ticks`, `market_anomalies`), ILP HTTP ingestion (`204 OK`), and ASOF JOIN queries.
- [x] **Step 2:** Write a dedicated Go persistence service ([`persister/persister.go`](../persister/persister.go)) to ingest raw tick data (`market-ticks`) and anomaly alerts (`market-anomalies`) from Kafka into dedicated partitioned QuestDB tables (`market_ticks` and `market_anomalies`) via InfluxDB Line Protocol (ILP) with a dual-trigger batching strategy (5,000 rows or 500ms).
- [x] **Step 3:** Package every custom microservice (Ingest Gateway, Mock Generator, ML Worker, Persistence Worker) using multi-stage, minimal-footprint Dockerfiles (`Dockerfile.gateway`, `Dockerfile.generator`, `Dockerfile.persister`, `Dockerfile.consumer`). All Go binaries compiled to static binaries on `scratch` (~2.5MB–8MB) and Python worker in `python:3.12-slim` (~413MB) running as unprivileged user `appuser` (UID 10001).
- [ ] **Step 4:** Write OpenTofu scripts to declaratively provision local containers, storage volumes, and network bridges.
- [ ] **Step 5:** Create local Kubernetes (Kind) manifests declaring Deployments, Services, ConfigMaps, and Horizontal Pod Autoscalers (HPA) for the Python consumer tier.

**Definition of Done (DoD):**
- [x] Financial tick data and anomaly records are persistently queryable in QuestDB via SQL. (Verified live: 69,966 ticks and 1,622 anomalies ingested via ILP and queried via ASOF JOIN).
- [ ] Running a single `tofu apply` command spins up the required auxiliary infrastructure from a clean state.
- [ ] The Python inference worker tier scales pod replicas up and down inside Kind based on Kafka consumer lag.

---

### Phase 5: High-Load Profiling, Chaos Engineering & Interview Packaging (Weeks 13–14) — ⏳ PENDING
*Break the system under load, remove performance bottlenecks, and prepare portfolio assets.*

- [ ] **Step 1:** Run a 50,000+ message/sec load test using your Go generator. Profile the Go gateway using `pprof` to identify CPU/memory bottlenecks.
- [ ] **Step 2:** Refactor the Go ingestion path to use `sync.Pool`, recycling byte buffers to eliminate per-packet heap allocations and reduce garbage collector (GC) pauses.
- [ ] **Step 3:** Execute failure testing: pull down worker pods, simulate database drops, and record metrics on how the system recovers.
- [ ] **Step 4:** Build a clean GitHub repository containing: detailed architectural diagrams, reproducible benchmark commands, and a written post-mortem.

**Definition of Done (DoD):**
- [ ] Memory profiling traces confirm a measurable reduction in heap allocations and GC pause times after implementing `sync.Pool`.
- [ ] The system demonstrates self-healing behavior under node termination without data corruption.
- [ ] The GitHub repository provides one-command local reproduction scripts and clear documentation.

---

### Optional Extension: Physical Raspberry Pi Cluster Deployment — ⏳ OPTIONAL
- [ ] Migrate the working Kind Kubernetes manifests to a physical 3- to 4-node k3s cluster running on Raspberry Pi 4s/5s.
- [ ] Set up multi-arch builds (`docker buildx` for `linux/arm64`), configure persistent storage via NFS or Longhorn, and run physical cable-pulling chaos tests.

---

## Phase Kickoff Status

| Task | Target Timeline | Status |
| ---- | --------------- | ------ |
| Set up Go development environment & verify `main.go` runs locally | Week 1, Day 1 | [x] Completed |
| Run single-threaded TCP server & reproduce blocking failure with `netcat` | Week 1, Day 2-3 | [x] Completed |
| Design bounded worker pool & channels for non-blocking I/O | Week 2 | [x] Completed |
| Implement graceful shutdown (`context.Context` + `sync.WaitGroup`) | Week 2-3 | [x] Completed |
| Spin up local Redpanda/Kafka container (Docker) | Week 4 | [x] Completed |
| Integrate `segmentio/kafka-go` producer into Gateway | Week 4 | [x] Completed |
| Configure Producer resilience, timeouts & backpressure | Week 4-5 | [x] Completed |
| Spin up local Valkey container (Docker Compose) | Week 7 | [x] Completed |
| Write stateless Python consumer with Consumer Groups | Week 7-8 | [x] Completed |
| Implement Idempotent Consumer (Valkey SETNX deduplication) | Week 8 | [x] Completed |
| Build Two-Tier anomaly detector (`detector.py`: Z-Score + IsolationForest) | Week 8-9 | [x] Completed |
| Emit anomalies to `market-anomalies` Kafka topic | Week 9 | [x] Completed |
| Refine ML reliability: `ml_score_delay=500` deferred fitting to avoid early boundary instability | Week 9 | [x] Completed |
| Spin up local QuestDB container (Docker Compose) | Week 10 | [x] Completed |
| Build Go ILP persistence worker (`persister/`) with dual-trigger batching | Week 10-11 | [x] Completed |
| End-to-end verification of Phase 4 pipeline (Kafka -> Persister -> QuestDB) | Week 11 | [x] Completed |
| Package microservices using multi-stage Dockerfiles (`gateway`, `generator`, `persister`, `consumer`) | Week 11-12 | [x] Completed |
| Declarative container & volume orchestration via OpenTofu (IaC) | Week 12 | [x] Completed |

