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
* **Kubernetes / Kind Deep Dive**: [`kubernetes.md`](kubernetes.md)
* **Post-Mortem & Chaos Engineering Report**: [`post-mortem-2026-11-09.md`](post-mortem-2026-11-09.md)

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

### Phase 4: Time-Series Persistence & Infrastructure-as-Code (✅ COMPLETED)
1. **QuestDB Ingestion Infrastructure (Step 1 - Complete)**: Added `questdb/questdb:latest` to [`docker-compose.yml`](../docker-compose.yml) exposing port `9000` (Web Console, REST, HTTP ILP), `9009` (TCP ILP), and `8812` (PGWire). Pre-created partitioned tables `market_ticks` and `market_anomalies` (`PARTITION BY DAY`) with symbol caching (`CAPACITY 16 CACHE`), and verified ILP HTTP writes and `ASOF JOIN` queries.
2. **Go Persistence Worker (Step 2 - Built & Documented)**:
   * Built dedicated streaming worker in [`persister/persister.go`](../persister/persister.go).
   * Separate consumer groups: `gobbler-ticks-persister-group` and `gobbler-anomalies-persister-group` with `StartOffset: kafka.FirstOffset`.
   * High-performance ILP formatting via `strconv.Append*` and slice buffer reuse.
   * Dual-trigger batching engine (`batchSize=5000` or `flushInterval=500ms`).
   * HTTP POST flushes to QuestDB `/write` endpoint with Keep-Alive connection pooling.
   * Multi-partition offset commits (`reader.CommitMessages(ctx, batchKafkaMessages...)`) executed only post-flush acknowledgment (HTTP 204/200).
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
     - `redpanda`: Configured with `--smp 1`, `--memory 1G`, and **Dual Named Listeners** (`internal://0.0.0.0:29092` advertised as `internal://redpanda:29092` for container-to-container traffic, and `external://0.0.0.0:9092` advertised as `external://172.18.0.1:9092` for hybrid host + Kind cluster routing).
     - `questdb`: Configured with ports `9000` (Web/REST/ILP), `9009` (TCP ILP), `8812` (PGWire), and persistent volume mount.
   * **Option C Modular Architecture ([`infra/workloads.tf`](../infra/workloads.tf))**:
     - Feature toggle via `count = var.enable_microservices ? 1 : 0`.
     - Default (`false`): Provisions auxiliary data plane only, enabling rapid host-based development loops.
     - Full-Stack (`true`): Provisions `gobbler-gateway`, `gobbler-persister`, and `gobbler-consumer` containers with 12-factor environment variables and DAG dependencies (`depends_on`).
   * **Output Contracts ([`infra/outputs.tf`](../infra/outputs.tf))**: Exports connection endpoints and dynamic gateway status (`localhost:8008` vs disabled).
   * **Live Verification**: `terraform apply` successfully provisioned all resources; verified container health, Redpanda `rpk cluster info`, Valkey `PONG`, and QuestDB SQL REST API (`/exec?query=show+tables`).
5. **Local Kubernetes (Kind) Orchestration & Autoscaling (Step 5 - Complete & Verified)**:
   * **Multi-Node Cluster ([`k8s/kind-config.yaml`](../k8s/kind-config.yaml))**: 3 nodes (1 control-plane, 2 workers) with `extraPortMappings` for `30008 -> 8008` (TCP ingress) and `30060 -> 6060` (pprof).
   * **Snap Docker Image Loading Fix**: Bypassed Snap's strict `/tmp` mount namespace confinement by setting `TMPDIR=$HOME/tmp` during `kind load docker-image`.
   * **Hybrid Ingress Architecture**:
     - `gobbler-gateway`: `Deployment` (50m CPU / 32Mi RAM request) + `NodePort Service` exposing ports 30008 and 30060.
     - `gobbler-persister`: Pure background consumer `Deployment` (50m CPU / 32Mi RAM request). No Service required.
     - `gobbler-consumer`: Python ML analytical `Deployment` (100m CPU / 128Mi RAM request).
   * **Centralized Configuration ([`k8s/01-configmap.yaml`](../k8s/01-configmap.yaml))**: Injected via `envFrom.configMapRef` with host gateway routing (`172.18.0.1`).
   * **Kafka Advertised Listener Fix**: Configured Redpanda's external advertised address as `172.18.0.1:9092` in [`infra/auxiliary.tf`](../infra/auxiliary.tf) to eliminate the Kafka advertised listener trap where pods resolved `localhost` internally.
   * **Horizontal Pod Autoscaler ([`k8s/05-hpa.yaml`](../k8s/05-hpa.yaml))**: Autoscaling targeted at `gobbler-consumer` (min 1, max 4, 60% CPU utilization).
   * **Live Load & Scalability Verification**: Ingested streaming load via host generator; HPA detected 501% CPU utilization and scaled consumer replicas from 1 to 4 (perfect 1:1 alignment across all 4 Kafka partitions); persister flushed **133,459 ticks** and **2,796 anomalies** to QuestDB with 0 lag across all partitions.

### Phase 5: High-Load Profiling, Chaos Engineering & Interview Packaging (✅ COMPLETED)
1. **High-Load Saturation & Baseline Profiling (Step 1 - Complete)**:
   * **Problem Encountered**: The original synthetic generator maxed out around ~1,600 msgs/sec because unbuffered `conn.Write` calls invoked a `write()` syscall per JSON line, `time.Sleep(500µs)` suffered from OS scheduler timer slack, and the gateway's `kafka.Writer` blocked worker goroutines on synchronous broker round-trips (`Async: false`).
   * **Solution**: Refactored [`generator/generator.go`](../generator/generator.go) to wrap TCP sockets in a 64KB `bufio.Writer` (matching Linux loopback `lo` MTU) with 60-tick bursts per 10ms across 10 tickers and a 5ms periodic flush ticker. Enabled `Async: true` on `kafka.Writer` in [`main.go`](../main.go), surging throughput to **56,400+ msgs/sec** (peaking at 67,200 msgs/sec).
   * **Memory Profiling Discovery**: Captured `baseline_heap_50k.pb.gz` via `net/http/pprof` (`-alloc_objects` / `-alloc_space`), pinpointing **294,934 heap allocations (22.50 MB in 5s)** inside `handleConnection` (`payloadCopy := make([]byte, len(rawBytes))`).
2. **Zero-Allocation Buffer Recycling via `sync.Pool` (Step 2 - Complete)**:
   * **Problems Encountered**:
     1. Passing a value slice `[]byte` (24-byte header) to `sync.Pool.Put(any)` triggers an interface boxing allocation on the heap (`runtime.convTslice`).
     2. Under `Async: true`, `kafkaWriter.WriteMessages` returns immediately while retaining a reference to `msg.Value` in background batch queues; returning the buffer to `sync.Pool` inside `worker` causes a concurrent data race when `handleConnection` overwrites the recycled slice.
   * **Solution**: Pooled pointer-to-slice `*[]byte` (`make([]byte, 0, 512)`) in `bufferPool`, changed `jobQueue` to `chan *[]byte`, attached `WriterData: bufPtr` to each `kafka.Message`, and deferred `bufferPool.Put(bufPtr)` to the `kafkaWriter.Completion` callback (with defensive `bufferPool.Put` on early `json.Unmarshal` or `ctx.Done()` exits in `worker`).
   * **Verification**: Differential `pprof -base baseline_heap_50k.pb.gz -alloc_objects syncpool_heap_50k.pb.gz` proved **-294,934 allocations eliminated** (0 flat allocations in `handleConnection`).
3. **Multi-Tier Chaos Engineering & The "Phantom Offset Skip" Fix (Step 3 - Complete)**:
   * **Experiment 1 (Broker Drop)**: Paused Redpanda (`docker pause redpanda`) for 4s mid-stream at 56k msgs/sec; verified 100.000% zero-loss delivery (**+675,720 ticks**) upon unpause.
   * **Experiment 2 (Consumer Crash & Rebalance)**: Added `WORKER_ID` tag and log throttling (every 1,000th normal tick) to [`consumer/consumer.py`](../consumer/consumer.py). Killed `WORKER-A` with `kill -9` mid-stream; verified instant partition handoff to `WORKER-B` (`rpk group describe gobbler-ml-consumers`) and zero duplicate anomalies via Valkey `SETNX` (`EX 60`).
   * **Experiment 3 (Database Outage & "Phantom Offset Skip")**:
     - **Problem Encountered**: In [`persister/persister.go`](../persister/persister.go), when `flushToQuestDB` failed, the worker skipped `CommitMessages` (good) but still truncated `batchMessages = batchMessages[:0]` and continued calling `reader.FetchMessage`. Because `kafka.Reader` advances its internal cursor on fetch, a subsequent successful batch commit permanently jumped over the failed batch's offsets in Kafka (silent data loss). Additionally, the shutdown drain block unconditionally committed offsets even if the final flush failed.
     - **Solution**: Implemented `flushWithRetry` with exponential backoff (`50ms` initial up to `1s` ceiling) using `time.NewTimer` + `select` on `<-ctx.Done()`, keeping `batchMessages` in RAM until QuestDB acknowledges HTTP 204/200 and gating the shutdown `CommitMessages` behind an `else` branch. Verified live by stopping QuestDB (`docker stop questdb -t 1`) mid-stream and restarting (`docker start questdb`), persisting **+454,042 ticks** with zero loss.
4. **Written Post-Mortem (Step 4 - Complete)**:
   * Authored [`docs/post-mortem-2026-11-09.md`](post-mortem-2026-11-09.md) with GitHub-relative links, profiling flamegraph tables, chaos timelines, and reproduction commands.
5. **Automated End-to-End Verification Harness (Step 5 - Complete)**:
   * Built [`scripts/e2e_test.sh`](../scripts/e2e_test.sh) with extensive shell-scripting educational notes (`set -euo pipefail`, `trap cleanup EXIT`, readiness polling, PCRE JSON extraction).
   * **Problems Encountered & Solved**:
     1. *Topic Auto-Creation Race*: Starting microservices simultaneously with Redpanda causes consumers to trigger 1-partition topic auto-creation before 4-partition topics exist. Solved with a **Two-Stage Terraform Apply** (`enable_microservices=false` -> initialize 4-partition topics & QuestDB DDLs -> `enable_microservices=true`).
     2. *Host Port 6379 Collision*: Local system `redis-server` daemon bound to `127.0.0.1:6379` blocked Valkey container host port binding. Solved by probing `ss -tln` for `:6379` and dynamically exporting `TF_VAR_valkey_port=16379` while preserving internal `valkey:6379` container DNS on `gobbler-network`.
   * **Live Verification**: Executed `./scripts/e2e_test.sh` end-to-end; ingested and persisted **640,001 market ticks** and **31 anomalies** in QuestDB in a 12-second burst followed by clean automatic teardown.

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
| **Container Base Images** | **`FROM scratch` (Go) & `python:3.12-slim` (Python)** | `alpine` for Python / `ubuntu` for Go | Go binaries are compiled with `CGO_ENABLED=0` into static ELF binaries (~2.5–8MB) that need zero OS userspace. Conversely, Python ML wheels (`numpy`, `scipy`, `scikit-learn`, `confluent-kafka`) rely on `manylinux` (`glibc`); Alpine (`musl`) forces 15-minute C/Fortran source builds. |
| **Snap Docker & Kind Loading** | **`TMPDIR=$HOME/tmp kind load docker-image`** | Default `/tmp` export | Ubuntu Snap Docker runs inside a confined mount namespace where `/tmp` is isolated from the host `/tmp`, causing `kind load` tar exports to fail with `unable toopen ... no such file`. Redirecting `TMPDIR` to `$HOME/tmp` bridges the Snap boundary cleanly. |
| **Kafka Advertised Listeners (Hybrid K8s/Host)** | **`external://172.18.0.1:9092` (`gobbler-network` gateway)** | `external://localhost:9092` | When Redpanda advertises `localhost:9092`, Kind pods connect to `172.18.0.1:9092` for bootstrap metadata and are told to reconnect to `localhost:9092`—which resolves to the pod's own loopback (`ECONNREFUSED`). Advertising the Docker bridge gateway IP `172.18.0.1:9092` works from both host processes and Kind pods. |
| **Generator Loopback Framing & Timer Granularity** | **64KB `bufio.Writer` + 60-tick bursts every `10ms`** | Unbuffered `conn.Write` + `500µs` sleep per tick | Per-line `conn.Write` invokes a kernel `write()` syscall per tick, and Linux timer slack turns `500µs` sleeps into `1–2ms`, capping throughput at ~1,600 msgs/sec. A 64KB user-space buffer matches the Linux loopback (`lo`) MTU (65,536 bytes), amortizing syscalls across `10ms` bursts to exceed **56,400+ msgs/sec**. |
| **Gateway Kafka Producer Mode** | **Asynchronous (`Async: true` + `Completion` callback)** | Synchronous (`Async: false`) | Synchronous `WriteMessages` blocks each worker goroutine until a batch fills (`BatchTimeout: 5ms`) and the broker round-trip completes, capping 12 workers at ~2,400 msgs/sec. `Async: true` decouples worker handoff from broker I/O, scaling to 56k+ msgs/sec. |
| **Memory Pool Representation & Lifecycle (`sync.Pool`)** | **Pooling `*[]byte` (512B cap) + recycling via `kafka.Message.WriterData` in `Completion`** | Pooling `[]byte` value slices or recycling inside `worker` | Passing a 24-byte `[]byte` header to `sync.Pool.Put(any)` allocates a heap box (`runtime.convTslice`). Furthermore, with `Async: true`, `WriteMessages` returns while background batches still read `msg.Value`; recycling in `worker` causes a data race. Recycling `*[]byte` via `WriterData` inside `Completion` eliminated **294,934 heap allocations** per 5s window with zero races. |
| **Persistence Outage Recovery ("Phantom Offset Skip")** | **Cancellable Exponential Backoff (`flushWithRetry`: `50ms` -> `1s`) retaining `batchMessages` in RAM** | Logging error and truncating `batchMessages[:0]` without committing | Truncating `batchMessages[:0]` after a failed DB write while keeping the consumer loop running allows `reader.FetchMessage` to advance its cursor. When the DB recovers and a later batch commits, Kafka jumps over the failed batch's offsets (silent data loss). Blocking in `flushWithRetry` holds the batch in memory and applies natural backpressure to Kafka until QuestDB recovers. |
| **Automated E2E Provisioning Order & Host Port Collisions** | **Two-Stage Terraform Apply + Dynamic `TF_VAR_valkey_port` Override in `scripts/e2e_test.sh`** | Single-pass `terraform apply` with hardcoded host ports | Starting microservices simultaneously with Redpanda causes consumers to auto-create 1-partition topics before 4-partition topics are initialized. Furthermore, developer hosts often have a local `redis-server` on `:6379`; probing `ss -tln` and exporting `TF_VAR_valkey_port=16379` avoids host port conflicts while preserving `valkey:6379` internal container DNS. |

---

## 4. Configuration Invariants & Network Ports

* **TCP Gateway Port**: `127.0.0.1:8008` (Host) / `30008` (Kind NodePort)
* **Gateway pprof Port**: `127.0.0.1:6060` (Host) / `30060` (Kind NodePort, `/debug/pprof/`)
* **Redpanda Kafka Port**: `localhost:9092` (Host) / `172.18.0.1:9092` (External Advertised) / `redpanda:29092` (Internal Docker DNS)
* **Redpanda Admin Port**: `localhost:9644`
* **Valkey Port**: `localhost:6379`
* **Target Topic**: `market-ticks` (Pre-created with **4 partitions**)
* **Anomaly Topic**: `market-anomalies` (Pre-created with **4 partitions**, keyed by ticker)
* **Consumer Group ID**: `gobbler-ml-consumers`
* **Gateway Buffer Capacity**: `50,000` payloads (`jobQueue chan *[]byte`)
* **Gateway Buffer Pool**: `sync.Pool` storing `*[]byte` (initial capacity `512` bytes)
* **Detector Parameters**:
  * `window_size=100` ticks (circular buffer per ticker)
  * `min_observations=20` ticks (Tier 1 cold-start guard)
  * `z_threshold=3.0` (Tier 1 outlier threshold)
  * `min_pct_change=0.02` (Tier 1 material-move guard)
  * `refit_interval=100` ticks (Tier 2 periodic re-fit cadence)
  * `ml_score_delay=500` ticks (Tier 2 deferred activation threshold)
  * `ml_score_threshold=-0.65` (Tier 2 isolated-outlier boundary)
* **Producer Batching Tuning**:
  * `BatchSize: 100` messages
  * `BatchTimeout: 5 * time.Millisecond`
  * `RequiredAcks: kafka.RequireOne`
  * `WriteTimeout: 2 * time.Second`
  * `Async: true` (buffer recycling triggered via `Completion` callback inspecting `kafka.Message.WriterData`)
* **Retry Parameters**: Initial backoff `50ms`, multiplier `2x`, max ceiling `1s`.
* **Valkey Deduplication TTL**: `60` seconds (`EX 60`).
* **QuestDB Web Console / REST / HTTP ILP Port**: `localhost:9000`
* **QuestDB TCP ILP Port**: `localhost:9009`
* **QuestDB PostgreSQL Wire Port**: `localhost:8812`
* **QuestDB Target Tables**: `market_ticks` (`PARTITION BY DAY`), `market_anomalies` (`PARTITION BY DAY`)
* **Persister Consumer Group IDs**: `gobbler-ticks-persister-group`, `gobbler-anomalies-persister-group`
* **Persister Batch Tuning**: `MaxBatchSize: 5000` rows, `MaxFlushInterval: 500ms`
* **Persister Flush Retry**: `flushWithRetry` exponential backoff (`50ms` initial, `2x` multiplier, `1s` max ceiling, cancellable via `ctx.Done()`).

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
├── docker-compose.yml           # Redpanda, Valkey & QuestDB container service definitions
├── docs/
│   ├── context.md               # Continuous architecture state (this file)
│   ├── kubernetes.md            # Kubernetes & Kind orchestration guide, primitives & HPA
│   ├── mlinferencing.md         # Study guide: rolling windows, Z-score, IsolationForest math
│   ├── plan.md                  # Project roadmap & progress tracker
│   ├── post-mortem-2026-11-09.md # High-load profiling, sync.Pool & multi-tier chaos post-mortem
│   ├── questdb.md               # QuestDB concepts, ILP format, table partitioning & SQL reference
│   ├── redpanda.md              # Redpanda concepts, CLI guide & architecture reference
│   ├── terraform.md             # Terraform & OpenTofu concepts, Docker provider & CLI guide
│   └── valkey.md                # Valkey concepts, Idempotent Consumer & CLI guide
├── generator/
│   └── generator.go             # Package main - standalone exchange simulator (10 B3 assets, 64KB buffer)
├── go.mod                       # Module github.com/pedrotorreao/gobbler
├── go.sum                       # Dependency checksums
├── infra/
│   ├── auxiliary.tf             # Redpanda, Valkey & QuestDB Docker container resources
│   ├── main.tf                  # Docker network (gobbler-network) & QuestDB named volume
│   ├── outputs.tf               # Provisioned service endpoint outputs
│   ├── variables.tf             # Input variables & enable_microservices toggle
│   ├── versions.tf              # Terraform & kreuzwerker/docker provider requirements
│   └── workloads.tf             # Optional containerized microservice resources
├── k8s/
│   ├── 01-configmap.yaml        # Centralized environment config pointing to 172.18.0.1
│   ├── 02-gateway.yaml          # Gateway Deployment & NodePort Service (30008, 30060)
│   ├── 03-persister.yaml        # Persister Deployment
│   ├── 04-consumer.yaml         # Python ML Consumer Deployment
│   ├── 05-hpa.yaml              # HorizontalPodAutoscaler (1-4 replicas @ 60% CPU)
│   └── kind-config.yaml         # 3-node Kind cluster topology with host port mappings
├── main.go                      # Package main - Ingestion Gateway, sync.Pool, pprof & async Kafka producer
├── persister/
│   └── persister.go             # Package main - ILP persistence worker with flushWithRetry
├── scripts/
│   └── e2e_test.sh              # Automated E2E verification harness (IaC -> Stream -> Assert -> Teardown)
└── README.md                    # Public project overview & quickstart
```

---

## 6. Immediate Next Steps: Phase 5 (High-Load Profiling, Chaos Engineering & Memory Pool Refactoring — ✅ ALL STEPS COMPLETED)

* [x] **Step 1**: High-Load Saturation & Empirical Profiling.
  - Saturated Go Ingestion Gateway at **56,400+ msgs/sec** (676,800 msgs in 12s) using optimized synthetic generator (64KB loopback buffer, 60 ticks/10ms bursts) and asynchronous Kafka producer (`Async: true`).
  - Captured baseline heap allocation profile (`baseline_heap_50k.pb.gz`).
  - Identified primary heap allocation bottleneck: `handleConnection` inlining allocating fresh slices on every scan (`payloadCopy := make([]byte, len(rawBytes))`) accounting for **294,934 heap allocations (22.50 MB)** in a 5-second sampling window. Secondary bottleneck: JSON unmarshaling (`MarketTick`) in `worker`.
* [x] **Step 2**: Buffer Recycling via `sync.Pool`.
  - Implemented `sync.Pool` storing `*[]byte` (pre-allocated 512-byte capacity) to eliminate slice header boxing.
  - Linked end-to-end zero-copy lifecycle across `handleConnection`, `jobQueue chan *[]byte`, `worker`, and asynchronous Kafka writer via `kafka.Message.WriterData` and `kafkaWriter.Completion` callback.
  - Handled defensive early returns on JSON unmarshal errors and shutdown channel drain.
  - Re-ran 56,000+ msgs/sec benchmark (675,060 msgs in 12s, peaking at 67,200 msgs/sec) and captured `syncpool_heap_50k.pb.gz`.
  - Comparative profiling (`pprof -base baseline_heap_50k.pb.gz syncpool_heap_50k.pb.gz`) confirmed **-294,934 object allocations eliminated** from the network ingestion loop, reducing per-packet heap allocations in `handleConnection` to **0**.
* [x] **Step 3**: Chaos Engineering & Failure Recovery.
  - **Broker Drop**: Mid-stream Redpanda container pause proved 100.000% zero-loss delivery (675,720 / 675,720 ticks) with asynchronous buffer queuing and `Completion` callback drainage on unpause.
  - **Consumer Crash & Rebalance**: Abrupt `SIGKILL` (`kill -9`) of active ML worker triggered instant Redpanda partition reassignment to surviving workers with Valkey atomic `SETNX` (TTL 60s) idempotency preventing duplicate inference.
  - **Database Outage**: Simulated QuestDB container crash during streaming; implemented `flushWithRetry` with exponential backoff (`50ms` to `1s`) and cancellable `time.NewTimer` select loop in [`persister/persister.go`](../persister/persister.go). Eliminated phantom offset skips, held in-memory buffers under backpressure, and verified 100% zero-loss batch delivery upon database recovery (454k+ ticks persisted).
* [x] **Step 4**: Architecture documentation, benchmark playbooks & post-mortem.
  - Authored comprehensive system post-mortem in [`docs/post-mortem-2026-11-09.md`](post-mortem-2026-11-09.md) covering high-load memory profiling, the `sync.Pool` zero-allocation refactoring, failure analysis across all 3 tiers, the "phantom offset skip" discovery and remediation, architectural invariants, and local benchmark reproduction playbooks.
* [x] **Step 5**: Automated E2E verification script ([`scripts/e2e_test.sh`](../scripts/e2e_test.sh)).
  - Implemented a 7-stage automated E2E harness with `set -euo pipefail`, `trap cleanup EXIT` (`--keep` flag supported), dynamic `TF_VAR_valkey_port` collision detection, multi-stage image builds, two-stage Terraform provisioning, readiness polling, 12-second synthetic load streaming, and QuestDB SQL row assertions (**640,001 ticks** and **31 anomalies** verified live).


