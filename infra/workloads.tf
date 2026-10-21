# ==============================================================================
# Gobbler Infrastructure: Application Microservices (Option C Workloads)
# ==============================================================================
# This file declares Gobbler's custom application microservices packaged in
# Phase 4 Step 3:
#   1. Ingestion Gateway (Go / scratch)    - High-throughput TCP tick ingestion
#   2. Time-Series Persister (Go / scratch) - Dual-trigger ILP batch writer
#   3. ML Consumer (Python / 3.12-slim)     - Rolling Z-Score & IsolationForest
#
# Feature Toggle Architecture:
# Each resource uses 'count = var.enable_microservices ? 1 : 0'.
#   - Default (false): Only auxiliary data plane is provisioned. Developers run
#     microservices on the host (e.g. 'go run main.go') with sub-second feedback.
#   - Full-Stack Demo (true): Provisions the entire distributed surveillance stack
#     inside Docker ('terraform apply -var="enable_microservices=true"').
# ==============================================================================

# ==============================================================================
# 1. Ingestion Gateway (Go Concurrency Engine)
# ==============================================================================
# Ingests high-throughput synthetic exchange order flows over raw TCP, batches
# payloads via bounded channels, and produces to Redpanda's 'market-ticks' topic.

resource "docker_container" "gateway" {
  # Conditional idiom: 1 instance when enabled, 0 instances when disabled
  count = var.enable_microservices ? 1 : 0

  name  = "gobbler-gateway"
  image = "gobbler-gateway:latest"

  # Attach to bridge network to resolve and route traffic to 'redpanda:29092'
  networks_advanced {
    name = docker_network.gobbler_network.name
  }

  # Publish TCP ingestion listener to host port (8008) for synthetic generators
  ports {
    internal = 8008
    external = var.gateway_port
  }

  # Publish pprof diagnostic server (6060) for real-time CPU/memory heap profiling
  ports {
    internal = 6060
    external = var.pprof_port
  }

  # 12-Factor Environment Variables:
  # Overrides hardcoded localhost defaults to point to container network peers.
  env = [
    "KAFKA_BROKER=redpanda:29092",
    "INGESTION_GATEWAY_ADDR=0.0.0.0",
    "INGESTION_GATEWAY_PORT=8008",
    "PPROF_ADDR=0.0.0.0:6060"
  ]

  # Explicit DAG Dependency: Ensures Redpanda is healthy and accepting connections
  # before the gateway initializes its kafka.Writer connection pool.
  depends_on = [docker_container.redpanda]

  restart = "unless-stopped"
}

# ==============================================================================
# 2. Time-Series Persister (Go ILP Streaming Worker)
# ==============================================================================
# Consumes both 'market-ticks' and 'market-anomalies' streams from Redpanda,
# buffers records in memory (5,000 rows or 500ms flush ceiling), formats zero-copy
# InfluxDB Line Protocol (ILP), and flushes batches to QuestDB over HTTP POST.

resource "docker_container" "persister" {
  count = var.enable_microservices ? 1 : 0

  name  = "gobbler-persister"
  image = "gobbler-persister:latest"

  networks_advanced {
    name = docker_network.gobbler_network.name
  }

  # Configure target Kafka broker and QuestDB HTTP write endpoint
  env = [
    "KAFKA_BROKER=redpanda:29092",
    "QUESTDB_URL=http://questdb:9000/write"
  ]

  # Persister depends on both the event broker and the time-series database
  depends_on = [
    docker_container.redpanda,
    docker_container.questdb
  ]

  restart = "unless-stopped"
}

# ==============================================================================
# 3. Python ML Consumer (Two-Tier Anomaly Detection Engine)
# ==============================================================================
# Consumes ticks, deduplicates via atomic Valkey SETNX, updates per-ticker rolling
# statistical windows (Z-score), scores non-linear feature interactions using
# scikit-learn IsolationForest, and emits anomaly events to 'market-anomalies'.

resource "docker_container" "consumer" {
  count = var.enable_microservices ? 1 : 0

  name  = "gobbler-consumer"
  image = "gobbler-consumer:latest"

  networks_advanced {
    name = docker_network.gobbler_network.name
  }

  # Configure endpoints for Kafka stream ingestion and Valkey deduplication cache
  env = [
    "KAFKA_BROKER=redpanda:29092",
    "VALKEY_HOST=valkey",
    "VALKEY_PORT=6379"
  ]

  # Consumer requires both the Kafka broker and the distributed deduplication cache
  depends_on = [
    docker_container.redpanda,
    docker_container.valkey
  ]

  restart = "unless-stopped"
}

# ==============================================================================
# Architectural Note: Synthetic Market Generator ('gobbler-generator')
# ==============================================================================
# The synthetic generator ('Dockerfile.generator') is intentionally excluded from
# long-running daemon workloads. As a high-volume load simulation tool, it is
# run on demand with configurable message limits rather than running perpetually:
#   docker run --rm --net=gobbler-network gobbler-generator:latest
# ==============================================================================
