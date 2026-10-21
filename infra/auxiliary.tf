# ==============================================================================
# Gobbler Infrastructure: Auxiliary Data Plane Services
# ==============================================================================
# This file provisions the stateful core services forming Gobbler's streaming
# and persistence backbone:
#   1. Valkey   - Ultra-low-latency in-memory cache for stream deduplication
#   2. Redpanda - High-throughput Kafka-compatible event streaming broker
#   3. QuestDB  - Append-optimized time-series database with ILP ingestion
# ==============================================================================

# ==============================================================================
# 1. Image Lifecycle Management
# ==============================================================================
# Declaring docker_image resources ensures that images are present in the local
# Docker cache prior to container creation.
#
# keep_locally = true:
# When executing 'terraform destroy', Terraform preserves the downloaded image
# layers on disk rather than untagging/deleting them. This prevents redundant,
# bandwidth-heavy downloads during rapid development and re-apply iterations.

resource "docker_image" "valkey" {
  name         = var.valkey_image
  keep_locally = true
}

resource "docker_image" "redpanda" {
  name         = var.redpanda_image
  keep_locally = true
}

resource "docker_image" "questdb" {
  name         = var.questdb_image
  keep_locally = true
}

# ==============================================================================
# 2. Container Orchestration: Valkey (Distributed Cache)
# ==============================================================================
# Valkey serves as our high-speed, single-threaded deduplication engine.
# Python ML consumers execute atomic SETNX (SET if Not eXists with EX 60)
# operations to filter duplicate ticks before they reach inference models.

resource "docker_container" "valkey" {
  name = "valkey"

  # Implicit DAG Dependency: referencing docker_image.valkey.image_id ensures
  # Terraform pulls/validates the image before attempting container creation.
  image = docker_image.valkey.image_id

  # Connect to the user-defined bridge network for inter-container DNS resolution
  networks_advanced {
    name = docker_network.gobbler_network.name
  }

  # Publish Valkey's default port 6379 to the host machine for local debugging/CLI
  ports {
    internal = var.valkey_port
    external = var.valkey_port
  }

  # Disable background RDB snapshots ('--save ""').
  # For real-time stream deduplication, keys have a 60s TTL; avoiding periodic
  # disk I/O forks guarantees ultra-flat microsecond write latencies.
  # Note: Escaped quotes (\"\" ) satisfy the Terraform Docker provider's non-empty string rule.
  command = ["valkey-server", "--save", "\"\""]

  # Automatically restart the container unless explicitly stopped by the developer
  restart = "unless-stopped"
}

# ==============================================================================
# 3. Container Orchestration: Redpanda (Event Streaming Broker)
# ==============================================================================
# Redpanda is a C++20 Kafka-compatible streaming platform utilizing the Seastar
# framework for thread-per-core asynchronous I/O with flat p99 latency.

resource "docker_container" "redpanda" {
  name  = "redpanda"
  image = docker_image.redpanda.image_id

  # Attach to user-defined bridge network so containerized microservices reach it
  networks_advanced {
    name = docker_network.gobbler_network.name
  }

  # Port 9092: External Kafka Wire Protocol (host applications and rpk CLI)
  ports {
    internal = var.redpanda_kafka_port
    external = var.redpanda_kafka_port
  }

  # Port 9644: HTTP Admin API (cluster status, topic creation, partition health)
  ports {
    internal = var.redpanda_admin_port
    external = var.redpanda_admin_port
  }

  # Startup flags configured for local containerized development:
  #   --mode dev-container: Disables hardware checks for lightweight local execution.
  #   --smp 1: Restricts Redpanda Seastar runtime to 1 CPU core thread.
  #   --memory 1G: Enforces an explicit 1GB memory ceiling.
  #   Dual-Listener Strategy:
  #     - Internal (0.0.0.0:29092): Advertised as 'redpanda:29092' for containerized
  #       microservices communicating across 'gobbler-network'.
  #     - External (0.0.0.0:9092): Advertised as 'localhost:9092' for host tools
  #       (e.g., 'go run main.go', host Python, rpk CLI).
  command = [
    "redpanda", "start",
    "--mode", "dev-container",
    "--smp", "1",
    "--memory", "1G",
    "--kafka-addr", "internal://0.0.0.0:29092,external://0.0.0.0:9092",
    "--advertise-kafka-addr", "internal://redpanda:29092,external://localhost:9092"
  ]

  restart = "unless-stopped"
}

# ==============================================================================
# 4. Container Orchestration: QuestDB (Time-Series Database)
# ==============================================================================
# QuestDB is an append-optimized time-series SQL database designed to ingest
# millions of rows/sec via InfluxDB Line Protocol (ILP) with zero-copy caching.

resource "docker_container" "questdb" {
  name  = "questdb"
  image = docker_image.questdb.image_id

  # Attach to bridge network so Go persister reaches it at 'http://questdb:9000/write'
  networks_advanced {
    name = docker_network.gobbler_network.name
  }

  # Port 9000: Web Console, REST Query API, and HTTP ILP (/write endpoint)
  ports {
    internal = var.questdb_web_ui_port
    external = var.questdb_web_ui_port
  }

  # Port 9009: InfluxDB Line Protocol over TCP (alternative streaming ingestion)
  ports {
    internal = var.questdb_ilp_port
    external = var.questdb_ilp_port
  }

  # Port 8812: PostgreSQL Wire Protocol (allows connecting via standard psql / BI tools)
  ports {
    internal = var.questdb_pgwire_port
    external = var.questdb_pgwire_port
  }

  # Disable outbound telemetry reporting for local development performance
  env = ["QDB_TELEMETRY_ENABLED=false"]

  # Mount dedicated persistent volume to QuestDB root data directory
  # Ensures partitions, symbol tables, and schemas survive container replacement
  volumes {
    volume_name    = docker_volume.questdb_data.name
    container_path = "/var/lib/questdb"
  }

  restart = "unless-stopped"
}
