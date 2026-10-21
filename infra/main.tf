# ==============================================================================
# Gobbler Infrastructure: Core Network & Storage Foundations
# ==============================================================================
# This file provisions the foundational infrastructure shared across all
# auxiliary engines and application microservices:
#   1. A user-defined Docker bridge network providing isolated inter-container
#      routing and automatic DNS service discovery.
#   2. A named persistent Docker volume ensuring QuestDB's time-series tables
#      and partition directories survive container recreation and restarts.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. User-Defined Bridge Network
# ------------------------------------------------------------------------------
# In Docker, user-defined bridge networks provide two critical capabilities:
#   - DNS Resolution: Containers can address each other by container name
#     (e.g., 'http://questdb:9000', 'redpanda:29092', 'valkey:6379') rather than
#     dynamic IP addresses.
#   - Network Isolation: Traffic is confined to this bridge, preventing external
#     containers from sniffing or interfering with market surveillance streams.
resource "docker_network" "gobbler_network" {
  name   = var.network_name
  driver = "bridge"
}

# ------------------------------------------------------------------------------
# 2. Named Storage Volume for Time-Series Persistence
# ------------------------------------------------------------------------------
# QuestDB stores daily time-series partitions on disk (/var/lib/questdb).
# By backing this directory with a dedicated named volume, table schemas
# ('market_ticks', 'market_anomalies') and persisted rows persist across
# container teardown/recreation cycles, avoiding data loss during upgrades.
resource "docker_volume" "questdb_data" {
  name = var.questdb_volume_name
}
