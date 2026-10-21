# ==============================================================================
# Gobbler Infrastructure: Output Values
# ==============================================================================
# This file exports key connection endpoints, URLs, and resource identifiers
# provisioned by Terraform.
#
# Operational Usage:
#   - Human developers: Run 'terraform output' to view local service URLs.
#   - Automated scripts: Query specific values using 'terraform output -raw <name>'
#     or parse the complete infrastructure state with 'terraform output -json'.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Network Topology
# ------------------------------------------------------------------------------

output "network_name" {
  description = "Name of the dedicated Docker bridge network; pass to 'docker run --net' for ad-hoc container testing"
  value       = docker_network.gobbler_network.name
}

# ------------------------------------------------------------------------------
# 2. Auxiliary Data Plane Connection Endpoints (Host-Accessible)
# ------------------------------------------------------------------------------

output "kafka_broker" {
  description = "Redpanda external Kafka wire-protocol endpoint for host applications, Go tests, and rpk CLI"
  value       = "localhost:${var.redpanda_kafka_port}"
}

output "valkey_endpoint" {
  description = "Valkey in-memory cache endpoint for local CLI inspection ('valkey-cli -p 6379 ping')"
  value       = "localhost:${var.valkey_port}"
}

output "questdb_web_url" {
  description = "QuestDB Web Console and SQL REST API URL (open in browser to inspect tables and execute queries)"
  value       = "http://localhost:${var.questdb_web_ui_port}"
}

output "questdb_ilp_url" {
  description = "QuestDB InfluxDB Line Protocol (ILP) HTTP ingestion endpoint used by persistence workers"
  value       = "http://localhost:${var.questdb_web_ui_port}/write"
}

# ------------------------------------------------------------------------------
# 3. Application Workload Endpoints
# ------------------------------------------------------------------------------

output "gateway_endpoint" {
  description = "Ingestion Gateway TCP ingress endpoint for synthetic generators; indicates whether container is running or host execution is expected"
  value       = var.enable_microservices ? "localhost:${var.gateway_port}" : "disabled (run locally via 'go run main.go')"
}
