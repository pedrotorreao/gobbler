# ==============================================================================
# Gobbler Infrastructure: Input Variables
# ==============================================================================
# This file parameterizes the infrastructure deployment, decoupling resource
# definitions from environment-specific ports, image tags, and operational modes.
# Values can be overridden via CLI flags (-var), environment variables
# (TF_VAR_*), or terraform.tfvars files.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Core Networking & Storage
# ------------------------------------------------------------------------------

variable "network_name" {
  type        = string
  description = "Name of the dedicated Docker bridge network facilitating inter-service communication"
  default     = "gobbler-network"
}

variable "questdb_volume_name" {
  type        = string
  description = "Name of the named Docker volume mounted to QuestDB (/var/lib/questdb) for table persistence"
  default     = "questdb_data"
}

# ------------------------------------------------------------------------------
# 2. Auxiliary Data Plane: Container Images
# ------------------------------------------------------------------------------

variable "redpanda_image" {
  type        = string
  description = "Official Redpanda image repository and version tag"
  default     = "docker.redpanda.com/redpandadata/redpanda:latest"
}

variable "valkey_image" {
  type        = string
  description = "Official Valkey image repository and version tag"
  default     = "valkey/valkey:latest"
}

variable "questdb_image" {
  type        = string
  description = "Official QuestDB image repository and version tag"
  default     = "questdb/questdb:latest"
}

# ------------------------------------------------------------------------------
# 3. Auxiliary Data Plane: Exposed Host Ports
# ------------------------------------------------------------------------------

variable "redpanda_kafka_port" {
  type        = number
  description = "External host port mapped to Redpanda's external Kafka API listener"
  default     = 9092
}

variable "redpanda_admin_port" {
  type        = number
  description = "External host port mapped to Redpanda's HTTP Admin API (used by rpk CLI)"
  default     = 9644
}

variable "valkey_port" {
  type        = number
  description = "External host port mapped to Valkey in-memory cache"
  default     = 6379
}

variable "questdb_web_ui_port" {
  type        = number
  description = "External host port mapped to QuestDB Web Console, SQL REST API, and HTTP ILP (/write)"
  default     = 9000
}

variable "questdb_ilp_port" {
  type        = number
  description = "External host port mapped to QuestDB InfluxDB Line Protocol (ILP) over TCP"
  default     = 9009
}

variable "questdb_pgwire_port" {
  type        = number
  description = "External host port mapped to QuestDB PostgreSQL Wire Protocol (PGWire)"
  default     = 8812
}

# ------------------------------------------------------------------------------
# 4. Option C: Microservices Deployment Toggle & Workload Ingress
# ------------------------------------------------------------------------------

variable "enable_microservices" {
  type        = bool
  description = "Option C toggle: when false (default), provisions only the auxiliary data plane; when true, deploys custom Go/Python microservices in Docker"
  default     = false
}

variable "gateway_port" {
  type        = number
  description = "Host port mapped to the Go Ingestion Gateway TCP ingress listener"
  default     = 8008
}

variable "pprof_port" {
  type        = number
  description = "Host port mapped to the Ingestion Gateway net/http/pprof diagnostic server"
  default     = 6060
}
