# ==============================================================================
# Gobbler Infrastructure: Provider & Version Requirements
# ==============================================================================
# This file establishes the engine constraints, required provider plugins,
# and provider initialization parameters for provisioning Gobbler's local
# infrastructure via HashiCorp Configuration Language (HCL).
#
# Interoperability Note:
# Both Terraform (>= 1.5.0) and OpenTofu (>= 1.6.0) read this manifest identically,
# providing complete engine portability without vendor lock-in.
# ==============================================================================

terraform {
  # Enforce minimum engine version supporting modern HCL features (ternary count, etc.)
  required_version = ">= 1.5.0"

  required_providers {
    # kreuzwerker/docker manages local Docker daemon resources (containers, networks, volumes, images)
    # via the Docker Engine API over the local Unix domain socket.
    docker = {
      source  = "kreuzwerker/docker"
      version = ">= 3.0.2"
    }
  }
}

# ==============================================================================
# Provider Configuration
# ==============================================================================
# Connects to the local Docker daemon via the standard Unix domain socket.
# On Linux, /var/run/docker.sock exposes the Docker REST API locally without TLS overhead.
provider "docker" {
  host = "unix:///var/run/docker.sock"
}
