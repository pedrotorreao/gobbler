#!/usr/bin/env bash
# ==============================================================================
# Gobbler: Automated End-to-End (E2E) Verification Harness
# ==============================================================================
# Purpose:
#   This script automates the complete lifecycle verification of the Gobbler
#   platform from a cold start to clean teardown:
#     1. Pre-flight CLI dependency checks (Docker, Go, curl, OpenTofu/Terraform).
#     2. Multi-stage container image builds (scratch Go & slim Python images).
#     3. Stage 1 Infrastructure Provisioning (Auxiliary Data Plane: Redpanda,
#        Valkey, QuestDB).
#     4. Readiness polling & schema initialization (4-partition Kafka topics
#        and daily-partitioned QuestDB columnar tables).
#     5. Stage 2 Infrastructure Provisioning (Application Microservices:
#        gobbler-gateway, gobbler-consumer, gobbler-persister).
#     6. High-throughput synthetic market order-flow generation (~50k msgs/sec).
#     7. Automated SQL assertions against QuestDB verifying end-to-end tick
#        and anomaly persistence.
#     8. Guaranteed infrastructure teardown via POSIX signal traps.
#
# Usage:
#   ./scripts/e2e_test.sh           # Run full E2E suite and tear down on exit
#   ./scripts/e2e_test.sh --keep    # Run full E2E suite and keep containers alive
# ==============================================================================

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 1: The "Strict Mode" Header (set -euo pipefail)
# ------------------------------------------------------------------------------
# By default, Bash ignores failed commands and continues executing the rest of
# the script, which is dangerous in automation and CI/CD pipelines.
#   -e (errexit)  : Exit the script immediately if any command returns a
#                   non-zero (failure) exit status.
#   -u (nounset)  : Treat unset/undefined variables as an error and exit
#                   immediately (prevents typos like rm -rf "$UNSET_DIR/").
#   -o pipefail   : In a pipeline (cmd1 | cmd2 | cmd3), the pipeline's return
#                   code is normally just the last command's status. 'pipefail'
#                   forces the pipeline to fail if ANY command in the chain fails.
set -euo pipefail

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 2: Resolving the Repository Root Directory
# ------------------------------------------------------------------------------
# Users might invoke this script from the repo root (./scripts/e2e_test.sh) or
# from inside the scripts folder (cd scripts && ./e2e_test.sh).
#   - "${BASH_SOURCE[0]}" gives the path to this script file itself.
#   - "dirname" strips the filename, leaving the directory path.
#   - "$(cd ... && pwd)" resolves relative paths into an absolute directory path.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INFRA_DIR="${PROJECT_ROOT}/infra"

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 3: Parameter Expansion & Default Values ("${1:-}")
# ------------------------------------------------------------------------------
# Because 'set -u' is active, referencing "$1" when the user passes no arguments
# would crash the script with "1: unbound variable".
# The syntax "${1:-}" means: "Use the value of $1 if provided; otherwise, fall
# back to an empty string."
KEEP_INFRA=false
if [[ "${1:-}" == "--keep" ]]; then
    KEEP_INFRA=true
fi

# Detect whether the host uses 'terraform' or 'tofu' (OpenTofu)
IAC_CLI=""
if command -v terraform >/dev/null 2>&1; then
    IAC_CLI="terraform"
elif command -v tofu >/dev/null 2>&1; then
    IAC_CLI="tofu"
else
    echo "[ERROR] Neither 'terraform' nor 'tofu' was found in PATH." >&2
    exit 1
fi

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 4: Signal Traps (trap cleanup EXIT)
# ------------------------------------------------------------------------------
# In Go, we use 'defer cleanup()' to ensure resources are released when a
# function returns. In Bash, 'trap <function_name> EXIT' is the exact equivalent:
# the shell guarantees that 'cleanup' will execute whenever the script exits,
# whether it finishes normally (exit 0), fails mid-way due to 'set -e', or is
# interrupted by the user pressing Ctrl+C (SIGINT).
cleanup() {
    # Capture the exit code of the last command before cleanup runs so we don't
    # mask test failures.
    local exit_code=$?

    echo ""
    echo "=============================================================================="
    if [[ "${KEEP_INFRA}" == "true" ]]; then
        echo "[TEARDOWN] '--keep' flag detected. Leaving containers running for inspection."
        echo "           To tear down manually later, run:"
        echo "           cd infra && ${IAC_CLI} destroy -var=\"enable_microservices=true\" -auto-approve"
    else
        echo "[TEARDOWN] Destroying all provisioned containers, networks, and volumes..."
        cd "${INFRA_DIR}"
        "${IAC_CLI}" destroy -var="enable_microservices=true" -auto-approve >/dev/null || true
        echo "[TEARDOWN] Clean teardown complete. Zero orphaned containers or volumes left."
    fi
    echo "=============================================================================="

    exit "${exit_code}"
}

# Helper function for structured logging with timestamps
log_step() {
    echo ""
    echo "------------------------------------------------------------------------------"
    echo "[$(date +'%H:%M:%S')] $1"
    echo "------------------------------------------------------------------------------"
}

# ==============================================================================
# STEP 1: Pre-Flight Dependency Checks & Clean Slate Reset
# ==============================================================================
log_step "STEP 1/7: Running Pre-Flight Checks & Resetting Environment"

# Verify required CLI tools are installed on the host.
# 'command -v <tool> >/dev/null 2>&1' checks if a binary exists in PATH while
# discarding both standard output (>/dev/null) and standard error (2>&1).
for tool in docker go curl grep; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
        echo "[ERROR] Required tool '${tool}' is not installed." >&2
        exit 1
    fi
done
echo "[OK] Found all required CLI tools (docker, go, curl, grep, ${IAC_CLI})."

# Register the EXIT trap AFTER verifying that Docker and Terraform/OpenTofu
# exist on the host so the teardown handler can safely communicate with Docker.
trap cleanup EXIT

# Ensure we start from a 100% clean slate (no stale QuestDB rows or old containers)
cd "${INFRA_DIR}"
"${IAC_CLI}" init -input=false >/dev/null
"${IAC_CLI}" destroy -var="enable_microservices=true" -auto-approve >/dev/null
echo "[OK] Workspace reset to a clean state."

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 5: Dynamic Terraform Variable Overrides (TF_VAR_<name>)
# ------------------------------------------------------------------------------
# Developer workstations often run background system services like a local
# 'redis-server' bound to 127.0.0.1:6379. If Docker tries to bind Valkey to
# host port 6379 while a host daemon already owns it, container startup fails
# with "address already in use".
# Because we parameterized 'variable "valkey_port"' in 'infra/variables.tf',
# exporting 'TF_VAR_valkey_port=16379' tells Terraform/OpenTofu to automatically
# override that variable for all subsequent 'apply' and 'destroy' commands!
# Meanwhile, containers inside 'gobbler-network' still talk to 'valkey:6379'
# over internal container DNS without any configuration changes.
if ss -tln 2>/dev/null | grep -q ":6379 "; then
    echo "[INFO] Host port 6379 is occupied (e.g., local redis-server)."
    echo "       Exporting TF_VAR_valkey_port=16379 for external host port mapping."
    export TF_VAR_valkey_port=16379
fi

# ==============================================================================
# STEP 2: Build Production Multi-Stage Container Images
# ==============================================================================
log_step "STEP 2/7: Building Multi-Stage Container Images (Go scratch & Python slim)"

cd "${PROJECT_ROOT}"
echo "Building gobbler-gateway:latest (Dockerfile.gateway)..."
docker build -q -t gobbler-gateway:latest -f Dockerfile.gateway . >/dev/null

echo "Building gobbler-persister:latest (Dockerfile.persister)..."
docker build -q -t gobbler-persister:latest -f Dockerfile.persister . >/dev/null

echo "Building gobbler-consumer:latest (Dockerfile.consumer)..."
docker build -q -t gobbler-consumer:latest -f Dockerfile.consumer . >/dev/null

echo "[OK] All microservice images built with latest Phase 5 code."

# ==============================================================================
# STEP 3: Provision Stage 1 — Auxiliary Data Plane (Redpanda, Valkey, QuestDB)
# ==============================================================================
# Why two stages?
# If the Consumer and Persister containers start BEFORE 'market-ticks' and
# 'market-anomalies' exist, Redpanda may auto-create those topics with only 1
# partition instead of 4. Provisioning the data plane first allows us to
# initialize 4-partition topics and QuestDB tables before microservices connect.
log_step "STEP 3/7: Provisioning Auxiliary Data Plane via ${IAC_CLI}"

cd "${INFRA_DIR}"
"${IAC_CLI}" apply -var="enable_microservices=false" -auto-approve >/dev/null
echo "[OK] Containers 'redpanda', 'valkey', and 'questdb' started on 'gobbler-network'."

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 6: Readiness Polling Loops (until <cmd>; do ...; done)
# ------------------------------------------------------------------------------
# Starting a Docker container only means the Linux process has launched—NOT that
# the database or broker inside it is ready to accept network connections.
# Instead of a fragile 'sleep 10', an 'until' loop repeatedly runs a health check
# command until it returns exit code 0 (success), failing safely after a maximum
# number of retries.
echo "Waiting for Valkey (:6379) to respond to PING..."
retries=0
until docker exec valkey valkey-cli ping 2>/dev/null | grep -q "PONG"; do
    retries=$((retries + 1))
    if (( retries > 30 )); then
        echo "[ERROR] Valkey failed to become ready within 30 seconds." >&2
        exit 1
    fi
    sleep 1
done
echo "[OK] Valkey is healthy (PONG)."

echo "Waiting for Redpanda (:9092) cluster health..."
retries=0
until docker exec redpanda rpk cluster info >/dev/null 2>&1; do
    retries=$((retries + 1))
    if (( retries > 30 )); then
        echo "[ERROR] Redpanda failed to become ready within 30 seconds." >&2
        exit 1
    fi
    sleep 1
done
echo "[OK] Redpanda broker is ready."

echo "Waiting for QuestDB REST SQL engine (:9000)..."
retries=0
until curl -sf "http://localhost:9000/exec?query=SELECT+1;" >/dev/null 2>&1; do
    retries=$((retries + 1))
    if (( retries > 30 )); then
        echo "[ERROR] QuestDB failed to become ready within 30 seconds." >&2
        exit 1
    fi
    sleep 1
done
echo "[OK] QuestDB SQL/ILP engine is ready."

# ==============================================================================
# STEP 4: Initialize Kafka Topics & QuestDB Partitioned Tables
# ==============================================================================
log_step "STEP 4/7: Initializing 4-Partition Kafka Topics & QuestDB Schemas"

docker exec redpanda rpk topic create market-ticks --partitions 4 >/dev/null
docker exec redpanda rpk topic create market-anomalies --partitions 4 >/dev/null
echo "[OK] Created Redpanda topics: 'market-ticks' (4P) and 'market-anomalies' (4P)."

curl -sf -G "http://localhost:9000/exec" \
    --data-urlencode "query=CREATE TABLE IF NOT EXISTS market_ticks (ticker SYMBOL CAPACITY 16 CACHE, price DOUBLE, volume LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;" >/dev/null

curl -sf -G "http://localhost:9000/exec" \
    --data-urlencode "query=CREATE TABLE IF NOT EXISTS market_anomalies (ticker SYMBOL CAPACITY 16 CACHE, price DOUBLE, volume LONG, z_score DOUBLE, anomaly_tier SYMBOL CAPACITY 16 CACHE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;" >/dev/null
echo "[OK] Initialized QuestDB tables: 'market_ticks' and 'market_anomalies' (PARTITION BY DAY)."

# ==============================================================================
# STEP 5: Provision Stage 2 — Application Microservices
# ==============================================================================
log_step "STEP 5/7: Deploying Containerized Microservices (Gateway, Consumer, Persister)"

cd "${INFRA_DIR}"
"${IAC_CLI}" apply -var="enable_microservices=true" -auto-approve >/dev/null
echo "[OK] Containers 'gobbler-gateway', 'gobbler-consumer', and 'gobbler-persister' started."

# Wait for the Go Ingestion Gateway's embedded pprof/HTTP port (:6060) to respond,
# which confirms the binary has initialized and is listening for traffic.
echo "Waiting for Ingestion Gateway (:8008 / :6060) to accept connections..."
retries=0
until curl -sf "http://localhost:6060/debug/pprof/" >/dev/null 2>&1; do
    retries=$((retries + 1))
    if (( retries > 30 )); then
        echo "[ERROR] Ingestion Gateway failed to become ready within 30 seconds." >&2
        exit 1
    fi
    sleep 1
done
echo "[OK] Ingestion Gateway is online and listening on 127.0.0.1:8008."

# Give the Python ML Consumer and Go Persister 3 seconds to complete their
# initial Kafka consumer group partition assignment handshake.
sleep 3

# ==============================================================================
# STEP 6: Stream High-Throughput Synthetic Market Order Flow
# ==============================================================================
log_step "STEP 6/7: Streaming High-Throughput Synthetic Order Flow (12 Seconds)"

cd "${PROJECT_ROOT}"

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 7: Handling Expected Non-Zero Exit Codes Under 'set -e'
# ------------------------------------------------------------------------------
# The Linux 'timeout' command runs a process for a fixed duration and then sends
# a signal (here SIGINT) to stop it. When 'timeout' stops the process, it exits
# with status code 124 (or 130 for SIGINT).
# Because 'set -e' is enabled at the top of the script, a raw 'timeout' command
# would cause the entire script to abort!
# Adding '|| true' tells Bash: "Expect this command to be terminated by the
# timer; do not treat its timeout exit code as a script failure."
timeout -s INT 12s go run generator/generator.go || true

echo ""
echo "Waiting 5 seconds for in-flight Kafka batches and Persister 500ms flush timer to drain..."
sleep 5

# ==============================================================================
# STEP 7: Automated Data Integrity & Persistence Assertions
# ==============================================================================
log_step "STEP 7/7: Running End-to-End Persistence & Anomaly Assertions"

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 8: Extracting JSON Values with 'grep -oP'
# ------------------------------------------------------------------------------
# QuestDB's /exec endpoint returns JSON shaped like:
#   {"query":"SELECT count() FROM market_ticks;","columns":[...],"dataset":[[642180]],"count":1}
# Rather than requiring an external JSON parser like 'jq', we use GNU grep with:
#   -o : Print ONLY the matched text (not the entire line).
#   -P : Enable Perl-Compatible Regular Expressions (PCRE).
#   '\[\[\K[0-9]+' : Match the literal '[[' before the number, discard the '[['
#                    using '\K' (keep), and extract the digits '[0-9]+'.
TICKS_JSON="$(curl -sf -G "http://localhost:9000/exec" --data-urlencode "query=SELECT count() FROM market_ticks;")"
TICKS_COUNT="$(echo "${TICKS_JSON}" | grep -oP '\[\[\K[0-9]+' || echo "0")"

ANOMALIES_JSON="$(curl -sf -G "http://localhost:9000/exec" --data-urlencode "query=SELECT count() FROM market_anomalies;")"
ANOMALIES_COUNT="$(echo "${ANOMALIES_JSON}" | grep -oP '\[\[\K[0-9]+' || echo "0")"

echo "  -> Persisted Market Ticks in QuestDB    : ${TICKS_COUNT}"
echo "  -> Persisted Market Anomalies in QuestDB: ${ANOMALIES_COUNT}"

# ------------------------------------------------------------------------------
# BASH LEARNING NOTE 9: Arithmetic Evaluation (( ... ))
# ------------------------------------------------------------------------------
# In Bash, double parentheses '(( ... ))' create an arithmetic context where you
# can use standard C/Go-style math operators (>, <, ==, +) on integer variables
# without needing '$' prefixes or '-gt' flags.
if (( TICKS_COUNT <= 0 )); then
    echo "[FAIL] Assertion failed: Expected market_ticks > 0, got ${TICKS_COUNT}." >&2
    exit 1
fi

if (( ANOMALIES_COUNT <= 0 )); then
    echo "[FAIL] Assertion failed: Expected market_anomalies > 0, got ${ANOMALIES_COUNT}." >&2
    exit 1
fi

echo ""
echo "=============================================================================="
echo "[PASS] END-TO-END VERIFICATION SUCCEEDED!"
echo "       - Ticks Ingested & Persisted    : ${TICKS_COUNT}"
echo "       - Anomalies Detected & Persisted: ${ANOMALIES_COUNT}"
echo "=============================================================================="