"""Gobbler Real-Time Market Surveillance Consumer & Idempotent Analytical Worker.

This module consumes high-frequency financial market ticks from the Redpanda
(Kafka-compatible) event stream, enforces strictly idempotent stream processing
using a distributed Valkey (Redis-compatible) cache, and coordinates partition
assignments across horizontally scalable consumer groups.

Architectural Guarantees:
1. At-Least-Once Delivery & Manual Commits:
   By disabling 'enable.auto.commit', the worker explicitly commits offsets only
   after a message is verified by the deduplication layer and processed.
2. Distributed Idempotency (Effectively-Once Semantics):
   Uses atomic Valkey 'SET tick:<ticker>:<timestamp_ns> seen EX 60 NX' operations.
   Redelivered messages (from network retries or rebalances) are dropped before
   reaching the analytical and ML inference layers.
3. Consumer Group Parallelism:
   Workers sharing the 'gobbler-ml-consumers' group ID automatically divide the
   4 partitions of 'market-ticks' for concurrent stream processing.
4. Real-Time Stream Inference (Tier 1):
   Maintains in-memory rolling statistical baselines per ticker using
   RollingZScoreDetector, evaluating incoming trade prices against a Dual-Condition
   Guard (|Z| > 3.0 and price move >= 2.0%) with sub-millisecond latency.
"""

import json
import signal
import sys
import redis
from confluent_kafka import Consumer, KafkaError, KafkaException
try:
    from detector import RollingZScoreDetector
except ImportError:
    from consumer.detector import RollingZScoreDetector

# Global execution flag toggled by OS interrupt signals for graceful teardown
running = True

# Initialize a thread-safe connection pool to the distributed Valkey cache.
# decode_responses=True ensures string return values instead of raw byte strings.
valkey_client = redis.Redis(host='localhost', port=6379, decode_responses=True)


def signal_handler(sig, frame):
    """Intercepts OS signals (SIGINT/SIGTERM) to coordinate a graceful shutdown."""
    global running
    print("\n[SHUTDOWN] Signal received. Initiating graceful consumer teardown...")
    running = False


# Register signal handlers for clean container and terminal exits
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def run_consumer():
    """Main event loop for the streaming analytical worker.

    Connects to Redpanda, subscribes to the 'market-ticks' topic, executes
    the poll-deduplicate-infer-commit cycle, and sends an explicit LeaveGroup
    request upon termination to trigger an immediate partition rebalance.
    """
    conf = {
        # Network address of the Redpanda broker
        'bootstrap.servers': 'localhost:9092',
        # Shared consumer group ID enabling horizontal scaling across worker processes
        'group.id': 'gobbler-ml-consumers',
        # Read from earliest available offset if no previous committed offset exists
        'auto.offset.reset': 'earliest',
        # Disable automatic commits to enforce strict manual offset management
        'enable.auto.commit': False,
    }

    consumer = Consumer(conf)
    consumer.subscribe(['market-ticks'])

    print("[STARTUP] Consumer subscribed to 'market-ticks' topic.")
    print("[STARTUP] Awaiting streaming financial ticks from Redpanda...")
    # Initialized once at startup so circular buffers persist across iterations:
    detector = RollingZScoreDetector(
        window_size=100,
        z_threshold=3.0,
        min_observations=20,
        min_pct_change=0.02,
    )

    try:
        while running:
            # ------------------------------------------------------------------
            # Step 1: Poll Redpanda Event Stream
            # ------------------------------------------------------------------
            # Block up to 1.0 second awaiting incoming batch messages from broker
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Polling deadline exceeded with no messages available; loop and wait
                continue

            if msg.error():
                # Handle non-fatal partition boundary events vs fatal network/broker failures
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition log reached; continue polling
                    continue
                else:
                    raise KafkaException(msg.error())

            # ------------------------------------------------------------------
            # Step 2: Payload Deserialization
            # ------------------------------------------------------------------
            tick_data = json.loads(msg.value().decode('utf-8'))
            ticker = tick_data.get('ticker')
            price = tick_data.get('price')
            volume = int(tick_data.get('volume') or 0)
            timestamp = tick_data.get('timestamp_ns')

            # ------------------------------------------------------------------
            # Step 3: Distributed Idempotency via Valkey (SETNX + TTL)
            # ------------------------------------------------------------------
            # Construct a deterministic composite key: asset ticker + nanosecond timestamp
            dedup_key = f"tick:{ticker}:{timestamp}"

            # Atomically set the key ONLY if it does not already exist (NX)
            # with a 60-second time-to-live (EX 60) to prevent unbounded memory growth.
            is_new = valkey_client.set(dedup_key, "seen", ex=60, nx=True)

            if not is_new:
                # Duplicate message detected (caused by producer retry or consumer rebalance)
                # 1. Skip expensive analytical and ML inference
                # 2. Acknowledge offset to Kafka so the broker advances beyond the duplicate
                print(f"[DEDUP] Duplicate dropped: {dedup_key} (Partition {msg.partition()} @ Offset {msg.offset()})")
                consumer.commit(message=msg, asynchronous=False)
                continue

            # ------------------------------------------------------------------
            # Step 4: Stream Inference & Anomaly Detection (Phase 3, Step 4)
            # ------------------------------------------------------------------
            is_anomaly, z_score, ml_score = detector.evaluate(ticker, price, volume)
            if is_anomaly:
                print(f"[ANOMALY ALERT] {ticker} @ ${price:.2f} (Vol: {volume}, Z-Score: {z_score:+.2f}, ML-Score: {ml_score:.3f})")
            else:
                print(f"Received: Ticker={ticker}, Price={price}, Timestamp={timestamp}")

            # ------------------------------------------------------------------
            # Step 5: Explicit Manual Offset Commit
            # ------------------------------------------------------------------
            # Persist offset commit to Redpanda's internal '__consumer_offsets' topic
            consumer.commit(message=msg, asynchronous=False)

    finally:
        # ----------------------------------------------------------------------
        # Step 6: Graceful LeaveGroup & Partition Revocation
        # ----------------------------------------------------------------------
        # Crucial: Sends an immediate LeaveGroup request to the Redpanda coordinator.
        # This triggers instantaneous partition rebalancing to surviving workers
        # without waiting for session.timeout.ms (~45s) to expire.
        consumer.close()
        print("[SHUTDOWN] Consumer cleanly closed and removed from consumer group.")


if __name__ == "__main__":
    run_consumer()
