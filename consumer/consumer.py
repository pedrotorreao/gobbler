"""Gobbler Real-Time Market Surveillance Consumer & Idempotent Analytical Worker.

This module consumes high-frequency financial market ticks from the Redpanda
(Kafka-compatible) event stream, enforces strictly idempotent stream processing
using a distributed Valkey (Redis-compatible) cache, and coordinates partition
assignments across horizontally scalable consumer groups.

Architectural Guarantees:
1. At-Least-Once Delivery & Manual Commits:
   By disabling 'enable.auto.commit', the worker explicitly commits offsets only
   after a message is verified by the deduplication layer and processed.
   This prevents tick loss on crashes while bounding re-processing to the
   deduplication window (60 seconds).
2. Distributed Idempotency (Effectively-Once Semantics):
   Uses atomic Valkey 'SET tick:<ticker>:<timestamp_ns> seen EX 60 NX' operations.
   Redelivered messages (from network retries or rebalances) are dropped before
   reaching the analytical and ML inference layers.
3. Consumer Group Parallelism:
   Workers sharing the 'gobbler-ml-consumers' group ID automatically divide the
   4 partitions of 'market-ticks' for concurrent stream processing.
4. Two-Tier Real-Time Stream Inference:
   Delegates each deduplicated tick to RollingZScoreDetector.evaluate(), which
   runs a Dual-Condition Z-Score guard (Tier 1, sub-0.1ms) followed by an
   independent IsolationForest score check on normalized [z_price, z_volume]
   features (Tier 2, sub-2ms). Either tier can independently trigger an alert.
5. Anomaly Event Emission:
   Confirmed anomalies are published to the 'market-anomalies' Redpanda topic
   as JSON events keyed by asset ticker (preserving per-ticker FIFO ordering).
   The payload includes: ticker, price, volume, timestamp_ns, z_score, ml_score,
   and anomaly_tier (the string name of the AnomalyTierIndicator enum member:
   'STATISTICAL', 'ML', or 'BOTH').
"""

import json
import signal
import sys
import redis
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
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

    Connects to Redpanda, subscribes to the 'market-ticks' topic, and executes
    the following cycle on each message:
        1. Poll:        Block up to 1s for the next batch from the broker.
        2. Deserialize: Decode the JSON tick payload (ticker, price, volume, timestamp_ns).
        3. Deduplicate: Atomic Valkey SETNX check — drop duplicates before inference.
        4. Infer:       Evaluate the tick against the two-tier anomaly detector.
        5. Emit:        On confirmed anomaly, produce a structured JSON event to
                        the 'market-anomalies' topic keyed by ticker.
        6. Commit:      Explicit synchronous offset commit to '__consumer_offsets'.

    On SIGINT/SIGTERM, the loop exits cleanly: the consumer sends an immediate
    LeaveGroup request (triggering instantaneous partition rebalancing to surviving
    workers), and the producer flushes all buffered anomaly events before exit.
    """
    conf_consumer = {
        # Network address of the Redpanda broker
        'bootstrap.servers': 'localhost:9092',
        # Shared consumer group ID enabling horizontal scaling across worker processes
        'group.id': 'gobbler-ml-consumers',
        # Read from earliest available offset if no previous committed offset exists
        'auto.offset.reset': 'earliest',
        # Disable automatic commits to enforce strict manual offset management
        'enable.auto.commit': False,
    }

    conf_producer = {
        # Network address of the Redpanda broker
        'bootstrap.servers': 'localhost:9092',
        # acks='1': Leader acknowledges the write before responding.
        # Balances durability (vs acks='0' fire-and-forget) with throughput
        # (vs acks='all' which waits for all ISR replicas to confirm).
        'acks': '1',
        # Retry up to 3 times on transient broker errors (librdkafka config name).
        'message.send.max.retries': 3,
    }

    consumer = Consumer(conf_consumer)
    consumer.subscribe(['market-ticks'])

    producer = Producer(conf_producer)

    print("[STARTUP] Consumer subscribed to 'market-ticks' topic.")
    print("[STARTUP] Awaiting streaming financial ticks from Redpanda...")
    # Initialized once at startup so circular buffers persist across iterations:
    detector = RollingZScoreDetector(
        window_size=100,
        z_threshold=3.0,
        min_observations=20,
        min_pct_change=0.02,
        ml_score_delay=500
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
            # Step 4: Stream Inference & Anomaly Detection
            # ------------------------------------------------------------------
            is_anomaly, z_score, ml_score, anomaly_tier = detector.evaluate(ticker, price, volume)
            if is_anomaly:
                print(f"[ANOMALY ALERT] {ticker} @ ${price:.2f} (Vol: {volume}, Z-Score: {z_score:+.2f}, ML-Score: {ml_score:.3f})")
                # ------------------------------------------------------------------
                # Step 5: Anomaly Event Emission
                # ------------------------------------------------------------------
                # Serialize the confirmed anomaly into a structured JSON event and
                # publish it to the 'market-anomalies' topic.
                # Key = ticker: ensures all events for the same asset land on the
                # same partition, preserving per-ticker FIFO ordering for downstream
                # consumers (audit logs, QuestDB persistence, alerting dashboards).
                # anomaly_tier.name: serializes the IntEnum as a human-readable
                # string ('STATISTICAL', 'ML', 'BOTH') rather than a numeric value.
                anomaly_payload = json.dumps({
                    'ticker': ticker,
                    'price': price,
                    'volume': volume,
                    'timestamp_ns': timestamp,
                    'z_score': z_score,
                    'ml_score': ml_score,
                    'anomaly_tier': anomaly_tier.name
                })
                producer.produce('market-anomalies', anomaly_payload.encode('utf-8'), ticker)
                # poll(0): Non-blocking drain of librdkafka's internal delivery report
                # callback queue. Prevents the internal buffer from filling during a
                # sustained anomaly burst without introducing blocking latency.
                producer.poll(0)
            else:
                print(f"Received: Ticker={ticker}, Price={price}, Timestamp={timestamp}")

            # ------------------------------------------------------------------
            # Step 5: Explicit Manual Offset Commit
            # ------------------------------------------------------------------
            # Persist offset commit to Redpanda's internal '__consumer_offsets' topic
            consumer.commit(message=msg, asynchronous=False)

    finally:
        # ----------------------------------------------------------------------
        # Step 7: Graceful Shutdown — LeaveGroup, Producer Flush & Partition Revocation
        # ----------------------------------------------------------------------
        # consumer.close(): Sends an immediate LeaveGroup request to the Redpanda
        # coordinator, triggering instantaneous partition rebalancing to surviving
        # workers without waiting for session.timeout.ms (~45s) to expire.
        consumer.close()
        # producer.flush(): Blocks until all internally buffered anomaly events are
        # delivered to the broker (or their delivery timeout expires). Ensures no
        # confirmed anomaly events are silently dropped on shutdown.
        producer.flush()
        print("[SHUTDOWN] Consumer cleanly closed and producer flushed.")


if __name__ == "__main__":
    run_consumer()
