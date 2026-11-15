// Package main provides a high-throughput time-series persistence worker for the Gobbler platform.
//
// Key Responsibilities:
//   - Concurrently consumes raw trade ticks ('market-ticks') and anomaly events ('market-anomalies') from Redpanda.
//   - Uses distinct consumer groups ('gobbler-ticks-persister-group' and 'gobbler-anomalies-persister-group')
//     to decouple offset tracking and partition balancing between high- and low-frequency streams.
//   - Serializes JSON payloads into InfluxDB Line Protocol (ILP) using zero-allocation byte buffers (strconv.Append*).
//   - Implements a dual-condition batching engine (5,000 messages OR 500ms elapsed time) to maximize QuestDB ingestion throughput.
//   - Streams batched ILP over HTTP POST to QuestDB's '/write' endpoint, reusing persistent TCP connections (HTTP Keep-Alive).
//   - Enforces strict at-least-once persistence semantics: multi-partition Kafka offsets are committed only after QuestDB acknowledges receipt (HTTP 204/200).
//   - Supports graceful shutdown (SIGINT/SIGTERM) via signal.NotifyContext, flushing in-flight buffers and committing remaining offsets under a bounded timeout.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// MarketTick defines the data schema contract for incoming trade events published by the Ingestion Gateway.
type MarketTick struct {
	Ticker      string  `json:"ticker"`       // Financial ticker symbol (e.g., "PETR4", "VALE3")
	Price       float64 `json:"price"`        // Executed trade price
	Volume      int     `json:"volume"`       // Quantity of shares/contracts traded
	TimestampNs int64   `json:"timestamp_ns"` // Trade execution timestamp in UTC nanoseconds
}

// MarketAnomaly defines the data schema contract for confirmed anomaly events emitted by the Python ML detector.
type MarketAnomaly struct {
	Ticker      string  `json:"ticker"`       // Financial ticker symbol (e.g., "PETR4", "VALE3")
	AnomalyTier string  `json:"anomaly_tier"` // Trigger tier: 'STATISTICAL', 'ML', or 'BOTH'
	Price       float64 `json:"price"`        // Executed trade price at the time of anomaly
	Volume      int     `json:"volume"`       // Quantity of shares/contracts traded at the time of anomaly
	ZScore      float64 `json:"z_score"`      // Rolling Z-score metric for univariate price deviation
	MLScore     float64 `json:"ml_score"`     // IsolationForest path-length anomaly score
	TimestampNs int64   `json:"timestamp_ns"` // Anomaly detection timestamp in UTC nanoseconds
}

// ILPParser represents a serialization function that unmarshals a Kafka message payload
// and appends its formatted InfluxDB Line Protocol (ILP) representation to an existing byte buffer.
type ILPParser func(msg kafka.Message, buf []byte) ([]byte, error)

// main initializes decoupled Kafka readers, sets up the shared HTTP transport,
// launches concurrent persistence workers inside a sync.WaitGroup, and coordinates graceful shutdown.
func main() {
	// -------------------------------------------------------------------------
	// Step 1: Initialize Decoupled Kafka Readers
	// -------------------------------------------------------------------------
	// Independent consumer groups ensure high-throughput ticks (~50k msgs/sec)
	// and sparse anomaly alerts (~10 msgs/sec) scale, commit, and rebalance independently.
	ticksReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		Topic:       "market-ticks",
		GroupID:     "gobbler-ticks-persister-group",
		MinBytes:    1000,
		MaxBytes:    10 * 1024 * 1024,
		StartOffset: kafka.FirstOffset, // Replay from beginning if no prior group commit exists
	})
	anomaliesReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		Topic:       "market-anomalies",
		GroupID:     "gobbler-anomalies-persister-group",
		MinBytes:    1000,
		MaxBytes:    10 * 1024 * 1024,
		StartOffset: kafka.FirstOffset,
	})
	defer ticksReader.Close()
	defer anomaliesReader.Close()

	// -------------------------------------------------------------------------
	// Step 2: Configure Shared HTTP Client
	// -------------------------------------------------------------------------
	// Reuses persistent TCP connections (HTTP Keep-Alive) across batch flushes.
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// -------------------------------------------------------------------------
	// Step 3: Lifecycle Coordination & Graceful Teardown
	// -------------------------------------------------------------------------
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		runPersistenceWorker(ctx, ticksReader, parseMarketTickFromKafkaMsgToILP, client)
	}()
	go func() {
		defer wg.Done()
		runPersistenceWorker(ctx, anomaliesReader, parseMarketAnomalyFromKafkaMsgToILP, client)
	}()

	fmt.Println("Gobbler Persistence Service active. Subscribed to 'market-ticks' and 'market-anomalies'...")
	wg.Wait()
	fmt.Println("All persistence workers drained. Service stopped cleanly.")
}

// parseMarketTickFromKafkaMsgToILP serializes a MarketTick into QuestDB InfluxDB Line Protocol (ILP):
//
//	market_ticks,ticker=<TICKER> price=<PRICE>,volume=<VOLUME>i <TIMESTAMP_NS>\n
//
// LEARNING NOTE — ILP Wire Grammar & Zero-Allocation strconv.Append*:
// 1. ILP Grammar Rules:
//   - Measurement & Tags ('market_ticks,ticker=PETR4'): Comma-separated, NO spaces.
//     Tags map to QuestDB's indexed SYMBOL column type (interned strings for O(1) filtering).
//   - First Space (' '): Delimits the Tag Set from the Field Set.
//   - Fields ('price=38.5,volume=400i'): Unindexed numeric columns. The trailing 'i' on
//     volume tells QuestDB to store a 64-bit integer (LONG) instead of a 64-bit float (DOUBLE).
//   - Second Space (' '): Delimits the Field Set from the Designated Timestamp (UTC nanoseconds).
//
// 2. Why strconv.Append* Instead of fmt.Sprintf?
//   - Calling fmt.Sprintf("market_ticks,ticker=%s price=%f...", ...) uses reflection, boxes
//     arguments into interfaces, and allocates a brand-new string on the heap on every tick.
//   - strconv.AppendFloat and strconv.AppendInt write ASCII digits directly into our pre-allocated
//     'tickDataBuf' byte slice with ZERO intermediate string allocations.
func parseMarketTickFromKafkaMsgToILP(msg kafka.Message, tickDataBuf []byte) ([]byte, error) {
	var tick MarketTick

	err := json.Unmarshal(msg.Value, &tick)
	if err != nil {
		return tickDataBuf, err
	}

	// 1. Measurement and tags (SYMBOL columns)
	tickDataBuf = append(tickDataBuf, "market_ticks,ticker="...)
	tickDataBuf = append(tickDataBuf, tick.Ticker...)

	// 2. Fields (space separator transitions from tags to fields)
	tickDataBuf = append(tickDataBuf, " price="...)
	tickDataBuf = strconv.AppendFloat(tickDataBuf, tick.Price, 'f', -1, 64)

	tickDataBuf = append(tickDataBuf, ",volume="...)
	tickDataBuf = strconv.AppendInt(tickDataBuf, int64(tick.Volume), 10)
	tickDataBuf = append(tickDataBuf, 'i', ' ') // 'i' indicates 64-bit integer in ILP; trailing space precedes timestamp

	// 3. Designated timestamp (UTC nanoseconds) & line-framing newline
	tickDataBuf = strconv.AppendInt(tickDataBuf, tick.TimestampNs, 10)
	tickDataBuf = append(tickDataBuf, '\n')

	return tickDataBuf, nil
}

// parseMarketAnomalyFromKafkaMsgToILP serializes a MarketAnomaly into QuestDB InfluxDB Line Protocol:
//
//	market_anomalies,ticker=<TICKER>,anomaly_tier=<TIER> price=<PRICE>,volume=<VOLUME>i,z_score=<Z>,ml_score=<ML> <TIMESTAMP_NS>\n
//
// Tags: ticker (SYMBOL), anomaly_tier (SYMBOL)
// Fields: price (DOUBLE), volume (LONG - 'i' suffix), z_score (DOUBLE), ml_score (DOUBLE)
// Timestamp: UTC Nanoseconds
func parseMarketAnomalyFromKafkaMsgToILP(msg kafka.Message, anomDataBuf []byte) ([]byte, error) {
	var anom MarketAnomaly

	err := json.Unmarshal(msg.Value, &anom)
	if err != nil {
		return anomDataBuf, err
	}

	// 1. Measurement and tags
	anomDataBuf = append(anomDataBuf, "market_anomalies,ticker="...)
	anomDataBuf = append(anomDataBuf, anom.Ticker...)
	anomDataBuf = append(anomDataBuf, ",anomaly_tier="...)
	anomDataBuf = append(anomDataBuf, anom.AnomalyTier...)

	// 2. Fields (space separator transitions from tags to fields)
	anomDataBuf = append(anomDataBuf, " price="...)
	anomDataBuf = strconv.AppendFloat(anomDataBuf, anom.Price, 'f', -1, 64)

	anomDataBuf = append(anomDataBuf, ",volume="...)
	anomDataBuf = strconv.AppendInt(anomDataBuf, int64(anom.Volume), 10)
	anomDataBuf = append(anomDataBuf, 'i')

	anomDataBuf = append(anomDataBuf, ",z_score="...)
	anomDataBuf = strconv.AppendFloat(anomDataBuf, anom.ZScore, 'f', -1, 64)

	anomDataBuf = append(anomDataBuf, ",ml_score="...)
	anomDataBuf = strconv.AppendFloat(anomDataBuf, anom.MLScore, 'f', -1, 64)
	anomDataBuf = append(anomDataBuf, ' ') // Trailing space precedes timestamp

	// 3. Designated timestamp & newline
	anomDataBuf = strconv.AppendInt(anomDataBuf, anom.TimestampNs, 10)
	anomDataBuf = append(anomDataBuf, '\n')

	return anomDataBuf, nil
}

// flushToQuestDB executes an HTTP POST request transmitting the assembled ILP byte buffer
// to QuestDB's '/write' endpoint. It verifies receipt via HTTP 204 No Content (or 200 OK)
// and drains the response body to allow TCP connection pooling.
func flushToQuestDB(ctx context.Context, client *http.Client, payload []byte) error {
	var questDBUrl = getEnv("QUESTDB_URL", "http://localhost:9000/write")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, questDBUrl, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("questdb write failed (%d): %s", resp.StatusCode, string(body))
	}
	io.Copy(io.Discard, resp.Body)

	return nil
}

// runPersistenceWorker implements the core streaming ingestion loop:
//   - Continuously fetches messages from Redpanda.
//   - Accumulates ILP formatted bytes in reusable slices.
//   - Evaluates the dual-condition batch trigger: count >= 5,000 OR elapsed >= 500ms.
//   - Flushes accumulated ILP to QuestDB and commits offsets across all batch partitions upon success.
//   - Handles graceful shutdown: drains in-flight payloads using a bounded timeout context.
//
// LEARNING NOTE — Preventing the "Phantom Offset Skip" Distributed Systems Trap:
// In Kafka/Redpanda, a consumer group tracks progress via a single high-water-mark integer
// offset per partition (not per-message ACK receipts). Meanwhile, 'reader.FetchMessage(ctx)'
// advances an internal in-memory cursor independently of 'reader.CommitMessages(...)'.
//   - The Trap: If Batch 1 (offsets 0..4999) fails to write to QuestDB and the worker
//     logs the error, skips CommitMessages, truncates 'batchMessages[:0]', and loops back
//     to FetchMessage, the next calls fetch Batch 2 (offsets 5000..9999). If QuestDB recovers
//     and Batch 2 succeeds, 'CommitMessages' commits offset 10,000 to Kafka—permanently
//     jumping over Batch 1 (0..4999) and causing silent data loss!
//   - The Fix: Blocking inside 'flushWithRetry' retains 'batchMessages' in RAM, halts
//     subsequent 'FetchMessage' calls (applying natural backpressure to Kafka), and only
//     truncates buffers or commits offsets after QuestDB acknowledges receipt (HTTP 204/200).
func runPersistenceWorker(ctx context.Context, reader *kafka.Reader, parser ILPParser, client *http.Client) {
	const flushInterval = 500 * time.Millisecond
	const batchSize = 5000

	batchKafkaMessages := make([]kafka.Message, 0, batchSize)
	batchMessages := make([]byte, 0, batchSize*80) // Pre-allocate estimated buffer capacity (~400KB)
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// -----------------------------------------------------------------
			// Shutdown Phase: Drain In-Flight Messages
			// -----------------------------------------------------------------
			fmt.Println("Context cancelled. Flushing remaining in-flight messages...")
			if len(batchMessages) > 0 {
				shtdwnCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				// Only commit offsets if the final shutdown flush to QuestDB succeeded;
				// otherwise leave offsets uncommitted so Redpanda replays the batch on restart.
				if err := flushWithRetry(shtdwnCtx, client, batchMessages); err != nil {
					fmt.Printf("Error flushing remaining messages on shutdown: %v\n", err)
				} else {
					if err := reader.CommitMessages(shtdwnCtx, batchKafkaMessages...); err != nil {
						fmt.Printf("Error committing messages on shutdown: %v\n", err)
					}
				}

				batchKafkaMessages = batchKafkaMessages[:0]
				batchMessages = batchMessages[:0]
			}
			return

		default:
			// Non-blocking fallthrough to message fetch
		}

		// ---------------------------------------------------------------------
		// Fetch Message from Kafka
		// ---------------------------------------------------------------------
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				continue
			}
			fmt.Printf("Error fetching message from Kafka: %v\n", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// ---------------------------------------------------------------------
		// Parse & Serialize to ILP
		// ---------------------------------------------------------------------
		var parseErr error
		batchMessages, parseErr = parser(msg, batchMessages)
		if parseErr != nil {
			fmt.Printf("Error parsing message payload: %v\n", parseErr)
			// Commit corrupted message (poison pill) so consumer is not blocked indefinitely
			if err := reader.CommitMessages(ctx, msg); err != nil {
				fmt.Printf("Error committing unparseable message: %v\n", err)
			}
			continue
		}
		batchKafkaMessages = append(batchKafkaMessages, msg)

		// ---------------------------------------------------------------------
		// Evaluate Dual-Trigger Batch Flush
		// ---------------------------------------------------------------------
		if len(batchKafkaMessages) > 0 && (len(batchKafkaMessages) >= batchSize || time.Since(startTime) >= flushInterval) {
			if err := flushWithRetry(ctx, client, batchMessages); err != nil {
				// Shutdown signal received during outage: abort without committing so Kafka redelivers on restart
				return
			}
			// Commit offsets across all partitions represented in this batch
			if err := reader.CommitMessages(ctx, batchKafkaMessages...); err != nil {
				log.Printf("[PERSISTER ERROR] Error committing batch offsets to Kafka: %v\n", err)
			}

			// Reset slice lengths to 0 while preserving underlying allocated array capacity
			batchKafkaMessages = batchKafkaMessages[:0]
			batchMessages = batchMessages[:0]
			startTime = time.Now()
		}
	}
}

// flushWithRetry attempts to write the ILP payload to QuestDB, retrying with exponential
// backoff (50ms -> 100ms -> 200ms -> 400ms -> 800ms -> 1s ceiling) until success or context cancellation.
//
// LEARNING NOTE — Why time.NewTimer + select Instead of time.Sleep?
// A raw 'time.Sleep(backoff)' cannot be interrupted by a context cancellation signal
// (SIGINT / SIGTERM), forcing a shutting-down container to hang until the sleep finishes.
// Using 'time.NewTimer(backoff)' inside a 'select' statement lets the goroutine wake up
// instantaneously if '<-ctx.Done()' fires, while 'timer.Stop()' releases the timer resource immediately.
func flushWithRetry(ctx context.Context, client *http.Client, payload []byte) error {
	backoff := 50 * time.Millisecond
	const maxBackoff = 1 * time.Second

	for {
		err := flushToQuestDB(ctx, client, payload)
		if err == nil {
			return nil // Successfully written and acknowledged (HTTP 200/204)
		}

		log.Printf("[PERSISTER WARNING] QuestDB write failed: %v. Retrying in %v...", err, backoff)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// Backoff timer elapsed; proceed to next attempt.
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
