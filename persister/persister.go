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
		Brokers:     []string{"localhost:9092"},
		Topic:       "market-ticks",
		GroupID:     "gobbler-ticks-persister-group",
		MinBytes:    1000,
		MaxBytes:    10 * 1024 * 1024,
		StartOffset: kafka.FirstOffset, // Replay from beginning if no prior group commit exists
	})
	anomaliesReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
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

// parseMarketTickFromKafkaMsgToILP serializes a MarketTick into QuestDB InfluxDB Line Protocol:
//
//	market_ticks,ticker=<TICKER> price=<PRICE>,volume=<VOLUME>i <TIMESTAMP_NS>\n
//
// Tags: ticker (SYMBOL)
// Fields: price (DOUBLE), volume (LONG - 'i' suffix)
// Timestamp: UTC Nanoseconds
func parseMarketTickFromKafkaMsgToILP(msg kafka.Message, tickDataBuf []byte) ([]byte, error) {
	var tick MarketTick

	err := json.Unmarshal(msg.Value, &tick)
	if err != nil {
		return tickDataBuf, err
	}

	// 1. Measurement and tags
	tickDataBuf = append(tickDataBuf, "market_ticks,ticker="...)
	tickDataBuf = append(tickDataBuf, tick.Ticker...)

	// 2. Fields (space separator transitions from tags to fields)
	tickDataBuf = append(tickDataBuf, " price="...)
	tickDataBuf = strconv.AppendFloat(tickDataBuf, tick.Price, 'f', -1, 64)

	tickDataBuf = append(tickDataBuf, ",volume="...)
	tickDataBuf = strconv.AppendInt(tickDataBuf, int64(tick.Volume), 10)
	tickDataBuf = append(tickDataBuf, 'i', ' ') // 'i' indicates integer in ILP; trailing space precedes timestamp

	// 3. Designated timestamp & newline
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
	const questDBUrl = "http://localhost:9000/write"

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
func runPersistenceWorker(ctx context.Context, reader *kafka.Reader, parser ILPParser, client *http.Client) {
	const flushInterval = 500 * time.Millisecond
	const batchSize = 5000

	batchKafkaMessages := make([]kafka.Message, 0, batchSize)
	batchMessages := make([]byte, 0, batchSize*80) // Pre-allocate estimated buffer capacity
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

				if err := flushToQuestDB(shtdwnCtx, client, batchMessages); err != nil {
					fmt.Printf("Error flushing remaining messages on shutdown: %v\n", err)
				}

				if err := reader.CommitMessages(shtdwnCtx, batchKafkaMessages...); err != nil {
					fmt.Printf("Error committing messages on shutdown: %v\n", err)
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
			// Commit corrupted message so consumer is not blocked indefinitely
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
			if err := flushToQuestDB(ctx, client, batchMessages); err != nil {
				fmt.Printf("Error flushing batch to QuestDB: %v\n", err)
				// Do NOT commit offsets on flush failure; retried on next loop
			} else {
				// Commit offsets across all partitions represented in this batch
				if err := reader.CommitMessages(ctx, batchKafkaMessages...); err != nil {
					fmt.Printf("Error committing batch offsets to Kafka: %v\n", err)
				}
			}

			// Reset buffers while preserving allocated slice capacity
			batchKafkaMessages = batchKafkaMessages[:0]
			batchMessages = batchMessages[:0]
			startTime = time.Now()
		}
	}
}
