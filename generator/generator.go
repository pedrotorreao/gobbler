// Package main provides a high-throughput synthetic financial exchange simulator.
//
// Key Responsibilities:
//   - Simulates realistic order flows for 10 B3 stock tickers (PETR4, VALE3, ITUB4, BBDC4,
//     JBSS3, ABEV3, BBAS3, WEGE3, MGLU3, RENT3) using a geometric random-walk algorithm.
//   - Injects artificial market anomalies (0.1% probability flash crashes/pumps with 20x volume
//     surges) to exercise downstream statistical (Z-Score) and ML (IsolationForest) detectors.
//   - Concurrently generates tick streams across 10 dedicated per-ticker goroutines.
//   - Merges parallel streams into a single-writer channel loop with a 64KB user-space buffer
//     (bufio.Writer aligned to the Linux loopback MTU) to prevent concurrent socket write races
//     and amortize kernel write() syscalls at 56,000+ msgs/sec.
//   - Frames output payloads with a trailing newline ('\n') for stream framing by downstream scanners.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

// MarketTick defines the data schema contract for outgoing stock/crypto trade events across the pipeline.
type MarketTick struct {
	Ticker      string  `json:"ticker"`               // Financial ticker symbol (e.g., "PETR4", "VALE3")
	Price       float64 `json:"price"`                // Executed trade price
	Volume      int     `json:"volume"`               // Quantity of shares/contracts traded
	TimestampNs int64   `json:"timestamp_ns"`         // High-resolution UTC trade timestamp in nanoseconds (used for Valkey SETNX deduplication & QuestDB designated timestamp)
	IsAnomaly   bool    `json:"is_anomaly,omitempty"` // Synthetic ground-truth anomaly flag (omitted from JSON when false to save wire bytes)
}

// TickerState maintains the evolving runtime price state for a specific financial asset.
type TickerState struct {
	Symbol       string  // Ticker identifier
	CurrentPrice float64 // Latest calculated market price
}

// main establishes a TCP socket connection to the Gobbler ingestion gateway (default 127.0.0.1:8008),
// initializes baseline ticker seeds across all 4 Kafka partitions, launches parallel ticker simulation
// goroutines, and streams newline-framed JSON ticks over a buffered TCP writer.
func main() {
	// -------------------------------------------------------------------------
	// Step 1: Network Socket Dialing
	// -------------------------------------------------------------------------
	targetAddr := getEnv("INGESTION_GATEWAY_ADDR", "127.0.0.1:8008")
	conn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Fatalf("Failed to connect to gateway at %s: %v\n", targetAddr, err)
	}
	defer conn.Close()
	fmt.Printf("Connected to Ingestion Gateway at %s\n", targetAddr)

	// -------------------------------------------------------------------------
	// Step 2: Asset Baseline Seeding & Partition Distribution
	// -------------------------------------------------------------------------
	// LEARNING NOTE — Deterministic Partition Mapping (FNV-1a Hashing):
	// The Ingestion Gateway routes ticks to Redpanda's 4 partitions by hashing
	// the ticker symbol with 32-bit FNV-1a (&kafka.Hash{}):
	//   partition = fnv1a_32(ticker) % 4
	// With only 4 tickers, hash collisions can leave a partition completely idle
	// (e.g., PETR4, VALE3, and BBDC4 all hash to Partition 0!). Expanding the
	// universe to 10 B3 equities guarantees every one of the 4 partitions
	// receives active traffic during horizontal consumer scaling tests.
	seeds := []TickerState{
		// Partition 0 (fnv1a % 4 == 0)
		{Symbol: "PETR4", CurrentPrice: 38.50}, // Energy
		{Symbol: "VALE3", CurrentPrice: 60.20}, // Mining
		{Symbol: "BBDC4", CurrentPrice: 14.80}, // Financials
		{Symbol: "JBSS3", CurrentPrice: 35.40}, // Protein / Meatpacking
		// Partition 1 (fnv1a % 4 == 1)
		{Symbol: "ITUB4", CurrentPrice: 34.10}, // Financials
		// Partition 2 (fnv1a % 4 == 2)
		{Symbol: "ABEV3", CurrentPrice: 12.50}, // Beverages
		{Symbol: "BBAS3", CurrentPrice: 27.30}, // Financials
		{Symbol: "WEGE3", CurrentPrice: 52.10}, // Industrials / Capital Goods
		// Partition 3 (fnv1a % 4 == 3)
		{Symbol: "MGLU3", CurrentPrice: 9.80},  // Retail / E-commerce
		{Symbol: "RENT3", CurrentPrice: 44.60}, // Car Rental / Logistics
	}

	// -------------------------------------------------------------------------
	// Step 3: Stream Aggregation & Concurrent Simulation
	// -------------------------------------------------------------------------
	// Buffered channel to collect generated ticks from parallel ticker goroutines.
	tickStream := make(chan MarketTick, 10000)

	var wg sync.WaitGroup
	for _, seed := range seeds {
		wg.Add(1)
		go func(state TickerState) {
			defer wg.Done()
			simulateTicker(state, tickStream)
		}(seed)
	}

	// -------------------------------------------------------------------------
	// Step 4: Single-Writer Socket Dispatcher Loop & Syscall Amortization
	// -------------------------------------------------------------------------
	// LEARNING NOTE — Fan-In Single Writer & User-Space Buffering:
	// 1. Thread Safety: Calling conn.Write() concurrently from 10 goroutines can
	//    interleave partial byte slices under load. Fanning all 10 ticker streams
	//    into 'tickStream' and draining from a single writer goroutine serializes
	//    socket access without mutex contention.
	// 2. Syscall Amortization (64KB Buffer): An unbuffered conn.Write() invokes a
	//    Linux kernel write() syscall on every ~85-byte JSON tick. At 60,000 ticks/sec,
	//    60,000 syscalls/sec + TCP small-packet overhead severely bottlenecks CPU.
	//    Wrapping 'conn' in bufio.NewWriterSize(conn, 64*1024) accumulates ~750 ticks
	//    in user-space RAM and flushes them in a single 64KB write() syscall—matching
	//    the exact 65,536-byte Maximum Transmission Unit (MTU) of the Linux loopback ('lo') interface.
	go func() {
		bufferedWriter := bufio.NewWriterSize(conn, 64*1024) // 64KB matches Linux loopback (lo) MTU
		ticker := time.NewTicker(5 * time.Millisecond)       // Bounds maximum buffering latency to 5ms during quiet periods
		defer ticker.Stop()

		for {
			select {
			case tick, ok := <-tickStream:
				if !ok {
					bufferedWriter.Flush()
					return
				}

				payload, err := json.Marshal(tick)
				if err != nil {
					continue
				}

				// Frame message with a trailing newline ('\n') so the TCP scanner can delimit lines.
				payload = append(payload, '\n')
				bufferedWriter.Write(payload)

				// Proactive flush before the next ~100-byte JSON line overflows the 64KB buffer
				if bufferedWriter.Available() < 1024 {
					bufferedWriter.Flush()
				}

			case <-ticker.C:
				bufferedWriter.Flush()
			}
		}
	}()

	// Wait for simulation routines to finish (runs indefinitely until SIGINT/SIGTERM)
	wg.Wait()
}

// simulateTicker runs an infinite loop generating price ticks for a given asset using a random walk.
// It applies random price drift, generates trade volumes, injects synthetic anomalies, and pushes ticks into out.
func simulateTicker(state TickerState, out chan<- MarketTick) {
	// Initialize a goroutine-local random number generator (avoids global rand mutex contention).
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	const batchSize = 60 // Emits 60 ticks per 10ms burst (6,000 ticks/sec per ticker = 60,000 ticks/sec across 10 tickers)

	for {
		for i := 0; i < batchSize; i++ {
			// ---------------------------------------------------------------------
			// Price Calculation: Geometric Random Walk (-0.15% to +0.15% per tick)
			// ---------------------------------------------------------------------
			driftPercent := (rng.Float64() - 0.5) * 0.003
			state.CurrentPrice += state.CurrentPrice * driftPercent
			if state.CurrentPrice < 0.01 {
				state.CurrentPrice = 0.01 // Floor price at 1 cent ($0.01)
			}

			volume := rng.Intn(500) + 100 // Standard trade lot sizes (100 - 599 shares)
			isAnomaly := false

			// ---------------------------------------------------------------------
			// Anomaly Injection: 0.1% probability (1 in 1,000 ticks) of flash shock
			// ---------------------------------------------------------------------
			if rng.Float64() < 0.001 {
				isAnomaly = true
				if rng.Float64() < 0.5 {
					state.CurrentPrice *= 0.85 // 15% sudden flash crash
				} else {
					state.CurrentPrice *= 1.15 // 15% sudden price pump
				}
				volume *= 20 // 20x volume surge accompanying price shock
			}

			out <- MarketTick{
				Ticker:      state.Symbol,
				Price:       float64(int(state.CurrentPrice*100)) / 100, // Truncate price to 2 decimal places (cents)
				Volume:      volume,
				TimestampNs: int64(time.Now().UnixNano()),
				IsAnomaly:   isAnomaly,
			}
		}

		// LEARNING NOTE — Overcoming OS Timer Slack via Micro-Batching:
		// Sleeping after every single tick (e.g., time.Sleep(166µs)) fails to achieve
		// 6,000 ticks/sec per goroutine because Linux kernel timer slack and Go scheduler
		// wakeups add ~500µs–1ms of overhead per sleep. By emitting a micro-batch of
		// 60 ticks and sleeping 10ms once per burst, we bypass sub-millisecond timer jitter.
		time.Sleep(10 * time.Millisecond)
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
