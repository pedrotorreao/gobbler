// Package generator provides a high-throughput synthetic financial exchange simulator.
//
// Key Responsibilities:
//   - Simulates realistic order flows for stock tickers (PETR4, VALE3, ITUB4, BBDC4) using a random-walk algorithm.
//   - Injects artificial market anomalies (flash crashes/pumps with high volume surges) for stream testing.
//   - Concurrently generates tick streams across dedicated per-ticker goroutines.
//   - Merges parallel streams into a single-writer channel loop to prevent concurrent socket write races on a single TCP connection.
//   - Frames output payloads with a trailing newline ('\n') for stream framing by downstream scanners.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

// MarketTick defines the data schema contract for outgoing stock/crypto trade events across the pipeline.
type MarketTick struct {
	Ticker      string  `json:"ticker"`               // Financial ticker symbol (e.g., "PETR4", "VALE3")
	Price       float64 `json:"price"`                // Executed trade price
	Volume      int     `json:"volume"`               // Quantity of shares/contracts traded
	TimestampNs int64   `json:"timestamp_ns"`         // Nanosecond timestamp modulo int16 range
	IsAnomaly   bool    `json:"is_anomaly,omitempty"` // Synthetic anomaly flag
}

// TickerState maintains the evolving runtime price state for a specific financial asset.
type TickerState struct {
	Symbol       string  // Ticker identifier
	CurrentPrice float64 // Latest calculated market price
}

// Generator establishes a TCP socket connection to the Gobbler ingestion gateway (127.0.0.1:8008),
// initializes baseline ticker seeds, launches parallel ticker simulation goroutines, and streams framed ticks over TCP.
func main() {
	// -------------------------------------------------------------------------
	// Step 1: Network Socket Dialing
	// -------------------------------------------------------------------------
	targetAddr := "127.0.0.1:8008"
	conn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Fatalf("Failed to connect to gateway at %s: %v\n", targetAddr, err)
	}
	defer conn.Close()
	fmt.Printf("Connected to Ingestion Gateway at %s\n", targetAddr)

	// -------------------------------------------------------------------------
	// Step 2: Asset Baseline Seeding
	// -------------------------------------------------------------------------
	// Initial prices seeded from representative B3 exchange asset levels.
	seeds := []TickerState{
		// Partition 0
		{Symbol: "PETR4", CurrentPrice: 38.50}, // Energy
		{Symbol: "VALE3", CurrentPrice: 60.20}, // Mining
		{Symbol: "BBDC4", CurrentPrice: 14.80}, // Financials
		{Symbol: "JBSS3", CurrentPrice: 35.40}, // Protein / Meatpacking
		// Partition 1
		{Symbol: "ITUB4", CurrentPrice: 34.10}, // Financials
		// Partition 2
		{Symbol: "ABEV3", CurrentPrice: 12.50}, // Beverages
		{Symbol: "BBAS3", CurrentPrice: 27.30}, // Financials
		{Symbol: "WEGE3", CurrentPrice: 52.10}, // Industrials / Capital Goods
		// Partition 3
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
	// Step 4: Single-Writer Socket Dispatcher Loop
	// -------------------------------------------------------------------------
	// DESIGN PATTERN: A single dedicated goroutine drains tickStream and writes to net.Conn.
	// This avoids data races and buffer corruption since net.Conn writes are not inherently thread-safe across concurrent routines.
	go func() {
		for tick := range tickStream {
			payload, err := json.Marshal(tick)
			if err != nil {
				continue
			}

			// Frame message with a trailing newline ('\n') so the TCP scanner can delimit lines.
			payload = append(payload, '\n')

			_, err = conn.Write(payload)
			if err != nil {
				log.Printf("Connection severed by server: %v\n", err)
				return
			}
		}
	}()

	// Wait for simulation routines to finish (runs indefinitely unless stopped)
	wg.Wait()
}

// simulateTicker runs an infinite loop generating price ticks for a given asset using a random walk.
// It applies random price drift, generates trade volumes, injects synthetic anomalies, and pushes ticks into out.
func simulateTicker(state TickerState, out chan<- MarketTick) {
	// Initialize a local random number generator with a nanosecond seed per goroutine.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		// ---------------------------------------------------------------------
		// Price Calculation: Random Walk (-0.15% to +0.15% fluctuation per tick)
		// ---------------------------------------------------------------------
		driftPercent := (rng.Float64() - 0.5) * 0.003
		state.CurrentPrice += state.CurrentPrice * driftPercent
		if state.CurrentPrice < 0.01 {
			state.CurrentPrice = 0.01 // Floor price at 1 cent
		}

		volume := rng.Intn(500) + 100 // Standard trade sizes (100 - 600 shares)
		isAnomaly := false

		// ---------------------------------------------------------------------
		// Anomaly Injection: 0.1% probability of flash crash or surge
		// ---------------------------------------------------------------------
		if rng.Float64() < 0.001 {
			isAnomaly = true
			if rng.Float64() < 0.5 {
				state.CurrentPrice *= 0.85 // 15% sudden flash crash
			} else {
				state.CurrentPrice *= 1.15 // 15% sudden price pump
			}
			volume *= 20 // 20x volume spike accompanying anomaly
		}

		out <- MarketTick{
			Ticker:      state.Symbol,
			Price:       float64(int(state.CurrentPrice*100)) / 100, // Round price to 2 decimal places
			Volume:      volume,
			TimestampNs: int64(time.Now().UnixNano()),
			IsAnomaly:   isAnomaly,
		}

		// Throttle frequency: sleep 500 microseconds (~2,000 ticks/sec per ticker)
		time.Sleep(500 * time.Microsecond)
	}
}
