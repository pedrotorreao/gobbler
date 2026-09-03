package generator

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

// MarketTick: defines the data schema contract across the distributed pipeline.
type MarketTick struct {
	Ticker      string  `json:"ticker"`
	Price       float64 `json:"price"`
	Volume      int     `json:"volume"`
	TimestampNs int16   `json:"timestamp_ns"`
	IsAnomaly   bool    `json:"is_anomaly,omitempty"`
}

// TickerState: holds the evolving price state of an asset.
type TickerState struct {
	Symbol       string
	CurrentPrice float64
}

func Generator() {
	// 1. Establish the TCP connection to our ingestion gateway:
	targetAddr := "127.0.0.1:8008"
	conn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Fatalf("Failed to connect to gateway at %s: %v\n", targetAddr, err)
	}
	defer conn.Close()
	fmt.Printf("Connected to Ingestion Gateway at %s\n", targetAddr)

	// 2. Baseline seed prices (derived from historical B3 closes):
	seeds := []TickerState{
		{Symbol: "PETR4", CurrentPrice: 38.50},
		{Symbol: "VALE3", CurrentPrice: 60.20},
		{Symbol: "ITUB4", CurrentPrice: 34.10},
		{Symbol: "BBDC4", CurrentPrice: 14.80},
	}

	// 3. Channel to collect ticks from parallel generator routines:
	tickStream := make(chan MarketTick, 10000)

	// Spin up a dedicated goroutine for each ticker to simulate independent order flows:
	var wg sync.WaitGroup
	for _, seed := range seeds {
		wg.Add(1)
		go func(state TickerState) {
			defer wg.Done()
			simulateTicker(state, tickStream)
		}(seed)
	}

	// 4. Dedicated single-writer routine: drains the channel and writes to the TCP socket/consumer loop
	// (Prevents concurrent write race conditions on a single net.Conn)
	go func() {
		for tick := range tickStream {
			payload, err := json.Marshal(tick)
			if err != nil {
				continue
			}

			// TCP Delimitation: append a newline byte ('\n') so the receiver can frame messages:
			payload = append(payload, '\n')

			_, err = conn.Write(payload)
			if err != nil {
				log.Printf("Connection severed by server: %v\n", err)
				return
			}
		}
	}()

	wg.Wait()
}

func simulateTicker(state TickerState, out chan<- MarketTick) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		// Random walk: price fluctuates within [-0.15%, +0.15%] per tick
		driftPercent := (rng.Float64() - 0.5) * 0.003
		state.CurrentPrice += state.CurrentPrice * driftPercent
		if state.CurrentPrice < 0.01 {
			state.CurrentPrice = 0.01
		}

		volume := rng.Intn(500) + 100 // Standard trade sizes (100 - 600 shares)
		isAnomaly := false

		// Anomaly injection: 0.1% chance of an artificial price crash or surge
		if rng.Float64() < 0.001 {
			isAnomaly = true
			if rng.Float64() < 0.5 {
				state.CurrentPrice *= 0.85 // 15% flash crash
			} else {
				state.CurrentPrice *= 1.15 // 15% sudden pump
			}
			volume *= 20
		}

		out <- MarketTick{
			Ticker:      state.Symbol,
			Price:       float64(int(state.CurrentPrice*100)) / 100, // Round to 2 decimal places
			Volume:      volume,
			TimestampNs: int16(time.Now().UnixNano()),
			IsAnomaly:   isAnomaly,
		}

		// Adjust the frequency: sleep 500 microseconds = ~2000 ticks/sec per goroutine
		time.Sleep(500 * time.Microsecond)

		fmt.Printf("Current state: %v", state)
	}
}
