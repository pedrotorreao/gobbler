package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// MarketTick mirrors the schema contract emitted by the generator
type MarketTick struct {
	Ticker      string  `json:"ticker"`
	Price       float64 `json:"price"`
	Volume      int     `json:"volume"`
	TimestampNs int16   `json:"timestamp_ns"`
	IsAnomaly   bool    `json:"is_anomaly,omitempty"`
}

// Global atomic counter to track throughput across all workers
var totalProcessed uint64

func main() {
	// 1. Allocate a bounded buffered channel for raw byte payloads:
	const queueCapacity = 50000
	jobQueue := make(chan []byte, queueCapacity)

	// 2. Spawn the Bounded Worker Pool
	// Scale worker routines to available logical CPU cores for CPU-bounded tasks (JSON unmarshaling):
	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup

	fmt.Printf("Initializing %d processing workers...\n", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobQueue, &wg)
	}

	// 3. Background metrics reporter: prints throughput every second
	go reportThroughput()

	// 4. Start the TCP Listener:
	serverIPAddr := "127.0.0.1"
	serverPort := "8008"

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", serverIPAddr, serverPort))
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", serverPort, err)
	}
	defer listener.Close()

	fmt.Printf("Ingestion Gateway online at %s. Waiting for connections...", fmt.Sprintf("%s:%s", serverIPAddr, serverPort))

	// 5. Connection Acceptance Loop:
	for {
		// Accept blocks execution until a client connects:
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		fmt.Printf("Client connected from: %s\n", conn.RemoteAddr().String())

		// 3. Read data from the client
		go handleConnection(conn, jobQueue)
	}
}

func handleConnection(conn net.Conn, jobQueue chan<- []byte) {
	defer conn.Close() // Automatically close the socket when this function ends

	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("[+] New connection established: %s\n", remoteAddr)

	// bufio.Scanner breaks incoming byte stream on newline ('\n') boudaries:
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		rawBytes := scanner.Bytes()

		// CRITICAL MEMORY GOTCHA:
		// scanner.Bytes() returns a slice pointing to an internal buffer that gets overwritten
		// on the next scan. We MUST allocate a clean copy before sendind across the channel.
		payloadCopy := make([]byte, len(rawBytes))
		copy(payloadCopy, rawBytes)

		// Push to worker pool. If the buffer fills up, this line blocks,
		// naturally applying backpressure to the TCP client socket.
		jobQueue <- payloadCopy
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error on %s: %v", remoteAddr, err)
	}
	fmt.Printf("[-] Connection closed: %s\n", remoteAddr)
}

func worker(id int, jobs <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	for payload := range jobs {
		var tick MarketTick
		if err := json.Unmarshal(payload, &tick); err != nil {
			continue // In production, route corrupted payloads to a Dead Letter Queue (DLQ)
		}

		// Update global atomic counter:
		atomic.AddUint64(&totalProcessed, 1)

		// In Phase 2, this worker will push parsed tick into Kafka/Redpanda.
		// For now, let's log if an anomaly flag was received from our generator:
		if tick.IsAnomaly {
			fmt.Printf("[WORKER %d ALERT] Injected Anomaly Caught: %s @ $%.2f (Vol: %d)\n", id, tick.Ticker, tick.Price, tick.Volume)
		}
	}
}

func reportThroughput() {
	var lastCount uint64
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		current := atomic.LoadUint64(&totalProcessed)
		delta := current - lastCount
		lastCount = current

		if delta > 0 {
			fmt.Printf("==> Throughput: %d msgs/sec | Cumulative: %d msgs\n", delta, current)
		}
	}
}
