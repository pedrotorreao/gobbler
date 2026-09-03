package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
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
	// Trap OS termination signals using signal.NotifyContext
	// Catches Ctrl+C (SIGINT) and Kubernetes termination signals (SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // Ensure the context is cancelled when the function exits

	// 1. Allocate a bounded buffered channel for raw byte payloads:
	const queueCapacity = 50000
	jobQueue := make(chan []byte, queueCapacity)

	// 2. Spawn the Bounded Worker Pool
	// Scale worker routines to available logical CPU cores for CPU-bounded tasks (JSON unmarshaling):
	numWorkers := runtime.NumCPU()
	var workerWg sync.WaitGroup

	fmt.Printf("Initializing %d processing workers...\n", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		workerWg.Add(1)
		go worker(i, jobQueue, &workerWg)
	}

	// 3. Background metrics reporter: prints throughput every second
	go reportThroughput(ctx)

	// 4. Start the TCP Listener:
	serverIPAddr := "127.0.0.1"
	serverPort := "8008"

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", serverIPAddr, serverPort))
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", serverPort, err)
	}
	defer listener.Close()

	fmt.Printf("Ingestion Gateway online at %s. Waiting for connections...", fmt.Sprintf("%s:%s", serverIPAddr, serverPort))

	// Synchronization barriers for active connections:
	var connWg sync.WaitGroup
	var activeConns sync.Map // Thread-safe map to track active net.Conn references

	// 5. Connection Acceptance Loop in its own goorutine so main can block on shutdown signals:
	go func() {
		for {
			// Accept blocks execution until a client connects:
			conn, err := listener.Accept()
			if err != nil {
				// When listener.Close() is called during shutdown, Accept returns net.ErrClosed
				if errors.Is(err, net.ErrClosed) {
					return // Normal shutdown exit
				}
				log.Printf("Failed to accept connection: %v", err)
				continue
			}

			activeConns.Store(conn, struct{}{}) // Store empty struct as value to save memory
			connWg.Add(1)

			go func(c net.Conn) {
				defer connWg.Done()
				defer activeConns.Delete(c)
				defer c.Close()

				fmt.Printf("Client connected from: %s\n", conn.RemoteAddr().String())

				// Read data from the client
				handleConnection(c, jobQueue)
			}(conn)
		}
	}()

	// 6. BLOCK main until an OS signal arrives:
	<-ctx.Done()
	fmt.Printf("\n[!] Shutdown signal received. Starting graceful drain...\n")

	// Step A: Stop accepting new connections
	listener.Close()
	fmt.Println("[1/4] Listener closed. Rejecting new connections.")

	// Step B: Sever existing client sockets to break blocked scanner loops
	activeConns.Range(func(key, value any) bool {
		if conn, ok := key.(net.Conn); ok {
			remoteAddr := conn.RemoteAddr().String()
			conn.Close()
			fmt.Printf("[-] Connection closed: %s\n", remoteAddr)
		}
		return true
	})

	// Step C: Wait for all connection handler goroutines to finish pushing into jobQueue
	connWg.Wait()
	fmt.Println("[2/4] All connection handlers stopped. Zero incoming network writes remaining.")

	// Step D: Safely close the jobQueue (guaranteed no more writers exist)
	close(jobQueue)
	fmt.Printf("[3/4] Closed job queue (%d buffered items remaining to drain).\n", len(jobQueue))

	// Step E: Wait for all workers to finish processing remaining jobs in the channel
	workerWg.Wait()
	fmt.Println("[4/4] All worker threads terminated cleanly.")

	finalCount := atomic.LoadUint64(&totalProcessed)
	fmt.Printf("==> Shutdown complete. All systems terminated. Total ticks processed: %d\n", finalCount)
}

func handleConnection(conn net.Conn, jobQueue chan<- []byte) {
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

}

func worker(id int, jobs <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	// The loop terminates automatically when jobQueue is closed AND completely empty
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

func reportThroughput(ctx context.Context) {
	var lastCount uint64
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return // Exit reporter routine immediately on shutdown
		case <-ticker.C:
			current := atomic.LoadUint64(&totalProcessed)
			delta := current - lastCount
			lastCount = current

			if delta > 0 {
				fmt.Printf("==> Throughput: %d msgs/sec | Cumulative: %d msgs\n", delta, current)
			}
		}

	}
}
