// Package main implements the Gobbler high-throughput financial tick ingestion gateway.
//
// Key Responsibilities:
//   - Intercepts OS signals (SIGINT, SIGTERM) via signal.NotifyContext for graceful teardown.
//   - Listens for incoming TCP client connections on port 8008 in a non-blocking loop.
//   - Tracks active network socket references using thread-safe sync.Map to enable immediate socket severance on shutdown.
//   - Dispatches payload scanning to handleConnection, copying byte slices to bypass bufio buffer recycling gotchas.
//   - Distributes workloads to a CPU-scaled worker pool via a bounded job queue channel.
//   - Exposes an embedded net/http/pprof diagnostic server on port 6060 for live performance profiling.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"         // Exposes HTTP endpoints for diagnostics
	_ "net/http/pprof" // Side-effect import: automatically registers /debug/pprof handlers on http.DefaultServeMux
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// MarketTick defines the data schema contract for incoming stock/crypto trade events across the pipeline.
type MarketTick struct {
	Ticker      string  `json:"ticker"`               // Financial ticker symbol (e.g., "PETR4", "BTC-USD")
	Price       float64 `json:"price"`                // Executed trade price
	Volume      int     `json:"volume"`               // Quantity of shares/contracts traded
	TimestampNs int16   `json:"timestamp_ns"`         // High-resolution nanosecond timestamp
	IsAnomaly   bool    `json:"is_anomaly,omitempty"` // Synthetic anomaly flag emitted by the generator
}

// totalProcessed is a thread-safe global atomic counter tracking the total count of market ticks
// unmarshaled and processed across all active worker goroutines since startup.
var totalProcessed uint64

func main() {
	// -------------------------------------------------------------------------
	// Step 1: Signal Interception & Context Lifecycle Setup
	// -------------------------------------------------------------------------
	// Create a root context that automatically cancels when an OS interrupt signal
	// (Ctrl+C / SIGINT) or Kubernetes termination signal (SIGTERM) is received.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // Ensure signal resources are released when main exits

	// -------------------------------------------------------------------------
	// Step 2: Bounded Job Queue & Worker Pool Initialization
	// -------------------------------------------------------------------------
	// Allocate a bounded buffered channel to hold raw byte payloads awaiting processing.
	// Bounding the capacity (50,000) prevents unbounded memory growth under traffic spikes.
	const queueCapacity = 50000
	jobQueue := make(chan []byte, queueCapacity)

	// Scale worker goroutines to match the available logical CPU cores for CPU-bound JSON parsing.
	numWorkers := runtime.NumCPU()
	var workerWg sync.WaitGroup

	fmt.Printf("Initializing %d processing workers...\n", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		workerWg.Add(1)
		go worker(i, jobQueue, &workerWg)
	}

	// -------------------------------------------------------------------------
	// Step 3: Background Metrics Reporter
	// -------------------------------------------------------------------------
	// Launch a dedicated goroutine to log throughput metrics once per second.
	go reportThroughput(ctx)

	// -------------------------------------------------------------------------
	// Step 4: TCP Listener Setup
	// -------------------------------------------------------------------------
	serverIPAddr := "127.0.0.1"
	serverPort := "8008"

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", serverIPAddr, serverPort))
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", serverPort, err)
	}
	defer listener.Close()

	fmt.Printf("Ingestion Gateway online at %s. Waiting for connections...\n", fmt.Sprintf("%s:%s", serverIPAddr, serverPort))

	// Track connection routine lifecycles and maintain a thread-safe map of active sockets for forced teardown.
	var connWg sync.WaitGroup
	var activeConns sync.Map // Map[net.Conn]struct{}

	// -------------------------------------------------------------------------
	// Step 5: Background Diagnostic Server (pprof)
	// -------------------------------------------------------------------------
	// Launch an embedded HTTP server on port 6060 to expose live pprof memory and CPU profiling data.
	go func() {
		log.Println("pprof server listening on http://127.0.0.1:6060/debug/pprof/")
		if err := http.ListenAndServe("127.0.0.1:6060", nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("pprof server error: %v", err)
		}
	}()

	// -------------------------------------------------------------------------
	// Step 6: Non-blocking Connection Acceptance Loop
	// -------------------------------------------------------------------------
	// Run the accept loop in a separate goroutine so the main thread can block waiting for OS shutdown signals.
	go func() {
		for {
			// Accept blocks execution until a client connects:
			conn, err := listener.Accept()
			if err != nil {
				// When listener.Close() is called during shutdown, Accept returns net.ErrClosed.
				if errors.Is(err, net.ErrClosed) {
					return // Normal exit on listener shutdown
				}
				log.Printf("Failed to accept connection: %v", err)
				continue
			}

			// Store the active connection in the map to enable active disconnection on shutdown.
			activeConns.Store(conn, struct{}{})
			connWg.Add(1)

			// Spawn a dedicated handler routine per client socket.
			go func(c net.Conn) {
				defer connWg.Done()
				defer activeConns.Delete(c)
				defer c.Close()

				fmt.Printf("Client connected from: %s\n", c.RemoteAddr().String())
				handleConnection(c, jobQueue)
			}(conn)
		}
	}()

	// -------------------------------------------------------------------------
	// Step 7: Graceful Shutdown Coordination
	// -------------------------------------------------------------------------
	// Block main until an OS termination signal cancels the context.
	<-ctx.Done()
	fmt.Printf("\n[!] Shutdown signal received. Starting graceful drain...\n")

	// Phase A: Close the TCP listener to block any new incoming connections.
	listener.Close()
	fmt.Println("[1/4] Listener closed. Rejecting new connections.")

	// Phase B: Sever existing client sockets to interrupt blocked bufio.Scanner loops.
	activeConns.Range(func(key, value any) bool {
		if conn, ok := key.(net.Conn); ok {
			remoteAddr := conn.RemoteAddr().String()
			conn.Close()
			fmt.Printf("[-] Connection closed: %s\n", remoteAddr)
		}
		return true
	})

	// Phase C: Wait for all connection handler goroutines to stop pushing to jobQueue.
	connWg.Wait()
	fmt.Println("[2/4] All connection handlers stopped. Zero incoming network writes remaining.")

	// Phase D: Safely close the jobQueue channel (guaranteed no active producers exist).
	close(jobQueue)
	fmt.Printf("[3/4] Closed job queue (%d buffered items remaining to drain).\n", len(jobQueue))

	// Phase E: Wait for all worker goroutines to drain the channel and finish processing.
	workerWg.Wait()
	fmt.Println("[4/4] All worker threads terminated cleanly.")

	finalCount := atomic.LoadUint64(&totalProcessed)
	fmt.Printf("==> Shutdown complete. All systems terminated. Total ticks processed: %d\n", finalCount)
}

// handleConnection streams raw bytes from a single TCP client connection, frames payloads
// on newline ('\n') boundaries using bufio.Scanner, and dispatches payload copies into jobQueue.
func handleConnection(conn net.Conn, jobQueue chan<- []byte) {
	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("[+] New connection established: %s\n", remoteAddr)

	// Break incoming byte stream into discrete lines using newline delimiter ('\n').
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		rawBytes := scanner.Bytes()

		// CRITICAL MEMORY PATTERN:
		// scanner.Bytes() returns a slice pointing to bufio's internal buffer, which is overwritten
		// on the subsequent Scan() call. We MUST allocate a distinct copy before passing it across channels.
		payloadCopy := make([]byte, len(rawBytes))
		copy(payloadCopy, rawBytes)

		// Push to the worker queue. If jobQueue capacity (50,000) is reached, this line blocks,
		// triggering TCP socket backpressure back to the sending client.
		jobQueue <- payloadCopy
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error on %s: %v", remoteAddr, err)
	}
}

// worker is a consumer routine in the worker pool. It reads raw byte payloads from jobs,
// unmarshals them into MarketTick structs, updates global metrics, and flags anomalies.
func worker(id int, jobs <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	// Range loop continuously receives jobs until jobs channel is closed AND fully drained.
	for payload := range jobs {
		var tick MarketTick
		if err := json.Unmarshal(payload, &tick); err != nil {
			continue // In production, route corrupted payloads to a Dead Letter Queue (DLQ)
		}

		// Increment atomic throughput metric counter
		atomic.AddUint64(&totalProcessed, 1)

		// Log alerted anomalies detected by the synthetic generator
		if tick.IsAnomaly {
			fmt.Printf("[WORKER %d ALERT] Injected Anomaly Caught: %s @ $%.2f (Vol: %d)\n", id, tick.Ticker, tick.Price, tick.Volume)
		}
	}
}

// reportThroughput periodically logs the message processing rate (msgs/sec) and cumulative totals.
// It terminates cleanly when the provided context is cancelled.
func reportThroughput(ctx context.Context) {
	var lastCount uint64
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return // Exit reporter routine on system context cancellation
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
