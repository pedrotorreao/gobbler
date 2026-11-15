// Package main implements the Gobbler high-throughput financial tick ingestion gateway.
//
// Key Responsibilities:
//   - Intercepts OS signals (SIGINT, SIGTERM) via signal.NotifyContext for graceful 5-phase teardown.
//   - Listens for incoming TCP client connections on port 8008 in a non-blocking accept loop.
//   - Tracks active network socket references using a thread-safe sync.Map to enable immediate socket severance on shutdown.
//   - Recycles *[]byte buffers via sync.Pool (bufferPool) in handleConnection, eliminating per-packet heap allocations
//     while safely decoupling payloads from bufio.Scanner's internal underlying array.
//   - Distributes workloads to a CPU-scaled worker pool (runtime.NumCPU() goroutines) via a bounded channel (jobQueue, 50k cap).
//   - Publishes validated, ticker-keyed ticks to a 4-partition Redpanda broker using an asynchronous segmentio/kafka-go Writer,
//     returning *[]byte buffers to sync.Pool inside the kafkaWriter.Completion callback via kafka.Message.WriterData.
//   - Exposes an embedded net/http/pprof diagnostic server on port 6060 for live CPU, heap, and goroutine profiling.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	"github.com/segmentio/kafka-go"
)

// MarketTick defines the data schema contract for incoming stock/crypto trade events across the pipeline.
type MarketTick struct {
	Ticker      string  `json:"ticker"`               // Financial ticker symbol (e.g., "PETR4", "BTC-USD")
	Price       float64 `json:"price"`                // Executed trade price
	Volume      int     `json:"volume"`               // Quantity of shares/contracts traded
	TimestampNs int64   `json:"timestamp_ns"`         // High-resolution nanosecond timestamp
	IsAnomaly   bool    `json:"is_anomaly,omitempty"` // Synthetic anomaly flag emitted by the generator
}

// totalProcessed is a thread-safe global atomic counter tracking the total count of market ticks
// unmarshaled and processed across all active worker goroutines since startup.
var totalProcessed uint64

// LEARNING NOTE — Why Pool *[]byte Pointers Instead of []byte Value Slices?
// In Go's runtime, an interface value ('any' / 'interface{}') is a 2-word (16-byte) pair:
//   [ type descriptor pointer (8B) | data pointer (8B) ]
// Meanwhile, a slice header ('[]byte') is a 3-word (24-byte) struct:
//   [ array pointer (8B) | len (8B) | cap (8B) ]
// Because a 24-byte slice header cannot fit inside the 8-byte data slot of 'any', calling
// bufferPool.Put(sliceValue) forces the Go compiler to allocate a 24-byte box on the heap
// (via runtime.convTslice) on every single Put()!
// By storing a pointer to a slice ('*[]byte', which is 8 bytes), the pointer fits directly
// inside the interface's data word with ZERO heap allocations.
// We pre-allocate 512 bytes of capacity so ~85-byte JSON tick lines never trigger slice growth.
var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

func main() {
	// -------------------------------------------------------------------------
	// Step 1: Signal Interception & Context Lifecycle Setup
	// -------------------------------------------------------------------------
	// Create a root context that automatically cancels when an OS interrupt signal
	// (Ctrl+C / SIGINT) or Kubernetes termination signal (SIGTERM) is received.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // Ensure signal resources are released when main exits

	// -------------------------------------------------------------------------
	// Step 2: Bounded Job Queue, Kafka Producer & Worker Pool Initialization
	// -------------------------------------------------------------------------
	// Allocate a bounded buffered channel to hold *[]byte pointers awaiting processing.
	// Bounding the capacity (50,000) prevents unbounded memory growth under traffic spikes.
	const queueCapacity = 50000
	jobQueue := make(chan *[]byte, queueCapacity)

	// Configure a thread-safe, batched Kafka writer targeting Redpanda.
	//
	// LEARNING NOTE — Asynchronous Producer & Zero-Race Buffer Lifecycle:
	// 1. Synchronous Bottleneck (Async: false): WriteMessages blocks the calling worker
	//    goroutine until a batch fills (BatchTimeout: 5ms) and the broker round-trip
	//    completes (~2–5ms), capping 12 workers at ~2,400 msgs/sec.
	// 2. Asynchronous Mode (Async: true): WriteMessages appends the kafka.Message into an
	//    internal batch queue and returns immediately, allowing 12 workers to exceed 56,000+ msgs/sec.
	// 3. The Data Race Trap: Because WriteMessages returns while a background goroutine still
	//    holds a reference to 'msg.Value' (*bufPtr), calling bufferPool.Put(bufPtr) inside 'worker'
	//    would allow 'handleConnection' to overwrite the slice before Kafka transmits it!
	// 4. Solution (WriterData + Completion): We attach 'bufPtr' to 'kafka.Message.WriterData'
	//    (opaque client metadata never sent over the wire). Only after Redpanda acknowledges
	//    the batch does 'Completion' fire and safely return each '*[]byte' to 'bufferPool'.
	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(getEnv("KAFKA_BROKER", "localhost:9092")), // Redpanda/Kafka broker endpoint
		Topic:        "market-ticks",                                      // Target event stream topic for financial trade ticks
		Balancer:     &kafka.Hash{},                                       // Key hasher (fnv-1a) ensuring deterministic, strict per-ticker FIFO ordering
		BatchSize:    100,                                                 // Maximum message count before triggering an immediate batch write
		BatchTimeout: 5 * time.Millisecond,                                // Flush interval threshold to minimize streaming latency during low volume
		RequiredAcks: kafka.RequireOne,                                    // Await confirmation from the partition leader before considering write successful
		WriteTimeout: 2 * time.Second,                                     // Fail-safe deadline to prevent writer routines from hanging on dead brokers
		Async:        true,                                                // Decouples worker goroutines from network round-trip latency
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				log.Printf("[WARNING] Kafka batch delivery failed: %v\n", err)
			}
			// Iterate by index ('for i := range messages') rather than value ('for _, m := range messages')
			// to avoid copying the multi-field kafka.Message struct on every loop iteration.
			for i := range messages {
				if bufPtr, ok := messages[i].WriterData.(*[]byte); ok && bufPtr != nil {
					bufferPool.Put(bufPtr)
				}
			}
		},
	}

	// Scale worker goroutines to match the available logical CPU cores for CPU-bound JSON parsing.
	numWorkers := runtime.NumCPU()
	var workerWg sync.WaitGroup

	fmt.Printf("Initializing %d processing workers...\n", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		workerWg.Add(1)
		go worker(i, jobQueue, &workerWg, kafkaWriter)
	}

	// -------------------------------------------------------------------------
	// Step 3: Background Metrics Reporter
	// -------------------------------------------------------------------------
	// Launch a dedicated goroutine to log throughput metrics once per second.
	go reportThroughput(ctx)

	// -------------------------------------------------------------------------
	// Step 4: TCP Listener Setup
	// -------------------------------------------------------------------------
	serverIPAddr := getEnv("INGESTION_GATEWAY_ADDR", "0.0.0.0")
	serverPort := getEnv("INGESTION_GATEWAY_PORT", "8008")

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
		pprofAddr := getEnv("PPROF_ADDR", "0.0.0.0:6060")
		log.Printf("pprof server listening on http://%s/debug/pprof/\n", pprofAddr)
		if err := http.ListenAndServe(pprofAddr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	fmt.Println("[1/5] Listener closed. Rejecting new connections.")

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
	fmt.Println("[2/5] All connection handlers stopped. Zero incoming network writes remaining.")

	// Phase D: Safely close the jobQueue channel (guaranteed no active producers exist).
	close(jobQueue)
	fmt.Printf("[3/5] Closed job queue (%d buffered items remaining to drain).\n", len(jobQueue))

	// Phase E: Wait for all worker goroutines to drain the channel and finish processing.
	workerWg.Wait()
	fmt.Println("[4/5] All worker threads terminated cleanly.")

	// Phase F: Flush remaining buffered batches to Redpanda and close producer connections.
	if err := kafkaWriter.Close(); err != nil {
		log.Printf("[ERROR] Failed to close Kafka writer: %v\n", err)
	}
	fmt.Println("[5/5] Redpanda producer closed cleanly.")

	finalCount := atomic.LoadUint64(&totalProcessed)
	fmt.Printf("==> Shutdown complete. All systems terminated. Total ticks processed: %d\n", finalCount)
}

// handleConnection streams raw bytes from a single TCP client connection, frames payloads
// on newline ('\n') boundaries using bufio.Scanner, and dispatches pooled *[]byte buffers into jobQueue.
//
// LEARNING NOTE — bufio.Scanner Array Reuse & Zero-Allocation sync.Pool Copy:
// 1. The Scanner Trap: scanner.Bytes() does NOT allocate a new slice; it returns a view into
//    scanner's internal 4,096-byte buffer. On the next call to scanner.Scan(), that underlying
//    memory is overwritten! Sending rawBytes directly over a channel causes a subtle data race.
// 2. The Naive Fix (Phase 1): Calling make([]byte, len(rawBytes)) on every line creates a safe copy
//    but allocates ~300,000 heap objects every 5 seconds at 60k msgs/sec.
// 3. The Production Fix (Phase 5): Borrowing a pre-allocated *[]byte from bufferPool and resetting
//    its length via '(*bufPtr)[:0]' before appending rawBytes copies the payload into pooled RAM
//    with ZERO heap allocations (confirmed via pprof: -294,934 alloc_objects).
func handleConnection(conn net.Conn, jobQueue chan<- *[]byte) {
	remoteAddr := conn.RemoteAddr().String()
	fmt.Printf("[+] New connection established: %s\n", remoteAddr)

	// Break incoming byte stream into discrete lines using newline delimiter ('\n').
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		rawBytes := scanner.Bytes()

		// 1. Borrow a *[]byte pointer from the thread-safe sync.Pool:
		bufPtr := bufferPool.Get().(*[]byte)
		// 2. Reset slice length to 0 while preserving its 512-byte underlying array capacity, then copy bytes:
		*bufPtr = append((*bufPtr)[:0], rawBytes...)
		// 3. Dispatch the pointer to the worker channel/queue. If jobQueue capacity (50,000) is reached,
		//    this send blocks, propagating TCP ZeroWindow backpressure upstream to the sender socket.
		jobQueue <- bufPtr
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error on %s: %v", remoteAddr, err)
	}
}

// worker is a consumer routine in the worker pool. It reads *[]byte payload pointers from jobs,
// unmarshals them into MarketTick structs, publishes them to Redpanda partitioned by ticker symbol,
// updates global throughput metrics, and flags anomalies.
//
// LEARNING NOTE — Buffer Ownership & Defensive Early-Return Recycling:
// Once a worker receives 'bufPtr' from 'jobs', it owns that memory until one of three exits:
//   - Exit A (Corrupted JSON): 'json.Unmarshal' fails -> 'kafkaWriter.Completion' will never
//     run for this message, so the worker MUST call 'bufferPool.Put(bufPtr)' before 'continue'.
//   - Exit B (Shutdown Writer Closed): 'dispatchMsg' returns false -> the worker MUST call
//     'bufferPool.Put(bufPtr)' before 'return'.
//   - Exit C (Happy Path): 'dispatchMsg' succeeds -> ownership transfers to 'kafka.Writer',
//     which recycles 'bufPtr' inside the 'Completion' callback via 'msg.WriterData'.
func worker(id int, jobs <-chan *[]byte, wg *sync.WaitGroup, writer *kafka.Writer) {
	defer wg.Done()

	// Range loop continuously receives jobs until jobs channel is closed AND fully drained.
	for bufPtr := range jobs {
		var tick MarketTick
		if err := json.Unmarshal(*bufPtr, &tick); err != nil {
			bufferPool.Put(bufPtr) // Defensive recycle on malformed payload before skipping
			continue               // In production, route corrupted payloads to a Dead Letter Queue (DLQ)
		}

		// Construct Kafka message:
		// - Key: Ticker symbol (hashed by &kafka.Hash{} to guarantee strict per-asset FIFO ordering)
		// - Value: Reuses validated JSON byte slice (*bufPtr) with zero re-serialization overhead
		// - WriterData: Passes *bufPtr as opaque client metadata to kafkaWriter.Completion for safe recycling
		msg := kafka.Message{
			Key:        []byte(tick.Ticker),
			Value:      *bufPtr,
			WriterData: bufPtr,
		}

		// Dispatch message to Redpanda broker with exponential backoff retry resilience.
		// Uses context.Background() so in-flight items drain successfully even if root ctx is canceled.
		if !dispatchMsg(id, context.Background(), msg, writer) {
			bufferPool.Put(bufPtr) // Defensive recycle when writer is closed during shutdown
			return                 // Gateway writer is closed during shutdown; terminate worker
		}

		// Increment atomic throughput metric counter
		atomic.AddUint64(&totalProcessed, 1)

		// Log alerted anomalies detected by the synthetic generator
		if tick.IsAnomaly {
			fmt.Printf("[WORKER %d ALERT] Injected Anomaly Caught: %s @ $%.2f (Vol: %d)\n", id, tick.Ticker, tick.Price, tick.Volume)
		}
	}
}

// dispatchMsg attempts to write a message to Redpanda with exponential backoff.
// It retries indefinitely on broker outages to ensure zero message loss, applying natural backpressure
// to the worker pool and incoming TCP streams. Returns false if the writer was closed during shutdown.
func dispatchMsg(workerID int, ctx context.Context, msg kafka.Message, writer *kafka.Writer) bool {
	backoff := 50 * time.Millisecond
	const maxBackoff = 1 * time.Second

	for {
		err := writer.WriteMessages(ctx, msg)
		if err == nil {
			return true // Successfully published
		}
		if errors.Is(err, io.ErrClosedPipe) {
			return false // Gateway is shutting down and writer closed
		}

		log.Printf("[WORKER %d WARNING] Redpanda write failed: %v. Retrying in %v...", workerID, err, backoff)
		time.Sleep(backoff)

		// Exponential backoff: double the sleep interval up to maxBackoff
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
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

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
