package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

var (
	targetURL      = flag.String("url", "http://localhost:8080", "target server URL")
	fileSize       = flag.Int("file-size", 1024*1024, "file size in bytes")
	maxConcurrency = flag.Int("max-concurrency", 16, "maximum number of concurrent writers")
	duration       = flag.Duration("duration", 30*time.Second, "duration per concurrency level")
	warmupDuration = flag.Duration("warmup", 5*time.Second, "warmup duration before measurement")
	outputFile     = flag.String("output", "results.json", "output JSON file for results")
)

// Result represents throughput at a given concurrency level
type Result struct {
	Concurrency    int     `json:"concurrency"`
	ThroughputMBps float64 `json:"throughput_mbps"`
	OpsPerSec      float64 `json:"ops_per_sec"`
	AvgLatencyMs   float64 `json:"avg_latency_ms"`
	P50LatencyMs   float64 `json:"p50_latency_ms"`
	P95LatencyMs   float64 `json:"p95_latency_ms"`
	P99LatencyMs   float64 `json:"p99_latency_ms"`
	Errors         int64   `json:"errors"`
}

// BenchmarkResults contains all results and metadata
type BenchmarkResults struct {
	Timestamp   string   `json:"timestamp"`
	FileSizeKB  int      `json:"file_size_kb"`
	DurationSec int      `json:"duration_sec"`
	Results     []Result `json:"results"`
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	log.Printf("Throughput benchmark: url=%s file_size=%d max_concurrency=%d duration=%s",
		*targetURL, *fileSize, *maxConcurrency, *duration)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Println("received interrupt, shutting down...")
		cancel()
	}()

	// Prepare test data
	testData := make([]byte, *fileSize)
	for i := range testData {
		testData[i] = byte(rand.IntN(256))
	}

	results := BenchmarkResults{
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		FileSizeKB:  *fileSize / 1024,
		DurationSec: int(duration.Seconds()),
	}

	// Test increasing concurrency levels: 1, 2, 4, 8, 16, ...
	for concurrency := 1; concurrency <= *maxConcurrency; concurrency *= 2 {
		select {
		case <-ctx.Done():
			break
		default:
		}

		log.Printf("Testing concurrency=%d", concurrency)
		result, err := runBenchmark(ctx, concurrency, testData)
		if err != nil {
			log.Printf("Error at concurrency %d: %v", concurrency, err)
			continue
		}
		results.Results = append(results.Results, result)
		log.Printf("  Throughput: %.2f MB/s, Ops: %.2f/s, Avg Latency: %.2fms",
			result.ThroughputMBps, result.OpsPerSec, result.AvgLatencyMs)
	}

	// Write results to JSON
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal results: %w", err)
	}
	if err := os.WriteFile(*outputFile, data, 0644); err != nil {
		return fmt.Errorf("write results: %w", err)
	}

	log.Printf("Results written to %s", *outputFile)
	return nil
}

func runBenchmark(ctx context.Context, concurrency int, testData []byte) (Result, error) {
	var (
		bytesWritten atomic.Int64
		opsCompleted atomic.Int64
		errors       atomic.Int64
		latencies    = make(chan time.Duration, 100000)
	)

	// Create worker context
	benchCtx, benchCancel := context.WithCancel(ctx)
	defer benchCancel()

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(benchCtx, id, testData, &bytesWritten, &opsCompleted, &errors, latencies)
		}(i)
	}

	// Warmup phase
	log.Printf("  Warmup for %s...", *warmupDuration)
	time.Sleep(*warmupDuration)

	// Reset counters after warmup
	bytesWritten.Store(0)
	opsCompleted.Store(0)
	errors.Store(0)
	// Drain latency channel
	for len(latencies) > 0 {
		<-latencies
	}

	// Measurement phase
	log.Printf("  Measuring for %s...", *duration)
	time.Sleep(*duration)

	// Stop workers
	benchCancel()
	wg.Wait()
	close(latencies)

	// Calculate results
	elapsed := duration.Seconds()
	totalBytes := bytesWritten.Load()
	totalOps := opsCompleted.Load()
	totalErrors := errors.Load()

	throughputMBps := float64(totalBytes) / (1024 * 1024) / elapsed
	opsPerSec := float64(totalOps) / elapsed

	// Collect latencies
	var allLatencies []time.Duration
	for lat := range latencies {
		allLatencies = append(allLatencies, lat)
	}

	var avgLatency, p50, p95, p99 float64
	if len(allLatencies) > 0 {
		// Sort for percentiles
		sortDurations(allLatencies)

		var total time.Duration
		for _, l := range allLatencies {
			total += l
		}
		avgLatency = float64(total.Milliseconds()) / float64(len(allLatencies))
		p50 = float64(percentile(allLatencies, 50).Milliseconds())
		p95 = float64(percentile(allLatencies, 95).Milliseconds())
		p99 = float64(percentile(allLatencies, 99).Milliseconds())
	}

	return Result{
		Concurrency:    concurrency,
		ThroughputMBps: throughputMBps,
		OpsPerSec:      opsPerSec,
		AvgLatencyMs:   avgLatency,
		P50LatencyMs:   p50,
		P95LatencyMs:   p95,
		P99LatencyMs:   p99,
		Errors:         totalErrors,
	}, nil
}

func worker(ctx context.Context, id int, testData []byte, bytesWritten, opsCompleted, errors *atomic.Int64, latencies chan<- time.Duration) {
	client := &http.Client{Timeout: 30 * time.Second}
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(id)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		filePath := path.Join("/", fmt.Sprintf("perf_%d_%s.bin", id, randomName(rng)))

		start := time.Now()

		// Write file
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, *targetURL+filePath, bytes.NewReader(testData))
		if err != nil {
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() == nil {
				errors.Add(1)
			}
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
			errors.Add(1)
			continue
		}

		elapsed := time.Since(start)

		bytesWritten.Add(int64(len(testData)))
		opsCompleted.Add(1)

		select {
		case latencies <- elapsed:
		default:
			// Channel full, skip this latency sample
		}

		// Cleanup - delete file (don't count this in latency)
		delReq, _ := http.NewRequestWithContext(ctx, http.MethodDelete, *targetURL+filePath, nil)
		if delReq != nil {
			delResp, err := client.Do(delReq)
			if err == nil {
				io.Copy(io.Discard, delResp.Body)
				delResp.Body.Close()
			}
		}
	}
}

func randomName(rng *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	length := 8
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rng.IntN(len(chars))]
	}
	return string(b)
}

func sortDurations(d []time.Duration) {
	// Simple insertion sort (good enough for our purposes)
	for i := 1; i < len(d); i++ {
		j := i
		for j > 0 && d[j-1] > d[j] {
			d[j-1], d[j] = d[j], d[j-1]
			j--
		}
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
