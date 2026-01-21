// Package main implements a soak test for HelmetFS that focuses on
// verifying concurrency safety through targeted invariant checks.
//
// Design principles:
//   - Each test uses isolated files (unique names) to avoid cross-test interference
//   - Tests verify specific invariants, not just "did it work"
//   - No in-memory state tracking - we verify server consistency directly
//   - Chaos workers provide background load while invariant tests run
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

var (
	targetURL   = flag.String("url", "http://localhost:8080", "target server URL")
	duration    = flag.Duration("duration", 0, "test duration (0 = run forever)")
	concurrency = flag.Int("concurrency", 4, "number of concurrent workers")
	seed        = flag.Int64("seed", 0, "random seed (0 = use current time)")

	// Failure injection flags
	blobsDir    = flag.String("blobs", "", "path to helmetfs blobs directory (enables corruption injection)")
	corruptRate = flag.Float64("corrupt-rate", 0, "probability of corrupting a blob after write (0-1)")

	// Output flags
	reportFile = flag.String("report", "", "path to write markdown report (optional)")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	s := *seed
	if s == 0 {
		s = time.Now().UnixNano()
	}
	startTime := time.Now()
	log.Printf("soak test starting: url=%s concurrency=%d seed=%d", *targetURL, *concurrency, s)

	ctx, cancel := context.WithCancel(context.Background())
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, *duration)
	}
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Println("received interrupt, shutting down...")
		cancel()
	}()

	var wg sync.WaitGroup
	failures := make(chan Failure, 1000)
	var opCount atomic.Int64
	stats := &Stats{counts: make(map[string]int64)}

	// Initialize failure injector if configured
	var injector *Injector
	if *blobsDir != "" && *corruptRate > 0 {
		injector = &Injector{
			blobsDir:    *blobsDir,
			corruptRate: *corruptRate,
		}
		log.Printf("failure injection enabled: corrupt-rate=%.2f", *corruptRate)
	}

	// Start workers
	for i := range *concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := &Worker{
				id:       id,
				client:   &http.Client{Timeout: 30 * time.Second},
				rng:      rand.New(rand.NewPCG(uint64(s), uint64(id))),
				baseURL:  *targetURL,
				failures: failures,
				stats:    stats,
				injector: injector,
			}
			w.Run(ctx, &opCount)
		}(i)
	}

	wg.Wait()
	close(failures)
	elapsed := time.Since(startTime)

	var failureList []Failure
	for f := range failures {
		failureList = append(failureList, f)
	}

	log.Printf("soak test complete: operations=%d failures=%d elapsed=%s", opCount.Load(), len(failureList), elapsed.Round(time.Second))
	if len(failureList) > 0 {
		log.Println("failures:")
		for _, f := range failureList {
			log.Printf("  [%s] %s", f.Test, f.Error)
		}
	}

	if *reportFile != "" {
		report := generateReport(s, startTime, elapsed, opCount.Load(), stats.Snapshot(), failureList)
		if err := os.WriteFile(*reportFile, []byte(report), 0644); err != nil {
			log.Printf("warning: failed to write report: %v", err)
		}
	}

	if len(failureList) > 0 {
		return fmt.Errorf("%d invariant violations", len(failureList))
	}
	return nil
}

type Failure struct {
	Test      string
	Error     string
	Timestamp time.Time
}

type Stats struct {
	mu     sync.Mutex
	counts map[string]int64
}

func (s *Stats) Inc(name string) {
	s.mu.Lock()
	s.counts[name]++
	s.mu.Unlock()
}

func (s *Stats) Snapshot() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]int64, len(s.counts))
	for k, v := range s.counts {
		result[k] = v
	}
	return result
}

func generateReport(seed int64, startTime time.Time, elapsed time.Duration, totalOps int64, opCounts map[string]int64, failures []Failure) string {
	var status, emoji string
	if len(failures) == 0 {
		status, emoji = "Passed", "✅"
	} else {
		status, emoji = "Failed", "❌"
	}

	report := fmt.Sprintf("# %s Soak Test %s\n\n", emoji, status)
	report += "## Summary\n\n"
	report += fmt.Sprintf("| Metric | Value |\n|--------|-------|\n")
	report += fmt.Sprintf("| Duration | %s |\n", elapsed.Round(time.Second))
	report += fmt.Sprintf("| Operations | %d |\n", totalOps)
	report += fmt.Sprintf("| Ops/sec | %.1f |\n", float64(totalOps)/elapsed.Seconds())
	report += fmt.Sprintf("| Failures | %d |\n", len(failures))
	report += fmt.Sprintf("| Concurrency | %d |\n", *concurrency)
	report += fmt.Sprintf("| Seed | `%d` |\n", seed)
	report += fmt.Sprintf("| Started | %s |\n\n", startTime.UTC().Format(time.RFC3339))

	if len(opCounts) > 0 {
		report += "## Operations\n\n| Test | Count |\n|------|------:|\n"
		for name, count := range opCounts {
			report += fmt.Sprintf("| %s | %d |\n", name, count)
		}
		report += "\n"
	}

	if len(failures) > 0 {
		report += "## Failures\n\n"
		for _, f := range failures {
			report += fmt.Sprintf("### %s\n```\n%s\n```\n\n", f.Test, f.Error)
		}
	}

	report += "## Reproduce\n\n```bash\n"
	report += fmt.Sprintf("go run ./soaktest -seed %d -duration %s -concurrency %d", seed, *duration, *concurrency)
	if *corruptRate > 0 {
		report += fmt.Sprintf(" -corrupt-rate %.4f -blobs %s", *corruptRate, *blobsDir)
	}
	report += "\n```\n"

	return report
}
