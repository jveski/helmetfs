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
	"sort"
	"strings"
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
	dbPath      = flag.String("db", "", "path to helmetfs database (enables failure injection)")
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

	// Initialize failure injection if configured
	var injector *FailureInjector
	if *dbPath != "" || *blobsDir != "" {
		var err error
		injector, err = newFailureInjector(*dbPath, *blobsDir, *corruptRate)
		if err != nil {
			return fmt.Errorf("failed to initialize failure injector: %w", err)
		}
		if injector != nil {
			defer injector.Close()
			log.Printf("failure injection enabled: corrupt-rate=%.2f", *corruptRate)
		}
	}

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

	state := &State{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
	state.dirs["/"] = true

	var wg sync.WaitGroup
	failures := make(chan Failure, 1000)
	var opCount atomic.Int64
	opStats := &OpStats{count: make(map[string]int64)}

	for i := range *concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := &Worker{
				id:       id,
				client:   &http.Client{Timeout: 30 * time.Second},
				state:    state,
				rng:      rand.New(rand.NewPCG(uint64(s), uint64(id))),
				baseURL:  *targetURL,
				failures: failures,
				injector: injector,
				opStats:  opStats,
			}
			w.run(ctx, &opCount)
		}(i)
	}

	wg.Wait()
	close(failures)
	endTime := time.Now()
	elapsed := endTime.Sub(startTime)

	var failureList []Failure
	for f := range failures {
		failureList = append(failureList, f)
	}

	log.Printf("soak test complete: operations=%d failures=%d", opCount.Load(), len(failureList))
	if len(failureList) > 0 {
		log.Println("failures:")
		for _, f := range failureList {
			log.Printf("  [%s] %s: %s", f.Operation, f.Path, f.Error)
		}
	}

	// Generate markdown report if requested
	if *reportFile != "" {
		report := generateMarkdownReport(s, startTime, elapsed, opCount.Load(), opStats.Get(), failureList, injector)
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
	Operation string
	Path      string
	Error     string
	Timestamp time.Time
}

// OpStats tracks statistics for each operation type
type OpStats struct {
	mu    sync.Mutex
	count map[string]int64
}

func (o *OpStats) Inc(op string) {
	o.mu.Lock()
	o.count[op]++
	o.mu.Unlock()
}

func (o *OpStats) Get() map[string]int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make(map[string]int64, len(o.count))
	for k, v := range o.count {
		result[k] = v
	}
	return result
}

func generateMarkdownReport(seed int64, startTime time.Time, elapsed time.Duration, totalOps int64, opCounts map[string]int64, failures []Failure, injector *FailureInjector) string {
	var sb strings.Builder

	// Header with result emoji
	if len(failures) == 0 {
		sb.WriteString("# ✅ Soak Test Passed\n\n")
	} else {
		sb.WriteString("# ❌ Soak Test Failed\n\n")
	}

	// Summary section
	sb.WriteString("## Summary\n\n")
	sb.WriteString("| Metric | Value |\n")
	sb.WriteString("|--------|-------|\n")
	sb.WriteString(fmt.Sprintf("| **Status** | %s |\n", statusBadge(len(failures) == 0)))
	sb.WriteString(fmt.Sprintf("| **Duration** | %s |\n", elapsed.Round(time.Second)))
	sb.WriteString(fmt.Sprintf("| **Total Operations** | %s |\n", formatNumber(totalOps)))
	sb.WriteString(fmt.Sprintf("| **Operations/sec** | %.1f |\n", float64(totalOps)/elapsed.Seconds()))
	sb.WriteString(fmt.Sprintf("| **Failures** | %d |\n", len(failures)))
	sb.WriteString(fmt.Sprintf("| **Concurrency** | %d workers |\n", *concurrency))
	sb.WriteString(fmt.Sprintf("| **Random Seed** | `%d` |\n", seed))
	sb.WriteString(fmt.Sprintf("| **Started** | %s |\n", startTime.UTC().Format(time.RFC3339)))
	sb.WriteString("\n")

	// Failure injection status
	if injector != nil {
		sb.WriteString("## Failure Injection\n\n")
		sb.WriteString("| Setting | Value |\n")
		sb.WriteString("|---------|-------|\n")
		if *corruptRate > 0 {
			sb.WriteString(fmt.Sprintf("| Corruption Rate | %.2f%% |\n", *corruptRate*100))
		} else {
			sb.WriteString("| Corruption Rate | Disabled |\n")
		}
		sb.WriteString("\n")
	}

	// Operation breakdown
	sb.WriteString("## Operation Breakdown\n\n")
	if len(opCounts) > 0 {
		// Sort operations by count (descending)
		type opEntry struct {
			name  string
			count int64
		}
		var ops []opEntry
		for name, count := range opCounts {
			ops = append(ops, opEntry{name, count})
		}
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].count > ops[j].count
		})

		sb.WriteString("| Operation | Count | % of Total |\n")
		sb.WriteString("|-----------|------:|----------:|\n")
		for _, op := range ops {
			pct := float64(op.count) / float64(totalOps) * 100
			sb.WriteString(fmt.Sprintf("| `%s` | %s | %.1f%% |\n", op.name, formatNumber(op.count), pct))
		}
		sb.WriteString("\n")
	} else {
		sb.WriteString("_No operation statistics collected._\n\n")
	}

	// Failures section
	if len(failures) > 0 {
		sb.WriteString("## Failures\n\n")
		sb.WriteString(fmt.Sprintf("**%d invariant violation(s) detected:**\n\n", len(failures)))

		// Group failures by operation type
		failuresByOp := make(map[string][]Failure)
		for _, f := range failures {
			failuresByOp[f.Operation] = append(failuresByOp[f.Operation], f)
		}

		// Sort operation types
		var opTypes []string
		for op := range failuresByOp {
			opTypes = append(opTypes, op)
		}
		sort.Strings(opTypes)

		for _, op := range opTypes {
			opFailures := failuresByOp[op]
			sb.WriteString(fmt.Sprintf("### %s (%d failure%s)\n\n", op, len(opFailures), pluralize(len(opFailures))))

			for i, f := range opFailures {
				if i >= 10 {
					sb.WriteString(fmt.Sprintf("_...and %d more failures of this type_\n\n", len(opFailures)-10))
					break
				}
				sb.WriteString("<details>\n")
				if f.Path != "" {
					sb.WriteString(fmt.Sprintf("<summary><code>%s</code></summary>\n\n", f.Path))
				} else {
					sb.WriteString(fmt.Sprintf("<summary>Failure %d</summary>\n\n", i+1))
				}
				sb.WriteString("```\n")
				sb.WriteString(f.Error)
				sb.WriteString("\n```\n\n")
				if !f.Timestamp.IsZero() {
					sb.WriteString(fmt.Sprintf("_Occurred at: %s_\n\n", f.Timestamp.UTC().Format(time.RFC3339)))
				}
				sb.WriteString("</details>\n\n")
			}
		}
	} else {
		sb.WriteString("## Failures\n\n")
		sb.WriteString("_No invariant violations detected._\n\n")
	}

	// Reproduction instructions
	sb.WriteString("## Reproduce This Run\n\n")
	sb.WriteString("To reproduce this exact test run, use the following seed:\n\n")
	sb.WriteString("```bash\n")
	sb.WriteString(fmt.Sprintf("go run ./soaktest -seed %d -duration %s -concurrency %d", seed, *duration, *concurrency))
	if *corruptRate > 0 {
		sb.WriteString(fmt.Sprintf(" -corrupt-rate %.4f", *corruptRate))
	}
	sb.WriteString("\n```\n")

	return sb.String()
}

func statusBadge(passed bool) string {
	if passed {
		return "✅ Passed"
	}
	return "❌ Failed"
}

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%.2fM", float64(n)/1000000)
}

func pluralize(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
