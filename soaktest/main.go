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
	dbPath      = flag.String("db", "", "path to helmetfs database (enables failure injection)")
	blobsDir    = flag.String("blobs", "", "path to helmetfs blobs directory (enables corruption injection)")
	corruptRate = flag.Float64("corrupt-rate", 0, "probability of corrupting a blob after write (0-1)")
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
			}
			w.run(ctx, &opCount)
		}(i)
	}

	wg.Wait()
	close(failures)

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
		return fmt.Errorf("%d invariant violations", len(failureList))
	}
	return nil
}

type Failure struct {
	Operation string
	Path      string
	Error     string
}
