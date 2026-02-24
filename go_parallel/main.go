package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"bigtable-optimizer/parallel"

	"cloud.google.com/go/bigtable"
)

func sendOneRpcPerKey(ctx context.Context, tbl *bigtable.Table, rowKeys []string) error {
	var wg sync.WaitGroup
	wg.Add(len(rowKeys))

	var firstErr error
	var errMu sync.Mutex

	for _, key := range rowKeys {
		go func(k string) {
			defer wg.Done()
			err := tbl.ReadRows(ctx, bigtable.RowList{k}, func(row bigtable.Row) bool {
				return true
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(key)
	}

	wg.Wait()
	return firstErr
}

// generateRandomKeys creates 'count' number of random keys matching your table's schema.
func generateRandomKeys(count int) []string {
	rand.Seed(time.Now().UnixNano())
	keys := make([]string, count)

	for i := 0; i < count; i++ {
		// Generates a random number between 0 and 9999999, padded with leading zeros
		randomSuffix := rand.Intn(10000000)
		keys[i] = fmt.Sprintf("5120#10000#00%07d", randomSuffix)
	}
	return keys
}

func main() {
	ctx := context.Background()
	projectPtr := flag.String("project", "my-project", "Google Cloud Project ID")
	instancePtr := flag.String("instance", "my-instance", "Bigtable Instance ID")
	tablePtr := flag.String("table", "my-table", "Bigtable Table Name")

	flag.Parse()

	log.Println("Connecting to Bigtable...")
	client, err := bigtable.NewClient(ctx, *projectPtr, *instancePtr)
	if err != nil {
		log.Fatalf("Could not create client: %v", err)
	}
	defer client.Close()

	tbl := client.Open(*tablePtr)

	log.Println("Warming up tablet splits cache...")
	if err := parallel.WarmUpTableSplits(ctx, tbl); err != nil {
		log.Printf("Warning: Could not warm up cache (will fall back to 1 RPC per key): %v\n", err)
	}

	if err != nil {
		log.Printf("not able to warm up tablet splits")
	}

	iterations := 1000
	keysToFetch := generateRandomKeys(100)

	log.Printf("Starting experiment with 100 random keys (%d iterations)...\n\n", iterations)

	// 1. Benchmark: Optimized Split-Batching
	var totalSplitTime time.Duration
	for i := 0; i < iterations; i++ {
		start := time.Now()
		_ = parallel.ReadParallelRowsKeys(ctx, tbl, keysToFetch, func(row bigtable.Row) bool { return true })
		duration := time.Since(start)
		totalSplitTime += duration
		fmt.Printf("[Splits] Iteration %d took: %v\n", i+1, duration)
	}
	avgSplitTime := totalSplitTime / time.Duration(iterations)

	fmt.Println("--------------------------------------------------")

	// 2. Benchmark: 1 RPC Per Key
	var totalBruteTime time.Duration
	for i := 0; i < iterations; i++ {
		start := time.Now()
		_ = sendOneRpcPerKey(ctx, tbl, keysToFetch)
		duration := time.Since(start)
		totalBruteTime += duration
		fmt.Printf("[1-Per-Key] Iteration %d took: %v\n", i+1, duration)
	}
	avgBruteTime := totalBruteTime / time.Duration(iterations)

	// --- RESULTS ---
	fmt.Println("\n================ EXPERIMENT RESULTS ================")
	fmt.Printf("Optimized (Tablet Splits) Average: %v\n", avgSplitTime)
	fmt.Printf("Brute Force (1 RPC Per Key) Average: %v\n", avgBruteTime)
	fmt.Println("====================================================")
}
