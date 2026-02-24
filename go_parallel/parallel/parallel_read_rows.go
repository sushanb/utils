package parallel

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
)

type sampleCacheEntry struct {
	splitKeys []string
	fetchedAt time.Time
}

var (
	// Map to hold cache per table instance.
	tableSampleCache = make(map[*bigtable.Table]sampleCacheEntry)
	// Map to track if a refresh is currently running for a table
	isRefreshing = make(map[*bigtable.Table]bool)
	cacheMu      sync.RWMutex
)

// // WarmUpTableSplits synchronously fetches tablet splits and populates the cache.
// It is safe to call concurrently; subsequent concurrent calls will become no-ops
// if a refresh is already in progress.
func WarmUpTableSplits(ctx context.Context, tbl *bigtable.Table) error {
	cacheMu.Lock()
	if isRefreshing[tbl] {
		cacheMu.Unlock()
		return nil // Refresh already in progress
	}
	isRefreshing[tbl] = true
	cacheMu.Unlock()

	// Ensure we clear the refreshing flag when we exit
	defer func() {
		cacheMu.Lock()
		delete(isRefreshing, tbl)
		cacheMu.Unlock()
	}()

	sampleCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	// Fetch new samples from Bigtable
	splitKeys, err := tbl.SampleRowKeys(sampleCtx)
	if err != nil {
		return fmt.Errorf("failed to sample row keys: %w", err)
	}

	cacheMu.Lock()
	tableSampleCache[tbl] = sampleCacheEntry{
		splitKeys: splitKeys,
		fetchedAt: time.Now(),
	}
	cacheMu.Unlock()

	return nil
}

// triggerAsyncRefresh initiates a background refresh using WarmUpTableSplits.
func triggerAsyncRefresh(tbl *bigtable.Table) {
	cacheMu.RLock()
	refreshing := isRefreshing[tbl]
	cacheMu.RUnlock()

	if refreshing {
		return
	}

	go func() {
		// Use a detached context so it doesn't fail if the parent request cancels
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if err := WarmUpTableSplits(ctx, tbl); err != nil {
			log.Printf("Async refresh failed: %v\n", err)
		} else {
			log.Printf("Async refresh complete.\n")
		}
	}()
}

// ReadParallelRowsKeys reads specific rows (max 100) using a fixed worker pool of 100.
func ReadParallelRowsKeys(ctx context.Context, tbl *bigtable.Table, rowKeys []string, f func(bigtable.Row) bool) error {
	if len(rowKeys) == 0 {
		return nil
	}

	sort.Strings(rowKeys)

	cacheMu.RLock()
	entry, exists := tableSampleCache[tbl]
	cacheMu.RUnlock()

	var splitKeys []string

	if !exists {
		triggerAsyncRefresh(tbl)
		splitKeys = nil
	} else {
		if time.Since(entry.fetchedAt) > 1*time.Hour {
			triggerAsyncRefresh(tbl)
		}
		splitKeys = entry.splitKeys
	}

	var batches []bigtable.RowList

	splitIdx := 0
	numSplits := len(splitKeys)
	start := 0

	if numSplits == 0 {
		// simple strategy for fanning out
		for i := 0; i < len(rowKeys); i++ {
			batches = append(batches, bigtable.RowList(rowKeys[i:i+1]))
		}
	} else {
		for i, key := range rowKeys {
			// If key extends beyond current tablet split
			for splitIdx < numSplits && key > splitKeys[splitIdx] {
				if i > start {
					batches = append(batches, bigtable.RowList(rowKeys[start:i]))
				}
				start = i
				splitIdx++
			}
		}
		// Append final batch
		if start < len(rowKeys) {
			batches = append(batches, bigtable.RowList(rowKeys[start:]))
		}
	}

	// 4. Execute with Worker Pool (Max 100 Goroutines)
	numBatches := len(batches)
	if numBatches == 0 {
		return nil
	}

	const maxWorkers = 100
	numWorkers := maxWorkers
	if numBatches < numWorkers {
		numWorkers = numBatches
	}

	jobs := make(chan bigtable.RowList, numBatches)
	for _, b := range batches {
		jobs <- b
	}
	close(jobs)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	var (
		firstErr error
		errMu    sync.Mutex
	)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for batch := range jobs {
				errMu.Lock()
				if firstErr != nil {
					errMu.Unlock()
					return
				}
				errMu.Unlock()

				if err := tbl.ReadRows(ctx, batch, f); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	return firstErr
}
