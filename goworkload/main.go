package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
	channelz "github.com/rantav/go-grpc-channelz"
	"golang.org/x/time/rate"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	channelzservice "google.golang.org/grpc/channelz/service"
)

const (
	overallRequestDuration = 10 * time.Second
)

// Use atomic counters instead of logging every request
var (
	successCount uint64
	errorCount   uint64
	timeoutCount uint64
	rowCounter   uint64 // Fast, lock-free counter for unique row keys
)

func delegatorPingAndWarm(ctx context.Context, client *bigtable.Client) error {
	return client.PingAndWarm(ctx)
}

func delegatorMutateRow(ctx context.Context, tbl bigtable.TableAPI) error {
	// Generate a highly random, collision-free row key without blocking on math/rand locks
	// Format: "row-<timestamp>-<atomic_increment>"
	rowKey := fmt.Sprintf("row-%d-%d", time.Now().UnixNano(), atomic.AddUint64(&rowCounter, 1))

	mut := bigtable.NewMutation()
	// Set data in column family "cf", column "col1"
	mut.Set("cf", "col1", bigtable.Now(), []byte("load-test-data"))

	return tbl.Apply(ctx, rowKey, mut)
}

func runWorkload(ctx context.Context, client *bigtable.Client, tbl bigtable.TableAPI, cbtOp string) {
	reqCtx, reqCancel := context.WithTimeout(ctx, overallRequestDuration)
	defer reqCancel()

	// Run the request synchronously within the worker goroutine
	// to avoid massive goroutine churn and channel allocations.
	errChan := make(chan error, 1)
	go func() {
		if cbtOp == "Mutate" {
			errChan <- delegatorMutateRow(reqCtx, tbl)
		} else {
			errChan <- delegatorPingAndWarm(reqCtx, client)
		}
	}()

	select {
	case err := <-errChan:
		if err != nil {
			atomic.AddUint64(&errorCount, 1)
		} else {
			atomic.AddUint64(&successCount, 1)
		}
	case <-reqCtx.Done():
		atomic.AddUint64(&timeoutCount, 1)
	}
}

func main() {
	var (
		projectIDPtr  = flag.String("project", "", "Your Google Cloud Project ID (required)")
		instanceIDPtr = flag.String("instance", "", "Your Bigtable Instance ID (required)")
		pprofPortPtr  = flag.Int("pprof-port", 6060, "The port for the pprof HTTP server")
	)

	flag.Parse()

	if *projectIDPtr == "" || *instanceIDPtr == "" {
		fmt.Println("Error: All project, instance are required.")
		flag.Usage()
		return
	}

	projectID := *projectIDPtr
	instanceID := *instanceIDPtr
	pprofAddr := fmt.Sprintf(":%d", *pprofPortPtr)
	env := os.Getenv("CBT_ENV_VAR")

	cbtOp := os.Getenv("CBT_OP")
	if cbtOp == "" {
		cbtOp = "PingAndWarm" // Default fallback
	}

	// Parse TARGET_RPS from environment variables
	targetRPS := 100 // Default fallback
	if envRPS := os.Getenv("TARGET_QPS"); envRPS != "" {
		if parsedRPS, err := strconv.Atoi(envRPS); err == nil && parsedRPS > 0 {
			targetRPS = parsedRPS
		} else {
			log.Printf("Warning: Invalid TARGET_RPS value '%s', falling back to default %d", envRPS, targetRPS)
		}
	}

	// --- NEW: Parse NUM_WORKERS from environment variables ---
	numWorkers := 50 // Default fallback
	if envWorkers := os.Getenv("NUM_WORKERS"); envWorkers != "" {
		if parsedWorkers, err := strconv.Atoi(envWorkers); err == nil && parsedWorkers > 0 {
			numWorkers = parsedWorkers
		} else {
			log.Printf("Warning: Invalid NUM_WORKERS value '%s', falling back to default %d", envWorkers, numWorkers)
		}
	}

	go func() {
		log.Printf("Starting pprof server on http://localhost%s/debug/pprof/", pprofAddr)
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			log.Printf("pprof server failed: %v", err)
		}
	}()

	go func() {
		grpcServer := grpc.NewServer()
		http.Handle("/", channelz.CreateHandler("/", ":8001"))
		channelzservice.RegisterChannelzServiceToServer(grpcServer)

		channelListener, err := net.Listen("tcp", ":8001")
		if err != nil {
			log.Fatal(err)
		}
		go grpcServer.Serve(channelListener)

		adminListener, err := net.Listen("tcp", ":8082")
		if err != nil {
			log.Fatal(err)
		}
		go http.Serve(adminListener, nil)
	}()

	ctx := context.Background()

	var opts []option.ClientOption
	switch env {
	case "prod":
		opts = append(opts, option.WithEndpoint("bigtable.googleapis.com:443"))
	case "staging":
		opts = append(opts, option.WithEndpoint("staging-bigtable.sandbox.googleapis.com:443"))
	default:
		opts = append(opts, option.WithEndpoint("test-bigtable.sandbox.googleapis.com:443"))
	}

	var configs bigtable.ClientConfig
	configs.AppProfile = "default"
	if appProfile := os.Getenv("APP_PROFILE"); appProfile != "" {
		configs.AppProfile = appProfile
	}

	client, err := bigtable.NewClientWithConfig(ctx, projectID, instanceID, configs, opts...)
	if err != nil {
		log.Fatalf("bigtable.NewClient: %v", err)
	}
	defer client.Close()

	log.Printf("Client created. Starting workload with target QPS: %d across %d workers", targetRPS, numWorkers)

	tbl := client.OpenTable("sushanb")

	// Use a Token Bucket Rate Limiter with dynamic RPS
	limiter := rate.NewLimiter(rate.Limit(targetRPS), targetRPS)

	var wg sync.WaitGroup

	// Start a periodic metrics logger
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			s := atomic.SwapUint64(&successCount, 0)
			e := atomic.SwapUint64(&errorCount, 0)
			t := atomic.SwapUint64(&timeoutCount, 0)
			log.Printf("Stats/sec -> Success: %d | Errors: %d | Timeouts: %d", s, e, t)
		}
	}()

	// Start Worker Pool
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				runWorkload(ctx, client, tbl, cbtOp)
			}
		}()
	}

	wg.Wait()
}
