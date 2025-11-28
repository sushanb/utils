package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"net/http"
	_ "net/http/pprof" // Required to register pprof handlers on the default mux

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
)

const (
	sleepSeconds     = 10
	defaultPprofPort = 6060 // Standard port for pprof
	// This struct is ~100 bytes (8+8+8+8+64), so 100,000 instances is ~10MB per tick.
	ALLOCATION_COUNT = 100000
)

// Data structure to hold in memory
type HeapObject struct {
	ID        uint64
	Timestamp int64
	Value     float64
	Padding   [64]byte // Use padding to ensure size is easily visible
}

// Global slice to intentionally hold the allocated memory, preventing garbage collection.
var memoryHog = make([]HeapObject, 0)

func main() {

	var (
		projectIDPtr  = flag.String("project", "", "Your Google Cloud Project ID (required)")
		instanceIDPtr = flag.String("instance", "", "Your Bigtable Instance ID (required)")
		pprofPortPtr  = flag.Int("pprof-port", defaultPprofPort, "The port for the pprof HTTP server")
	)

	// 2. Parse the flags
	flag.Parse()

	// 3. Validate required flags
	if *projectIDPtr == "" || *instanceIDPtr == "" {
		fmt.Println("Error: All project, instance are required.")
		flag.Usage() // Print usage instructions
		return
	}
	// Assign flag values to local variables for cleaner use
	projectID := *projectIDPtr
	instanceID := *instanceIDPtr
	pprofAddr := fmt.Sprintf(":%d", *pprofPortPtr)

	runtime.SetBlockProfileRate(1)
	runtime.SetCPUProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	// 4. Start pprof HTTP server in a goroutine
	go func() {
		log.Printf("Starting pprof server on http://localhost%s/debug/pprof/", pprofAddr)
		// The imported _ "net/http/pprof" registers the profiling handlers
		// on the default HTTP mux. Passing nil uses this default mux.
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			// Note: This error only happens if the server fails to start (e.g., port in use)
			log.Printf("pprof server failed: %v", err)
		}
	}()

	// Create a context
	ctx := context.Background()

	var opts []option.ClientOption
	opts = append(opts, option.WithEndpoint("test-bigtable.sandbox.googleapis.com:443"))

	client, err := bigtable.NewClient(ctx, projectID, instanceID, opts...)
	if err != nil {
		log.Fatalf("bigtable.NewClient: %v", err)
	}
	defer client.Close()

	log.Printf("Client created for Project: %s, Instance: %s", projectID, instanceID)

	// Create a ticker to manage the 10-second interval
	ticker := time.NewTicker(time.Duration(sleepSeconds) * time.Second)
	defer ticker.Stop()

	// 3. Main loop to send requests
	for i := 0; ; i++ {
		<-ticker.C // Wait for the tick
		// Create a large slice and append it to the global memoryHog
		// newAllocations := make([]HeapObject, ALLOCATION_COUNT)
		// for j := 0; j < ALLOCATION_COUNT; j++ {
		// 	// Populate the struct to simulate real data
		// 	newAllocations[j] = HeapObject{
		// 		ID:        uint64(i*ALLOCATION_COUNT + j),
		// 		Timestamp: time.Now().Unix(),
		// 		Value:     float64(i) + float64(j)*0.01,
		// 	}
		// }
		// Appending to the global slice ensures the memory is NOT garbage collected.
		// memoryHog = append(memoryHog, newAllocations...)

		// Apply the mutation to the row (MutateRow equivalent)
		err = client.PingAndWarm(ctx)
		if err != nil {
			// Log the error but don't stop the program for a single failed request
			log.Printf("ERROR: PingAndWarm failed", err)
		} else {
			log.Printf("SUCCESS")
		}
	}
}
