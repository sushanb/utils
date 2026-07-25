//go:build ignore

package main

// package main

// import (
// 	"context"
// 	"log"
// 	"os"
// 	"os/signal"
// 	"syscall"

// 	"cloud.google.com/go/bigtable"
// )

// func main() {
// 	// 1. Create a context that listens for OS interrupt signals (like Ctrl+C)
// 	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
// 	defer stop() // Restore default signal behavior when the function exits

// 	// 2. Define your specific Bigtable parameters
// 	projectID := "autonomous-mote-782"
// 	instanceID := "sushanb-eu-w4"
// 	appProfile := "default"

// 	// C2P target and concurrency level
// 	target := "bigtable.googleapis.com"
// 	requestsInFlight := 20

// 	log.Printf("Starting traffic generator for %s/%s. Press Ctrl+C to stop.", projectID, instanceID)

// 	// 3. Call your function
// 	// This call blocks and maintains exactly 200 active streams
// 	err := bigtable.CallSingleChannel(
// 		ctx,
// 		projectID,
// 		instanceID,
// 		appProfile,
// 		target,
// 		requestsInFlight,
// 	)

// 	// 4. Handle the exit condition gracefully
// 	if err != nil && err != context.Canceled {
// 		log.Fatalf("Process terminated with an unexpected error: %v", err)
// 	}

// 	log.Println("Successfully shut down all workers.")
// }
