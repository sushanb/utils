package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/debugview"

	"golang.org/x/time/rate"
	"google.golang.org/api/option"
)

var requestTimeout = 25 * time.Millisecond

const rowKeySpace = 2_000_000

type counters struct {
	success  uint64
	errors   uint64
	timeouts uint64
}

var (
	rowCounter  uint64
	readCounter uint64
)

func (c *counters) record(err error) {
	switch {
	case err == nil:
		atomic.AddUint64(&c.success, 1)
	case errors.Is(err, context.DeadlineExceeded):
		atomic.AddUint64(&c.timeouts, 1)
	default:
		atomic.AddUint64(&c.errors, 1)
	}
}

func (c *counters) snapshot() (success, errs, timeouts uint64) {
	return atomic.SwapUint64(&c.success, 0),
		atomic.SwapUint64(&c.errors, 0),
		atomic.SwapUint64(&c.timeouts, 0)
}

func mutateRow(ctx context.Context, tbl bigtable.TableAPI) error {
	n := atomic.AddUint64(&rowCounter, 1) % rowKeySpace
	mut := bigtable.NewMutation()
	mut.Set("cf12", "col1", bigtable.Now(), []byte(fmt.Sprintf("val-worker-%d", n)))
	return tbl.Apply(ctx, fmt.Sprintf("myrow-%d", n), mut)
}

func readRow(ctx context.Context, tbl bigtable.TableAPI) error {
	n := atomic.AddUint64(&readCounter, 1) % rowKeySpace
	_, err := tbl.ReadRow(ctx, fmt.Sprintf("myrow-%d", n), bigtable.RowFilter(bigtable.LatestNFilter(1)))

	return err
}

// modulator returns a time-varying QPS oscillating between peak*minRatio and peak.
// Patterns:
//   sine — smooth cosine, max at t=0 (with phase offset applied).
//   step — square wave: low for the first half of each period, high for the second half.
//   flat — no modulation; always returns peak (preserves legacy behavior).
type modulator struct {
	peak     float64
	minRatio float64
	pattern  string
	period   time.Duration
	phase    time.Duration
	start    time.Time
}

func (m *modulator) qps(now time.Time) float64 {
	if m.pattern == "flat" || m.period <= 0 {
		return m.peak
	}
	elapsed := now.Sub(m.start) + m.phase
	frac := math.Mod(float64(elapsed)/float64(m.period), 1.0)
	if frac < 0 {
		frac += 1
	}
	var ratio float64
	switch m.pattern {
	case "step":
		if frac < 0.5 {
			ratio = m.minRatio
		} else {
			ratio = 1.0
		}
	default: // sine
		amp := (1.0 - m.minRatio) / 2.0
		mid := (1.0 + m.minRatio) / 2.0
		ratio = mid + amp*math.Cos(2*math.Pi*frac)
	}
	return m.peak * ratio
}

func runDriver(ctx context.Context, wg *sync.WaitGroup, label string, mod *modulator, retune time.Duration, workers int, c *counters, op func(context.Context) error) {
	initial := math.Max(1, mod.qps(time.Now()))
	limiter := rate.NewLimiter(rate.Limit(initial), int(initial))
	log.Printf("[%s] starting %d workers | peak=%.0f QPS pattern=%s period=%s phase=%s minRatio=%.2f retune=%s",
		label, workers, mod.peak, mod.pattern, mod.period, mod.phase, mod.minRatio, retune)

	// Retune loop: rewrites the limiter's rate & burst on each tick.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if mod.pattern == "flat" || mod.period <= 0 {
			return
		}
		ticker := time.NewTicker(retune)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				q := math.Max(1, mod.qps(now))
				limiter.SetLimit(rate.Limit(q))
				limiter.SetBurst(int(q))
				log.Printf("[%s] retune qps=%.0f", label, q)
			}
		}
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
				c.record(op(reqCtx))
				cancel()
			}
		}()
	}
}

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("Warning: invalid %s=%q, falling back to %d", name, v, def)
		return def
	}
	return n
}

func envDuration(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Printf("Warning: invalid %s=%q, falling back to %s", name, v, def)
		return def
	}
	return d
}

func envFloat(name string, def, minVal, maxVal float64) float64 {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Printf("Warning: invalid %s=%q, falling back to %g", name, v, def)
		return def
	}
	if f < minVal {
		f = minVal
	}
	if f > maxVal {
		f = maxVal
	}
	return f
}

func envString(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func endpointFor(env string) string {
	switch env {
	case "prod":
		return "bigtable.googleapis.com:443"
	case "staging":
		return "staging-bigtable.sandbox.googleapis.com:443"
	default:
		return "test-bigtable.sandbox.googleapis.com:443"
	}
}

func main() {
	var (
		projectID    = flag.String("project", "", "Google Cloud project ID (required)")
		instanceID   = flag.String("instance", "", "Bigtable instance ID (required)")
		tableID      = flag.String("table", "sushanb", "Bigtable table ID")
		pprofPort    = flag.Int("pprof-port", 6060, "Port for the pprof HTTP server")
		sessionzPort = flag.Int("sessionz-port", 8082, "Port for the bigtable sessionz debug UI")
	)
	flag.Parse()

	if *projectID == "" || *instanceID == "" {
		fmt.Println("Error: -project and -instance are required.")
		flag.Usage()
		os.Exit(2)
	}

	// READ_QPS / WRITE_QPS are now interpreted as PEAK QPS.
	readQPS := envInt("READ_QPS", 100)
	writeQPS := envInt("WRITE_QPS", 100)
	readWorkers := envInt("READ_WORKERS", 25)
	writeWorkers := envInt("WRITE_WORKERS", 25)

	appProfile := os.Getenv("APP_PROFILE")

	requestTimeout = time.Duration(envInt("REQUEST_TIMEOUT_MS", 25)) * time.Millisecond
	if appProfile == "" {
		appProfile = "default"
	}

	// Diurnal / oscillation knobs.
	cyclePeriod := envDuration("CYCLE_PERIOD", 10*time.Minute)
	minRatio := envFloat("MIN_QPS_RATIO", 0.1, 0.01, 1.0)
	pattern := envString("WORKLOAD_PATTERN", "sine")
	retune := envDuration("RETUNE_INTERVAL", 5*time.Second)
	writePhase := envDuration("WRITE_PHASE_OFFSET", cyclePeriod/2)

	// pprof server.
	go func() {
		addr := fmt.Sprintf(":%d", *pprofPort)
		log.Printf("pprof listening on http://localhost%s/debug/pprof/", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("pprof server exited: %v", err)
		}
	}()

	ctx := context.Background()

	configs := bigtable.ClientConfig{
		AppProfile:        appProfile,
		EnableSessionPool: true,
		EnableClientDebug: true, // add
	}
	tcpStats := bigtable.NewTCPStats()
	opts := []option.ClientOption{
		option.WithEndpoint(endpointFor(os.Getenv("CBT_ENV_VAR"))),
		tcpStats.ClientOption(),
	}

	client, err := bigtable.NewClientWithConfig(ctx, *projectID, *instanceID, configs, opts...)
	if err != nil {
		log.Fatalf("bigtable.NewClient: %v", err)
	}
	defer client.Close()

	// sessionz debug UI (separate mux so it doesn't share the pprof handler).
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/debug/", http.StripPrefix("/debug", debugview.Handler(client, tcpStats)))
		addr := fmt.Sprintf(":%d", *sessionzPort)
		log.Printf("bigtable debug listening on http://localhost%s/debug/{sessionz,channelz,configz}/", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("debug server exited: %v", err)
		}
	}()

	tableName := *tableID
	if v := os.Getenv("TABLE"); v != "" {
		tableName = v
	}
	tbl := client.OpenTable(tableName)

	var (
		readCounters  counters
		writeCounters counters
		wg            sync.WaitGroup
	)

	// Per-second stats logger.
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rs, re, rt := readCounters.snapshot()
				ws, we, wt := writeCounters.snapshot()
				log.Printf("READ ok=%d err=%d timeout=%d | WRITE ok=%d err=%d timeout=%d",
					rs, re, rt, ws, we, wt)
			}
		}
	}()

	start := time.Now()
	readMod := &modulator{
		peak: float64(readQPS), minRatio: minRatio, pattern: pattern,
		period: cyclePeriod, phase: 0, start: start,
	}
	writeMod := &modulator{
		peak: float64(writeQPS), minRatio: minRatio, pattern: pattern,
		period: cyclePeriod, phase: writePhase, start: start,
	}

	runDriver(ctx, &wg, "READ", readMod, retune, readWorkers, &readCounters, func(c context.Context) error {
		return readRow(c, tbl)
	})
	runDriver(ctx, &wg, "WRITE", writeMod, retune, writeWorkers, &writeCounters, func(c context.Context) error {
		return mutateRow(c, tbl)
	})

	wg.Wait()
}
