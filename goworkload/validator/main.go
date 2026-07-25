package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/debugview"

	"golang.org/x/time/rate"
	"google.golang.org/api/option"
)

const (
	requestTimeout = 1 * time.Second
	rowKeySpace    = 2_000_000
)

// counters holds cumulative totals across the process lifetime; readers
// use atomic.LoadUint64. `sent` is incremented once per attempted
// validation; the other fields are terminal outcomes and are mutually
// exclusive per attempt.
type counters struct {
	sent             uint64
	match            uint64
	mismatch         uint64
	deadlineExceeded uint64
	errors           uint64
}

var readCounter uint64

type stats struct {
	Sent             uint64
	Match            uint64
	Mismatch         uint64
	DeadlineExceeded uint64
	Errors           uint64
}

func (c *counters) load() stats {
	return stats{
		Sent:             atomic.LoadUint64(&c.sent),
		Match:            atomic.LoadUint64(&c.match),
		Mismatch:         atomic.LoadUint64(&c.mismatch),
		DeadlineExceeded: atomic.LoadUint64(&c.deadlineExceeded),
		Errors:           atomic.LoadUint64(&c.errors),
	}
}

// validateRow reads the same row via classic and session clients and
// asserts byte-for-byte parity of the returned Row.
func validateRow(ctx context.Context, classicTbl, sessionTbl bigtable.TableAPI, c *counters) {
	atomic.AddUint64(&c.sent, 1)

	n := atomic.AddUint64(&readCounter, 1) % rowKeySpace
	rowKey := fmt.Sprintf("myrow-%d", n)

	classicCtx, cancelC := context.WithTimeout(ctx, requestTimeout)
	classicRow, cErr := classicTbl.ReadRow(classicCtx, rowKey, bigtable.RowFilter(bigtable.LatestNFilter(1)))
	cancelC()

	sessionCtx, cancelS := context.WithTimeout(ctx, requestTimeout)
	sessionRow, sErr := sessionTbl.ReadRow(sessionCtx, rowKey, bigtable.RowFilter(bigtable.LatestNFilter(1)))
	cancelS()

	if cErr != nil || sErr != nil {
		// Either side failed — classify by whichever error we saw first.
		firstErr := cErr
		if firstErr == nil {
			firstErr = sErr
		}
		if errors.Is(firstErr, context.DeadlineExceeded) {
			atomic.AddUint64(&c.deadlineExceeded, 1)
		} else {
			atomic.AddUint64(&c.errors, 1)
		}
		return
	}

	if !rowsEqual(classicRow, sessionRow) {
		atomic.AddUint64(&c.mismatch, 1)
		log.Printf("MISMATCH row=%q\n  classic=%s\n  session=%s", rowKey, formatRow(classicRow), formatRow(sessionRow))
		return
	}
	atomic.AddUint64(&c.match, 1)
}

func rowsEqual(a, b bigtable.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for fam, itemsA := range a {
		itemsB, ok := b[fam]
		if !ok || len(itemsA) != len(itemsB) {
			return false
		}
		sa := sortedCopy(itemsA)
		sb := sortedCopy(itemsB)
		for i := range sa {
			if sa[i].Row != sb[i].Row ||
				sa[i].Column != sb[i].Column ||
				sa[i].Timestamp != sb[i].Timestamp ||
				!bytes.Equal(sa[i].Value, sb[i].Value) {
				return false
			}
		}
	}
	return true
}

func sortedCopy(items []bigtable.ReadItem) []bigtable.ReadItem {
	out := make([]bigtable.ReadItem, len(items))
	copy(out, items)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Column != out[j].Column {
			return out[i].Column < out[j].Column
		}
		if out[i].Timestamp != out[j].Timestamp {
			return out[i].Timestamp < out[j].Timestamp
		}
		return bytes.Compare(out[i].Value, out[j].Value) < 0
	})
	return out
}

func formatRow(r bigtable.Row) string {
	if r == nil {
		return "<nil>"
	}
	var b bytes.Buffer
	fams := make([]string, 0, len(r))
	for fam := range r {
		fams = append(fams, fam)
	}
	sort.Strings(fams)
	for _, fam := range fams {
		for _, item := range sortedCopy(r[fam]) {
			fmt.Fprintf(&b, "[%s ts=%d %q] ", item.Column, item.Timestamp, item.Value)
		}
	}
	return b.String()
}

func runDriver(ctx context.Context, wg *sync.WaitGroup, qps, workers int, op func(context.Context)) {
	limiter := rate.NewLimiter(rate.Limit(qps), qps)
	log.Printf("[VALIDATE] starting %d workers at %d QPS", workers, qps)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				op(ctx)
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

func newClient(ctx context.Context, projectID, instanceID, appProfile string, enableSession bool, opts ...option.ClientOption) (*bigtable.Client, error) {
	cfg := bigtable.ClientConfig{
		AppProfile:        appProfile,
		EnableSessionPool: enableSession,
	}
	return bigtable.NewClientWithConfig(ctx, projectID, instanceID, cfg, opts...)
}

var statusTmpl = template.Must(template.New("status").Parse(`<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="1">
<title>Bigtable Validation</title>
<style>
  body { font-family: -apple-system, sans-serif; background: #0d1117; color: #e6edf3; padding: 2rem; }
  h1 { font-weight: 400; letter-spacing: .02em; }
  table { border-collapse: collapse; margin-top: 1rem; }
  td { padding: .5rem 1.5rem .5rem 0; font-size: 1.4rem; }
  td.label { color: #8b949e; font-size: 1rem; text-transform: uppercase; letter-spacing: .1em; }
  td.value { font-variant-numeric: tabular-nums; }
  td.value.match { color: #56d364; }
  td.value.mismatch { color: #f85149; }
  td.value.deadline { color: #d29922; }
  td.value.errors { color: #f85149; }
  .footer { color: #6e7681; margin-top: 2rem; font-size: .8rem; }
</style>
</head>
<body>
<h1>Bigtable Validation Worker</h1>
<table>
  <tr><td class="label">Requests sent</td><td class="value">{{.Sent}}</td></tr>
  <tr><td class="label">Match</td><td class="value match">{{.Match}}</td></tr>
  <tr><td class="label">Mismatch</td><td class="value mismatch">{{.Mismatch}}</td></tr>
  <tr><td class="label">Deadline exceeded</td><td class="value deadline">{{.DeadlineExceeded}}</td></tr>
  <tr><td class="label">Other errors</td><td class="value errors">{{.Errors}}</td></tr>
</table>
<div class="footer">Auto-refresh 1s &middot; per-request deadline 1s.</div>
</body>
</html>`))

func serveStatus(port int, c *counters) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := statusTmpl.Execute(w, c.load()); err != nil {
			log.Printf("status template: %v", err)
		}
	})
	mux.HandleFunc("/stats.json", func(w http.ResponseWriter, r *http.Request) {
		s := c.load()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"sent":%d,"match":%d,"mismatch":%d,"deadline_exceeded":%d,"errors":%d}`,
			s.Sent, s.Match, s.Mismatch, s.DeadlineExceeded, s.Errors)
	})
	addr := fmt.Sprintf(":%d", port)
	log.Printf("validation status UI listening on http://localhost%s/", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("status server exited: %v", err)
	}
}

func main() {
	var (
		projectID           = flag.String("project", "", "Google Cloud project ID (required)")
		instanceID          = flag.String("instance", "", "Bigtable instance ID (required)")
		tableID             = flag.String("table", "sushanb", "Bigtable table ID")
		pprofPort           = flag.Int("pprof-port", 6060, "Port for the pprof HTTP server")
		statusPort          = flag.Int("status-port", 8080, "Port for the validation status UI")
		classicSessionzPort = flag.Int("classic-sessionz-port", 8082, "sessionz debug UI port for the classic client")
		sessionSessionzPort = flag.Int("session-sessionz-port", 8083, "sessionz debug UI port for the session client")
	)
	flag.Parse()

	if *projectID == "" || *instanceID == "" {
		fmt.Println("Error: -project and -instance are required.")
		flag.Usage()
		os.Exit(2)
	}

	readQPS := envInt("READ_QPS", 100)
	readWorkers := envInt("READ_WORKERS", 25)

	appProfile := os.Getenv("APP_PROFILE")
	if appProfile == "" {
		appProfile = "default"
	}

	go func() {
		addr := fmt.Sprintf(":%d", *pprofPort)
		log.Printf("pprof listening on http://localhost%s/debug/pprof/", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("pprof server exited: %v", err)
		}
	}()

	ctx := context.Background()
	endpoint := endpointFor(os.Getenv("CBT_ENV_VAR"))

	classicTCP := bigtable.NewTCPStats()
	classicOpts := []option.ClientOption{
		option.WithEndpoint(endpoint),
		classicTCP.ClientOption(),
	}
	classicClient, err := newClient(ctx, *projectID, *instanceID, appProfile, false, classicOpts...)
	if err != nil {
		log.Fatalf("classic bigtable.NewClient: %v", err)
	}
	defer classicClient.Close()

	sessionTCP := bigtable.NewTCPStats()
	sessionOpts := []option.ClientOption{
		option.WithEndpoint(endpoint),
		sessionTCP.ClientOption(),
	}
	sessionClient, err := newClient(ctx, *projectID, *instanceID, appProfile, true, sessionOpts...)
	if err != nil {
		log.Fatalf("session bigtable.NewClient: %v", err)
	}
	defer sessionClient.Close()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/debug/", http.StripPrefix("/debug", debugview.Handler(classicClient, classicTCP)))
		addr := fmt.Sprintf(":%d", *classicSessionzPort)
		log.Printf("classic bigtable debug listening on http://localhost%s/debug/", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("classic debug server exited: %v", err)
		}
	}()
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/debug/", http.StripPrefix("/debug", debugview.Handler(sessionClient, sessionTCP)))
		addr := fmt.Sprintf(":%d", *sessionSessionzPort)
		log.Printf("session bigtable debug listening on http://localhost%s/debug/", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("session debug server exited: %v", err)
		}
	}()

	tableName := *tableID
	if v := os.Getenv("TABLE"); v != "" {
		tableName = v
	}
	classicTbl := classicClient.OpenTable(tableName)
	sessionTbl := sessionClient.OpenTable(tableName)

	var (
		validateCounters counters
		wg               sync.WaitGroup
	)

	go serveStatus(*statusPort, &validateCounters)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var prev stats
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cur := validateCounters.load()
				log.Printf("VALIDATE sent=%d match=%d mismatch=%d deadline=%d err=%d (Δ sent=%d match=%d mismatch=%d deadline=%d err=%d)",
					cur.Sent, cur.Match, cur.Mismatch, cur.DeadlineExceeded, cur.Errors,
					cur.Sent-prev.Sent, cur.Match-prev.Match, cur.Mismatch-prev.Mismatch,
					cur.DeadlineExceeded-prev.DeadlineExceeded, cur.Errors-prev.Errors)
				prev = cur
			}
		}
	}()

	runDriver(ctx, &wg, readQPS, readWorkers, func(c context.Context) {
		validateRow(c, classicTbl, sessionTbl, &validateCounters)
	})

	wg.Wait()
}
