// Command client is a closed-loop-with-rate-target load generator for banyan.
//
// Bamboo shipped a key-value client that banyan's fork removed along with the
// whole request path. This replaces it with one shaped like the load
// generators the surrounding benchmark suite uses for other protocols: a
// fixed request size, a target send rate, a cap on outstanding requests, and
// client-side offered/delivered counters plus end-to-end latency.
//
// Requests are spread round-robin over the replicas' HTTP endpoints, which is
// what keeps every node's mempool fed. Since a node proposes only from its
// own pool, sending everything to one node would leave the other n-1 pools
// empty and measure something quite different.
//
// Output is a JSON summary on stdout (and to -out), so the harness can read
// it without parsing logs.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"banyan/config"
	"banyan/log"
)

var (
	rate      = flag.Float64("rate", 1000, "target send rate for this client process, requests/sec (0 = as fast as in-flight allows)")
	size      = flag.Int("size", 1024, "request payload size in bytes")
	duration  = flag.Int("duration", 30, "how long to send for, seconds")
	inFlight  = flag.Int("in-flight", 1000, "max outstanding requests")
	timeoutMs = flag.Int("timeout", 30000, "per-request timeout, milliseconds")
	warmup    = flag.Int("warmup", 3, "seconds to send before starting measurement")
	outPath   = flag.String("out", "", "write the JSON summary here as well as stdout")
	targets   = flag.String("targets", "", "comma-separated http addresses to send to (default: every node in the config)")
	// Not "transport": banyan's own transport package already registers a flag
	// by that name (the node's socket scheme), and a duplicate makes the flag
	// package panic at init before main runs.
	clientTransport = flag.String("client-transport", "tcp", "how to reach the nodes: `tcp` (one pipelined connection per node) or `http` (one connection per outstanding request)")
)

type sample struct {
	latency time.Duration
	ok      bool
}

// Outcome of a request, for the counters.
const (
	outcomeOK = iota
	outcomeFailed
	outcomeTimedOut
)

// measuredPrefix marks request ids issued inside the measurement window, so a
// reply arriving later can be attributed without a second map of ids.
const measuredPrefix = "m"

type summary struct {
	Targets        []string  `json:"targets"`
	Rate           float64   `json:"target_rate"`
	Size           int       `json:"size"`
	DurationS      float64   `json:"duration_s"`
	Sent           int64     `json:"sent"`
	Committed      int64     `json:"committed"`
	Failed         int64     `json:"failed"`
	TimedOut       int64     `json:"timed_out"`
	OfferedRate    float64   `json:"offered_rate"`
	DeliveredRate  float64   `json:"delivered_rate"`
	LatencyMeanMs  float64   `json:"latency_ms_mean"`
	LatencyP50Ms   float64   `json:"latency_ms_p50"`
	LatencyP90Ms   float64   `json:"latency_ms_p90"`
	LatencyP99Ms   float64   `json:"latency_ms_p99"`
	LatencySamples []float64 `json:"latency_ms_samples,omitempty"`
}

func main() {
	flag.Parse()
	log.Setup()
	// Only read the node config when we have to discover targets from it.
	// With -targets given, a client machine needs no config.json or ips.txt —
	// which is what lets clients run on VMs that hold no replica state.
	if *targets == "" {
		config.Configuration.Load()
	}

	addrs := resolveTargets()
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "no target addresses; pass -targets or provide ips.txt")
		os.Exit(1)
	}

	// One connection pool, sized to the in-flight cap so the client is never
	// the queue: without raising these, Go's default of 2 idle conns per host
	// serializes requests and the "rate" we report is the transport's, not
	// the protocol's. Only used by the http transport.
	httpTransport := &http.Transport{
		MaxIdleConns:        *inFlight * 2,
		MaxIdleConnsPerHost: *inFlight,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: httpTransport,
		Timeout:   time.Duration(*timeoutMs) * time.Millisecond,
	}

	payload := make([]byte, *size)
	if _, err := rand.New(rand.NewSource(time.Now().UnixNano())).Read(payload); err != nil {
		panic(err)
	}

	var (
		sent      int64
		committed int64
		failed    int64
		timedOut  int64
		mu        sync.Mutex
		samples   []sample
	)

	sem := make(chan struct{}, *inFlight)
	timeout := time.Duration(*timeoutMs) * time.Millisecond

	measureStart := time.Time{}
	var measuredSent int64

	// record is called once per request, from whichever path resolved it.
	// It releases the in-flight slot, so a reply (or an expiry) is what admits
	// the next request — that is the closed-loop part of the load model.
	record := func(id string, latency time.Duration, outcome int) {
		<-sem
		switch outcome {
		case outcomeOK:
			atomic.AddInt64(&committed, 1)
		case outcomeTimedOut:
			atomic.AddInt64(&timedOut, 1)
		default:
			atomic.AddInt64(&failed, 1)
		}
		if strings.HasPrefix(id, measuredPrefix) {
			mu.Lock()
			samples = append(samples, sample{latency: latency, ok: outcome == outcomeOK})
			mu.Unlock()
		}
	}

	var conns []*pipelinedConn
	if *clientTransport == "tcp" {
		onReply := func(id string, latency time.Duration, ok bool) {
			outcome := outcomeFailed
			if ok {
				outcome = outcomeOK
			}
			record(id, latency, outcome)
		}
		for _, addr := range addrs {
			target, err := clientAddr(addr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "bad target %q: %v\n", addr, err)
				os.Exit(1)
			}
			conn, err := dialPipelined(target, onReply)
			if err != nil {
				fmt.Fprintf(os.Stderr, "dial %s: %v\n", target, err)
				os.Exit(1)
			}
			conns = append(conns, conn)
		}
		// Reclaim slots held by requests whose replies never arrive, so one
		// lost reply cannot wedge the whole client behind the in-flight cap.
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for range ticker.C {
				for _, c := range conns {
					for i := 0; i < c.expire(timeout); i++ {
						// expire() already reported each id through onReply as
						// a failure; count them as timeouts instead.
						atomic.AddInt64(&failed, -1)
						atomic.AddInt64(&timedOut, 1)
					}
				}
			}
		}()
	}

	var wg sync.WaitGroup // http path only
	stop := time.After(time.Duration(*warmup+*duration) * time.Second)
	measureFrom := time.Now().Add(time.Duration(*warmup) * time.Second)

	// Ticker-free pacing: a fixed inter-request interval with a deadline that
	// advances by exactly that interval keeps the long-run rate on target
	// even when individual sends stall, without the burst a caught-up ticker
	// would emit.
	var interval time.Duration
	if *rate > 0 {
		interval = time.Duration(float64(time.Second) / *rate)
	}
	next := time.Now()
	seq := 0

loop:
	for {
		select {
		case <-stop:
			break loop
		default:
		}

		if interval > 0 {
			if d := time.Until(next); d > 0 {
				time.Sleep(d)
			}
			next = next.Add(interval)
		}

		if measureStart.IsZero() && !time.Now().Before(measureFrom) {
			measureStart = time.Now()
			measuredSent = 0
		}

		sem <- struct{}{}
		seq++
		measuring := !measureStart.IsZero()
		id := fmt.Sprintf("%d-%d", os.Getpid(), seq)
		if measuring {
			id = measuredPrefix + id
		}
		idx := seq % len(addrs)

		atomic.AddInt64(&sent, 1)
		if measuring {
			atomic.AddInt64(&measuredSent, 1)
		}

		if *clientTransport == "tcp" {
			// The send itself does not block on the reply: the connection is
			// pipelined and the reader goroutine resolves it later.
			if err := conns[idx].send(id, payload); err != nil {
				record(id, 0, outcomeFailed)
			}
			continue
		}

		wg.Add(1)
		go func(id, target string) {
			defer wg.Done()
			start := time.Now()
			ok, timedOutErr := send(client, target, id, payload)
			outcome := outcomeFailed
			switch {
			case ok:
				outcome = outcomeOK
			case timedOutErr:
				outcome = outcomeTimedOut
			}
			record(id, time.Since(start), outcome)
		}(id, addrs[idx])
	}

	if *clientTransport == "tcp" {
		// Drain: wait for outstanding replies, bounded by the request timeout
		// plus slack, then close. Without this the tail of the run — exactly
		// the requests that queued longest — would be missing from the sample.
		drainUntil := time.Now().Add(timeout + 5*time.Second)
		for time.Now().Before(drainUntil) {
			outstanding := 0
			for _, c := range conns {
				c.mu.Lock()
				outstanding += len(c.pending)
				c.mu.Unlock()
			}
			if outstanding == 0 {
				break
			}
			for _, c := range conns {
				for i := 0; i < c.expire(timeout); i++ {
					atomic.AddInt64(&failed, -1)
					atomic.AddInt64(&timedOut, 1)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
		for _, c := range conns {
			c.close()
		}
	} else {
		wg.Wait()
	}

	elapsed := time.Since(measureStart).Seconds()
	if measureStart.IsZero() || elapsed <= 0 {
		elapsed = float64(*duration)
	}

	report(addrs, samples, atomic.LoadInt64(&measuredSent), &sent, &committed,
		&failed, &timedOut, elapsed)
}

// clientAddr maps an http endpoint to the node's pipelined client port.
//
// The harness passes http targets (that is what the config knows), and the
// node derives every port from its 1-based index: http is 8069+i, the client
// listener is 5000+i. So the offset between them is constant.
func clientAddr(httpTarget string) (string, error) {
	u, err := url.Parse(httpTarget)
	if err != nil {
		return "", err
	}
	httpPort, err := strconv.Atoi(u.Port())
	if err != nil {
		return "", fmt.Errorf("no port in %q", httpTarget)
	}
	nodeIndex := httpPort - httpPortBase
	if nodeIndex <= 0 {
		return "", fmt.Errorf("port %d is not an http node port", httpPort)
	}
	return net.JoinHostPort(u.Hostname(), strconv.Itoa(clientPortBase+nodeIndex)), nil
}

// Must match config/config.go:Load and node.ClientPortBase.
const (
	httpPortBase   = 8069
	clientPortBase = 5000
)

// send posts one request and waits for the commit reply. Returns (ok,
// timedOut) so the caller can tell a saturated protocol from a broken one.
func send(client *http.Client, target, id string, payload []byte) (bool, bool) {
	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, target+"/request", bytes.NewReader(payload))
	if err != nil {
		return false, false
	}
	req.Header.Set("Cid", id)
	req.Header.Set("Content-Type", "application/octet-stream")
	// Disable net/http's automatic replay. A *bytes.Reader body makes the
	// request retryable, and the transport silently re-sends it when a
	// connection dies before the response — which under load delivers the same
	// request id to a node twice, inflating what the servers count as offered
	// load above what this client actually sent. A load generator must send
	// exactly what it reports.
	req.GetBody = nil

	resp, err := client.Do(req)
	if err != nil {
		return false, os.IsTimeout(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK, false
}

func resolveTargets() []string {
	if *targets != "" {
		var out []string
		for _, t := range splitComma(*targets) {
			out = append(out, t)
		}
		return out
	}
	var out []string
	for _, addr := range config.GetConfig().HTTPAddrs {
		out = append(out, addr)
	}
	// Map iteration order is random; sort so every client process spreads
	// load the same way and the round-robin is reproducible.
	sort.Strings(out)
	return out
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func report(addrs []string, samples []sample, measuredSent int64,
	sent, committed, failed, timedOut *int64, elapsed float64) {

	lat := make([]float64, 0, len(samples))
	var okCount int64
	for _, s := range samples {
		if s.ok {
			lat = append(lat, float64(s.latency.Microseconds())/1000)
			okCount++
		}
	}
	sort.Float64s(lat)

	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		idx := p * float64(len(lat)-1)
		lo := int(idx)
		hi := lo + 1
		if hi >= len(lat) {
			return lat[len(lat)-1]
		}
		return lat[lo] + (lat[hi]-lat[lo])*(idx-float64(lo))
	}
	mean := 0.0
	for _, v := range lat {
		mean += v
	}
	if len(lat) > 0 {
		mean /= float64(len(lat))
	}

	s := summary{
		Targets:       addrs,
		Rate:          *rate,
		Size:          *size,
		DurationS:     elapsed,
		Sent:          atomic.LoadInt64(sent),
		Committed:     atomic.LoadInt64(committed),
		Failed:        atomic.LoadInt64(failed),
		TimedOut:      atomic.LoadInt64(timedOut),
		OfferedRate:   float64(measuredSent) / elapsed,
		DeliveredRate: float64(okCount) / elapsed,
		LatencyMeanMs: mean,
		LatencyP50Ms:  pct(0.50),
		LatencyP90Ms:  pct(0.90),
		LatencyP99Ms:  pct(0.99),
	}
	// Keep a bounded sample of the raw latencies so the harness can rebuild a
	// CDF without shipping every request.
	if len(lat) > 0 {
		stride := 1
		if len(lat) > 10000 {
			stride = len(lat) / 10000
		}
		for i := 0; i < len(lat); i += stride {
			s.LatencySamples = append(s.LatencySamples, lat[i])
		}
	}

	out, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
	if *outPath != "" {
		if err := os.WriteFile(*outPath, out, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "could not write %s: %v\n", *outPath, err)
		}
	}
}
