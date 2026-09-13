package replica

import (
	"math/rand"
	"sync"
	"time"

	"banyan/config"
	"banyan/identity"
	"banyan/log"
	"banyan/message"
)

// waitStats summarizes a delay distribution without shipping every sample.
//
// A run can commit millions of requests, so the /query report carries count,
// sum and max plus a bounded reservoir the harness turns into percentiles.
// Reservoir sampling (rather than "keep the first N") matters here: the
// interesting tail is the backlog late in a run, and a prefix sample would
// miss it entirely.
type waitStats struct {
	count   int64
	sumMs   int64
	maxMs   int64
	sample  []int64
	rand    *rand.Rand
	maxSize int
}

func newWaitStats(size int, r *rand.Rand) *waitStats {
	return &waitStats{sample: make([]int64, 0, size), maxSize: size, rand: r}
}

func (w *waitStats) add(d time.Duration) {
	ms := d.Milliseconds()
	w.count++
	w.sumMs += ms
	if ms > w.maxMs {
		w.maxMs = ms
	}
	if len(w.sample) < w.maxSize {
		w.sample = append(w.sample, ms)
		return
	}
	// Replace with probability maxSize/count, giving every observation an
	// equal chance of being in the final sample.
	if j := w.rand.Int63n(w.count); j < int64(w.maxSize) {
		w.sample[j] = ms
	}
}

func (w *waitStats) meanMs() float64 {
	if w.count == 0 {
		return 0
	}
	return float64(w.sumMs) / float64(w.count)
}

// pendingRequests tracks requests this node accepted from clients and has not
// answered yet. Only the receiving node holds a request's reply channel — gob
// drops chan fields when a block travels — so this map is what lets a commit
// find the client waiting on it.
type pendingRequests struct {
	mu sync.Mutex
	m  map[string]*message.Request
}

func newPendingRequests() *pendingRequests {
	return &pendingRequests{m: make(map[string]*message.Request)}
}

func (p *pendingRequests) add(req *message.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.m[req.ID] = req
}

// take removes and returns a request if this node is the one holding it.
func (p *pendingRequests) take(id string) (*message.Request, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	req, ok := p.m[id]
	if ok {
		delete(p.m, id)
	}
	return req, ok
}

func (p *pendingRequests) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.m)
}

// handleRequest admits a client request into this node's mempool.
//
// No forwarding to the current leader: a node proposes only what it holds, so
// the request waits here until this node's turn to propose comes round. That
// wait is what proposeWait measures.
func (r *Replica) handleRequest(req message.Request) {
	r.startSignal()
	if !r.clientDriven {
		req.Reply(message.RequestReply{ID: req.ID, Err: "node is not running the client workload"})
		return
	}
	// Verify before admitting, so an unauthenticated request never reaches the
	// mempool and can never be proposed. This is the arrival-side half of the
	// per-request work; the other half is every peer re-verifying it out of the
	// block that carries it (message.VerifyRequestPayload). Together they cost
	// n verifies per request across the committee, which is what aspen and
	// PBFT pay.
	if !req.Verify() {
		r.requestsRejected++
		req.Reply(message.RequestReply{ID: req.ID, Err: "invalid client signature"})
		return
	}
	// Store the addressable copy so the reply channel survives; the mempool
	// stamps arrival on this same struct.
	held := &req
	if !r.pool.Add(held) {
		req.Reply(message.RequestReply{ID: req.ID, Err: "mempool full"})
		return
	}
	r.pending.add(held)
}

// buildPayload produces the bytes for the next block this node proposes.
//
// Client workload: drain the mempool up to the per-block byte budget and
// record how long each drained request waited. An empty pool yields an empty
// block — the budget is a cap, not a target, so an underfull block simply
// commits fewer requests rather than being padded with bytes nobody asked to
// send.
//
// Generated workload: the fork's original behaviour, random bytes sized by
// payload_size.
func (r *Replica) buildPayload() []byte {
	if !r.clientDriven {
		payload := make([]byte, r.oneBlockPayloadBytes)
		if _, err := r.payloadRand.Read(payload); err != nil {
			log.Errorf("[%v] could not generate payload: %v", r.ID(), err)
		}
		return payload
	}

	batch := r.pool.Drain(r.oneBlockPayloadBytes)
	if len(batch) == 0 {
		return nil
	}
	now := time.Now()
	if r.inMeasurementWindow() {
		for _, req := range batch {
			r.proposeWait.add(now.Sub(req.Arrival))
		}
	}
	payload, err := message.EncodeRequests(batch)
	if err != nil {
		log.Errorf("[%v] could not encode %d request(s): %v", r.ID(), len(batch), err)
		return nil
	}
	return payload
}

// answerCommitted replies to the clients whose requests are in a committed
// block, and records how long each waited end to end on this node.
//
// The repliers for a block are its proposer and the next ReplyCount-1 nodes in
// node order. The proposer received every request in the block and answers on
// the request's own reply channel; the others answer on the client's
// connection to them. The client counts a request committed once ReplyCount
// distinct nodes have replied.
func (r *Replica) answerCommitted(payload []byte, proposer identity.NodeID) int {
	reqs, err := message.DecodeRequests(payload)
	if err != nil || len(reqs) == 0 {
		// Not request-encoded (generated workload, or an empty block).
		return 0
	}
	now := time.Now()
	inWindow := r.inMeasurementWindow()
	replier := r.repliesFor(proposer)
	for _, req := range reqs {
		held, mine := r.pending.take(req.ID)
		if mine {
			if inWindow {
				r.commitWait.add(now.Sub(held.Arrival))
			}
			held.Reply(message.RequestReply{ID: req.ID})
			continue
		}
		if replier {
			r.ReplyTo(req.ClientID, message.RequestReply{ID: req.ID})
		}
	}
	return len(reqs)
}

// repliesFor reports whether this node replies for blocks from proposer.
func (r *Replica) repliesFor(proposer identity.NodeID) bool {
	cfg := config.GetConfig()
	offset := (r.ID().Node() - proposer.Node() + cfg.N) % cfg.N
	return offset < cfg.ReplyCount()
}
