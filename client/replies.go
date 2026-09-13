package main

import (
	"sync"
	"time"
)

// replyTracker matches replies to outstanding requests across every node
// connection. A request completes once `need` distinct nodes have replied,
// or fails as soon as any node replies with an error.
type replyTracker struct {
	need   int
	onDone func(id string, latency time.Duration, ok bool)

	mu      sync.Mutex
	pending map[string]*outstanding
}

type outstanding struct {
	sent  time.Time
	seen  uint64 // bit i is set once node i has replied
	count int
}

func newReplyTracker(need int, onDone func(string, time.Duration, bool)) *replyTracker {
	return &replyTracker{
		need:    need,
		onDone:  onDone,
		pending: make(map[string]*outstanding),
	}
}

func (t *replyTracker) add(id string) {
	t.mu.Lock()
	t.pending[id] = &outstanding{sent: time.Now()}
	t.mu.Unlock()
}

// drop forgets a request that never reached the wire.
func (t *replyTracker) drop(id string) {
	t.mu.Lock()
	delete(t.pending, id)
	t.mu.Unlock()
}

// reply records node's answer to id. Only the node that received a request
// replies with an error, and a request it refused is never committed, so an
// error fails the request outright.
func (t *replyTracker) reply(id string, node int, errMsg string) {
	t.mu.Lock()
	o, ok := t.pending[id]
	if !ok {
		t.mu.Unlock()
		return // already complete, or timed out
	}
	bit := uint64(1) << uint(node)
	if errMsg == "" && o.seen&bit != 0 {
		t.mu.Unlock()
		return // a second reply from the same node
	}
	o.seen |= bit
	o.count++
	done := errMsg != "" || o.count >= t.need
	if done {
		delete(t.pending, id)
	}
	t.mu.Unlock()
	if done {
		t.onDone(id, time.Since(o.sent), errMsg == "")
	}
}

// expire fails requests older than `timeout`, so a lost reply frees its
// in-flight slot instead of stalling the client forever. Returns how many.
func (t *replyTracker) expire(timeout time.Duration) int {
	cutoff := time.Now().Add(-timeout)
	var stale []string
	t.mu.Lock()
	for id, o := range t.pending {
		if o.sent.Before(cutoff) {
			stale = append(stale, id)
		}
	}
	for _, id := range stale {
		delete(t.pending, id)
	}
	t.mu.Unlock()
	for _, id := range stale {
		t.onDone(id, timeout, false)
	}
	return len(stale)
}

func (t *replyTracker) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}
