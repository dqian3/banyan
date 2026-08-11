// Package mempool buffers client requests between arrival and proposal.
//
// Restored from Bamboo (which banyan's fork removed along with the client)
// and adapted in two ways: the block budget is in bytes rather than a
// transaction count, matching banyan's byte-sized blocks, and admission
// stamps an arrival time so the replica can report how long a request waited
// to be proposed.
//
// A node proposes only from its own pool and never forwards, so a request
// waits at the node the client sent it to until that node is leader. That
// wait is the queueing delay the benchmark reports.
package mempool

import (
	"container/list"
	"sync"
	"time"

	"banyan/message"
)

// MemPool is a FIFO queue of pending client requests, bounded so a node under
// sustained overload sheds new requests instead of growing without limit.
type MemPool struct {
	mu       sync.Mutex
	requests *list.List // of *message.Request
	limit    int

	totalReceived int64
	totalDropped  int64
}

func NewMemPool(limit int) *MemPool {
	if limit <= 0 {
		limit = 100000
	}
	return &MemPool{requests: list.New(), limit: limit}
}

// Add admits a client request, stamping its arrival time. Returns false when
// the pool is full, which the caller should report to the client rather than
// silently dropping — an overloaded node that answers nothing is
// indistinguishable from a slow one in the client's latency numbers.
func (mp *MemPool) Add(req *message.Request) bool {
	if req == nil {
		return false
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if mp.requests.Len() >= mp.limit {
		mp.totalDropped++
		return false
	}
	req.Arrival = time.Now()
	mp.totalReceived++
	mp.requests.PushBack(req)
	return true
}

// Drain removes requests for one block, up to maxBytes of encoded payload.
// Returns them in arrival order. An empty pool yields an empty block rather
// than padding: an underfull block commits fewer requests, and the byte
// budget stays a cap rather than a target.
func (mp *MemPool) Drain(maxBytes int) []*message.Request {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	var (
		batch []*message.Request
		used  int
	)
	for mp.requests.Len() > 0 {
		front := mp.requests.Front()
		req, ok := front.Value.(*message.Request)
		if !ok {
			mp.requests.Remove(front)
			continue
		}
		size := req.EncodedSize()
		// Always take at least one request, even if a single oversized
		// payload blows the budget on its own — otherwise it wedges the head
		// of the queue forever and the node silently stops committing.
		if used+size > maxBytes && len(batch) > 0 {
			break
		}
		mp.requests.Remove(front)
		batch = append(batch, req)
		used += size
	}
	return batch
}

// Size is the current queue depth.
func (mp *MemPool) Size() int {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.requests.Len()
}

// Stats reports admitted and shed counts since startup.
func (mp *MemPool) Stats() (received int64, dropped int64) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.totalReceived, mp.totalDropped
}
