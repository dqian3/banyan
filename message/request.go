package message

import (
	"bytes"
	"encoding/gob"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"banyan/crypto"
)

// ErrBadRequestSignature reports a block carrying a request whose client
// signature does not check out.
var ErrBadRequestSignature = errors.New("block carries a request with an invalid client signature")

func init() {
	gob.Register(Request{})
	gob.Register(RequestReply{})
	gob.Register(ClientRequest{})
}

// ClientRequest is what a client puts on the wire over a pipelined
// connection. It carries no reply channel and no timestamps: the reply is
// matched by ID on the way back, and arrival is stamped by the node.
type ClientRequest struct {
	ID      string
	Payload []byte

	// ClientID names the keypair that signed this request, and Sig is the
	// signature over crypto.SignedRequestBytes. Both travel on into the block
	// so that every node -- not just the one the client happened to reach --
	// can check the request it is being asked to order.
	ClientID uint32
	Sig      crypto.Signature
}

// Request is a client request awaiting inclusion in a block.
//
// The reply channel is created by whichever node received the request over
// HTTP, and only that node can answer: gob drops chan fields when a block
// travels to peers, so a request decoded from a block payload elsewhere has
// C == nil. That is also why Arrival is only meaningful on the receiving node
// — which is exactly where the queueing delay we want to measure happens,
// since a node proposes from its own mempool and never forwards.
type Request struct {
	ID      string
	Payload []byte

	// Carried from the client through the mempool and into the block payload,
	// so a peer can verify this request without having seen the client.
	ClientID uint32
	Sig      crypto.Signature

	// Arrival is stamped by the mempool when the request is admitted, and is
	// the baseline for both the propose wait (arrival -> included in a block)
	// and the commit wait (arrival -> that block commits). Local to the
	// receiving node, so no cross-machine clock skew enters either number.
	Arrival time.Time

	C chan RequestReply
}

// Reply answers the client that sent this request. A no-op on nodes that
// received the request inside a block rather than from a client.
func (r *Request) Reply(reply RequestReply) {
	if r.C == nil {
		return
	}
	// Non-blocking: the client may have given up (its context deadline
	// passed), and a commit path must never stall on a departed client.
	select {
	case r.C <- reply:
	default:
	}
}

// RequestReply tells a client its request committed.
type RequestReply struct {
	ID  string
	Err string
}

// wireRequest is what actually goes into a block payload: identity and bytes,
// no timestamps and no channel. Keeping the payload encoding independent of
// the in-memory struct means a block hashes identically on every node
// regardless of when each one saw the request.
type wireRequest struct {
	ID       string
	Payload  []byte
	ClientID uint32
	Sig      crypto.Signature
}

// EncodeRequests serializes requests for a block payload.
func EncodeRequests(reqs []*Request) ([]byte, error) {
	wire := make([]wireRequest, 0, len(reqs))
	for _, r := range reqs {
		wire = append(wire, wireRequest{
			ID:       r.ID,
			Payload:  r.Payload,
			ClientID: r.ClientID,
			Sig:      r.Sig,
		})
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(wire); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeRequests reads back what EncodeRequests wrote. Payloads produced by
// the generated workload are not request-encoded, so callers should treat a
// decode failure as "this block carries no client requests" rather than an
// error worth killing the node over.
func DecodeRequests(payload []byte) ([]*Request, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var wire []wireRequest
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&wire); err != nil {
		return nil, err
	}
	reqs := make([]*Request, 0, len(wire))
	for i := range wire {
		reqs = append(reqs, &Request{
			ID:       wire[i].ID,
			Payload:  wire[i].Payload,
			ClientID: wire[i].ClientID,
			Sig:      wire[i].Sig,
		})
	}
	return reqs, nil
}

// Verify checks the client's signature over this request.
func (r *Request) Verify() bool {
	return crypto.VerifyRequest(r.ClientID, r.ID, r.Payload, r.Sig)
}

// VerifyRequestPayload checks every client signature in a block payload,
// returning the number of requests it covered.
//
// This is the per-request work the other protocols in the evaluation do and
// banyan did not: a PBFT backup re-verifies each client signature carried in a
// PRE-PREPARE, and aspen verifies one per request before it may be ordered.
// Here it runs on the block a peer proposed, so a node never orders a request
// it has not checked itself.
//
// Verification is spread across cores. A block can carry hundreds of requests
// and an Ed25519 verify is tens of microseconds, so doing this serially on the
// consensus path would add milliseconds per block and measure Go's scheduler
// rather than the protocol.
func VerifyRequestPayload(payload []byte) (int, error) {
	reqs, err := DecodeRequests(payload)
	if err != nil || len(reqs) == 0 {
		// Not request-encoded: the generated workload, or an empty block.
		return 0, nil
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(reqs) {
		workers = len(reqs)
	}
	if workers < 1 {
		workers = 1
	}

	var (
		next int64 = -1
		bad  int64
		wg   sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&next, 1))
				if i >= len(reqs) {
					return
				}
				// Stop early once one has failed: the block is rejected
				// whatever the rest say, and a peer that sends garbage should
				// not be able to make every node verify a full block of it.
				if atomic.LoadInt64(&bad) > 0 {
					return
				}
				if !reqs[i].Verify() {
					atomic.AddInt64(&bad, 1)
					return
				}
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt64(&bad) > 0 {
		return len(reqs), ErrBadRequestSignature
	}
	return len(reqs), nil
}

// EncodedSize is the marshalled size of a request inside a block payload,
// used by the mempool to fill a block up to a byte budget without encoding
// speculatively. The constant covers gob's per-element framing and the ID;
// it only has to be close, since the budget is a cap and not a guarantee.
func (r *Request) EncodedSize() int {
	sig := 0
	for _, part := range r.Sig {
		sig += len(part)
	}
	// The signature rides along in the block, so it counts against the byte
	// budget: leaving it out would silently overfill every block by 64 bytes
	// per request once requests became signed.
	return len(r.Payload) + len(r.ID) + sig + 32
}
