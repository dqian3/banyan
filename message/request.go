package message

import (
	"bytes"
	"encoding/gob"
	"time"
)

func init() {
	gob.Register(Request{})
	gob.Register(RequestReply{})
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
	ID      string
	Payload []byte
}

// EncodeRequests serializes requests for a block payload.
func EncodeRequests(reqs []*Request) ([]byte, error) {
	wire := make([]wireRequest, 0, len(reqs))
	for _, r := range reqs {
		wire = append(wire, wireRequest{ID: r.ID, Payload: r.Payload})
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
		reqs = append(reqs, &Request{ID: wire[i].ID, Payload: wire[i].Payload})
	}
	return reqs, nil
}

// EncodedSize is the marshalled size of a request inside a block payload,
// used by the mempool to fill a block up to a byte budget without encoding
// speculatively. The constant covers gob's per-element framing and the ID;
// it only has to be close, since the budget is a cap and not a guarantee.
func (r *Request) EncodedSize() int {
	return len(r.Payload) + len(r.ID) + 24
}
